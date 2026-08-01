"""Collect auditable point-in-time DEX pool-state depth snapshots.

The collector measures how much quote notional can trade against one pool
before its *marginal* price reaches 10/25/50/100 bps from the starting price.
That makes the result analogous to consuming a CEX order book up to a price
band.  It never derives depth from TVL or historical volume.

Supported pool-state models in this first release:

- Uniswap V2 and SushiSwap V2 constant-product pools;
- Uniswap V3-compatible concentrated-liquidity pools whose contracts expose
  ``slot0``, ``liquidity``, ``fee``, ``tickSpacing``, ``tickBitmap`` and
  ``ticks``.

Every inventory pool receives an explicit observed/partial/unsupported/failed
row.  All EVM calls for a chain use one fixed block tag, and the raw JSON-RPC
request/response transcript is retained with a SHA-256 hash.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from decimal import (
    Decimal,
    InvalidOperation,
    ROUND_FLOOR,
    localcontext,
)
from pathlib import Path
from typing import Any, Callable, Iterable

try:
    import certifi
except ImportError:  # pragma: no cover - system trust remains the safe fallback
    certifi = None

try:
    from scripts.collection_deadline import (
        CollectionDeadline,
        CollectionDeadlineExceeded,
    )
    from scripts.atomic_publication import atomic_replace_bundle, csv_payload
    from scripts.bounded_snapshot_merge import (
        merge_exact_market_snapshot,
        require_aligned_depth_execution_lineage,
        validate_exact_publication_scope,
    )
    from scripts.execution_cost import (
        EXECUTION_COST_COLUMNS,
        EXECUTION_DIRECTIONS,
        EXECUTION_NOTIONALS_USD,
        execution_fact_row,
        status_counts as execution_status_counts,
        usd_price_timing,
        validate_execution_snapshot,
    )
    from scripts.publication_gate import (
        CoverageRegressionError,
        bind_passing_coverage_report,
        enforce_publication_coverage,
        enforce_publication_coverage_bundle,
        publication_rows_sha256,
        validate_passing_coverage_report,
    )
    from scripts.quality_outcomes import (
        normalize_dex_depth_source_outcome,
        quality_outcome_resolution_state,
    )
    from scripts.timestamp_contract import validate_observation_bounds
except ModuleNotFoundError:
    from collection_deadline import CollectionDeadline, CollectionDeadlineExceeded
    from atomic_publication import atomic_replace_bundle, csv_payload
    from bounded_snapshot_merge import (
        merge_exact_market_snapshot,
        require_aligned_depth_execution_lineage,
        validate_exact_publication_scope,
    )
    from execution_cost import (
        EXECUTION_COST_COLUMNS,
        EXECUTION_DIRECTIONS,
        EXECUTION_NOTIONALS_USD,
        execution_fact_row,
        status_counts as execution_status_counts,
        usd_price_timing,
        validate_execution_snapshot,
    )
    from publication_gate import (
        CoverageRegressionError,
        bind_passing_coverage_report,
        enforce_publication_coverage,
        enforce_publication_coverage_bundle,
        publication_rows_sha256,
        validate_passing_coverage_report,
    )
    from quality_outcomes import (
        normalize_dex_depth_source_outcome,
        quality_outcome_resolution_state,
    )
    from timestamp_contract import validate_observation_bounds


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TVL_CSV = PROJECT_ROOT / "data/local/dex_pool_tvl_latest.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/processed"
DEFAULT_PUBLISH_DIR = PROJECT_ROOT / "data/local"
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data/raw/dex-depth"

CURRENT_FILENAME = "dex_depth_snapshot.csv"
LATEST_FILENAME = "dex_depth_latest.csv"
HISTORY_FILENAME = "dex_depth_history.csv"
EXECUTION_CURRENT_FILENAME = "dex_execution_cost_snapshot.csv"
EXECUTION_LATEST_FILENAME = "dex_execution_cost_latest.csv"
MINIMUM_PUBLISHABLE_COVERAGE_BPS = 8000
MINIMUM_BASELINE_RETENTION_BPS = 9500
DEPTH_COVERAGE_POLICY = {
    "thresholds": {
        "allow_no_eligible_candidate": False,
        "minimum_candidate_usable_bps": MINIMUM_PUBLISHABLE_COVERAGE_BPS,
        "minimum_baseline_retention_bps": MINIMUM_BASELINE_RETENTION_BPS,
        "minimum_cohort_baseline_count": 5,
        "minimum_cohort_lost_count": 2,
        "minimum_cohort_retention_bps": 5000,
    },
    "usable_statuses": ["observed", "partial"],
    "excluded_statuses": ["unsupported"],
    "valid_statuses": ["failed", "observed", "partial", "unsupported"],
}
EXECUTION_COVERAGE_POLICY = {
    **DEPTH_COVERAGE_POLICY,
    "thresholds": {
        **DEPTH_COVERAGE_POLICY["thresholds"],
        "allow_no_eligible_candidate": True,
    },
}
EXACT_DEPTH_COVERAGE_POLICY = {
    **DEPTH_COVERAGE_POLICY,
    "thresholds": {
        **DEPTH_COVERAGE_POLICY["thresholds"],
        "allow_no_eligible_candidate": True,
        "minimum_candidate_usable_bps": 0,
        "minimum_baseline_retention_bps": 10_000,
    },
}
EXACT_EXECUTION_COVERAGE_POLICY = {
    **EXACT_DEPTH_COVERAGE_POLICY,
    "thresholds": {
        **EXACT_DEPTH_COVERAGE_POLICY["thresholds"],
        "allow_no_eligible_candidate": True,
    },
}

DEPTH_BANDS_BPS = (10, 25, 50, 100)
DEX_DEPTH_METHOD = "fixed_block_pool_state_marginal_price_band"
DEX_EXECUTION_METHOD = "fixed_block_pool_state_exact_target_quantity_v1"
DEX_EXECUTION_REFERENCE = "pre_fee_pool_state_marginal_price"
DEX_EXECUTION_EXCLUDED_COSTS = (
    "gas,router_fee,token_transfer_tax,MEV,post_block_state_changes"
)
REQUEST_SLEEP_SECONDS = 0.15
MAX_RETRIES = 4
Q96 = Decimal(2**96)
ONE_MILLION = Decimal(1_000_000)
TLS_CONTEXT = (
    ssl.create_default_context(cafile=certifi.where())
    if certifi
    else ssl.create_default_context()
)

DEFAULT_RPC_URLS = {
    "eth": "https://ethereum-rpc.publicnode.com",
    "arbitrum": "https://arb1.arbitrum.io/rpc",
    "optimism": "https://mainnet.optimism.io",
    # Base documents mainnet.base.org as rate-limited and not suitable for
    # production systems. PublicNode is the keyless default; operators can
    # override it with DEX_DEPTH_RPC_BASE.
    "base": "https://base-rpc.publicnode.com",
    "bsc": "https://bsc-dataseed.bnbchain.org",
    "zksync": "https://mainnet.era.zksync.io",
}
RPC_ENV_KEYS = {
    chain: f"DEX_DEPTH_RPC_{chain.upper()}"
    for chain in DEFAULT_RPC_URLS
}
_PUBLIC_RPC_HOSTS = frozenset(
    {
        "ethereum-rpc.publicnode.com",
        "arb1.arbitrum.io",
        "mainnet.optimism.io",
        "base-rpc.publicnode.com",
        "bsc-dataseed.bnbchain.org",
        "mainnet.era.zksync.io",
        # Deterministic non-routable fixtures used by the data-contract tests.
        "example.test",
        "rpc.example.test",
    }
)

V2_FEE_BPS = {
    "uniswap_v2": Decimal("30"),
    "sushiswap": Decimal("30"),
    "shibaswap": Decimal("30"),
    "pancakeswap_v2": Decimal("25"),
    "pancakeswap-v2-zksync": Decimal("25"),
}
V3_DEXES = {
    "aerodrome-slipstream",
    "uniswap_v3",
    "uniswap_v3_arbitrum",
    "uniswap_v3_optimism",
    "uniswap-v3-base",
    "uniswap-v3-zksync",
    "sushiswap-v3-ethereum",
    "pancakeswap-v3-bsc",
    "velodrome-finance-slipstream",
}

# Ethereum ABI selectors.  The signatures are documented in the protocol
# interfaces cited by docs/dex-depth-data-contract.md.
SELECTOR_TOKEN0 = "0x0dfe1681"
SELECTOR_TOKEN1 = "0xd21220a7"
SELECTOR_GET_RESERVES = "0x0902f1ac"
SELECTOR_DECIMALS = "0x313ce567"
SELECTOR_SYMBOL = "0x95d89b41"
SELECTOR_SLOT0 = "0x3850c7bd"
SELECTOR_LIQUIDITY = "0x1a686502"
SELECTOR_FEE = "0xddca3f43"
SELECTOR_TICK_SPACING = "0xd0c93a7c"
SELECTOR_TICK_BITMAP = "0x5339c296"
SELECTOR_TICKS = "0xf30dba93"

BASE_COLUMNS = [
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "protocol_model",
    "block_number",
    "block_timestamp",
    "target_token_address",
    "target_token_position",
    "token0_address",
    "token0_symbol",
    "token0_decimals",
    "token0_price_usd",
    "token1_address",
    "token1_symbol",
    "token1_decimals",
    "token1_price_usd",
    "fee_bps",
    "pool_state_price_usd",
    "source_target_price_usd",
    "price_difference_bps",
    "usd_price_source_snapshot_id",
    "usd_price_observed_at",
    "usd_price_skew_seconds",
    "usd_price_freshness_status",
    "usd_price_source",
    "usd_price_source_endpoint",
    "usd_price_raw_response_sha256",
]
DEPTH_COLUMNS = [
    field
    for band in DEPTH_BANDS_BPS
    for field in (
        f"sell_depth_{band}bps_usd",
        f"buy_depth_{band}bps_usd",
        f"total_depth_{band}bps_usd",
        f"depth_{band}bps_complete",
    )
]
TRAILING_COLUMNS = [
    "depth_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
    "status",
    "error",
]
DEX_DEPTH_COLUMNS = BASE_COLUMNS + DEPTH_COLUMNS + TRAILING_COLUMNS


class RpcError(RuntimeError):
    """Raised when a JSON-RPC endpoint returns no usable result."""


def utc_now_text() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def pool_key(chain: str, pool_address: str) -> tuple[str, str]:
    address = pool_address.strip()
    if address.startswith("0x"):
        address = address.lower()
    return chain.strip().lower(), address


def finite_decimal(value: Any, *, positive: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"Invalid decimal value: {value}") from error
    if not number.is_finite() or (positive and number <= 0):
        raise ValueError(f"Invalid {'positive ' if positive else ''}decimal value: {value}")
    return number


def decimal_text(value: Decimal | int | float | str | None) -> str:
    if value is None:
        return ""
    number = finite_decimal(value)
    if number == 0:
        return "0"
    # Decimal.normalize() applies the active context precision and can silently
    # round large raw-unit facts.  Fixed-point formatting preserves every
    # coefficient digit; only insignificant fractional trailing zeroes are
    # removed.
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def bool_text(value: bool) -> str:
    return "1" if value else "0"


def address_from_token_id(value: str) -> str:
    token_id = value.strip()
    if "_" not in token_id:
        return ""
    address = token_id.split("_", 1)[1]
    return address.lower() if address.startswith("0x") else address


def sanitize_endpoint(url: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(url)
        scheme = parsed.scheme.lower()
        host = (parsed.hostname or "").lower()
        port_number = parsed.port
    except (TypeError, ValueError):
        return "rpc-endpoint:redacted"
    if scheme not in {"http", "https"} or not host:
        return "rpc-endpoint:redacted"
    port = ":{}".format(port_number) if port_number is not None else ""
    canonical_endpoint = "{}://{}{}".format(scheme, host, port)
    if host in _PUBLIC_RPC_HOSTS:
        return canonical_endpoint
    digest = hashlib.sha256(canonical_endpoint.encode("utf-8")).hexdigest()
    return "rpc-endpoint-sha256:{}".format(digest)


def is_canonical_rpc_quantity(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("0x"):
        return False
    digits = value[2:]
    if not digits or any(character not in "0123456789abcdef" for character in digits):
        return False
    return digits == "0" or digits[0] != "0"


def canonical_rpc_quantity(value: Any, method: str) -> str:
    if not is_canonical_rpc_quantity(value):
        raise RpcError(f"{method} returned a noncanonical RPC quantity")
    return value


def expected_rpc_id(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    request_id = payload.get("id")
    return request_id if type(request_id) is int else None


def redacted_rpc_record_request(payload: Any) -> Any:
    """Remove concrete calls and simulation senders from persisted RPC logs."""
    if not isinstance(payload, dict) or payload.get("method") != "eth_estimateGas":
        return payload
    request_id = expected_rpc_id(payload)
    params = payload.get("params")
    if not isinstance(params, list) or len(params) != 2:
        return {
            "jsonrpc": payload.get("jsonrpc"),
            "id": request_id,
            "method": "eth_estimateGas",
            "params": "redacted_invalid_request",
        }
    call = params[0]
    if isinstance(call, dict):
        try:
            encoded = json.dumps(
                call,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        except (TypeError, ValueError):
            call_hash = "invalid_call"
        else:
            call_hash = hashlib.sha256(encoded).hexdigest()
    else:
        call_hash = "invalid_call"
    block_tag = params[1]
    if not (
        isinstance(block_tag, str)
        and block_tag.startswith("0x")
        and len(block_tag) > 2
        and all(character in "0123456789abcdef" for character in block_tag[2:])
    ):
        block_tag = "invalid_block"
    return {
        "jsonrpc": payload.get("jsonrpc"),
        "id": request_id,
        "method": "eth_estimateGas",
        "params": [
            {"tx_call_sha256": call_hash},
            block_tag,
        ],
    }


def redacted_rpc_record_response(payload: Any, response: Any) -> Any:
    if isinstance(payload, list):
        if not isinstance(response, list):
            return {"status": "invalid_batch_response"}
        by_id = {
            item.get("id"): item
            for item in response
            if isinstance(item, dict) and type(item.get("id")) is int
        }
        return [
            redacted_rpc_record_response(
                request,
                by_id.get(expected_rpc_id(request)),
            )
            for request in payload
        ]
    if not isinstance(payload, dict):
        return {"status": "invalid_request_envelope"}
    request_id = expected_rpc_id(payload)
    method = payload.get("method")
    safe = {"jsonrpc": "2.0", "id": request_id}
    if not isinstance(response, dict):
        return {**safe, "status": "invalid_response"}
    if (
        response.get("jsonrpc") != "2.0"
        or type(response.get("id")) is not int
        or response.get("id") != request_id
    ):
        return {**safe, "status": "invalid_response_envelope"}
    if "error" in response:
        error = response.get("error")
        error_code = error.get("code") if isinstance(error, dict) else None
        if type(error_code) is not int:
            error_code = None
        return {
            **safe,
            "error_code": error_code,
            "status": "rpc_method_failed",
        }
    if "result" not in response:
        return {**safe, "status": "missing_result"}
    result = response.get("result")
    if method == "eth_getBlockByNumber":
        if not isinstance(result, dict):
            return {**safe, "status": "invalid_block_result"}
        number = result.get("number")
        block_hash = result.get("hash")
        timestamp = result.get("timestamp")
        if not is_canonical_rpc_quantity(number):
            return {**safe, "status": "invalid_block_result"}
        if not (
            isinstance(block_hash, str)
            and block_hash.startswith("0x")
            and len(block_hash) == 66
            and all(
                character in "0123456789abcdef"
                for character in block_hash[2:]
            )
        ):
            block_hash = None
        if timestamp is not None and not is_canonical_rpc_quantity(timestamp):
            timestamp = None
        return {
            **safe,
            "result": {
                "number": number,
                "hash": block_hash,
                "timestamp": timestamp,
            },
        }
    if method in ("eth_chainId", "eth_blockNumber", "eth_estimateGas"):
        if not is_canonical_rpc_quantity(result):
            return {**safe, "status": "invalid_quantity_result"}
        return {**safe, "result": result}
    if isinstance(result, str) and result.startswith("0x") and all(
        character in "0123456789abcdef" for character in result[2:]
    ):
        return {**safe, "result": result}
    return {**safe, "status": "result_redacted"}


def rpc_url_for_chain(chain: str) -> str | None:
    normalized = chain.lower()
    configured = os.environ.get(RPC_ENV_KEYS.get(normalized, ""))
    return configured or DEFAULT_RPC_URLS.get(normalized)


def protocol_model(dex: str, chain: str, pool_address: str) -> tuple[str, str]:
    normalized = dex.lower()
    if chain.lower() not in DEFAULT_RPC_URLS:
        return "unsupported", f"unsupported_chain:{chain.lower()}"
    if not (
        pool_address.startswith("0x")
        and len(pool_address) == 42
        and all(character in "0123456789abcdefABCDEF" for character in pool_address[2:])
    ):
        return "unsupported", "pool_is_not_an_evm_contract_address"
    if normalized in V2_FEE_BPS:
        return "constant_product_v2", ""
    if normalized in V3_DEXES:
        return "concentrated_liquidity_v3", ""
    return "unsupported", f"unsupported_pool_model:{normalized}"


def load_pool_inventory(path: Path = DEFAULT_TVL_CSV) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"TVL inventory does not exist: {path}")
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "snapshot_id",
            "observed_at",
            "token_symbol",
            "chain",
            "dex",
            "pool_address",
            "pool_name",
            "base_token_id",
            "quote_token_id",
            "base_token_price_usd",
            "quote_token_price_usd",
            "status",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{path.name} is missing columns: {', '.join(missing)}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no pool rows")

    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (
            row["token_symbol"].upper(),
            *pool_key(row["chain"], row["pool_address"]),
        )
        timestamp = row.get("response_received_at") or row.get("observed_at") or ""
        previous = latest.get(key)
        previous_timestamp = (
            previous.get("response_received_at") or previous.get("observed_at") or ""
            if previous
            else ""
        )
        if previous is None or timestamp >= previous_timestamp:
            latest[key] = row
    return sorted(
        latest.values(),
        key=lambda row: (
            row["chain"].lower(),
            row["token_symbol"].upper(),
            row["dex"].lower(),
            row["pool_address"].lower(),
        ),
    )


def pool_usd_price_timing(
    pool: dict[str, str],
    block_timestamp: str,
) -> dict[str, Any]:
    """Return the observable timing relationship for one pool's USD inputs."""
    return usd_price_timing(
        block_timestamp,
        pool.get("response_received_at") or pool.get("observed_at") or "",
    )


def require_usable_pool_usd_price(
    pool: dict[str, str],
    block_timestamp: str,
) -> dict[str, Any]:
    """Fail closed before publishing USD depth or execution from stale inputs."""
    if pool.get("status") != "observed":
        raise ValueError(
            "usd_price_conversion_unavailable:"
            f"tvl_inventory_status_{pool.get('status') or 'missing'}"
        )
    finite_decimal(pool.get("base_token_price_usd"), positive=True)
    finite_decimal(pool.get("quote_token_price_usd"), positive=True)
    timing = pool_usd_price_timing(pool, block_timestamp)
    if not timing["usable"]:
        raise ValueError(
            "usd_price_conversion_unavailable:"
            f"{timing['reason']}"
        )
    return timing


def http_json_rpc(
    url: str,
    payload: Any,
    *,
    deadline: CollectionDeadline | None = None,
    timeout_seconds: float = 30,
    max_retries: int = MAX_RETRIES,
) -> tuple[Any, bytes]:
    if max_retries < 1:
        raise ValueError("max_retries must be at least 1")
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "CEX-DEX-Market-Monitor/2.0",
        },
        method="POST",
    )
    for attempt in range(max_retries):
        try:
            timeout = (
                deadline.request_timeout(timeout_seconds)
                if deadline is not None
                else timeout_seconds
            )
            with urllib.request.urlopen(
                request,
                timeout=timeout,
                context=TLS_CONTEXT,
            ) as response:
                raw = response.read()
                return json.loads(raw.decode("utf-8")), raw
        except urllib.error.HTTPError as error:
            if deadline is not None:
                deadline.require_remaining()
            retryable = error.code == 429 or 500 <= error.code < 600
            if not retryable or attempt + 1 >= max_retries:
                raise
            retry_after = float(error.headers.get("Retry-After") or 0)
            delay = max(retry_after, 2 ** attempt)
            if deadline is not None:
                deadline.sleep_before_retry(delay)
            else:
                time.sleep(delay)
        except urllib.error.URLError:
            if deadline is not None:
                deadline.require_remaining()
            if attempt + 1 >= max_retries:
                raise
            delay = max(1.0, 2 ** attempt)
            if deadline is not None:
                deadline.sleep_before_retry(delay)
            else:
                time.sleep(delay)
    raise RpcError(f"JSON-RPC request failed after retries: {sanitize_endpoint(url)}")


class RpcClient:
    def __init__(
        self,
        chain: str,
        url: str,
        *,
        request: Callable[[str, Any], tuple[Any, bytes]] = http_json_rpc,
        deadline: CollectionDeadline | None = None,
        timeout_seconds: float = 30,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self.chain = chain
        self.url = url
        self.endpoint = sanitize_endpoint(url)
        self.request = request
        self.deadline = deadline
        self._call_deadline = deadline
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.records: list[dict[str, Any]] = []
        self._next_id = 1

    def _send(self, payload: Any) -> Any:
        effective_deadline = self._call_deadline or self.deadline
        if effective_deadline is not None:
            effective_deadline.require_remaining()
        if (
            effective_deadline is None
            and self.timeout_seconds == 30
            and self.max_retries == MAX_RETRIES
        ):
            response, raw = self.request(self.url, payload)
        else:
            response, raw = self.request(
                self.url,
                payload,
                deadline=effective_deadline,
                timeout_seconds=self.timeout_seconds,
                max_retries=self.max_retries,
            )
        self.records.append(
            {
                "request": redacted_rpc_record_request(payload),
                "response": redacted_rpc_record_response(payload, response),
                "response_sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
        return response

    def method(self, method: str, params: list[Any]) -> Any:
        request_id = self._next_id
        self._next_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
        response = self._send(payload)
        if not isinstance(response, dict):
            raise RpcError(f"{method} returned a non-object response")
        if response.get("jsonrpc") != "2.0":
            raise RpcError(f"{method} returned an invalid JSON-RPC version")
        if type(response.get("id")) is not int or response.get("id") != request_id:
            raise RpcError(f"{method} returned a mismatched JSON-RPC id")
        if "error" in response:
            error = response.get("error")
            error_code = error.get("code") if isinstance(error, dict) else None
            suffix = (
                f" code={error_code}"
                if type(error_code) is int
                else ""
            )
            raise RpcError(f"{method} failed{suffix}")
        if "result" not in response:
            raise RpcError(f"{method} returned no result")
        return response["result"]

    def block_number(self) -> int:
        result = self.method("eth_blockNumber", [])
        return int(canonical_rpc_quantity(result, "eth_blockNumber"), 16)

    def chain_id(self) -> str:
        result = self.method("eth_chainId", [])
        return canonical_rpc_quantity(result, "eth_chainId")

    def block(self, block_tag: str) -> dict[str, Any]:
        result = self.method("eth_getBlockByNumber", [block_tag, False])
        if not isinstance(result, dict):
            raise RpcError("eth_getBlockByNumber returned no block")
        canonical_rpc_quantity(
            result.get("number"),
            "eth_getBlockByNumber.number",
        )
        return result

    def fee_history(
        self,
        block_count: str,
        newest_block: str,
        reward_percentiles: list[int],
    ) -> dict[str, Any]:
        canonical_rpc_quantity(block_count, "eth_feeHistory.block_count")
        canonical_rpc_quantity(newest_block, "eth_feeHistory.newest_block")
        if reward_percentiles != [50]:
            raise RpcError("eth_feeHistory reward percentiles are unsupported")
        result = self.method(
            "eth_feeHistory",
            [block_count, newest_block, reward_percentiles],
        )
        if not isinstance(result, dict):
            raise RpcError("eth_feeHistory returned a non-object result")
        return result

    def estimate_gas(self, tx_call: dict[str, str], block_tag: str) -> str:
        result = self.method("eth_estimateGas", [tx_call, block_tag])
        return canonical_rpc_quantity(result, "eth_estimateGas")

    def eth_calls(self, to: str, data_values: list[str], block_tag: str) -> list[str]:
        requests = []
        for data in data_values:
            request_id = self._next_id
            self._next_id += 1
            requests.append(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "eth_call",
                    "params": [{"to": to, "data": data}, block_tag],
                }
            )
        try:
            response = self._send(requests)
            if not isinstance(response, list):
                raise RpcError("Batch eth_call returned a non-list response")
            by_id = {item.get("id"): item for item in response if isinstance(item, dict)}
            results = []
            for request_item in requests:
                item = by_id.get(request_item["id"])
                if not item or item.get("error") or "result" not in item:
                    raise RpcError(f"eth_call failed: {item}")
                results.append(item["result"])
            return results
        except (RpcError, ValueError):
            results = []
            for request_item in requests:
                response_item = self._send(request_item)
                if (
                    not isinstance(response_item, dict)
                    or response_item.get("error")
                    or "result" not in response_item
                ):
                    raise RpcError(f"eth_call failed: {response_item}")
                results.append(response_item["result"])
            return results


class _DeadlineBoundRpcClient:
    """Check a deadline before each call while preserving client-owned state."""

    def __init__(self, client: RpcClient, deadline: CollectionDeadline) -> None:
        self._client = client
        self._deadline = deadline
        self.records = client.records
        self.endpoint = client.endpoint

    def _call(self, operation: Callable[..., Any], *args: Any) -> Any:
        self._deadline.require_remaining()
        if not isinstance(self._client, RpcClient):
            return operation(*args)
        previous_deadline = self._client._call_deadline
        self._client._call_deadline = self._deadline
        try:
            return operation(*args)
        finally:
            self._client._call_deadline = previous_deadline

    def block_number(self) -> int:
        return self._call(self._client.block_number)

    def chain_id(self) -> str:
        return self._call(self._client.chain_id)

    def block(self, block_tag: str) -> dict[str, Any]:
        return self._call(self._client.block, block_tag)

    def fee_history(
        self,
        block_count: str,
        newest_block: str,
        reward_percentiles: list[int],
    ) -> dict[str, Any]:
        return self._call(
            self._client.fee_history,
            block_count,
            newest_block,
            reward_percentiles,
        )

    def estimate_gas(self, tx_call: dict[str, str], block_tag: str) -> str:
        return self._call(self._client.estimate_gas, tx_call, block_tag)

    def eth_calls(self, to: str, data_values: list[str], block_tag: str) -> list[str]:
        return self._call(self._client.eth_calls, to, data_values, block_tag)


def words(hex_data: str) -> list[str]:
    value = hex_data[2:] if hex_data.startswith("0x") else hex_data
    if len(value) % 64:
        raise ValueError("ABI response is not aligned to 32-byte words")
    return [value[index:index + 64] for index in range(0, len(value), 64)]


def decode_uint(hex_data: str, index: int = 0) -> int:
    values = words(hex_data)
    if index >= len(values):
        raise ValueError("ABI response contains too few words")
    return int(values[index], 16)


def decode_int(hex_data: str, index: int = 0, bits: int = 256) -> int:
    value = decode_uint(hex_data, index)
    sign_bit = 1 << (bits - 1)
    mask = (1 << bits) - 1
    value &= mask
    return value - (1 << bits) if value & sign_bit else value


def decode_address(hex_data: str) -> str:
    value = words(hex_data)[0][-40:]
    return "0x" + value.lower()


def decode_symbol(hex_data: str) -> str:
    values = words(hex_data)
    if not values:
        return ""
    if int(values[0], 16) == 32 and len(values) >= 2:
        length = int(values[1], 16)
        raw_hex = "".join(values[2:])[: length * 2]
    else:
        raw_hex = values[0].rstrip("0")
    try:
        return bytes.fromhex(raw_hex).decode("utf-8", errors="strict").strip("\x00")
    except (ValueError, UnicodeDecodeError):
        return ""


def encode_signed_word(value: int, bits: int) -> str:
    minimum = -(1 << (bits - 1))
    maximum = (1 << (bits - 1)) - 1
    if not minimum <= value <= maximum:
        raise ValueError(f"value is outside int{bits}")
    encoded = value if value >= 0 else (1 << 256) + value
    return f"{encoded:064x}"


def call_with_int(selector: str, value: int, bits: int) -> str:
    return selector + encode_signed_word(value, bits)


def price_map_from_inventory(row: dict[str, str]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for side in ("base", "quote"):
        address = address_from_token_id(row.get(f"{side}_token_id", ""))
        raw_price = row.get(f"{side}_token_price_usd", "")
        if address and raw_price:
            result[address.lower()] = finite_decimal(raw_price, positive=True)
    return result


def target_position(
    target_symbol: str,
    token0_symbol: str,
    token1_symbol: str,
) -> int:
    target = target_symbol.strip().upper()
    matches = [
        index
        for index, symbol in enumerate((token0_symbol, token1_symbol))
        if symbol.strip().upper() == target
    ]
    if len(matches) != 1:
        raise ValueError(
            f"target_token_not_identified:{target}:"
            f"{token0_symbol or '?'}:{token1_symbol or '?'}"
        )
    return matches[0]


def v2_band_amounts(
    reserve0: Decimal,
    reserve1: Decimal,
    fee_bps: Decimal,
    band_bps: int,
) -> dict[str, Decimal]:
    if reserve0 <= 0 or reserve1 <= 0:
        raise ValueError("V2 reserves must be positive")
    fee_fraction = fee_bps / Decimal(10_000)
    down_factor = Decimal(1) - Decimal(band_bps) / Decimal(10_000)
    up_factor = Decimal(1) + Decimal(band_bps) / Decimal(10_000)
    with localcontext() as context:
        context.prec = 90
        net0_in = reserve0 * (Decimal(1) / down_factor.sqrt() - Decimal(1))
        gross0_in = net0_in / (Decimal(1) - fee_fraction)
        new_reserve0 = reserve0 + net0_in
        token1_out = reserve1 - reserve0 * reserve1 / new_reserve0

        net1_in = reserve1 * (up_factor.sqrt() - Decimal(1))
        gross1_in = net1_in / (Decimal(1) - fee_fraction)
        new_reserve1 = reserve1 + net1_in
        token0_out = reserve0 - reserve0 * reserve1 / new_reserve1
    return {
        "zero_for_one_gross_input": gross0_in,
        "zero_for_one_output": token1_out,
        "one_for_zero_gross_input": gross1_in,
        "one_for_zero_output": token0_out,
    }


def _v2_integer(value: Decimal, *, label: str) -> int:
    number = finite_decimal(value)
    if number != number.to_integral_value():
        raise ValueError(f"{label} must be an integer raw-unit value")
    integer = int(number)
    if integer <= 0:
        raise ValueError(f"{label} must be positive")
    return integer


def _v2_fee_numerator(fee_bps: Decimal) -> int:
    fee = finite_decimal(fee_bps)
    if fee != fee.to_integral_value():
        raise ValueError("V2 fee_bps must be an integer")
    fee_integer = int(fee)
    if not 0 <= fee_integer < 10_000:
        raise ValueError("V2 fee_bps must be in [0, 10000)")
    return 10_000 - fee_integer


def v2_exact_input_quote(
    reserve_in: Decimal,
    reserve_out: Decimal,
    fee_bps: Decimal,
    amount_in: Decimal,
) -> Decimal:
    """Return integer quote output for one V2 exact-input swap."""
    reserve_in_integer = _v2_integer(reserve_in, label="reserve_in")
    reserve_out_integer = _v2_integer(reserve_out, label="reserve_out")
    amount_in_integer = _v2_integer(amount_in, label="amount_in")
    fee_denominator = 10_000
    fee_numerator = _v2_fee_numerator(fee_bps)
    amount_in_with_fee = amount_in_integer * fee_numerator
    output = (
        amount_in_with_fee
        * reserve_out_integer
        // (
            reserve_in_integer * fee_denominator
            + amount_in_with_fee
        )
    )
    return Decimal(output)


def v2_exact_output_quote(
    reserve_in: Decimal,
    reserve_out: Decimal,
    fee_bps: Decimal,
    amount_out: Decimal,
) -> Decimal | None:
    """Return integer gross input, or ``None`` when exact output is impossible."""
    reserve_in_integer = _v2_integer(reserve_in, label="reserve_in")
    reserve_out_integer = _v2_integer(reserve_out, label="reserve_out")
    amount_out_integer = _v2_integer(amount_out, label="amount_out")
    if amount_out_integer >= reserve_out_integer:
        return None
    fee_denominator = 10_000
    fee_numerator = _v2_fee_numerator(fee_bps)
    numerator = (
        reserve_in_integer
        * amount_out_integer
        * fee_denominator
    )
    denominator = (
        reserve_out_integer - amount_out_integer
    ) * fee_numerator
    # V2 getAmountIn-style implementations add one raw input unit after the
    # exact integer division; Decimal context precision never participates.
    return Decimal(numerator // denominator + 1)


def tick_sqrt_ratio_x96(tick: int) -> Decimal:
    if not -887272 <= tick <= 887272:
        raise ValueError("tick outside Uniswap V3 bounds")
    with localcontext() as context:
        context.prec = 100
        return (
            Decimal("1.0001") ** (Decimal(tick) / Decimal(2)) * Q96
        ).to_integral_value(rounding=ROUND_FLOOR)


def v3_segment_amounts(
    sqrt_start: Decimal,
    sqrt_end: Decimal,
    liquidity: Decimal,
    fee_pips: int,
    *,
    zero_for_one: bool,
) -> tuple[Decimal, Decimal]:
    if liquidity <= 0:
        raise ValueError("V3 active liquidity is zero")
    fee_fraction = Decimal(fee_pips) / ONE_MILLION
    if not Decimal(0) <= fee_fraction < Decimal(1):
        raise ValueError("V3 fee is invalid")
    with localcontext() as context:
        context.prec = 100
        if zero_for_one:
            if sqrt_end > sqrt_start:
                raise ValueError("zero-for-one target price must be lower")
            net_input = (
                liquidity * Q96 * (sqrt_start - sqrt_end)
                / (sqrt_start * sqrt_end)
            )
            output = liquidity * (sqrt_start - sqrt_end) / Q96
        else:
            if sqrt_end < sqrt_start:
                raise ValueError("one-for-zero target price must be higher")
            net_input = liquidity * (sqrt_end - sqrt_start) / Q96
            output = (
                liquidity * Q96 * (sqrt_end - sqrt_start)
                / (sqrt_end * sqrt_start)
            )
        gross_input = net_input / (Decimal(1) - fee_fraction)
    return gross_input, output


def v3_move_to_price(
    sqrt_start: int,
    target_sqrt: Decimal,
    liquidity: int,
    fee_pips: int,
    initialized_ticks: dict[int, int],
    *,
    zero_for_one: bool,
) -> tuple[Decimal, Decimal, bool]:
    current_sqrt = Decimal(sqrt_start)
    current_liquidity = Decimal(liquidity)
    if zero_for_one:
        boundaries = sorted(
            (
                (tick, tick_sqrt_ratio_x96(tick))
                for tick in initialized_ticks
                if target_sqrt < tick_sqrt_ratio_x96(tick) <= current_sqrt
            ),
            reverse=True,
        )
    else:
        boundaries = sorted(
            (
                (tick, tick_sqrt_ratio_x96(tick))
                for tick in initialized_ticks
                if current_sqrt < tick_sqrt_ratio_x96(tick) < target_sqrt
            )
        )

    total_input = Decimal(0)
    total_output = Decimal(0)
    for tick, boundary_sqrt in boundaries:
        gross_input, output = v3_segment_amounts(
            current_sqrt,
            boundary_sqrt,
            current_liquidity,
            fee_pips,
            zero_for_one=zero_for_one,
        )
        total_input += gross_input
        total_output += output
        liquidity_net = Decimal(initialized_ticks[tick])
        current_liquidity += -liquidity_net if zero_for_one else liquidity_net
        current_sqrt = boundary_sqrt
        if current_liquidity <= 0:
            return total_input, total_output, False

    gross_input, output = v3_segment_amounts(
        current_sqrt,
        target_sqrt,
        current_liquidity,
        fee_pips,
        zero_for_one=zero_for_one,
    )
    return total_input + gross_input, total_output + output, True


def initialized_tick_range(
    current_tick: int,
    tick_spacing: int,
    max_band_bps: int = max(DEPTH_BANDS_BPS),
) -> tuple[int, int]:
    if tick_spacing <= 0:
        raise ValueError("tick spacing must be positive")
    down = abs(math.log1p(-max_band_bps / 10_000) / math.log(1.0001))
    up = abs(math.log1p(max_band_bps / 10_000) / math.log(1.0001))
    margin = 2 * tick_spacing
    return (
        math.floor(current_tick - down - margin),
        math.ceil(current_tick + up + margin),
    )


def collect_initialized_ticks(
    client: RpcClient,
    pool_address: str,
    block_tag: str,
    current_tick: int,
    tick_spacing: int,
) -> dict[int, int]:
    minimum_tick, maximum_tick = initialized_tick_range(current_tick, tick_spacing)
    minimum_word = (minimum_tick // tick_spacing) >> 8
    maximum_word = (maximum_tick // tick_spacing) >> 8
    word_positions = list(range(minimum_word, maximum_word + 1))
    bitmap_data = [
        call_with_int(SELECTOR_TICK_BITMAP, position, 16)
        for position in word_positions
    ]
    bitmap_results = client.eth_calls(pool_address, bitmap_data, block_tag)
    ticks: list[int] = []
    for word_position, result in zip(word_positions, bitmap_results):
        bitmap = decode_uint(result)
        for bit in range(256):
            if bitmap & (1 << bit):
                tick = (word_position * 256 + bit) * tick_spacing
                if minimum_tick <= tick <= maximum_tick:
                    ticks.append(tick)
    if not ticks:
        return {}
    tick_results = client.eth_calls(
        pool_address,
        [call_with_int(SELECTOR_TICKS, tick, 24) for tick in ticks],
        block_tag,
    )
    return {
        tick: decode_int(result, 1, 128)
        for tick, result in zip(ticks, tick_results)
        if decode_uint(result, 0) > 0
    }


def base_row(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
) -> dict[str, str]:
    row = {column: "" for column in DEX_DEPTH_COLUMNS}
    row.update(
        {
            "snapshot_id": snapshot_id,
            "observed_at": response_received_at,
            "request_started_at": request_started_at,
            "response_received_at": response_received_at,
            "token_symbol": pool["token_symbol"].upper(),
            "chain": pool["chain"].lower(),
            "dex": pool["dex"].lower(),
            "pool_address": pool["pool_address"],
            "pool_name": pool.get("pool_name", ""),
            "depth_method": DEX_DEPTH_METHOD,
            "source": "fixed-block EVM JSON-RPC eth_call",
            "usd_price_source_snapshot_id": pool.get("snapshot_id", ""),
            "usd_price_observed_at": (
                pool.get("response_received_at")
                or pool.get("observed_at")
                or ""
            ),
            "usd_price_source": pool.get("source", ""),
            "usd_price_source_endpoint": pool.get("source_endpoint", ""),
            "usd_price_raw_response_sha256": pool.get(
                "raw_response_sha256",
                "",
            ),
        }
    )
    return row


def unsupported_row(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    reason: str,
) -> dict[str, str]:
    row = base_row(
        pool,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )
    row.update(
        {
            "protocol_model": "unsupported",
            "status": "unsupported",
            "error": reason,
        }
    )
    return row


def token_metadata(
    client: RpcClient,
    token_addresses: tuple[str, str],
    block_tag: str,
) -> tuple[tuple[str, int], tuple[str, int]]:
    result = []
    for address in token_addresses:
        decimals_result, symbol_result = client.eth_calls(
            address,
            [SELECTOR_DECIMALS, SELECTOR_SYMBOL],
            block_tag,
        )
        decimals = decode_uint(decimals_result)
        symbol = decode_symbol(symbol_result)
        if not 0 <= decimals <= 255:
            raise ValueError(f"invalid token decimals for {address}")
        if not symbol:
            raise ValueError(f"missing token symbol for {address}")
        result.append((symbol, decimals))
    return result[0], result[1]


def depth_fields(
    *,
    target_position_index: int,
    token0_decimals: int,
    token1_decimals: int,
    token0_price: Decimal,
    token1_price: Decimal,
    band_amounts: dict[int, dict[str, Any]],
) -> dict[str, str]:
    fields: dict[str, str] = {}
    scale0 = Decimal(10) ** token0_decimals
    scale1 = Decimal(10) ** token1_decimals
    for band, amounts in band_amounts.items():
        zero_input = amounts["zero_input"] / scale0
        zero_output = amounts["zero_output"] / scale1
        one_input = amounts["one_input"] / scale1
        one_output = amounts["one_output"] / scale0
        if target_position_index == 0:
            sell_usd = zero_output * token1_price
            buy_usd = one_input * token1_price
        else:
            sell_usd = one_output * token0_price
            buy_usd = zero_input * token0_price
        complete = bool(amounts["zero_complete"] and amounts["one_complete"])
        fields[f"sell_depth_{band}bps_usd"] = decimal_text(sell_usd)
        fields[f"buy_depth_{band}bps_usd"] = decimal_text(buy_usd)
        fields[f"total_depth_{band}bps_usd"] = decimal_text(sell_usd + buy_usd)
        fields[f"depth_{band}bps_complete"] = bool_text(complete)
    return fields


def pool_state_price_usd(
    *,
    target_position_index: int,
    raw_token1_per_token0: Decimal,
    token0_price: Decimal,
    token1_price: Decimal,
) -> Decimal:
    if raw_token1_per_token0 <= 0:
        raise ValueError("pool state price must be positive")
    with localcontext() as context:
        context.prec = 200
        return (
            raw_token1_per_token0 * token1_price
            if target_position_index == 0
            else token0_price / raw_token1_per_token0
        )


def dex_market_id(pool: dict[str, str]) -> str:
    chain, address = pool_key(pool["chain"], pool["pool_address"])
    return (
        f"dex:{chain}:{pool['dex'].strip().lower()}:"
        f"{address}:{pool['token_symbol'].strip().upper()}"
    )


def block_timestamp_text(block: dict[str, Any]) -> str:
    from datetime import datetime, timezone

    raw = block.get("timestamp")
    if raw is None:
        raise ValueError("fixed block is missing timestamp")
    timestamp = int(raw, 16) if isinstance(raw, str) else int(raw)
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def _execution_common(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    protocol: str,
    block_number: int | None = None,
    block_timestamp: str = "",
    source_endpoint: str = "",
    raw_response_sha256: str = "",
    target_token_address: str = "",
    target_token_decimals: int | None = None,
    quote_token_address: str = "",
    quote_token_decimals: int | None = None,
    fee_bps: Decimal | None = None,
) -> dict[str, Any]:
    common: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "source_snapshot_id": snapshot_id,
        "calculation_method": DEX_EXECUTION_METHOD,
        "observed_at": response_received_at,
        "state_observed_at": block_timestamp,
        "request_started_at": request_started_at,
        "response_received_at": response_received_at,
        "market_id": dex_market_id(pool),
        "market_type": "dex",
        "token_symbol": pool["token_symbol"].upper(),
        "chain": pool["chain"].lower(),
        "dex": pool["dex"].lower(),
        "pool_address": pool["pool_address"],
        "block_number": str(block_number) if block_number is not None else "",
        "block_timestamp": block_timestamp,
        "protocol_model": protocol,
        "target_token_address": target_token_address,
        "target_token_decimals": (
            str(target_token_decimals)
            if target_token_decimals is not None
            else ""
        ),
        "quote_token_address": quote_token_address,
        "quote_token_decimals": (
            str(quote_token_decimals)
            if quote_token_decimals is not None
            else ""
        ),
        "reference_price_method": DEX_EXECUTION_REFERENCE,
        "usd_price_source_snapshot_id": pool.get("snapshot_id", ""),
        "usd_price_observed_at": (
            pool.get("response_received_at")
            or pool.get("observed_at")
            or ""
        ),
        "fee_status": "included_protocol_fee" if fee_bps is not None else "",
        "fee_rate_bps": decimal_text(fee_bps),
        "usd_conversion_status": (
            "observed_inventory_token_price"
            if target_token_address and quote_token_address
            else ""
        ),
        "excluded_costs": DEX_EXECUTION_EXCLUDED_COSTS,
        "source": "fixed-block EVM JSON-RPC pool state",
        "source_endpoint": source_endpoint,
        "source_sequence": str(block_number) if block_number is not None else "",
        "raw_response_sha256": raw_response_sha256,
    }
    return common


def terminal_execution_rows(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    protocol: str,
    status: str,
    status_reason: str,
    error: str,
    block_number: int | None = None,
    block_timestamp: str = "",
    source_endpoint: str = "",
    raw_response_sha256: str = "",
) -> list[dict[str, str]]:
    common = _execution_common(
        pool,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
        protocol=protocol,
        block_number=block_number,
        block_timestamp=block_timestamp,
        source_endpoint=source_endpoint,
        raw_response_sha256=raw_response_sha256,
    )
    return [
        execution_fact_row(
            common=common,
            direction=direction,
            requested_notional_usd=notional,
            status=status,
            status_reason=status_reason,
            error=error,
        )
        for notional in EXECUTION_NOTIONALS_USD
        for direction in EXECUTION_DIRECTIONS
    ]


def _human_token1_per_token0(
    reserve0_raw: Decimal,
    reserve1_raw: Decimal,
    token0_decimals: int,
    token1_decimals: int,
) -> Decimal:
    if reserve0_raw <= 0 or reserve1_raw <= 0:
        raise ValueError("pool ending reserves must be positive")
    with localcontext() as context:
        context.prec = 200
        return (
            reserve1_raw
            / (Decimal(10) ** token1_decimals)
            / (reserve0_raw / (Decimal(10) ** token0_decimals))
        )


def _target_quote_ratio(
    token1_per_token0: Decimal,
    target_position_index: int,
) -> Decimal:
    if token1_per_token0 <= 0:
        raise ValueError("pool target/quote ratio must be positive")
    with localcontext() as context:
        context.prec = 200
        return (
            token1_per_token0
            if target_position_index == 0
            else Decimal(1) / token1_per_token0
        )


def _quantized_target(
    requested_notional_usd: Decimal,
    reference_price_quote_per_token: Decimal,
    quote_to_usd: Decimal,
    target_decimals: int,
) -> tuple[Decimal, Decimal]:
    scale = Decimal(10) ** target_decimals
    with localcontext() as context:
        context.prec = 200
        raw = (
            requested_notional_usd
            / (reference_price_quote_per_token * quote_to_usd)
            * scale
        ).to_integral_value(rounding=ROUND_FLOOR)
    if raw <= 0:
        raise ValueError("execution target is below one Token base unit")
    return raw, raw / scale


def v2_execution_rows(
    pool: dict[str, str],
    *,
    common: dict[str, Any],
    target_position_index: int,
    token0_decimals: int,
    token1_decimals: int,
    token0_price: Decimal,
    token1_price: Decimal,
    reserve0: Decimal,
    reserve1: Decimal,
    fee_bps: Decimal,
) -> list[dict[str, str]]:
    """Calculate V2 execution facts under a precision-safe Decimal context."""
    with localcontext() as context:
        context.prec = 200
        return _v2_execution_rows(
            pool,
            common=common,
            target_position_index=target_position_index,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            token0_price=token0_price,
            token1_price=token1_price,
            reserve0=reserve0,
            reserve1=reserve1,
            fee_bps=fee_bps,
        )


def _v2_execution_rows(
    pool: dict[str, str],
    *,
    common: dict[str, Any],
    target_position_index: int,
    token0_decimals: int,
    token1_decimals: int,
    token0_price: Decimal,
    token1_price: Decimal,
    reserve0: Decimal,
    reserve1: Decimal,
    fee_bps: Decimal,
) -> list[dict[str, str]]:
    scale0 = Decimal(10) ** token0_decimals
    scale1 = Decimal(10) ** token1_decimals
    starting_ratio = _human_token1_per_token0(
        reserve0,
        reserve1,
        token0_decimals,
        token1_decimals,
    )
    reference_quote = _target_quote_ratio(starting_ratio, target_position_index)
    quote_to_usd = token1_price if target_position_index == 0 else token0_price
    target_scale = scale0 if target_position_index == 0 else scale1
    quote_scale = scale1 if target_position_index == 0 else scale0
    rows: list[dict[str, str]] = []

    for notional in EXECUTION_NOTIONALS_USD:
        target_raw, target_quantity = _quantized_target(
            notional,
            reference_quote,
            quote_to_usd,
            token0_decimals if target_position_index == 0 else token1_decimals,
        )
        if target_position_index == 0:
            sell_quote_raw = v2_exact_input_quote(
                reserve0, reserve1, fee_bps, target_raw
            )
            if sell_quote_raw <= 0:
                raise ValueError(
                    "V2 exact-input execution produced zero quote output"
                )
            sell_ending_ratio = _human_token1_per_token0(
                reserve0 + target_raw,
                reserve1 - sell_quote_raw,
                token0_decimals,
                token1_decimals,
            )
            buy_quote_raw = v2_exact_output_quote(
                reserve1, reserve0, fee_bps, target_raw
            )
            buy_ending_ratio = (
                _human_token1_per_token0(
                    reserve0 - target_raw,
                    reserve1 + buy_quote_raw,
                    token0_decimals,
                    token1_decimals,
                )
                if buy_quote_raw is not None
                else None
            )
        else:
            sell_quote_raw = v2_exact_input_quote(
                reserve1, reserve0, fee_bps, target_raw
            )
            if sell_quote_raw <= 0:
                raise ValueError(
                    "V2 exact-input execution produced zero quote output"
                )
            sell_ending_ratio = _human_token1_per_token0(
                reserve0 - sell_quote_raw,
                reserve1 + target_raw,
                token0_decimals,
                token1_decimals,
            )
            buy_quote_raw = v2_exact_output_quote(
                reserve0, reserve1, fee_bps, target_raw
            )
            buy_ending_ratio = (
                _human_token1_per_token0(
                    reserve0 + buy_quote_raw,
                    reserve1 - target_raw,
                    token0_decimals,
                    token1_decimals,
                )
                if buy_quote_raw is not None
                else None
            )

        rows.append(
            execution_fact_row(
                common=common,
                direction="sell_token",
                requested_notional_usd=notional,
                status="observed",
                status_reason="full_target_quantity_filled",
                reference_price_quote_per_token=reference_quote,
                quote_to_usd=quote_to_usd,
                target_token_quantity=target_quantity,
                filled_token_quantity=target_quantity,
                quote_amount=sell_quote_raw / quote_scale,
                levels_or_ticks_consumed=1,
                ending_marginal_price_quote_per_token=_target_quote_ratio(
                    sell_ending_ratio,
                    target_position_index,
                ),
            )
        )
        if buy_quote_raw is None:
            rows.append(
                execution_fact_row(
                    common=common,
                    direction="buy_token",
                    requested_notional_usd=notional,
                    status="partial",
                    status_reason="full_pool_reserve_insufficient",
                    reference_price_quote_per_token=reference_quote,
                    quote_to_usd=quote_to_usd,
                    target_token_quantity=target_quantity,
                )
            )
        else:
            assert buy_ending_ratio is not None
            rows.append(
                execution_fact_row(
                    common=common,
                    direction="buy_token",
                    requested_notional_usd=notional,
                    status="observed",
                    status_reason="full_target_quantity_filled",
                    reference_price_quote_per_token=reference_quote,
                    quote_to_usd=quote_to_usd,
                    target_token_quantity=target_quantity,
                    filled_token_quantity=target_quantity,
                    quote_amount=buy_quote_raw / quote_scale,
                    levels_or_ticks_consumed=1,
                    ending_marginal_price_quote_per_token=_target_quote_ratio(
                        buy_ending_ratio,
                        target_position_index,
                    ),
                )
            )
    return rows


def _sqrt_human_token1_per_token0(
    sqrt_price_x96: Decimal,
    token0_decimals: int,
    token1_decimals: int,
) -> Decimal:
    with localcontext() as context:
        context.prec = 100
        return (
            (sqrt_price_x96 / Q96) ** 2
            * (Decimal(10) ** (token0_decimals - token1_decimals))
        )


def observed_pool_row(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    block_number: int,
    block_timestamp: str,
    client: RpcClient,
    request_started_at: str,
    raw_response_sha256: str,
    protocol: str,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    block_tag = hex(block_number)
    pool_address = pool["pool_address"].lower()
    price_timing = require_usable_pool_usd_price(
        pool,
        block_timestamp,
    )
    price_map = price_map_from_inventory(pool)
    if len(price_map) < 2:
        raise ValueError("TVL inventory is missing one or both token USD prices")

    if protocol == "constant_product_v2":
        token0_result, token1_result, reserves_result = client.eth_calls(
            pool_address,
            [SELECTOR_TOKEN0, SELECTOR_TOKEN1, SELECTOR_GET_RESERVES],
            block_tag,
        )
        token0 = decode_address(token0_result)
        token1 = decode_address(token1_result)
        reserve0 = Decimal(decode_uint(reserves_result, 0))
        reserve1 = Decimal(decode_uint(reserves_result, 1))
        fee_bps = V2_FEE_BPS[pool["dex"].lower()]
        sqrt_price_x96 = None
        current_tick = None
        active_liquidity = None
        initialized_ticks: dict[int, int] = {}
    else:
        (
            token0_result,
            token1_result,
            slot0_result,
            liquidity_result,
            fee_result,
            spacing_result,
        ) = client.eth_calls(
            pool_address,
            [
                SELECTOR_TOKEN0,
                SELECTOR_TOKEN1,
                SELECTOR_SLOT0,
                SELECTOR_LIQUIDITY,
                SELECTOR_FEE,
                SELECTOR_TICK_SPACING,
            ],
            block_tag,
        )
        token0 = decode_address(token0_result)
        token1 = decode_address(token1_result)
        sqrt_price_x96 = decode_uint(slot0_result, 0)
        current_tick = decode_int(slot0_result, 1, 24)
        active_liquidity = decode_uint(liquidity_result)
        fee_pips = decode_uint(fee_result)
        fee_bps = Decimal(fee_pips) / Decimal(100)
        tick_spacing = decode_int(spacing_result, 0, 24)
        if sqrt_price_x96 <= 0 or active_liquidity <= 0:
            raise ValueError("V3 pool is uninitialized or has zero active liquidity")
        initialized_ticks = collect_initialized_ticks(
            client,
            pool_address,
            block_tag,
            current_tick,
            tick_spacing,
        )
        reserve0 = reserve1 = Decimal(0)

    (token0_symbol, token0_decimals), (token1_symbol, token1_decimals) = token_metadata(
        client,
        (token0, token1),
        block_tag,
    )
    target_index = target_position(
        pool["token_symbol"],
        token0_symbol,
        token1_symbol,
    )
    if token0 not in price_map or token1 not in price_map:
        raise ValueError(
            f"pool token addresses do not match TVL inventory:{token0}:{token1}"
        )
    token0_price = price_map[token0]
    token1_price = price_map[token1]

    band_amounts: dict[int, dict[str, Any]] = {}
    if protocol == "constant_product_v2":
        for band in DEPTH_BANDS_BPS:
            amounts = v2_band_amounts(reserve0, reserve1, fee_bps, band)
            band_amounts[band] = {
                "zero_input": amounts["zero_for_one_gross_input"],
                "zero_output": amounts["zero_for_one_output"],
                "one_input": amounts["one_for_zero_gross_input"],
                "one_output": amounts["one_for_zero_output"],
                "zero_complete": True,
                "one_complete": True,
            }
        raw_ratio = _human_token1_per_token0(
            reserve0,
            reserve1,
            token0_decimals,
            token1_decimals,
        )
    else:
        assert sqrt_price_x96 is not None
        assert active_liquidity is not None
        assert current_tick is not None
        fee_pips = int(fee_bps * Decimal(100))
        for band in DEPTH_BANDS_BPS:
            with localcontext() as context:
                context.prec = 100
                down_target = (
                    Decimal(sqrt_price_x96)
                    * (
                        Decimal(1) - Decimal(band) / Decimal(10_000)
                    ).sqrt()
                )
                up_target = (
                    Decimal(sqrt_price_x96)
                    * (
                        Decimal(1) + Decimal(band) / Decimal(10_000)
                    ).sqrt()
                )
            zero_input, zero_output, zero_complete = v3_move_to_price(
                sqrt_price_x96,
                down_target,
                active_liquidity,
                fee_pips,
                initialized_ticks,
                zero_for_one=True,
            )
            one_input, one_output, one_complete = v3_move_to_price(
                sqrt_price_x96,
                up_target,
                active_liquidity,
                fee_pips,
                initialized_ticks,
                zero_for_one=False,
            )
            band_amounts[band] = {
                "zero_input": zero_input,
                "zero_output": zero_output,
                "one_input": one_input,
                "one_output": one_output,
                "zero_complete": zero_complete,
                "one_complete": one_complete,
            }
        with localcontext() as context:
            context.prec = 100
            raw_ratio = (
                (Decimal(sqrt_price_x96) / Q96) ** 2
                * (Decimal(10) ** (token0_decimals - token1_decimals))
            )

    response_received_at = utc_now_text()
    row = base_row(
        pool,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )
    source_target_price = (
        token0_price if target_index == 0 else token1_price
    )
    state_price = pool_state_price_usd(
        target_position_index=target_index,
        raw_token1_per_token0=raw_ratio,
        token0_price=token0_price,
        token1_price=token1_price,
    )
    with localcontext() as context:
        context.prec = 200
        price_difference_bps = (
            abs(state_price - source_target_price)
            / ((state_price + source_target_price) / Decimal(2))
            * Decimal(10_000)
        )
    row.update(
        {
            "protocol_model": protocol,
            "block_number": str(block_number),
            "block_timestamp": block_timestamp,
            "target_token_address": token0 if target_index == 0 else token1,
            "target_token_position": f"token{target_index}",
            "token0_address": token0,
            "token0_symbol": token0_symbol,
            "token0_decimals": str(token0_decimals),
            "token0_price_usd": decimal_text(token0_price),
            "token1_address": token1,
            "token1_symbol": token1_symbol,
            "token1_decimals": str(token1_decimals),
            "token1_price_usd": decimal_text(token1_price),
            "fee_bps": decimal_text(fee_bps),
            "pool_state_price_usd": decimal_text(state_price),
            "source_target_price_usd": decimal_text(source_target_price),
            "price_difference_bps": decimal_text(price_difference_bps),
            "usd_price_skew_seconds": str(price_timing["skew_seconds"]),
            "usd_price_freshness_status": str(price_timing["status"]),
            "source_endpoint": client.endpoint,
            "raw_response_sha256": raw_response_sha256,
        }
    )
    row.update(
        depth_fields(
            target_position_index=target_index,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            token0_price=token0_price,
            token1_price=token1_price,
            band_amounts=band_amounts,
        )
    )
    row["status"] = (
        "observed"
        if all(row[f"depth_{band}bps_complete"] == "1" for band in DEPTH_BANDS_BPS)
        else "partial"
    )
    response_received_at = row["response_received_at"]
    if protocol == "concentrated_liquidity_v3":
        # The depth bands above are valid pool-state facts, but producing an
        # executable V3 quote requires protocol-identical integer swap math at
        # every step and tick crossing.  The removed continuous Decimal
        # approximation was not strong enough to publish as an exact fact.
        return row, terminal_execution_rows(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            protocol=protocol,
            status="unsupported",
            status_reason="exact_integer_swap_math_not_implemented",
            error="V3 exact integer swap math is not implemented",
            block_number=block_number,
            block_timestamp=block_timestamp,
            source_endpoint=client.endpoint,
            raw_response_sha256=raw_response_sha256,
        )

    target_token = token0 if target_index == 0 else token1
    quote_token = token1 if target_index == 0 else token0
    target_decimals = token0_decimals if target_index == 0 else token1_decimals
    quote_decimals = token1_decimals if target_index == 0 else token0_decimals
    try:
        common = _execution_common(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            protocol=protocol,
            block_number=block_number,
            block_timestamp=block_timestamp,
            source_endpoint=client.endpoint,
            raw_response_sha256=raw_response_sha256,
            target_token_address=target_token,
            target_token_decimals=target_decimals,
            quote_token_address=quote_token,
            quote_token_decimals=quote_decimals,
            fee_bps=fee_bps,
        )
        execution_rows = v2_execution_rows(
            pool,
            common=common,
            target_position_index=target_index,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            token0_price=token0_price,
            token1_price=token1_price,
            reserve0=reserve0,
            reserve1=reserve1,
            fee_bps=fee_bps,
        )
    except Exception as execution_error:
        # Depth is already a valid independent fact at this point.  A defect or
        # unsupported edge in the derived execution calculation must not erase
        # that successfully observed pool-state snapshot.
        execution_rows = terminal_execution_rows(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            protocol=protocol,
            status="failed",
            status_reason="execution_calculation_failed",
            error=f"{type(execution_error).__name__}: {execution_error}",
            block_number=block_number,
            block_timestamp=block_timestamp,
            source_endpoint=client.endpoint,
            raw_response_sha256=raw_response_sha256,
        )
    return row, execution_rows


def raw_transcript_bytes(
    *,
    pool: dict[str, str],
    block_number: int | None,
    endpoint: str,
    records: list[dict[str, Any]],
    error: Exception | None = None,
) -> bytes:
    payload: dict[str, Any] = {
        "pool": {
            "token_symbol": pool["token_symbol"],
            "chain": pool["chain"],
            "dex": pool["dex"],
            "pool_address": pool["pool_address"],
        },
        "block_number": block_number,
        "source_endpoint": endpoint,
        "records": records,
    }
    if error is not None:
        payload["error_type"] = type(error).__name__
        payload["error"] = str(error)
    return (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def collect_dex_pool_observation(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    raw_path: Path,
    rpc_factory: Callable[[str, str], RpcClient] = RpcClient,
    client: RpcClient | None = None,
    fixed_block_number: int | None = None,
    fixed_block_timestamp: str = "",
    deadline: CollectionDeadline | None = None,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Collect one DEX pool with isolated client state unless one is supplied."""
    if deadline is not None:
        deadline.require_remaining()
    request_started_at = utc_now_text()
    protocol, unsupported_reason = protocol_model(
        pool["dex"],
        pool["chain"],
        pool["pool_address"],
    )
    if protocol == "unsupported":
        response_received_at = utc_now_text()
        return unsupported_row(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            reason=unsupported_reason,
        ), terminal_execution_rows(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            protocol="unsupported",
            status="unsupported",
            status_reason="unsupported_protocol_or_chain",
            error=unsupported_reason,
        )

    chain = pool["chain"].lower()
    rpc_url = rpc_url_for_chain(chain)
    if not rpc_url:
        response_received_at = utc_now_text()
        reason = f"missing_rpc_endpoint:{chain}"
        return unsupported_row(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            reason=reason,
        ), terminal_execution_rows(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            protocol="unsupported",
            status="unsupported",
            status_reason="unsupported_protocol_or_chain",
            error=reason,
        )

    if client is None:
        if deadline is None:
            client = rpc_factory(chain, rpc_url)
        else:
            client = rpc_factory(chain, rpc_url, deadline=deadline)
    active_client = (
        _DeadlineBoundRpcClient(client, deadline)
        if deadline is not None
        else client
    )
    record_start = len(active_client.records)
    block_number = fixed_block_number
    block_timestamp = fixed_block_timestamp
    try:
        if block_number is None:
            block_number = active_client.block_number()
        if not block_timestamp:
            block = active_client.block(hex(block_number))
            returned_number = block.get("number")
            if returned_number is not None:
                normalized_number = (
                    int(returned_number, 16)
                    if isinstance(returned_number, str)
                    else int(returned_number)
                )
                if normalized_number != block_number:
                    raise ValueError("fixed block response number does not match")
            block_timestamp = block_timestamp_text(block)
        row, pool_execution_rows = observed_pool_row(
            pool,
            snapshot_id=snapshot_id,
            block_number=block_number,
            block_timestamp=block_timestamp,
            client=active_client,
            request_started_at=request_started_at,
            raw_response_sha256="",
            protocol=protocol,
        )
        transcript = raw_transcript_bytes(
            pool=pool,
            block_number=block_number,
            endpoint=active_client.endpoint,
            records=active_client.records[record_start:],
        )
        raw_path.write_bytes(transcript)
        raw_hash = hashlib.sha256(transcript).hexdigest()
        row["raw_response_sha256"] = raw_hash
        for execution_row in pool_execution_rows:
            execution_row["raw_response_sha256"] = raw_hash
    except CollectionDeadlineExceeded:
        raise
    except Exception as error:
        transcript = raw_transcript_bytes(
            pool=pool,
            block_number=block_number,
            endpoint=active_client.endpoint,
            records=active_client.records[record_start:],
            error=error,
        )
        raw_path.write_bytes(transcript)
        raw_hash = hashlib.sha256(transcript).hexdigest()
        response_received_at = utc_now_text()
        row = base_row(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
        )
        price_timing = pool_usd_price_timing(pool, block_timestamp)
        row.update(
            {
                "protocol_model": protocol,
                "block_number": str(block_number) if block_number is not None else "",
                "block_timestamp": block_timestamp,
                "usd_price_skew_seconds": (
                    str(price_timing["skew_seconds"])
                    if price_timing["skew_seconds"] is not None
                    else ""
                ),
                "usd_price_freshness_status": price_timing["status"],
                "source_endpoint": active_client.endpoint,
                "raw_response_sha256": raw_hash,
                "status": "failed",
                "error": f"{type(error).__name__}: {error}",
            }
        )
        status_reason = (
            "usd_price_conversion_stale_or_unavailable"
            if str(error).startswith("usd_price_conversion_unavailable:")
            else "pool_state_collection_failed"
        )
        pool_execution_rows = terminal_execution_rows(
            pool,
            snapshot_id=snapshot_id,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            protocol=protocol,
            status="failed",
            status_reason=status_reason,
            error=f"{type(error).__name__}: {error}",
            block_number=block_number,
            block_timestamp=block_timestamp,
            source_endpoint=active_client.endpoint,
            raw_response_sha256=raw_hash,
        )
    return row, pool_execution_rows


def collect_dex_depth_with_execution(
    pools: list[dict[str, str]],
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    sleep_seconds: float = REQUEST_SLEEP_SECONDS,
    rpc_factory: Callable[[str, str], RpcClient] = RpcClient,
    allow_terminal_only: bool = False,
) -> tuple[str, list[dict[str, str]], list[dict[str, str]]]:
    from datetime import datetime, timezone

    snapshot_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    snapshot_raw_dir = raw_root / snapshot_id
    snapshot_raw_dir.mkdir(parents=True, exist_ok=False)
    clients: dict[str, RpcClient] = {}
    blocks: dict[str, int] = {}
    block_timestamps: dict[str, str] = {}
    rows: list[dict[str, str]] = []
    execution_rows: list[dict[str, str]] = []

    for index, pool in enumerate(pools, start=1):
        protocol, unsupported_reason = protocol_model(
            pool["dex"],
            pool["chain"],
            pool["pool_address"],
        )
        chain = pool["chain"].lower()
        rpc_url = rpc_url_for_chain(chain)
        raw_path = (
            snapshot_raw_dir
            / f"{index:03d}-{chain}-{pool['token_symbol']}-{pool['dex']}.json"
        )
        if protocol == "unsupported" or not rpc_url:
            row, pool_execution_rows = collect_dex_pool_observation(
                pool,
                snapshot_id=snapshot_id,
                raw_path=raw_path,
                rpc_factory=rpc_factory,
            )
            rows.append(row)
            execution_rows.extend(pool_execution_rows)
            if protocol == "unsupported":
                print(
                    f"[{index}/{len(pools)}] {pool['token_symbol']} "
                    f"{pool['chain']} {pool['dex']}: unsupported",
                    flush=True,
                )
            continue
        client = clients.setdefault(chain, rpc_factory(chain, rpc_url))
        block_number = blocks.get(chain)
        block_timestamp = block_timestamps.get(chain, "")
        row, pool_execution_rows = collect_dex_pool_observation(
            pool,
            snapshot_id=snapshot_id,
            raw_path=raw_path,
            rpc_factory=rpc_factory,
            client=client,
            fixed_block_number=block_number,
            fixed_block_timestamp=block_timestamp,
        )
        if row["block_number"] and row["block_timestamp"]:
            blocks[chain] = int(row["block_number"])
            block_timestamps[chain] = row["block_timestamp"]
        rows.append(row)
        execution_rows.extend(pool_execution_rows)
        print(
            f"[{index}/{len(pools)}] {pool['token_symbol']} "
            f"{pool['chain']} {pool['dex']}: {row['status']}",
            flush=True,
        )
        if index < len(pools) and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    manifest = {
        "snapshot_id": snapshot_id,
        "generated_at": utc_now_text(),
        "pool_count": len(rows),
        "token_count": len({row["token_symbol"] for row in rows}),
        "chain_blocks": blocks,
        "chain_block_timestamps": block_timestamps,
        "depth_bands_bps": list(DEPTH_BANDS_BPS),
        "execution_notionals_usd": [
            int(value) for value in EXECUTION_NOTIONALS_USD
        ],
        "execution_row_count": len(execution_rows),
        "execution_status_counts": execution_status_counts(execution_rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
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
    validate_execution_snapshot(
        [dex_market_id(pool) for pool in pools],
        execution_rows,
        enforce_usd_price_timing=True,
    )
    return snapshot_id, rows, execution_rows


def collect_dex_depth(
    pools: list[dict[str, str]],
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    sleep_seconds: float = REQUEST_SLEEP_SECONDS,
    rpc_factory: Callable[[str, str], RpcClient] = RpcClient,
    allow_terminal_only: bool = False,
) -> tuple[str, list[dict[str, str]]]:
    """Backward-compatible depth-only return shape."""
    snapshot_id, rows, _execution_rows = collect_dex_depth_with_execution(
        pools,
        raw_root=raw_root,
        sleep_seconds=sleep_seconds,
        rpc_factory=rpc_factory,
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
    expected = {
        (row["token_symbol"].upper(), *pool_key(row["chain"], row["pool_address"]))
        for row in inventory
    }
    actual = {
        (row["token_symbol"].upper(), *pool_key(row["chain"], row["pool_address"]))
        for row in rows
    }
    if len(rows) != len(actual):
        raise ValueError("DEX depth snapshot contains duplicate Token/pool rows")
    if expected != actual:
        raise ValueError("DEX depth snapshot coverage does not match the TVL inventory")
    validate_observation_bounds(rows)
    accepted = {"observed", "partial", "unsupported", "failed"}
    if any(row["status"] not in accepted for row in rows):
        raise ValueError("DEX depth snapshot contains an invalid status")
    if allow_terminal_only and any(
        quality_outcome_resolution_state(
            *normalize_dex_depth_source_outcome(
                row.get("status"),
                error=row.get("error"),
            )
        ) not in {"observed", "confirmed_terminal_absence"}
        for row in rows
    ):
        raise ValueError(
            "DEX refresh is not a terminal non-retryable or resolved exact candidate"
        )
    if (
        not allow_no_observed
        and not any(row["status"] in {"observed", "partial"} for row in rows)
    ):
        if not allow_terminal_only:
            raise ValueError("DEX depth snapshot contains no measured pools")
        if any(
            quality_outcome_resolution_state(
                *normalize_dex_depth_source_outcome(
                    row.get("status"),
                    error=row.get("error"),
                )
            ) != "confirmed_terminal_absence"
            for row in rows
        ):
            raise ValueError(
                "DEX exact candidate is not a terminal non-retryable outcome"
            )
    for row in rows:
        if row["status"] not in {"observed", "partial"}:
            continue
        previous = Decimal("-1")
        for band in DEPTH_BANDS_BPS:
            total = finite_decimal(row[f"total_depth_{band}bps_usd"])
            sell = finite_decimal(row[f"sell_depth_{band}bps_usd"])
            buy = finite_decimal(row[f"buy_depth_{band}bps_usd"])
            if total < 0 or sell < 0 or buy < 0:
                raise ValueError("DEX depth snapshot contains negative depth")
            if total + Decimal("1e-12") < previous:
                raise ValueError("DEX depth must be monotonic across wider bands")
            previous = total


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def merge_exact_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    target_market_id: str,
    publish_dir: Path,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Merge one collected DEX pool into the validated full publication."""
    baseline_depth = read_csv_rows(publish_dir / LATEST_FILENAME)
    baseline_execution = read_csv_rows(
        publish_dir / EXECUTION_LATEST_FILENAME
    )
    require_aligned_depth_execution_lineage(
        baseline_depth,
        baseline_execution,
    )
    require_aligned_depth_execution_lineage(depth_rows, execution_rows)
    merged_depth = merge_exact_market_snapshot(
        baseline_depth,
        depth_rows,
        target_market_id=target_market_id,
        market_id_for_row=dex_market_id,
        row_identity=dex_market_id,
    )
    merged_execution = merge_exact_market_snapshot(
        baseline_execution,
        execution_rows,
        target_market_id=target_market_id,
        market_id_for_row=lambda row: str(row.get("market_id") or ""),
        row_identity=lambda row: (
            str(row.get("market_id") or ""),
            str(row.get("direction") or ""),
            str(row.get("requested_notional_usd") or ""),
        ),
        rebind_source_snapshot_id=True,
    )
    if {row["snapshot_id"] for row in merged_depth} != {
        row["snapshot_id"] for row in merged_execution
    }:
        raise ValueError(
            "bounded DEX depth and execution publications are not coherent"
        )
    validate_snapshot(
        baseline_depth,
        merged_depth,
        allow_no_observed=True,
    )
    expected_market_ids = {dex_market_id(row) for row in baseline_depth}
    validate_execution_snapshot(
        expected_market_ids,
        merged_execution,
        enforce_usd_price_timing=True,
    )
    return merged_depth, merged_execution


def normalized_depth_gate_rows(
    rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Count only structurally unsupported pools outside the supported floor."""
    normalized = []
    for source_row in rows:
        row = dict(source_row)
        if row.get("status") == "unsupported":
            model, _reason = protocol_model(
                row.get("dex", ""),
                row.get("chain", ""),
                row.get("pool_address", ""),
            )
            if model != "unsupported":
                row["status"] = "failed"
        normalized.append(row)
    return normalized


def normalized_execution_gate_rows(
    rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Treat unsupported V2 execution as failure; V3 remains unsupported."""
    normalized = []
    for source_row in rows:
        row = dict(source_row)
        if row.get("status") == "unsupported":
            model, _reason = protocol_model(
                row.get("dex", ""),
                row.get("chain", ""),
                row.get("pool_address", ""),
            )
            if model == "constant_product_v2":
                row["status"] = "failed"
        normalized.append(row)
    return normalized


def depth_publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
) -> dict[str, Any]:
    latest_path = publish_dir / LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path) if latest_path.exists() else None
    report = enforce_publication_coverage(
        normalized_depth_gate_rows(rows),
        (
            normalized_depth_gate_rows(baseline_rows)
            if baseline_rows is not None
            else None
        ),
        fact_family="dex_depth",
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            *pool_key(
                row.get("chain", ""),
                row.get("pool_address", ""),
            ),
        ),
        cohort=lambda row: row.get("chain", "").strip().lower(),
        usable_statuses={"observed", "partial"},
        excluded_statuses={"unsupported"},
        valid_statuses={"observed", "partial", "unsupported", "failed"},
        minimum_candidate_usable_bps=MINIMUM_PUBLISHABLE_COVERAGE_BPS,
        minimum_baseline_retention_bps=MINIMUM_BASELINE_RETENTION_BPS,
    )
    return bind_passing_coverage_report(
        report,
        fact_family="dex_depth",
        baseline_path=latest_path,
    )


def execution_publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
) -> dict[str, Any]:
    latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path) if latest_path.exists() else None
    report = enforce_publication_coverage(
        normalized_execution_gate_rows(rows),
        (
            normalized_execution_gate_rows(baseline_rows)
            if baseline_rows is not None
            else None
        ),
        fact_family="dex_execution_cost",
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
        cohort=lambda row: row.get("chain", "").strip().lower(),
        usable_statuses={"observed", "partial"},
        excluded_statuses={"unsupported"},
        valid_statuses={"observed", "partial", "unsupported", "failed"},
        allow_no_eligible_candidate=True,
        minimum_candidate_usable_bps=MINIMUM_PUBLISHABLE_COVERAGE_BPS,
        minimum_baseline_retention_bps=MINIMUM_BASELINE_RETENTION_BPS,
    )
    return bind_passing_coverage_report(
        report,
        fact_family="dex_execution_cost",
        baseline_path=latest_path,
    )


def exact_depth_publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
    *,
    target_market_id: str,
) -> dict[str, Any]:
    latest_path = publish_dir / LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path)
    scope = validate_exact_publication_scope(
        baseline_rows,
        rows,
        target_market_id=target_market_id,
        market_id_for_row=dex_market_id,
        row_identity=dex_market_id,
    )
    report = enforce_publication_coverage(
        normalized_depth_gate_rows(rows),
        normalized_depth_gate_rows(baseline_rows),
        fact_family="dex_depth",
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            *pool_key(
                row.get("chain", ""),
                row.get("pool_address", ""),
            ),
        ),
        usable_statuses={"observed", "partial"},
        excluded_statuses={"unsupported"},
        valid_statuses={"observed", "partial", "unsupported", "failed"},
        allow_no_eligible_candidate=True,
        minimum_candidate_usable_bps=0,
        minimum_baseline_retention_bps=10_000,
    )
    target_rows = [
        row for row in rows if dex_market_id(row) == target_market_id
    ]
    normalized_target_rows = normalized_depth_gate_rows(target_rows)
    resolutions = {
        quality_outcome_resolution_state(
            *normalize_dex_depth_source_outcome(
                row.get("status"),
                error=row.get("error"),
            )
        )
        for row in normalized_target_rows
    }
    report["candidate"]["identity_row_sha256"] = publication_rows_sha256(
        rows,
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            *pool_key(
                row.get("chain", ""),
                row.get("pool_address", ""),
            ),
        ),
    )
    report.update(
        {
            "mode": "exact_target_recovery/v1",
            "exact_target": {**scope, "resolutions": sorted(resolutions)},
        }
    )
    if not target_rows or not resolutions.issubset(
        {"observed", "confirmed_terminal_absence"}
    ):
        report["status"] = "rejected"
        report["passed"] = False
        report["reasons"] = list(
            dict.fromkeys(
                list(report.get("reasons") or [])
                + ["exact_target_unresolved"]
            )
        )
        raise CoverageRegressionError(report)
    return bind_passing_coverage_report(
        report,
        fact_family="dex_depth",
        baseline_path=latest_path,
    )


def exact_execution_publication_coverage_gate(
    rows: list[dict[str, str]],
    publish_dir: Path,
    *,
    target_market_id: str,
) -> dict[str, Any]:
    latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    baseline_rows = read_csv_rows(latest_path)
    scope = validate_exact_publication_scope(
        baseline_rows,
        rows,
        target_market_id=target_market_id,
        market_id_for_row=lambda row: str(row.get("market_id") or ""),
        row_identity=lambda row: (
            str(row.get("market_id") or ""),
            str(row.get("direction") or ""),
            str(row.get("requested_notional_usd") or ""),
        ),
        rebound_fields=("snapshot_id", "source_snapshot_id"),
    )
    report = enforce_publication_coverage(
        normalized_execution_gate_rows(rows),
        normalized_execution_gate_rows(baseline_rows),
        fact_family="dex_execution_cost",
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
        usable_statuses={"observed", "partial"},
        excluded_statuses={"unsupported"},
        valid_statuses={"observed", "partial", "unsupported", "failed"},
        allow_no_eligible_candidate=True,
        minimum_candidate_usable_bps=0,
        minimum_baseline_retention_bps=10_000,
    )
    report["candidate"]["identity_row_sha256"] = publication_rows_sha256(
        rows,
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
    )
    report.update(
        {
            "mode": "exact_target_recovery/v1",
            "exact_target": scope,
        }
    )
    return bind_passing_coverage_report(
        report,
        fact_family="dex_execution_cost",
        baseline_path=latest_path,
    )


def preflight_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    publish_dir: Path,
    *,
    target_market_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Reject either coverage regression before writing either latest view."""
    require_aligned_depth_execution_lineage(depth_rows, execution_rows)
    if target_market_id is not None:
        target = str(target_market_id).strip()
        if not target:
            raise ValueError("exact target market identity is empty")
        return enforce_publication_coverage_bundle(
            (
                (
                    "dex_depth",
                    lambda: exact_depth_publication_coverage_gate(
                        depth_rows,
                        publish_dir,
                        target_market_id=target,
                    ),
                ),
                (
                    "dex_execution_cost",
                    lambda: exact_execution_publication_coverage_gate(
                        execution_rows,
                        publish_dir,
                        target_market_id=target,
                    ),
                ),
            ),
            bundle="dex_depth_execution_exact",
        )
    return enforce_publication_coverage_bundle(
        (
            (
                "dex_depth",
                lambda: depth_publication_coverage_gate(
                    depth_rows,
                    publish_dir,
                ),
            ),
            (
                "dex_execution_cost",
                lambda: execution_publication_coverage_gate(
                    execution_rows,
                    publish_dir,
                ),
            ),
        ),
        bundle="dex_depth_execution",
    )


def atomic_write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=DEX_DEPTH_COLUMNS,
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(
                {field: row.get(field, "") for field in DEX_DEPTH_COLUMNS}
                for row in rows
            )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_execution_csv(
    path: Path,
    rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=EXECUTION_COST_COLUMNS,
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(
                {field: row.get(field, "") for field in EXECUTION_COST_COLUMNS}
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
    preflight_report: dict[str, Any] | None = None,
    history_rows_to_append: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    current_path = output_dir / CURRENT_FILENAME
    atomic_write_csv(current_path, rows)
    result: dict[str, Any] = {"current_path": str(current_path), "row_count": len(rows)}
    if publish_dir is None:
        return result

    publish_dir.mkdir(parents=True, exist_ok=True)
    publication_gate = (
        validate_passing_coverage_report(
            preflight_report,
            fact_family="dex_depth",
            candidate_rows=normalized_depth_gate_rows(rows),
            identity=lambda row: (
                row.get("token_symbol", "").strip().upper(),
                *pool_key(
                    row.get("chain", ""),
                    row.get("pool_address", ""),
                ),
            ),
            baseline_path=publish_dir / LATEST_FILENAME,
            expected_policy=DEPTH_COVERAGE_POLICY,
        )
        if preflight_report is not None
        else depth_publication_coverage_gate(rows, publish_dir)
    )
    history_path = publish_dir / HISTORY_FILENAME
    merged = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            *pool_key(row.get("chain", ""), row.get("pool_address", "")),
        ): row
        for row in read_csv_rows(history_path)
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
    atomic_write_csv(history_path, history_rows)
    atomic_write_csv(publish_dir / LATEST_FILENAME, rows)
    atomic_write_csv(publish_dir / CURRENT_FILENAME, rows)
    result.update(
        {
            "latest_path": str(publish_dir / LATEST_FILENAME),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
            "publication_gate": publication_gate,
        }
    )
    return result


def publish_execution_snapshot(
    rows: list[dict[str, str]],
    *,
    expected_market_ids: Iterable[str],
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    publish_dir: Path | None = None,
    preflight_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Publication is the hard boundary: a caller cannot write a superficially
    # valid subset that omits one inventory market or one of its ten scenarios.
    validate_execution_snapshot(
        expected_market_ids,
        rows,
        enforce_usd_price_timing=True,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    current_path = output_dir / EXECUTION_CURRENT_FILENAME
    atomic_write_execution_csv(current_path, rows)
    result: dict[str, Any] = {
        "execution_current_path": str(current_path),
        "execution_row_count": len(rows),
    }
    if publish_dir is None:
        return result

    publish_dir.mkdir(parents=True, exist_ok=True)
    publication_gate = (
        validate_passing_coverage_report(
            preflight_report,
            fact_family="dex_execution_cost",
            candidate_rows=normalized_execution_gate_rows(rows),
            identity=lambda row: (
                row.get("market_id", "").strip(),
                row.get("direction", "").strip(),
                row.get("requested_notional_usd", "").strip(),
            ),
            baseline_path=publish_dir / EXECUTION_LATEST_FILENAME,
            expected_policy=EXECUTION_COVERAGE_POLICY,
        )
        if preflight_report is not None
        else execution_publication_coverage_gate(rows, publish_dir)
    )
    atomic_write_execution_csv(
        publish_dir / EXECUTION_LATEST_FILENAME,
        rows,
    )
    result.update(
        {
            "execution_latest_path": str(
                publish_dir / EXECUTION_LATEST_FILENAME
            ),
            "publication_gate": publication_gate,
        }
    )
    return result


def _require_disjoint_publication_destinations(
    private_destinations: Iterable[Path],
    public_destinations: Iterable[Path],
) -> None:
    resolved_private = {
        path.resolve(strict=False) for path in private_destinations
    }
    resolved_public = {
        path.resolve(strict=False) for path in public_destinations
    }
    if resolved_private & resolved_public:
        raise ValueError(
            "private and public publication destinations overlap"
        )


def publish_full_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    output_dir: Path,
    publish_dir: Path,
    preflight_reports: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Failure-atomically publish one full DEX depth/execution cohort."""
    current_path = output_dir / CURRENT_FILENAME
    execution_current_path = output_dir / EXECUTION_CURRENT_FILENAME
    history_path = publish_dir / HISTORY_FILENAME
    latest_path = publish_dir / LATEST_FILENAME
    public_current_path = publish_dir / CURRENT_FILENAME
    execution_latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    _require_disjoint_publication_destinations(
        (current_path, execution_current_path),
        (
            history_path,
            latest_path,
            public_current_path,
            execution_latest_path,
        ),
    )
    require_aligned_depth_execution_lineage(depth_rows, execution_rows)
    expected_market_ids = {dex_market_id(row) for row in depth_rows}
    validate_execution_snapshot(
        expected_market_ids,
        execution_rows,
        enforce_usd_price_timing=True,
    )
    depth_gate = validate_passing_coverage_report(
        preflight_reports.get("dex_depth"),
        fact_family="dex_depth",
        candidate_rows=normalized_depth_gate_rows(depth_rows),
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            *pool_key(
                row.get("chain", ""),
                row.get("pool_address", ""),
            ),
        ),
        baseline_path=latest_path,
        expected_policy=DEPTH_COVERAGE_POLICY,
    )
    execution_gate = validate_passing_coverage_report(
        preflight_reports.get("dex_execution_cost"),
        fact_family="dex_execution_cost",
        candidate_rows=normalized_execution_gate_rows(execution_rows),
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
        baseline_path=execution_latest_path,
        expected_policy=EXECUTION_COVERAGE_POLICY,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_csv(current_path, depth_rows)
    atomic_write_execution_csv(execution_current_path, execution_rows)

    merged_history = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            *pool_key(row.get("chain", ""), row.get("pool_address", "")),
        ): row
        for row in read_csv_rows(history_path)
    }
    for row in depth_rows:
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
            (history_path, csv_payload(DEX_DEPTH_COLUMNS, history_rows)),
            (
                latest_path,
                csv_payload(DEX_DEPTH_COLUMNS, depth_rows),
            ),
            (
                public_current_path,
                csv_payload(DEX_DEPTH_COLUMNS, depth_rows),
            ),
            (
                execution_latest_path,
                csv_payload(EXECUTION_COST_COLUMNS, execution_rows),
            ),
        )
    )
    return (
        {
            "current_path": str(current_path),
            "row_count": len(depth_rows),
            "latest_path": str(latest_path),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
            "publication_gate": depth_gate,
        },
        {
            "execution_current_path": str(execution_current_path),
            "execution_row_count": len(execution_rows),
            "execution_latest_path": str(execution_latest_path),
            "publication_gate": execution_gate,
        },
    )


def publish_exact_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    target_market_id: str,
    history_rows_to_append: list[dict[str, str]],
    output_dir: Path,
    publish_dir: Path,
    preflight_reports: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Failure-atomically publish one bounded DEX depth/execution merge."""
    current_path = output_dir / CURRENT_FILENAME
    execution_current_path = output_dir / EXECUTION_CURRENT_FILENAME
    history_path = publish_dir / HISTORY_FILENAME
    latest_path = publish_dir / LATEST_FILENAME
    public_current_path = publish_dir / CURRENT_FILENAME
    execution_latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    _require_disjoint_publication_destinations(
        (current_path, execution_current_path),
        (
            history_path,
            latest_path,
            public_current_path,
            execution_latest_path,
        ),
    )
    require_aligned_depth_execution_lineage(depth_rows, execution_rows)
    expected_market_ids = {
        str(row.get("market_id") or "") for row in execution_rows
    }
    validate_execution_snapshot(
        expected_market_ids,
        execution_rows,
        enforce_usd_price_timing=True,
    )
    depth_gate = validate_passing_coverage_report(
        preflight_reports.get("dex_depth"),
        fact_family="dex_depth",
        candidate_rows=depth_rows,
        identity=lambda row: (
            row.get("token_symbol", "").strip().upper(),
            *pool_key(
                row.get("chain", ""),
                row.get("pool_address", ""),
            ),
        ),
        baseline_path=latest_path,
        expected_policy=EXACT_DEPTH_COVERAGE_POLICY,
    )
    execution_gate = validate_passing_coverage_report(
        preflight_reports.get("dex_execution_cost"),
        fact_family="dex_execution_cost",
        candidate_rows=execution_rows,
        identity=lambda row: (
            row.get("market_id", "").strip(),
            row.get("direction", "").strip(),
            row.get("requested_notional_usd", "").strip(),
        ),
        baseline_path=execution_latest_path,
        expected_policy=EXACT_EXECUTION_COVERAGE_POLICY,
    )
    target = str(target_market_id or "").strip()
    for report in (depth_gate, execution_gate):
        exact_target = report.get("exact_target")
        if (
            report.get("mode") != "exact_target_recovery/v1"
            or not isinstance(exact_target, dict)
            or exact_target.get("market_id") != target
        ):
            raise ValueError("exact preflight report does not match target")
    if (
        depth_gate["exact_target"]["candidate_snapshot_id"]
        != execution_gate["exact_target"]["candidate_snapshot_id"]
    ):
        raise ValueError("exact preflight reports do not share one generation")
    published_target_rows = [
        row for row in depth_rows if dex_market_id(row) == target
    ]
    if (
        len(history_rows_to_append) != 1
        or len(published_target_rows) != 1
        or dex_market_id(history_rows_to_append[0]) != target
        or str(history_rows_to_append[0].get("snapshot_id") or "")
        != depth_gate["exact_target"]["candidate_snapshot_id"]
        or history_rows_to_append[0] != published_target_rows[0]
    ):
        raise ValueError("exact history append does not match target publication")

    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_csv(current_path, depth_rows)
    atomic_write_execution_csv(execution_current_path, execution_rows)

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
            (history_path, csv_payload(DEX_DEPTH_COLUMNS, history_rows)),
            (
                latest_path,
                csv_payload(DEX_DEPTH_COLUMNS, depth_rows),
            ),
            (
                public_current_path,
                csv_payload(DEX_DEPTH_COLUMNS, depth_rows),
            ),
            (
                execution_latest_path,
                csv_payload(EXECUTION_COST_COLUMNS, execution_rows),
            ),
        )
    )
    return (
        {
            "current_path": str(current_path),
            "row_count": len(depth_rows),
            "latest_path": str(latest_path),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
            "publication_gate": depth_gate,
        },
        {
            "execution_current_path": str(execution_current_path),
            "execution_row_count": len(execution_rows),
            "execution_latest_path": str(execution_latest_path),
            "publication_gate": execution_gate,
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect fixed-block DEX pool-state depth"
    )
    parser.add_argument("--tvl-csv", type=Path, default=DEFAULT_TVL_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--publish-local", action="store_true")
    parser.add_argument(
        "--publish-dir",
        type=Path,
        help="Explicit runtime directory for an atomic publication",
    )
    parser.add_argument("--sleep-seconds", type=float, default=REQUEST_SLEEP_SECONDS)
    parser.add_argument("--tokens", help="Comma-separated token symbols")
    parser.add_argument("--chains", help="Comma-separated chain names")
    parser.add_argument(
        "--market-id",
        help="One canonical DEX market identity for bounded collection",
    )
    parser.add_argument(
        "--merge-publish",
        action="store_true",
        help="Merge one exact pool into an existing full publication",
    )
    return parser.parse_args()


def parse_filter(value: str | None, *, upper: bool) -> set[str]:
    if not value:
        return set()
    transform = str.upper if upper else str.lower
    return {transform(item.strip()) for item in value.split(",") if item.strip()}


def ensure_full_publish_scope(
    publish_local: bool,
    *filters: set[str],
) -> None:
    if publish_local and any(filters):
        raise ValueError(
            "--publish-local cannot be combined with token or chain filters"
        )


def main() -> None:
    args = parse_args()
    pools = load_pool_inventory(args.tvl_csv)
    tokens = parse_filter(args.tokens, upper=True)
    chains = parse_filter(args.chains, upper=False)
    market_id = str(args.market_id or "").strip()
    publish_dir = (
        args.publish_dir
        if args.publish_dir is not None
        else DEFAULT_PUBLISH_DIR if args.publish_local else None
    )
    if market_id and (tokens or chains):
        raise ValueError("--market-id cannot be combined with other filters")
    if args.merge_publish and (publish_dir is None or not market_id):
        raise ValueError(
            "--merge-publish requires --publish-dir and --market-id"
        )
    if publish_dir is not None and market_id and not args.merge_publish:
        raise ValueError(
            "filtered publication requires explicit --merge-publish"
        )
    if not args.merge_publish:
        ensure_full_publish_scope(publish_dir is not None, tokens, chains)
    if tokens:
        pools = [row for row in pools if row["token_symbol"].upper() in tokens]
    if chains:
        pools = [row for row in pools if row["chain"].lower() in chains]
    if market_id:
        pools = [row for row in pools if dex_market_id(row) == market_id]
    if not pools:
        raise ValueError("No DEX pools match the requested filters")

    snapshot_id, rows, execution_rows = collect_dex_depth_with_execution(
        pools,
        raw_root=args.raw_root,
        sleep_seconds=max(0.0, args.sleep_seconds),
        allow_terminal_only=args.merge_publish,
    )
    collected_rows = rows
    collected_execution_rows = execution_rows
    if args.merge_publish:
        assert publish_dir is not None
        rows, execution_rows = merge_exact_publication_bundle(
            rows,
            execution_rows,
            target_market_id=market_id,
            publish_dir=publish_dir,
        )
    publication_gates = (
        preflight_publication_bundle(
            rows,
            execution_rows,
            publish_dir,
            target_market_id=market_id if args.merge_publish else None,
        )
        if publish_dir is not None
        else {}
    )
    if args.merge_publish:
        assert publish_dir is not None
        result, execution_result = publish_exact_publication_bundle(
            rows,
            execution_rows,
            target_market_id=market_id,
            history_rows_to_append=collected_rows,
            output_dir=args.output_dir,
            publish_dir=publish_dir,
            preflight_reports=publication_gates,
        )
    elif publish_dir is not None:
        result, execution_result = publish_full_publication_bundle(
            rows,
            execution_rows,
            output_dir=args.output_dir,
            publish_dir=publish_dir,
            preflight_reports=publication_gates,
        )
    else:
        result = publish_snapshot(
            rows,
            output_dir=args.output_dir,
        )
        execution_result = publish_execution_snapshot(
            execution_rows,
            expected_market_ids={
                str(row.get("market_id") or "")
                for row in execution_rows
            },
            output_dir=args.output_dir,
        )
    depth_gate = result.pop("publication_gate", None)
    execution_gate = execution_result.pop("publication_gate", None)
    result.update(execution_result)
    counts = Counter(row["status"] for row in rows)
    result.update(
        {
            "snapshot_id": snapshot_id,
            "token_count": len({row["token_symbol"] for row in rows}),
            "pool_count": len(rows),
            "collected_pool_count": len(collected_rows),
            "collected_execution_row_count": len(
                collected_execution_rows
            ),
            "status_counts": dict(counts),
            "execution_status_counts": execution_status_counts(
                execution_rows
            ),
        }
    )
    result_publication_gates = {
        name: gate
        for name, gate in (
            ("dex_depth", depth_gate),
            ("dex_execution_cost", execution_gate),
        )
        if gate is not None
    }
    if result_publication_gates:
        result["publication_gates"] = result_publication_gates
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
