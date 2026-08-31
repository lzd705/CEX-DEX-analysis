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
import inspect
import json
import math
import os
import re
import ssl
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from dataclasses import dataclass
from decimal import (
    Decimal,
    InvalidOperation,
    ROUND_FLOOR,
    localcontext,
)
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

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
        dex_depth_reason_code,
        normalize_dex_depth_source_outcome,
        quality_outcome_resolution_state,
    )
    from scripts.route_quantity import (
        CommonTarget,
        MarketRules,
        QuantityQuote,
        V2PoolState,
        quote_v2_pool_quantity,
        validate_v2_quantity_quote_against_state,
    )
    from scripts.timestamp_contract import validate_observation_bounds
    from scripts.uniswap_v3_math import (
        MAX_SQRT_RATIO as V3_MAX_SQRT_RATIO,
        MAX_TICK as V3_MAX_TICK,
        MIN_SQRT_RATIO as V3_MIN_SQRT_RATIO,
        MIN_TICK as V3_MIN_TICK,
        count_initialized_ticks_crossed as count_v3_initialized_ticks_crossed,
        get_sqrt_ratio_at_tick as exact_v3_sqrt_ratio_at_tick,
        get_tick_at_sqrt_ratio as exact_v3_tick_at_sqrt_ratio,
        simulate_swap as simulate_exact_v3_swap,
        sqrt_price_limit_for_bps as exact_v3_price_limit_for_bps,
    )
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
        dex_depth_reason_code,
        normalize_dex_depth_source_outcome,
        quality_outcome_resolution_state,
    )
    from route_quantity import (
        CommonTarget,
        MarketRules,
        QuantityQuote,
        V2PoolState,
        quote_v2_pool_quantity,
        validate_v2_quantity_quote_against_state,
    )
    from timestamp_contract import validate_observation_bounds
    from uniswap_v3_math import (
        MAX_SQRT_RATIO as V3_MAX_SQRT_RATIO,
        MAX_TICK as V3_MAX_TICK,
        MIN_SQRT_RATIO as V3_MIN_SQRT_RATIO,
        MIN_TICK as V3_MIN_TICK,
        count_initialized_ticks_crossed as count_v3_initialized_ticks_crossed,
        get_sqrt_ratio_at_tick as exact_v3_sqrt_ratio_at_tick,
        get_tick_at_sqrt_ratio as exact_v3_tick_at_sqrt_ratio,
        simulate_swap as simulate_exact_v3_swap,
        sqrt_price_limit_for_bps as exact_v3_price_limit_for_bps,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_V3_EXECUTION_AUTHORITY = (
    PROJECT_ROOT / "config/uniswap_v3_execution_markets.json"
)
V3_EXECUTION_AUTHORITY_PATH = DEFAULT_V3_EXECUTION_AUTHORITY
DEFAULT_TVL_CSV = PROJECT_ROOT / "data/local/dex_pool_tvl_latest.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/processed"
DEFAULT_PUBLISH_DIR = PROJECT_ROOT / "data/local"
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data/raw/dex-depth"
DEFAULT_TVL_RAW_ROOT = PROJECT_ROOT / "data/raw/tvl"

CURRENT_FILENAME = "dex_depth_snapshot.csv"
LATEST_FILENAME = "dex_depth_latest.csv"
HISTORY_FILENAME = "dex_depth_history.csv"
EXECUTION_CURRENT_FILENAME = "dex_execution_cost_snapshot.csv"
EXECUTION_LATEST_FILENAME = "dex_execution_cost_latest.csv"
UNISWAP_V3_EXACT_LATEST_FILENAME = "uniswap_v3_exact_latest.json"
UNISWAP_V3_EXACT_RAW_RECEIPT_FILENAME = "uniswap_v3_exact_validation.json"
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
MAX_RPC_ENDPOINTS = 3
MAX_RPC_ATTEMPT_RECORDS = MAX_RPC_ENDPOINTS * MAX_RETRIES
MAX_RPC_RETRY_DELAY_SECONDS = 8.0
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
V3_CHAIN_ID_BY_NAME = {
    "eth": 1,
    "arbitrum": 42_161,
    "optimism": 10,
    "base": 8_453,
    "bsc": 56,
    "zksync": 324,
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
_V3_BLOCK_HASH = re.compile(r"0x[0-9a-f]{64}\Z", flags=re.ASCII)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_EVM_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z", flags=re.ASCII)
_EXACT_SNAPSHOT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_QUOTER_CALL_DATA = re.compile(r"0x[0-9a-f]{328}\Z", flags=re.ASCII)
_QUOTER_RESULT_DATA = re.compile(r"0x[0-9a-f]{256}\Z", flags=re.ASCII)
V3_MAX_BITMAP_WORDS_PER_DIRECTION = 8

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
SELECTOR_FACTORY = "0xc45a0155"
SELECTOR_FACTORY_GET_POOL = "0x1698ee82"
SELECTOR_QUOTE_EXACT_INPUT_SINGLE_V2 = "0xc6a5026a"
SELECTOR_QUOTE_EXACT_OUTPUT_SINGLE_V2 = "0xbd21704a"

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
    "reason_code",
    "error",
]
DEX_DEPTH_COLUMNS = BASE_COLUMNS + DEPTH_COLUMNS + TRAILING_COLUMNS
DEX_DEPTH_COLLECTION_FAILURE_REASON_CODES = frozenset(
    {
        "network",
        "rate_limit",
        "source_unavailable",
        "parse",
        "validation",
        "collection_failed",
        "depth_usd_price_time_mismatch",
    }
)
DEX_DEPTH_UNSUPPORTED_REASON_CODES = frozenset(
    {
        "source_range_unavailable",
        "unsupported_chain",
        "unsupported_protocol",
        "unsupported_method",
        "unsupported_source",
        "unsupported_protocol_or_chain",
    }
)


class RpcError(RuntimeError):
    """Raised when a JSON-RPC endpoint returns no usable result."""


class RpcConfigurationError(ValueError):
    """Raised for bounded, sanitized RPC configuration failures."""


class RpcTransportError(RpcError):
    """A sanitized transport failure with bounded failover metadata."""

    def __init__(
        self,
        *,
        outcome: str,
        http_status: int | None = None,
        retryable: bool,
        failover_eligible: bool,
    ) -> None:
        super().__init__("rpc_transport_failed")
        self.outcome = outcome
        self.http_status = http_status
        self.retryable = retryable
        self.failover_eligible = failover_eligible


class UsdPriceTimeMismatch(ValueError):
    """Pool state and its USD conversion evidence are not time-aligned."""


def dex_depth_failure_reason_code(error: BaseException) -> str:
    """Classify typed DEX-depth failures without parsing raw messages."""
    if isinstance(error, UsdPriceTimeMismatch):
        return "depth_usd_price_time_mismatch"
    if isinstance(error, RpcTransportError) and error.http_status == 429:
        return "rate_limit"
    if isinstance(error, urllib.error.HTTPError):
        if error.code == 429:
            return "rate_limit"
        return "source_unavailable"
    if isinstance(error, (urllib.error.URLError, TimeoutError)):
        return "network"
    if isinstance(error, (json.JSONDecodeError, UnicodeDecodeError)):
        return "parse"
    if isinstance(error, RpcError):
        return "source_unavailable"
    if isinstance(error, (ValueError, TypeError, KeyError, InvalidOperation)):
        return "validation"
    return "collection_failed"


def dex_unsupported_reason_code(reason: str) -> str:
    """Map collector-owned structural outcomes to bounded public reasons."""
    prefix = str(reason or "").strip().lower().split(":", 1)[0]
    return {
        "unsupported_chain": "unsupported_chain",
        "unsupported_pool_model": "unsupported_protocol",
        "pool_is_not_an_evm_contract_address": "unsupported_method",
        "missing_rpc_endpoint": "unsupported_source",
        "source_range_unavailable": "source_range_unavailable",
        "unsupported_protocol": "unsupported_protocol",
        "unsupported_method": "unsupported_method",
        "unsupported_source": "unsupported_source",
        "unsupported_protocol_or_chain": "unsupported_protocol_or_chain",
    }.get(prefix, "unsupported_source")


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


def validate_rpc_endpoint_url(url: Any) -> str:
    """Accept only structurally valid HTTP(S) RPC URLs without echoing them."""
    try:
        if not isinstance(url, str) or not url:
            raise ValueError
        if any(ord(character) < 32 or character.isspace() for character in url):
            raise ValueError
        if re.search(r"%(?![0-9a-fA-F]{2})", url):
            raise ValueError
        parsed = urllib.parse.urlsplit(url)
        hostname = parsed.hostname
        _port = parsed.port
        if (
            parsed.scheme.lower() not in {"http", "https"}
            or not parsed.netloc
            or not hostname
            or parsed.fragment
            or any(character in hostname for character in "/\\?#@")
        ):
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise RpcConfigurationError("invalid_rpc_endpoint") from None
    return url


def _bounded_rpc_transport_error(error: BaseException) -> RpcTransportError:
    if isinstance(error, CollectionDeadlineExceeded):
        raise error from None
    if isinstance(error, RpcTransportError):
        return error
    if isinstance(error, urllib.error.HTTPError):
        status = error.code if type(error.code) is int else None
        if status in (401, 403, 404):
            return RpcTransportError(
                outcome="provider_rejected",
                http_status=status,
                retryable=False,
                failover_eligible=True,
            )
        if status == 429:
            return RpcTransportError(
                outcome="rate_limited",
                http_status=status,
                retryable=True,
                failover_eligible=True,
            )
        if status is not None and 500 <= status < 600:
            return RpcTransportError(
                outcome="provider_unavailable",
                http_status=status,
                retryable=True,
                failover_eligible=True,
            )
        return RpcTransportError(
            outcome="http_error",
            http_status=status,
            retryable=False,
            failover_eligible=False,
        )
    if isinstance(error, urllib.error.URLError):
        if isinstance(error.reason, CollectionDeadlineExceeded):
            raise error.reason from None
        return RpcTransportError(
            outcome="network_error",
            retryable=True,
            failover_eligible=True,
        )
    if isinstance(error, TimeoutError):
        return RpcTransportError(
            outcome="timeout",
            retryable=True,
            failover_eligible=True,
        )
    if isinstance(error, OSError):
        return RpcTransportError(
            outcome="network_error",
            retryable=True,
            failover_eligible=True,
        )
    return RpcTransportError(
        outcome="transport_error",
        retryable=False,
        failover_eligible=False,
    )


def _rpc_retry_delay(error: BaseException, attempt: int) -> float:
    retry_after = 0.0
    if isinstance(error, urllib.error.HTTPError):
        try:
            retry_after = float(error.headers.get("Retry-After") or 0)
        except (AttributeError, TypeError, ValueError):
            retry_after = 0.0
    return min(
        max(0.0, retry_after, float(2 ** attempt)),
        MAX_RPC_RETRY_DELAY_SECONDS,
    )


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
    url = configured or DEFAULT_RPC_URLS.get(normalized)
    return validate_rpc_endpoint_url(url) if url is not None else None


@dataclass(frozen=True)
class RpcEndpoint:
    """One ordered RPC endpoint with a private URL and safe identity."""

    endpoint_id: str
    url: str
    identity: str = ""

    def __post_init__(self) -> None:
        validate_rpc_endpoint_url(self.url)
        object.__setattr__(self, "identity", sanitize_endpoint(self.url))


def rpc_endpoints_for_chain(chain: str) -> tuple[RpcEndpoint, ...]:
    """Return the bounded, ordered endpoint pool for one supported chain."""
    normalized = chain.lower()
    primary = rpc_url_for_chain(normalized)
    if not primary:
        return ()
    fallback_key = "{}_FALLBACKS".format(RPC_ENV_KEYS.get(normalized, ""))
    fallback_value = os.environ.get(fallback_key)
    if fallback_value is None:
        fallbacks: list[str] = []
    else:
        try:
            fallbacks = json.loads(fallback_value)
        except (json.JSONDecodeError, TypeError):
            raise RpcConfigurationError("invalid_rpc_fallbacks") from None
        if not isinstance(fallbacks, list):
            raise RpcConfigurationError("invalid_rpc_fallbacks") from None
        if any(not isinstance(item, str) or not item.strip() for item in fallbacks):
            raise RpcConfigurationError("invalid_rpc_fallbacks") from None
        if len(fallbacks) > 2:
            raise RpcConfigurationError("invalid_rpc_fallbacks") from None
        if len({primary, *fallbacks}) != len(fallbacks) + 1:
            raise RpcConfigurationError("invalid_rpc_fallbacks") from None
    urls = [primary, *fallbacks]
    return tuple(
        RpcEndpoint(
            "{}-primary".format(normalized)
            if index == 0
            else "{}-fallback-{}".format(normalized, index),
            url,
        )
        for index, url in enumerate(urls)
    )


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
    if is_uniswap_v3_execution_approved(pool):
        require_v3_usd_price_lineage(
            snapshot_id=pool.get("snapshot_id"),
            source=pool.get("source"),
            endpoint=pool.get("source_endpoint"),
            raw_sha256=pool.get("raw_response_sha256"),
        )
    timing = pool_usd_price_timing(pool, block_timestamp)
    if not timing["usable"]:
        raise UsdPriceTimeMismatch(
            "usd_price_conversion_unavailable:"
            f"{timing['reason']}"
        )
    return timing


def require_v3_usd_price_lineage(
    *,
    snapshot_id: Any,
    source: Any,
    endpoint: Any,
    raw_sha256: Any,
) -> None:
    if not str(snapshot_id or "").strip():
        raise ValueError("V3 USD price source snapshot identity is missing")
    if str(source or "").strip() != "GeckoTerminal API v2":
        raise ValueError("V3 USD price source is not GeckoTerminal API v2")
    source_endpoint = str(endpoint or "").strip()
    if not source_endpoint.startswith(
        "https://api.geckoterminal.com/api/v2/"
    ):
        raise ValueError("V3 USD price source endpoint is invalid")
    source_hash = str(raw_sha256 or "").strip()
    if re.fullmatch(r"[0-9a-f]{64}", source_hash, flags=re.ASCII) is None:
        raise ValueError("V3 USD price raw response hash is invalid")


def http_json_rpc(
    url: str,
    payload: Any,
    *,
    deadline: CollectionDeadline | None = None,
    timeout_seconds: float = 30,
    max_retries: int = MAX_RETRIES,
) -> tuple[Any, bytes]:
    validate_rpc_endpoint_url(url)
    if type(max_retries) is not int or not 1 <= max_retries <= MAX_RETRIES:
        raise RpcConfigurationError("invalid_rpc_retry_configuration") from None
    try:
        configured_timeout = float(timeout_seconds)
    except (TypeError, ValueError):
        raise RpcConfigurationError("invalid_rpc_timeout") from None
    if not math.isfinite(configured_timeout) or configured_timeout <= 0:
        raise RpcConfigurationError("invalid_rpc_timeout") from None
    try:
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
    except Exception:
        raise RpcConfigurationError("invalid_rpc_request") from None
    for attempt in range(max_retries):
        try:
            timeout = (
                deadline.request_timeout(configured_timeout)
                if deadline is not None
                else configured_timeout
            )
            with urllib.request.urlopen(
                request,
                timeout=timeout,
                context=TLS_CONTEXT,
            ) as response:
                raw = response.read()
                return json.loads(raw.decode("utf-8")), raw
        except CollectionDeadlineExceeded as error:
            raise error from None
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise
        except Exception as error:
            transport_error = _bounded_rpc_transport_error(error)
            if deadline is not None:
                try:
                    deadline.require_remaining()
                except CollectionDeadlineExceeded as deadline_error:
                    raise deadline_error from None
            if not transport_error.retryable or attempt + 1 >= max_retries:
                raise transport_error from None
            delay = _rpc_retry_delay(error, attempt)
            if deadline is not None:
                try:
                    deadline.sleep_before_retry(delay)
                except CollectionDeadlineExceeded as deadline_error:
                    raise deadline_error from None
            else:
                time.sleep(delay)
    raise RpcTransportError(
        outcome="transport_error",
        retryable=False,
        failover_eligible=False,
    ) from None


def _request_accepts_retry_controls(request: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(request, follow_wrapped=False)
    except Exception:
        raise RpcConfigurationError("invalid_rpc_request_boundary") from None
    try:
        signature.bind(
            object(),
            object(),
            deadline=None,
            timeout_seconds=30.0,
            max_retries=1,
        )
    except TypeError:
        try:
            signature.bind(object(), object())
        except Exception:
            raise RpcConfigurationError("invalid_rpc_request_boundary") from None
        return False
    except Exception:
        raise RpcConfigurationError("invalid_rpc_request_boundary") from None
    return True


class RpcClient:
    def __init__(
        self,
        chain: str,
        url: str,
        *,
        request: Callable[[str, Any], tuple[Any, bytes]] | None = None,
        deadline: CollectionDeadline | None = None,
        timeout_seconds: float = 30,
        max_retries: int = MAX_RETRIES,
        endpoints: Iterable[RpcEndpoint] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.chain = chain
        validate_rpc_endpoint_url(url)
        try:
            endpoint_pool = tuple(endpoints or ())
        except Exception:
            raise RpcConfigurationError("invalid_rpc_endpoint_pool") from None
        if not endpoint_pool:
            endpoint_pool = (
                RpcEndpoint(
                    "{}-primary".format(chain.lower()),
                    url,
                ),
            )
        if (
            len(endpoint_pool) > MAX_RPC_ENDPOINTS
            or any(not isinstance(item, RpcEndpoint) for item in endpoint_pool)
            or len({item.endpoint_id for item in endpoint_pool}) != len(endpoint_pool)
        ):
            raise RpcConfigurationError("invalid_rpc_endpoint_pool") from None
        expected_ids = tuple(
            "{}-primary".format(chain.lower())
            if index == 0
            else "{}-fallback-{}".format(chain.lower(), index)
            for index in range(len(endpoint_pool))
        )
        if tuple(item.endpoint_id for item in endpoint_pool) != expected_ids:
            raise RpcConfigurationError("invalid_rpc_endpoint_pool") from None
        self._endpoints = endpoint_pool
        self._endpoint_index = 0
        self.url = endpoint_pool[0].url
        self.endpoint = endpoint_pool[0].identity
        if request is not None and not callable(request):
            raise RpcConfigurationError("invalid_rpc_request_boundary") from None
        if request is None:
            self.request = self._default_one_attempt_request
            self._request_accepts_retry_controls = True
        else:
            self.request = request
            self._request_accepts_retry_controls = (
                _request_accepts_retry_controls(request)
            )
        self.deadline = deadline
        self._call_deadline = deadline
        try:
            self.timeout_seconds = float(timeout_seconds)
        except (TypeError, ValueError):
            raise RpcConfigurationError("invalid_rpc_timeout") from None
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise RpcConfigurationError("invalid_rpc_timeout") from None
        self.max_retries = max_retries
        if type(self.max_retries) is not int or not 1 <= self.max_retries <= MAX_RETRIES:
            raise RpcConfigurationError("invalid_rpc_retry_configuration") from None
        self.clock = clock
        self.sleeper = sleeper
        self.records: list[dict[str, Any]] = []
        self.endpoint_attempts: list[dict[str, Any]] = []
        self.attempt_ledger = self.endpoint_attempts
        self.endpoint_attempts_dropped = 0
        self._network_attempt_ordinal = 0
        self.endpoint_generation = 0
        self.selected_endpoint_id = endpoint_pool[0].endpoint_id
        self._open_endpoint_ids: set[str] = set()
        self._next_id = 1

    @staticmethod
    def _default_one_attempt_request(
        url: str,
        payload: Any,
        *,
        deadline: CollectionDeadline | None = None,
        timeout_seconds: float = 30,
        max_retries: int = 1,
    ) -> tuple[Any, bytes]:
        del max_retries
        return http_json_rpc(
            url,
            payload,
            deadline=deadline,
            timeout_seconds=timeout_seconds,
            max_retries=1,
        )

    @property
    def open_endpoint_ids(self) -> tuple[str, ...]:
        return tuple(
            item.endpoint_id
            for item in self._endpoints
            if item.endpoint_id in self._open_endpoint_ids
        )

    def _active_endpoint(self) -> RpcEndpoint | None:
        while self._endpoint_index < len(self._endpoints):
            endpoint = self._endpoints[self._endpoint_index]
            if endpoint.endpoint_id not in self._open_endpoint_ids:
                if endpoint.endpoint_id != self.selected_endpoint_id:
                    self.endpoint_generation += 1
                    self.selected_endpoint_id = endpoint.endpoint_id
                self.url = endpoint.url
                self.endpoint = endpoint.identity
                return endpoint
            self._endpoint_index += 1
        return None

    def _record_endpoint_attempt(
        self,
        endpoint: RpcEndpoint,
        payload: Any,
        *,
        outcome: str,
        decision: str,
        duration_seconds: float,
        attempt_ordinal: int,
        endpoint_attempt: int,
        http_status: int | None = None,
    ) -> None:
        if len(self.endpoint_attempts) >= MAX_RPC_ATTEMPT_RECORDS:
            self.endpoint_attempts_dropped += 1
            return
        record: dict[str, Any] = {
            "endpoint_id": endpoint.endpoint_id,
            "endpoint": endpoint.identity,
            "method": (
                payload.get("method", "unknown")
                if isinstance(payload, dict)
                else "batch"
            ),
            "outcome": outcome,
            "decision": decision,
            "duration_seconds": round(max(0.0, duration_seconds), 6),
            "attempt_ordinal": attempt_ordinal,
            "endpoint_attempt": endpoint_attempt,
        }
        if http_status is not None:
            record["http_status"] = http_status
        self.endpoint_attempts.append(record)

    def _request_endpoint(
        self,
        endpoint: RpcEndpoint,
        payload: Any,
        effective_deadline: CollectionDeadline | None,
    ) -> tuple[Any, bytes]:
        if not self._request_accepts_retry_controls:
            return self.request(endpoint.url, payload)
        return self.request(
            endpoint.url,
            payload,
            deadline=effective_deadline,
            timeout_seconds=self.timeout_seconds,
            max_retries=1,
        )

    def _sleep_before_retry(
        self,
        effective_deadline: CollectionDeadline | None,
        attempt: int,
    ) -> None:
        delay = min(float(2 ** attempt), MAX_RPC_RETRY_DELAY_SECONDS)
        if effective_deadline is not None:
            try:
                effective_deadline.sleep_before_retry(delay)
            except CollectionDeadlineExceeded as error:
                raise error from None
        else:
            self.sleeper(delay)

    def _send(self, payload: Any) -> Any:
        effective_deadline = self._call_deadline or self.deadline
        if effective_deadline is not None:
            effective_deadline.require_remaining()
        while True:
            if effective_deadline is not None:
                effective_deadline.require_remaining()
            endpoint = self._active_endpoint()
            if endpoint is None:
                raise RpcError("rpc_endpoint_exhausted")
            for attempt in range(self.max_retries):
                self._network_attempt_ordinal += 1
                attempt_ordinal = self._network_attempt_ordinal
                started = self.clock()
                try:
                    response, raw = self._request_endpoint(
                        endpoint,
                        payload,
                        effective_deadline,
                    )
                except CollectionDeadlineExceeded as error:
                    self._record_endpoint_attempt(
                        endpoint,
                        payload,
                        outcome="deadline_exceeded",
                        decision="abort",
                        duration_seconds=self.clock() - started,
                        attempt_ordinal=attempt_ordinal,
                        endpoint_attempt=attempt + 1,
                    )
                    raise error from None
                except Exception as error:
                    if isinstance(error, RpcConfigurationError):
                        self._record_endpoint_attempt(
                            endpoint,
                            payload,
                            outcome="configuration_error",
                            decision="abort",
                            duration_seconds=self.clock() - started,
                            attempt_ordinal=attempt_ordinal,
                            endpoint_attempt=attempt + 1,
                        )
                        raise error from None
                    if isinstance(error, (json.JSONDecodeError, UnicodeDecodeError)):
                        self._record_endpoint_attempt(
                            endpoint,
                            payload,
                            outcome="protocol_error",
                            decision="abort",
                            duration_seconds=self.clock() - started,
                            attempt_ordinal=attempt_ordinal,
                            endpoint_attempt=attempt + 1,
                        )
                        raise
                    if isinstance(error, RpcError) and not isinstance(
                        error,
                        RpcTransportError,
                    ):
                        self._record_endpoint_attempt(
                            endpoint,
                            payload,
                            outcome="protocol_error",
                            decision="abort",
                            duration_seconds=self.clock() - started,
                            attempt_ordinal=attempt_ordinal,
                            endpoint_attempt=attempt + 1,
                        )
                        raise
                    transport_error = _bounded_rpc_transport_error(error)
                    is_last_attempt = attempt + 1 >= self.max_retries
                    if effective_deadline is not None:
                        try:
                            effective_deadline.require_remaining()
                        except CollectionDeadlineExceeded as deadline_error:
                            self._record_endpoint_attempt(
                                endpoint,
                                payload,
                                outcome="deadline_exceeded",
                                decision="abort",
                                duration_seconds=self.clock() - started,
                                attempt_ordinal=attempt_ordinal,
                                endpoint_attempt=attempt + 1,
                            )
                            raise deadline_error from None
                    has_next_endpoint = self._endpoint_index + 1 < len(
                        self._endpoints
                    )
                    if not transport_error.failover_eligible:
                        decision = "abort"
                    elif transport_error.retryable and not is_last_attempt:
                        decision = "retry"
                    elif has_next_endpoint:
                        decision = "switch"
                    else:
                        decision = "exhausted"
                    self._record_endpoint_attempt(
                        endpoint,
                        payload,
                        outcome=transport_error.outcome,
                        decision=decision,
                        duration_seconds=self.clock() - started,
                        attempt_ordinal=attempt_ordinal,
                        endpoint_attempt=attempt + 1,
                        http_status=transport_error.http_status,
                    )
                    if not transport_error.failover_eligible:
                        raise transport_error from None
                    if transport_error.retryable and not is_last_attempt:
                        self._sleep_before_retry(effective_deadline, attempt)
                        continue
                    self._open_endpoint_ids.add(endpoint.endpoint_id)
                    self._endpoint_index += 1
                    break
                self._record_endpoint_attempt(
                    endpoint,
                    payload,
                    outcome="success",
                    decision="use",
                    duration_seconds=self.clock() - started,
                    attempt_ordinal=attempt_ordinal,
                    endpoint_attempt=attempt + 1,
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


def encode_address_word(address: str) -> str:
    normalized = str(address).lower()
    if _EVM_ADDRESS.fullmatch(normalized) is None:
        raise ValueError("invalid EVM address")
    return normalized[2:].rjust(64, "0")


def factory_get_pool_call(token0: str, token1: str, fee_pips: int) -> str:
    if type(fee_pips) is not int or not 0 <= fee_pips < 1_000_000:
        raise ValueError("invalid Uniswap V3 fee")
    return (
        SELECTOR_FACTORY_GET_POOL
        + encode_address_word(token0)
        + encode_address_word(token1)
        + f"{fee_pips:064x}"
    )


def quoter_v2_single_call(
    *,
    exact_input: bool,
    token_in: str,
    token_out: str,
    amount: int,
    fee_pips: int,
    sqrt_price_limit_x96: int,
) -> str:
    if type(amount) is not int or amount <= 0 or amount >= 1 << 256:
        raise ValueError("invalid Quoter V2 amount")
    if type(fee_pips) is not int or not 0 <= fee_pips < 1_000_000:
        raise ValueError("invalid Quoter V2 fee")
    if (
        type(sqrt_price_limit_x96) is not int
        or not 0 <= sqrt_price_limit_x96 < 1 << 160
    ):
        raise ValueError("invalid Quoter V2 price limit")
    selector = (
        SELECTOR_QUOTE_EXACT_INPUT_SINGLE_V2
        if exact_input
        else SELECTOR_QUOTE_EXACT_OUTPUT_SINGLE_V2
    )
    return (
        selector
        + encode_address_word(token_in)
        + encode_address_word(token_out)
        + f"{amount:064x}"
        + f"{fee_pips:064x}"
        + f"{sqrt_price_limit_x96:064x}"
    )


def decode_v3_quoter_result(value: Any) -> tuple[int, int, int, int]:
    if not isinstance(value, str):
        raise ValueError("QuoterV2 response must be a string")
    if not value.startswith("0x"):
        raise ValueError("QuoterV2 response must be 0x-prefixed")
    if len(value) != 2 + 4 * 64:
        raise ValueError("QuoterV2 response must contain four words")
    payload = value[2:]
    if re.fullmatch(r"[0-9a-fA-F]{256}", payload, flags=re.ASCII) is None:
        raise ValueError("QuoterV2 response contains invalid hex")
    return tuple(
        int(payload[index:index + 64], 16)
        for index in range(0, len(payload), 64)
    )


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


_V2_POOL_STATE_FIELDS = (
    "chain",
    "chain_id",
    "dex",
    "pool_address",
    "token0_address",
    "token1_address",
    "token0_decimals",
    "token1_decimals",
    "reserve0_raw",
    "reserve1_raw",
    "reserve_timestamp_last_raw",
    "fee_bps",
    "fee_numerator",
    "fee_denominator",
    "fee_formula",
    "fee_proof_sha256",
    "block_number",
    "block_hash",
    "block_header_sha256",
    "observed_at",
    "raw_response_sha256",
)


def freeze_v2_pool_state(source: Any) -> V2PoolState:
    """Read every mutable V2 input exactly once into an immutable state."""
    if isinstance(source, V2PoolState):
        return source
    getter = getattr(source, "get", None)
    if not callable(getter):
        raise ValueError("V2 pool state source must be a mapping")
    frozen = {field: getter(field) for field in _V2_POOL_STATE_FIELDS}
    return V2PoolState(**frozen)


def route_quantity_quote_for_v2_pool(
    source: Any,
    *,
    direction: str,
    target_token_quantity: CommonTarget,
    market_rules: MarketRules,
    target_token_address: str,
    quote_token_address: str,
    expected_state_id: str,
    cohort_now: str,
) -> QuantityQuote:
    """Freeze once, verify the caller's binding, and quote the same state."""
    state = freeze_v2_pool_state(source)
    if state.state_id != expected_state_id:
        raise ValueError("V2 quantity state binding does not match")
    quote = quote_v2_pool_quantity(
        state,
        target_token_quantity,
        market_rules,
        direction=direction,
        target_token_address=target_token_address,
        quote_token_address=quote_token_address,
        cohort_now=cohort_now,
    )
    return validate_v2_quantity_quote_against_state(
        quote,
        state,
        target_token_quantity,
        market_rules,
        direction=direction,
        target_token_address=target_token_address,
        quote_token_address=quote_token_address,
        cohort_now=cohort_now,
    )


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


def _collect_exact_v3_bitmap_word(
    client: RpcClient,
    pool_address: str,
    block_tag: str,
    word_position: int,
    tick_spacing: int,
) -> tuple[dict[int, int], list[dict[str, int]]]:
    """Collect one complete bitmap word and every state row asserted by it."""
    bitmap_result = client.eth_calls(
        pool_address,
        [call_with_int(SELECTOR_TICK_BITMAP, word_position, 16)],
        block_tag,
    )[0]
    bitmap = decode_uint(bitmap_result)
    tick_indexes = [
        (word_position * 256 + bit) * tick_spacing
        for bit in range(256)
        if bitmap & (1 << bit)
    ]
    if any(not V3_MIN_TICK <= tick <= V3_MAX_TICK for tick in tick_indexes):
        raise ValueError("V3 bitmap asserts a tick outside protocol bounds")
    tick_results = (
        client.eth_calls(
            pool_address,
            [call_with_int(SELECTOR_TICKS, tick, 24) for tick in tick_indexes],
            block_tag,
        )
        if tick_indexes
        else []
    )
    ticks: dict[int, int] = {}
    evidence: list[dict[str, int]] = []
    for tick, result in zip(tick_indexes, tick_results):
        liquidity_gross = decode_uint(result, 0)
        liquidity_net = decode_int(result, 1, 128)
        if liquidity_gross <= 0:
            raise ValueError("V3 bitmap/tick liquidityGross evidence is inconsistent")
        ticks[tick] = liquidity_net
        evidence.append(
            {
                "word_position": word_position,
                "bit_position": (tick // tick_spacing) - word_position * 256,
                "tick": tick,
                "liquidity_gross": liquidity_gross,
                "liquidity_net": liquidity_net,
            }
        )
    return ticks, evidence


def collect_exact_v3_state_window(
    client: RpcClient,
    pool_address: str,
    block_tag: str,
    *,
    sqrt_price_x96: int,
    current_tick: int,
    active_liquidity: int,
    fee_pips: int,
    tick_spacing: int,
    zero_for_one_amount_specified: int,
    one_for_zero_amount_specified: int,
    bitmap_word_radius: int,
) -> dict[str, Any]:
    """Expand complete bitmap words until depth and max execution are proven."""
    if type(bitmap_word_radius) is not int or not (
        1 <= bitmap_word_radius <= V3_MAX_BITMAP_WORDS_PER_DIRECTION
    ):
        raise ValueError("V3 bitmap word radius is invalid")
    current_word = (current_tick // tick_spacing) >> 8
    word_cache: dict[int, tuple[dict[int, int], list[dict[str, int]]]] = {}
    all_ticks: dict[int, int] = {}
    tick_evidence: dict[int, dict[str, int]] = {}

    def load_word(word_position: int) -> None:
        if word_position in word_cache:
            return
        ticks, evidence = _collect_exact_v3_bitmap_word(
            client,
            pool_address,
            block_tag,
            word_position,
            tick_spacing,
        )
        word_cache[word_position] = (ticks, evidence)
        all_ticks.update(ticks)
        for item in evidence:
            tick_evidence[item["tick"]] = item

    direction_results: dict[str, dict[str, Any]] = {}
    maximum_depth_limit = {
        True: exact_v3_price_limit_for_bps(
            sqrt_price_x96,
            max(DEPTH_BANDS_BPS),
            zero_for_one=True,
        ),
        False: exact_v3_price_limit_for_bps(
            sqrt_price_x96,
            max(DEPTH_BANDS_BPS),
            zero_for_one=False,
        ),
    }
    for zero_for_one, amount_specified in (
        (True, zero_for_one_amount_specified),
        (False, one_for_zero_amount_specified),
    ):
        scanned_words: list[int] = []
        last_limit: int | None = None
        max_result = None
        for offset in range(bitmap_word_radius):
            word_position = current_word - offset if zero_for_one else current_word + offset
            load_word(word_position)
            scanned_words.append(word_position)
            if zero_for_one:
                boundary_tick = max(
                    V3_MIN_TICK,
                    word_position * 256 * tick_spacing,
                )
            else:
                boundary_tick = min(
                    V3_MAX_TICK,
                    (word_position * 256 + 255) * tick_spacing,
                )
            boundary_sqrt = exact_v3_sqrt_ratio_at_tick(boundary_tick)
            if zero_for_one and boundary_sqrt >= sqrt_price_x96:
                continue
            if not zero_for_one and boundary_sqrt <= sqrt_price_x96:
                continue
            last_limit = boundary_sqrt
            max_result = simulate_exact_v3_swap(
                sqrt_price_x96=sqrt_price_x96,
                current_tick=current_tick,
                liquidity=active_liquidity,
                fee_pips=fee_pips,
                initialized_ticks=all_ticks,
                amount_specified=amount_specified,
                zero_for_one=zero_for_one,
                sqrt_price_limit_x96=boundary_sqrt,
            )
            depth_covered = (
                boundary_sqrt <= maximum_depth_limit[True]
                if zero_for_one
                else boundary_sqrt >= maximum_depth_limit[False]
            )
            if max_result.complete and depth_covered:
                break
        if last_limit is None or max_result is None:
            raise ValueError("V3 bitmap scan did not establish a price window")
        direction_results["zero_for_one" if zero_for_one else "one_for_zero"] = {
            "price_limit_x96": last_limit,
            "word_positions": scanned_words,
            "max_execution_complete": max_result.complete,
            "terminal_reason": (
                "requirements_proven"
                if max_result.complete
                and (
                    last_limit <= maximum_depth_limit[True]
                    if zero_for_one
                    else last_limit >= maximum_depth_limit[False]
                )
                else "source_tick_scan_limit"
            ),
        }
    return {
        "initialized_ticks": dict(sorted(all_ticks.items())),
        "tick_evidence": [tick_evidence[tick] for tick in sorted(tick_evidence)],
        "bitmap_words": sorted(word_cache),
        "directions": direction_results,
        "bitmap_word_radius": bitmap_word_radius,
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
            "reason_code": dex_unsupported_reason_code(reason),
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


def load_uniswap_v3_execution_authority(
    path: Path = V3_EXECUTION_AUTHORITY_PATH,
) -> dict[str, dict[str, Any]]:
    """Load the exact two-pool V3 capability authority, failing closed."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Uniswap V3 execution authority is unavailable") from error
    if not isinstance(payload, dict) or payload.get("schema") != (
        "uniswap_v3_execution_markets/v1"
    ):
        raise ValueError("Uniswap V3 execution authority has wrong schema")
    markets = payload.get("markets")
    if not isinstance(markets, list) or len(markets) != 2:
        raise ValueError("Uniswap V3 execution authority must contain two markets")
    required = {
        "market_id", "chain", "chain_id", "dex", "pool_address",
        "factory_address", "quoter_v2_address", "token0_address",
        "token0_decimals", "token1_address", "token1_decimals",
        "fee_pips", "tick_spacing", "bitmap_word_radius",
    }
    result = {}
    for index, raw_market in enumerate(markets):
        if not isinstance(raw_market, dict):
            raise ValueError("authority market {} is not an object".format(index))
        missing = sorted(required - set(raw_market))
        if missing:
            raise ValueError("Uniswap V3 execution authority is missing " + missing[0])
        if set(raw_market) != required:
            raise ValueError("Uniswap V3 execution authority has unknown fields")
        market = dict(raw_market)
        for field in (
            "pool_address", "factory_address", "quoter_v2_address",
            "token0_address", "token1_address",
        ):
            value = market[field]
            if not isinstance(value, str) or _EVM_ADDRESS.fullmatch(value) is None:
                raise ValueError("Uniswap V3 execution authority has bad " + field)
        if market["chain"] != "eth":
            raise ValueError("Uniswap V3 execution authority has wrong chain")
        if market["dex"] != "uniswap_v3":
            raise ValueError("Uniswap V3 execution authority has wrong dex")
        bounds = {
            "chain_id": (1, 2**63 - 1),
            "token0_decimals": (0, 255), "token1_decimals": (0, 255),
            "fee_pips": (1, 999_999), "tick_spacing": (1, V3_MAX_TICK),
            "bitmap_word_radius": (1, V3_MAX_BITMAP_WORDS_PER_DIRECTION),
        }
        for field, (minimum, maximum) in bounds.items():
            value = market[field]
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError("Uniswap V3 execution authority has bad " + field)
        market_id = market["market_id"]
        if not isinstance(market_id, str):
            raise ValueError("Uniswap V3 execution authority has bad market_id")
        parts = market_id.split(":")
        if (
            len(parts) != 5 or parts[:3] != ["dex", market["chain"], market["dex"]]
            or parts[3] != market["pool_address"] or not parts[4]
            or parts[4] != parts[4].upper()
        ):
            raise ValueError("Uniswap V3 execution authority market identity mismatch")
        if market_id in result:
            raise ValueError("Uniswap V3 execution authority has duplicate market")
        result[market_id] = market
    return result


def match_uniswap_v3_execution_authority(
    pool: Mapping[str, Any],
    observed_identity: Mapping[str, Any],
    *,
    authority: Mapping[str, Mapping[str, Any]] = None,
) -> dict[str, Any] | None:
    """Return the authority record or reject an approved identity mismatch."""
    records = dict(authority) if authority is not None else load_uniswap_v3_execution_authority()
    try:
        derived_market_id = dex_market_id(dict(pool))
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError("Uniswap V3 authority pool identity is invalid") from error
    supplied_market_id = pool.get("market_id")
    if supplied_market_id not in (None, ""):
        supplied_record = records.get(str(supplied_market_id))
        if supplied_record is not None:
            for field in ("chain", "dex", "pool_address"):
                actual = str(pool.get(field) or "")
                if field == "pool_address":
                    actual = actual.lower()
                if actual != supplied_record[field]:
                    raise ValueError("Uniswap V3 authority mismatch: " + field)
        if str(supplied_market_id) != derived_market_id:
            raise ValueError("Uniswap V3 authority mismatch: market_id")

    pool_address = str(pool.get("pool_address") or "").strip().lower()
    if _EVM_ADDRESS.fullmatch(pool_address) is None:
        raise ValueError("Uniswap V3 authority pool identity has bad pool_address")
    pool_chain = str(pool.get("chain") or "").strip().lower()
    if not pool_chain:
        raise ValueError("Uniswap V3 authority pool identity is missing chain")
    if not str(pool.get("dex") or "").strip():
        raise ValueError("Uniswap V3 authority pool identity is missing dex")
    if not str(pool.get("token_symbol") or "").strip():
        raise ValueError("Uniswap V3 authority pool identity is missing token_symbol")
    if not isinstance(observed_identity, Mapping):
        raise ValueError("Uniswap V3 authority observed identity is invalid")

    observed_fields = (
        "chain_id", "pool_address", "factory_address",
        "factory_get_pool_address", "token0_address", "token0_decimals",
        "token1_address", "token1_decimals", "fee_pips", "tick_spacing",
    )
    for field in observed_fields:
        if field not in observed_identity:
            raise ValueError("Uniswap V3 authority evidence is missing " + field)
    for field in (
        "pool_address", "factory_address", "factory_get_pool_address",
        "token0_address", "token1_address",
    ):
        value = str(observed_identity[field]).lower()
        if _EVM_ADDRESS.fullmatch(value) is None:
            raise ValueError("Uniswap V3 authority evidence has bad " + field)
    for field, (minimum, maximum) in {
        "chain_id": (1, 2**63 - 1),
        "token0_decimals": (0, 255),
        "token1_decimals": (0, 255),
        "fee_pips": (1, 999_999),
        "tick_spacing": (1, V3_MAX_TICK),
    }.items():
        value = observed_identity[field]
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError("Uniswap V3 authority evidence has bad " + field)
    expected_chain_id = V3_CHAIN_ID_BY_NAME.get(pool_chain)
    if expected_chain_id is None:
        raise ValueError("Uniswap V3 authority chain_id has unsupported chain")
    if observed_identity["chain_id"] != expected_chain_id:
        raise ValueError("Uniswap V3 authority mismatch: chain_id")
    if str(observed_identity["pool_address"]).lower() != pool_address:
        raise ValueError("Uniswap V3 authority mismatch: pool_address")
    if str(observed_identity["factory_get_pool_address"]).lower() != pool_address:
        raise ValueError("Uniswap V3 authority mismatch: factory_get_pool_address")

    record = records.get(derived_market_id)
    if record is None:
        return None
    for field, expected in {
        "chain": record["chain"], "dex": record["dex"],
        "pool_address": record["pool_address"],
    }.items():
        actual = str(pool.get(field) or "")
        if field == "pool_address":
            actual = actual.lower()
        if actual != expected:
            raise ValueError("Uniswap V3 authority mismatch: " + field)
    expected_observed = {
        "chain_id": record["chain_id"], "pool_address": record["pool_address"],
        "factory_address": record["factory_address"],
        "factory_get_pool_address": record["pool_address"],
        "token0_address": record["token0_address"], "token0_decimals": record["token0_decimals"],
        "token1_address": record["token1_address"], "token1_decimals": record["token1_decimals"],
        "fee_pips": record["fee_pips"], "tick_spacing": record["tick_spacing"],
    }
    for field, expected in expected_observed.items():
        if field not in observed_identity:
            raise ValueError("Uniswap V3 authority evidence is missing " + field)
        actual = observed_identity[field]
        if field.endswith("_address"):
            actual = str(actual).lower()
        if actual != expected:
            raise ValueError("Uniswap V3 authority mismatch: " + field)
    return dict(record)


def is_uniswap_v3_execution_approved(pool: Mapping[str, Any]) -> bool:
    authority = load_uniswap_v3_execution_authority()
    market_id = str(pool.get("market_id") or "")
    if market_id and market_id in authority:
        return True
    try:
        if dex_market_id(dict(pool)) in authority:
            return True
    except (KeyError, TypeError):
        pass
    chain = str(pool.get("chain") or "").strip().lower()
    dex = str(pool.get("dex") or "").strip().lower()
    pool_address = str(pool.get("pool_address") or "").strip().lower()
    return any(
        record["chain"] == chain
        and record["dex"] == dex
        and record["pool_address"] == pool_address
        for record in authority.values()
    )


def _regular_evidence_bytes(path: Path, label: str) -> bytes:
    """Read one retained evidence file without following a symlink."""
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError(f"{label} is missing or is not regular evidence") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"{label} is not regular evidence")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            return source.read()
    finally:
        os.close(descriptor)


def _evidence_json(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    raw = _regular_evidence_bytes(path, label)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is unreadable") from error
    if not isinstance(payload, dict):
        raise ValueError(f"{label} is not a JSON object")
    return raw, payload


def _evidence_directory(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise ValueError(f"{label} directory is missing") from error
    if not stat.S_ISDIR(mode):
        raise ValueError(f"{label} directory is not regular evidence")


def _snapshot_evidence_directory(
    root: Path,
    snapshot_id: str,
    label: str,
) -> Path:
    if (
        type(snapshot_id) is not str
        or _EXACT_SNAPSHOT_ID.fullmatch(snapshot_id) is None
    ):
        raise ValueError(f"{label} snapshot ID is invalid")
    root = Path(root)
    directory = root / snapshot_id
    _evidence_directory(root, f"{label} evidence root")
    _evidence_directory(directory, label)
    try:
        resolved_root = root.resolve(strict=True)
        resolved_directory = directory.resolve(strict=True)
    except OSError as error:
        raise ValueError(f"{label} evidence root is invalid") from error
    if resolved_directory.parent != resolved_root:
        raise ValueError(f"{label} evidence escapes its configured evidence root")
    return directory


def _manifest_raw_paths(
    directory: Path,
    manifest: Mapping[str, Any],
    *,
    label: str,
) -> list[Path]:
    raw_files = manifest.get("raw_files")
    if (
        not isinstance(raw_files, list)
        or not raw_files
        or any(type(name) is not str for name in raw_files)
        or len(raw_files) != len(set(raw_files))
    ):
        raise ValueError(f"{label} manifest raw file inventory is invalid")
    for name in raw_files:
        if Path(name).name != name or name == "manifest.json" or not name.endswith(".json"):
            raise ValueError(f"{label} manifest raw file inventory is invalid")
    actual = sorted(
        path.name
        for path in directory.iterdir()
        if path.name
        not in {"manifest.json", UNISWAP_V3_EXACT_RAW_RECEIPT_FILENAME}
        and path.suffix == ".json"
    )
    if sorted(raw_files) != actual:
        raise ValueError(f"{label} manifest does not match retained evidence")
    paths = [directory / name for name in sorted(raw_files)]
    for path in paths:
        _regular_evidence_bytes(path, f"{label} raw evidence")
    return paths


def _exact_scenarios() -> list[tuple[str, str]]:
    return [
        (direction, decimal_text(notional))
        for notional in EXECUTION_NOTIONALS_USD
        for direction in EXECUTION_DIRECTIONS
    ]


def _retained_quoter_results(
    records: Any,
    *,
    quoter_v2_address: str,
    block_number: int,
) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        raise ValueError("Uniswap V3 raw Quoter transcript is missing")
    results = []
    request_ids = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        requests = record.get("request")
        responses = record.get("response")
        request_items = requests if isinstance(requests, list) else [requests]
        response_items = responses if isinstance(responses, list) else [responses]
        for request in request_items:
            if not isinstance(request, dict) or request.get("method") != "eth_call":
                continue
            params = request.get("params")
            if not isinstance(params, list) or len(params) != 2:
                continue
            call = params[0]
            if not isinstance(call, dict):
                continue
            if str(call.get("to") or "").lower() != quoter_v2_address:
                continue
            record_request_ids = [
                item.get("id")
                for item in request_items
                if isinstance(item, dict)
            ]
            record_response_ids = [
                item.get("id")
                for item in response_items
                if isinstance(item, dict)
            ]
            if (
                len(record_request_ids) != len(request_items)
                or len(record_response_ids) != len(response_items)
                or len(record_request_ids) != len(record_response_ids)
                or any(type(value) is not int for value in record_request_ids)
                or any(type(value) is not int for value in record_response_ids)
                or len(record_request_ids) != len(set(record_request_ids))
                or len(record_response_ids) != len(set(record_response_ids))
                or set(record_request_ids) != set(record_response_ids)
            ):
                raise ValueError("Uniswap V3 raw Quoter request mapping is invalid")
            data = str(call.get("data") or "").lower()
            if _QUOTER_CALL_DATA.fullmatch(data) is None:
                raise ValueError("Uniswap V3 raw Quoter calldata is invalid")
            selector = data[:10]
            if selector == SELECTOR_QUOTE_EXACT_INPUT_SINGLE_V2:
                direction = "sell_token"
            elif selector == SELECTOR_QUOTE_EXACT_OUTPUT_SINGLE_V2:
                direction = "buy_token"
            else:
                raise ValueError("Uniswap V3 raw Quoter selector is invalid")
            if params[1] != hex(block_number):
                raise ValueError("Uniswap V3 raw Quoter block is invalid")
            request_id = request.get("id")
            if (
                request.get("jsonrpc") != "2.0"
                or type(request_id) is not int
                or request_id in request_ids
            ):
                raise ValueError("Uniswap V3 raw Quoter request mapping is invalid")
            request_ids.add(request_id)
            matching_responses = [
                response
                for response in response_items
                if isinstance(response, dict) and response.get("id") == request_id
            ]
            if len(matching_responses) != 1:
                raise ValueError("Uniswap V3 raw Quoter request mapping is invalid")
            response = matching_responses[0]
            if response.get("jsonrpc") != "2.0" or "error" in response:
                raise ValueError("Uniswap V3 raw Quoter response is invalid")
            result = response.get("result") if isinstance(response, dict) else None
            if (
                not isinstance(result, str)
                or _QUOTER_RESULT_DATA.fullmatch(result) is None
            ):
                raise ValueError("Uniswap V3 raw Quoter result is invalid")
            calldata_words = [
                data[index:index + 64]
                for index in range(10, len(data), 64)
            ]
            if any(word[:24] != "0" * 24 for word in calldata_words[:2]):
                raise ValueError("Uniswap V3 raw Quoter token address is invalid")
            token_in = "0x" + calldata_words[0][24:]
            token_out = "0x" + calldata_words[1][24:]
            amount = int(calldata_words[2], 16)
            fee_pips = int(calldata_words[3], 16)
            sqrt_price_limit_x96 = int(calldata_words[4], 16)
            response_words = tuple(decode_uint(result, index) for index in range(4))
            if (
                token_in == token_out
                or amount <= 0
                or fee_pips >= 1_000_000
                or sqrt_price_limit_x96 >= 1 << 160
                or response_words[0] <= 0
                or response_words[1] >= 1 << 160
                or response_words[2] >= 1 << 32
                or response_words[3] <= 0
            ):
                raise ValueError("Uniswap V3 raw Quoter ABI value is invalid")
            results.append(
                {
                    "direction": direction,
                    "token_in": token_in,
                    "token_out": token_out,
                    "amount": amount,
                    "fee_pips": fee_pips,
                    "sqrt_price_limit_x96": sqrt_price_limit_x96,
                    "response_words": response_words,
                }
            )
    return results


def _retained_pool_call_results(
    records: Any,
    *,
    pool_address: str,
    block_number: int,
) -> dict[str, str]:
    """Return unique fixed-block pool eth_call results from retained records."""
    if not isinstance(records, list):
        raise ValueError("Uniswap V3 raw pool-state transcript is missing")
    expected_block = hex(block_number)
    results: dict[str, str] = {}
    request_ids = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        requests = record.get("request")
        responses = record.get("response")
        request_items = requests if isinstance(requests, list) else [requests]
        response_items = responses if isinstance(responses, list) else [responses]
        selected = []
        for request in request_items:
            if not isinstance(request, dict) or request.get("method") != "eth_call":
                continue
            params = request.get("params")
            call = params[0] if isinstance(params, list) and len(params) == 2 else None
            if (
                isinstance(call, dict)
                and str(call.get("to") or "").strip().lower() == pool_address
            ):
                selected.append(request)
        if not selected:
            continue
        record_request_ids = [
            item.get("id") for item in request_items if isinstance(item, dict)
        ]
        record_response_ids = [
            item.get("id") for item in response_items if isinstance(item, dict)
        ]
        if (
            len(record_request_ids) != len(request_items)
            or len(record_response_ids) != len(response_items)
            or len(record_request_ids) != len(record_response_ids)
            or any(type(value) is not int for value in record_request_ids)
            or any(type(value) is not int for value in record_response_ids)
            or len(record_request_ids) != len(set(record_request_ids))
            or len(record_response_ids) != len(set(record_response_ids))
            or set(record_request_ids) != set(record_response_ids)
        ):
            raise ValueError("Uniswap V3 raw pool-state request mapping is invalid")
        responses_by_id = {
            response["id"]: response
            for response in response_items
            if isinstance(response, dict) and type(response.get("id")) is int
        }
        for request in selected:
            params = request["params"]
            call = params[0]
            request_id = request.get("id")
            data = str(call.get("data") or "").strip().lower()
            response = responses_by_id.get(request_id)
            result = response.get("result") if isinstance(response, dict) else None
            if (
                request.get("jsonrpc") != "2.0"
                or type(request_id) is not int
                or request_id in request_ids
                or params[1] != expected_block
                or re.fullmatch(r"0x[0-9a-f]+", data, flags=re.ASCII) is None
                or len(data) % 2 != 0
                or not isinstance(response, dict)
                or response.get("jsonrpc") != "2.0"
                or response.get("id") != request_id
                or "error" in response
                or not isinstance(result, str)
                or re.fullmatch(
                    r"0x(?:[0-9a-f]{64})+", result, flags=re.ASCII
                )
                is None
                or data in results
            ):
                raise ValueError("Uniswap V3 raw pool-state evidence is invalid")
            request_ids.add(request_id)
            results[data] = result
    if not results:
        raise ValueError("Uniswap V3 raw pool-state evidence is missing")
    return results


def _required_pool_call(
    results: Mapping[str, str],
    data: str,
    label: str,
) -> str:
    result = results.get(data)
    if result is None:
        raise ValueError(f"Uniswap V3 raw {label} evidence is missing")
    return result


def _exact_abi_words(result: str, count: int, label: str) -> list[int]:
    encoded_words = words(result)
    if len(encoded_words) != count:
        raise ValueError(f"Uniswap V3 raw {label} ABI word count is invalid")
    return [int(value, 16) for value in encoded_words]


def _exact_abi_uint(word: int, bits: int, label: str) -> int:
    if type(word) is not int or not 0 <= word < 1 << bits:
        raise ValueError(f"Uniswap V3 raw {label} ABI uint{bits} is invalid")
    return word


def _exact_abi_int(word: int, bits: int, label: str) -> int:
    if type(word) is not int or not 0 <= word < 1 << 256:
        raise ValueError(f"Uniswap V3 raw {label} ABI int{bits} is invalid")
    masked = word & ((1 << bits) - 1)
    value = masked - (1 << bits) if masked & (1 << (bits - 1)) else masked
    if word != value % (1 << 256):
        raise ValueError(
            f"Uniswap V3 raw {label} ABI int{bits} extension is invalid"
        )
    return value


def _exact_abi_address(word: int, label: str) -> str:
    value = _exact_abi_uint(word, 160, label)
    return f"0x{value:040x}"


def _exact_abi_bool(word: int, label: str) -> bool:
    if word not in (0, 1):
        raise ValueError(f"Uniswap V3 raw {label} ABI bool is invalid")
    return bool(word)


def _exact_v3_max_liquidity_per_tick(tick_spacing: int) -> int:
    if type(tick_spacing) is not int or tick_spacing <= 0:
        raise ValueError("Uniswap V3 tick spacing is invalid")
    minimum_tick = -(abs(V3_MIN_TICK) // tick_spacing) * tick_spacing
    maximum_tick = (V3_MAX_TICK // tick_spacing) * tick_spacing
    tick_count = (maximum_tick - minimum_tick) // tick_spacing + 1
    return ((1 << 128) - 1) // tick_count


def _replayed_exact_v3_depth_fields(
    records: Any,
    scan: Mapping[str, Any],
    authority: Mapping[str, Any],
    depth_row: Mapping[str, Any],
    *,
    block_number: int,
    token0_price: Decimal,
    token1_price: Decimal,
) -> dict[str, str]:
    """Independently replay exact depth from retained fixed-block RPC calls."""
    pool_address = str(authority["pool_address"])
    call_results = _retained_pool_call_results(
        records,
        pool_address=pool_address,
        block_number=block_number,
    )
    try:
        token0_word = _exact_abi_words(
            _required_pool_call(call_results, SELECTOR_TOKEN0, "token0"),
            1,
            "token0",
        )[0]
        token1_word = _exact_abi_words(
            _required_pool_call(call_results, SELECTOR_TOKEN1, "token1"),
            1,
            "token1",
        )[0]
        slot0 = _required_pool_call(call_results, SELECTOR_SLOT0, "slot0")
        slot0_words = _exact_abi_words(slot0, 7, "slot0")
        sqrt_price_x96 = _exact_abi_uint(slot0_words[0], 160, "sqrtPriceX96")
        current_tick = _exact_abi_int(slot0_words[1], 24, "slot0 tick")
        observation_index = _exact_abi_uint(
            slot0_words[2], 16, "observationIndex"
        )
        observation_cardinality = _exact_abi_uint(
            slot0_words[3], 16, "observationCardinality"
        )
        observation_cardinality_next = _exact_abi_uint(
            slot0_words[4], 16, "observationCardinalityNext"
        )
        _exact_abi_uint(slot0_words[5], 8, "feeProtocol")
        unlocked = _exact_abi_bool(slot0_words[6], "unlocked")
        active_liquidity = _exact_abi_uint(
            _exact_abi_words(
                _required_pool_call(
                    call_results, SELECTOR_LIQUIDITY, "liquidity"
                ),
                1,
                "liquidity",
            )[0],
            128,
            "liquidity",
        )
        fee_pips = _exact_abi_uint(
            _exact_abi_words(
                _required_pool_call(call_results, SELECTOR_FEE, "fee"),
                1,
                "fee",
            )[0],
            24,
            "fee",
        )
        tick_spacing = _exact_abi_int(
            _exact_abi_words(
                _required_pool_call(
                    call_results, SELECTOR_TICK_SPACING, "tick spacing"
                ),
                1,
                "tick spacing",
            )[0],
            24,
            "tick spacing",
        )
        factory = _exact_abi_address(
            _exact_abi_words(
                _required_pool_call(call_results, SELECTOR_FACTORY, "factory"),
                1,
                "factory",
            )[0],
            "factory",
        )
        token0 = _exact_abi_address(token0_word, "token0")
        token1 = _exact_abi_address(token1_word, "token1")
    except (IndexError, KeyError, TypeError, ValueError) as error:
        raise ValueError("Uniswap V3 raw pool-state evidence is invalid") from error
    if (
        not V3_MIN_SQRT_RATIO <= sqrt_price_x96 < V3_MAX_SQRT_RATIO
        or not V3_MIN_TICK <= current_tick <= V3_MAX_TICK
    ):
        raise ValueError("Uniswap V3 raw pool-state evidence is invalid")
    derived_tick = exact_v3_tick_at_sqrt_ratio(sqrt_price_x96)
    at_exact_tick_boundary = (
        sqrt_price_x96 == exact_v3_sqrt_ratio_at_tick(derived_tick)
    )
    if (
        token0 != authority["token0_address"]
        or token1 != authority["token1_address"]
        or factory != authority["factory_address"]
        or fee_pips != authority["fee_pips"]
        or tick_spacing != authority["tick_spacing"]
        or active_liquidity <= 0
        or not unlocked
        or observation_cardinality <= 0
        or observation_index >= observation_cardinality
        or observation_cardinality_next < observation_cardinality
        or (
            current_tick != derived_tick
            and not (
                at_exact_tick_boundary and current_tick == derived_tick - 1
            )
        )
    ):
        raise ValueError("Uniswap V3 raw pool-state evidence is invalid")

    directions = scan.get("directions")
    if not isinstance(directions, dict) or set(directions) != {
        "zero_for_one",
        "one_for_zero",
    }:
        raise ValueError("Uniswap V3 depth replay direction evidence is invalid")
    current_word = (current_tick // tick_spacing) >> 8
    retained_words = set()
    maximum_depth_limits = {
        "zero_for_one": exact_v3_price_limit_for_bps(
            sqrt_price_x96,
            max(DEPTH_BANDS_BPS),
            zero_for_one=True,
        ),
        "one_for_zero": exact_v3_price_limit_for_bps(
            sqrt_price_x96,
            max(DEPTH_BANDS_BPS),
            zero_for_one=False,
        ),
    }
    for name, step in (("zero_for_one", -1), ("one_for_zero", 1)):
        direction = directions[name]
        if not isinstance(direction, dict):
            raise ValueError("Uniswap V3 depth replay direction evidence is invalid")
        word_positions = direction.get("word_positions")
        if (
            not isinstance(word_positions, list)
            or not 1 <= len(word_positions) <= authority["bitmap_word_radius"]
            or any(type(value) is not int for value in word_positions)
            or word_positions
            != [current_word + step * offset for offset in range(len(word_positions))]
            or direction.get("max_execution_complete") is not True
            or direction.get("terminal_reason") != "requirements_proven"
        ):
            raise ValueError("Uniswap V3 depth replay direction evidence is invalid")
        retained_words.update(word_positions)
        boundary_prices = []
        for word_position in word_positions:
            boundary_tick = (
                max(V3_MIN_TICK, word_position * 256 * tick_spacing)
                if name == "zero_for_one"
                else min(
                    V3_MAX_TICK,
                    (word_position * 256 + 255) * tick_spacing,
                )
            )
            boundary_price = exact_v3_sqrt_ratio_at_tick(boundary_tick)
            if (
                name == "zero_for_one" and boundary_price < sqrt_price_x96
            ) or (
                name == "one_for_zero" and boundary_price > sqrt_price_x96
            ):
                boundary_prices.append(boundary_price)
        price_limit = direction.get("price_limit_x96")
        if (
            not boundary_prices
            or type(price_limit) is not int
            or price_limit != boundary_prices[-1]
            or (
                name == "zero_for_one"
                and price_limit > maximum_depth_limits[name]
            )
            or (
                name == "one_for_zero"
                and price_limit < maximum_depth_limits[name]
            )
        ):
            raise ValueError("Uniswap V3 depth replay coverage evidence is invalid")

    bitmap_word_evidence = scan.get("bitmap_words")
    if not isinstance(bitmap_word_evidence, list):
        raise ValueError("Uniswap V3 depth replay bitmap evidence is invalid")
    bitmap_words = []
    for evidence in bitmap_word_evidence:
        if (
            not isinstance(evidence, dict)
            or set(evidence) != {"word_position"}
            or type(evidence["word_position"]) is not int
        ):
            raise ValueError("Uniswap V3 depth replay bitmap evidence is invalid")
        bitmap_words.append(evidence["word_position"])
    if bitmap_words != sorted(retained_words):
        raise ValueError("Uniswap V3 depth replay bitmap evidence is invalid")
    expected_bitmap_calls = {
        call_with_int(SELECTOR_TICK_BITMAP, word_position, 16)
        for word_position in retained_words
    }
    actual_bitmap_calls = {
        data for data in call_results if data.startswith(SELECTOR_TICK_BITMAP)
    }
    if actual_bitmap_calls != expected_bitmap_calls:
        raise ValueError("Uniswap V3 depth replay bitmap evidence is invalid")

    initialized_ticks = {}
    expected_tick_evidence = []
    maximum_liquidity_per_tick = _exact_v3_max_liquidity_per_tick(tick_spacing)
    for word_position in sorted(retained_words):
        bitmap = _exact_abi_words(
            _required_pool_call(
                call_results,
                call_with_int(SELECTOR_TICK_BITMAP, word_position, 16),
                "tick bitmap",
            ),
            1,
            "tick bitmap",
        )[0]
        for bit_position in range(256):
            if not bitmap & (1 << bit_position):
                continue
            tick = (word_position * 256 + bit_position) * tick_spacing
            if not V3_MIN_TICK <= tick <= V3_MAX_TICK:
                raise ValueError("Uniswap V3 depth replay tick evidence is invalid")
            tick_result = _required_pool_call(
                call_results,
                call_with_int(SELECTOR_TICKS, tick, 24),
                "initialized tick",
            )
            try:
                tick_words = _exact_abi_words(
                    tick_result,
                    8,
                    "initialized tick",
                )
                liquidity_gross = _exact_abi_uint(
                    tick_words[0], 128, "tick liquidityGross"
                )
                liquidity_net = _exact_abi_int(
                    tick_words[1], 128, "tick liquidityNet"
                )
                _exact_abi_int(
                    tick_words[4], 56, "tick tickCumulativeOutside"
                )
                _exact_abi_uint(
                    tick_words[5], 160, "tick secondsPerLiquidityOutsideX128"
                )
                _exact_abi_uint(
                    tick_words[6], 32, "tick secondsOutside"
                )
                initialized = _exact_abi_bool(
                    tick_words[7], "tick initialized"
                )
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "Uniswap V3 depth replay tick evidence is invalid"
                ) from error
            if (
                liquidity_gross <= 0
                or liquidity_gross > maximum_liquidity_per_tick
                or abs(liquidity_net) > liquidity_gross
                or not initialized
            ):
                raise ValueError("Uniswap V3 depth replay tick evidence is invalid")
            initialized_ticks[tick] = liquidity_net
            expected_tick_evidence.append(
                {
                    "word_position": word_position,
                    "bit_position": bit_position,
                    "tick": tick,
                    "liquidity_gross": liquidity_gross,
                    "liquidity_net": liquidity_net,
                }
            )
    expected_tick_calls = {
        call_with_int(SELECTOR_TICKS, tick, 24) for tick in initialized_ticks
    }
    actual_tick_calls = {
        data for data in call_results if data.startswith(SELECTOR_TICKS)
    }
    if (
        actual_tick_calls != expected_tick_calls
        or scan.get("tick_evidence") != expected_tick_evidence
    ):
        raise ValueError("Uniswap V3 depth replay tick evidence is invalid")

    band_amounts = {}
    for band in DEPTH_BANDS_BPS:
        down_target = exact_v3_price_limit_for_bps(
            sqrt_price_x96, band, zero_for_one=True
        )
        up_target = exact_v3_price_limit_for_bps(
            sqrt_price_x96, band, zero_for_one=False
        )
        zero_result = simulate_exact_v3_swap(
            sqrt_price_x96=sqrt_price_x96,
            current_tick=current_tick,
            liquidity=active_liquidity,
            fee_pips=fee_pips,
            initialized_ticks=initialized_ticks,
            amount_specified=(1 << 255) - 1,
            zero_for_one=True,
            sqrt_price_limit_x96=down_target,
        )
        one_result = simulate_exact_v3_swap(
            sqrt_price_x96=sqrt_price_x96,
            current_tick=current_tick,
            liquidity=active_liquidity,
            fee_pips=fee_pips,
            initialized_ticks=initialized_ticks,
            amount_specified=(1 << 255) - 1,
            zero_for_one=False,
            sqrt_price_limit_x96=up_target,
        )
        band_amounts[band] = {
            "zero_input": Decimal(zero_result.amount_in),
            "zero_output": Decimal(zero_result.amount_out),
            "one_input": Decimal(one_result.amount_in),
            "one_output": Decimal(one_result.amount_out),
            "zero_complete": zero_result.sqrt_price_x96 == down_target,
            "one_complete": one_result.sqrt_price_x96 == up_target,
        }
    if (
        str(depth_row.get("target_token_address") or "").strip().lower()
        != authority["token0_address"]
        or depth_row.get("target_token_position") != "token0"
        or str(depth_row.get("token0_decimals") or "")
        != str(authority["token0_decimals"])
        or str(depth_row.get("token1_decimals") or "")
        != str(authority["token1_decimals"])
        or str(depth_row.get("token0_price_usd") or "")
        != decimal_text(token0_price)
        or str(depth_row.get("token1_price_usd") or "")
        != decimal_text(token1_price)
    ):
        raise ValueError("Uniswap V3 exact depth replay authority is invalid")
    expected_fields = depth_fields(
        target_position_index=0,
        token0_decimals=authority["token0_decimals"],
        token1_decimals=authority["token1_decimals"],
        token0_price=token0_price,
        token1_price=token1_price,
        band_amounts=band_amounts,
    )
    if any(depth_row.get(field) != value for field, value in expected_fields.items()):
        raise ValueError("Uniswap V3 exact depth values do not match raw replay evidence")
    return expected_fields


def _execution_quantity_raw(row: Mapping[str, Any], field: str, decimals: int) -> int:
    if type(decimals) is not int or not 0 <= decimals <= 255:
        raise ValueError("Uniswap V3 execution token decimals are invalid")
    try:
        quantity = Decimal(str(row.get(field) or ""))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("Uniswap V3 execution quantity is invalid") from error
    if not quantity.is_finite() or quantity <= 0:
        raise ValueError("Uniswap V3 execution quantity is invalid")
    raw = quantity * (Decimal(10) ** decimals)
    integral = raw.to_integral_value()
    if raw != integral or integral >= 1 << 256:
        raise ValueError("Uniswap V3 execution quantity is not base-unit exact")
    return int(integral)


def _retained_finalized_block(
    records: Any,
    pinned_block: Mapping[str, Any],
) -> dict[str, str]:
    if not isinstance(records, list):
        raise ValueError("Uniswap V3 raw finalized block proof is missing")
    block_number = int(pinned_block["number"])
    expected = {
        "number": str(block_number),
        "hash": str(pinned_block["hash"]).lower(),
        "timestamp": str(pinned_block["timestamp"]),
    }
    pinned_identities = []
    finalized_checkpoints = []
    for record in records:
        if not isinstance(record, dict):
            continue
        requests = record.get("request")
        responses = record.get("response")
        request_items = requests if isinstance(requests, list) else [requests]
        response_items = responses if isinstance(responses, list) else [responses]
        responses_by_id = {
            response.get("id"): response
            for response in response_items
            if isinstance(response, dict) and type(response.get("id")) is int
        }
        for request in request_items:
            if (
                not isinstance(request, dict)
                or request.get("method") != "eth_getBlockByNumber"
            ):
                continue
            response = responses_by_id.get(request.get("id"))
            result = response.get("result") if isinstance(response, dict) else None
            if request.get("params") == [hex(block_number), False]:
                try:
                    pinned_identities.append(
                        exact_v3_block_identity(result, block_number)
                    )
                except (RpcError, TypeError, ValueError) as error:
                    raise ValueError(
                        "Uniswap V3 raw finalized block proof is invalid"
                    ) from error
            elif request.get("params") == ["finalized", False]:
                try:
                    checkpoint_number = int(
                        canonical_rpc_quantity(
                            result.get("number") if isinstance(result, dict) else None,
                            "Uniswap V3 finalized checkpoint number",
                        ),
                        16,
                    )
                    checkpoint_hash = str(
                        result.get("hash") if isinstance(result, dict) else ""
                    ).lower()
                    if _V3_BLOCK_HASH.fullmatch(checkpoint_hash) is None:
                        raise ValueError("Uniswap V3 finalized checkpoint hash is invalid")
                    block_timestamp_text(dict(result))
                except (RpcError, TypeError, ValueError) as error:
                    raise ValueError(
                        "Uniswap V3 raw finalized block proof is invalid"
                    ) from error
                finalized_checkpoints.append((checkpoint_number, checkpoint_hash))
    if (
        not pinned_identities
        or any(identity != expected for identity in pinned_identities)
        or not finalized_checkpoints
        or any(
            not checkpoint_number >= block_number
            or (
                checkpoint_number == block_number
                and checkpoint_hash != expected["hash"]
            )
            for checkpoint_number, checkpoint_hash in finalized_checkpoints
        )
    ):
        raise ValueError("Uniswap V3 raw finalized block proof is invalid")
    return expected


def require_uniswap_v3_publication_scope(
    inventory: Iterable[Mapping[str, Any]],
    *,
    market_id: str,
    merge_publish: bool,
    exact_validation_enabled: bool,
    publishing: bool,
) -> None:
    """Prevent authority pools from bypassing the two-market gate."""
    authority = load_uniswap_v3_execution_authority()
    target = str(market_id or "").strip()
    if merge_publish and target in authority:
        raise ValueError(
            "Uniswap V3 authority markets cannot use bounded merge-publication"
        )
    present = set()
    authority_pool_addresses = {
        record["pool_address"]: authority_market_id
        for authority_market_id, record in authority.items()
    }
    for row in inventory:
        pool_address = str(row.get("pool_address") or "").strip().lower()
        authority_market_id = authority_pool_addresses.get(pool_address)
        if authority_market_id is not None:
            present.add(authority_market_id)
    if publishing and not merge_publish and present and not exact_validation_enabled:
        raise ValueError(
            "Uniswap V3 authority publication requires exact validation"
        )


def validate_uniswap_v3_exact_candidate(
    inventory: list[dict[str, str]],
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    tvl_raw_root: Path,
    depth_raw_root: Path,
    authority_path: Path = V3_EXECUTION_AUTHORITY_PATH,
) -> dict[str, Any]:
    """Validate the exact two-pool candidate and return its public-safe receipt."""
    authority_bytes = _regular_evidence_bytes(
        authority_path, "Uniswap V3 execution authority"
    )
    authority = load_uniswap_v3_execution_authority(authority_path)
    market_ids = sorted(authority)
    authority_by_pool = {
        record["pool_address"]: (market_id, record)
        for market_id, record in authority.items()
    }

    inventory_by_market: dict[str, dict[str, str]] = {}
    for pool_address, (market_id, record) in authority_by_pool.items():
        matches = [
            row
            for row in inventory
            if str(row.get("pool_address") or "").strip().lower() == pool_address
        ]
        if len(matches) != 1:
            raise ValueError(
                "Uniswap V3 production inventory must contain both authority pools"
            )
        row = matches[0]
        expected_identity = {
            "chain": record["chain"],
            "dex": record["dex"],
            "pool_address": record["pool_address"],
        }
        actual_identity = {
            "chain": str(row.get("chain") or "").strip().lower(),
            "dex": str(row.get("dex") or "").strip().lower(),
            "pool_address": str(row.get("pool_address") or "").strip().lower(),
        }
        token_addresses = (
            address_from_token_id(str(row.get("base_token_id") or "")),
            address_from_token_id(str(row.get("quote_token_id") or "")),
        )
        if (
            actual_identity != expected_identity
            or str(row.get("token_symbol") or "").strip().upper()
            != market_id.rsplit(":", 1)[-1]
            or set(token_addresses)
            != {record["token0_address"], record["token1_address"]}
            or row.get("status") != "observed"
            or dex_market_id(dict(row)) != market_id
        ):
            raise ValueError("Uniswap V3 production inventory authority mismatch")
        require_v3_usd_price_lineage(
            snapshot_id=row.get("snapshot_id"),
            source=row.get("source"),
            endpoint=row.get("source_endpoint"),
            raw_sha256=row.get("raw_response_sha256"),
        )
        inventory_by_market[market_id] = row
    tvl_snapshot_ids = {
        str(row.get("snapshot_id") or "") for row in inventory_by_market.values()
    }
    if len(tvl_snapshot_ids) != 1 or "" in tvl_snapshot_ids:
        raise ValueError("Uniswap V3 production inventory TVL lineage is invalid")
    tvl_snapshot_id = next(iter(tvl_snapshot_ids))

    scoped_depth = [
        row for row in depth_rows if dex_market_id(row) in authority
    ]
    depth_by_market: dict[str, dict[str, str]] = {}
    for row in scoped_depth:
        market_id = dex_market_id(row)
        if market_id in depth_by_market:
            raise ValueError("Uniswap V3 exact depth inventory contains duplicates")
        depth_by_market[market_id] = row
    if set(depth_by_market) != set(market_ids):
        raise ValueError("Uniswap V3 exact depth inventory must contain 2/2 pools")
    for market_id, row in depth_by_market.items():
        record = authority[market_id]
        if (
            row.get("status") != "observed"
            or any(
                row.get(f"depth_{band}bps_complete") != "1"
                for band in DEPTH_BANDS_BPS
            )
            or str(row.get("token0_address") or "").lower()
            != record["token0_address"]
            or str(row.get("token1_address") or "").lower()
            != record["token1_address"]
            or decimal_text(record["fee_pips"] // 100)
            != str(row.get("fee_bps") or "")
        ):
            raise ValueError("Uniswap V3 exact depth must be complete and observed")
    depth_snapshot_ids = {
        str(row.get("snapshot_id") or "") for row in depth_by_market.values()
    }
    if len(depth_snapshot_ids) != 1 or "" in depth_snapshot_ids:
        raise ValueError("Uniswap V3 exact depth snapshot lineage is invalid")
    depth_snapshot_id = next(iter(depth_snapshot_ids))

    expected_scenarios = set(_exact_scenarios())
    scoped_execution = [
        row for row in execution_rows if str(row.get("market_id") or "") in authority
    ]
    execution_by_market: dict[str, list[dict[str, str]]] = {
        market_id: [] for market_id in market_ids
    }
    for row in scoped_execution:
        execution_by_market[str(row.get("market_id") or "")].append(row)
    for market_id, rows in execution_by_market.items():
        scenarios = [
            (
                str(row.get("direction") or ""),
                str(row.get("requested_notional_usd") or ""),
            )
            for row in rows
        ]
        if (
            len(rows) != len(expected_scenarios)
            or len(scenarios) != len(set(scenarios))
            or set(scenarios) != expected_scenarios
        ):
            raise ValueError("Uniswap V3 exact execution scenario inventory is invalid")
        if any(row.get("status") != "observed" for row in rows):
            raise ValueError("Uniswap V3 exact execution scenarios must be observed")
        if any(
            str(row.get("snapshot_id") or "") != depth_snapshot_id
            or str(row.get("source_snapshot_id") or "") != depth_snapshot_id
            for row in rows
        ):
            raise ValueError("Uniswap V3 exact execution snapshot lineage is invalid")

    tvl_directory = _snapshot_evidence_directory(
        Path(tvl_raw_root), tvl_snapshot_id, "TVL evidence"
    )
    depth_directory = _snapshot_evidence_directory(
        Path(depth_raw_root), depth_snapshot_id, "depth evidence"
    )
    tvl_manifest_bytes, tvl_manifest = _evidence_json(
        tvl_directory / "manifest.json", "TVL manifest"
    )
    depth_manifest_bytes, depth_manifest = _evidence_json(
        depth_directory / "manifest.json", "depth manifest"
    )
    expected_tvl_status_counts = {
        status: sum(row.get("status") == status for row in inventory)
        for status in ("observed", "missing", "not_found", "failed")
    }
    expected_tvl_reason_counts = dict(
        sorted(Counter(str(row.get("reason_code") or "") for row in inventory).items())
    )
    if (
        "" in expected_tvl_reason_counts
        or tvl_manifest.get("snapshot_id") != tvl_snapshot_id
        or tvl_manifest.get("pool_count") != len(inventory)
        or tvl_manifest.get("token_count")
        != len({row.get("token_symbol") for row in inventory})
        or tvl_manifest.get("chain_count")
        != len({row.get("chain") for row in inventory})
        or tvl_manifest.get("status_counts") != expected_tvl_status_counts
        or tvl_manifest.get("reason_code_counts") != expected_tvl_reason_counts
    ):
        raise ValueError("TVL manifest does not match the production inventory")
    if (
        depth_manifest.get("snapshot_id") != depth_snapshot_id
        or depth_manifest.get("pool_count") != len(depth_rows)
        or depth_manifest.get("execution_row_count") != len(execution_rows)
        or depth_manifest.get("status_counts")
        != dict(Counter(row.get("status") for row in depth_rows))
    ):
        raise ValueError("depth manifest does not match the candidate")
    tvl_raw_paths = _manifest_raw_paths(
        tvl_directory, tvl_manifest, label="TVL"
    )
    depth_raw_paths = _manifest_raw_paths(
        depth_directory, depth_manifest, label="depth"
    )
    tvl_payloads_by_hash = {}
    for path in tvl_raw_paths:
        raw, payload = _evidence_json(path, "GeckoTerminal raw response")
        tvl_payloads_by_hash[hashlib.sha256(raw).hexdigest()] = payload
    tvl_hashes = set(tvl_payloads_by_hash)
    inventory_tvl_hashes = {
        str(row.get("raw_response_sha256") or "") for row in inventory
    }
    if (
        not inventory_tvl_hashes
        or any(_SHA256.fullmatch(value) is None for value in inventory_tvl_hashes)
        or inventory_tvl_hashes != tvl_hashes
    ):
        raise ValueError(
            "retained GeckoTerminal raw files do not cover the production inventory"
        )
    scoped_tvl_hashes = {
        str(row.get("raw_response_sha256") or "")
        for row in inventory_by_market.values()
    }
    if not scoped_tvl_hashes or not scoped_tvl_hashes.issubset(tvl_hashes):
        raise ValueError("retained GeckoTerminal response does not match TVL rows")
    retained_prices_by_market: dict[str, dict[str, Decimal]] = {}
    for market_id, row in inventory_by_market.items():
        payload = tvl_payloads_by_hash[str(row["raw_response_sha256"])]
        data = payload.get("data")
        if not isinstance(data, list):
            raise ValueError("GeckoTerminal raw response identity is invalid")
        record = authority[market_id]
        matches = []
        for item in data:
            if not isinstance(item, dict):
                continue
            attributes = item.get("attributes")
            relationships = item.get("relationships")
            if not isinstance(attributes, dict) or not isinstance(relationships, dict):
                continue
            if str(attributes.get("address") or "").lower() == record["pool_address"]:
                matches.append((item, relationships))
        if len(matches) != 1:
            raise ValueError("GeckoTerminal authority pool identity is invalid")
        item, relationships = matches[0]
        attributes = item["attributes"]

        def relationship_id(name: str) -> str:
            relationship = relationships.get(name)
            if not isinstance(relationship, dict):
                return ""
            relationship_data = relationship.get("data")
            if not isinstance(relationship_data, dict):
                return ""
            return str(relationship_data.get("id") or "")

        if (
            item.get("type") != "pool"
            or str(item.get("id") or "").lower()
            != f"{record['chain']}_{record['pool_address']}"
            or relationship_id("dex") != record["dex"]
            or relationship_id("base_token").lower()
            != str(row.get("base_token_id") or "").lower()
            or relationship_id("quote_token").lower()
            != str(row.get("quote_token_id") or "").lower()
        ):
            raise ValueError("GeckoTerminal authority pool identity is invalid")
        try:
            retained_base_price = finite_decimal(
                attributes.get("base_token_price_usd"), positive=True
            )
            retained_quote_price = finite_decimal(
                attributes.get("quote_token_price_usd"), positive=True
            )
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ValueError(
                "GeckoTerminal retained USD price evidence is invalid"
            ) from error
        try:
            inventory_base_price = finite_decimal(
                row.get("base_token_price_usd"), positive=True
            )
            inventory_quote_price = finite_decimal(
                row.get("quote_token_price_usd"), positive=True
            )
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ValueError(
                "GeckoTerminal inventory USD price evidence is invalid"
            ) from error
        if (
            inventory_base_price != retained_base_price
            or inventory_quote_price != retained_quote_price
        ):
            raise ValueError(
                "GeckoTerminal retained USD price does not match inventory"
            )
        retained_prices = {
            address_from_token_id(str(row.get("base_token_id") or "")): (
                retained_base_price
            ),
            address_from_token_id(str(row.get("quote_token_id") or "")): (
                retained_quote_price
            ),
        }
        if set(retained_prices) != {
            record["token0_address"],
            record["token1_address"],
        }:
            raise ValueError("GeckoTerminal retained USD price identity is invalid")
        retained_prices_by_market[market_id] = retained_prices

    transcript_payloads: dict[str, tuple[Path, bytes, dict[str, Any]]] = {}
    for path in depth_raw_paths:
        raw, payload = _evidence_json(path, "depth pool transcript")
        scan = payload.get("v3_tick_scan_manifest")
        if not isinstance(scan, dict):
            continue
        market_id = str(scan.get("market_id") or "")
        if market_id not in authority:
            continue
        if market_id in transcript_payloads:
            raise ValueError("Uniswap V3 exact transcript coverage has duplicates")
        transcript_payloads[market_id] = (path, raw, payload)
    if set(transcript_payloads) != set(market_ids):
        raise ValueError("Uniswap V3 exact transcript coverage is incomplete")

    block_identities = set()
    pool_evidence = []
    for market_id in market_ids:
        _path, transcript_bytes, transcript = transcript_payloads[market_id]
        transcript_hash = hashlib.sha256(transcript_bytes).hexdigest()
        record = authority[market_id]
        pool = transcript.get("pool")
        scan = transcript.get("v3_tick_scan_manifest")
        usd = transcript.get("usd_price_evidence")
        if not isinstance(pool, dict) or not isinstance(scan, dict):
            raise ValueError("Uniswap V3 exact transcript identity is invalid")
        if {
            "token_symbol": pool.get("token_symbol"),
            "chain": pool.get("chain"),
            "dex": pool.get("dex"),
            "pool_address": str(pool.get("pool_address") or "").lower(),
        } != {
            "token_symbol": market_id.rsplit(":", 1)[-1],
            "chain": record["chain"],
            "dex": record["dex"],
            "pool_address": record["pool_address"],
        }:
            raise ValueError("Uniswap V3 exact transcript identity is invalid")
        if (
            scan.get("schema") != "uniswap_v3_tick_scan_manifest/v1"
            or scan.get("authority") != record
            or scan.get("market_id") != market_id
            or scan.get("chain_id") != hex(record["chain_id"])
            or str(scan.get("pool_address") or "").lower()
            != record["pool_address"]
            or scan.get("bitmap_word_radius") != record["bitmap_word_radius"]
            or not isinstance(scan.get("bitmap_words"), list)
            or not isinstance(scan.get("tick_evidence"), list)
            or not isinstance(scan.get("directions"), dict)
        ):
            raise ValueError("Uniswap V3 scan manifest authority is invalid")
        block = scan.get("block")
        if not isinstance(block, dict) or scan.get("block_final") != block:
            raise ValueError("Uniswap V3 shared finalized block evidence is invalid")
        block_number = str(block.get("number") or "")
        block_hash = str(block.get("hash") or "").lower()
        if (
            not block_number.isdigit()
            or _V3_BLOCK_HASH.fullmatch(block_hash) is None
            or not str(block.get("timestamp") or "")
            or scan.get("block_number") != int(block_number)
            or str(scan.get("block_hash") or "").lower() != block_hash
            or transcript.get("block_number") != int(block_number)
        ):
            raise ValueError("Uniswap V3 shared finalized block evidence is invalid")
        block_identities.add((int(block_number), block_hash))
        if _retained_finalized_block(transcript.get("records"), block) != block:
            raise ValueError("Uniswap V3 raw finalized block proof is inconsistent")
        depth_row = depth_by_market[market_id]
        market_execution = execution_by_market[market_id]
        if (
            str(depth_row.get("block_number") or "") != block_number
            or str(depth_row.get("raw_response_sha256") or "") != transcript_hash
            or any(
                str(row.get("block_number") or "") != block_number
                or str(row.get("raw_response_sha256") or "") != transcript_hash
                for row in market_execution
            )
        ):
            raise ValueError("Uniswap V3 transcript hash or block lineage is invalid")
        parity = scan.get("quoter_v2_parity")
        if not isinstance(parity, list):
            raise ValueError("Uniswap V3 Quoter parity evidence is missing")
        parity_scenarios = []
        parity_by_scenario = {}
        for item in parity:
            if (
                not isinstance(item, dict)
                or item.get("status") != "exact_match"
                or type(item.get("amount_raw")) is not int
                or type(item.get("sqrt_price_x96_after")) is not int
                or type(item.get("initialized_ticks_crossed")) is not int
                or type(item.get("gas_estimate_raw")) is not int
                or type(item.get("core_liquidity_boundaries_crossed")) is not int
                or item["amount_raw"] <= 0
                or not 0 <= item["sqrt_price_x96_after"] < 1 << 160
                or not 0 <= item["initialized_ticks_crossed"] < 1 << 32
                or item["gas_estimate_raw"] <= 0
                or item["core_liquidity_boundaries_crossed"] < 0
            ):
                raise ValueError("Uniswap V3 Quoter parity is not exact")
            scenario = (
                str(item.get("direction") or ""),
                str(item.get("requested_notional_usd") or ""),
            )
            parity_scenarios.append(scenario)
            parity_by_scenario[scenario] = item
        if (
            len(parity_scenarios) != len(expected_scenarios)
            or len(parity_scenarios) != len(set(parity_scenarios))
            or set(parity_scenarios) != expected_scenarios
        ):
            raise ValueError("Uniswap V3 Quoter scenario inventory is invalid")
        retained_quoter = _retained_quoter_results(
            transcript.get("records"),
            quoter_v2_address=record["quoter_v2_address"],
            block_number=int(block_number),
        )
        retained_by_call = {}
        for item in retained_quoter:
            call_identity = (
                item["direction"],
                item["token_in"],
                item["token_out"],
                item["amount"],
                item["fee_pips"],
                item["sqrt_price_limit_x96"],
            )
            if call_identity in retained_by_call:
                raise ValueError("Uniswap V3 raw Quoter request mapping is invalid")
            retained_by_call[call_identity] = item["response_words"]
        execution_by_scenario = {
            (
                str(row.get("direction") or ""),
                str(row.get("requested_notional_usd") or ""),
            ): row
            for row in market_execution
        }
        expected_calls = set()
        directions = scan["directions"]
        for scenario in sorted(expected_scenarios):
            direction, _notional = scenario
            execution_row = execution_by_scenario[scenario]
            target_address = str(
                execution_row.get("target_token_address") or ""
            ).lower()
            quote_address = str(
                execution_row.get("quote_token_address") or ""
            ).lower()
            if (
                target_address != record["token0_address"]
                or quote_address != record["token1_address"]
                or str(execution_row.get("target_token_decimals") or "")
                != str(record["token0_decimals"])
                or str(execution_row.get("quote_token_decimals") or "")
                != str(record["token1_decimals"])
                or decimal_text(execution_row.get("fee_rate_bps"))
                != decimal_text(Decimal(record["fee_pips"]) / Decimal(100))
            ):
                raise ValueError("Uniswap V3 execution authority fields are invalid")
            target_raw = _execution_quantity_raw(
                execution_row,
                "target_token_quantity",
                record["token0_decimals"],
            )
            if _execution_quantity_raw(
                execution_row,
                "filled_token_quantity",
                record["token0_decimals"],
            ) != target_raw:
                raise ValueError("Uniswap V3 observed execution fill is not exact")
            quote_raw = _execution_quantity_raw(
                execution_row,
                "quote_amount",
                record["token1_decimals"],
            )
            zero_for_one = direction == "sell_token"
            direction_name = "zero_for_one" if zero_for_one else "one_for_zero"
            direction_evidence = directions.get(direction_name)
            price_limit = (
                direction_evidence.get("price_limit_x96")
                if isinstance(direction_evidence, dict)
                else None
            )
            if type(price_limit) is not int or not 0 <= price_limit < 1 << 160:
                raise ValueError("Uniswap V3 Quoter price limit evidence is invalid")
            token_in, token_out = (
                (target_address, quote_address)
                if zero_for_one
                else (quote_address, target_address)
            )
            call_identity = (
                direction,
                token_in,
                token_out,
                target_raw,
                record["fee_pips"],
                price_limit,
            )
            expected_calls.add(call_identity)
            response_words = retained_by_call.get(call_identity)
            parity_item = parity_by_scenario[scenario]
            if response_words != (
                parity_item["amount_raw"],
                parity_item["sqrt_price_x96_after"],
                parity_item["initialized_ticks_crossed"],
                parity_item["gas_estimate_raw"],
            ) or response_words[0] != quote_raw:
                raise ValueError("Uniswap V3 raw Quoter parity is inconsistent")
        if set(retained_by_call) != expected_calls:
            raise ValueError("Uniswap V3 raw Quoter parity is inconsistent")
        inventory_row = inventory_by_market[market_id]
        if (
            not isinstance(usd, dict)
            or usd.get("source_snapshot_id") != tvl_snapshot_id
            or usd.get("source") != "GeckoTerminal API v2"
            or not str(usd.get("source_endpoint") or "").startswith(
                "https://api.geckoterminal.com/api/v2/"
            )
            or not str(usd.get("observed_at") or "")
            or usd.get("raw_response_sha256")
            != inventory_row.get("raw_response_sha256")
            or usd.get("base_token_id") != inventory_row.get("base_token_id")
            or usd.get("quote_token_id") != inventory_row.get("quote_token_id")
            or str(usd.get("raw_response_sha256") or "") not in tvl_hashes
        ):
            raise ValueError("Uniswap V3 GeckoTerminal transcript lineage is invalid")
        retained_prices = retained_prices_by_market[market_id]
        try:
            transcript_base_price = finite_decimal(
                usd.get("base_token_price_usd"), positive=True
            )
            transcript_quote_price = finite_decimal(
                usd.get("quote_token_price_usd"), positive=True
            )
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ValueError(
                "Uniswap V3 GeckoTerminal USD price evidence is invalid"
            ) from error
        transcript_prices = {
            address_from_token_id(str(usd.get("base_token_id") or "")): (
                transcript_base_price
            ),
            address_from_token_id(str(usd.get("quote_token_id") or "")): (
                transcript_quote_price
            ),
        }
        if (
            transcript_prices != retained_prices
        ):
            raise ValueError(
                "Uniswap V3 GeckoTerminal USD price evidence is inconsistent"
            )
        _replayed_exact_v3_depth_fields(
            transcript.get("records"),
            scan,
            record,
            depth_row,
            block_number=int(block_number),
            token0_price=retained_prices[record["token0_address"]],
            token1_price=retained_prices[record["token1_address"]],
        )
        parity_hash = hashlib.sha256(
            json.dumps(
                parity,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        pool_evidence.append(
            {
                "market_id": market_id,
                "transcript_sha256": transcript_hash,
                "geckoterminal_raw_response_sha256": str(
                    usd["raw_response_sha256"]
                ),
                "quoter_parity_sha256": parity_hash,
            }
        )
    if len(block_identities) != 1:
        raise ValueError("Uniswap V3 shared finalized block is inconsistent")
    shared_block_number, shared_block_hash = next(iter(block_identities))

    return {
        "schema": "uniswap_v3_exact_validation/v1",
        "authority_sha256": hashlib.sha256(authority_bytes).hexdigest(),
        "market_ids": market_ids,
        "tvl_snapshot_id": tvl_snapshot_id,
        "depth_snapshot_id": depth_snapshot_id,
        "shared_finalized_block": {
            "number": shared_block_number,
            "hash": shared_block_hash,
        },
        "depth_rows_sha256": publication_rows_sha256(
            scoped_depth, identity=dex_market_id
        ),
        "execution_rows_sha256": publication_rows_sha256(
            scoped_execution,
            identity=lambda row: (
                row.get("market_id"),
                row.get("direction"),
                row.get("requested_notional_usd"),
            ),
        ),
        "tvl_manifest_sha256": hashlib.sha256(tvl_manifest_bytes).hexdigest(),
        "depth_manifest_sha256": hashlib.sha256(depth_manifest_bytes).hexdigest(),
        "geckoterminal_raw_response_sha256": sorted(scoped_tvl_hashes),
        "pool_evidence": pool_evidence,
        "depth_observed_count": len(scoped_depth),
        "execution_observed_scenario_count": len(scoped_execution),
        "validated_scenario_inventory": {
            "directions": list(EXECUTION_DIRECTIONS),
            "notionals_usd": [decimal_text(value) for value in EXECUTION_NOTIONALS_USD],
            "scenario_count_per_market": len(expected_scenarios),
        },
    }


UNISWAP_V3_EXACT_RECEIPT_FIELDS = frozenset(
    {
        "schema",
        "authority_sha256",
        "market_ids",
        "tvl_snapshot_id",
        "depth_snapshot_id",
        "shared_finalized_block",
        "depth_rows_sha256",
        "execution_rows_sha256",
        "tvl_manifest_sha256",
        "depth_manifest_sha256",
        "geckoterminal_raw_response_sha256",
        "pool_evidence",
        "depth_observed_count",
        "execution_observed_scenario_count",
        "validated_scenario_inventory",
    }
)


def uniswap_v3_exact_receipt_bytes(receipt: Mapping[str, Any]) -> bytes:
    """Return the one canonical private/public representation of a receipt."""
    return (
        json.dumps(
            receipt,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def validate_uniswap_v3_exact_public_receipt(
    receipt: Mapping[str, Any],
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    authority_path: Path = V3_EXECUTION_AUTHORITY_PATH,
) -> dict[str, Any]:
    """Revalidate a raw-gate receipt against the exact public candidate rows."""
    if not isinstance(receipt, dict) or set(receipt) != set(
        UNISWAP_V3_EXACT_RECEIPT_FIELDS
    ):
        raise ValueError("Uniswap V3 exact validation receipt schema is invalid")
    if receipt.get("schema") != "uniswap_v3_exact_validation/v1":
        raise ValueError("Uniswap V3 exact validation receipt schema is invalid")

    authority_bytes = _regular_evidence_bytes(
        Path(authority_path), "Uniswap V3 execution authority"
    )
    authority = load_uniswap_v3_execution_authority(Path(authority_path))
    market_ids = sorted(authority)
    if (
        receipt.get("market_ids") != market_ids
        or receipt.get("authority_sha256")
        != hashlib.sha256(authority_bytes).hexdigest()
    ):
        raise ValueError("Uniswap V3 exact validation receipt authority is invalid")

    snapshot_ids = (
        receipt.get("tvl_snapshot_id"),
        receipt.get("depth_snapshot_id"),
    )
    if any(
        type(value) is not str or _EXACT_SNAPSHOT_ID.fullmatch(value) is None
        for value in snapshot_ids
    ):
        raise ValueError("Uniswap V3 exact validation receipt snapshot is invalid")

    hash_fields = (
        "authority_sha256",
        "depth_rows_sha256",
        "execution_rows_sha256",
        "tvl_manifest_sha256",
        "depth_manifest_sha256",
    )
    if any(
        type(receipt.get(field)) is not str
        or _SHA256.fullmatch(str(receipt.get(field))) is None
        for field in hash_fields
    ):
        raise ValueError("Uniswap V3 exact validation receipt hash is invalid")

    block = receipt.get("shared_finalized_block")
    if (
        not isinstance(block, dict)
        or set(block) != {"number", "hash"}
        or type(block.get("number")) is not int
        or block["number"] <= 0
        or type(block.get("hash")) is not str
        or _V3_BLOCK_HASH.fullmatch(block["hash"]) is None
    ):
        raise ValueError("Uniswap V3 exact validation receipt block is invalid")

    scoped_depth = [
        row for row in depth_rows if dex_market_id(row) in authority
    ]
    depth_by_market = {}
    for row in scoped_depth:
        market_id = dex_market_id(row)
        if market_id in depth_by_market:
            raise ValueError("Uniswap V3 exact public depth contains duplicates")
        depth_by_market[market_id] = row
    if set(depth_by_market) != set(market_ids):
        raise ValueError("Uniswap V3 exact public depth must contain 2/2 markets")
    for market_id, row in depth_by_market.items():
        record = authority[market_id]
        if (
            str(row.get("token0_address") or "").strip().lower()
            != record["token0_address"]
            or str(row.get("token1_address") or "").strip().lower()
            != record["token1_address"]
            or decimal_text(row.get("fee_bps"))
            != decimal_text(Decimal(record["fee_pips"]) / Decimal(100))
        ):
            raise ValueError("Uniswap V3 exact public depth authority is invalid")
    if any(
        row.get("status") != "observed"
        or str(row.get("snapshot_id") or "") != receipt["depth_snapshot_id"]
        or str(row.get("block_number") or "") != str(block["number"])
        or any(
            row.get("depth_{}bps_complete".format(band)) != "1"
            for band in DEPTH_BANDS_BPS
        )
        for row in scoped_depth
    ):
        raise ValueError("Uniswap V3 exact public depth is invalid")
    if (
        receipt.get("depth_observed_count") != len(scoped_depth)
        or len(scoped_depth) != len(market_ids)
        or receipt.get("depth_rows_sha256")
        != publication_rows_sha256(scoped_depth, identity=dex_market_id)
    ):
        raise ValueError("Uniswap V3 exact validation receipt depth hash is invalid")

    scoped_execution = [
        row
        for row in execution_rows
        if str(row.get("market_id") or "") in authority
    ]
    expected_scenarios = set(_exact_scenarios())
    execution_by_market = {market_id: [] for market_id in market_ids}
    for row in scoped_execution:
        execution_by_market[str(row.get("market_id") or "")].append(row)
    for market_id, rows in execution_by_market.items():
        record = authority[market_id]
        scenarios = [
            (
                str(row.get("direction") or ""),
                str(row.get("requested_notional_usd") or ""),
            )
            for row in rows
        ]
        if (
            len(rows) != len(expected_scenarios)
            or len(scenarios) != len(set(scenarios))
            or set(scenarios) != expected_scenarios
            or any(
                row.get("status") != "observed"
                or str(row.get("snapshot_id") or "")
                != receipt["depth_snapshot_id"]
                or str(row.get("source_snapshot_id") or "")
                != receipt["depth_snapshot_id"]
                or str(row.get("block_number") or "") != str(block["number"])
                for row in rows
            )
            or any(
                str(row.get("target_token_address") or "").strip().lower()
                != record["token0_address"]
                or str(row.get("quote_token_address") or "").strip().lower()
                != record["token1_address"]
                or str(row.get("target_token_decimals") or "")
                != str(record["token0_decimals"])
                or str(row.get("quote_token_decimals") or "")
                != str(record["token1_decimals"])
                or decimal_text(row.get("fee_rate_bps"))
                != decimal_text(Decimal(record["fee_pips"]) / Decimal(100))
                for row in rows
            )
        ):
            raise ValueError(
                "Uniswap V3 exact public execution scenario inventory is invalid"
            )
    if (
        receipt.get("execution_observed_scenario_count")
        != len(scoped_execution)
        or len(scoped_execution) != len(market_ids) * len(expected_scenarios)
        or receipt.get("execution_rows_sha256")
        != publication_rows_sha256(
            scoped_execution,
            identity=lambda row: (
                row.get("market_id"),
                row.get("direction"),
                row.get("requested_notional_usd"),
            ),
        )
    ):
        raise ValueError(
            "Uniswap V3 exact validation receipt execution hash is invalid"
        )

    scenario_inventory = receipt.get("validated_scenario_inventory")
    if scenario_inventory != {
        "directions": list(EXECUTION_DIRECTIONS),
        "notionals_usd": [
            decimal_text(value) for value in EXECUTION_NOTIONALS_USD
        ],
        "scenario_count_per_market": len(expected_scenarios),
    }:
        raise ValueError("Uniswap V3 exact validation receipt scenarios are invalid")

    gecko_hashes = receipt.get("geckoterminal_raw_response_sha256")
    if (
        not isinstance(gecko_hashes, list)
        or not gecko_hashes
        or gecko_hashes != sorted(set(gecko_hashes))
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in gecko_hashes
        )
    ):
        raise ValueError("Uniswap V3 exact validation receipt USD hashes are invalid")
    pool_evidence = receipt.get("pool_evidence")
    if (
        not isinstance(pool_evidence, list)
        or len(pool_evidence) != len(market_ids)
        or [item.get("market_id") for item in pool_evidence if isinstance(item, dict)]
        != market_ids
    ):
        raise ValueError("Uniswap V3 exact validation receipt pool evidence is invalid")
    referenced_gecko_hashes = set()
    for item in pool_evidence:
        if (
            not isinstance(item, dict)
            or set(item)
            != {
                "market_id",
                "transcript_sha256",
                "geckoterminal_raw_response_sha256",
                "quoter_parity_sha256",
            }
            or any(
                type(item.get(field)) is not str
                or _SHA256.fullmatch(str(item.get(field))) is None
                for field in (
                    "transcript_sha256",
                    "geckoterminal_raw_response_sha256",
                    "quoter_parity_sha256",
                )
            )
            or item["geckoterminal_raw_response_sha256"] not in gecko_hashes
        ):
            raise ValueError(
                "Uniswap V3 exact validation receipt pool evidence is invalid"
            )
        referenced_gecko_hashes.add(item["geckoterminal_raw_response_sha256"])
        market_id = item["market_id"]
        transcript_hash = item["transcript_sha256"]
        if (
            str(depth_by_market[market_id].get("raw_response_sha256") or "")
            != transcript_hash
            or any(
                str(row.get("raw_response_sha256") or "") != transcript_hash
                for row in execution_by_market[market_id]
            )
        ):
            raise ValueError(
                "Uniswap V3 exact validation receipt transcript hash is invalid"
            )
    if referenced_gecko_hashes != set(gecko_hashes):
        raise ValueError("Uniswap V3 exact validation receipt USD hashes are invalid")

    return dict(receipt)


def read_uniswap_v3_exact_raw_receipt_bytes(
    depth_raw_root: Path,
    snapshot_id: str,
) -> bytes:
    """Safely read the retained private receipt for one exact snapshot."""
    directory = _snapshot_evidence_directory(
        Path(depth_raw_root), snapshot_id, "depth evidence"
    )
    return _regular_evidence_bytes(
        directory / UNISWAP_V3_EXACT_RAW_RECEIPT_FILENAME,
        "Uniswap V3 exact raw receipt",
    )


def write_uniswap_v3_exact_raw_receipt(
    depth_raw_root: Path,
    receipt: Mapping[str, Any],
) -> Path:
    """Durably install the private receipt under its retained depth snapshot."""
    snapshot_id = str(receipt.get("depth_snapshot_id") or "")
    directory = _snapshot_evidence_directory(
        Path(depth_raw_root), snapshot_id, "depth evidence"
    )
    path = directory / UNISWAP_V3_EXACT_RAW_RECEIPT_FILENAME
    payload = uniswap_v3_exact_receipt_bytes(receipt)
    try:
        existing = _regular_evidence_bytes(path, "Uniswap V3 exact raw receipt")
    except ValueError:
        existing = None
    if existing is not None:
        if existing != payload:
            raise ValueError("Uniswap V3 exact raw receipt already differs")
        return path
    descriptor = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o644,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("Uniswap V3 exact receipt write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        directory_descriptor = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return path
    try:
        os.fsync(directory_descriptor)
    except OSError:
        pass
    finally:
        os.close(directory_descriptor)
    return path

def block_timestamp_text(block: dict[str, Any]) -> str:
    from datetime import datetime, timezone

    raw = block.get("timestamp")
    if raw is None:
        raise ValueError("fixed block is missing timestamp")
    timestamp = int(raw, 16) if isinstance(raw, str) else int(raw)
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def exact_v3_block_identity(
    block: Mapping[str, Any],
    expected_block_number: int,
) -> dict[str, str]:
    if not isinstance(block, Mapping):
        raise ValueError("V3 fixed block header is unavailable")
    number = block.get("number")
    if isinstance(number, str):
        canonical_rpc_quantity(number, "V3 fixed block number")
        parsed_number = int(number, 16)
    elif type(number) is int and number >= 0:
        parsed_number = number
    else:
        raise ValueError("V3 fixed block number is unavailable")
    if parsed_number != expected_block_number:
        raise ValueError("V3 fixed block number changed during collection")
    block_hash = str(block.get("hash") or "").lower()
    if _V3_BLOCK_HASH.fullmatch(block_hash) is None:
        raise ValueError("V3 fixed block hash is unavailable")
    return {
        "number": str(parsed_number),
        "hash": block_hash,
        "timestamp": block_timestamp_text(dict(block)),
    }


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


def v3_execution_rows(
    pool: dict[str, str],
    *,
    client: RpcClient,
    block_tag: str,
    token0_address: str,
    token1_address: str,
    quoter_v2_address: str,
    common: dict[str, Any],
    target_position_index: int,
    token0_decimals: int,
    token1_decimals: int,
    token0_price: Decimal,
    token1_price: Decimal,
    sqrt_price_x96: int,
    current_tick: int,
    active_liquidity: int,
    fee_pips: int,
    tick_spacing: int,
    initialized_ticks: Mapping[int, int],
    scan_directions: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    """Calculate fixed-notional facts with the shared protocol-exact engine."""
    starting_ratio = _sqrt_human_token1_per_token0(
        Decimal(sqrt_price_x96),
        token0_decimals,
        token1_decimals,
    )
    reference_quote = _target_quote_ratio(
        starting_ratio,
        target_position_index,
    )
    quote_to_usd = (
        token1_price if target_position_index == 0 else token0_price
    )
    target_decimals = (
        token0_decimals if target_position_index == 0 else token1_decimals
    )
    target_scale = Decimal(10) ** target_decimals
    quote_scale = Decimal(10) ** (
        token1_decimals if target_position_index == 0 else token0_decimals
    )
    rows: list[dict[str, str]] = []
    parity_evidence: list[dict[str, Any]] = []
    for notional in EXECUTION_NOTIONALS_USD:
        target_raw_decimal, target_quantity = _quantized_target(
            notional,
            reference_quote,
            quote_to_usd,
            target_decimals,
        )
        target_raw = int(target_raw_decimal)
        for direction in EXECUTION_DIRECTIONS:
            if direction == "sell_token":
                zero_for_one = target_position_index == 0
                amount_specified = target_raw
            else:
                zero_for_one = target_position_index == 1
                amount_specified = -target_raw
            scan = scan_directions[
                "zero_for_one" if zero_for_one else "one_for_zero"
            ]
            result = simulate_exact_v3_swap(
                sqrt_price_x96=sqrt_price_x96,
                current_tick=current_tick,
                liquidity=active_liquidity,
                fee_pips=fee_pips,
                initialized_ticks=initialized_ticks,
                amount_specified=amount_specified,
                zero_for_one=zero_for_one,
                sqrt_price_limit_x96=int(scan["price_limit_x96"]),
            )
            filled_raw = (
                result.amount_in if direction == "sell_token" else result.amount_out
            )
            quote_raw = (
                result.amount_out if direction == "sell_token" else result.amount_in
            )
            status = "observed" if result.complete else "partial"
            if result.complete:
                token_in = token0_address if zero_for_one else token1_address
                token_out = token1_address if zero_for_one else token0_address
                quoter_result = client.eth_calls(
                    quoter_v2_address,
                    [
                        quoter_v2_single_call(
                            exact_input=direction == "sell_token",
                            token_in=token_in,
                            token_out=token_out,
                            amount=target_raw,
                            fee_pips=fee_pips,
                            sqrt_price_limit_x96=int(scan["price_limit_x96"]),
                        )
                    ],
                    block_tag,
                )[0]
                (
                    quoter_amount,
                    quoter_sqrt_after,
                    quoter_ticks_crossed,
                    quoter_gas_estimate,
                ) = decode_v3_quoter_result(quoter_result)
                if quoter_sqrt_after >= 1 << 160:
                    raise ValueError("QuoterV2 sqrtPriceX96After exceeds uint160")
                if quoter_ticks_crossed >= 1 << 32:
                    raise ValueError(
                        "QuoterV2 initializedTicksCrossed exceeds uint32"
                    )
                local_quoter_ticks_crossed = count_v3_initialized_ticks_crossed(
                    tick_before=current_tick,
                    tick_after=result.tick,
                    tick_spacing=tick_spacing,
                    initialized_ticks=initialized_ticks,
                )
                local_quote_amount = (
                    result.amount_out
                    if direction == "sell_token"
                    else result.amount_in
                )
                if (
                    quoter_amount != local_quote_amount
                    or quoter_sqrt_after != result.sqrt_price_x96
                    or quoter_ticks_crossed != local_quoter_ticks_crossed
                ):
                    raise ValueError(
                        "Uniswap V3 exact engine does not match same-block "
                        "QuoterV2:"
                        f"amount={local_quote_amount}/{quoter_amount}:"
                        f"sqrt={result.sqrt_price_x96}/{quoter_sqrt_after}:"
                        "initialized_ticks_crossed="
                        f"{local_quoter_ticks_crossed}/{quoter_ticks_crossed}"
                    )
                parity_evidence.append(
                    {
                        "direction": direction,
                        "requested_notional_usd": decimal_text(notional),
                        "status": "exact_match",
                        "amount_raw": quoter_amount,
                        "sqrt_price_x96_after": quoter_sqrt_after,
                        "initialized_ticks_crossed": quoter_ticks_crossed,
                        "gas_estimate_raw": quoter_gas_estimate,
                        "core_liquidity_boundaries_crossed": (
                            result.initialized_ticks_crossed
                        ),
                    }
                )
            else:
                parity_evidence.append(
                    {
                        "direction": direction,
                        "requested_notional_usd": decimal_text(notional),
                        "status": "not_checked_partial_scan",
                    }
                )
            ending_ratio = _sqrt_human_token1_per_token0(
                Decimal(result.sqrt_price_x96),
                token0_decimals,
                token1_decimals,
            )
            rows.append(
                execution_fact_row(
                    common=common,
                    direction=direction,
                    requested_notional_usd=notional,
                    status=status,
                    status_reason=(
                        "full_target_quantity_filled"
                        if result.complete
                        else "source_tick_scan_limit"
                    ),
                    reference_price_quote_per_token=reference_quote,
                    quote_to_usd=quote_to_usd,
                    target_token_quantity=target_quantity,
                    filled_token_quantity=(
                        Decimal(filled_raw) / target_scale
                        if filled_raw > 0
                        else None
                    ),
                    quote_amount=(
                        Decimal(quote_raw) / quote_scale
                        if quote_raw > 0
                        else None
                    ),
                    levels_or_ticks_consumed=result.steps,
                    ending_marginal_price_quote_per_token=_target_quote_ratio(
                        ending_ratio,
                        target_position_index,
                    ),
                )
            )
    return rows, parity_evidence


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
    expected_v3_block_identity: Mapping[str, str] | None = None,
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
    exact_v3_enabled = (
        protocol == "concentrated_liquidity_v3"
        and is_uniswap_v3_execution_approved(pool)
    )
    exact_authority: dict[str, Any] | None = None
    exact_block_before: dict[str, str] | None = None
    exact_chain_id: int | None = None
    exact_factory: str | None = None
    exact_state_window: dict[str, Any] | None = None

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
        if exact_v3_enabled:
            exact_block_before = exact_v3_block_identity(
                client.block(block_tag),
                block_number,
            )
            if (
                expected_v3_block_identity is not None
                and dict(expected_v3_block_identity) != exact_block_before
            ):
                raise ValueError(
                    "V3 finalized block identity does not match state block"
                )
            chain_id_quantity = client.chain_id()
            exact_chain_id = int(
                canonical_rpc_quantity(chain_id_quantity, "V3 chain id"),
                16,
            )
        state_selectors = [
            SELECTOR_TOKEN0,
            SELECTOR_TOKEN1,
            SELECTOR_SLOT0,
            SELECTOR_LIQUIDITY,
            SELECTOR_FEE,
            SELECTOR_TICK_SPACING,
        ]
        if exact_v3_enabled:
            state_selectors.append(SELECTOR_FACTORY)
        state_results = client.eth_calls(
            pool_address,
            state_selectors,
            block_tag,
        )
        (
            token0_result,
            token1_result,
            slot0_result,
            liquidity_result,
            fee_result,
            spacing_result,
        ) = state_results[:6]
        if exact_v3_enabled:
            exact_factory = decode_address(state_results[6])
        token0 = decode_address(token0_result)
        token1 = decode_address(token1_result)
        sqrt_price_x96 = decode_uint(slot0_result, 0)
        current_tick = decode_int(slot0_result, 1, 24)
        active_liquidity = decode_uint(liquidity_result)
        fee_pips = decode_uint(fee_result)
        fee_bps = Decimal(fee_pips) / Decimal(100)
        tick_spacing = decode_int(spacing_result, 0, 24)
        if not V3_MIN_SQRT_RATIO <= sqrt_price_x96 < V3_MAX_SQRT_RATIO:
            raise ValueError("V3 pool sqrt price is outside protocol bounds")
        if not V3_MIN_TICK <= current_tick <= V3_MAX_TICK:
            raise ValueError("V3 pool current tick is outside protocol bounds")
        if not exact_v3_enabled and active_liquidity <= 0:
            raise ValueError("V3 pool is uninitialized or has zero active liquidity")
        initialized_ticks = (
            {}
            if exact_v3_enabled
            else collect_initialized_ticks(
                client,
                pool_address,
                block_tag,
                current_tick,
                tick_spacing,
            )
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

    if exact_v3_enabled:
        assert exact_factory is not None
        assert exact_chain_id is not None
        assert sqrt_price_x96 is not None
        assert current_tick is not None
        assert active_liquidity is not None
        factory_pool_result = client.eth_calls(
            exact_factory,
            [factory_get_pool_call(token0, token1, fee_pips)],
            block_tag,
        )[0]
        factory_pool_address = decode_address(factory_pool_result)
        exact_authority = match_uniswap_v3_execution_authority(
            pool,
            {
                "chain_id": exact_chain_id,
                "pool_address": pool_address,
                "factory_address": exact_factory,
                "factory_get_pool_address": factory_pool_address,
                "token0_address": token0,
                "token0_decimals": token0_decimals,
                "token1_address": token1,
                "token1_decimals": token1_decimals,
                "fee_pips": fee_pips,
                "tick_spacing": tick_spacing,
            },
        )
        if exact_authority is None:
            raise ValueError("approved V3 market lost its authority binding")
        exact_raw_ratio = _sqrt_human_token1_per_token0(
            Decimal(sqrt_price_x96),
            token0_decimals,
            token1_decimals,
        )
        exact_reference_quote = _target_quote_ratio(
            exact_raw_ratio,
            target_index,
        )
        exact_quote_to_usd = (
            token1_price if target_index == 0 else token0_price
        )
        maximum_target_raw, _maximum_target_quantity = _quantized_target(
            max(EXECUTION_NOTIONALS_USD),
            exact_reference_quote,
            exact_quote_to_usd,
            token0_decimals if target_index == 0 else token1_decimals,
        )
        if target_index == 0:
            zero_amount_specified = int(maximum_target_raw)
            one_amount_specified = -int(maximum_target_raw)
        else:
            zero_amount_specified = -int(maximum_target_raw)
            one_amount_specified = int(maximum_target_raw)
        exact_state_window = collect_exact_v3_state_window(
            client,
            pool_address,
            block_tag,
            sqrt_price_x96=sqrt_price_x96,
            current_tick=current_tick,
            active_liquidity=active_liquidity,
            fee_pips=fee_pips,
            tick_spacing=tick_spacing,
            zero_for_one_amount_specified=zero_amount_specified,
            one_for_zero_amount_specified=one_amount_specified,
            bitmap_word_radius=int(exact_authority["bitmap_word_radius"]),
        )
        initialized_ticks = exact_state_window["initialized_ticks"]
        exact_block_after = exact_v3_block_identity(
            client.block(block_tag),
            block_number,
        )
        if exact_block_before != exact_block_after:
            raise ValueError("V3 fixed block identity changed during collection")
        if exact_block_after["timestamp"] != block_timestamp:
            raise ValueError("V3 fixed block timestamp does not match cohort")

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
        if exact_v3_enabled:
            exact_depth_input = (1 << 255) - 1
            for band in DEPTH_BANDS_BPS:
                down_target = exact_v3_price_limit_for_bps(
                    sqrt_price_x96,
                    band,
                    zero_for_one=True,
                )
                up_target = exact_v3_price_limit_for_bps(
                    sqrt_price_x96,
                    band,
                    zero_for_one=False,
                )
                zero_result = simulate_exact_v3_swap(
                    sqrt_price_x96=sqrt_price_x96,
                    current_tick=current_tick,
                    liquidity=active_liquidity,
                    fee_pips=fee_pips,
                    initialized_ticks=initialized_ticks,
                    amount_specified=exact_depth_input,
                    zero_for_one=True,
                    sqrt_price_limit_x96=down_target,
                )
                one_result = simulate_exact_v3_swap(
                    sqrt_price_x96=sqrt_price_x96,
                    current_tick=current_tick,
                    liquidity=active_liquidity,
                    fee_pips=fee_pips,
                    initialized_ticks=initialized_ticks,
                    amount_specified=exact_depth_input,
                    zero_for_one=False,
                    sqrt_price_limit_x96=up_target,
                )
                band_amounts[band] = {
                    "zero_input": Decimal(zero_result.amount_in),
                    "zero_output": Decimal(zero_result.amount_out),
                    "one_input": Decimal(one_result.amount_in),
                    "one_output": Decimal(one_result.amount_out),
                    "zero_complete": zero_result.sqrt_price_x96 == down_target,
                    "one_complete": one_result.sqrt_price_x96 == up_target,
                }
            raw_ratio = _sqrt_human_token1_per_token0(
                Decimal(sqrt_price_x96),
                token0_decimals,
                token1_decimals,
            )
        else:
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
    if exact_v3_enabled:
        assert exact_state_window is not None
        assert exact_authority is not None
        assert exact_block_before is not None
        row["_v3_tick_scan_manifest"] = {
            "schema": "uniswap_v3_tick_scan_manifest/v1",
            "market_id": exact_authority["market_id"],
            "chain_id": hex(exact_chain_id),
            "block_number": block_number,
            "block_hash": exact_block_before["hash"],
            "pool_address": pool_address,
            "authority": exact_authority,
            "block": exact_block_before,
            "bitmap_words": [
                {"word_position": word_position}
                for word_position in exact_state_window["bitmap_words"]
            ],
            "tick_evidence": exact_state_window["tick_evidence"],
            "directions": exact_state_window["directions"],
            "bitmap_word_radius": exact_state_window["bitmap_word_radius"],
        }
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
    row["reason_code"] = (
        "observed" if row["status"] == "observed" else "measurement_limit"
    )
    response_received_at = row["response_received_at"]
    if protocol == "concentrated_liquidity_v3" and not exact_v3_enabled:
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
        if exact_v3_enabled:
            assert sqrt_price_x96 is not None
            assert current_tick is not None
            assert active_liquidity is not None
            assert exact_state_window is not None
            assert exact_authority is not None
            execution_rows, parity_evidence = v3_execution_rows(
                pool,
                client=client,
                block_tag=block_tag,
                token0_address=token0,
                token1_address=token1,
                quoter_v2_address=exact_authority["quoter_v2_address"],
                common=common,
                target_position_index=target_index,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                token0_price=token0_price,
                token1_price=token1_price,
                sqrt_price_x96=sqrt_price_x96,
                current_tick=current_tick,
                active_liquidity=active_liquidity,
                fee_pips=fee_pips,
                tick_spacing=tick_spacing,
                initialized_ticks=initialized_ticks,
                scan_directions=exact_state_window["directions"],
            )
            row["_v3_tick_scan_manifest"]["quoter_v2_parity"] = parity_evidence
        else:
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
        if exact_v3_enabled and isinstance(
            row.get("_v3_tick_scan_manifest"), dict
        ):
            row["_v3_tick_scan_manifest"]["execution_error"] = (
                f"{type(execution_error).__name__}: {execution_error}"
            )
    if exact_v3_enabled:
        assert exact_block_before is not None
        exact_block_final = exact_v3_block_identity(
            client.block(block_tag),
            block_number,
        )
        if exact_block_before != exact_block_final:
            raise ValueError("V3 fixed block identity changed during collection")
        if exact_block_final["timestamp"] != block_timestamp:
            raise ValueError("V3 fixed block timestamp does not match cohort")
        if isinstance(row.get("_v3_tick_scan_manifest"), dict):
            row["_v3_tick_scan_manifest"]["block_final"] = exact_block_final
        final_response_received_at = utc_now_text()
        row["observed_at"] = final_response_received_at
        row["response_received_at"] = final_response_received_at
        for execution_row in execution_rows:
            execution_row["observed_at"] = final_response_received_at
            execution_row["response_received_at"] = final_response_received_at
    return row, execution_rows


def raw_transcript_bytes(
    *,
    pool: dict[str, str],
    block_number: int | None,
    endpoint: str,
    records: list[dict[str, Any]],
    error: Exception | None = None,
    v3_tick_scan_manifest: Mapping[str, Any] | None = None,
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
    if any(
        str(pool.get(field) or "").strip()
        for field in ("source", "source_endpoint", "raw_response_sha256")
    ):
        payload["usd_price_evidence"] = {
            "source_snapshot_id": pool.get("snapshot_id", ""),
            "observed_at": (
                pool.get("response_received_at")
                or pool.get("observed_at")
                or ""
            ),
            "source": pool.get("source", ""),
            "source_endpoint": pool.get("source_endpoint", ""),
            "raw_response_sha256": pool.get("raw_response_sha256", ""),
            "base_token_id": pool.get("base_token_id", ""),
            "quote_token_id": pool.get("quote_token_id", ""),
            "base_token_price_usd": pool.get("base_token_price_usd", ""),
            "quote_token_price_usd": pool.get("quote_token_price_usd", ""),
        }
    if v3_tick_scan_manifest is not None:
        payload["v3_tick_scan_manifest"] = dict(v3_tick_scan_manifest)
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
    expected_v3_block_identity: Mapping[str, str] | None = None,
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
        exact_v3_approved = (
            protocol == "concentrated_liquidity_v3"
            and is_uniswap_v3_execution_approved(pool)
        )
        if block_number is None:
            if exact_v3_approved:
                finalized_block = active_client.block("finalized")
                finalized_number = finalized_block.get("number")
                if not isinstance(finalized_number, str):
                    raise ValueError("finalized Ethereum block is unavailable")
                block_number = int(
                    canonical_rpc_quantity(
                        finalized_number,
                        "finalized Ethereum block number",
                    ),
                    16,
                )
                block_timestamp = block_timestamp_text(finalized_block)
                finalized_identity = exact_v3_block_identity(
                    finalized_block,
                    block_number,
                )
                if (
                    expected_v3_block_identity is not None
                    and dict(expected_v3_block_identity) != finalized_identity
                ):
                    raise ValueError("V3 finalized block authority changed")
                expected_v3_block_identity = finalized_identity
            else:
                block_number = active_client.block_number()
        elif exact_v3_approved:
            finalized_head = active_client.block("finalized")
            finalized_number_raw = finalized_head.get("number")
            if not isinstance(finalized_number_raw, str):
                raise ValueError("finalized Ethereum block is unavailable")
            finalized_number = int(
                canonical_rpc_quantity(
                    finalized_number_raw,
                    "finalized Ethereum block number",
                ),
                16,
            )
            if block_number > finalized_number:
                raise ValueError("V3 fixed block is not finalized")
            if block_number == finalized_number:
                finalized_identity = exact_v3_block_identity(
                    finalized_head,
                    finalized_number,
                )
                if (
                    expected_v3_block_identity is not None
                    and dict(expected_v3_block_identity) != finalized_identity
                ):
                    raise ValueError("V3 finalized block authority changed")
                expected_v3_block_identity = finalized_identity
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
            expected_v3_block_identity=expected_v3_block_identity,
        )
        v3_tick_scan_manifest = row.get("_v3_tick_scan_manifest")
        transcript = raw_transcript_bytes(
            pool=pool,
            block_number=block_number,
            endpoint=active_client.endpoint,
            records=active_client.records[record_start:],
            v3_tick_scan_manifest=(
                v3_tick_scan_manifest
                if isinstance(v3_tick_scan_manifest, Mapping)
                else None
            ),
        )
        raw_path.write_bytes(transcript)
        raw_hash = hashlib.sha256(transcript).hexdigest()
        row["raw_response_sha256"] = raw_hash
        for execution_row in pool_execution_rows:
            execution_row["raw_response_sha256"] = raw_hash
        row.pop("_v3_tick_scan_manifest", None)
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
                "reason_code": dex_depth_failure_reason_code(error),
                "error": f"{type(error).__name__}: {error}",
            }
        )
        status_reason = (
            "usd_price_conversion_stale_or_unavailable"
            if isinstance(error, UsdPriceTimeMismatch)
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
    block_identities: dict[str, dict[str, str]] = {}
    finalized_chains = {
        pool["chain"].lower()
        for pool in pools
        if protocol_model(
            pool["dex"],
            pool["chain"],
            pool["pool_address"],
        )[0] == "concentrated_liquidity_v3"
        and is_uniswap_v3_execution_approved(pool)
    }
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
        if block_number is None and chain in finalized_chains:
            finalized_header = client.block("finalized")
            finalized_number_raw = finalized_header.get("number")
            if not isinstance(finalized_number_raw, str):
                raise ValueError("finalized Ethereum block is unavailable")
            block_number = int(
                canonical_rpc_quantity(
                    finalized_number_raw,
                    "finalized Ethereum block number",
                ),
                16,
            )
            block_timestamp = block_timestamp_text(finalized_header)
            block_identity = exact_v3_block_identity(
                finalized_header,
                block_number,
            )
            blocks[chain] = block_number
            block_timestamps[chain] = block_timestamp
            block_identities[chain] = block_identity
        row, pool_execution_rows = collect_dex_pool_observation(
            pool,
            snapshot_id=snapshot_id,
            raw_path=raw_path,
            rpc_factory=rpc_factory,
            client=client,
            fixed_block_number=block_number,
            fixed_block_timestamp=block_timestamp,
            expected_v3_block_identity=block_identities.get(chain),
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
        "reason_code_counts": dict(
            sorted(Counter(row["reason_code"] for row in rows).items())
        ),
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
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        supplied_reason = str(row.get("reason_code") or "").strip().lower()
        if "reason_code" in row and not supplied_reason:
            raise ValueError("DEX depth snapshot reason code is missing")
        if supplied_reason and dex_depth_reason_code(supplied_reason) is None:
            raise ValueError("DEX depth snapshot contains an invalid reason code")
        allowed_reasons = {
            "observed": {"observed"},
            "partial": {"measurement_limit"},
            "unsupported": set(DEX_DEPTH_UNSUPPORTED_REASON_CODES),
            "failed": set(DEX_DEPTH_COLLECTION_FAILURE_REASON_CODES),
        }[status]
        if supplied_reason and supplied_reason not in allowed_reasons:
            raise ValueError("DEX depth snapshot status and reason code conflict")
        if status == "failed":
            source_hash = str(row.get("raw_response_sha256") or "")
            if (
                len(source_hash) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in source_hash
                )
            ):
                raise ValueError(
                    "DEX depth snapshot failed row source hash is invalid"
                )
    if allow_terminal_only and any(
        quality_outcome_resolution_state(
            *normalize_dex_depth_source_outcome(
                row.get("status"),
                row.get("reason_code"),
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
                    row.get("reason_code"),
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


def migrate_legacy_dex_depth_reason_codes(
    rows: Iterable[dict[str, str]],
) -> list[dict[str, str]]:
    """Upgrade only keyless legacy rows to the explicit bounded reason schema."""
    source_rows = [dict(row) for row in rows]
    reason_presence = {"reason_code" in row for row in source_rows}
    if len(reason_presence) > 1:
        raise ValueError("mixed DEX depth reason_code schema is invalid")
    if reason_presence == {False}:
        for row in source_rows:
            status = str(row.get("status") or "").strip().lower()
            if status in {"observed", "complete"}:
                reason = "observed"
            elif status == "partial":
                reason = "measurement_limit"
            elif status == "unsupported":
                reason = dex_unsupported_reason_code(row.get("error", ""))
            elif status == "failed":
                reason = "collection_failed"
            else:
                raise ValueError("legacy DEX depth snapshot status is invalid")
            row["reason_code"] = reason
        return source_rows

    allowed_reasons_by_status = {
        "observed": {"observed"},
        "complete": {"observed"},
        "partial": {"measurement_limit"},
        "unsupported": set(DEX_DEPTH_UNSUPPORTED_REASON_CODES),
        "failed": set(DEX_DEPTH_COLLECTION_FAILURE_REASON_CODES),
    }
    for row in source_rows:
        status = str(row.get("status") or "").strip().lower()
        reason = str(row.get("reason_code") or "").strip().lower()
        if status not in allowed_reasons_by_status:
            raise ValueError("DEX depth snapshot status is invalid")
        if not reason:
            raise ValueError("DEX depth snapshot reason_code is missing")
        if (
            dex_depth_reason_code(reason) is None
            or reason not in allowed_reasons_by_status[status]
        ):
            raise ValueError(
                "DEX depth snapshot status and reason_code conflict"
            )
    return source_rows


def merge_exact_publication_bundle(
    depth_rows: list[dict[str, str]],
    execution_rows: list[dict[str, str]],
    *,
    target_market_id: str,
    publish_dir: Path,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Merge one collected DEX pool into the validated full publication."""
    require_uniswap_v3_publication_scope(
        (),
        market_id=target_market_id,
        merge_publish=True,
        exact_validation_enabled=False,
        publishing=True,
    )
    baseline_depth = migrate_legacy_dex_depth_reason_codes(
        read_csv_rows(publish_dir / LATEST_FILENAME)
    )
    depth_rows = migrate_legacy_dex_depth_reason_codes(depth_rows)
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
    baseline_rows = migrate_legacy_dex_depth_reason_codes(
        read_csv_rows(latest_path)
    )
    rows = migrate_legacy_dex_depth_reason_codes(rows)
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
                row.get("reason_code"),
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
    rows = migrate_legacy_dex_depth_reason_codes(rows)
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
    existing_history = migrate_legacy_dex_depth_reason_codes(
        read_csv_rows(history_path)
    )
    merged = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            *pool_key(row.get("chain", ""), row.get("pool_address", "")),
        ): row
        for row in existing_history
    }
    append_rows = migrate_legacy_dex_depth_reason_codes(
        rows if history_rows_to_append is None else history_rows_to_append
    )
    for row in append_rows:
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
    exact_validation_receipt: Mapping[str, Any] | None = None,
    authority_path: Path = V3_EXECUTION_AUTHORITY_PATH,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Failure-atomically publish one full DEX depth/execution cohort."""
    depth_rows = migrate_legacy_dex_depth_reason_codes(depth_rows)
    authority_market_ids = set(
        load_uniswap_v3_execution_authority(Path(authority_path))
    )
    contains_exact_scope = any(
        dex_market_id(row) in authority_market_ids for row in depth_rows
    )
    if contains_exact_scope and exact_validation_receipt is None:
        raise ValueError("Uniswap V3 exact validation receipt is required")
    validated_exact_receipt = None
    if exact_validation_receipt is not None:
        validated_exact_receipt = validate_uniswap_v3_exact_public_receipt(
            exact_validation_receipt,
            depth_rows,
            execution_rows,
            authority_path=Path(authority_path),
        )
    current_path = output_dir / CURRENT_FILENAME
    execution_current_path = output_dir / EXECUTION_CURRENT_FILENAME
    history_path = publish_dir / HISTORY_FILENAME
    latest_path = publish_dir / LATEST_FILENAME
    public_current_path = publish_dir / CURRENT_FILENAME
    execution_latest_path = publish_dir / EXECUTION_LATEST_FILENAME
    exact_receipt_path = publish_dir / UNISWAP_V3_EXACT_LATEST_FILENAME
    public_destinations = [
        history_path,
        latest_path,
        public_current_path,
        execution_latest_path,
    ]
    if validated_exact_receipt is not None:
        public_destinations.append(exact_receipt_path)
    _require_disjoint_publication_destinations(
        (current_path, execution_current_path),
        public_destinations,
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

    existing_history = migrate_legacy_dex_depth_reason_codes(
        read_csv_rows(history_path)
    )
    merged_history = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            *pool_key(row.get("chain", ""), row.get("pool_address", "")),
        ): row
        for row in existing_history
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
    public_bundle = [
        (history_path, csv_payload(DEX_DEPTH_COLUMNS, history_rows)),
        (latest_path, csv_payload(DEX_DEPTH_COLUMNS, depth_rows)),
        (public_current_path, csv_payload(DEX_DEPTH_COLUMNS, depth_rows)),
        (
            execution_latest_path,
            csv_payload(EXECUTION_COST_COLUMNS, execution_rows),
        ),
    ]
    if validated_exact_receipt is not None:
        public_bundle.append(
            (
                exact_receipt_path,
                uniswap_v3_exact_receipt_bytes(validated_exact_receipt),
            )
        )
    atomic_replace_bundle(public_bundle)
    return (
        {
            "current_path": str(current_path),
            "row_count": len(depth_rows),
            "latest_path": str(latest_path),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
            "publication_gate": depth_gate,
            "uniswap_v3_exact_receipt_path": (
                str(exact_receipt_path)
                if validated_exact_receipt is not None
                else None
            ),
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
    require_uniswap_v3_publication_scope(
        (),
        market_id=target_market_id,
        merge_publish=True,
        exact_validation_enabled=False,
        publishing=True,
    )
    depth_rows = migrate_legacy_dex_depth_reason_codes(depth_rows)
    history_rows_to_append = migrate_legacy_dex_depth_reason_codes(
        history_rows_to_append
    )
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

    existing_history = migrate_legacy_dex_depth_reason_codes(
        read_csv_rows(history_path)
    )
    merged_history = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            *pool_key(row.get("chain", ""), row.get("pool_address", "")),
        ): row
        for row in existing_history
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
    parser.add_argument(
        "--tvl-raw-root",
        type=Path,
        default=DEFAULT_TVL_RAW_ROOT,
        help="Retained GeckoTerminal evidence root for exact V3 validation",
    )
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
    parser.add_argument(
        "--require-uniswap-v3-exact-validation",
        action="store_true",
        help="Require the full two-authority-pool raw-evidence gate",
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
    if args.require_uniswap_v3_exact_validation and (
        market_id or tokens or chains or args.merge_publish
    ):
        raise ValueError(
            "exact Uniswap V3 validation requires an unfiltered full inventory"
        )
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
    require_uniswap_v3_publication_scope(
        pools,
        market_id=market_id,
        merge_publish=args.merge_publish,
        exact_validation_enabled=args.require_uniswap_v3_exact_validation,
        publishing=publish_dir is not None,
    )
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
    exact_validation_receipt = None
    if args.require_uniswap_v3_exact_validation:
        exact_validation_receipt = validate_uniswap_v3_exact_candidate(
            pools,
            rows,
            execution_rows,
            tvl_raw_root=args.tvl_raw_root,
            depth_raw_root=args.raw_root,
        )
        write_uniswap_v3_exact_raw_receipt(
            args.raw_root,
            exact_validation_receipt,
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
            exact_validation_receipt=exact_validation_receipt,
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
    if exact_validation_receipt is not None:
        result["uniswap_v3_exact_validation"] = exact_validation_receipt
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
