"""Fail-closed GeckoTerminal identity and pool discovery for Token onboarding."""

from __future__ import annotations

import json
import math
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

try:
    import certifi
except ImportError:  # pragma: no cover - system trust remains the safe fallback
    certifi = None

from scripts.token_registry import (
    TokenRegistryError,
    normalize_chain,
    normalize_contract_address,
    normalize_token_record,
    normalize_token_symbol,
)


GECKOTERMINAL_BASE_URL = "https://api.geckoterminal.com/api/v2"
MAX_DISCOVERED_POOLS = 8
MAX_SOURCE_RESPONSE_BYTES = 1024 * 1024
EVM_POOL_KEY_PATTERN = re.compile(r"^0x[0-9a-fA-F]{64}$")
EVM_CHAINS = {
    "eth",
    "arbitrum",
    "base",
    "optimism",
    "bsc",
    "avax",
    "zksync",
}
TLS_CONTEXT = (
    ssl.create_default_context(cafile=certifi.where())
    if certifi
    else ssl.create_default_context()
)


class TokenOnboardingError(ValueError):
    """Stable error contract for the future administrator HTTP layer."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = dict(details or {})

    def as_dict(self) -> Dict[str, Any]:
        payload = {
            "error": self.message,
            "error_code": self.code,
            "retryable": self.retryable,
        }
        if self.details:
            payload["details"] = self.details
        return payload


def _registry_error(error: TokenRegistryError) -> TokenOnboardingError:
    return TokenOnboardingError(
        error.code,
        error.message,
        retryable=False,
        details=error.details,
    )


def _source_error(error: BaseException) -> TokenOnboardingError:
    status_code = getattr(error, "code", None)
    if status_code == 404:
        return TokenOnboardingError(
            "source_not_found",
            "GeckoTerminal did not return the requested resource",
        )
    if status_code == 429:
        return TokenOnboardingError(
            "source_rate_limited",
            "GeckoTerminal rate limited the request",
            retryable=True,
        )
    if isinstance(status_code, int) and status_code >= 500:
        return TokenOnboardingError(
            "source_unavailable",
            "GeckoTerminal is temporarily unavailable",
            retryable=True,
        )
    if isinstance(error, (urllib.error.URLError, TimeoutError)):
        return TokenOnboardingError(
            "source_unavailable",
            "GeckoTerminal could not be reached",
            retryable=True,
        )
    return TokenOnboardingError(
        "source_unavailable",
        "GeckoTerminal request failed",
        retryable=True,
        details={"exception_type": type(error).__name__},
    )


def request_json(url: str) -> Dict[str, Any]:
    """Fetch one bounded GeckoTerminal JSON document over verified TLS."""
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "CEX-DEX-Market-Monitor/1.0",
        },
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=30,
            context=TLS_CONTEXT,
        ) as response:
            raw = response.read(MAX_SOURCE_RESPONSE_BYTES + 1)
            if len(raw) > MAX_SOURCE_RESPONSE_BYTES:
                raise TokenOnboardingError(
                    "source_invalid_response",
                    "GeckoTerminal response exceeded the allowed size",
                    retryable=True,
                )
    except TokenOnboardingError:
        raise
    except Exception as error:
        raise _source_error(error)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal returned invalid JSON",
            retryable=True,
        )
    if not isinstance(payload, dict):
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal response root must be an object",
            retryable=True,
        )
    return payload


def _fetch_payload(
    fetch_json: Callable[[str], Any],
    url: str,
    *,
    not_found_code: str,
    not_found_message: str,
) -> Dict[str, Any]:
    try:
        payload = fetch_json(url)
    except TokenOnboardingError as error:
        if error.code == "source_not_found":
            raise TokenOnboardingError(not_found_code, not_found_message)
        raise
    except Exception as error:
        mapped = _source_error(error)
        if mapped.code == "source_not_found":
            raise TokenOnboardingError(not_found_code, not_found_message)
        raise mapped
    if not isinstance(payload, dict):
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal response root must be an object",
            retryable=True,
        )
    return payload


def _source_token_id(
    chain: str,
    address: str,
    value: Any,
    *,
    field: str,
) -> Tuple[str, str]:
    token_id = str(value or "").strip()
    if "_" not in token_id:
        raise TokenOnboardingError(
            "source_invalid_response",
            "%s is not a chain-qualified token id" % field,
            retryable=True,
        )
    source_chain, source_address = token_id.split("_", 1)
    try:
        normalized_chain = normalize_chain(source_chain)
        normalized_address = normalize_contract_address(
            normalized_chain,
            source_address,
        )
    except TokenRegistryError as error:
        raise TokenOnboardingError(
            "source_invalid_response",
            "%s contains an invalid Token identity" % field,
            retryable=True,
            details={"reason": error.code},
        )
    if normalized_chain != chain or normalized_address != address:
        raise TokenOnboardingError(
            "identity_mismatch",
            "%s does not match the requested Token identity" % field,
            details={
                "expected_chain": chain,
                "expected_address": address,
                "source_token_id": token_id,
            },
        )
    return normalized_chain, normalized_address


def _bounded_number(value: Any, *, field: str) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise TokenOnboardingError(
            "source_invalid_response",
            "%s is not numeric" % field,
            retryable=True,
        )
    if not math.isfinite(number) or number < 0:
        raise TokenOnboardingError(
            "source_invalid_response",
            "%s must be a finite non-negative number" % field,
            retryable=True,
        )
    return number


def _relationship_id(pool: Mapping[str, Any], relationship: str) -> str:
    relationships = pool.get("relationships")
    if not isinstance(relationships, dict):
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool relationships must be an object",
            retryable=True,
        )
    relation = relationships.get(relationship)
    data = relation.get("data") if isinstance(relation, dict) else None
    relation_id = data.get("id") if isinstance(data, dict) else None
    if not isinstance(relation_id, str) or not relation_id.strip():
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool is missing %s identity" % relationship,
            retryable=True,
        )
    return relation_id.strip()


def _token_id_matches(chain: str, address: str, token_id: str) -> bool:
    if "_" not in token_id:
        return False
    source_chain, source_address = token_id.split("_", 1)
    try:
        return (
            normalize_chain(source_chain) == chain
            and normalize_contract_address(source_chain, source_address) == address
        )
    except TokenRegistryError:
        return False


def _normalize_pool_identifier(chain: str, value: Any) -> str:
    """Normalize a GeckoTerminal pool address or protocol-native pool key.

    Most EVM pools use a 20-byte contract address. Protocols such as
    Uniswap v4 expose a 32-byte pool key instead; GeckoTerminal uses that key
    as the pool resource identifier and accepts it in the OHLCV endpoint.
    """
    try:
        return normalize_contract_address(chain, value)
    except TokenRegistryError as error:
        raw_value = str(value or "").strip()
        if chain in EVM_CHAINS and EVM_POOL_KEY_PATTERN.fullmatch(raw_value):
            return raw_value.lower()
        raise error


def _pool_result(
    pool: Mapping[str, Any],
    *,
    chain: str,
    address: str,
) -> Dict[str, Any]:
    if not isinstance(pool, Mapping):
        raise TokenOnboardingError(
            "source_invalid_response",
            "Each GeckoTerminal pool must be an object",
            retryable=True,
        )
    attributes = pool.get("attributes")
    if not isinstance(attributes, dict):
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool attributes must be an object",
            retryable=True,
        )
    if pool.get("type") != "pool":
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal pool response is missing data.type=pool",
            retryable=True,
        )
    raw_pool_address = attributes.get("address")
    try:
        pool_address = _normalize_pool_identifier(chain, raw_pool_address)
    except TokenRegistryError as error:
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool identifier is invalid for the requested chain",
            retryable=True,
            details={"reason": error.code},
        )
    source_pool_id = str(pool.get("id") or "").strip()
    if "_" not in source_pool_id:
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool is missing a chain-qualified source identity",
            retryable=True,
        )
    source_chain, source_pool_address = source_pool_id.split("_", 1)
    try:
        normalized_source_chain = normalize_chain(source_chain)
        normalized_source_address = _normalize_pool_identifier(
            normalized_source_chain,
            source_pool_address,
        )
    except TokenRegistryError as error:
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool source identity is invalid",
            retryable=True,
            details={"reason": error.code},
        )
    if normalized_source_chain != chain or normalized_source_address != pool_address:
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool source identity does not match its attributes",
            retryable=True,
        )
    base_token_id = _relationship_id(pool, "base_token")
    quote_token_id = _relationship_id(pool, "quote_token")
    base_matches = _token_id_matches(chain, address, base_token_id)
    quote_matches = _token_id_matches(chain, address, quote_token_id)
    if base_matches == quote_matches:
        raise TokenOnboardingError(
            "pool_token_mismatch",
            "Pool does not contain the requested Token on exactly one side",
            details={
                "pool_address": pool_address,
                "base_token_id": base_token_id,
                "quote_token_id": quote_token_id,
            },
        )
    dex = _relationship_id(pool, "dex")
    pool_name = str(attributes.get("name") or "").strip()
    if not pool_name or len(pool_name) > 240:
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool name is missing or too long",
            retryable=True,
        )
    volume_payload = attributes.get("volume_usd")
    if volume_payload is None:
        volume_payload = {}
    if not isinstance(volume_payload, dict):
        raise TokenOnboardingError(
            "source_invalid_response",
            "Pool volume_usd must be an object",
            retryable=True,
        )
    return {
        "chain": chain,
        "dex": dex,
        "pool_address": pool_address,
        "pool_name": pool_name,
        "target_side": "base" if base_matches else "quote",
        "base_token_id": base_token_id,
        "quote_token_id": quote_token_id,
        "tvl_usd": _bounded_number(
            attributes.get("reserve_in_usd"),
            field="reserve_in_usd",
        ),
        "volume_24h_usd": _bounded_number(
            volume_payload.get("h24"),
            field="volume_usd.h24",
        ),
    }


def resolve_token_identity(
    chain: Any,
    contract_address: Any,
    *,
    fetch_json: Callable[[str], Any] = request_json,
    base_url: str = GECKOTERMINAL_BASE_URL,
) -> Dict[str, Any]:
    """Resolve source-backed identity metadata for one canonical contract."""
    try:
        normalized_chain = normalize_chain(chain)
        normalized_address = normalize_contract_address(
            normalized_chain,
            contract_address,
        )
    except TokenRegistryError as error:
        raise _registry_error(error)
    encoded_chain = urllib.parse.quote(normalized_chain, safe="")
    encoded_address = urllib.parse.quote(normalized_address, safe="")
    url = "%s/networks/%s/tokens/%s" % (
        base_url.rstrip("/"),
        encoded_chain,
        encoded_address,
    )
    payload = _fetch_payload(
        fetch_json,
        url,
        not_found_code="token_not_found",
        not_found_message="GeckoTerminal did not find this Token contract",
    )
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("type") != "token":
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal Token response is missing data.type=token",
            retryable=True,
        )
    source_token_id = data.get("id")
    _source_token_id(
        normalized_chain,
        normalized_address,
        source_token_id,
        field="data.id",
    )
    attributes = data.get("attributes")
    if not isinstance(attributes, dict):
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal Token attributes must be an object",
            retryable=True,
        )
    try:
        returned_address = normalize_contract_address(
            normalized_chain,
            attributes.get("address"),
        )
    except TokenRegistryError:
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal returned an invalid Token address",
            retryable=True,
        )
    if returned_address != normalized_address:
        raise TokenOnboardingError(
            "identity_mismatch",
            "GeckoTerminal Token address does not match the requested contract",
            details={
                "requested_address": normalized_address,
                "returned_address": returned_address,
            },
        )
    try:
        symbol = normalize_token_symbol(attributes.get("symbol"))
    except TokenRegistryError as error:
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal returned an invalid Token symbol",
            retryable=True,
            details={"reason": error.code},
        )
    token_name = str(attributes.get("name") or "").strip()
    if not token_name or len(token_name) > 160:
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal returned a missing or oversized Token name",
            retryable=True,
        )
    decimals_value = attributes.get("decimals")
    if decimals_value is None:
        decimals = None
    elif isinstance(decimals_value, bool):
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal Token decimals must be an integer",
            retryable=True,
        )
    else:
        try:
            decimals = int(decimals_value)
        except (TypeError, ValueError):
            raise TokenOnboardingError(
                "source_invalid_response",
                "GeckoTerminal Token decimals must be an integer",
                retryable=True,
            )
        if str(decimals) != str(decimals_value).strip() or not 0 <= decimals <= 255:
            raise TokenOnboardingError(
                "source_invalid_response",
                "GeckoTerminal Token decimals must be between 0 and 255",
                retryable=True,
            )
    coingecko_id_value = attributes.get("coingecko_coin_id")
    coingecko_id = (
        str(coingecko_id_value).strip()
        if coingecko_id_value not in (None, "")
        else None
    )
    if coingecko_id is not None and len(coingecko_id) > 160:
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal CoinGecko id is too long",
            retryable=True,
        )
    return {
        "chain": normalized_chain,
        "contract_address": normalized_address,
        "token_symbol": symbol,
        "token_name": token_name,
        "decimals": decimals,
        "coingecko_id": coingecko_id,
        "source": "geckoterminal",
        "source_token_id": str(source_token_id),
    }


def discover_token_pools(
    identity: Mapping[str, Any],
    *,
    fetch_json: Callable[[str], Any] = request_json,
    base_url: str = GECKOTERMINAL_BASE_URL,
    maximum_pools: int = MAX_DISCOVERED_POOLS,
) -> List[Dict[str, Any]]:
    """Return strictly validated top pools for one resolved Token identity."""
    if maximum_pools < 1 or maximum_pools > 20:
        raise TokenOnboardingError(
            "invalid_pool_limit",
            "maximum_pools must contain between 1 and 20 pools",
        )
    try:
        chain = normalize_chain(identity.get("chain"))
        address = normalize_contract_address(
            chain,
            identity.get("contract_address"),
        )
    except TokenRegistryError as error:
        raise _registry_error(error)
    url = "%s/networks/%s/tokens/%s/pools" % (
        base_url.rstrip("/"),
        urllib.parse.quote(chain, safe=""),
        urllib.parse.quote(address, safe=""),
    )
    payload = _fetch_payload(
        fetch_json,
        url,
        not_found_code="token_not_found",
        not_found_message="GeckoTerminal did not find pools for this Token contract",
    )
    data = payload.get("data")
    if not isinstance(data, list):
        raise TokenOnboardingError(
            "source_invalid_response",
            "GeckoTerminal pools response data must be a list",
            retryable=True,
        )
    pools = [
        _pool_result(pool, chain=chain, address=address)
        for pool in data
    ]
    if not pools:
        raise TokenOnboardingError(
            "no_usable_pool",
            "GeckoTerminal returned no usable pools for this Token",
        )
    pools.sort(
        key=lambda pool: (
            -(pool["volume_24h_usd"] or 0.0),
            -(pool["tvl_usd"] or 0.0),
            pool["pool_address"],
        )
    )
    return pools[:maximum_pools]


def resolve_token_candidate(
    chain: Any,
    contract_address: Any,
    *,
    fetch_json: Callable[[str], Any] = request_json,
    base_url: str = GECKOTERMINAL_BASE_URL,
    maximum_pools: int = MAX_DISCOVERED_POOLS,
) -> Dict[str, Any]:
    """Resolve a complete DEX-first onboarding preview with no CEX inference."""
    identity = resolve_token_identity(
        chain,
        contract_address,
        fetch_json=fetch_json,
        base_url=base_url,
    )
    pools = discover_token_pools(
        identity,
        fetch_json=fetch_json,
        base_url=base_url,
        maximum_pools=maximum_pools,
    )
    return {
        "identity": identity,
        "discovery": {
            "usable_pool_count": len(pools),
            "top_pools": pools,
        },
        "capabilities": {
            "dex_daily": "available",
            "tvl": "available_after_collection",
            "dex_depth": "protocol_dependent",
            "cex": "requires_manual_mapping",
        },
    }


def build_registry_record(
    candidate: Mapping[str, Any],
    *,
    created_by: str,
    status: str = "pending",
    job_id: Optional[str] = None,
    created_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the exact runtime record for a confirmed DEX-first candidate."""
    identity = candidate.get("identity")
    if not isinstance(identity, Mapping):
        raise TokenOnboardingError(
            "invalid_candidate",
            "Onboarding candidate is missing identity",
        )
    timestamp = created_at or datetime.now(timezone.utc).isoformat()
    try:
        return normalize_token_record(
            {
                **dict(identity),
                "status": status,
                "created_at": timestamp,
                "created_by": created_by,
                "last_job_id": job_id,
                "cex_mapping": {
                    "status": "requires_manual_review",
                    "cex_symbol": None,
                    "exchanges": [],
                },
            }
        )
    except TokenRegistryError as error:
        raise _registry_error(error)
