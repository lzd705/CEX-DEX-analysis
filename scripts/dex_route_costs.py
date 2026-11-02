"""Route-specific DEX costs with fail-closed evidence and privacy boundaries.

Pool swap fees remain embedded in the pool quote.  This module models only a
concrete transaction's network gas plus separately proven router, transfer-tax,
and scenario-only MEV components.  Raw calls, senders, RPC endpoints, account
identities, and exception text never enter returned objects.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Tuple

try:
    from scripts.execution_cost_components import cost_component_row
    from scripts.route_cost_evidence import (
        build_v2_swap_calldata as build_strict_v2_swap_calldata,
        decode_v2_swap_calldata as decode_strict_v2_swap_calldata,
        network_gas_usd as strict_network_gas_usd,
        next_base_fee_wei as strict_next_base_fee_wei,
        solidity_allowance_storage_key as strict_allowance_storage_key,
        solidity_balance_storage_key as strict_balance_storage_key,
    )
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from execution_cost_components import cost_component_row  # type: ignore
    from route_cost_evidence import (  # type: ignore
        build_v2_swap_calldata as build_strict_v2_swap_calldata,
        decode_v2_swap_calldata as decode_strict_v2_swap_calldata,
        network_gas_usd as strict_network_gas_usd,
        next_base_fee_wei as strict_next_base_fee_wei,
        solidity_allowance_storage_key as strict_allowance_storage_key,
        solidity_balance_storage_key as strict_balance_storage_key,
    )
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore


CHAIN_ID_BY_NAME = {
    "eth": 1,
    "optimism": 10,
    "bsc": 56,
    "zksync": 324,
    "base": 8453,
    "arbitrum": 42161,
}
ROUTE_ADAPTER_REGISTRY = {
    ("eth", "uniswap_v3"): {
        "chain_id": 1,
        "adapter_id": "uniswap_v3_router/v1",
        "router_address": "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",
    },
}
SUPPORTED_ROUTE_ADAPTERS = frozenset(
    registration["adapter_id"]
    for registration in ROUTE_ADAPTER_REGISTRY.values()
)
SENDER_POLICIES = frozenset({"opaque_simulation_sender"})
ALLOWANCE_BASES = frozenset(
    {
        "permit_embedded_in_call",
        "preapproved_at_fixed_block",
    }
)
FEE_CAP_SOURCES = frozenset({"eth_feeHistory"})
NATIVE_PRICE_SOURCES = frozenset({"synchronized_native_usd_quote"})
NATIVE_SYMBOL_BY_CHAIN_ID = {
    1: "ETH",
    10: "ETH",
    56: "BNB",
    324: "ETH",
    8453: "ETH",
    42161: "ETH",
}

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")
_HEX_DATA = re.compile(r"0x(?:[0-9a-f]{2}){4,}\Z")
_HEX_QUANTITY = re.compile(r"(?:0x0|0x[1-9a-f][0-9a-f]*)\Z")
_BLOCK_TAG = re.compile(r"0x[1-9a-f][0-9a-f]*\Z")
_BLOCK_HASH = re.compile(r"0x[0-9a-f]{64}\Z")
_ASSET = re.compile(r"[A-Z][A-Z0-9._-]{0,15}\Z")
_DEX_MARKET_ID = re.compile(
    r"dex:([a-z][a-z0-9_]{0,31}):"
    r"([a-z][a-z0-9_-]{0,63}):"
    r"(0x[0-9a-f]{40}):"
    r"([A-Z][A-Z0-9._-]{0,31})\Z"
)

_GAS_LINEAGE_FIELDS = (
    "chain_id",
    "block_tag",
    "block_number",
    "block_hash",
    "tx_call_sha256",
    "sender_policy",
    "allowance_basis",
    "adapter_id",
    "market_id",
    "pool_address",
    "token_symbol",
    "direction",
    "requested_notional_usd",
    "target_token_quantity",
    "target_token_raw",
    "market_token_address",
    "counter_token_address",
    "pool_token0_address",
    "pool_token1_address",
    "pool_fee",
    "calldata_selector",
    "adapter_call_evidence_sha256",
    "gas_units",
    "max_fee_per_gas_wei",
    "fee_cap_source",
    "fee_cap_observed_at",
    "fee_cap_valid_until",
    "fee_cap_source_sha256",
    "native_token_symbol",
    "native_token_usd",
    "native_price_source",
    "native_price_observed_at",
    "native_price_valid_until",
    "native_price_sha256",
    "native_price_source_bundle_sha256",
    "rpc_evidence_sha256",
)


class _EvidenceError(ValueError):
    def __init__(self, reason_code: str, *, status: str = "unavailable") -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.status = status


@dataclass(frozen=True)
class GasQuoteRequest:
    cohort_id: str
    opportunity_id: str
    leg: str
    market_id: str
    requested_notional_usd: Any
    target_token_quantity: Any
    now: str
    chain_id: Optional[int]
    tx_call: Optional[Mapping[str, Any]]
    tx_call_sha256: Optional[str]
    sender_policy: Optional[str]
    allowance_basis: Optional[str]
    block_tag: Optional[str]
    max_fee_per_gas_wei: Optional[int]
    fee_cap_source: Optional[str]
    fee_cap_observed_at: Optional[str]
    fee_cap_valid_until: Optional[str]
    fee_cap_source_sha256: Optional[str]
    native_token_symbol: Optional[str]
    native_token_usd: Any
    native_price_source: Optional[str]
    native_price_observed_at: Optional[str]
    native_price_valid_until: Optional[str]
    native_price_sha256: Optional[str]
    adapter_id: Optional[str]
    adapter_call_evidence: Any = None
    native_price_evidence: Optional[Mapping[str, Any]] = None


@dataclass(frozen=True)
class ControlledAdapterCallEvidence:
    """Context-bound integrity record; it does not authenticate pool facts."""

    adapter_id: str
    market_id: str
    pool_address: str
    token_symbol: str
    direction: str
    requested_notional_usd: str
    target_token_quantity: str
    target_token_raw: str
    block_tag: str
    tx_call_sha256: str
    market_token_address: str
    counter_token_address: str
    pool_token0_address: str
    pool_token1_address: str
    pool_fee: str
    calldata_selector: str
    source_record_sha256: str


@dataclass(frozen=True)
class SynchronizedNativePriceEvidence:
    """Immutable integrity record; it is not an authenticated cohort read."""

    evidence_type: str
    cohort_id: str
    market_id: str
    chain_id: int
    block_tag: str
    block_number: str
    block_hash: str
    native_token_symbol: str
    native_token_usd: str
    source: str
    observed_at: str
    valid_until: str
    source_bundle_sha256: str
    source_record_sha256: str


@dataclass(frozen=True)
class ControlledAdapterCostEvidence:
    """Context-bound integrity record, not authenticated adapter evidence."""

    adapter_id: str
    adapter_call_evidence_sha256: str
    cohort_id: str
    opportunity_id: str
    leg: str
    market_id: str
    direction: str
    requested_notional_usd: str
    target_token_quantity: str
    block_tag: str
    tx_call_sha256: str
    component_type: str
    evidence_kind: str
    rate_bps: Optional[str]
    basis_code: str
    observed_at: str
    valid_until: str
    source_record_sha256: str


@dataclass(frozen=True)
class MevProtectionEvidence:
    """Caller-buildable policy integrity record; never an authenticated bound."""

    route_id: str
    cohort_id: str
    opportunity_id: str
    adapter_id: str
    submission_mode: str
    policy_code: str
    max_loss_bps: str
    observed_at: str
    valid_until: str
    source_record_sha256: str


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError("{} must be non-empty canonical text".format(field))
    return value


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, (bool, float)) or not isinstance(
        value, (Decimal, int, str)
    ):
        raise ValueError("{} must be an exact Decimal value".format(field))
    if isinstance(value, str) and (not value or value != value.strip()):
        raise ValueError("{} must be canonical Decimal text".format(field))
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("{} must be an exact finite Decimal".format(field)) from error
    if not number.is_finite() or number < 0 or (positive and number <= 0):
        qualifier = "positive " if positive else "non-negative "
        raise ValueError("{} must be a {}finite Decimal".format(field, qualifier))
    return number


def _decimal_text(number: Decimal) -> str:
    if number == 0:
        return "0"
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _parts(number: Decimal) -> Tuple[int, int]:
    value = number.as_tuple()
    coefficient = 0
    for digit in value.digits:
        coefficient = coefficient * 10 + digit
    if value.sign:
        coefficient = -coefficient
    return coefficient, int(value.exponent)


def _from_parts(coefficient: int, exponent: int) -> Decimal:
    if coefficient == 0:
        return Decimal(0)
    sign = 1 if coefficient < 0 else 0
    digits = tuple(int(character) for character in str(abs(coefficient)))
    return Decimal((sign, digits, exponent))


def _exact_product(*values: Decimal) -> Decimal:
    coefficient = 1
    exponent = 0
    for value in values:
        part_coefficient, part_exponent = _parts(value)
        coefficient *= part_coefficient
        exponent += part_exponent
    return _from_parts(coefficient, exponent)


def _exact_quotient(numerator: Decimal, denominator: Decimal) -> Decimal:
    numerator_coefficient, numerator_exponent = _parts(numerator)
    denominator_coefficient, denominator_exponent = _parts(denominator)
    if denominator_coefficient <= 0:
        raise ValueError("denominator must be positive")
    exponent = numerator_exponent - denominator_exponent
    if exponent >= 0:
        numerator_coefficient *= 10 ** exponent
    else:
        denominator_coefficient *= 10 ** (-exponent)
    divisor = math.gcd(abs(numerator_coefficient), denominator_coefficient)
    numerator_coefficient //= divisor
    denominator_coefficient //= divisor
    twos = 0
    fives = 0
    while denominator_coefficient % 2 == 0:
        denominator_coefficient //= 2
        twos += 1
    while denominator_coefficient % 5 == 0:
        denominator_coefficient //= 5
        fives += 1
    if denominator_coefficient != 1:
        raise ValueError("exact quotient is not finite in base ten")
    scale = max(twos, fives)
    numerator_coefficient *= 2 ** (scale - twos)
    numerator_coefficient *= 5 ** (scale - fives)
    return _from_parts(numerator_coefficient, -scale)


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256(value: Any, field: str) -> str:
    try:
        text = _required_text(value, field)
    except ValueError:
        raise _EvidenceError("{}_unavailable".format(field)) from None
    if _SHA256.fullmatch(text) is None:
        raise _EvidenceError("{}_unavailable".format(field))
    return text


def _timestamp(value: Any, field: str) -> str:
    try:
        text = _required_text(value, field)
    except ValueError:
        raise _EvidenceError("{}_unavailable".format(field)) from None
    try:
        exact_rfc3339_epoch_seconds(text)
    except ValueError:
        raise _EvidenceError("{}_unavailable".format(field)) from None
    return text


def _current_window(
    observed_at: Any,
    valid_until: Any,
    *,
    now: str,
    prefix: str,
) -> Tuple[str, str]:
    observed = _timestamp(observed_at, "{}_observed_at".format(prefix))
    valid = _timestamp(valid_until, "{}_valid_until".format(prefix))
    now_epoch = exact_rfc3339_epoch_seconds(now)
    observed_epoch = exact_rfc3339_epoch_seconds(observed)
    valid_epoch = exact_rfc3339_epoch_seconds(valid)
    if valid_epoch <= observed_epoch:
        raise _EvidenceError("{}_window_invalid".format(prefix))
    if observed_epoch > now_epoch:
        raise _EvidenceError(
            "{}_observation_in_future".format(prefix),
            status="failed",
        )
    if now_epoch >= valid_epoch:
        raise _EvidenceError("{}_evidence_expired".format(prefix), status="stale")
    return observed, valid


def _direction(leg: str) -> str:
    if leg == "buy":
        return "buy_token"
    if leg == "sell":
        return "sell_token"
    if leg == "route":
        return "route"
    raise ValueError("leg must be buy, sell, or route")


def _parse_dex_market_id(value: Any) -> Tuple[str, str, str, str]:
    try:
        market_id = _required_text(value, "market_id")
    except ValueError:
        raise ValueError("market_id must be canonical DEX identity") from None
    match = _DEX_MARKET_ID.fullmatch(market_id)
    if match is None:
        raise ValueError(
            "market_id must be canonical dex:chain:dex:pool:TOKEN"
        )
    chain, dex, pool, token = match.groups()
    if chain not in CHAIN_ID_BY_NAME:
        raise ValueError("market_id chain is not registered")
    return chain, dex, pool, token


def _registered_route(
    market_id: str,
    *,
    adapter_id: Any,
) -> Tuple[str, str, Mapping[str, Any]]:
    chain, dex, _pool, _token = _parse_dex_market_id(market_id)
    registration = ROUTE_ADAPTER_REGISTRY.get((chain, dex))
    if registration is None or adapter_id != registration["adapter_id"]:
        raise _EvidenceError("dex_route_adapter_identity_mismatch")
    return chain, dex, registration


def _component_context(
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
) -> Dict[str, Any]:
    if leg != "route":
        _parse_dex_market_id(market_id)
    return {
        "cohort_id": _required_text(cohort_id, "cohort_id"),
        "opportunity_id": _required_text(opportunity_id, "opportunity_id"),
        "leg": leg,
        "market_id": market_id,
        "direction": _direction(leg),
        "requested_notional_usd": _decimal(
            requested_notional_usd,
            "requested_notional_usd",
            positive=True,
        ),
        "target_token_quantity": _decimal(
            target_token_quantity,
            "target_token_quantity",
            positive=True,
        ),
    }


def _terminal_component(
    context: Mapping[str, Any],
    *,
    component_type: str,
    status: str,
    reason_code: str,
    basis: str,
) -> Dict[str, Any]:
    return cost_component_row(
        **context,
        component_type=component_type,
        value_status=status,
        amount_usd=None,
        rate_bps=None,
        basis=basis,
        strict_eligible=False,
        observed_at=None,
        valid_until=None,
        source="DEX route-cost evidence gate",
        source_record_sha256=None,
        reason_code=reason_code,
    )


def _terminal_for_error(
    context: Mapping[str, Any],
    *,
    component_type: str,
    error: _EvidenceError,
) -> Dict[str, Any]:
    return _terminal_component(
        context,
        component_type=component_type,
        status=error.status,
        reason_code=error.reason_code,
        basis="required route-cost evidence is incomplete or invalid",
    )


def _gas_envelope(
    component: Mapping[str, Any],
    lineage: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "cost_component": dict(component),
        "value_status": component["value_status"],
        "strict_eligible": component["strict_eligible"],
        "amount_usd": component["amount_usd"],
        "rate_bps": component["rate_bps"],
        "reason_code": component["reason_code"],
    }
    for field in _GAS_LINEAGE_FIELDS:
        result[field] = lineage.get(field) if lineage is not None else None
    return result


def _validated_tx_call(value: Any) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or set(value) != {
        "from",
        "to",
        "data",
        "value",
    }:
        raise _EvidenceError("gas_tx_call_unavailable")
    sender = value.get("from")
    recipient = value.get("to")
    data = value.get("data")
    amount = value.get("value")
    if not isinstance(sender, str) or _ADDRESS.fullmatch(sender) is None:
        raise _EvidenceError("gas_sender_unavailable")
    if not isinstance(recipient, str) or _ADDRESS.fullmatch(recipient) is None:
        raise _EvidenceError("gas_router_unavailable")
    if not isinstance(data, str) or _HEX_DATA.fullmatch(data) is None:
        raise _EvidenceError("gas_calldata_unavailable")
    if not isinstance(amount, str) or _HEX_QUANTITY.fullmatch(amount) is None:
        raise _EvidenceError("gas_call_value_unavailable")
    return {
        "from": sender,
        "to": recipient,
        "data": data,
        "value": amount,
    }


def _decode_abi_address(word: str, field: str) -> str:
    if len(word) != 64 or word[:24] != "0" * 24:
        raise ValueError("{} calldata address is invalid".format(field))
    value = "0x" + word[24:]
    if _ADDRESS.fullmatch(value) is None:
        raise ValueError("{} calldata address is invalid".format(field))
    return value


def _decode_abi_uint(word: str, field: str) -> int:
    if len(word) != 64 or any(character not in "0123456789abcdef" for character in word):
        raise ValueError("{} calldata integer is invalid".format(field))
    return int(word, 16)


def build_uniswap_v3_adapter_call_evidence(
    *,
    adapter_id: str,
    market_id: str,
    direction: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    block_tag: str,
    tx_call: Mapping[str, Any],
    market_token_address: str,
    market_token_decimals: int,
    pool_token0_address: str,
    pool_token1_address: str,
    pool_fee: int,
) -> ControlledAdapterCallEvidence:
    """Decode and hash a call for integrity; do not authenticate pool state."""
    chain, dex, registration = _registered_route(
        market_id,
        adapter_id=adapter_id,
    )
    _chain, _dex, pool_address, token_symbol = _parse_dex_market_id(market_id)
    if (chain, dex) != (_chain, _dex):  # pragma: no cover - same parser
        raise ValueError("market_id adapter identity is invalid")
    if direction not in {"buy_token", "sell_token"}:
        raise ValueError("direction must be buy_token or sell_token")
    if not isinstance(block_tag, str) or _BLOCK_TAG.fullmatch(block_tag) is None:
        raise ValueError("block_tag must be one fixed numeric block")
    call = _validated_tx_call(tx_call)
    if call["to"] != registration["router_address"]:
        raise ValueError("tx_call router does not match registered adapter")
    addresses = (
        market_token_address,
        pool_token0_address,
        pool_token1_address,
    )
    if any(not isinstance(value, str) or _ADDRESS.fullmatch(value) is None for value in addresses):
        raise ValueError("pool and Token contracts must be canonical addresses")
    if pool_token0_address == pool_token1_address:
        raise ValueError("pool Token contracts must be distinct")
    if market_token_address not in {pool_token0_address, pool_token1_address}:
        raise ValueError("market Token contract is absent from pool contracts")
    if type(market_token_decimals) is not int or not 0 <= market_token_decimals <= 36:
        raise ValueError("market Token decimals are invalid")
    if type(pool_fee) is not int or not 0 < pool_fee < 2 ** 24:
        raise ValueError("pool fee is invalid")

    data = call["data"]
    encoded = data[2:]
    if len(encoded) != 8 + 7 * 64 or encoded[:8] != "04e45aaf":
        raise ValueError("calldata must be SwapRouter02 exactInputSingle")
    words = [
        encoded[index:index + 64]
        for index in range(8, len(encoded), 64)
    ]
    token_in = _decode_abi_address(words[0], "tokenIn")
    token_out = _decode_abi_address(words[1], "tokenOut")
    decoded_fee = _decode_abi_uint(words[2], "fee")
    recipient = _decode_abi_address(words[3], "recipient")
    amount_in = _decode_abi_uint(words[4], "amountIn")
    amount_out_minimum = _decode_abi_uint(words[5], "amountOutMinimum")
    _decode_abi_uint(words[6], "sqrtPriceLimitX96")
    if {token_in, token_out} != {pool_token0_address, pool_token1_address}:
        raise ValueError("calldata Token contracts do not match pool contracts")
    if decoded_fee != pool_fee:
        raise ValueError("calldata fee does not match pool fee")
    if recipient != call["from"]:
        raise ValueError("calldata recipient does not match simulation sender")

    requested = _decimal(
        requested_notional_usd,
        "requested_notional_usd",
        positive=True,
    )
    target = _decimal(
        target_token_quantity,
        "target_token_quantity",
        positive=True,
    )
    target_raw_decimal = _exact_product(
        target,
        Decimal(10 ** market_token_decimals),
    )
    target_raw = int(target_raw_decimal)
    if Decimal(target_raw) != target_raw_decimal:
        raise ValueError("target Token quantity is not exact in raw units")
    if direction == "buy_token":
        if token_out != market_token_address or amount_out_minimum != target_raw:
            raise ValueError("buy target does not match decoded calldata")
        counter_token_address = token_in
    else:
        if token_in != market_token_address or amount_in != target_raw:
            raise ValueError("sell target does not match decoded calldata")
        counter_token_address = token_out
    call_hash = _canonical_sha256(call)
    record = {
        "adapter_id": adapter_id,
        "market_id": market_id,
        "pool_address": pool_address,
        "token_symbol": token_symbol,
        "direction": direction,
        "requested_notional_usd": _decimal_text(requested),
        "target_token_quantity": _decimal_text(target),
        "target_token_raw": str(target_raw),
        "block_tag": block_tag,
        "tx_call_sha256": call_hash,
        "market_token_address": market_token_address,
        "counter_token_address": counter_token_address,
        "pool_token0_address": pool_token0_address,
        "pool_token1_address": pool_token1_address,
        "pool_fee": str(pool_fee),
        "calldata_selector": "0x04e45aaf",
    }
    return ControlledAdapterCallEvidence(
        source_record_sha256=_canonical_sha256(record),
        **record,
    )


def build_synchronized_native_price_evidence(
    *,
    cohort_id: str,
    market_id: str,
    chain_id: int,
    block_tag: str,
    block_number: int,
    block_hash: str,
    native_token_symbol: str,
    native_token_usd: Any,
    observed_at: str,
    valid_until: str,
    source_bundle_sha256: str,
) -> SynchronizedNativePriceEvidence:
    """Create an integrity record, not a read from the real cohort bundle."""
    chain_name, _dex, _pool, _token = _parse_dex_market_id(market_id)
    if type(chain_id) is not int or chain_id <= 0:
        raise ValueError("chain_id must be a positive integer")
    if chain_id != CHAIN_ID_BY_NAME[chain_name]:
        raise ValueError("native price chain does not match market")
    if not isinstance(block_tag, str) or _BLOCK_TAG.fullmatch(block_tag) is None:
        raise ValueError("block_tag must be one fixed numeric block")
    if type(block_number) is not int or block_number != int(block_tag, 16):
        raise ValueError("block_number must match block_tag")
    if not isinstance(block_hash, str) or _BLOCK_HASH.fullmatch(block_hash) is None:
        raise ValueError("block_hash must be canonical")
    expected_symbol = NATIVE_SYMBOL_BY_CHAIN_ID.get(chain_id)
    if native_token_symbol != expected_symbol:
        raise ValueError("native Token symbol does not match chain")
    price = _decimal(native_token_usd, "native_token_usd", positive=True)
    try:
        observed_epoch = exact_rfc3339_epoch_seconds(observed_at)
        valid_epoch = exact_rfc3339_epoch_seconds(valid_until)
    except ValueError:
        raise ValueError("native price timestamps must be exact RFC3339 seconds") from None
    if valid_epoch <= observed_epoch:
        raise ValueError("native price validity window is invalid")
    if not isinstance(source_bundle_sha256, str) or _SHA256.fullmatch(
        source_bundle_sha256
    ) is None:
        raise ValueError("source_bundle_sha256 must be canonical")
    record = {
        "evidence_type": "synchronized_native_usd_quote/v1",
        "cohort_id": _required_text(cohort_id, "cohort_id"),
        "market_id": market_id,
        "chain_id": chain_id,
        "block_tag": block_tag,
        "block_number": str(block_number),
        "block_hash": block_hash,
        "native_token_symbol": native_token_symbol,
        "native_token_usd": _decimal_text(price),
        "source": "synchronized_route_cohort",
        "observed_at": observed_at,
        "valid_until": valid_until,
        "source_bundle_sha256": source_bundle_sha256,
    }
    return SynchronizedNativePriceEvidence(
        source_record_sha256=_canonical_sha256(record),
        **record,
    )


_ADAPTER_COST_BASIS = {
    "router_or_integrator_fee": {
        "numeric": "router_fee_rate",
        "not_applicable": "router_fee_not_applicable",
    },
    "token_transfer_tax": {
        "numeric": "transfer_tax_rate",
        "not_applicable": "transfer_tax_not_applicable",
    },
}


def build_route_adapter_cost_evidence(
    *,
    adapter_call_evidence: ControlledAdapterCallEvidence,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    component_type: str,
    evidence_kind: str,
    rate_bps: Any,
    basis_code: str,
    observed_at: str,
    valid_until: str,
) -> ControlledAdapterCostEvidence:
    """Build a context-bound but unauthenticated adapter-cost record."""
    if not isinstance(adapter_call_evidence, ControlledAdapterCallEvidence):
        raise ValueError("adapter_call_evidence must be controlled")
    call_record = dict(vars(adapter_call_evidence))
    call_hash = call_record.pop("source_record_sha256", None)
    if call_hash != _canonical_sha256(call_record):
        raise ValueError("adapter_call_evidence hash mismatch")
    if leg not in {"buy", "sell"} or _direction(leg) != adapter_call_evidence.direction:
        raise ValueError("leg does not match controlled adapter call")
    allowed = _ADAPTER_COST_BASIS.get(component_type, {}).get(evidence_kind)
    if allowed is None or basis_code != allowed:
        raise ValueError("adapter cost evidence behavior is unsupported")
    if evidence_kind == "numeric":
        rate_text: Optional[str] = _decimal_text(
            _decimal(rate_bps, "rate_bps", positive=True)
        )
    else:
        if rate_bps is not None:
            raise ValueError("not-applicable adapter evidence cannot have a rate")
        rate_text = None
    try:
        observed_epoch = exact_rfc3339_epoch_seconds(observed_at)
        valid_epoch = exact_rfc3339_epoch_seconds(valid_until)
    except ValueError:
        raise ValueError("adapter evidence timestamps must be exact RFC3339 seconds") from None
    if valid_epoch <= observed_epoch:
        raise ValueError("adapter evidence validity window is invalid")
    record = {
        "adapter_id": adapter_call_evidence.adapter_id,
        "adapter_call_evidence_sha256": call_hash,
        "cohort_id": _required_text(cohort_id, "cohort_id"),
        "opportunity_id": _required_text(opportunity_id, "opportunity_id"),
        "leg": leg,
        "market_id": adapter_call_evidence.market_id,
        "direction": adapter_call_evidence.direction,
        "requested_notional_usd": adapter_call_evidence.requested_notional_usd,
        "target_token_quantity": adapter_call_evidence.target_token_quantity,
        "block_tag": adapter_call_evidence.block_tag,
        "tx_call_sha256": adapter_call_evidence.tx_call_sha256,
        "component_type": component_type,
        "evidence_kind": evidence_kind,
        "rate_bps": rate_text,
        "basis_code": basis_code,
        "observed_at": observed_at,
        "valid_until": valid_until,
    }
    return ControlledAdapterCostEvidence(
        source_record_sha256=_canonical_sha256(record),
        **record,
    )


def build_mev_protection_evidence(
    *,
    route_id: str,
    cohort_id: str,
    opportunity_id: str,
    adapter_id: str,
    submission_mode: str,
    policy_code: str,
    max_loss_bps: Any,
    observed_at: str,
    valid_until: str,
) -> MevProtectionEvidence:
    """Build a policy integrity record that cannot authenticate a relay bound."""
    if adapter_id not in SUPPORTED_ROUTE_ADAPTERS:
        raise ValueError("adapter_id is not registered")
    if submission_mode != "private_relay":
        raise ValueError("bounded MEV evidence requires private_relay")
    if policy_code != "private_relay_bounded_loss":
        raise ValueError("bounded MEV policy is unsupported")
    maximum = _decimal(max_loss_bps, "max_loss_bps")
    try:
        observed_epoch = exact_rfc3339_epoch_seconds(observed_at)
        valid_epoch = exact_rfc3339_epoch_seconds(valid_until)
    except ValueError:
        raise ValueError("MEV evidence timestamps must be exact RFC3339 seconds") from None
    if valid_epoch <= observed_epoch:
        raise ValueError("MEV evidence validity window is invalid")
    record = {
        "route_id": _required_text(route_id, "route_id"),
        "cohort_id": _required_text(cohort_id, "cohort_id"),
        "opportunity_id": _required_text(opportunity_id, "opportunity_id"),
        "adapter_id": adapter_id,
        "submission_mode": submission_mode,
        "policy_code": policy_code,
        "max_loss_bps": _decimal_text(maximum),
        "observed_at": observed_at,
        "valid_until": valid_until,
    }
    return MevProtectionEvidence(
        source_record_sha256=_canonical_sha256(record),
        **record,
    )


def _positive_integer(value: Any, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise _EvidenceError("{}_unavailable".format(field))
    return value


def _gas_evidence(request: GasQuoteRequest) -> Dict[str, Any]:
    now = _timestamp(request.now, "now")
    if any(
        value is not None
        for value in (
            request.max_fee_per_gas_wei,
            request.fee_cap_source,
            request.fee_cap_observed_at,
            request.fee_cap_valid_until,
            request.fee_cap_source_sha256,
            request.native_token_symbol,
            request.native_token_usd,
            request.native_price_source,
            request.native_price_observed_at,
            request.native_price_valid_until,
            request.native_price_sha256,
        )
    ):
        raise _EvidenceError("gas_caller_supplied_cost_evidence_forbidden")
    chain_name, dex, registration = _registered_route(
        request.market_id,
        adapter_id=request.adapter_id,
    )
    chain_id = _positive_integer(request.chain_id, "gas_chain_id")
    if (
        chain_id != CHAIN_ID_BY_NAME[chain_name]
        or chain_id != registration["chain_id"]
    ):
        raise _EvidenceError("gas_market_chain_id_mismatch")
    expected_symbol = NATIVE_SYMBOL_BY_CHAIN_ID.get(chain_id)
    if expected_symbol is None:
        raise _EvidenceError("gas_chain_unsupported")
    tx_call = _validated_tx_call(request.tx_call)
    if tx_call["to"] != registration["router_address"]:
        raise _EvidenceError("gas_registered_router_mismatch")
    tx_hash = _sha256(request.tx_call_sha256, "gas_tx_call_sha256")
    if tx_hash != _canonical_sha256(tx_call):
        raise _EvidenceError("gas_tx_call_hash_mismatch")
    if request.sender_policy not in SENDER_POLICIES:
        raise _EvidenceError("gas_sender_policy_unavailable")
    if request.allowance_basis not in ALLOWANCE_BASES:
        raise _EvidenceError("gas_allowance_basis_unavailable")
    if not isinstance(request.block_tag, str) or _BLOCK_TAG.fullmatch(
        request.block_tag
    ) is None:
        raise _EvidenceError("gas_fixed_block_unavailable")
    adapter_evidence = request.adapter_call_evidence
    if not isinstance(adapter_evidence, ControlledAdapterCallEvidence):
        raise _EvidenceError("gas_controlled_adapter_evidence_unavailable")
    adapter_record = dict(vars(adapter_evidence))
    adapter_hash = adapter_record.pop("source_record_sha256", None)
    if adapter_hash != _canonical_sha256(adapter_record):
        raise _EvidenceError("gas_controlled_adapter_evidence_hash_mismatch")
    expected_adapter_fields = {
        "adapter_id": request.adapter_id,
        "market_id": request.market_id,
        "direction": _direction(request.leg),
        "requested_notional_usd": _decimal_text(
            _decimal(
                request.requested_notional_usd,
                "requested_notional_usd",
                positive=True,
            )
        ),
        "target_token_quantity": _decimal_text(
            _decimal(
                request.target_token_quantity,
                "target_token_quantity",
                positive=True,
            )
        ),
        "block_tag": request.block_tag,
        "tx_call_sha256": tx_hash,
    }
    if any(
        getattr(adapter_evidence, field) != expected
        for field, expected in expected_adapter_fields.items()
    ):
        raise _EvidenceError("gas_controlled_adapter_context_mismatch")

    native_evidence = request.native_price_evidence
    if not isinstance(native_evidence, SynchronizedNativePriceEvidence):
        raise _EvidenceError("gas_native_price_evidence_unavailable")
    if (
        native_evidence.cohort_id != request.cohort_id
        or native_evidence.market_id != request.market_id
        or native_evidence.chain_id != chain_id
        or native_evidence.block_tag != request.block_tag
        or native_evidence.source != "synchronized_route_cohort"
    ):
        raise _EvidenceError("gas_native_price_context_mismatch")
    return {
        "now": now,
        "chain_id": chain_id,
        "tx_call": tx_call,
        "tx_call_sha256": tx_hash,
        "sender_policy": request.sender_policy,
        "allowance_basis": request.allowance_basis,
        "block_tag": request.block_tag,
        "adapter_call_evidence": adapter_evidence,
        "adapter_call_evidence_sha256": adapter_hash,
        "native_price_evidence": native_evidence,
        "native_token_symbol": expected_symbol,
        "adapter_id": request.adapter_id,
        "chain_name": chain_name,
        "dex": dex,
    }


def _rpc_integer(value: Any, field: str) -> int:
    if isinstance(value, str) and _HEX_QUANTITY.fullmatch(value):
        number = int(value, 16)
    else:
        raise _EvidenceError("{}_invalid".format(field), status="failed")
    if number <= 0:
        raise _EvidenceError("{}_invalid".format(field), status="failed")
    return number


def _rpc_nonnegative_integer(value: Any, field: str) -> int:
    if isinstance(value, str) and _HEX_QUANTITY.fullmatch(value):
        return int(value, 16)
    raise _EvidenceError("{}_invalid".format(field), status="failed")


def _utc_timestamp_from_epoch(value: int) -> str:
    try:
        instant = datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as error:
        raise _EvidenceError("gas_rpc_block_timestamp_invalid", status="failed") from error
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fee_history_evidence(
    rpc: Any,
    *,
    chain_id: int,
    block_tag: str,
    block_number: int,
    block_hash: str,
    block_timestamp: int,
    block_base_fee_per_gas_wei: int,
    now: str,
) -> Dict[str, Any]:
    history = rpc.fee_history("0x1", block_tag, [50])
    if not isinstance(history, Mapping) or set(history) != {
        "oldestBlock",
        "baseFeePerGas",
        "gasUsedRatio",
        "reward",
    }:
        raise _EvidenceError("gas_fee_history_invalid", status="failed")
    oldest = history.get("oldestBlock")
    base_fees = history.get("baseFeePerGas")
    gas_ratios = history.get("gasUsedRatio")
    rewards = history.get("reward")
    if (
        oldest != block_tag
        or not isinstance(base_fees, list)
        or len(base_fees) != 2
        or not isinstance(gas_ratios, list)
        or len(gas_ratios) != 1
        or not isinstance(rewards, list)
        or len(rewards) != 1
        or not isinstance(rewards[0], list)
        or len(rewards[0]) != 1
    ):
        raise _EvidenceError("gas_fee_history_lineage_invalid", status="failed")
    ratio = gas_ratios[0]
    if (
        isinstance(ratio, bool)
        or not isinstance(ratio, (int, float))
        or not math.isfinite(ratio)
        or ratio < 0
        or ratio > 1
    ):
        raise _EvidenceError("gas_fee_history_ratio_invalid", status="failed")
    current_base = _rpc_nonnegative_integer(
        base_fees[0],
        "gas_fee_history_current_base_fee",
    )
    if current_base != block_base_fee_per_gas_wei:
        raise _EvidenceError(
            "gas_fee_history_block_base_fee_mismatch",
            status="failed",
        )
    next_base = _rpc_nonnegative_integer(
        base_fees[1],
        "gas_fee_history_next_base_fee",
    )
    priority = _rpc_nonnegative_integer(
        rewards[0][0],
        "gas_fee_history_priority_fee",
    )
    max_fee = 2 * next_base + priority
    if max_fee <= 0:
        raise _EvidenceError("gas_fee_history_cap_invalid", status="failed")
    observed_at = _utc_timestamp_from_epoch(block_timestamp)
    valid_until = (
        datetime.fromtimestamp(block_timestamp, tz=timezone.utc)
        + timedelta(seconds=120)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    _current_window(
        observed_at,
        valid_until,
        now=now,
        prefix="gas_fee_cap",
    )
    record = {
        "chain_id": chain_id,
        "block_tag": block_tag,
        "block_number": str(block_number),
        "block_hash": block_hash,
        "oldest_block": oldest,
        "current_base_fee_per_gas_wei": str(current_base),
        "next_base_fee_per_gas_wei": str(next_base),
        "priority_fee_per_gas_wei": str(priority),
        "max_fee_per_gas_wei": str(max_fee),
        "gas_used_ratio": str(ratio),
        "source": "eth_feeHistory",
        "observed_at": observed_at,
        "valid_until": valid_until,
    }
    return {
        "max_fee_per_gas_wei": max_fee,
        "fee_cap_source": "eth_feeHistory",
        "fee_cap_observed_at": observed_at,
        "fee_cap_valid_until": valid_until,
        "fee_cap_source_sha256": _canonical_sha256(record),
    }


_NATIVE_PRICE_EVIDENCE_FIELDS = {
    "evidence_type",
    "cohort_id",
    "market_id",
    "chain_id",
    "block_tag",
    "block_number",
    "block_hash",
    "native_token_symbol",
    "native_token_usd",
    "source",
    "observed_at",
    "valid_until",
    "source_bundle_sha256",
    "source_record_sha256",
}


def _validated_native_price_evidence(
    controlled_evidence: SynchronizedNativePriceEvidence,
    *,
    cohort_id: str,
    market_id: str,
    chain_id: int,
    block_tag: str,
    block_number: int,
    block_hash: str,
    expected_symbol: str,
    block_observed_at: str,
    now: str,
) -> Dict[str, Any]:
    evidence = dict(vars(controlled_evidence))
    if set(evidence) != _NATIVE_PRICE_EVIDENCE_FIELDS:
        raise _EvidenceError("gas_native_price_evidence_invalid")
    expected = {
        "evidence_type": "synchronized_native_usd_quote/v1",
        "cohort_id": cohort_id,
        "market_id": market_id,
        "chain_id": chain_id,
        "block_tag": block_tag,
        "block_number": str(block_number),
        "block_hash": block_hash,
        "native_token_symbol": expected_symbol,
        "source": "synchronized_route_cohort",
    }
    if any(evidence.get(field) != value for field, value in expected.items()):
        raise _EvidenceError("gas_native_price_context_mismatch")
    observed, valid = _current_window(
        evidence.get("observed_at"),
        evidence.get("valid_until"),
        now=now,
        prefix="gas_native_price",
    )
    if exact_rfc3339_epoch_seconds(observed) != exact_rfc3339_epoch_seconds(
        block_observed_at
    ):
        raise _EvidenceError("gas_native_price_cohort_time_mismatch")
    bundle_hash = _sha256(
        evidence.get("source_bundle_sha256"),
        "gas_native_price_source_bundle_sha256",
    )
    record_hash = _sha256(
        evidence.get("source_record_sha256"),
        "gas_native_price_sha256",
    )
    record = dict(evidence)
    record.pop("source_record_sha256")
    if record_hash != _canonical_sha256(record):
        raise _EvidenceError("gas_native_price_hash_mismatch")
    price = _decimal(
        evidence.get("native_token_usd"),
        "native_token_usd",
        positive=True,
    )
    return {
        "native_token_usd": price,
        "native_price_source": evidence["source"],
        "native_price_observed_at": observed,
        "native_price_valid_until": valid,
        "native_price_sha256": record_hash,
        "native_price_source_bundle_sha256": bundle_hash,
    }


def estimate_route_gas(*, rpc: Any, request: GasQuoteRequest) -> Dict[str, Any]:
    """Quote a concrete fixed-block call and return a redacted gas envelope."""
    if not isinstance(request, GasQuoteRequest):
        raise ValueError("request must be a GasQuoteRequest")
    context = _component_context(
        cohort_id=request.cohort_id,
        opportunity_id=request.opportunity_id,
        leg=request.leg,
        market_id=request.market_id,
        requested_notional_usd=request.requested_notional_usd,
        target_token_quantity=request.target_token_quantity,
    )
    try:
        evidence = _gas_evidence(request)
    except _EvidenceError as error:
        return _gas_envelope(
            _terminal_for_error(
                context,
                component_type="network_gas",
                error=error,
            )
        )
    except (TypeError, ValueError):
        return _gas_envelope(
            _terminal_component(
                context,
                component_type="network_gas",
                status="unavailable",
                reason_code="gas_evidence_invalid",
                basis="required gas evidence failed exact validation",
            )
        )

    try:
        rpc_chain_id = _rpc_integer(rpc.chain_id(), "gas_rpc_chain")
        if rpc_chain_id != evidence["chain_id"]:
            raise _EvidenceError("gas_rpc_chain_mismatch", status="failed")
        block = rpc.block(evidence["block_tag"])
        if not isinstance(block, Mapping):
            raise _EvidenceError("gas_rpc_block_invalid", status="failed")
        block_number = _rpc_integer(block.get("number"), "gas_rpc_block")
        if block_number != int(evidence["block_tag"], 16):
            raise _EvidenceError("gas_rpc_block_mismatch", status="failed")
        block_hash = block.get("hash")
        if not isinstance(block_hash, str) or _BLOCK_HASH.fullmatch(block_hash) is None:
            raise _EvidenceError("gas_rpc_block_hash_invalid", status="failed")
        block_timestamp = _rpc_integer(
            block.get("timestamp"),
            "gas_rpc_block_timestamp",
        )
        block_base_fee = _rpc_nonnegative_integer(
            block.get("baseFeePerGas"),
            "gas_rpc_block_base_fee",
        )
        block_observed_at = _utc_timestamp_from_epoch(block_timestamp)
        evidence.update(
            _fee_history_evidence(
                rpc,
                chain_id=evidence["chain_id"],
                block_tag=evidence["block_tag"],
                block_number=block_number,
                block_hash=block_hash,
                block_timestamp=block_timestamp,
                block_base_fee_per_gas_wei=block_base_fee,
                now=evidence["now"],
            )
        )
        evidence.update(
            _validated_native_price_evidence(
                evidence["native_price_evidence"],
                cohort_id=request.cohort_id,
                market_id=request.market_id,
                chain_id=evidence["chain_id"],
                block_tag=evidence["block_tag"],
                block_number=block_number,
                block_hash=block_hash,
                expected_symbol=evidence["native_token_symbol"],
                block_observed_at=block_observed_at,
                now=evidence["now"],
            )
        )
        gas_units = _rpc_integer(
            rpc.estimate_gas(evidence["tx_call"], evidence["block_tag"]),
            "gas_estimate",
        )
    except _EvidenceError as error:
        return _gas_envelope(
            _terminal_for_error(
                context,
                component_type="network_gas",
                error=error,
            )
        )
    except Exception:
        return _gas_envelope(
            _terminal_component(
                context,
                component_type="network_gas",
                status="failed",
                reason_code="gas_rpc_estimate_failed",
                basis="fixed-block eth_estimateGas call failed",
            )
        )

    amount = _exact_product(
        Decimal(gas_units),
        Decimal(evidence["max_fee_per_gas_wei"]),
        Decimal("0.000000000000000001"),
        evidence["native_token_usd"],
    )
    try:
        rate = _exact_quotient(
            _exact_product(amount, Decimal("10000")),
            context["requested_notional_usd"],
        )
    except ValueError:
        return _gas_envelope(
            _terminal_component(
                context,
                component_type="network_gas",
                status="failed",
                reason_code="gas_rate_not_exactly_representable",
                basis="gas USD cannot be represented as an exact finite bps rate",
            )
        )

    valid_until = min(
        (
            evidence["fee_cap_valid_until"],
            evidence["native_price_valid_until"],
        ),
        key=exact_rfc3339_epoch_seconds,
    )
    source_record = {
        "chain_id": evidence["chain_id"],
        "block_tag": evidence["block_tag"],
        "block_number": block_number,
        "block_hash": block_hash,
        "tx_call_sha256": evidence["tx_call_sha256"],
        "sender_policy": evidence["sender_policy"],
        "allowance_basis": evidence["allowance_basis"],
        "adapter_id": evidence["adapter_id"],
        "market_id": request.market_id,
        "pool_address": evidence["adapter_call_evidence"].pool_address,
        "token_symbol": evidence["adapter_call_evidence"].token_symbol,
        "direction": evidence["adapter_call_evidence"].direction,
        "requested_notional_usd": evidence[
            "adapter_call_evidence"
        ].requested_notional_usd,
        "target_token_quantity": evidence[
            "adapter_call_evidence"
        ].target_token_quantity,
        "target_token_raw": evidence["adapter_call_evidence"].target_token_raw,
        "market_token_address": evidence[
            "adapter_call_evidence"
        ].market_token_address,
        "counter_token_address": evidence[
            "adapter_call_evidence"
        ].counter_token_address,
        "pool_token0_address": evidence[
            "adapter_call_evidence"
        ].pool_token0_address,
        "pool_token1_address": evidence[
            "adapter_call_evidence"
        ].pool_token1_address,
        "pool_fee": evidence["adapter_call_evidence"].pool_fee,
        "calldata_selector": evidence["adapter_call_evidence"].calldata_selector,
        "adapter_call_evidence_sha256": evidence[
            "adapter_call_evidence_sha256"
        ],
        "gas_units": str(gas_units),
        "max_fee_per_gas_wei": str(evidence["max_fee_per_gas_wei"]),
        "fee_cap_source": evidence["fee_cap_source"],
        "fee_cap_observed_at": evidence["fee_cap_observed_at"],
        "fee_cap_valid_until": evidence["fee_cap_valid_until"],
        "fee_cap_source_sha256": evidence["fee_cap_source_sha256"],
        "native_token_symbol": evidence["native_token_symbol"],
        "native_token_usd": _decimal_text(evidence["native_token_usd"]),
        "native_price_source": evidence["native_price_source"],
        "native_price_observed_at": evidence["native_price_observed_at"],
        "native_price_valid_until": evidence["native_price_valid_until"],
        "native_price_sha256": evidence["native_price_sha256"],
        "native_price_source_bundle_sha256": evidence[
            "native_price_source_bundle_sha256"
        ],
        "quoted_at": evidence["now"],
    }
    source_hash = _canonical_sha256(source_record)
    basis = (
        "fixed-block eth_estimateGas; chain_id={}; block={}; call_sha256={}; "
        "sender_policy={}; allowance_basis={}; gas_units={}; "
        "max_fee_per_gas_wei={}; native_token_usd={}; adapter_id={}"
    ).format(
        evidence["chain_id"],
        block_number,
        evidence["tx_call_sha256"],
        evidence["sender_policy"],
        evidence["allowance_basis"],
        gas_units,
        evidence["max_fee_per_gas_wei"],
        _decimal_text(evidence["native_token_usd"]),
        evidence["adapter_id"],
    )
    component = cost_component_row(
        **context,
        component_type="network_gas",
        value_status="assumed",
        amount_usd=amount,
        rate_bps=rate,
        basis="integrity-only self-built inputs; {}".format(basis),
        strict_eligible=False,
        observed_at=evidence["now"],
        valid_until=valid_until,
        source=(
            "fixed-block RPC calculation with unauthenticated self-built "
            "route and native/USD inputs"
        ),
        source_record_sha256=source_hash,
    )
    lineage = {
        "chain_id": str(evidence["chain_id"]),
        "block_tag": evidence["block_tag"],
        "block_number": str(block_number),
        "block_hash": block_hash,
        "tx_call_sha256": evidence["tx_call_sha256"],
        "sender_policy": evidence["sender_policy"],
        "allowance_basis": evidence["allowance_basis"],
        "adapter_id": evidence["adapter_id"],
        "market_id": request.market_id,
        "pool_address": evidence["adapter_call_evidence"].pool_address,
        "token_symbol": evidence["adapter_call_evidence"].token_symbol,
        "direction": evidence["adapter_call_evidence"].direction,
        "requested_notional_usd": evidence[
            "adapter_call_evidence"
        ].requested_notional_usd,
        "target_token_quantity": evidence[
            "adapter_call_evidence"
        ].target_token_quantity,
        "target_token_raw": evidence["adapter_call_evidence"].target_token_raw,
        "market_token_address": evidence[
            "adapter_call_evidence"
        ].market_token_address,
        "counter_token_address": evidence[
            "adapter_call_evidence"
        ].counter_token_address,
        "pool_token0_address": evidence[
            "adapter_call_evidence"
        ].pool_token0_address,
        "pool_token1_address": evidence[
            "adapter_call_evidence"
        ].pool_token1_address,
        "pool_fee": evidence["adapter_call_evidence"].pool_fee,
        "calldata_selector": evidence["adapter_call_evidence"].calldata_selector,
        "adapter_call_evidence_sha256": evidence[
            "adapter_call_evidence_sha256"
        ],
        "gas_units": str(gas_units),
        "max_fee_per_gas_wei": str(evidence["max_fee_per_gas_wei"]),
        "fee_cap_source": evidence["fee_cap_source"],
        "fee_cap_observed_at": evidence["fee_cap_observed_at"],
        "fee_cap_valid_until": evidence["fee_cap_valid_until"],
        "fee_cap_source_sha256": evidence["fee_cap_source_sha256"],
        "native_token_symbol": evidence["native_token_symbol"],
        "native_token_usd": _decimal_text(evidence["native_token_usd"]),
        "native_price_source": evidence["native_price_source"],
        "native_price_observed_at": evidence["native_price_observed_at"],
        "native_price_valid_until": evidence["native_price_valid_until"],
        "native_price_sha256": evidence["native_price_sha256"],
        "native_price_source_bundle_sha256": evidence[
            "native_price_source_bundle_sha256"
        ],
        "rpc_evidence_sha256": source_hash,
    }
    return _gas_envelope(component, lineage)


def _adapter_cost_component(
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    now: str,
    adapter_id: str,
    block_tag: Any,
    tx_call_sha256: Any,
    evidence: Any,
    component_type: str,
    numeric_basis_code: str,
    not_applicable_basis_code: str,
    reason_prefix: str,
) -> Dict[str, Any]:
    context = _component_context(
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        leg=leg,
        market_id=market_id,
        requested_notional_usd=requested_notional_usd,
        target_token_quantity=target_token_quantity,
    )
    try:
        now_text = _timestamp(now, "now")
    except _EvidenceError as error:
        return _terminal_for_error(
            context,
            component_type=component_type,
            error=error,
        )
    if (
        adapter_id not in SUPPORTED_ROUTE_ADAPTERS
        or not isinstance(evidence, ControlledAdapterCostEvidence)
    ):
        return _terminal_component(
            context,
            component_type=component_type,
            status="unavailable",
            reason_code="{}_evidence_unavailable".format(reason_prefix),
            basis="validated route-adapter evidence is unavailable",
        )
    try:
        _registered_route(market_id, adapter_id=adapter_id)
    except _EvidenceError as error:
        return _terminal_for_error(
            context,
            component_type=component_type,
            error=error,
        )
    try:
        if not isinstance(block_tag, str) or _BLOCK_TAG.fullmatch(block_tag) is None:
            raise _EvidenceError("{}_block_context_invalid".format(reason_prefix))
        call_hash = _sha256(
            tx_call_sha256,
            "{}_tx_call_sha256".format(reason_prefix),
        )
        source_record = dict(vars(evidence))
        provided_hash = source_record.pop("source_record_sha256", None)
        if provided_hash != _canonical_sha256(source_record):
            raise _EvidenceError("{}_evidence_hash_mismatch".format(reason_prefix))
        expected_context = {
            "adapter_id": adapter_id,
            "cohort_id": cohort_id,
            "opportunity_id": opportunity_id,
            "leg": leg,
            "market_id": market_id,
            "direction": context["direction"],
            "requested_notional_usd": _decimal_text(
                context["requested_notional_usd"]
            ),
            "target_token_quantity": _decimal_text(
                context["target_token_quantity"]
            ),
            "block_tag": block_tag,
            "tx_call_sha256": call_hash,
            "component_type": component_type,
        }
        if any(
            getattr(evidence, field) != expected
            for field, expected in expected_context.items()
        ):
            raise _EvidenceError("{}_context_mismatch".format(reason_prefix))
        if not isinstance(evidence.adapter_call_evidence_sha256, str) or _SHA256.fullmatch(
            evidence.adapter_call_evidence_sha256
        ) is None:
            raise _EvidenceError("{}_call_evidence_invalid".format(reason_prefix))
        observed, valid = _current_window(
            evidence.observed_at,
            evidence.valid_until,
            now=now_text,
            prefix=reason_prefix,
        )
        kind = evidence.evidence_kind
        basis_code = evidence.basis_code
        if kind == "numeric" and basis_code == numeric_basis_code:
            rate = _decimal(evidence.rate_bps, "rate_bps", positive=True)
            amount = _exact_product(
                context["requested_notional_usd"],
                rate,
                Decimal("0.0001"),
            )
            return cost_component_row(
                **context,
                component_type=component_type,
                value_status="assumed",
                amount_usd=amount,
                rate_bps=rate,
                basis=(
                    "caller-buildable adapter integrity record {}"
                ).format(numeric_basis_code),
                strict_eligible=False,
                observed_at=observed,
                valid_until=valid,
                source="unauthenticated adapter-cost assumption",
                source_record_sha256=provided_hash,
            )
        if kind == "not_applicable" and basis_code == not_applicable_basis_code:
            if evidence.rate_bps is not None:
                raise _EvidenceError("{}_evidence_invalid".format(reason_prefix))
            return _terminal_component(
                context,
                component_type=component_type,
                status="unavailable",
                reason_code="{}_not_applicable_unverified".format(reason_prefix),
                basis=(
                    "caller-buildable integrity record cannot prove "
                    "not-applicable behavior"
                ),
            )
        raise _EvidenceError("{}_behavior_unknown".format(reason_prefix))
    except _EvidenceError as error:
        return _terminal_for_error(
            context,
            component_type=component_type,
            error=error,
        )
    except (TypeError, ValueError):
        return _terminal_component(
            context,
            component_type=component_type,
            status="failed",
            reason_code="{}_evidence_invalid".format(reason_prefix),
            basis="route-adapter evidence failed exact validation",
        )


def router_fee_component(
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    now: str,
    adapter_id: str,
    block_tag: Any,
    tx_call_sha256: Any,
    evidence: Any,
) -> Dict[str, Any]:
    return _adapter_cost_component(
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        leg=leg,
        market_id=market_id,
        requested_notional_usd=requested_notional_usd,
        target_token_quantity=target_token_quantity,
        now=now,
        adapter_id=adapter_id,
        block_tag=block_tag,
        tx_call_sha256=tx_call_sha256,
        evidence=evidence,
        component_type="router_or_integrator_fee",
        numeric_basis_code="router_fee_rate",
        not_applicable_basis_code="router_fee_not_applicable",
        reason_prefix="router_fee",
    )


def transfer_tax_component(
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    now: str,
    adapter_id: str,
    block_tag: Any,
    tx_call_sha256: Any,
    evidence: Any,
) -> Dict[str, Any]:
    return _adapter_cost_component(
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        leg=leg,
        market_id=market_id,
        requested_notional_usd=requested_notional_usd,
        target_token_quantity=target_token_quantity,
        now=now,
        adapter_id=adapter_id,
        block_tag=block_tag,
        tx_call_sha256=tx_call_sha256,
        evidence=evidence,
        component_type="token_transfer_tax",
        numeric_basis_code="transfer_tax_rate",
        not_applicable_basis_code="transfer_tax_not_applicable",
        reason_prefix="transfer_tax",
    )


def mev_route_policy(
    *,
    cohort_id: str,
    opportunity_id: str,
    leg: str,
    market_id: str,
    requested_notional_usd: Any,
    target_token_quantity: Any,
    now: str,
    route_id: str,
    adapter_id: str,
    submission_mode: str,
    protection_policy: Optional[str] = None,
    scenario_rate_bps: Any = None,
    protection_evidence: Any = None,
) -> Dict[str, Any]:
    """Return only terminal, assumed, or bounded MEV scenario components."""
    context = _component_context(
        cohort_id=cohort_id,
        opportunity_id=opportunity_id,
        leg=leg,
        market_id=market_id,
        requested_notional_usd=requested_notional_usd,
        target_token_quantity=target_token_quantity,
    )
    try:
        now_text = _timestamp(now, "now")
        route_identity = _required_text(route_id, "route_id")
    except _EvidenceError as error:
        return _terminal_for_error(
            context,
            component_type="mev_buffer",
            error=error,
        )
    except ValueError:
        return _terminal_component(
            context,
            component_type="mev_buffer",
            status="unavailable",
            reason_code="mev_route_identity_unavailable",
            basis="canonical route identity is unavailable",
        )
    if adapter_id not in SUPPORTED_ROUTE_ADAPTERS:
        return _terminal_component(
            context,
            component_type="mev_buffer",
            status="unavailable",
            reason_code="mev_adapter_policy_unavailable",
            basis="registered route-adapter MEV policy is unavailable",
        )
    if submission_mode not in ("public_mempool", "private_relay"):
        return _terminal_component(
            context,
            component_type="mev_buffer",
            status="unavailable",
            reason_code="mev_submission_policy_unavailable",
            basis="recognized transaction-submission policy is unavailable",
        )
    if scenario_rate_bps is None:
        return _terminal_component(
            context,
            component_type="mev_buffer",
            status="unavailable",
            reason_code="mev_protection_unavailable",
            basis="no positive MEV scenario buffer or bounded protection evidence",
        )
    try:
        rate = _decimal(scenario_rate_bps, "scenario_rate_bps", positive=True)
    except ValueError:
        return _terminal_component(
            context,
            component_type="mev_buffer",
            status="unavailable",
            reason_code="mev_zero_buffer_not_evidence",
            basis="zero or invalid MEV input is not evidence of zero MEV cost",
        )
    amount = _exact_product(
        context["requested_notional_usd"],
        rate,
        Decimal("0.0001"),
    )

    def assumed_component() -> Dict[str, Any]:
        return cost_component_row(
            **context,
            component_type="mev_buffer",
            value_status="assumed",
            amount_usd=amount,
            rate_bps=rate,
            basis="user-supplied positive MEV scenario buffer",
            strict_eligible=False,
            observed_at=None,
            valid_until=None,
            source="user scenario",
            source_record_sha256=None,
        )

    # Future authenticated relay-policy evidence must enter through a private
    # connector-owned verification path.  Public arguments and integrity-only
    # records deliberately have no override that can elevate this scenario.
    return assumed_component()
