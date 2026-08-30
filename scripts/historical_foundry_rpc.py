"""Staged anchor authority and sealed historical archive-RPC run boundary."""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from decimal import Decimal
import hashlib
import hmac
import importlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
import urllib.error
import urllib.parse
import urllib.request
import weakref

from scripts.bounded_json import (
    BoundedJsonError,
    decode_bounded_json_response,
    validate_bounded_json_response_headers,
)
from scripts.historical_foundry_contracts import (
    HistoricalFoundryConfigSet,
    build_validated_executor_artifact,
    load_historical_foundry_config_set,
    validate_historical_foundry_authority,
    validate_historical_foundry_policy_shape,
)
from scripts.route_cost_evidence import (
    build_factory_get_pair_calldata,
    keccak256,
    solidity_allowance_storage_key,
    solidity_balance_storage_key,
)


_HISTORICAL_WINDOW_MODULE_GENERATION = object()
_HISTORICAL_WINDOW_BOUND_IDENTITY_NAMES = (
    ("rpc", "_ArchiveRpcError"),
    ("rpc", "_ProductionHistoricalWindowRunClaim"),
    ("rpc", "_ProductionHistoricalWindowLogicalBatchScope"),
    ("rpc", "_ClaimedHistoricalWindowSourceCapsule"),
    ("rpc", "_ProductionArchiveRpcFinalization"),
    ("rpc", "_get_claimed_historical_window_config"),
    ("rpc", "_consume_claimed_historical_window_source_capsule_for_storage"),
    ("rpc", "_commit_claimed_historical_window_source_capsule_move"),
    ("rpc", "_abort_claimed_historical_window_source_capsule_move"),
    ("rpc", "_open_production_archive_rpc_historical_window_logical_batch"),
    ("rpc", "_production_archive_rpc_historical_window_logical_batch_attempt"),
    ("rpc", "_finalize_claimed_production_archive_rpc_run_for_historical_window"),
    ("rpc", "_verify_claimed_historical_window_finalization"),
    ("rpc", "_ProductionHistoricalWindowRunClaim.__enter__"),
    ("rpc", "_ProductionHistoricalWindowRunClaim.__exit__"),
    ("rpc", "_ProductionHistoricalWindowRunClaim.close"),
    ("scan", "_ProductionHistoricalWindowPreFinalization"),
    ("scan", "_ProductionHistoricalWindowReconciliation"),
    ("scan", "_capture_production_historical_window"),
    ("scan", "_verify_production_historical_window_prefinalization"),
    ("scan", "_reconcile_production_historical_window"),
    ("scan", "_verify_production_historical_window_reconciliation"),
    ("storage", "_HistoricalWindowExchangeSpool"),
    ("storage", "_SealedHistoricalWindowExchangeSpool"),
    ("storage", "_HistoricalWindowSpoolReconciliationCursor"),
    ("storage", "_ProductionHistoricalWindowCapability"),
    ("storage", "_ConsumedProductionHistoricalWindowCapabilityView"),
    ("storage", "_HistoricalWindowExchangeSpool._bind_claimed_source_authority_from_rpc"),
    ("storage", "_HistoricalWindowExchangeSpool._verify_bound_source_authority_for_claimed_finalization"),
    ("storage", "_HistoricalWindowExchangeSpool.issue_transfer_from_bound_rpc"),
    ("storage", "_HistoricalWindowExchangeSpool.append_transfer"),
    ("storage", "_HistoricalWindowExchangeSpool.verify_pending_receipt"),
    ("storage", "_HistoricalWindowExchangeSpool.commit_transfer"),
    ("storage", "_HistoricalWindowExchangeSpool.verify_committed_receipt"),
    ("storage", "_HistoricalWindowExchangeSpool.release_verified_transfer"),
    ("storage", "_HistoricalWindowExchangeSpool.abort_transfer"),
    ("storage", "_HistoricalWindowExchangeSpool.reread_exchange"),
    ("storage", "_HistoricalWindowExchangeSpool.seal"),
    ("storage", "_HistoricalWindowExchangeSpool.close"),
    ("storage", "_SealedHistoricalWindowExchangeSpool.reread_exchange"),
    ("storage", "_SealedHistoricalWindowExchangeSpool._open_reconciliation_cursor_from_bound_scan"),
    ("storage", "_SealedHistoricalWindowExchangeSpool.mint_production_historical_window_capability"),
    ("storage", "_SealedHistoricalWindowExchangeSpool.close"),
    ("storage", "_HistoricalWindowSpoolReconciliationCursor.__enter__"),
    ("storage", "_HistoricalWindowSpoolReconciliationCursor.__iter__"),
    ("storage", "_HistoricalWindowSpoolReconciliationCursor.__next__"),
    ("storage", "_HistoricalWindowSpoolReconciliationCursor.__exit__"),
    ("storage", "_HistoricalWindowSpoolReconciliationCursor.close"),
    ("storage", "_ProductionHistoricalWindowCapability.__enter__"),
    ("storage", "_ProductionHistoricalWindowCapability.__exit__"),
    ("storage", "_ProductionHistoricalWindowCapability.close"),
    ("storage", "_ConsumedProductionHistoricalWindowCapabilityView.__enter__"),
    ("storage", "_ConsumedProductionHistoricalWindowCapabilityView.__exit__"),
    ("storage", "_ConsumedProductionHistoricalWindowCapabilityView.close"),
    ("storage", "consume_production_historical_window_capability"),
)


_PLAN_SCHEMA = "historical_foundry_anchor_request_plan/v1"
_CAPTURE_SCHEMA = "historical_foundry_anchor_capture/v1"

_MAX_JSON_NODES = 1_048_576
_MAX_SCALAR_BYTES = 8 * 1024 * 1024
_MAX_ORDINARY_STRING_BYTES = 256 * 1024
_MAX_NESTING_DEPTH = 128
_MAX_CONTAINER_ITEMS = 64
_MAX_RUNTIME_BYTES = 24_576
_MAX_ABI_BYTES = 256 * 1024

_HASH32 = re.compile(r"0x[0-9a-f]{64}\Z", re.ASCII)
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z", re.ASCII)
_HEX_BYTES = re.compile(r"0x(?:[0-9a-f]{2})*\Z", re.ASCII)
_QUANTITY = re.compile(r"(?:0x0|0x[1-9a-f][0-9a-f]*)\Z", re.ASCII)

_UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
_WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
_EXECUTOR = "0x68778b870ceee58d82ba9f97cb4219981fdafa72"
_SENDER = "0x5ca9e6c3ed27cc0acfb355061fcab6964d4fc444"
_FEED_PROXY = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
_VENUES = (
    (
        "uniswap_v2",
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
        "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
    ),
    (
        "sushiswap_v2",
        "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",
        "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac",
    ),
)
_FIXED_AUTHORITY_ADDRESSES = frozenset((
    _UNI,
    _WETH,
    _VENUES[0][1],
    _VENUES[0][2],
    _VENUES[1][1],
    _VENUES[1][2],
    _FEED_PROXY,
    _EXECUTOR,
    _SENDER,
))

_DECIMALS = "0x313ce567"
_BALANCE_OF = "0x70a08231"
_ALLOWANCE = "0xdd62ed3e"
_FACTORY = "0xc45a0155"
_WETH_GETTER = "0xad5c4648"
_DESCRIPTION = "0x7284e416"
_AGGREGATOR = "0x245a7bfc"
_PHASE = "0x58303b10"
_LATEST_ROUND = "0xfeaf968c"
_TOKEN0 = "0x0dfe1681"
_TOKEN1 = "0xd21220a7"

_PARAMS_HASH_DOMAIN = b"historical_foundry_anchor_request_params/v1"
_REQUEST_HASH_DOMAIN = b"historical_foundry_anchor_request/v1"
_RESULT_HASH_DOMAIN = b"historical_foundry_anchor_response_result/v1"
_RESPONSE_HASH_DOMAIN = b"historical_foundry_anchor_response/v1"

_RESPONSE_FIELDS = frozenset(("jsonrpc", "id", "result"))
_WIRE_FIELDS = frozenset(("jsonrpc", "id", "method", "params"))
_TEMPLATE_FIELDS = frozenset((
    "id", "role", "method", "dependencies", "bindings", "params_template",
))
_STAGE_FIELDS = frozenset((
    "index", "name", "dependencies", "bindings", "requests",
))
_PLAN_FIELDS = frozenset((
    "schema", "chain_id", "anchor_tag", "stages", "request_count",
    "request_ids",
))
_ANCHOR_RESULT_FIELDS = frozenset((
    "number", "hash", "parentHash", "stateRoot", "timestamp", "gasLimit",
    "gasUsed", "baseFeePerGas",
))
_CAPTURE_FIELDS = frozenset((
    "schema", "chain_id", "anchor", "tokens", "venues", "price_feed",
    "executor", "sender", "request_inventory",
))
_INVENTORY_FIELDS = frozenset((
    "id", "role", "method", "request", "response", "params_sha256",
    "request_sha256", "result_sha256", "response_sha256",
))


def _resource_error() -> ValueError:
    return ValueError("historical anchor resource limit exceeded")


def _normalize_config(value: Any) -> Any:
    """Detach MappingProxy/tuple config values while enforcing JSON limits."""
    nodes = 0
    scalar_bytes = 0

    def visit(current: Any, depth: int) -> Any:
        nonlocal nodes, scalar_bytes
        nodes += 1
        if nodes > _MAX_JSON_NODES or depth > _MAX_NESTING_DEPTH:
            raise _resource_error()
        if isinstance(current, MappingABC):
            if type(current) is dict:
                iterator = iter(current.items())
            else:
                try:
                    # MappingProxy may wrap an adversarial Mapping, so exercise
                    # only the capabilities needed to consume it. Other Mapping
                    # implementations retain their closed protocol check, but no
                    # reported length is trusted as a resource boundary.
                    if type(current) is not MappingProxyType:
                        len(current)
                    iterator = iter(current.items())
                except Exception:
                    raise ValueError(
                        "historical anchor config mapping is invalid"
                    ) from None
            result = {}
            count = 0
            while True:
                try:
                    row = next(iterator)
                except StopIteration:
                    break
                except Exception:
                    raise ValueError(
                        "historical anchor config mapping is invalid"
                    ) from None
                count += 1
                if count > _MAX_CONTAINER_ITEMS:
                    raise _resource_error()
                if type(row) is not tuple or len(row) != 2:
                    raise ValueError(
                        "historical anchor config mapping is invalid"
                    )
                key, nested = row
                if type(key) is not str:
                    raise ValueError("historical anchor config key is invalid")
                if key in result:
                    raise ValueError("historical anchor config mapping is invalid")
                encoded = key.encode("utf-8")
                if len(encoded) > _MAX_ORDINARY_STRING_BYTES:
                    raise _resource_error()
                scalar_bytes += len(encoded)
                if scalar_bytes > _MAX_SCALAR_BYTES:
                    raise _resource_error()
                result[key] = visit(nested, depth + 1)
            return result
        if type(current) in (list, tuple):
            if len(current) > _MAX_CONTAINER_ITEMS:
                raise _resource_error()
            return [visit(nested, depth + 1) for nested in current]
        if type(current) is str:
            encoded = current.encode("utf-8")
            if len(encoded) > _MAX_ORDINARY_STRING_BYTES:
                raise _resource_error()
            scalar_bytes += len(encoded)
            if scalar_bytes > _MAX_SCALAR_BYTES:
                raise _resource_error()
            return current
        if current is None or type(current) in (bool, int):
            return current
        raise ValueError("historical anchor config value is invalid")

    return visit(value, 0)


def _guard_exact_json(value: Any) -> None:
    """Match the shared decoder's limits for already-materialized pure inputs."""
    nodes = 0
    scalar_bytes = 0
    pending = [(value, 0)]
    while pending:
        current, depth = pending.pop()
        nodes += 1
        if nodes > _MAX_JSON_NODES or depth > _MAX_NESTING_DEPTH:
            raise _resource_error()
        if type(current) is dict:
            for key, nested in current.items():
                if type(key) is not str:
                    raise ValueError("historical anchor object key is invalid")
                encoded = key.encode("utf-8")
                if len(encoded) > _MAX_ORDINARY_STRING_BYTES:
                    raise _resource_error()
                scalar_bytes += len(encoded)
                pending.append((nested, depth + 1))
        elif type(current) is list:
            pending.extend((nested, depth + 1) for nested in current)
        elif type(current) is str:
            encoded = current.encode("utf-8")
            if len(encoded) > _MAX_ORDINARY_STRING_BYTES:
                raise _resource_error()
            scalar_bytes += len(encoded)
        elif current is None or type(current) in (bool, int):
            pass
        else:
            raise ValueError("historical anchor value type is invalid")
        if scalar_bytes > _MAX_SCALAR_BYTES:
            raise _resource_error()


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("historical anchor value is not canonical JSON") from exc


def _copy_json(value: Any) -> Any:
    return json.loads(_canonical_bytes(value).decode("utf-8"))


def _typed_hash(domain: bytes, value: Any) -> str:
    return hashlib.sha256(domain + b"\0" + _canonical_bytes(value)).hexdigest()


def _binding(name: str) -> Dict[str, str]:
    return {"binding": name}


def _address_argument(address: str) -> str:
    return "0" * 24 + address[2:]


def _balance_calldata() -> str:
    return _BALANCE_OF + _address_argument(_EXECUTOR)


def _allowance_calldata(router: str) -> str:
    return _ALLOWANCE + _address_argument(_EXECUTOR) + _address_argument(router)


def _call(to: Any, data: str) -> Dict[str, Any]:
    return {"to": to, "data": data}


def _template(
    request_id: int,
    role: str,
    method: str,
    params: List[Any],
    dependencies: Sequence[str],
    bindings: Sequence[str],
) -> Dict[str, Any]:
    return {
        "id": request_id,
        "role": role,
        "method": method,
        "dependencies": list(dependencies),
        "bindings": list(bindings),
        "params_template": params,
    }


def _fixed_templates() -> List[Dict[str, Any]]:
    anchor = _binding("anchor_block_reference")
    rows = []
    request_id = 3
    for role, token, balance_slot, allowance_slot in (
        ("uni", _UNI, 4, 3),
        ("weth", _WETH, 3, 4),
    ):
        rows.extend((
            _template(request_id, role + "_runtime", "eth_getCode",
                      [token, anchor], ("anchor_block_reference",), ()),
            _template(request_id + 1, role + "_decimals", "eth_call",
                      [_call(token, _DECIMALS), anchor],
                      ("anchor_block_reference",), ()),
            _template(request_id + 2, role + "_executor_balance_getter", "eth_call",
                      [_call(token, _balance_calldata()), anchor],
                      ("anchor_block_reference",), ()),
            _template(request_id + 3, role + "_executor_balance_storage",
                      "eth_getStorageAt",
                      [token, solidity_balance_storage_key(_EXECUTOR, balance_slot), anchor],
                      ("anchor_block_reference",), ()),
        ))
        request_id += 4
        for venue_id, router, _factory_address in _VENUES:
            rows.extend((
                _template(
                    request_id, role + "_" + venue_id + "_allowance_getter",
                    "eth_call", [_call(token, _allowance_calldata(router)), anchor],
                    ("anchor_block_reference",), (),
                ),
                _template(
                    request_id + 1,
                    role + "_" + venue_id + "_allowance_storage",
                    "eth_getStorageAt",
                    [token, solidity_allowance_storage_key(
                        _EXECUTOR, router, allowance_slot
                    ), anchor],
                    ("anchor_block_reference",), (),
                ),
            ))
            request_id += 2

    for venue_offset, (venue_id, router, factory) in enumerate(_VENUES):
        base = 19 + 6 * venue_offset
        forward_binding = venue_id + "_pair_forward"
        reverse_binding = venue_id + "_pair_reverse"
        rows.extend((
            _template(base, venue_id + "_router_runtime", "eth_getCode",
                      [router, anchor], ("anchor_block_reference",), ()),
            _template(base + 1, venue_id + "_router_factory", "eth_call",
                      [_call(router, _FACTORY), anchor],
                      ("anchor_block_reference",), ()),
            _template(base + 2, venue_id + "_router_weth", "eth_call",
                      [_call(router, _WETH_GETTER), anchor],
                      ("anchor_block_reference",), ()),
            _template(base + 3, venue_id + "_factory_runtime", "eth_getCode",
                      [factory, anchor], ("anchor_block_reference",), ()),
            _template(
                base + 4, venue_id + "_pair_forward", "eth_call",
                [_call(factory, build_factory_get_pair_calldata(_UNI, _WETH)), anchor],
                ("anchor_block_reference",), (forward_binding,),
            ),
            _template(
                base + 5, venue_id + "_pair_reverse", "eth_call",
                [_call(factory, build_factory_get_pair_calldata(_WETH, _UNI)), anchor],
                ("anchor_block_reference",), (reverse_binding,),
            ),
        ))

    rows.extend((
        _template(31, "chainlink_proxy_runtime", "eth_getCode",
                  [_FEED_PROXY, anchor], ("anchor_block_reference",), ()),
        _template(32, "chainlink_description", "eth_call",
                  [_call(_FEED_PROXY, _DESCRIPTION), anchor],
                  ("anchor_block_reference",), ()),
        _template(33, "chainlink_decimals", "eth_call",
                  [_call(_FEED_PROXY, _DECIMALS), anchor],
                  ("anchor_block_reference",), ()),
        _template(34, "chainlink_aggregator", "eth_call",
                  [_call(_FEED_PROXY, _AGGREGATOR), anchor],
                  ("anchor_block_reference",), ("chainlink_aggregator_address",)),
        _template(35, "chainlink_phase", "eth_call",
                  [_call(_FEED_PROXY, _PHASE), anchor],
                  ("anchor_block_reference",), ()),
        _template(36, "chainlink_latest_round", "eth_call",
                  [_call(_FEED_PROXY, _LATEST_ROUND), anchor],
                  ("anchor_block_reference",), ()),
        _template(37, "executor_prior_runtime", "eth_getCode",
                  [_EXECUTOR, anchor], ("anchor_block_reference",), ()),
        _template(38, "executor_prior_nonce", "eth_getTransactionCount",
                  [_EXECUTOR, anchor], ("anchor_block_reference",), ()),
        _template(39, "sender_prior_nonce", "eth_getTransactionCount",
                  [_SENDER, anchor], ("anchor_block_reference",), ()),
    ))
    return rows


def _derived_templates() -> List[Dict[str, Any]]:
    anchor = _binding("anchor_block_reference")
    rows = []
    for offset, (venue_id, _router, _factory) in enumerate(_VENUES):
        base = 40 + 4 * offset
        pair = _binding(venue_id + "_pair_address")
        dependencies = (venue_id + "_pair_address", "anchor_block_reference")
        rows.extend((
            _template(base, venue_id + "_pair_runtime", "eth_getCode",
                      [pair, anchor], dependencies, ()),
            _template(base + 1, venue_id + "_pair_factory", "eth_call",
                      [_call(pair, _FACTORY), anchor], dependencies, ()),
            _template(base + 2, venue_id + "_pair_token0", "eth_call",
                      [_call(pair, _TOKEN0), anchor], dependencies, ()),
            _template(base + 3, venue_id + "_pair_token1", "eth_call",
                      [_call(pair, _TOKEN1), anchor], dependencies, ()),
        ))
    rows.append(_template(
        48, "chainlink_aggregator_runtime", "eth_getCode",
        [_binding("chainlink_aggregator_address"), anchor],
        ("chainlink_aggregator_address", "anchor_block_reference"), (),
    ))
    return rows


def _build_closed_plan() -> Dict[str, Any]:
    anchor_requests = [
        _template(1, "chain_id", "eth_chainId", [], (), ("chain_id",)),
        _template(
            2, "finalized_anchor", "eth_getBlockByNumber",
            ["finalized", False], (),
            ("anchor_header", "anchor_block_reference"),
        ),
    ]
    fixed = _fixed_templates()
    derived = _derived_templates()
    return {
        "schema": _PLAN_SCHEMA,
        "chain_id": 1,
        "anchor_tag": "finalized",
        "stages": [
            {
                "index": 0,
                "name": "anchor",
                "dependencies": [],
                "bindings": ["chain_id", "anchor_header", "anchor_block_reference"],
                "requests": anchor_requests,
            },
            {
                "index": 1,
                "name": "fixed_authority",
                "dependencies": ["anchor_block_reference"],
                "bindings": [
                    "uniswap_v2_pair_forward", "uniswap_v2_pair_reverse",
                    "sushiswap_v2_pair_forward", "sushiswap_v2_pair_reverse",
                    "chainlink_aggregator_address",
                ],
                "requests": fixed,
            },
            {
                "index": 2,
                "name": "derived_authority",
                "dependencies": [
                    "anchor_block_reference", "uniswap_v2_pair_forward",
                    "uniswap_v2_pair_reverse", "sushiswap_v2_pair_forward",
                    "sushiswap_v2_pair_reverse", "chainlink_aggregator_address",
                ],
                "bindings": [],
                "requests": derived,
            },
        ],
        "request_count": 48,
        "request_ids": list(range(1, 49)),
    }


def build_historical_anchor_request_plan(
    policy: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Return the detached closed three-stage symbolic request authority."""
    normalized_policy = _normalize_config(policy)
    normalized_authority = _normalize_config(authority)
    validated_policy = validate_historical_foundry_policy_shape(normalized_policy)
    validated_authority = validate_historical_foundry_authority(normalized_authority)
    if (
        validated_policy["chain_id"] != validated_authority["chain_id"]
        or validated_policy["chain_id"] != 1
        or validated_policy["anchor_tag"] != "finalized"
    ):
        raise ValueError("historical anchor policy authority differs")
    # The validators above close every identity and selector used by this plan.
    return _copy_json(_build_closed_plan())


def _validate_closed_plan(plan: Any) -> Dict[str, Any]:
    _guard_exact_json(plan)
    if type(plan) is not dict or set(plan) != _PLAN_FIELDS:
        raise ValueError("historical anchor request plan schema is invalid")
    expected = _build_closed_plan()
    if plan != expected:
        raise ValueError("historical anchor request plan differs from authority")
    for stage in plan["stages"]:
        if type(stage) is not dict or set(stage) != _STAGE_FIELDS:
            raise ValueError("historical anchor stage schema is invalid")
        for row in stage["requests"]:
            if type(row) is not dict or set(row) != _TEMPLATE_FIELDS:
                raise ValueError("historical anchor template schema is invalid")
    return _copy_json(expected)


def _validate_success_rows(
    rows: Any, expected_ids: Sequence[int]
) -> Dict[int, Dict[str, Any]]:
    if type(rows) not in (list, tuple):
        raise ValueError("historical anchor responses must be a sequence")
    normalized_rows = list(rows)
    _guard_exact_json(normalized_rows)
    expected = list(expected_ids)
    if len(normalized_rows) != len(expected):
        raise ValueError("historical anchor response count differs")
    result = {}
    for row in normalized_rows:
        if type(row) is not dict or set(row) != _RESPONSE_FIELDS:
            raise ValueError("historical anchor response schema is invalid")
        if row["jsonrpc"] != "2.0" or type(row["jsonrpc"]) is not str:
            raise ValueError("historical anchor response version is invalid")
        request_id = row["id"]
        if type(request_id) is not int or request_id <= 0 or request_id in result:
            raise ValueError("historical anchor response ID is invalid")
        result[request_id] = _copy_json(row)
    if sorted(result) != expected:
        raise ValueError("historical anchor response ID set differs")
    return result


def _quantity(value: Any, label: str) -> Tuple[str, int]:
    if type(value) is not str or _QUANTITY.fullmatch(value) is None:
        raise ValueError(label + " quantity is invalid")
    return value, int(value, 16)


def _hash32(value: Any, label: str) -> str:
    if type(value) is not str or _HASH32.fullmatch(value) is None:
        raise ValueError(label + " hash is invalid")
    return value


def _hex_bytes(
    value: Any,
    label: str,
    maximum: int,
    allow_empty: bool = True,
) -> bytes:
    if type(value) is not str or _HEX_BYTES.fullmatch(value) is None:
        raise ValueError(label + " hex bytes are invalid")
    size = (len(value) - 2) // 2
    if size > maximum:
        raise _resource_error()
    payload = bytes.fromhex(value[2:])
    if not allow_empty and not payload:
        raise ValueError(label + " must be nonempty")
    return payload


def _uint_word(value: Any, label: str) -> int:
    payload = _hex_bytes(value, label, 32)
    if len(payload) != 32:
        raise ValueError(label + " ABI word is invalid")
    return int.from_bytes(payload, "big")


def _address_word(value: Any, label: str, nonzero: bool = False) -> str:
    payload = _hex_bytes(value, label, 32)
    if len(payload) != 32 or any(payload[:12]):
        raise ValueError(label + " ABI address is invalid")
    address = "0x" + payload[12:].hex()
    if _ADDRESS.fullmatch(address) is None or (
        nonzero and address == "0x" + "00" * 20
    ):
        raise ValueError(label + " address is invalid")
    return address


def _abi_string(value: Any, label: str) -> str:
    payload = _hex_bytes(value, label, _MAX_ABI_BYTES)
    if len(payload) < 64 or len(payload) % 32 != 0:
        raise ValueError(label + " ABI string is invalid")
    if int.from_bytes(payload[:32], "big") != 32:
        raise ValueError(label + " ABI string offset is invalid")
    length = int.from_bytes(payload[32:64], "big")
    padded_length = ((length + 31) // 32) * 32
    if len(payload) != 64 + padded_length:
        raise ValueError(label + " ABI string length is invalid")
    raw = payload[64:64 + length]
    if any(payload[64 + length:]):
        raise ValueError(label + " ABI string padding is invalid")
    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(label + " ABI string encoding is invalid") from exc
    if len(raw) > _MAX_ORDINARY_STRING_BYTES:
        raise _resource_error()
    return decoded


def _anchor_projection(result: Any) -> Dict[str, str]:
    if type(result) is not dict or not _ANCHOR_RESULT_FIELDS.issubset(result):
        raise ValueError("historical anchor header schema is invalid")
    number, _number_value = _quantity(result["number"], "anchor number")
    timestamp, _timestamp_value = _quantity(result["timestamp"], "anchor timestamp")
    gas_limit, gas_limit_value = _quantity(result["gasLimit"], "anchor gas limit")
    gas_used, gas_used_value = _quantity(result["gasUsed"], "anchor gas used")
    base_fee, _base_fee_value = _quantity(
        result["baseFeePerGas"], "anchor base fee"
    )
    if gas_limit_value <= 0 or gas_used_value > gas_limit_value:
        raise ValueError("historical anchor gas values are invalid")
    return {
        "number": number,
        "hash": _hash32(result["hash"], "anchor"),
        "parent_hash": _hash32(result["parentHash"], "anchor parent"),
        "state_root": _hash32(result["stateRoot"], "anchor state root"),
        "timestamp": timestamp,
        "gas_limit": gas_limit,
        "gas_used": gas_used,
        "base_fee_per_gas": base_fee,
    }


def _anchor_bindings(rows: Mapping[int, Mapping[str, Any]]) -> Dict[str, Any]:
    chain_text, chain_id = _quantity(rows[1]["result"], "chain ID")
    if chain_text != "0x1" or chain_id != 1:
        raise ValueError("historical anchor chain differs")
    anchor = _anchor_projection(rows[2]["result"])
    return {
        "anchor_header": anchor,
        "anchor_block_reference": {
            "blockHash": anchor["hash"], "requireCanonical": True,
        },
    }


def _resolve_template(value: Any, bindings: Mapping[str, Any]) -> Any:
    if type(value) is dict and set(value) == {"binding"}:
        name = value["binding"]
        if type(name) is not str or name not in bindings:
            raise ValueError("historical anchor template binding is invalid")
        return _copy_json(bindings[name])
    if type(value) is dict:
        return {key: _resolve_template(nested, bindings)
                for key, nested in value.items()}
    if type(value) is list:
        return [_resolve_template(nested, bindings) for nested in value]
    return value


def _stage_identity(stage: Any) -> Tuple[int, str]:
    if type(stage) is int and type(stage) is not bool and stage in (0, 1, 2):
        return (stage, ("anchor", "fixed_authority", "derived_authority")[stage])
    if type(stage) is str and stage in (
        "anchor", "fixed_authority", "derived_authority"
    ):
        return (("anchor", "fixed_authority", "derived_authority").index(stage), stage)
    raise ValueError("historical anchor stage is invalid")


def _derived_bindings(rows: Mapping[int, Mapping[str, Any]]) -> Dict[str, Any]:
    bindings = _anchor_bindings(rows)
    pair_addresses = []
    for venue_id, forward_id, reverse_id in (
        ("uniswap_v2", 23, 24), ("sushiswap_v2", 29, 30),
    ):
        forward = _address_word(
            rows[forward_id]["result"], venue_id + " forward pair", True
        )
        reverse = _address_word(
            rows[reverse_id]["result"], venue_id + " reverse pair", True
        )
        if forward != reverse:
            raise ValueError(venue_id + " pair directions differ")
        pair_addresses.append(forward)
        bindings[venue_id + "_pair_address"] = forward
    if len(set(pair_addresses)) != 2:
        raise ValueError("historical anchor pair addresses collide")
    aggregator_address = _address_word(
        rows[34]["result"], "Chainlink aggregator", True
    )
    _require_derived_authority_addresses(pair_addresses, aggregator_address)
    bindings["chainlink_aggregator_address"] = aggregator_address
    return bindings


def _require_derived_authority_addresses(
    pair_addresses: Sequence[str], aggregator_address: str
) -> None:
    derived_addresses = tuple(pair_addresses) + (aggregator_address,)
    if (
        len(derived_addresses) != 3
        or len(set(derived_addresses)) != 3
        or not set(derived_addresses).isdisjoint(_FIXED_AUTHORITY_ADDRESSES)
    ):
        raise ValueError("historical derived authority addresses collide")


def _materialize_historical_anchor_stage(
    plan: Mapping[str, Any],
    stage: Any,
    prior_success_rows: Sequence[Mapping[str, Any]],
) -> Tuple[Mapping[str, Any], ...]:
    """Materialize one closed stage from the exact preceding success rows."""
    validated_plan = _validate_closed_plan(plan)
    stage_index, _stage_name = _stage_identity(stage)
    if stage_index == 0:
        prior = _validate_success_rows(prior_success_rows, ())
        bindings = {}
    elif stage_index == 1:
        prior = _validate_success_rows(prior_success_rows, range(1, 3))
        bindings = _anchor_bindings(prior)
    else:
        prior = _validate_success_rows(prior_success_rows, range(1, 40))
        bindings = _derived_bindings(prior)
    rows = []
    for template in validated_plan["stages"][stage_index]["requests"]:
        missing = [name for name in template["dependencies"] if name not in bindings]
        if missing:
            raise ValueError("historical anchor stage dependency is absent")
        row = {
            "jsonrpc": "2.0",
            "id": template["id"],
            "method": template["method"],
            "params": _resolve_template(template["params_template"], bindings),
        }
        if set(row) != _WIRE_FIELDS:
            raise ValueError("historical anchor wire row is invalid")
        rows.append(row)
    return tuple(_copy_json(rows))


def _runtime_projection(role: str, address: str, value: Any) -> Dict[str, str]:
    runtime = _hex_bytes(value, role, _MAX_RUNTIME_BYTES, allow_empty=False)
    return {
        "role": role,
        "address": address,
        "sha256": hashlib.sha256(runtime).hexdigest(),
        "keccak256": keccak256(runtime).hex(),
    }


def _zero_word(value: Any, label: str) -> int:
    decoded = _uint_word(value, label)
    if decoded != 0:
        raise ValueError(label + " must be zero")
    return decoded


def _token_projection(
    role: str,
    address: str,
    decimals_id: int,
    runtime_id: int,
    balance_ids: Tuple[int, int],
    allowance_ids: Tuple[Tuple[int, int], Tuple[int, int]],
    balance_slot: int,
    allowance_slot: int,
    rows: Mapping[int, Mapping[str, Any]],
) -> Dict[str, Any]:
    if _uint_word(rows[decimals_id]["result"], role + " decimals") != 18:
        raise ValueError(role + " decimals differ")
    balance_getter = _zero_word(
        rows[balance_ids[0]]["result"], role + " balance getter"
    )
    balance_storage = _zero_word(
        rows[balance_ids[1]]["result"], role + " balance storage"
    )
    if balance_getter != balance_storage:
        raise ValueError(role + " balance getter/storage differ")
    allowances = []
    for (venue_id, router, _factory), ids in zip(_VENUES, allowance_ids):
        getter = _zero_word(rows[ids[0]]["result"], role + " allowance getter")
        storage = _zero_word(rows[ids[1]]["result"], role + " allowance storage")
        if getter != storage:
            raise ValueError(role + " allowance getter/storage differ")
        allowances.append({
            "venue_id": venue_id,
            "router_address": router,
            "storage_key": solidity_allowance_storage_key(
                _EXECUTOR, router, allowance_slot
            ),
            "prior_value_raw": getter,
        })
    return {
        "role": role,
        "address": address,
        "decimals": 18,
        "prior_balance_raw": balance_storage,
        "allowances": allowances,
        "runtime": _runtime_projection(
            role + "_runtime", address, rows[runtime_id]["result"]
        ),
    }


def _venue_projection(
    venue_index: int,
    rows: Mapping[int, Mapping[str, Any]],
    pair_address: str,
) -> Dict[str, Any]:
    venue_id, router_address, factory_address = _VENUES[venue_index]
    fixed_base = 19 + 6 * venue_index
    derived_base = 40 + 4 * venue_index
    router_factory = _address_word(
        rows[fixed_base + 1]["result"], venue_id + " router factory", True
    )
    router_weth = _address_word(
        rows[fixed_base + 2]["result"], venue_id + " router WETH", True
    )
    pair_factory = _address_word(
        rows[derived_base + 1]["result"], venue_id + " pair factory", True
    )
    token0 = _address_word(
        rows[derived_base + 2]["result"], venue_id + " pair token0", True
    )
    token1 = _address_word(
        rows[derived_base + 3]["result"], venue_id + " pair token1", True
    )
    if router_factory != factory_address or pair_factory != factory_address:
        raise ValueError(venue_id + " factory authority differs")
    if router_weth != _WETH:
        raise ValueError(venue_id + " WETH authority differs")
    if token0 == token1 or {token0, token1} != {_UNI, _WETH}:
        raise ValueError(venue_id + " pair token authority differs")
    return {
        "venue_id": venue_id,
        "router": {
            "address": router_address,
            "factory_address": router_factory,
            "weth_address": router_weth,
            "runtime": _runtime_projection(
                venue_id + "_router_runtime", router_address,
                rows[fixed_base]["result"],
            ),
        },
        "factory": {
            "address": factory_address,
            "runtime": _runtime_projection(
                venue_id + "_factory_runtime", factory_address,
                rows[fixed_base + 3]["result"],
            ),
        },
        "pair": {
            "address": pair_address,
            "factory_address": pair_factory,
            "token0": token0,
            "token1": token1,
            "runtime": _runtime_projection(
                venue_id + "_pair_runtime", pair_address,
                rows[derived_base]["result"],
            ),
        },
    }


def _latest_round_projection(
    value: Any, phase_id: int, anchor_timestamp: int
) -> Dict[str, int]:
    payload = _hex_bytes(value, "Chainlink latest round", 160)
    if len(payload) != 160:
        raise ValueError("Chainlink latest round ABI is invalid")
    words = [int.from_bytes(payload[index:index + 32], "big")
             for index in range(0, 160, 32)]
    round_id, answer_unsigned, started_at, updated_at, answered_in_round = words
    answer = answer_unsigned
    if answer >= (1 << 255):
        answer -= 1 << 256
    if (
        phase_id <= 0
        or phase_id >= (1 << 16)
        or round_id >= (1 << 80)
        or answered_in_round >= (1 << 80)
        or round_id >> 64 != phase_id
        or answered_in_round >> 64 != phase_id
        or (round_id & ((1 << 64) - 1)) == 0
        or (answered_in_round & ((1 << 64) - 1)) == 0
        or answer <= 0
        or started_at <= 0
        or started_at > updated_at
        or updated_at > anchor_timestamp
        or answered_in_round < round_id
    ):
        raise ValueError("Chainlink latest round authority differs")
    return {
        "round_id": round_id,
        "answer": answer,
        "started_at": started_at,
        "updated_at": updated_at,
        "answered_in_round": answered_in_round,
    }


def _feed_projection(
    rows: Mapping[int, Mapping[str, Any]],
    aggregator_address: str,
    anchor_timestamp: int,
) -> Dict[str, Any]:
    description = _abi_string(rows[32]["result"], "Chainlink description")
    decimals = _uint_word(rows[33]["result"], "Chainlink decimals")
    observed_aggregator = _address_word(
        rows[34]["result"], "Chainlink aggregator", True
    )
    phase_id = _uint_word(rows[35]["result"], "Chainlink phase")
    if description != "ETH / USD" or decimals != 8:
        raise ValueError("Chainlink feed metadata differs")
    if observed_aggregator != aggregator_address:
        raise ValueError("Chainlink aggregator binding differs")
    if phase_id <= 0 or phase_id >= (1 << 16):
        raise ValueError("Chainlink phase is invalid")
    return {
        "description": description,
        "decimals": decimals,
        "phase_id": phase_id,
        "latest_round": _latest_round_projection(
            rows[36]["result"], phase_id, anchor_timestamp
        ),
        "proxy": {
            "address": _FEED_PROXY,
            "runtime": _runtime_projection(
                "chainlink_proxy_runtime", _FEED_PROXY, rows[31]["result"]
            ),
        },
        "aggregator": {
            "address": aggregator_address,
            "runtime": _runtime_projection(
                "chainlink_aggregator_runtime", aggregator_address,
                rows[48]["result"],
            ),
        },
    }


def _inventory(
    plan: Mapping[str, Any],
    requests: Sequence[Mapping[str, Any]],
    responses: Mapping[int, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    roles = {}
    for stage in plan["stages"]:
        for template in stage["requests"]:
            roles[template["id"]] = template["role"]
    result = []
    for request in requests:
        request_id = request["id"]
        response = responses[request_id]
        result.append({
            "id": request_id,
            "role": roles[request_id],
            "method": request["method"],
            "request": _copy_json(request),
            "response": _copy_json(response),
            "params_sha256": _typed_hash(
                _PARAMS_HASH_DOMAIN, request["params"]
            ),
            "request_sha256": _typed_hash(_REQUEST_HASH_DOMAIN, request),
            "result_sha256": _typed_hash(_RESULT_HASH_DOMAIN, response["result"]),
            "response_sha256": _typed_hash(_RESPONSE_HASH_DOMAIN, response),
        })
    return result


def _project_capture(
    plan: Mapping[str, Any], responses: Sequence[Mapping[str, Any]]
) -> Dict[str, Any]:
    validated_plan = _validate_closed_plan(plan)
    response_rows = _validate_success_rows(responses, range(1, 49))
    anchor = _anchor_bindings(response_rows)["anchor_header"]
    derived = _derived_bindings(response_rows)
    anchor_requests = _materialize_historical_anchor_stage(
        validated_plan, "anchor", []
    )
    fixed_requests = _materialize_historical_anchor_stage(
        validated_plan, "fixed_authority",
        [response_rows[index] for index in range(1, 3)],
    )
    derived_requests = _materialize_historical_anchor_stage(
        validated_plan, "derived_authority",
        [response_rows[index] for index in range(1, 40)],
    )
    requests = anchor_requests + fixed_requests + derived_requests

    tokens = [
        _token_projection(
            "uni", _UNI, 4, 3, (5, 6), ((7, 8), (9, 10)), 4, 3,
            response_rows,
        ),
        _token_projection(
            "weth", _WETH, 12, 11, (13, 14), ((15, 16), (17, 18)), 3, 4,
            response_rows,
        ),
    ]
    pair_addresses = [
        derived["uniswap_v2_pair_address"],
        derived["sushiswap_v2_pair_address"],
    ]
    _require_derived_authority_addresses(
        pair_addresses, derived["chainlink_aggregator_address"]
    )
    venues = [
        _venue_projection(index, response_rows, pair_addresses[index])
        for index in range(2)
    ]
    timestamp_value = int(anchor["timestamp"], 16)
    feed = _feed_projection(
        response_rows, derived["chainlink_aggregator_address"], timestamp_value
    )
    executor_code = _hex_bytes(
        response_rows[37]["result"], "executor prior code", _MAX_RUNTIME_BYTES
    )
    if executor_code:
        raise ValueError("executor prior code is not empty")
    _executor_nonce_text, executor_nonce = _quantity(
        response_rows[38]["result"], "executor nonce"
    )
    _sender_nonce_text, sender_nonce = _quantity(
        response_rows[39]["result"], "sender nonce"
    )
    if executor_nonce != 0 or sender_nonce != 0:
        raise ValueError("historical anchor prior nonce differs")
    capture = {
        "schema": _CAPTURE_SCHEMA,
        "chain_id": 1,
        "anchor": anchor,
        "tokens": tokens,
        "venues": venues,
        "price_feed": feed,
        "executor": {
            "address": _EXECUTOR, "prior_code": "0x", "prior_nonce": 0,
        },
        "sender": {"address": _SENDER, "prior_nonce": 0},
        "request_inventory": _inventory(
            validated_plan, requests, response_rows
        ),
    }
    _guard_exact_json(capture)
    return _copy_json(capture)


def _validate_historical_anchor_capture(capture: Mapping[str, Any]) -> bool:
    """Semantically replay one self-contained capture from retained preimages."""
    _guard_exact_json(capture)
    if type(capture) is not dict or set(capture) != _CAPTURE_FIELDS:
        raise ValueError("historical anchor capture schema is invalid")
    inventory = capture.get("request_inventory")
    if type(inventory) is not list or len(inventory) != 48:
        raise ValueError("historical anchor request inventory is invalid")
    requests = []
    responses = []
    for expected_id, row in enumerate(inventory, 1):
        if type(row) is not dict or set(row) != _INVENTORY_FIELDS:
            raise ValueError("historical anchor inventory row is invalid")
        if type(row["id"]) is not int or row["id"] != expected_id:
            raise ValueError("historical anchor inventory order differs")
        request = row["request"]
        response = row["response"]
        if (
            type(request) is not dict
            or set(request) != _WIRE_FIELDS
            or type(response) is not dict
            or set(response) != _RESPONSE_FIELDS
            or request["id"] != expected_id
            or response["id"] != expected_id
            or row["method"] != request["method"]
            or row["params_sha256"] != _typed_hash(
                _PARAMS_HASH_DOMAIN, request["params"]
            )
            or row["request_sha256"] != _typed_hash(
                _REQUEST_HASH_DOMAIN, request
            )
            or row["result_sha256"] != _typed_hash(
                _RESULT_HASH_DOMAIN, response["result"]
            )
            or row["response_sha256"] != _typed_hash(
                _RESPONSE_HASH_DOMAIN, response
            )
        ):
            raise ValueError("historical anchor inventory binding differs")
        requests.append(request)
        responses.append(response)
    expected_plan = _build_closed_plan()
    expected_roles = [
        template["role"]
        for stage in expected_plan["stages"]
        for template in stage["requests"]
    ]
    if [row["role"] for row in inventory] != expected_roles:
        raise ValueError("historical anchor inventory roles differ")
    expected = _project_capture(expected_plan, responses)
    if capture != expected:
        raise ValueError("historical anchor capture semantic replay differs")
    return True


def project_historical_anchor_capture(
    plan: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    """Validate all 48 observations and return a replayable closed capture."""
    capture = _project_capture(plan, responses)
    _validate_historical_anchor_capture(capture)
    return _copy_json(capture)


_ARCHIVE_REQUEST_BODY_BYTES = 4_194_304
_ARCHIVE_LOGICAL_WIRE_BYTES = 8_388_608
_ARCHIVE_LOGICAL_DECODED_BYTES = 8_388_608
_ARCHIVE_RESPONSE_HEADER_BYTES = 65_536
_ARCHIVE_RESPONSE_HEADER_ROWS = 64
_ARCHIVE_JSON_NODES = 1_048_576
_ARCHIVE_JSON_SCALAR_BYTES = 8_388_608
_ARCHIVE_JSON_STRING_BYTES = 262_144
_ARCHIVE_JSON_DEPTH = 128
_ARCHIVE_JSON_NUMERIC_TOKEN_BYTES = 4_096
_ARCHIVE_ATTEMPT_DEADLINE_SECONDS = 30
_ARCHIVE_COLLECTION_DEADLINE_SECONDS = 21_600
_ARCHIVE_MEMBER_BYTES = 1_048_576

_ARCHIVE_METHODS = (
    "eth_chainId",
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_call",
    "eth_getCode",
    "eth_getBalance",
    "eth_getTransactionCount",
    "eth_getStorageAt",
    "eth_feeHistory",
)
_ARCHIVE_METHOD_SET = frozenset(_ARCHIVE_METHODS)

_HISTORICAL_RELAY_METHODS = frozenset((
    "eth_chainId",
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_getCode",
    "eth_getBalance",
    "eth_getTransactionCount",
    "eth_getStorageAt",
    "eth_call",
    "eth_getProof",
))
_HISTORICAL_RELAY_RESOURCE_LIMITS = {
    "inbound_header_bytes": 65_536,
    "inbound_body_bytes": 4_194_304,
    "upstream_request_bytes": 4_194_304,
    "upstream_header_bytes": 65_536,
    "upstream_wire_bytes": 67_108_864,
    "upstream_decoded_bytes": 67_108_864,
    "downstream_header_bytes": 4_096,
    "downstream_body_bytes": 67_108_864,
    "cumulative_wire_bytes": 67_108_864,
    "cumulative_decoded_bytes": 67_108_864,
}


def _validate_historical_relay_resource_counts(
    *,
    inbound_header_bytes: int,
    inbound_body_bytes: int,
    upstream_request_bytes: int,
    upstream_header_bytes: int,
    upstream_wire_bytes: int,
    upstream_decoded_bytes: int,
    downstream_header_bytes: int,
    downstream_body_bytes: int,
    cumulative_wire_bytes: int,
    cumulative_decoded_bytes: int,
    elapsed_seconds: float,
) -> None:
    values = locals()
    for name, limit in _HISTORICAL_RELAY_RESOURCE_LIMITS.items():
        value = values[name]
        if type(value) is not int or value < 0 or value > limit:
            raise ValueError("historical relay resource limit exceeded")
    if (
        type(elapsed_seconds) not in (int, float)
        or not math.isfinite(float(elapsed_seconds))
        or float(elapsed_seconds) < 0
    ):
        raise ValueError("historical relay clock is invalid")
    if float(elapsed_seconds) >= 30.0:
        raise TimeoutError("historical relay deadline expired")
    return None


def _initialize_historical_relay_lease_type():
    provenance = object()

    class _HistoricalRelayLease:
        __slots__ = (
            "_state", "_key", "_endpoint_bytes", "_endpoint_identity",
            "_operation", "_clock", "_last_clock", "_connection_url",
            "_run_deadline",
        )

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical relay lease provenance is invalid")
            for name in self.__slots__:
                object.__setattr__(self, name, values[name])

        def __repr__(self) -> str:
            return "_HistoricalRelayLease(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("historical relay lease is immutable")

        def __copy__(self) -> Any:
            raise TypeError("historical relay lease is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("historical relay lease is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("historical relay lease is not serializable")

        def close(self) -> None:
            if self._state == "closed":
                return None
            _erase_archive_key(self._key)
            object.__setattr__(self, "_key", None)
            object.__setattr__(self, "_endpoint_bytes", None)
            object.__setattr__(self, "_endpoint_identity", None)
            object.__setattr__(self, "_operation", None)
            object.__setattr__(self, "_clock", None)
            object.__setattr__(self, "_connection_url", None)
            object.__setattr__(self, "_state", "closed")
            return None

    def issue(
        *,
        endpoint: str,
        operation: Callable[[bytes, float], bytes],
        monotonic: Callable[[], float],
        entropy: Callable[[int], bytes],
    ) -> _HistoricalRelayLease:
        if not callable(operation) or not callable(monotonic) or not callable(entropy):
            raise ValueError("historical relay lease input is invalid")
        _projection, endpoint_bytes, connection_url = (
            _canonicalize_archive_endpoint(endpoint)
        )
        started = _initial_clock_sample(monotonic)
        if started is None:
            raise ValueError("historical relay clock is invalid")
        try:
            key_bytes = entropy(32)
        except Exception:
            key_bytes = None
        if type(key_bytes) is not bytes or len(key_bytes) != 32:
            raise ValueError("historical relay entropy is invalid")
        key = bytearray(key_bytes)
        digest = hmac.new(key, endpoint_bytes, hashlib.sha256).hexdigest()
        return _HistoricalRelayLease(
            _provenance=provenance,
            _state="active",
            _key=key,
            _endpoint_bytes=endpoint_bytes,
            _endpoint_identity=_frozen_archive_value({
                "schema": "historical_foundry_rpc_endpoint_identity/v1",
                "scope": "single_run_nonreversible",
                "endpoint_hmac_sha256": digest,
            }),
            _operation=operation,
            _clock=monotonic,
            _last_clock=started,
            _connection_url=connection_url,
            _run_deadline=float(started) + 21_600.0,
        )

    def issue_shared(
        *, key: bytearray, endpoint_bytes: bytes,
        endpoint_identity: Mapping[str, Any], connection_url: str,
        operation: Callable[[bytes, float], bytes],
        monotonic: Callable[[], float], last_clock: float,
    ) -> _HistoricalRelayLease:
        if (
            type(key) is not bytearray
            or len(key) != 32
            or type(endpoint_bytes) is not bytes
            or type(connection_url) is not str
            or not callable(operation)
            or not callable(monotonic)
            or type(last_clock) is not float
            or not hmac.compare_digest(
                hmac.new(key, endpoint_bytes, hashlib.sha256).hexdigest(),
                endpoint_identity.get("endpoint_hmac_sha256", ""),
            )
        ):
            raise ValueError("historical relay shared authority is invalid")
        return _HistoricalRelayLease(
            _provenance=provenance,
            _state="active",
            _key=key,
            _endpoint_bytes=endpoint_bytes,
            _endpoint_identity=endpoint_identity,
            _operation=operation,
            _clock=monotonic,
            _last_clock=last_clock,
            _connection_url=connection_url,
            _run_deadline=float(last_clock) + 21_600.0,
        )

    def require(lease: Any) -> _HistoricalRelayLease:
        if (
            type(lease) is not _HistoricalRelayLease
            or getattr(lease, "_state", None) != "active"
        ):
            raise ValueError("historical relay lease is invalid")
        if (
            type(lease._key) is not bytearray
            or type(lease._endpoint_bytes) is not bytes
            or not hmac.compare_digest(
                hmac.new(
                    lease._key, lease._endpoint_bytes, hashlib.sha256
                ).hexdigest(),
                lease._endpoint_identity["endpoint_hmac_sha256"],
            )
        ):
            raise ValueError("historical relay endpoint identity differs")
        return lease

    return _HistoricalRelayLease, issue, issue_shared, require


(
    _HistoricalRelayLease,
    _issue_historical_relay_lease_for_test,
    _issue_historical_relay_lease_from_run,
    _require_historical_relay_lease,
) = _initialize_historical_relay_lease_type()
del _initialize_historical_relay_lease_type


class _HistoricalBytesHeaders:
    __slots__ = ()

    @staticmethod
    def raw_items() -> Tuple[Tuple[str, str], ...]:
        return ()


class _HistoricalBytesResponse:
    __slots__ = ("headers", "_body", "_offset")

    def __init__(self, body: bytes) -> None:
        self.headers = _HistoricalBytesHeaders()
        self._body = body
        self._offset = 0

    def read(self, size: int) -> bytes:
        start = self._offset
        stop = min(len(self._body), start + size)
        self._offset = stop
        return self._body[start:stop]


def _decode_historical_relay_json(
    body: bytes, *, limit: int, require_canonical: bool = True
) -> Any:
    if (
        type(body) is not bytes
        or not body
        or len(body) > limit
        or type(require_canonical) is not bool
    ):
        raise ValueError("historical relay JSON is invalid")
    try:
        return decode_bounded_json_response(
            _HistoricalBytesResponse(body),
            header_limit=65_536,
            wire_limit=limit,
            decoded_limit=limit,
            scalar_limit=8_388_608,
            node_limit=1_048_576,
            ordinary_string_limit=262_144,
            require_canonical=require_canonical,
        )
    except BoundedJsonError:
        raise ValueError("historical relay JSON is invalid") from None


def _historical_relay_block_tag(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType({
        "blockHash": row["block_hash"], "requireCanonical": True,
    })


def _historical_protocol_system_addresses(*, hardfork: Any) -> frozenset:
    if hardfork == "osaka":
        return frozenset((
            "0x0000f90827f1c53a10cb7a02335b175320002935",
        ))
    return frozenset()


def _build_historical_relay_scenario_authority(
    *, config: HistoricalFoundryConfigSet, row: Mapping[str, Any]
) -> Mapping[str, Any]:
    authority = config.authority.value
    tokens = {value["role"]: value for value in authority["tokens"]}
    venues = {value["venue_id"]: value for value in authority["venues"]}
    pairs = {
        venue_id: row["reserves"][venue_id]["pair_address"]
        for venue_id in ("uniswap_v2", "sushiswap_v2")
    }
    addresses = {
        authority["executor"]["address"], authority["sender"]["address"],
        authority["price_feed"]["proxy_address"],
    }
    addresses.update(_historical_protocol_system_addresses(
        hardfork=config.toolchain.value["compiler_settings"]["fork_hardfork"]
    ))
    addresses.update(value["address"] for value in tokens.values())
    for venue in venues.values():
        addresses.add(venue["router_address"])
        addresses.add(venue["factory_address"])
    addresses.update(pairs.values())

    calls = set()
    executor = authority["executor"]["address"]
    owners = (executor,) + tuple(pairs.values())
    for token in tokens.values():
        for owner in owners:
            calls.add((
                token["address"],
                "0x" + token["balance_descriptor"]["getter_selector"][2:]
                + owner[2:].rjust(64, "0"),
            ))
    for venue in venues.values():
        calls.add((venue["router_address"], venue["factory_selector"]))
        calls.add((venue["router_address"], venue["weth_selector"]))
        pair_data = (
            venue["pair_getter_selector"]
            + tokens["uni"]["address"][2:].rjust(64, "0")
            + tokens["weth"]["address"][2:].rjust(64, "0")
        )
        calls.add((venue["factory_address"], pair_data))
    for pair in pairs.values():
        calls.add((pair, "0x0902f1ac"))
    for selector in (
        authority["price_feed"]["latest_round_selector"],
        authority["price_feed"]["aggregator_selector"],
        authority["price_feed"]["phase_selector"],
        "0x313ce567", "0x7284e416",
    ):
        calls.add((authority["price_feed"]["proxy_address"], selector))
    return MappingProxyType({
        "block_number": row["block_number"],
        "block_hash": row["block_hash"],
        "fork_header": MappingProxyType(dict(row["header"])),
        "block_tag": _historical_relay_block_tag(row),
        "addresses": frozenset(addresses),
        "calls": frozenset(calls),
    })


def _initialize_historical_relay_scenario_facade_type():
    provenance = object()

    class _HistoricalRelayScenarioFacade:
        __slots__ = ("_lease", "_authority", "_scenario_deadline", "_state")

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical relay facade provenance is invalid")
            for name in self.__slots__:
                object.__setattr__(self, name, values[name])

        def __repr__(self) -> str:
            return "_HistoricalRelayScenarioFacade(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("historical relay facade is immutable")

        def __reduce__(self) -> Any:
            raise TypeError("historical relay facade is not serializable")

        def close(self) -> None:
            object.__setattr__(self, "_state", "closed")

    def issue(
        *, relay_lease: _HistoricalRelayLease,
        authority: Mapping[str, Any],
        absolute_deadline: float,
    ) -> _HistoricalRelayScenarioFacade:
        _require_historical_relay_lease(relay_lease)
        if not isinstance(authority, Mapping):
            raise ValueError("historical relay scenario authority is invalid")
        now = relay_lease._clock()
        if (
            type(now) not in (int, float)
            or type(absolute_deadline) not in (int, float)
            or isinstance(absolute_deadline, bool)
            or now >= absolute_deadline
            or absolute_deadline > relay_lease._run_deadline
        ):
            raise TimeoutError("historical relay run deadline expired")
        return _HistoricalRelayScenarioFacade(
            _provenance=provenance, _lease=relay_lease,
            _authority=authority,
            _scenario_deadline=float(absolute_deadline),
            _state="active",
        )

    def require(value: Any) -> _HistoricalRelayScenarioFacade:
        if (
            type(value) is not _HistoricalRelayScenarioFacade
            or value._state != "active"
        ):
            raise ValueError("historical relay facade is invalid")
        _require_historical_relay_lease(value._lease)
        return value

    return _HistoricalRelayScenarioFacade, issue, require


(
    _HistoricalRelayScenarioFacade,
    _issue_historical_relay_scenario_facade,
    _require_historical_relay_scenario_facade,
) = _initialize_historical_relay_scenario_facade_type()
del _initialize_historical_relay_scenario_facade_type


def _bind_historical_relay_scenario(
    *, relay_lease: _HistoricalRelayLease,
    config: HistoricalFoundryConfigSet, scenario: Any,
    absolute_deadline: float,
) -> _HistoricalRelayScenarioFacade:
    import scripts.historical_foundry_scan as scan

    if type(config) is not HistoricalFoundryConfigSet:
        raise ValueError("historical relay scenario authority is invalid")
    row = scan._validated_replay_scenario_projection(scenario=scenario)
    scenario_authority = _build_historical_relay_scenario_authority(
        config=config, row=row
    )
    return _issue_historical_relay_scenario_facade(
        relay_lease=relay_lease, authority=scenario_authority,
        absolute_deadline=absolute_deadline,
    )


def _validate_historical_relay_scenario_request(
    *, authority: Mapping[str, Any], request: Mapping[str, Any]
) -> None:
    method = request["method"]
    params = request["params"]
    block_tag = dict(authority["block_tag"])
    exact_block = hex(authority["block_number"])
    if method == "eth_chainId":
        valid = params == []
    elif method == "eth_getBlockByNumber":
        valid = (
            len(params) == 2
            and params[0] == exact_block
            and type(params[1]) is bool
        )
    elif method == "eth_getBlockByHash":
        valid = params == [authority["block_hash"], False]
    elif method in (
        "eth_getCode", "eth_getBalance", "eth_getTransactionCount",
    ):
        valid = (
            len(params) == 2
            and type(params[0]) is str
            and params[0].lower() in authority["addresses"]
            and params[1] in (exact_block, block_tag)
        )
    elif method == "eth_getStorageAt":
        valid = (
            len(params) == 3
            and type(params[0]) is str
            and params[0].lower() in authority["addresses"]
            and type(params[1]) is str
            and re.fullmatch(r"0x[0-9a-f]{64}", params[1]) is not None
            and params[2] in (exact_block, block_tag)
        )
    elif method == "eth_call":
        valid = (
            len(params) == 2
            and type(params[0]) is dict
            and set(params[0]) == {"to", "data"}
            and (params[0]["to"].lower(), params[0]["data"].lower())
            in authority["calls"]
            and params[1] in (exact_block, block_tag)
        )
    elif method == "eth_getProof":
        valid = (
            len(params) == 3
            and type(params[0]) is str
            and params[0].lower() in authority["addresses"]
            and type(params[1]) is list
            and all(
                type(slot) is str
                and re.fullmatch(r"0x[0-9a-f]{64}", slot) is not None
                for slot in params[1]
            )
            and params[2] in (exact_block, block_tag)
        )
    else:
        valid = False
    if not valid:
        raise ValueError("historical relay request is outside scenario")
    return None


def _canonical_historical_storage_slot(value: Any) -> str:
    if type(value) is not str:
        raise ValueError("historical relay storage slot is invalid")
    if re.fullmatch(r"0x[0-9a-f]{64}", value) is not None:
        return value
    if re.fullmatch(r"0x(?:0|[1-9a-f][0-9a-f]{0,63})", value) is None:
        raise ValueError("historical relay storage slot is invalid")
    try:
        slot = int(value[2:], 16)
    except ValueError:
        raise ValueError("historical relay storage slot is invalid") from None
    return "0x" + slot.to_bytes(32, "big").hex()


def _canonicalize_historical_relay_request_slots(
    request: Mapping[str, Any]
) -> Mapping[str, Any]:
    method = request["method"]
    params = request["params"]
    if method == "eth_getStorageAt" and len(params) == 3:
        params[1] = _canonical_historical_storage_slot(params[1])
    elif method == "eth_getProof" and len(params) == 3:
        if type(params[1]) is not list:
            raise ValueError("historical relay storage slot is invalid")
        params[1] = [
            _canonical_historical_storage_slot(slot) for slot in params[1]
        ]
    return request


def _validate_historical_relay_response(
    *, authority: Mapping[str, Any], request: Mapping[str, Any],
    response: Mapping[str, Any],
) -> None:
    method = request.get("method")
    if method not in ("eth_getBlockByNumber", "eth_getBlockByHash"):
        return None
    result = response.get("result")
    if type(result) is not dict:
        raise ValueError("historical relay response is invalid")
    import scripts.historical_foundry_scan as scan
    try:
        normalized = scan._normalized_from_raw(result)
        expected = scan._validate_normalized_header(
            dict(authority["fork_header"])
        )
    except (TypeError, ValueError):
        raise ValueError("historical relay response is invalid") from None
    transactions = result.get("transactions")
    if (
        normalized != expected
        or type(transactions) is not list
        or len(transactions) > 65_536
    ):
        raise ValueError("historical relay response is invalid")
    full_transactions = (
        method == "eth_getBlockByNumber"
        and request.get("params")
        == [hex(authority["block_number"]), True]
    )
    hashes = set()
    for index, transaction in enumerate(transactions):
        if full_transactions:
            if (
                type(transaction) is not dict
                or type(transaction.get("hash")) is not str
                or re.fullmatch(r"0x[0-9a-f]{64}", transaction["hash"])
                is None
                or transaction.get("blockHash") != authority["block_hash"]
                or transaction.get("blockNumber")
                != hex(authority["block_number"])
                or transaction.get("transactionIndex") != hex(index)
            ):
                raise ValueError("historical relay response is invalid")
            transaction_hash = transaction["hash"]
        else:
            if (
                type(transaction) is not str
                or re.fullmatch(r"0x[0-9a-f]{64}", transaction) is None
            ):
                raise ValueError("historical relay response is invalid")
            transaction_hash = transaction
        if transaction_hash in hashes:
            raise ValueError("historical relay response is invalid")
        hashes.add(transaction_hash)
    return None


def _relay_historical_archive_call(
    *, relay_lease: _HistoricalRelayScenarioFacade,
    canonical_request_bytes: bytes
) -> bytes:
    facade = _require_historical_relay_scenario_facade(relay_lease)
    lease = facade._lease
    if (
        type(canonical_request_bytes) is not bytes
        or not canonical_request_bytes
        or len(canonical_request_bytes) > 4_194_304
    ):
        raise ValueError("historical relay request is invalid")
    request = _decode_historical_relay_json(
        canonical_request_bytes, limit=4_194_304, require_canonical=False
    )
    if (
        type(request) is not dict
        or set(request) != {"id", "jsonrpc", "method", "params"}
        or type(request["id"]) is not int
        or request["id"] < 0
        or request["jsonrpc"] != "2.0"
        or type(request["method"]) is not str
        or request["method"] not in _HISTORICAL_RELAY_METHODS
        or type(request["params"]) is not list
    ):
        raise ValueError("historical relay request is invalid")
    _validate_archive_json_value(request)
    request = _canonicalize_historical_relay_request_slots(request)
    canonical_request_bytes = _archive_canonical_bytes(request)
    if len(canonical_request_bytes) > 4_194_304:
        raise ValueError("historical relay request is invalid")
    _validate_historical_relay_scenario_request(
        authority=facade._authority, request=request
    )
    started = _initial_clock_sample(lease._clock)
    if started is None or started < lease._last_clock:
        raise ValueError("historical relay clock is invalid")
    object.__setattr__(lease, "_last_clock", started)
    remaining = min(
        30.0, lease._run_deadline - started,
        facade._scenario_deadline - started,
    )
    if remaining <= 0:
        raise TimeoutError("historical relay deadline expired")
    try:
        response_bytes = lease._operation(canonical_request_bytes, remaining)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        raise ValueError("historical relay upstream failed") from None
    finished = _initial_clock_sample(lease._clock)
    if finished is None or finished < started:
        raise ValueError("historical relay clock is invalid")
    object.__setattr__(lease, "_last_clock", finished)
    if (
        finished >= lease._run_deadline
        or finished >= facade._scenario_deadline
        or finished - started >= 30.0
    ):
        raise TimeoutError("historical relay deadline expired")
    if type(response_bytes) is not bytes or len(response_bytes) > 67_108_864:
        raise ValueError("historical relay response is invalid")
    response = _decode_historical_relay_json(
        response_bytes, limit=67_108_864, require_canonical=False
    )
    canonical_response_bytes = _archive_canonical_bytes(response)
    canonical_response = _decode_historical_relay_json(
        canonical_response_bytes, limit=67_108_864
    )
    if (
        type(response) is not dict
        or set(response) not in (
            {"id", "jsonrpc", "result"}, {"error", "id", "jsonrpc"}
        )
        or response.get("id") != request["id"]
        or response.get("jsonrpc") != "2.0"
        or canonical_response != response
    ):
        raise ValueError("historical relay response is invalid")
    _validate_archive_json_value(response)
    _validate_historical_relay_response(
        authority=facade._authority, request=request, response=response
    )
    return bytes(canonical_response_bytes)

_PRODUCTION_SOURCE_MEMBERS = (
    ("source:atomic_publication", "scripts.atomic_publication", "scripts/atomic_publication.py"),
    ("source:bootstrap_historical_foundry_toolchain", "scripts.bootstrap_historical_foundry_toolchain", "scripts/bootstrap_historical_foundry_toolchain.py"),
    ("source:bounded_json", "scripts.bounded_json", "scripts/bounded_json.py"),
    ("source:bounded_snapshot_merge", "scripts.bounded_snapshot_merge", "scripts/bounded_snapshot_merge.py"),
    ("source:cex_instrument_lifecycle", "scripts.cex_instrument_lifecycle", "scripts/cex_instrument_lifecycle.py"),
    ("source:collection_deadline", "scripts.collection_deadline", "scripts/collection_deadline.py"),
    ("source:execution_cost", "scripts.execution_cost", "scripts/execution_cost.py"),
    ("source:fact_quality", "scripts.fact_quality", "scripts/fact_quality.py"),
    ("source:fetch_cex", "scripts.fetch_cex", "scripts/fetch_cex.py"),
    ("source:fetch_cex_depth", "scripts.fetch_cex_depth", "scripts/fetch_cex_depth.py"),
    ("source:historical_foundry_contracts", "scripts.historical_foundry_contracts", "scripts/historical_foundry_contracts.py"),
    ("source:historical_foundry_anvil", None, "scripts/historical_foundry_anvil.py"),
    ("source:historical_foundry_rpc", "scripts.historical_foundry_rpc", "scripts/historical_foundry_rpc.py"),
    ("source:market_lifecycle_reviews", "scripts.market_lifecycle_reviews", "scripts/market_lifecycle_reviews.py"),
    ("source:publication_gate", "scripts.publication_gate", "scripts/publication_gate.py"),
    ("source:quality_outcomes", "scripts.quality_outcomes", "scripts/quality_outcomes.py"),
    ("source:route_cost_evidence", "scripts.route_cost_evidence", "scripts/route_cost_evidence.py"),
    ("source:route_quantity", "scripts.route_quantity", "scripts/route_quantity.py"),
    ("source:timestamp_contract", "scripts.timestamp_contract", "scripts/timestamp_contract.py"),
    ("config:replay_policy", None, "config/historical_foundry_replay_policy.json"),
    ("config:replay_authority", None, "config/historical_foundry_replay_authority.json"),
    ("config:replay_toolchain", None, "config/historical_foundry_replay_toolchain.json"),
    ("build:foundry_toml", None, "foundry.toml"),
    ("build:foundry_lock", None, "foundry.lock"),
    ("build:gitmodules", None, ".gitmodules"),
    ("build:executor_source", None, "foundry/src/TwoVenueV2Executor.sol"),
    ("source:historical_foundry_scan", None, "scripts/historical_foundry_scan.py"),
    ("source:historical_foundry_storage", None, "scripts/historical_foundry_storage.py"),
)

_ARCHIVE_ERROR_PAIRS = frozenset({
    ("authority_mismatch", "preflight_invalid"),
    ("authority_mismatch", "context_invalid"),
    ("authority_mismatch", "context_closed"),
    ("authority_mismatch", "logical_batch_scope_invalid"),
    ("authority_mismatch", "request_invalid"),
    ("authority_mismatch", "endpoint_identity_mismatch"),
    ("authority_mismatch", "response_identity_invalid"),
    ("authority_mismatch", "final_identity_drift"),
    ("authority_mismatch", "historical_window_context_not_fresh"),
    ("authority_mismatch", "historical_window_specialized_batch_required"),
    ("authority_mismatch", "historical_window_transfer_outstanding"),
    ("authority_mismatch", "historical_window_spool_handoff_failed"),
    ("authority_mismatch", "historical_window_reconciliation_mismatch"),
    ("authority_mismatch", "historical_window_capability_invalid"),
    ("archive_state_unavailable", "endpoint_missing"),
    ("archive_state_unavailable", "endpoint_invalid"),
    ("archive_state_unavailable", "attempt_timeout"),
    ("archive_state_unavailable", "collection_timeout"),
    ("archive_state_unavailable", "transport_unavailable"),
    ("archive_state_unavailable", "redirect_forbidden"),
    ("archive_state_unavailable", "http_413"),
    ("archive_state_unavailable", "http_status"),
    ("archive_state_unavailable", "response_decode_invalid"),
    ("archive_state_unavailable", "response_encoding_unsupported"),
    ("archive_state_unavailable", "response_resource_limit"),
    ("archive_state_unavailable", "json_rpc_error"),
})


class _ArchiveRpcError(RuntimeError):
    """One secret-free closed archive failure."""

    __slots__ = ("_reason_code", "_failure_kind")

    def __init__(self, reason_code: str, failure_kind: str) -> None:
        pair = (reason_code, failure_kind)
        if type(reason_code) is not str or type(failure_kind) is not str or pair not in _ARCHIVE_ERROR_PAIRS:
            raise ValueError("historical archive RPC classification is invalid")
        RuntimeError.__setattr__(self, "_reason_code", reason_code)
        RuntimeError.__setattr__(self, "_failure_kind", failure_kind)
        super().__init__(
            "historical archive RPC failure: {}/{}".format(
                reason_code, failure_kind
            )
        )
        RuntimeError.__setattr__(self, "__suppress_context__", True)

    def __init_subclass__(cls, **_kwargs: Any) -> None:
        raise TypeError("_ArchiveRpcError is sealed")

    @property
    def reason_code(self) -> str:
        return RuntimeError.__getattribute__(self, "_reason_code")

    @property
    def failure_kind(self) -> str:
        return RuntimeError.__getattribute__(self, "_failure_kind")

    def __getattribute__(self, name: str) -> Any:
        if name == "__dict__":
            return {
                "reason_code": RuntimeError.__getattribute__(
                    self, "_reason_code"
                ),
                "failure_kind": RuntimeError.__getattribute__(
                    self, "_failure_kind"
                ),
            }
        return RuntimeError.__getattribute__(self, name)

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("_ArchiveRpcError is immutable")

    def __copy__(self) -> Any:
        raise TypeError("_ArchiveRpcError is not copyable")

    def __deepcopy__(self, _memo: Any) -> Any:
        raise TypeError("_ArchiveRpcError is not copyable")

    def __reduce__(self) -> Any:
        raise TypeError("_ArchiveRpcError is not serializable")

    def __reduce_ex__(self, _protocol: int) -> Any:
        raise TypeError("_ArchiveRpcError is not serializable")


class _ArchiveDeadlineExpired(TimeoutError):
    pass


class _ArchiveDecoderClockInvalid(BoundedJsonError):
    def __init__(self) -> None:
        super().__init__("unavailable", "monotonic_invalid")


class _FrozenArchiveList(tuple):
    """Internal immutable marker that detaches back to an exact list."""

    __slots__ = ()


def _archive_error(pair: Tuple[str, str]) -> _ArchiveRpcError:
    return _ArchiveRpcError(pair[0], pair[1])


def _raise_archive_error(pair: Tuple[str, str]) -> None:
    raise _archive_error(pair) from None


def _detach_archive_value(value: Any, allow_decimal: bool) -> Any:
    if type(value) in (dict, MappingProxyType):
        return {
            key: _detach_archive_value(nested, allow_decimal)
            for key, nested in value.items()
        }
    if type(value) in (list, _FrozenArchiveList):
        return [_detach_archive_value(nested, allow_decimal) for nested in value]
    if type(value) is tuple:
        return tuple(
            _detach_archive_value(nested, allow_decimal) for nested in value
        )
    if type(value) is bytes:
        return bytes(value)
    if allow_decimal and type(value) is Decimal:
        return value
    if value is None or type(value) in (str, int, bool):
        return value
    raise ValueError("historical archive projection type is invalid")


def _detached_archive_value(value: Any) -> Any:
    return _detach_archive_value(value, False)


def _detached_archive_response_value(value: Any) -> Any:
    return _detach_archive_value(value, True)


def _frozen_archive_value(value: Any) -> Any:
    if type(value) is dict:
        return MappingProxyType({
            key: _frozen_archive_value(nested)
            for key, nested in value.items()
        })
    if type(value) is list:
        return _FrozenArchiveList(
            _frozen_archive_value(nested) for nested in value
        )
    if type(value) is tuple:
        return tuple(_frozen_archive_value(nested) for nested in value)
    if type(value) is bytes:
        return bytes(value)
    if value is None or type(value) in (str, int, bool):
        return value
    raise ValueError("historical archive projection type is invalid")


def _archive_canonical_bytes(value: Any) -> bytes:
    failed = False
    result = None
    try:
        result = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError):
        failed = True
    if failed or result is None:
        _raise_archive_error(("authority_mismatch", "request_invalid"))
    return result


def _validate_percent_escapes(value: str) -> bool:
    index = 0
    while index < len(value):
        if value[index] != "%":
            index += 1
            continue
        if (
            index + 2 >= len(value)
            or value[index + 1] not in "0123456789abcdefABCDEF"
            or value[index + 2] not in "0123456789abcdefABCDEF"
        ):
            return False
        index += 3
    return True


def _canonicalize_archive_endpoint(
    endpoint: Any,
) -> Tuple[Dict[str, Any], bytes, str]:
    invalid = False
    if type(endpoint) is not str or not endpoint:
        invalid = True
    elif any(ord(character) < 0x21 or ord(character) > 0x7E for character in endpoint):
        invalid = True
    elif "\\" in endpoint or "#" in endpoint:
        invalid = True
    if invalid:
        _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
    try:
        split = urllib.parse.urlsplit(endpoint)
    except Exception:
        split = None
    if split is None:
        _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
    delimiter = endpoint.find("://")
    if delimiter <= 0 or endpoint[:delimiter].lower() != "https":
        _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
    remainder = endpoint[delimiter + 3:]
    authority_stop = len(remainder)
    for marker in ("/", "?"):
        position = remainder.find(marker)
        if position >= 0:
            authority_stop = min(authority_stop, position)
    authority = remainder[:authority_stop]
    tail = remainder[authority_stop:]
    if (
        not authority
        or "@" in authority
        or split.scheme.lower() != "https"
        or split.netloc != authority
        or split.fragment
    ):
        _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))

    explicit_port = None
    ipv6 = False
    if authority.startswith("["):
        close = authority.find("]")
        if close <= 1 or authority.count("[") != 1 or authority.count("]") != 1:
            _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
        host_text = authority[1:close]
        port_text = authority[close + 1:]
        if "%" in host_text or "." in host_text:
            _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
        if port_text:
            if not port_text.startswith(":"):
                _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
            explicit_port = port_text[1:]
        host_failed = False
        host = None
        try:
            host = ipaddress.IPv6Address(host_text).compressed.lower()
        except ipaddress.AddressValueError:
            host_failed = True
        if host_failed or host is None:
            _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
        ipv6 = True
    else:
        if authority.count(":") > 1:
            _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
        if ":" in authority:
            host_text, explicit_port = authority.rsplit(":", 1)
        else:
            host_text = authority
        if not host_text:
            _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
        if all(character in "0123456789." for character in host_text):
            tokens = host_text.split(".")
            if len(tokens) != 4:
                _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
            octets = []
            for token in tokens:
                if (
                    not token
                    or not token.isascii()
                    or not token.isdecimal()
                    or (len(token) > 1 and token.startswith("0"))
                ):
                    _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
                octet = int(token)
                if octet > 255:
                    _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
                octets.append(str(octet))
            host = ".".join(octets)
        else:
            host = host_text.lower()
            if len(host.encode("ascii")) > 253:
                _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
            labels = host.split(".")
            for label in labels:
                if (
                    not 1 <= len(label) <= 63
                    or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", label, re.ASCII) is None
                ):
                    _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
    if explicit_port is None:
        port = 443
    else:
        if (
            not explicit_port
            or not explicit_port.isascii()
            or not explicit_port.isdecimal()
            or (len(explicit_port) > 1 and explicit_port.startswith("0"))
        ):
            _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
        port = int(explicit_port)
        if not 1 <= port <= 65_535:
            _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))

    if "?" in tail:
        path, query = tail.split("?", 1)
        if query == "":
            _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
    else:
        path, query = tail, ""
    if not path:
        path = "/"
    if (
        not path.startswith("/")
        or not _validate_percent_escapes(path)
        or not _validate_percent_escapes(query)
    ):
        _raise_archive_error(("archive_state_unavailable", "endpoint_invalid"))
    projection = {
        "host": host,
        "path": path,
        "port": port,
        "query": query,
        "scheme": "https",
    }
    canonical = _archive_canonical_bytes(projection)
    wire_host = "[{}]".format(host) if ipv6 else host
    connection_url = "https://{}:{}{}".format(wire_host, port, path)
    if query:
        connection_url += "?" + query
    return projection, canonical, connection_url


def _validate_archive_json_value(value: Any) -> None:
    nodes = 0
    scalar_bytes = 0
    pending = [(value, 0)]
    while pending:
        current, depth = pending.pop()
        nodes += 1
        if nodes > _ARCHIVE_JSON_NODES or depth > _ARCHIVE_JSON_DEPTH:
            _raise_archive_error(("authority_mismatch", "request_invalid"))
        if type(current) is dict:
            for key, nested in current.items():
                if type(key) is not str:
                    _raise_archive_error(("authority_mismatch", "request_invalid"))
                encode_failed = False
                encoded = None
                try:
                    encoded = key.encode("utf-8")
                except UnicodeEncodeError:
                    encode_failed = True
                if encode_failed or encoded is None:
                    _raise_archive_error(("authority_mismatch", "request_invalid"))
                if len(encoded) > _ARCHIVE_JSON_STRING_BYTES:
                    _raise_archive_error(("authority_mismatch", "request_invalid"))
                scalar_bytes += len(encoded)
                pending.append((nested, depth + 1))
        elif type(current) is list:
            pending.extend((nested, depth + 1) for nested in current)
        elif type(current) is str:
            encode_failed = False
            encoded = None
            try:
                encoded = current.encode("utf-8")
            except UnicodeEncodeError:
                encode_failed = True
            if encode_failed or encoded is None:
                _raise_archive_error(("authority_mismatch", "request_invalid"))
            if len(encoded) > _ARCHIVE_JSON_STRING_BYTES:
                _raise_archive_error(("authority_mismatch", "request_invalid"))
            scalar_bytes += len(encoded)
        elif type(current) is int:
            encode_failed = False
            encoded = None
            try:
                encoded = str(current).encode("ascii")
            except (ValueError, OverflowError):
                encode_failed = True
            if encode_failed or encoded is None:
                _raise_archive_error(("authority_mismatch", "request_invalid"))
            if len(encoded) > _ARCHIVE_JSON_NUMERIC_TOKEN_BYTES:
                _raise_archive_error(("authority_mismatch", "request_invalid"))
            scalar_bytes += len(encoded)
        elif current is None or type(current) is bool:
            scalar_bytes += len(str(current).lower())
        else:
            _raise_archive_error(("authority_mismatch", "request_invalid"))
        if scalar_bytes > _ARCHIVE_JSON_SCALAR_BYTES:
            _raise_archive_error(("authority_mismatch", "request_invalid"))


def _freeze_archive_request_rows(
    request_rows: Any,
) -> Tuple[Tuple[Dict[str, Any], ...], bytes, Tuple[int, ...]]:
    if type(request_rows) not in (list, tuple) or not request_rows:
        _raise_archive_error(("authority_mismatch", "request_invalid"))
    rows = []
    request_ids = []
    for row in request_rows:
        if type(row) is not dict or set(row) != {"jsonrpc", "id", "method", "params"}:
            _raise_archive_error(("authority_mismatch", "request_invalid"))
        if type(row["jsonrpc"]) is not str or row["jsonrpc"] != "2.0":
            _raise_archive_error(("authority_mismatch", "request_invalid"))
        if type(row["id"]) is not int or row["id"] <= 0 or row["id"] in request_ids:
            _raise_archive_error(("authority_mismatch", "request_invalid"))
        if type(row["method"]) is not str or row["method"] not in _ARCHIVE_METHOD_SET:
            _raise_archive_error(("authority_mismatch", "request_invalid"))
        if type(row["params"]) is not list:
            _raise_archive_error(("authority_mismatch", "request_invalid"))
        _validate_archive_json_value(row)
        detached = _detached_archive_value(row)
        rows.append(detached)
        request_ids.append(row["id"])
    canonical = _archive_canonical_bytes(rows)
    if len(canonical) > _ARCHIVE_REQUEST_BODY_BYTES or b"\n" in canonical:
        _raise_archive_error(("authority_mismatch", "request_invalid"))
    return tuple(rows), canonical, tuple(request_ids)


def _initialize_production_archive_types():
    provenance = object()

    class _ProductionArchiveRpcRunContext:
        __slots__ = (
            "_state", "_clock", "_last_clock", "_collection_deadline",
            "_key", "_endpoint_projection", "_endpoint_bytes",
            "_connection_url", "_endpoint_identity", "_operation",
            "_preflight", "_opening_identity", "_active_scope",
            "_reserved_scope", "_logical_summaries", "_records",
            "_next_logical_batch_index", "_next_exchange_index",
            "_historical_window_lock", "_historical_window_consumer",
            "_historical_window_close", "_relay_lease", "_relay_moved",
        )

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive production provenance is invalid")
            for name in self.__slots__:
                object.__setattr__(self, name, values[name])

        def __repr__(self) -> str:
            return "_ProductionArchiveRpcRunContext(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ProductionArchiveRpcRunContext is immutable")

        def __copy__(self) -> Any:
            raise TypeError("_ProductionArchiveRpcRunContext is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ProductionArchiveRpcRunContext is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ProductionArchiveRpcRunContext is not serializable")

        def __enter__(self) -> "_ProductionArchiveRpcRunContext":
            if self._state != "active":
                _raise_archive_error(("authority_mismatch", "context_closed"))
            return self

        def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
            closer = self._historical_window_close
            if callable(closer):
                closer()
                return
            _abandon_archive_context(self)

    class _ProductionArchiveRpcLogicalBatchScope:
        __slots__ = (
            "_context", "_root_rows", "_root_bytes", "_root_ids",
            "_logical_batch_index", "_wire_remaining", "_decoded_remaining",
            "_wire_count", "_decoded_count", "_attempt_count",
            "_recoverable_failures", "_success_exchange_indices",
            "_pending", "_entered", "_consumed", "_implicit",
        )

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive production scope provenance is invalid")
            for name in self.__slots__:
                object.__setattr__(self, name, values[name])

        def __repr__(self) -> str:
            return "_ProductionArchiveRpcLogicalBatchScope(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ProductionArchiveRpcLogicalBatchScope is immutable")

        def __copy__(self) -> Any:
            raise TypeError("_ProductionArchiveRpcLogicalBatchScope is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ProductionArchiveRpcLogicalBatchScope is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ProductionArchiveRpcLogicalBatchScope is not serializable")

        def __enter__(self) -> "_ProductionArchiveRpcLogicalBatchScope":
            with self._context._historical_window_lock:
                _enter_archive_scope(self)
            return self

        def __exit__(self, error_type: Any, error: Any, traceback: Any) -> None:
            with self._context._historical_window_lock:
                _exit_archive_scope(self, error_type, error, traceback)

    class _ProductionArchiveRpcSuccessRecord:
        __slots__ = ("_projection",)

        def __init__(self, projection: Mapping[str, Any], *, _provenance: object = None) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive production record provenance is invalid")
            object.__setattr__(self, "_projection", _frozen_archive_value(dict(projection)))

        def __repr__(self) -> str:
            return "_ProductionArchiveRpcSuccessRecord(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ProductionArchiveRpcSuccessRecord is immutable")

        def __copy__(self) -> Any:
            raise TypeError("_ProductionArchiveRpcSuccessRecord is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ProductionArchiveRpcSuccessRecord is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ProductionArchiveRpcSuccessRecord is not serializable")

    class _ProductionArchiveRpcFinalization(MappingABC):
        __slots__ = ("_projection",)

        def __init__(self, projection: Mapping[str, Any], *, _provenance: object = None) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive production finalization provenance is invalid")
            object.__setattr__(self, "_projection", _frozen_archive_value(dict(projection)))

        def __repr__(self) -> str:
            return "_ProductionArchiveRpcFinalization(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ProductionArchiveRpcFinalization is immutable")

        def __getitem__(self, key: str) -> Any:
            return _detached_archive_value(self._projection[key])

        def __iter__(self) -> Iterator[str]:
            return iter(self._projection)

        def __len__(self) -> int:
            return len(self._projection)

        def __copy__(self) -> Any:
            raise TypeError("_ProductionArchiveRpcFinalization is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ProductionArchiveRpcFinalization is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ProductionArchiveRpcFinalization is not serializable")

    def issue_context(**values: Any) -> _ProductionArchiveRpcRunContext:
        values.setdefault("_historical_window_lock", threading.RLock())
        values.setdefault("_historical_window_consumer", None)
        values.setdefault("_historical_window_close", None)
        values.setdefault("_relay_lease", None)
        values.setdefault("_relay_moved", False)
        return _ProductionArchiveRpcRunContext(_provenance=provenance, **values)

    def issue_scope(**values: Any) -> _ProductionArchiveRpcLogicalBatchScope:
        return _ProductionArchiveRpcLogicalBatchScope(_provenance=provenance, **values)

    def issue_record(projection: Mapping[str, Any]) -> _ProductionArchiveRpcSuccessRecord:
        return _ProductionArchiveRpcSuccessRecord(projection, _provenance=provenance)

    def issue_finalization(projection: Mapping[str, Any]) -> _ProductionArchiveRpcFinalization:
        return _ProductionArchiveRpcFinalization(projection, _provenance=provenance)

    return (
        _ProductionArchiveRpcRunContext,
        _ProductionArchiveRpcLogicalBatchScope,
        _ProductionArchiveRpcSuccessRecord,
        _ProductionArchiveRpcFinalization,
        issue_context,
        issue_scope,
        issue_record,
        issue_finalization,
    )


(
    _ProductionArchiveRpcRunContext,
    _ProductionArchiveRpcLogicalBatchScope,
    _ProductionArchiveRpcSuccessRecord,
    _ProductionArchiveRpcFinalization,
    _issue_production_context,
    _issue_production_scope,
    _issue_production_record,
    _issue_production_finalization,
) = _initialize_production_archive_types()
del _initialize_production_archive_types


_historical_window_claimed_core_gate = {
    "claimed": {},
    "open": {},
    "finalize": {},
}


def _initialize_production_historical_window_types(core_gate: Any):
    provenance = object()
    opened_registry = {}
    claim_registry = {}
    logical_scope_registry = {}
    capsule_registry = {}

    def register_claim(
        claim: "_ProductionHistoricalWindowRunClaim",
        record: Dict[str, Any],
    ) -> None:
        claim_id = id(claim)

        def retire(reference: Any) -> None:
            entry = claim_registry.get(claim_id)
            if entry is not None and entry[0] is reference:
                claim_registry.pop(claim_id, None)

        reference = weakref.ref(claim, retire)
        claim_registry[claim_id] = (reference, record)

    def closed_function_value(function: Any, name: str) -> Any:
        code = getattr(function, "__code__", None)
        closure = getattr(function, "__closure__", None)
        if code is None or closure is None:
            raise ValueError("historical bound closure differs")
        names = code.co_freevars
        if name not in names or len(names) != len(closure):
            raise ValueError("historical bound closure differs")
        return closure[names.index(name)].cell_contents

    def reject_construction() -> None:
        _raise_archive_error((
            "authority_mismatch", "historical_window_context_not_fresh"
        ))

    class _ProductionHistoricalWindowRunClaim:
        """Closure-issued authority over one fresh production RPC context."""

        __slots__ = ("__weakref__",)

        def __new__(cls, *, _provenance: object = None):
            if _provenance is not provenance:
                reject_construction()
            return object.__new__(cls)

        def __init_subclass__(cls, **_kwargs: Any) -> None:
            raise TypeError("_ProductionHistoricalWindowRunClaim is sealed")

        def __repr__(self) -> str:
            return "_ProductionHistoricalWindowRunClaim(<redacted>)"

        def __copy__(self) -> Any:
            raise TypeError("_ProductionHistoricalWindowRunClaim is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ProductionHistoricalWindowRunClaim is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ProductionHistoricalWindowRunClaim is not serializable")

        def close(self) -> None:
            entry = claim_registry.get(id(self))
            if entry is None or entry[0]() is not self:
                reject_construction()
            record = entry[1]
            if record.get("state") == "closed":
                return None
            context = record["context"]
            with context._historical_window_lock:
                control = None
                try:
                    terminalize_claim_context(context, "abandoned")
                except BaseException as error:
                    control = error
                finally:
                    record["state"] = "closed"
                    object.__setattr__(
                        context, "_historical_window_consumer", None
                    )
                    object.__setattr__(
                        context, "_historical_window_close", None
                    )
                    opened_registry.pop(id(context), None)
                    record.clear()
                    record["state"] = "closed"
                if control is not None:
                    raise control
            return None

        def __enter__(self) -> "_ProductionHistoricalWindowRunClaim":
            require_live_claim(self)
            return self

        def __exit__(
            self,
            error_type: Any,
            error: Any,
            traceback: Any,
        ) -> None:
            del error_type, error, traceback
            self.close()

    def terminalize_claim_context(
        context: "_ProductionArchiveRpcRunContext", state: str
    ) -> None:
        core_gate["claimed"].pop(id(context), None)
        core_gate["open"].pop(id(context), None)
        core_gate["finalize"].pop(id(context), None)
        if context._state not in ("active", "finalizing"):
            return
        key = context._key
        if type(key) is bytearray:
            for index in range(len(key)):
                key[index] = 0
        preflight = context._preflight
        closer = getattr(preflight, "close", None)
        control = None
        if callable(closer):
            try:
                closer()
            except BaseException as error:
                if not isinstance(error, Exception):
                    control = error
        object.__setattr__(context, "_key", None)
        object.__setattr__(context, "_endpoint_projection", None)
        object.__setattr__(context, "_endpoint_bytes", None)
        object.__setattr__(context, "_connection_url", None)
        object.__setattr__(context, "_endpoint_identity", None)
        object.__setattr__(context, "_operation", None)
        object.__setattr__(context, "_preflight", None)
        object.__setattr__(context, "_opening_identity", None)
        object.__setattr__(context, "_active_scope", None)
        object.__setattr__(context, "_reserved_scope", None)
        context._logical_summaries.clear()
        context._records.clear()
        object.__setattr__(context, "_state", state)
        if control is not None:
            raise control

    def fail_undelivered_claim_authority(
        claim: "_ProductionHistoricalWindowRunClaim",
        claim_record: Dict[str, Any],
        original_error: BaseException,
    ) -> None:
        context = claim_record.get("context")
        spool = claim_record.get("spool")
        cleanup_control = None
        if type(context) is _ProductionArchiveRpcRunContext:
            with context._historical_window_lock:
                try:
                    terminalize_claim_context(context, "failed")
                except BaseException as error:
                    if not isinstance(error, Exception):
                        cleanup_control = error
                finally:
                    for scope_id, scope_entry in tuple(
                        logical_scope_registry.items()
                    ):
                        if scope_entry[1].get("claim_record") is claim_record:
                            scope_entry[1]["state"] = "consumed"
                            logical_scope_registry.pop(scope_id, None)
                    object.__setattr__(
                        context, "_historical_window_consumer", None
                    )
                    object.__setattr__(
                        context, "_historical_window_close", None
                    )
                    opened_registry.pop(id(context), None)
                    claim_registry.pop(id(claim), None)
                    claim_record.clear()
                    claim_record["state"] = "closed"
                    claim_record["phase"] = "closed"
        else:
            claim_registry.pop(id(claim), None)
        if spool is not None:
            try:
                spool.close()
            except BaseException as error:
                if (
                    not isinstance(error, Exception)
                    and cleanup_control is None
                ):
                    cleanup_control = error
        if not isinstance(original_error, Exception):
            raise original_error
        if cleanup_control is not None:
            raise cleanup_control
        raise original_error

    def require_live_claim(
        claim: "_ProductionHistoricalWindowRunClaim",
    ) -> Dict[str, Any]:
        if type(claim) is not _ProductionHistoricalWindowRunClaim:
            reject_construction()
        entry = claim_registry.get(id(claim))
        if entry is None or entry[0]() is not claim:
            reject_construction()
        record = entry[1]
        context = record.get("context")
        if context is None:
            _raise_archive_error(("authority_mismatch", "context_closed"))
        with context._historical_window_lock:
            if (
                record["state"] != "claimed"
                or context._state != "active"
                or context._historical_window_consumer is not claim
            ):
                _raise_archive_error(("authority_mismatch", "context_closed"))
        return record

    def mark_opened(
        context: "_ProductionArchiveRpcRunContext",
    ) -> "_ProductionArchiveRpcRunContext":
        if type(context) is not _ProductionArchiveRpcRunContext:
            reject_construction()
        with context._historical_window_lock:
            preflight = context._preflight
            opened_registry[id(context)] = (context, {
                "config": getattr(preflight, "config", None),
                "opening_identity": context._opening_identity,
                "state": "fresh",
            })
        return context

    def authority_class(name: str):
        def new(cls, *, _provenance: object = None):
            if _provenance is not provenance:
                reject_construction()
            return object.__new__(cls)

        def init_subclass(cls, **_kwargs: Any) -> None:
            raise TypeError(name + " is sealed")

        def representation(self) -> str:
            return name + "(<redacted>)"

        def reject_copy(self, *_args: Any) -> Any:
            raise TypeError(name + " is not copyable")

        def reject_reduce(self) -> Any:
            raise TypeError(name + " is not serializable")

        return type(name, (), {
            "__slots__": (),
            "__new__": new,
            "__init_subclass__": classmethod(init_subclass),
            "__repr__": representation,
            "__copy__": reject_copy,
            "__deepcopy__": reject_copy,
            "__reduce__": reject_reduce,
            "__module__": __name__,
        })

    _ProductionHistoricalWindowLogicalBatchScope = authority_class(
        "_ProductionHistoricalWindowLogicalBatchScope"
    )
    _ClaimedHistoricalWindowSourceCapsule = authority_class(
        "_ClaimedHistoricalWindowSourceCapsule"
    )

    def _enter_logical_scope_core(
        self: "_ProductionHistoricalWindowLogicalBatchScope",
        delivery_guard: List[Any],
    ) -> "_ProductionHistoricalWindowLogicalBatchScope":
        if type(self) is not _ProductionHistoricalWindowLogicalBatchScope:
            reject_construction()
        entry = logical_scope_registry.get(id(self))
        if entry is None or entry[0] is not self:
            reject_construction()
        logical_record = entry[1]
        claim_record = require_live_claim(logical_record["claim"])
        context = logical_record["context"]
        delivery_guard[0] = (
            logical_record["claim"], claim_record, self
        )
        with context._historical_window_lock:
            if (
                logical_record["state"] != "fresh"
                or claim_record is not logical_record["claim_record"]
                or claim_record["phase"] != "bound"
                or context._reserved_scope is not logical_record["underlying"]
                or context._active_scope is not None
            ):
                _raise_archive_error((
                    "authority_mismatch", "logical_batch_scope_invalid"
                ))
            _enter_archive_scope(logical_record["underlying"])
            logical_record["state"] = "active"
        return self

    def enter_logical_scope(
        self: "_ProductionHistoricalWindowLogicalBatchScope",
    ) -> "_ProductionHistoricalWindowLogicalBatchScope":
        delivery_guard = [None]
        try:
            result = _enter_logical_scope_core(self, delivery_guard)
            return result
        except BaseException as error:
            guarded = delivery_guard[0]
            if guarded is None:
                raise
            claim, claim_record, _logical = guarded
            fail_undelivered_claim_authority(
                claim, claim_record, error
            )
        raise _ArchiveInternalFailure()

    def exit_logical_scope(
        self: "_ProductionHistoricalWindowLogicalBatchScope",
        error_type: Any,
        error: Any,
        traceback: Any,
    ) -> None:
        if type(self) is not _ProductionHistoricalWindowLogicalBatchScope:
            reject_construction()
        entry = logical_scope_registry.get(id(self))
        if entry is None or entry[0] is not self:
            reject_construction()
        logical_record = entry[1]
        context = logical_record["context"]
        with context._historical_window_lock:
            if logical_record["state"] != "active":
                _raise_archive_error((
                    "authority_mismatch", "logical_batch_scope_invalid"
                ))
            try:
                _exit_archive_scope(
                    logical_record["underlying"],
                    error_type,
                    error,
                    traceback,
                )
            finally:
                logical_record["state"] = "consumed"
                logical_scope_registry.pop(id(self), None)
        return None

    setattr(
        _ProductionHistoricalWindowLogicalBatchScope,
        "__enter__",
        enter_logical_scope,
    )
    setattr(
        _ProductionHistoricalWindowLogicalBatchScope,
        "__exit__",
        exit_logical_scope,
    )

    def _claim_fresh_core(
        *,
        context: "_ProductionArchiveRpcRunContext",
        delivery_guard: List[Any],
    ) -> "_ProductionHistoricalWindowRunClaim":
        if type(context) is not _ProductionArchiveRpcRunContext:
            reject_construction()
        with context._historical_window_lock:
            entry = opened_registry.get(id(context))
            if entry is None or entry[0] is not context:
                try:
                    terminalize_claim_context(context, "abandoned")
                finally:
                    object.__setattr__(
                        context, "_historical_window_consumer", None
                    )
                    object.__setattr__(
                        context, "_historical_window_close", None
                    )
                reject_construction()
            opening = entry[1]
            preflight = context._preflight
            if (
                opening["state"] != "fresh"
                or context._state != "active"
                or context._historical_window_consumer is not None
                or context._active_scope is not None
                or context._reserved_scope is not None
                or context._records
                or context._logical_summaries
                or context._next_logical_batch_index != 1
                or context._next_exchange_index != 1
                or preflight is None
                or getattr(preflight, "config", None) is not opening["config"]
                or context._opening_identity is not opening["opening_identity"]
            ):
                closer = context._historical_window_close
                if callable(closer):
                    closer()
                else:
                    try:
                        terminalize_claim_context(context, "abandoned")
                    finally:
                        opening["state"] = "closed"
                        opened_registry.pop(id(context), None)
                reject_construction()
            claim = _ProductionHistoricalWindowRunClaim(_provenance=provenance)
            record = {
                "state": "claimed",
                "phase": "claimed",
                "context": context,
                "config": opening["config"],
                "bound_scan": None,
                "bound_storage": None,
                "binding": None,
                "spool": None,
                "transfer_state_checker": None,
                "logical_root_consumer": None,
                "prefinalization": None,
                "prefinalization_digests": None,
                "finalization": None,
            }
            delivery_guard[0] = (claim, record)
            register_claim(claim, record)
            opening["state"] = "claimed"
            object.__setattr__(context, "_historical_window_consumer", claim)
            object.__setattr__(context, "_historical_window_close", claim.close)
            core_gate["claimed"][id(context)] = (context, claim)
            return claim

    def claim_fresh(
        *, context: "_ProductionArchiveRpcRunContext"
    ) -> "_ProductionHistoricalWindowRunClaim":
        register_claim_reference = register_claim
        del register_claim_reference
        delivery_guard = [None]
        try:
            result = _claim_fresh_core(
                context=context, delivery_guard=delivery_guard
            )
            return result
        except BaseException as error:
            guarded = delivery_guard[0]
            if guarded is None:
                raise
            claim, record = guarded
            fail_undelivered_claim_authority(claim, record, error)
        raise _ArchiveInternalFailure()

    def claimed_config(
        *, claim: "_ProductionHistoricalWindowRunClaim"
    ) -> Any:
        return require_live_claim(claim)["config"]

    def bind_scan(
        *, claim: "_ProductionHistoricalWindowRunClaim"
    ) -> None:
        record = require_live_claim(claim)
        if record["phase"] != "resolving":
            reject_construction()
        canonical = sys.modules.get("scripts.historical_foundry_scan")
        main = sys.modules.get("__main__")
        main_spec = getattr(main, "__spec__", None)
        if getattr(main_spec, "name", None) == "scripts.historical_foundry_scan":
            if canonical is not None and canonical is not main:
                _raise_archive_error(("authority_mismatch", "final_identity_drift"))
            module = main
            key = "__main__"
        else:
            module = canonical
            key = "scripts.historical_foundry_scan"
        if (
            module is None
            or getattr(getattr(module, "__spec__", None), "name", None)
            != "scripts.historical_foundry_scan"
        ):
            _raise_archive_error(("authority_mismatch", "final_identity_drift"))
        record["candidate"]["scan"] = (key, module)
        return None

    def bind_storage(
        *, claim: "_ProductionHistoricalWindowRunClaim", module: Any
    ) -> None:
        record = require_live_claim(claim)
        if record["phase"] != "resolving":
            reject_construction()
        canonical = sys.modules.get("scripts.historical_foundry_storage")
        if (
            module is not canonical
            or module is None
            or getattr(getattr(module, "__spec__", None), "name", None)
            != "scripts.historical_foundry_storage"
        ):
            _raise_archive_error(("authority_mismatch", "final_identity_drift"))
        record["candidate"]["storage"] = (
            "scripts.historical_foundry_storage", module
        )
        return None

    def resolve_qualified(module: Any, qualified: str) -> Any:
        value = module
        for component in qualified.split("."):
            value = getattr(value, component)
        return value

    def bound_module_row(
        role: str,
        canonical_name: str,
        actual_key: str,
        module: Any,
        sources: Any,
    ) -> Tuple[Any, ...]:
        try:
            source = sources.files["source:historical_foundry_" + role]
            expected_path = (sources.root / source[3]).resolve(strict=True)
            spec = getattr(module, "__spec__", None)
            origin = getattr(spec, "origin", None)
            file_name = getattr(module, "__file__", None)
            generation = getattr(
                module, "_HISTORICAL_WINDOW_MODULE_GENERATION", None
            )
            if (
                getattr(spec, "name", None) != canonical_name
                or type(origin) is not str
                or type(file_name) is not str
                or Path(origin).resolve(strict=True) != expected_path
                or Path(file_name).resolve(strict=True) != expected_path
                or generation is None
            ):
                _raise_archive_error((
                    "authority_mismatch", "final_identity_drift"
                ))
            objects = tuple(
                resolve_qualified(module, name)
                for object_role, name in _HISTORICAL_WINDOW_BOUND_IDENTITY_NAMES
                if object_role == role
            )
            return (
                role, canonical_name, actual_key, module, generation,
                spec.name, origin, file_name, objects,
            )
        except _ArchiveRpcError:
            raise
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            _raise_archive_error((
                "authority_mismatch", "final_identity_drift"
            ))
        raise _ArchiveInternalFailure()

    def duplicate_into_ledger(fd: int, duplicate_fds: Any) -> int:
        duplicated = os.dup(fd); duplicate_fds.append(duplicated)
        return duplicated

    def duplicate_claimed_sources(
        sources: Any,
        duplicate_fds: Any,
    ) -> Tuple[Tuple[Any, ...], Tuple[Any, ...]]:
        sources.verify()
        try:
            ancestry_rows = []
            ancestry_indices = {}
            for key in ((), ("scripts",)):
                fd, _parent_fd, name, identity = sources.directories[key]
                duplicated = duplicate_into_ledger(fd, duplicate_fds)
                os.set_inheritable(duplicated, False)
                if _archive_directory_identity(os.fstat(duplicated)) != identity:
                    raise ValueError("source ancestry changed")
                parent_index = None if key == () else ancestry_indices[key[:-1]]
                ancestry_indices[key] = len(ancestry_rows)
                ancestry_rows.append((
                    key, duplicated, parent_index, name, identity,
                ))
            source_rows = []
            for role in ("rpc", "scan", "storage", "anvil"):
                row = sources.files["source:historical_foundry_" + role]
                fd, _parent_fd, name, relative, _module_name = row[:5]
                identity, payload, digest = row[5], row[6], row[7]
                duplicated = duplicate_into_ledger(fd, duplicate_fds)
                os.set_inheritable(duplicated, False)
                observed = _read_archive_fd(duplicated)
                expected_bytes = memoryview(payload).tobytes()
                if (
                    _archive_file_identity(os.fstat(duplicated)) != identity
                    or observed != expected_bytes
                    or hashlib.sha256(observed).hexdigest() != digest
                ):
                    raise ValueError("source member changed")
                source_rows.append((
                    role, duplicated, ancestry_indices[("scripts",)], name,
                    relative, identity, expected_bytes, len(expected_bytes),
                    digest,
                ))
            sources.verify()
            return tuple(ancestry_rows), tuple(source_rows)
        except BaseException as original_error:
            control = (
                original_error
                if not isinstance(original_error, Exception) else None
            )
            while duplicate_fds:
                fd = duplicate_fds[-1]
                try:
                    duplicate_fds.pop(); os.close(fd)
                except BaseException as close_error:
                    if (
                        not isinstance(close_error, Exception)
                        and control is None
                    ):
                        control = close_error
            if control is not None and control is not original_error:
                raise control
            raise

    def bind_sources(
        *,
        claim: "_ProductionHistoricalWindowRunClaim",
        spool: "_HistoricalWindowExchangeSpool",
    ) -> "_HistoricalWindowSpoolSourceBinding":
        record = require_live_claim(claim)
        context = record["context"]
        with context._historical_window_lock:
            if record["phase"] != "claimed" or record["binding"] is not None:
                reject_construction()
            record["phase"] = "resolving"
            record["candidate"] = {}
            capsule = None
            pending_duplicate_fds = []
            authenticated_spool = False
            try:
                storage_module = importlib.import_module(
                    "scripts.historical_foundry_storage"
                )
                authenticated_spool = type(spool) is getattr(
                    storage_module, "_HistoricalWindowExchangeSpool", None
                )
                if not authenticated_spool:
                    _raise_archive_error((
                        "authority_mismatch",
                        "historical_window_capability_invalid",
                    ))
                bind_scan(claim=claim)
                bind_storage(claim=claim, module=storage_module)
                rpc_module = sys.modules.get("scripts.historical_foundry_rpc")
                if rpc_module is not sys.modules.get(__name__):
                    _raise_archive_error((
                        "authority_mismatch", "final_identity_drift"
                    ))
                scan_key, scan_module = record["candidate"]["scan"]
                storage_key, storage_module = record["candidate"]["storage"]
                root_consumer = closed_function_value(
                    scan_module._capture_production_historical_window,
                    "_consume_scheduler_logical_root",
                )
                if not callable(root_consumer):
                    _raise_archive_error((
                        "authority_mismatch", "final_identity_drift"
                    ))
                sources = context._preflight.sources
                bound_rows = (
                    bound_module_row(
                        "rpc", "scripts.historical_foundry_rpc",
                        "scripts.historical_foundry_rpc", rpc_module, sources,
                    ),
                    bound_module_row(
                        "scan", "scripts.historical_foundry_scan",
                        scan_key, scan_module, sources,
                    ),
                    bound_module_row(
                        "storage", "scripts.historical_foundry_storage",
                        storage_key, storage_module, sources,
                    ),
                )
                ancestry_rows, source_rows = duplicate_claimed_sources(
                    sources, pending_duplicate_fds
                )
                capsule = _ClaimedHistoricalWindowSourceCapsule(
                    _provenance=provenance
                )
                capsule_registry[id(capsule)] = (capsule, {
                    "state": "prepared",
                    "claim": claim,
                    "spool": spool,
                    "storage_module": storage_module,
                    "payload": (
                        "historical_foundry_claimed_source_payload/v1",
                        ancestry_rows,
                        source_rows,
                        bound_rows,
                    ),
                    "binding": None,
                })
                record["phase"] = "binding"
                binding = spool._bind_claimed_source_authority_from_rpc(
                    claim=claim,
                    bound_rpc_module=rpc_module,
                    bound_scan_module=scan_module,
                    bound_storage_module=storage_module,
                    source_capsule=capsule,
                )
                capsule_record = capsule_registry[id(capsule)][1]
                if (
                    capsule_record["state"] != "consumed"
                    or capsule_record["binding"] is not binding
                    or record["phase"] != "bound"
                    or record["binding"] is not binding
                    or record["spool"] is not spool
                ):
                    _raise_archive_error((
                        "authority_mismatch",
                        "historical_window_spool_handoff_failed",
                    ))
                record["transfer_state_checker"] = (
                    type(spool)._project_bound_rpc_transfer_state
                )
                record["logical_root_consumer"] = root_consumer
                pending_duplicate_fds[:] = []
                record.pop("candidate", None)
                capsule_registry.pop(id(capsule), None)
                return binding
            except BaseException as original_error:
                record["phase"] = "claimed"
                record.pop("candidate", None)
                control = (
                    original_error
                    if not isinstance(original_error, Exception) else None
                )
                storage_owns_sources = (
                    authenticated_spool
                    and record.get("spool") is spool
                    and record.get("binding") is not None
                )
                if capsule is not None:
                    capsule_entry = capsule_registry.get(id(capsule))
                    if capsule_entry is not None:
                        capsule_record = capsule_entry[1]
                        if (
                            not storage_owns_sources
                            and capsule_record["state"]
                            in ("prepared", "moving")
                        ):
                            while pending_duplicate_fds:
                                fd = pending_duplicate_fds[-1]
                                try:
                                    pending_duplicate_fds.pop(); os.close(fd)
                                except BaseException as close_error:
                                    if (
                                        not isinstance(close_error, Exception)
                                        and control is None
                                    ):
                                        control = close_error
                    elif not storage_owns_sources:
                        while pending_duplicate_fds:
                            fd = pending_duplicate_fds[-1]
                            try:
                                pending_duplicate_fds.pop(); os.close(fd)
                            except BaseException as close_error:
                                if (
                                    not isinstance(close_error, Exception)
                                    and control is None
                                ):
                                    control = close_error
                elif not storage_owns_sources:
                    while pending_duplicate_fds:
                        fd = pending_duplicate_fds[-1]
                        try:
                            pending_duplicate_fds.pop(); os.close(fd)
                        except BaseException as close_error:
                            if (
                                not isinstance(close_error, Exception)
                                and control is None
                            ):
                                control = close_error
                if authenticated_spool:
                    try:
                        spool.close()
                    except BaseException as close_error:
                        if (
                            not isinstance(close_error, Exception)
                            and control is None
                        ):
                            control = close_error
                    if capsule is not None:
                        capsule_entry = capsule_registry.get(id(capsule))
                        if capsule_entry is not None:
                            capsule_entry[1]["state"] = "revoked"
                            capsule_registry.pop(id(capsule), None)
                    try:
                        claim.close()
                    except BaseException as close_error:
                        if (
                            not isinstance(close_error, Exception)
                            and control is None
                        ):
                            control = close_error
                if control is not None and control is not original_error:
                    raise control
                if not isinstance(original_error, Exception):
                    raise original_error
                if type(original_error) is _ArchiveRpcError:
                    raise original_error
                if authenticated_spool:
                    _raise_archive_error((
                        "authority_mismatch",
                        "historical_window_spool_handoff_failed",
                    ))
                raise original_error

    def require_clear_bound_transfer(
        claim_record: Dict[str, Any],
        claim: "_ProductionHistoricalWindowRunClaim",
    ) -> None:
        checker = claim_record.get("transfer_state_checker")
        spool = claim_record.get("spool")
        if not callable(checker) or spool is None:
            _raise_archive_error((
                "authority_mismatch", "historical_window_capability_invalid"
            ))
        try:
            state = checker(spool, claim=claim)
        except _ArchiveRpcError:
            raise
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            _raise_archive_error((
                "authority_mismatch", "historical_window_capability_invalid"
            ))
        if state == "clear":
            return None
        if state in (
            "issued",
            "pending",
            "pending_verified",
            "committed_unverified",
            "committed_verified",
        ):
            _raise_archive_error((
                "authority_mismatch", "historical_window_transfer_outstanding"
            ))
        _raise_archive_error((
            "authority_mismatch", "historical_window_capability_invalid"
        ))

    def consume_capsule(
        *,
        capsule: "_ClaimedHistoricalWindowSourceCapsule",
        expected_claim: "_ProductionHistoricalWindowRunClaim",
        expected_spool: Any,
        expected_storage_module: Any,
    ) -> Tuple[Any, ...]:
        if type(capsule) is not _ClaimedHistoricalWindowSourceCapsule:
            reject_construction()
        entry = capsule_registry.get(id(capsule))
        if entry is None or entry[0] is not capsule:
            reject_construction()
        record = entry[1]
        claim_record = require_live_claim(expected_claim)
        if (
            record["state"] != "prepared"
            or record["claim"] is not expected_claim
            or record["spool"] is not expected_spool
            or record["storage_module"] is not expected_storage_module
            or claim_record["phase"] != "binding"
        ):
            reject_construction()
        record["state"] = "moving"
        return record["payload"]

    def commit_capsule(
        *,
        capsule: "_ClaimedHistoricalWindowSourceCapsule",
        expected_claim: "_ProductionHistoricalWindowRunClaim",
        expected_spool: Any,
        binding: Any,
    ) -> None:
        if type(capsule) is not _ClaimedHistoricalWindowSourceCapsule:
            reject_construction()
        entry = capsule_registry.get(id(capsule))
        if entry is None or entry[0] is not capsule:
            reject_construction()
        record = entry[1]
        claim_record = require_live_claim(expected_claim)
        if (
            record["state"] != "moving"
            or record["claim"] is not expected_claim
            or record["spool"] is not expected_spool
            or claim_record["phase"] != "binding"
        ):
            reject_construction()
        record["binding"] = binding
        record["state"] = "consumed_undelivered"
        scan_candidate = claim_record["candidate"]["scan"]
        storage_candidate = claim_record["candidate"]["storage"]
        claim_record["binding"] = binding
        claim_record["spool"] = expected_spool
        claim_record["bound_scan"] = scan_candidate[1]
        claim_record["bound_storage"] = storage_candidate[1]
        claim_record["phase"] = "bound"
        record["state"] = "consumed"
        return None

    def abort_capsule(
        *,
        capsule: "_ClaimedHistoricalWindowSourceCapsule",
        expected_claim: "_ProductionHistoricalWindowRunClaim",
        expected_spool: Any,
    ) -> None:
        if type(capsule) is not _ClaimedHistoricalWindowSourceCapsule:
            reject_construction()
        entry = capsule_registry.get(id(capsule))
        if entry is None or entry[0] is not capsule:
            reject_construction()
        record = entry[1]
        if (
            record["claim"] is not expected_claim
            or record["spool"] is not expected_spool
            or record["state"] not in (
                "prepared", "moving", "consumed_undelivered", "consumed"
            )
        ):
            reject_construction()
        claim_record = require_live_claim(expected_claim)
        if (
            record.get("binding") is not None
            and claim_record.get("binding") is record.get("binding")
            and claim_record.get("spool") is expected_spool
        ):
            claim_record["binding"] = None
            claim_record["spool"] = None
            claim_record["bound_scan"] = None
            claim_record["bound_storage"] = None
            claim_record["phase"] = "binding"
        record["state"] = "revoked"
        return None

    def validated_logical_root(
        logical_root: Mapping[str, Any], expected_index: int
    ) -> Tuple[Tuple[Mapping[str, Any], ...], bool]:
        if type(logical_root) is not dict:
            reject_construction()
        schema = logical_root.get("schema")
        segment = logical_root.get("segment")
        if schema == "historical_foundry_anchor_stage_logical_root/v1":
            fields = {
                "schema", "segment", "stage_index", "stage_name",
                "logical_batch_index", "requests",
                "allow_http_413_bisection",
            }
            stage_names = ("anchor", "fixed_authority", "derived_authority")
            index = logical_root.get("stage_index")
            valid = (
                segment == "anchor_stage"
                and type(index) is int
                and index in (0, 1, 2)
                and logical_root.get("stage_name") == stage_names[index]
                and logical_root.get("allow_http_413_bisection") is False
            )
            allow = False
        elif schema == "historical_foundry_lower_observation_logical_root/v1":
            fields = {
                "schema", "segment", "observation_index",
                "observation_kind", "kind_index", "logical_batch_index",
                "block_number", "requests", "allow_http_413_bisection",
            }
            valid = (
                segment == "lower_observation"
                and type(logical_root.get("observation_index")) is int
                and logical_root["observation_index"] >= 0
                and logical_root.get("observation_kind")
                in ("search_probe", "boundary_witness")
                and type(logical_root.get("kind_index")) is int
                and logical_root["kind_index"] >= 0
                and type(logical_root.get("block_number")) is int
                and logical_root["block_number"] >= 0
                and logical_root.get("allow_http_413_bisection") is False
            )
            allow = False
        elif schema == "historical_foundry_window_logical_root/v1":
            fields = {
                "schema", "segment", "root_index", "kind", "block_start",
                "block_stop", "logical_batch_index", "requests",
                "allow_http_413_bisection",
            }
            kind = logical_root.get("kind")
            requests_value = logical_root.get("requests")
            derived_allow = (
                kind in ("header", "reserve", "price")
                and type(requests_value) is tuple
                and len(requests_value) >= 2
            )
            valid = (
                segment == "window_root"
                and type(logical_root.get("root_index")) is int
                and logical_root["root_index"] >= 0
                and kind in (
                    "header", "reserve", "price", "fee_history",
                    "final_anchor",
                )
                and type(logical_root.get("block_start")) is int
                and type(logical_root.get("block_stop")) is int
                and logical_root["block_start"] <= logical_root["block_stop"]
                and logical_root.get("allow_http_413_bisection")
                is derived_allow
            )
            allow = derived_allow
        else:
            reject_construction()
        if (
            set(logical_root) != fields
            or not valid
            or logical_root.get("logical_batch_index") != expected_index
            or type(logical_root.get("requests")) is not tuple
        ):
            reject_construction()
        rows, _canonical, _ids = _freeze_archive_request_rows(
            logical_root["requests"]
        )
        return tuple(rows), allow

    def _open_logical_scope_core(
        *,
        claim: "_ProductionHistoricalWindowRunClaim",
        logical_root: Mapping[str, Any],
        spool: "_HistoricalWindowExchangeSpool",
        delivery_guard: List[Any],
    ) -> "_ProductionHistoricalWindowLogicalBatchScope":
        claim_record = require_live_claim(claim)
        context = claim_record["context"]
        with context._historical_window_lock:
            if (
                claim_record["phase"] != "bound"
                or claim_record["binding"] is None
            ):
                _raise_archive_error((
                    "authority_mismatch",
                    "historical_window_specialized_batch_required",
                ))
            if spool is not claim_record["spool"]:
                _raise_archive_error((
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ))
            require_clear_bound_transfer(claim_record, claim)
            root_consumer = claim_record.get("logical_root_consumer")
            try:
                authoritative_root = (
                    root_consumer(
                        claim=claim,
                        spool=spool,
                        logical_root=logical_root,
                    )
                    if callable(root_consumer) else None
                )
                caller_root = _detached_archive_value(logical_root)
            except _ArchiveRpcError:
                raise
            except BaseException as error:
                if not isinstance(error, Exception):
                    raise
                authoritative_root = None
                caller_root = None
            if (
                type(authoritative_root) is not dict
                or caller_root != authoritative_root
            ):
                _raise_archive_error((
                    "authority_mismatch", "logical_batch_scope_invalid"
                ))
            rows, allow_413 = validated_logical_root(
                authoritative_root, context._next_logical_batch_index
            )
            permit = object()
            core_gate["open"][id(context)] = (context, permit)
            try:
                underlying = _open_archive_scope(
                    context, rows, implicit=False
                )
            finally:
                permit_entry = core_gate["open"].get(id(context))
                if (
                    permit_entry is not None
                    and permit_entry[0] is context
                    and permit_entry[1] is permit
                ):
                    core_gate["open"].pop(id(context), None)
            delivery_guard[0] = (claim, claim_record, None)
            logical = _ProductionHistoricalWindowLogicalBatchScope(
                _provenance=provenance
            )
            delivery_guard[0] = (claim, claim_record, logical)
            logical_scope_registry[id(logical)] = (logical, {
                "state": "fresh",
                "claim": claim,
                "claim_record": claim_record,
                "context": context,
                "spool": spool,
                "underlying": underlying,
                "allow_413": allow_413,
                "caller_root": logical_root,
                "authoritative_root": authoritative_root,
            })
            return logical

    def open_logical_scope(
        *,
        claim: "_ProductionHistoricalWindowRunClaim",
        logical_root: Mapping[str, Any],
        spool: "_HistoricalWindowExchangeSpool",
    ) -> "_ProductionHistoricalWindowLogicalBatchScope":
        logical_scope_registry_reference = logical_scope_registry
        del logical_scope_registry_reference
        delivery_guard = [None]
        try:
            result = _open_logical_scope_core(
                claim=claim,
                logical_root=logical_root,
                spool=spool,
                delivery_guard=delivery_guard,
            )
            return result
        except BaseException as error:
            guarded = delivery_guard[0]
            if guarded is None:
                raise
            guarded_claim, claim_record, _logical = guarded
            fail_undelivered_claim_authority(
                guarded_claim, claim_record, error
            )
        raise _ArchiveInternalFailure()

    def _attempt_logical_scope_core(
        *,
        logical_scope: "_ProductionHistoricalWindowLogicalBatchScope",
        request_rows: Sequence[Mapping[str, Any]],
        delivery_guard: List[Any],
    ) -> Tuple[Tuple[Mapping[str, Any], ...], "_HistoricalWindowSpoolReceipt"]:
        if type(logical_scope) is not _ProductionHistoricalWindowLogicalBatchScope:
            reject_construction()
        entry = logical_scope_registry.get(id(logical_scope))
        if entry is None or entry[0] is not logical_scope:
            reject_construction()
        record = entry[1]
        claim_record = require_live_claim(record["claim"])
        context = record["context"]; delivery_guard[0] = (context, record["spool"])
        with context._historical_window_lock:
            scope = record["underlying"]
            if (
                record["state"] != "active"
                or claim_record is not record["claim_record"]
                or claim_record["phase"] != "bound"
                or context._active_scope is not scope
                or not scope._pending
            ):
                _raise_archive_error(("authority_mismatch", "context_closed"))
            try:
                caller_root = _detached_archive_value(
                    record["caller_root"]
                )
            except BaseException as error:
                if not isinstance(error, Exception):
                    raise
                caller_root = None
            if caller_root != record["authoritative_root"]:
                _raise_archive_error((
                    "authority_mismatch", "logical_batch_scope_invalid"
                ))
            require_clear_bound_transfer(claim_record, record["claim"])
            rows, canonical, request_ids = _freeze_archive_request_rows(
                request_rows
            )
            start, stop = scope._pending[0]
            expected_rows = _detached_archive_value(
                scope._root_rows[start:stop]
            )
            if (
                rows != expected_rows
                or canonical != _archive_canonical_bytes(expected_rows)
            ):
                _raise_archive_error((
                    "authority_mismatch", "logical_batch_scope_invalid"
                ))
            attempt_index = scope._attempt_count + 1
            object.__setattr__(scope, "_attempt_count", attempt_index)
            try:
                pair, outcome = _perform_archive_attempt(
                    context, scope, canonical, request_ids
                )
            except BaseException:
                _terminalize_archive_context(context)
                raise
            _require_archive_collection_time(context)
            if pair is not None:
                if (
                    pair == ("archive_state_unavailable", "http_413")
                    and record["allow_413"]
                    and stop - start >= 2
                ):
                    mid = start + ((stop - start) // 2)
                    object.__setattr__(
                        scope,
                        "_pending",
                        [(start, mid), (mid, stop)] + scope._pending[1:],
                    )
                    scope._recoverable_failures.append(_frozen_archive_value({
                        "attempt_index": attempt_index,
                        "reason_code": "archive_state_unavailable",
                        "failure_kind": "http_413",
                        "request_ids": tuple(request_ids),
                    }))
                    _raise_archive_error(pair)
                _terminalize_archive_context(context)
                _raise_archive_error(pair)
            if outcome is None:
                _terminalize_archive_context(context)
                _raise_archive_error((
                    "archive_state_unavailable", "transport_unavailable"
                ))
            exchange_index = context._next_exchange_index
            exchange_projection = {
                "exchange_index": exchange_index,
                "logical_batch_index": scope._logical_batch_index,
                "attempt_index": attempt_index,
                "request_byte_count": len(canonical),
                "request_sha256": hashlib.sha256(canonical).hexdigest(),
                "request_ids": tuple(request_ids),
                "wire_byte_count": outcome["wire_byte_count"],
                "wire_sha256": outcome["wire_sha256"],
                "decoded_byte_count": outcome["decoded_byte_count"],
                "decoded_sha256": outcome["decoded_sha256"],
                "response_ids": tuple(outcome["response_ids"]),
            }
            transfer = None
            pending = None
            receipt = None
            try:
                transfer = record["spool"].issue_transfer_from_bound_rpc(
                    claim=record["claim"],
                    exchange_projection=exchange_projection,
                    canonical_request_bytes=canonical,
                    decoded_response_bytes=outcome["decoded_response_bytes"],
                )
                pending = record["spool"].append_transfer(transfer=transfer)
                record["spool"].verify_pending_receipt(
                    transfer=transfer, pending_receipt=pending
                )
                receipt = record["spool"].commit_transfer(
                    transfer=transfer, pending_receipt=pending
                )
                record["spool"].verify_committed_receipt(
                    transfer=transfer, receipt=receipt
                )
                record["spool"].release_verified_transfer(
                    transfer=transfer, receipt=receipt
                )
            except BaseException as error:
                abort_control = None
                if transfer is not None and pending is not None and receipt is None:
                    try:
                        record["spool"].abort_transfer(
                            transfer=transfer, pending_receipt=pending
                        )
                    except BaseException as abort_error:
                        if not isinstance(abort_error, Exception):
                            abort_control = abort_error
                if not isinstance(error, Exception):
                    raise error
                if abort_control is not None:
                    raise abort_control
                _raise_archive_error((
                    "authority_mismatch",
                    "historical_window_spool_handoff_failed",
                ))
            compact = dict(receipt)
            compact["schema"] = (
                "historical_foundry_archive_rpc_spooled_success_exchange/v1"
            )
            context._records.append(_issue_production_record(compact))
            object.__setattr__(
                context, "_next_exchange_index", exchange_index + 1
            )
            scope._success_exchange_indices.append(exchange_index)
            object.__setattr__(scope, "_pending", scope._pending[1:])
            return (
                tuple(
                    _detached_archive_response_value(row)
                    for row in outcome["rows"]
                ),
                receipt,
            )

    def attempt_logical_scope(
        *,
        logical_scope: "_ProductionHistoricalWindowLogicalBatchScope",
        request_rows: Sequence[Mapping[str, Any]],
    ) -> Tuple[Tuple[Mapping[str, Any], ...], "_HistoricalWindowSpoolReceipt"]:
        delivery_guard = [None]
        try:
            result = _attempt_logical_scope_core(
                logical_scope=logical_scope,
                request_rows=request_rows,
                delivery_guard=delivery_guard,
            )
            return result
        except BaseException as error:
            guarded = delivery_guard[0]
            if guarded is None:
                raise
            context, spool = guarded
            if (
                type(error) is _ArchiveRpcError
                and error.reason_code == "archive_state_unavailable"
                and error.failure_kind == "http_413"
                and context._state == "active"
            ):
                raise
            cleanup_control = None
            cleanup_ordinary = False
            try:
                terminalize_claim_context(context, "failed")
            except BaseException as cleanup_error:
                if isinstance(cleanup_error, Exception):
                    cleanup_ordinary = True
                else:
                    cleanup_control = cleanup_error
            try:
                spool.close()
            except BaseException as cleanup_error:
                if isinstance(cleanup_error, Exception):
                    cleanup_ordinary = True
                elif cleanup_control is None:
                    cleanup_control = cleanup_error
            if not isinstance(error, Exception):
                raise error
            if cleanup_control is not None:
                raise cleanup_control
            if type(error) is _ArchiveRpcError:
                raise
            del cleanup_ordinary
            _raise_archive_error((
                "authority_mismatch",
                "historical_window_spool_handoff_failed",
            ))
        raise _ArchiveInternalFailure()

    def finalize_claimed(
        *,
        claim: "_ProductionHistoricalWindowRunClaim",
        prefinalization: Any,
    ) -> "_ProductionArchiveRpcFinalization":
        record = require_live_claim(claim)
        context = record["context"]
        with context._historical_window_lock:
            if (
                record["phase"] != "bound"
                or record["binding"] is None
                or record["spool"] is None
            ):
                _raise_archive_error((
                    "authority_mismatch",
                    "historical_window_specialized_batch_required",
                ))
            require_clear_bound_transfer(record, claim)
            if (
                context._active_scope is not None
                or context._reserved_scope is not None
            ):
                _raise_archive_error((
                    "authority_mismatch", "final_identity_drift"
                ))
            try:
                record["spool"]._verify_bound_source_authority_for_claimed_finalization(
                    claim=claim, prefinalization=prefinalization
                )
            except BaseException as error:
                terminalize_claim_context(context, "failed")
                record["state"] = "closed"
                record["phase"] = "closed"
                object.__setattr__(context, "_historical_window_consumer", None)
                object.__setattr__(context, "_historical_window_close", None)
                opened_registry.pop(id(context), None)
                if not isinstance(error, Exception):
                    raise
                _raise_archive_error((
                    "authority_mismatch", "final_identity_drift"
                ))
            digest = hashlib.sha256()
            digest.update(
                b"historical_foundry_exchange_spool_receipt_inventory/v1\0"
            )
            for success_record in context._records:
                projection = _detached_archive_value(
                    success_record._projection
                )
                if (
                    type(projection) is not dict
                    or projection.get("schema")
                    != "historical_foundry_archive_rpc_spooled_success_exchange/v1"
                    or len(projection) != 16
                ):
                    terminalize_claim_context(context, "failed")
                    record["state"] = "closed"
                    record["phase"] = "closed"
                    _raise_archive_error((
                        "authority_mismatch", "final_identity_drift"
                    ))
                projection["schema"] = (
                    "historical_foundry_exchange_spool_receipt/v1"
                )
                payload = json.dumps(
                    projection,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
                digest.update(len(payload).to_bytes(8, "big"))
                digest.update(payload)
            receipt_inventory_sha256 = digest.hexdigest()
            relay_lease = context._relay_lease
            if relay_lease is not None:
                try:
                    storage_module = importlib.import_module(
                        "scripts.historical_foundry_storage"
                    )
                    storage_module._bind_historical_relay_lease_from_production_spool(
                        spool=record["spool"], relay_lease=relay_lease
                    )
                    object.__setattr__(context, "_relay_moved", True)
                except BaseException as error:
                    terminalize_claim_context(context, "failed")
                    record["state"] = "closed"
                    record["phase"] = "closed"
                    if not isinstance(error, Exception):
                        raise
                    _raise_archive_error((
                        "authority_mismatch",
                        "historical_window_spool_handoff_failed",
                    ))
            object.__setattr__(context, "_historical_window_close", None)
            permit = object()
            core_gate["finalize"][id(context)] = (context, permit)
            try:
                finalization = _finalize_production_archive_rpc_run(context)
            except BaseException as error:
                core_gate["finalize"].pop(id(context), None)
                core_gate["claimed"].pop(id(context), None)
                record["state"] = "closed"
                record["phase"] = "closed"
                opened_registry.pop(id(context), None)
                cleanup_control = None
                try:
                    record["spool"].close()
                except BaseException as cleanup_error:
                    if not isinstance(cleanup_error, Exception):
                        cleanup_control = cleanup_error
                if not isinstance(error, Exception):
                    raise error
                if cleanup_control is not None:
                    raise cleanup_control
                raise error
            core_gate["finalize"].pop(id(context), None)
            core_gate["claimed"].pop(id(context), None)
            record["state"] = "finalized"
            record["phase"] = "finalized"
            record["prefinalization"] = prefinalization
            record["prefinalization_digests"] = prefinalization._digests
            record["receipt_inventory_sha256"] = receipt_inventory_sha256
            record["finalization"] = finalization
            opened_registry.pop(id(context), None)
            return finalization

    def verify_finalization(
        *,
        claim: "_ProductionHistoricalWindowRunClaim",
        finalization: "_ProductionArchiveRpcFinalization",
        expected_prefinalization: Any,
        expected_receipt_inventory_sha256: str,
    ) -> None:
        if type(claim) is not _ProductionHistoricalWindowRunClaim:
            reject_construction()
        entry = claim_registry.get(id(claim))
        if entry is None or entry[0]() is not claim:
            reject_construction()
        record = entry[1]
        if (
            record.get("state") != "finalized"
            or record.get("phase") != "finalized"
            or type(finalization) is not _ProductionArchiveRpcFinalization
            or record.get("finalization") is not finalization
            or record.get("prefinalization") is not expected_prefinalization
            or record.get("prefinalization_digests")
            is not expected_prefinalization._digests
            or type(expected_receipt_inventory_sha256) is not str
            or record.get("receipt_inventory_sha256")
            != expected_receipt_inventory_sha256
        ):
            _raise_archive_error((
                "authority_mismatch", "final_identity_drift"
            ))
        return None

    return (
        _ProductionHistoricalWindowRunClaim,
        _ProductionHistoricalWindowLogicalBatchScope,
        _ClaimedHistoricalWindowSourceCapsule,
        mark_opened,
        claim_fresh,
        claimed_config,
        bind_scan,
        bind_storage,
        bind_sources,
        consume_capsule,
        commit_capsule,
        abort_capsule,
        open_logical_scope,
        attempt_logical_scope,
        finalize_claimed,
        verify_finalization,
    )


(
    _ProductionHistoricalWindowRunClaim,
    _ProductionHistoricalWindowLogicalBatchScope,
    _ClaimedHistoricalWindowSourceCapsule,
    _mark_fresh_production_archive_rpc_run_for_historical_window,
    _claim_fresh_production_archive_rpc_run_for_historical_window,
    _get_claimed_historical_window_config,
    _bind_claimed_historical_window_scan_source_module,
    _bind_claimed_historical_window_storage_source_module,
    _bind_claimed_historical_window_sources_to_spool,
    _consume_claimed_historical_window_source_capsule_for_storage,
    _commit_claimed_historical_window_source_capsule_move,
    _abort_claimed_historical_window_source_capsule_move,
    _open_production_archive_rpc_historical_window_logical_batch,
    _production_archive_rpc_historical_window_logical_batch_attempt,
    _finalize_claimed_production_archive_rpc_run_for_historical_window,
    _verify_claimed_historical_window_finalization,
) = _initialize_production_historical_window_types(
    _historical_window_claimed_core_gate
)
del _initialize_production_historical_window_types


def _initialize_test_archive_types():
    provenance = object()

    class _ArchiveRpcTestPreflight:
        __slots__ = ("_checkpoint", "_identity")

        def __init__(self, checkpoint: Callable[[str], None], identity: Mapping[str, Any], *, _provenance: object = None) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive test preflight provenance is invalid")
            object.__setattr__(self, "_checkpoint", checkpoint)
            object.__setattr__(self, "_identity", _frozen_archive_value(dict(identity)))

        def __repr__(self) -> str:
            return "_ArchiveRpcTestPreflight(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ArchiveRpcTestPreflight is immutable")

        def __copy__(self) -> Any:
            raise TypeError("_ArchiveRpcTestPreflight is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ArchiveRpcTestPreflight is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ArchiveRpcTestPreflight is not serializable")

    class _ArchiveRpcTestRunContext:
        __slots__ = (
            "_state", "_clock", "_last_clock", "_collection_deadline",
            "_key", "_endpoint_projection", "_endpoint_bytes",
            "_connection_url", "_endpoint_identity", "_operation",
            "_preflight", "_opening_identity", "_active_scope",
            "_reserved_scope", "_logical_summaries", "_records",
            "_next_logical_batch_index", "_next_exchange_index",
        )

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive test context provenance is invalid")
            for name in self.__slots__:
                object.__setattr__(self, name, values[name])

        def __repr__(self) -> str:
            return "_ArchiveRpcTestRunContext(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ArchiveRpcTestRunContext is immutable")

        def __copy__(self) -> Any:
            raise TypeError("_ArchiveRpcTestRunContext is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ArchiveRpcTestRunContext is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ArchiveRpcTestRunContext is not serializable")

        def __enter__(self) -> "_ArchiveRpcTestRunContext":
            if self._state != "active":
                _raise_archive_error(("authority_mismatch", "context_closed"))
            return self

        def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
            _abandon_archive_context(self)

    class _ArchiveRpcTestLogicalBatchScope:
        __slots__ = (
            "_context", "_root_rows", "_root_bytes", "_root_ids",
            "_logical_batch_index", "_wire_remaining", "_decoded_remaining",
            "_wire_count", "_decoded_count", "_attempt_count",
            "_recoverable_failures", "_success_exchange_indices",
            "_pending", "_entered", "_consumed", "_implicit",
        )

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive test scope provenance is invalid")
            for name in self.__slots__:
                object.__setattr__(self, name, values[name])

        def __repr__(self) -> str:
            return "_ArchiveRpcTestLogicalBatchScope(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ArchiveRpcTestLogicalBatchScope is immutable")

        def __copy__(self) -> Any:
            raise TypeError("_ArchiveRpcTestLogicalBatchScope is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ArchiveRpcTestLogicalBatchScope is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ArchiveRpcTestLogicalBatchScope is not serializable")

        def __enter__(self) -> "_ArchiveRpcTestLogicalBatchScope":
            _enter_archive_scope(self)
            return self

        def __exit__(self, error_type: Any, error: Any, traceback: Any) -> None:
            _exit_archive_scope(self, error_type, error, traceback)

    class _ArchiveRpcTestSuccessRecord:
        __slots__ = ("_projection",)

        def __init__(self, projection: Mapping[str, Any], *, _provenance: object = None) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive test record provenance is invalid")
            object.__setattr__(self, "_projection", _frozen_archive_value(dict(projection)))

        def __repr__(self) -> str:
            return "_ArchiveRpcTestSuccessRecord(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ArchiveRpcTestSuccessRecord is immutable")

        def __copy__(self) -> Any:
            raise TypeError("_ArchiveRpcTestSuccessRecord is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ArchiveRpcTestSuccessRecord is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ArchiveRpcTestSuccessRecord is not serializable")

    class _ArchiveRpcTestFinalization(MappingABC):
        __slots__ = ("_projection",)

        def __init__(self, projection: Mapping[str, Any], *, _provenance: object = None) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive test finalization provenance is invalid")
            object.__setattr__(self, "_projection", _frozen_archive_value(dict(projection)))

        def __repr__(self) -> str:
            return "_ArchiveRpcTestFinalization(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ArchiveRpcTestFinalization is immutable")

        def __getitem__(self, key: str) -> Any:
            return _detached_archive_value(self._projection[key])

        def __iter__(self) -> Iterator[str]:
            return iter(self._projection)

        def __len__(self) -> int:
            return len(self._projection)

        def __copy__(self) -> Any:
            raise TypeError("_ArchiveRpcTestFinalization is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ArchiveRpcTestFinalization is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ArchiveRpcTestFinalization is not serializable")

    class _ArchiveRpcTestResponse:
        __slots__ = (
            "_status", "_header_items", "_chunks", "_chunk_offset",
            "_before_status", "_before_headers", "_before_chunk", "_closed",
            "fp",
        )

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical archive test response provenance is invalid")
            for name, value in values.items():
                object.__setattr__(self, name, value)
            object.__setattr__(self, "_chunk_offset", 0)
            object.__setattr__(self, "_closed", False)
            object.__setattr__(self, "fp", _ArchiveDecoderFp())

        def __repr__(self) -> str:
            return "_ArchiveRpcTestResponse(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("_ArchiveRpcTestResponse is immutable")

        def __copy__(self) -> Any:
            raise TypeError("_ArchiveRpcTestResponse is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("_ArchiveRpcTestResponse is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("_ArchiveRpcTestResponse is not serializable")

        @property
        def status(self) -> int:
            if self._before_status is not None:
                self._before_status()
            return self._status

        @property
        def headers(self) -> Any:
            if self._before_headers is not None:
                self._before_headers()
            return _ArchiveDecoderHeaders(self._header_items)

        def read1(self, maximum: int) -> bytes:
            if self._closed:
                return b""
            chunks = self._chunks
            if not chunks:
                return b""
            index = self._chunk_offset
            if self._before_chunk is not None:
                self._before_chunk(index)
            chunk = chunks[0]
            if len(chunk) <= maximum:
                object.__setattr__(self, "_chunks", chunks[1:])
                object.__setattr__(self, "_chunk_offset", index + 1)
                return chunk
            object.__setattr__(self, "_chunks", (chunk[maximum:],) + chunks[1:])
            object.__setattr__(self, "_chunk_offset", index + 1)
            return chunk[:maximum]

        read = read1

        def close(self) -> None:
            object.__setattr__(self, "_closed", True)

    def issue_preflight(checkpoint: Callable[[str], None], identity: Mapping[str, Any]) -> _ArchiveRpcTestPreflight:
        return _ArchiveRpcTestPreflight(checkpoint, identity, _provenance=provenance)

    def issue_context(**values: Any) -> _ArchiveRpcTestRunContext:
        return _ArchiveRpcTestRunContext(_provenance=provenance, **values)

    def issue_scope(**values: Any) -> _ArchiveRpcTestLogicalBatchScope:
        return _ArchiveRpcTestLogicalBatchScope(_provenance=provenance, **values)

    def issue_record(projection: Mapping[str, Any]) -> _ArchiveRpcTestSuccessRecord:
        return _ArchiveRpcTestSuccessRecord(projection, _provenance=provenance)

    def issue_finalization(projection: Mapping[str, Any]) -> _ArchiveRpcTestFinalization:
        return _ArchiveRpcTestFinalization(projection, _provenance=provenance)

    def issue_response(**values: Any) -> _ArchiveRpcTestResponse:
        return _ArchiveRpcTestResponse(_provenance=provenance, **values)

    return (
        _ArchiveRpcTestPreflight,
        _ArchiveRpcTestRunContext,
        _ArchiveRpcTestLogicalBatchScope,
        _ArchiveRpcTestSuccessRecord,
        _ArchiveRpcTestFinalization,
        _ArchiveRpcTestResponse,
        issue_preflight,
        issue_context,
        issue_scope,
        issue_record,
        issue_finalization,
        issue_response,
    )


class _ArchiveDecoderSocket:
    __slots__ = ("timeout",)

    def __init__(self) -> None:
        self.timeout = None

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout


class _ArchiveDecoderRaw:
    __slots__ = ("_sock",)

    def __init__(self) -> None:
        self._sock = _ArchiveDecoderSocket()


class _ArchiveDecoderFp:
    __slots__ = ("raw",)

    def __init__(self) -> None:
        self.raw = _ArchiveDecoderRaw()


class _ArchiveDecoderHeaders:
    __slots__ = ("_items",)

    def __init__(self, items: Tuple[Tuple[str, str], ...]) -> None:
        self._items = items

    def raw_items(self) -> Iterator[Tuple[str, str]]:
        return iter(self._items)


(
    _ArchiveRpcTestPreflight,
    _ArchiveRpcTestRunContext,
    _ArchiveRpcTestLogicalBatchScope,
    _ArchiveRpcTestSuccessRecord,
    _ArchiveRpcTestFinalization,
    _ArchiveRpcTestResponse,
    _issue_test_preflight,
    _issue_test_context,
    _issue_test_scope,
    _issue_test_record,
    _issue_test_finalization,
    _issue_test_response,
) = _initialize_test_archive_types()
del _initialize_test_archive_types


def _resource_policy_projection() -> Dict[str, Any]:
    return {
        "schema": "historical_foundry_archive_rpc_resource_policy/v1",
        "request_body_bytes": _ARCHIVE_REQUEST_BODY_BYTES,
        "logical_batch_wire_bytes": _ARCHIVE_LOGICAL_WIRE_BYTES,
        "logical_batch_decoded_bytes": _ARCHIVE_LOGICAL_DECODED_BYTES,
        "response_header_bytes": _ARCHIVE_RESPONSE_HEADER_BYTES,
        "response_header_rows": _ARCHIVE_RESPONSE_HEADER_ROWS,
        "json_nodes": _ARCHIVE_JSON_NODES,
        "json_scalar_bytes": _ARCHIVE_JSON_SCALAR_BYTES,
        "json_string_bytes": _ARCHIVE_JSON_STRING_BYTES,
        "json_depth": _ARCHIVE_JSON_DEPTH,
        "json_numeric_token_bytes": _ARCHIVE_JSON_NUMERIC_TOKEN_BYTES,
        "attempt_deadline_seconds": _ARCHIVE_ATTEMPT_DEADLINE_SECONDS,
        "collection_deadline_seconds": _ARCHIVE_COLLECTION_DEADLINE_SECONDS,
        "request_method": "POST",
        "retry_count": 0,
        "methods": _ARCHIVE_METHODS,
    }


def _test_preflight_identity() -> Dict[str, Any]:
    hashes = {
        role: hashlib.sha256((role + "\0" + relative).encode("utf-8")).hexdigest()
        for role, _module, relative in _PRODUCTION_SOURCE_MEMBERS
    }
    source_rows = tuple({
        "role": role,
        "size_bytes": len(relative.encode("utf-8")),
        "sha256": hashes[role],
    } for role, _module, relative in _PRODUCTION_SOURCE_MEMBERS)
    project_inputs = {
        "schema": "historical_foundry_project_input_identity/v1",
        "foundry_toml_sha256": hashes["build:foundry_toml"],
        "foundry_lock_sha256": hashes["build:foundry_lock"],
        "gitmodules_sha256": hashes["build:gitmodules"],
        "forge_std_commit": "1" * 40,
        "forge_std_tree_sha256": "2" * 64,
    }
    toolchain = {
        "source_lock_sha256": "3" * 64,
        "foundry_release": {
            "version": "v1.7.1",
            "archive_sha256": "4" * 64,
            "checksum_sha256": "5" * 64,
            "sigstore_sha256": "6" * 64,
            "sigstore_issuer": "reviewed-issuer",
            "sigstore_san": "reviewed-san",
            "spdx_sha256": "7" * 64,
            "release_commit": "8" * 40,
        },
        "binaries": tuple({
            "name": name,
            "sha256": character * 64,
            "version": "v1.7.1" if name != "solc" else "0.8.36+commit.8a079791",
        } for name, character in (("forge", "9"), ("cast", "a"), ("anvil", "b"))),
        "solc": {
            "version": "0.8.36+commit.8a079791",
            "artifact_sha256": "c" * 64,
            "sha256": "c" * 64,
            "source_commit": "d" * 40,
        },
        "forge_std": {"version": "v1.16.1", "commit": "1" * 40},
        "compiler_settings": {
            "append_cbor": False,
            "bytecode_hash": "none",
            "cbor_metadata": False,
            "evm_version": "osaka",
            "fork_hardfork": "osaka",
            "optimizer_enabled": True,
            "optimizer_runs": 200,
            "via_ir": False,
        },
    }
    artifact = {
        "source_tree_sha256": "1" * 64,
        "constructor_args_sha256": "2" * 64,
        "creation_bytecode_sha256": "3" * 64,
        "deployed_runtime_sha256": "4" * 64,
        "immutable_references_sha256": "5" * 64,
        "artifact_manifest_sha256": "6" * 64,
        "policy_physical_sha256": "7" * 64,
        "authority_physical_sha256": "8" * 64,
        "toolchain_physical_sha256": "9" * 64,
    }
    return {
        "repository_head": "0" * 40,
        "python": {
            "implementation": "CPython", "major": 3, "minor": 8,
            "micro": 10, "releaselevel": "final", "serial": 0,
            "cache_tag": "cpython-38",
        },
        "configs": {
            "policy_id": "policy:" + "a" * 64,
            "policy_physical_sha256": "7" * 64,
            "authority_physical_sha256": "8" * 64,
            "toolchain_physical_sha256": "9" * 64,
        },
        "sources": source_rows,
        "project_inputs": project_inputs,
        "toolchain": toolchain,
        "executor_artifact": artifact,
        "deployed_runtime_sha256": artifact["deployed_runtime_sha256"],
    }


def _issue_archive_rpc_test_preflight_for_test(
    checkpoint: Callable[[str], None],
) -> "_ArchiveRpcTestPreflight":
    if not callable(checkpoint):
        _raise_archive_error(("authority_mismatch", "preflight_invalid"))
    return _issue_test_preflight(checkpoint, _test_preflight_identity())


def _call_test_checkpoint(
    preflight: "_ArchiveRpcTestPreflight",
    name: str,
) -> bool:
    failed = False
    try:
        preflight._checkpoint(name)
    except Exception:
        failed = True
    return not failed


def _initial_clock_sample(clock: Callable[[], float]) -> Optional[float]:
    failed = False
    sample = None
    try:
        sample = clock()
    except Exception:
        failed = True
    if (
        failed
        or type(sample) not in (int, float)
        or not math.isfinite(float(sample))
    ):
        return None
    return float(sample)


def _issue_archive_rpc_test_run_for_test(
    *,
    endpoint: str,
    operation: Callable[[bytes, float], "_ArchiveRpcTestResponse"],
    monotonic: Callable[[], float],
    entropy: Callable[[int], bytes],
    preflight: "_ArchiveRpcTestPreflight",
) -> "_ArchiveRpcTestRunContext":
    if (
        type(preflight) is not _ArchiveRpcTestPreflight
        or not callable(operation)
        or not callable(monotonic)
        or not callable(entropy)
    ):
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    if not _call_test_checkpoint(preflight, "open"):
        _raise_archive_error(("authority_mismatch", "preflight_invalid"))
    projection, endpoint_bytes, connection_url = _canonicalize_archive_endpoint(endpoint)
    started = _initial_clock_sample(monotonic)
    if started is None:
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    entropy_failed = False
    key_bytes = None
    try:
        key_bytes = entropy(32)
    except Exception:
        entropy_failed = True
    if entropy_failed or type(key_bytes) is not bytes or len(key_bytes) != 32:
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    key = bytearray(key_bytes)
    digest = hmac.new(key, endpoint_bytes, hashlib.sha256).hexdigest()
    endpoint_identity = {
        "schema": "historical_foundry_rpc_endpoint_identity/v1",
        "scope": "single_run_nonreversible",
        "endpoint_hmac_sha256": digest,
    }
    return _issue_test_context(
        _state="active",
        _clock=monotonic,
        _last_clock=started,
        _collection_deadline=started + _ARCHIVE_COLLECTION_DEADLINE_SECONDS,
        _key=key,
        _endpoint_projection=_frozen_archive_value(projection),
        _endpoint_bytes=endpoint_bytes,
        _connection_url=connection_url,
        _endpoint_identity=_frozen_archive_value(endpoint_identity),
        _operation=operation,
        _preflight=preflight,
        _opening_identity=_frozen_archive_value(
            _detached_archive_value(preflight._identity)
        ),
        _active_scope=None,
        _reserved_scope=None,
        _logical_summaries=[],
        _records=[],
        _next_logical_batch_index=1,
        _next_exchange_index=1,
    )


def _make_archive_rpc_test_response_for_test(
    *,
    status: int,
    header_items: Tuple[Tuple[str, str], ...],
    body_chunks: Tuple[bytes, ...],
    before_status: Optional[Callable[[], None]] = None,
    before_headers: Optional[Callable[[], None]] = None,
    before_chunk: Optional[Callable[[int], None]] = None,
) -> "_ArchiveRpcTestResponse":
    if type(status) is not int or not 100 <= status <= 599:
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    if type(header_items) is not tuple or type(body_chunks) is not tuple:
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    if any(
        type(row) is not tuple
        or len(row) != 2
        or type(row[0]) is not str
        or type(row[1]) is not str
        for row in header_items
    ) or any(type(chunk) is not bytes for chunk in body_chunks):
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    for callback in (before_status, before_headers, before_chunk):
        if callback is not None and not callable(callback):
            _raise_archive_error(("authority_mismatch", "context_invalid"))
    return _issue_test_response(
        _status=status,
        _header_items=tuple(header_items),
        _chunks=tuple(bytes(chunk) for chunk in body_chunks),
        _before_status=before_status,
        _before_headers=before_headers,
        _before_chunk=before_chunk,
    )


def _sample_context_clock(context: Any) -> float:
    failed = False
    sample = None
    try:
        sample = context._clock()
    except _ArchiveDeadlineExpired:
        raise
    except Exception:
        failed = True
    if (
        failed
        or type(sample) not in (int, float)
        or not math.isfinite(float(sample))
        or float(sample) < context._last_clock
    ):
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    normalized = float(sample)
    object.__setattr__(context, "_last_clock", normalized)
    return normalized


def _erase_archive_key(key: Any) -> None:
    if type(key) is not bytearray:
        return
    try:
        for index in range(len(key)):
            key[index] = 0
    except Exception:
        pass


def _cleanup_archive_context(context: Any, state: str) -> None:
    key = context._key
    relay_lease = getattr(context, "_relay_lease", None)
    relay_moved = getattr(context, "_relay_moved", False) is True
    if relay_lease is not None and not relay_moved:
        try:
            relay_lease.close()
        except BaseException:
            _erase_archive_key(key)
            raise
    elif not relay_moved:
        _erase_archive_key(key)
    preflight = context._preflight
    closer = getattr(preflight, "close", None)
    control = None
    if callable(closer):
        try:
            closer()
        except BaseException as error:
            if not isinstance(error, Exception):
                control = error
    object.__setattr__(context, "_key", None)
    object.__setattr__(context, "_endpoint_projection", None)
    object.__setattr__(context, "_endpoint_bytes", None)
    object.__setattr__(context, "_connection_url", None)
    object.__setattr__(context, "_endpoint_identity", None)
    object.__setattr__(context, "_operation", None)
    object.__setattr__(context, "_preflight", None)
    object.__setattr__(context, "_opening_identity", None)
    object.__setattr__(context, "_active_scope", None)
    object.__setattr__(context, "_reserved_scope", None)
    if type(context) is _ProductionArchiveRpcRunContext:
        object.__setattr__(context, "_relay_lease", None)
    context._logical_summaries.clear()
    context._records.clear()
    object.__setattr__(context, "_state", state)
    if control is not None:
        raise control


def _terminalize_archive_context(context: Any) -> None:
    if context._state in ("active", "finalizing"):
        _cleanup_archive_context(context, "failed")


def _abandon_archive_context(context: Any) -> None:
    if type(context) not in (_ProductionArchiveRpcRunContext, _ArchiveRpcTestRunContext):
        return
    if type(context) is _ProductionArchiveRpcRunContext:
        with context._historical_window_lock:
            closer = context._historical_window_close
            if callable(closer):
                closer()
                return
            if context._state in ("active", "finalizing"):
                _cleanup_archive_context(context, "abandoned")
        return
    if context._state in ("active", "finalizing"):
        _cleanup_archive_context(context, "abandoned")


def _require_archive_context(context: Any, expected: type) -> None:
    if type(context) is not expected:
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    if context._state != "active":
        _raise_archive_error(("authority_mismatch", "context_closed"))


def _open_archive_scope(
    context: Any,
    request_rows: Any,
    *,
    implicit: bool,
) -> Any:
    if (
        type(context) is _ProductionArchiveRpcRunContext
        and context._historical_window_consumer is not None
    ):
        _raise_archive_error((
            "authority_mismatch",
            "historical_window_specialized_batch_required",
        ))
    expected_context = (
        _ProductionArchiveRpcRunContext
        if type(context) is _ProductionArchiveRpcRunContext
        else _ArchiveRpcTestRunContext
    )
    _require_archive_context(context, expected_context)
    if context._active_scope is not None or context._reserved_scope is not None:
        _terminalize_archive_context(context)
        _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
    rows, canonical, request_ids = _freeze_archive_request_rows(request_rows)
    index = context._next_logical_batch_index
    object.__setattr__(context, "_next_logical_batch_index", index + 1)
    values = dict(
        _context=context,
        _root_rows=_frozen_archive_value(rows),
        _root_bytes=canonical,
        _root_ids=request_ids,
        _logical_batch_index=index,
        _wire_remaining=_ARCHIVE_LOGICAL_WIRE_BYTES,
        _decoded_remaining=_ARCHIVE_LOGICAL_DECODED_BYTES,
        _wire_count=0,
        _decoded_count=0,
        _attempt_count=0,
        _recoverable_failures=[],
        _success_exchange_indices=[],
        _pending=[(0, len(rows))],
        _entered=False,
        _consumed=False,
        _implicit=implicit,
    )
    scope = (
        _issue_production_scope(**values)
        if expected_context is _ProductionArchiveRpcRunContext
        else _issue_test_scope(**values)
    )
    object.__setattr__(context, "_reserved_scope", scope)
    return scope


def _enter_archive_scope(scope: Any) -> None:
    valid = (
        (type(scope) is _ProductionArchiveRpcLogicalBatchScope
         and type(scope._context) is _ProductionArchiveRpcRunContext)
        or
        (type(scope) is _ArchiveRpcTestLogicalBatchScope
         and type(scope._context) is _ArchiveRpcTestRunContext)
    )
    if not valid:
        _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
    context = scope._context
    if (
        scope._entered
        or scope._consumed
        or context._state != "active"
        or context._reserved_scope is not scope
        or context._active_scope is not None
    ):
        _terminalize_archive_context(context)
        _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
    object.__setattr__(scope, "_entered", True)
    object.__setattr__(context, "_active_scope", scope)


def _complete_archive_scope(scope: Any) -> None:
    context = scope._context
    summary = {
        "schema": "historical_foundry_archive_rpc_logical_batch_summary/v1",
        "logical_batch_index": scope._logical_batch_index,
        "status": "complete",
        "logical_request_byte_count": len(scope._root_bytes),
        "logical_request_sha256": hashlib.sha256(scope._root_bytes).hexdigest(),
        "logical_request_ids": tuple(scope._root_ids),
        "attempt_count": scope._attempt_count,
        "recoverable_failures": tuple(
            _detached_archive_value(row) for row in scope._recoverable_failures
        ),
        "success_exchange_indices": tuple(scope._success_exchange_indices),
        "wire_byte_count": scope._wire_count,
        "decoded_byte_count": scope._decoded_count,
    }
    context._logical_summaries.append(_frozen_archive_value(summary))
    object.__setattr__(scope, "_consumed", True)
    object.__setattr__(context, "_active_scope", None)
    object.__setattr__(context, "_reserved_scope", None)


def _exit_archive_scope(
    scope: Any,
    error_type: Any,
    error: Any,
    traceback: Any,
) -> None:
    del traceback
    context = scope._context
    if scope._consumed:
        if error_type is None:
            _terminalize_archive_context(context)
            _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
        return
    if error_type is not None:
        _terminalize_archive_context(context)
        object.__setattr__(scope, "_consumed", True)
        return
    if context._state != "active" or context._active_scope is not scope or scope._pending:
        _terminalize_archive_context(context)
        object.__setattr__(scope, "_consumed", True)
        _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
    _complete_archive_scope(scope)


class _ArchiveCountingResponse:
    __slots__ = ("_response", "_scope", "_wire_sha256", "fp")

    def __init__(self, response: Any, scope: Any) -> None:
        self._response = response
        self._scope = scope
        self._wire_sha256 = hashlib.sha256()
        self.fp = response.fp

    @property
    def status(self) -> Any:
        return self._response.status

    @property
    def headers(self) -> Any:
        return self._response.headers

    def read1(self, maximum: int) -> bytes:
        reader = getattr(self._response, "read1", None)
        if not callable(reader):
            reader = getattr(self._response, "read", None)
        if not callable(reader):
            raise OSError("archive response stream unavailable")
        chunk = reader(maximum)
        if not isinstance(chunk, bytes):
            return chunk
        exact = bytes(chunk)
        if exact:
            scope = self._scope
            object.__setattr__(scope, "_wire_count", scope._wire_count + len(exact))
            object.__setattr__(scope, "_wire_remaining", scope._wire_remaining - len(exact))
            self._wire_sha256.update(exact)
            if scope._wire_remaining < 0:
                raise BoundedJsonError("resource_limit", "resource_limit")
        return exact

    read = read1


class _ArchiveMemoryResponse:
    __slots__ = ("_body", "_offset", "headers", "fp")

    def __init__(self, body: bytes) -> None:
        self._body = body
        self._offset = 0
        self.headers = _ArchiveDecoderHeaders((
            ("Content-Length", str(len(body))),
            ("Content-Encoding", "identity"),
        ))
        self.fp = _ArchiveDecoderFp()

    def read1(self, maximum: int) -> bytes:
        if self._offset >= len(self._body):
            return b""
        stop = min(len(self._body), self._offset + maximum)
        result = self._body[self._offset:stop]
        self._offset = stop
        return result

    read = read1


def _decoder_clock(context: Any) -> float:
    try:
        return _sample_context_clock(context)
    except _ArchiveRpcError:
        raise _ArchiveDecoderClockInvalid() from None


def _deadline_pair(context: Any, attempt_deadline: float) -> Optional[Tuple[str, str]]:
    try:
        current = _sample_context_clock(context)
    except _ArchiveRpcError:
        return ("authority_mismatch", "context_invalid")
    if current >= context._collection_deadline:
        return ("archive_state_unavailable", "collection_timeout")
    if current >= attempt_deadline:
        return ("archive_state_unavailable", "attempt_timeout")
    return None


def _require_archive_collection_time(context: Any) -> None:
    try:
        pair = _deadline_pair(context, float("inf"))
    except _ArchiveDeadlineExpired:
        pair = ("authority_mismatch", "context_invalid")
    except BaseException:
        _terminalize_archive_context(context)
        raise
    if pair is not None:
        _terminalize_archive_context(context)
        _raise_archive_error(pair)


def _decode_error_pair(error: BoundedJsonError) -> Tuple[str, str]:
    if error.reason_code == "deadline":
        return ("archive_state_unavailable", "attempt_timeout")
    if error.reason_code == "unavailable":
        return ("archive_state_unavailable", "transport_unavailable")
    if error.reason_code == "encoding_unsupported":
        return ("archive_state_unavailable", "response_encoding_unsupported")
    if error.reason_code == "resource_limit":
        return ("archive_state_unavailable", "response_resource_limit")
    return ("archive_state_unavailable", "response_decode_invalid")


def _guarded_decode_error_pair(
    error: BoundedJsonError,
    context: Any,
    attempt_deadline: float,
    guard_installed: bool,
) -> Tuple[str, str]:
    if guard_installed:
        try:
            timer_consumed = signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        except Exception:
            return ("authority_mismatch", "context_invalid")
        if timer_consumed:
            return _deadline_pair(context, attempt_deadline) or (
                "archive_state_unavailable", "attempt_timeout"
            )
    pair = _decode_error_pair(error)
    if pair[1] == "attempt_timeout":
        return _deadline_pair(context, attempt_deadline) or pair
    return pair


def _validate_archive_response_rows(
    value: Any,
    expected_ids: Tuple[int, ...],
) -> Tuple[Optional[Tuple[str, str]], Optional[Tuple[Dict[str, Any], ...]], Optional[Tuple[int, ...]]]:
    if type(value) is not list or len(value) != len(expected_ids):
        return (("authority_mismatch", "response_identity_invalid"), None, None)
    by_id = {}
    response_ids = []
    has_error = False
    for row in value:
        if type(row) is not dict or type(row.get("jsonrpc")) is not str or row.get("jsonrpc") != "2.0":
            return (("authority_mismatch", "response_identity_invalid"), None, None)
        request_id = row.get("id")
        if type(request_id) is not int or request_id <= 0 or request_id in by_id:
            return (("authority_mismatch", "response_identity_invalid"), None, None)
        response_ids.append(request_id)
        if set(row) == {"jsonrpc", "id", "result"}:
            try:
                detached = _detached_archive_response_value(row)
            except ValueError:
                return (("authority_mismatch", "response_identity_invalid"), None, None)
        elif set(row) == {"jsonrpc", "id", "error"}:
            error = row["error"]
            if type(error) is not dict or set(error) not in (
                {"code", "message"}, {"code", "message", "data"}
            ):
                return (("authority_mismatch", "response_identity_invalid"), None, None)
            code = error.get("code")
            message = error.get("message")
            if (
                type(code) is not int
                or not -(1 << 31) <= code < (1 << 31)
                or type(message) is not str
                or not message
                or len(message.encode("utf-8")) > _ARCHIVE_JSON_STRING_BYTES
            ):
                return (("authority_mismatch", "response_identity_invalid"), None, None)
            has_error = True
            detached = None
        else:
            return (("authority_mismatch", "response_identity_invalid"), None, None)
        by_id[request_id] = detached
    if set(by_id) != set(expected_ids):
        return (("authority_mismatch", "response_identity_invalid"), None, None)
    if has_error:
        return (("archive_state_unavailable", "json_rpc_error"), None, None)
    return (None, tuple(by_id[request_id] for request_id in expected_ids), tuple(response_ids))


def _alarm_handler(_signum: int, _frame: Any) -> None:
    raise _ArchiveDeadlineExpired()


def _perform_archive_attempt_guarded(
    context: Any,
    scope: Any,
    canonical_request: bytes,
    request_ids: Tuple[int, ...],
    deadline_holder: List[Optional[float]],
) -> Tuple[Optional[Tuple[str, str]], Optional[Dict[str, Any]]]:
    pair = None
    result = None
    response = None
    counting = None
    guard_installed = False
    attempt_deadline = None
    close_deadline_expired = False
    cancellation = None
    wire_before = scope._wire_count
    try:
        try:
            started = _sample_context_clock(context)
        except _ArchiveRpcError:
            return (("authority_mismatch", "context_invalid"), None)
        if started >= context._collection_deadline:
            return (("archive_state_unavailable", "collection_timeout"), None)
        attempt_deadline = min(
            started + _ARCHIVE_ATTEMPT_DEADLINE_SECONDS,
            context._collection_deadline,
        )
        deadline_holder[0] = attempt_deadline
        remaining = attempt_deadline - started
        if remaining <= 0:
            return (("archive_state_unavailable", "collection_timeout"), None)
        try:
            alarm_handler = signal.getsignal(signal.SIGALRM)
            alarm_timer = signal.getitimer(signal.ITIMER_REAL)
            guard_safe = (
                threading.current_thread() is threading.main_thread()
                and alarm_handler is signal.SIG_DFL
                and alarm_timer == (0.0, 0.0)
            )
        except Exception:
            guard_safe = False
        if not guard_safe:
            return (("authority_mismatch", "context_invalid"), None)
        try:
            previous_handler = signal.signal(signal.SIGALRM, _alarm_handler)
            previous_timer = signal.setitimer(signal.ITIMER_REAL, remaining, 0.0)
            if previous_handler is not signal.SIG_DFL or previous_timer != (0.0, 0.0):
                pair = ("authority_mismatch", "context_invalid")
            else:
                guard_installed = True
        except Exception:
            pair = ("authority_mismatch", "context_invalid")

        if pair is None:
            if (
                type(context._key) is not bytearray
                or type(context._endpoint_bytes) is not bytes
                or not hmac.compare_digest(
                    hmac.new(context._key, context._endpoint_bytes, hashlib.sha256).hexdigest(),
                    context._endpoint_identity["endpoint_hmac_sha256"],
                )
            ):
                pair = ("authority_mismatch", "endpoint_identity_mismatch")
        if pair is None:
            try:
                response = context._operation(canonical_request, remaining)
            except _ArchiveDeadlineExpired:
                pair = _deadline_pair(context, attempt_deadline)
                if pair is None:
                    pair = ("archive_state_unavailable", "attempt_timeout")
            except Exception:
                pair = ("archive_state_unavailable", "transport_unavailable")
        if pair is None:
            sampled = _deadline_pair(context, attempt_deadline)
            if sampled is not None:
                pair = sampled
        if pair is None and type(context) is _ArchiveRpcTestRunContext and type(response) is not _ArchiveRpcTestResponse:
            pair = ("authority_mismatch", "context_invalid")
        if pair is None:
            try:
                status = response.status
            except _ArchiveDeadlineExpired:
                pair = _deadline_pair(context, attempt_deadline)
                if pair is None:
                    pair = ("archive_state_unavailable", "attempt_timeout")
            except Exception:
                pair = ("archive_state_unavailable", "transport_unavailable")
            if pair is None:
                sampled = _deadline_pair(context, attempt_deadline)
                if sampled is not None:
                    pair = sampled
        if (
            pair is None
            and type(status) is int
            and 100 <= status <= 599
            and status != 200
        ):
            try:
                validate_bounded_json_response_headers(
                    response, header_limit=_ARCHIVE_RESPONSE_HEADER_BYTES
                )
            except _ArchiveDeadlineExpired:
                raise
            except BoundedJsonError as error:
                pair = _guarded_decode_error_pair(
                    error, context, attempt_deadline, guard_installed
                )
            except Exception:
                pair = ("archive_state_unavailable", "response_decode_invalid")
        if pair is None:
            if type(status) is not int or not 100 <= status <= 599:
                pair = ("archive_state_unavailable", "transport_unavailable")
            elif 300 <= status <= 399:
                pair = ("archive_state_unavailable", "redirect_forbidden")
            elif status == 413:
                pair = ("archive_state_unavailable", "http_413")
            elif status != 200:
                pair = ("archive_state_unavailable", "http_status")
        if pair is None:
            try:
                counting = _ArchiveCountingResponse(response, scope)
                decoded = decode_bounded_json_response(
                    counting,
                    header_limit=_ARCHIVE_RESPONSE_HEADER_BYTES,
                    wire_limit=scope._wire_remaining,
                    decoded_limit=scope._decoded_remaining,
                    scalar_limit=_ARCHIVE_JSON_SCALAR_BYTES,
                    node_limit=_ARCHIVE_JSON_NODES,
                    ordinary_string_limit=_ARCHIVE_JSON_STRING_BYTES,
                    require_canonical=False,
                    materialize_exact_floats=False,
                    absolute_deadline=attempt_deadline,
                    monotonic=lambda: _decoder_clock(context),
                    return_decoded_bytes=True,
                )
            except _ArchiveDeadlineExpired:
                pair = _deadline_pair(context, attempt_deadline)
                if pair is None:
                    pair = ("archive_state_unavailable", "attempt_timeout")
            except _ArchiveDecoderClockInvalid:
                pair = ("authority_mismatch", "context_invalid")
            except BoundedJsonError as error:
                pair = _guarded_decode_error_pair(
                    error, context, attempt_deadline, guard_installed
                )
            except _ArchiveRpcError:
                pair = ("authority_mismatch", "context_invalid")
            except Exception:
                pair = ("archive_state_unavailable", "response_decode_invalid")
        if pair is None:
            object.__setattr__(scope, "_decoded_remaining", scope._decoded_remaining - len(decoded))
            object.__setattr__(scope, "_decoded_count", scope._decoded_count + len(decoded))
            if scope._decoded_remaining < 0:
                pair = ("archive_state_unavailable", "response_resource_limit")
        if pair is None:
            try:
                parsed = decode_bounded_json_response(
                    _ArchiveMemoryResponse(decoded),
                    header_limit=_ARCHIVE_RESPONSE_HEADER_BYTES,
                    wire_limit=_ARCHIVE_LOGICAL_DECODED_BYTES,
                    decoded_limit=_ARCHIVE_LOGICAL_DECODED_BYTES,
                    scalar_limit=_ARCHIVE_JSON_SCALAR_BYTES,
                    node_limit=_ARCHIVE_JSON_NODES,
                    ordinary_string_limit=_ARCHIVE_JSON_STRING_BYTES,
                    require_canonical=False,
                    materialize_exact_floats=False,
                    absolute_deadline=attempt_deadline,
                    monotonic=lambda: _decoder_clock(context),
                    return_decoded_bytes=False,
                )
            except _ArchiveDecoderClockInvalid:
                pair = ("authority_mismatch", "context_invalid")
            except BoundedJsonError as error:
                pair = _guarded_decode_error_pair(
                    error, context, attempt_deadline, guard_installed
                )
            except _ArchiveRpcError:
                pair = ("authority_mismatch", "context_invalid")
            except Exception:
                pair = ("archive_state_unavailable", "response_decode_invalid")
        if pair is None:
            pair, ordered_rows, response_ids = _validate_archive_response_rows(
                parsed, request_ids
            )
        if pair is None:
            sampled = _deadline_pair(context, attempt_deadline)
            if sampled is not None:
                pair = sampled
        if pair is None and counting is not None:
            result = {
                "rows": ordered_rows,
                "wire_byte_count": scope._wire_count - wire_before,
                "wire_sha256": counting._wire_sha256.hexdigest(),
                "decoded_response_bytes": bytes(decoded),
                "decoded_byte_count": len(decoded),
                "decoded_sha256": hashlib.sha256(decoded).hexdigest(),
                "response_ids": response_ids,
            }
    finally:
        close_failed = False
        if response is not None:
            try:
                closer = getattr(response, "close", None)
                if callable(closer):
                    closer()
            except _ArchiveDeadlineExpired:
                close_deadline_expired = True
            except (KeyboardInterrupt, SystemExit) as error:
                cancellation = error
            except Exception:
                close_failed = True
        guard_failed = False
        if guard_installed:
            try:
                signal.setitimer(signal.ITIMER_REAL, 0.0, 0.0)
                signal.signal(signal.SIGALRM, signal.SIG_DFL)
                if signal.getsignal(signal.SIGALRM) is not signal.SIG_DFL or signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
                    guard_failed = True
            except Exception:
                guard_failed = True
        elif pair is not None:
            try:
                if signal.getsignal(signal.SIGALRM) is _alarm_handler:
                    signal.setitimer(signal.ITIMER_REAL, 0.0, 0.0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
            except Exception:
                guard_failed = True
        if close_failed:
            pair = ("archive_state_unavailable", "transport_unavailable")
        if guard_failed:
            pair = ("authority_mismatch", "context_invalid")
    if cancellation is not None:
        raise cancellation
    if guard_installed and attempt_deadline is not None:
        sampled = _deadline_pair(context, attempt_deadline)
        if sampled is not None:
            pair = sampled
        elif close_deadline_expired:
            pair = ("archive_state_unavailable", "attempt_timeout")
    if pair is not None:
        result = None
    return pair, result


def _perform_archive_attempt(
    context: Any,
    scope: Any,
    canonical_request: bytes,
    request_ids: Tuple[int, ...],
) -> Tuple[Optional[Tuple[str, str]], Optional[Dict[str, Any]]]:
    deadline_holder: List[Optional[float]] = [None]
    try:
        return _perform_archive_attempt_guarded(
            context,
            scope,
            canonical_request,
            request_ids,
            deadline_holder,
        )
    except _ArchiveDeadlineExpired:
        attempt_deadline = deadline_holder[0]
        if type(attempt_deadline) is not float:
            return ("authority_mismatch", "context_invalid"), None
        try:
            pair = _deadline_pair(context, attempt_deadline)
        except _ArchiveDeadlineExpired:
            pair = None
        return pair or (
            "archive_state_unavailable", "attempt_timeout"
        ), None


def _batch_with_active_scope_unlocked(
    context: Any,
    request_rows: Any,
) -> Tuple[Mapping[str, Any], ...]:
    if (
        type(context) is _ProductionArchiveRpcRunContext
        and context._historical_window_consumer is not None
    ):
        _raise_archive_error((
            "authority_mismatch",
            "historical_window_specialized_batch_required",
        ))
    scope = context._active_scope
    expected_scope_type = (
        _ProductionArchiveRpcLogicalBatchScope
        if type(context) is _ProductionArchiveRpcRunContext
        else _ArchiveRpcTestLogicalBatchScope
    )
    if (
        type(scope) is not expected_scope_type
        or scope._context is not context
        or not scope._pending
    ):
        _terminalize_archive_context(context)
        _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
    rows, canonical, request_ids = _freeze_archive_request_rows(request_rows)
    start, stop = scope._pending[0]
    expected_rows = _detached_archive_value(scope._root_rows[start:stop])
    expected_bytes = _archive_canonical_bytes(expected_rows)
    if rows != expected_rows or canonical != expected_bytes or request_ids != tuple(
        row["id"] for row in expected_rows
    ):
        _terminalize_archive_context(context)
        _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
    attempt_index = scope._attempt_count + 1
    object.__setattr__(scope, "_attempt_count", attempt_index)
    try:
        pair, outcome = _perform_archive_attempt(
            context, scope, canonical, request_ids
        )
    except BaseException:
        _terminalize_archive_context(context)
        raise
    _require_archive_collection_time(context)
    if pair is not None:
        if (
            pair == ("archive_state_unavailable", "http_413")
            and not scope._implicit
            and stop - start >= 2
        ):
            mid = start + ((stop - start) // 2)
            object.__setattr__(
                scope,
                "_pending",
                [(start, mid), (mid, stop)] + scope._pending[1:],
            )
            scope._recoverable_failures.append(_frozen_archive_value({
                "attempt_index": attempt_index,
                "reason_code": "archive_state_unavailable",
                "failure_kind": "http_413",
                "request_ids": tuple(request_ids),
            }))
            _raise_archive_error(pair)
        _terminalize_archive_context(context)
        _raise_archive_error(pair)
    if outcome is None:
        _terminalize_archive_context(context)
        _raise_archive_error(("archive_state_unavailable", "transport_unavailable"))
    exchange_index = context._next_exchange_index
    object.__setattr__(context, "_next_exchange_index", exchange_index + 1)
    record_projection = {
        "schema": "historical_foundry_archive_rpc_success_exchange/v1",
        "exchange_index": exchange_index,
        "logical_batch_index": scope._logical_batch_index,
        "attempt_index": attempt_index,
        "canonical_request_bytes": bytes(canonical),
        "request_byte_count": len(canonical),
        "request_sha256": hashlib.sha256(canonical).hexdigest(),
        "request_ids": tuple(request_ids),
        "wire_byte_count": outcome["wire_byte_count"],
        "wire_sha256": outcome["wire_sha256"],
        "decoded_response_bytes": bytes(outcome["decoded_response_bytes"]),
        "decoded_byte_count": outcome["decoded_byte_count"],
        "decoded_sha256": outcome["decoded_sha256"],
        "response_ids": tuple(outcome["response_ids"]),
    }
    record = (
        _issue_production_record(record_projection)
        if type(context) is _ProductionArchiveRpcRunContext
        else _issue_test_record(record_projection)
    )
    context._records.append(record)
    scope._success_exchange_indices.append(exchange_index)
    object.__setattr__(scope, "_pending", scope._pending[1:])
    return tuple(
        _detached_archive_response_value(row) for row in outcome["rows"]
    )


def _batch_with_active_scope(
    context: Any,
    request_rows: Any,
) -> Tuple[Mapping[str, Any], ...]:
    if type(context) is _ProductionArchiveRpcRunContext:
        with context._historical_window_lock:
            if context._historical_window_consumer is not None:
                _raise_archive_error((
                    "authority_mismatch",
                    "historical_window_specialized_batch_required",
                ))
            return _batch_with_active_scope_unlocked(context, request_rows)
    return _batch_with_active_scope_unlocked(context, request_rows)


def _archive_batch_common(
    context: Any,
    request_rows: Any,
    expected_context_type: type,
) -> Tuple[Mapping[str, Any], ...]:
    _require_archive_context(context, expected_context_type)
    if context._active_scope is not None:
        return _batch_with_active_scope(context, request_rows)
    scope = _open_archive_scope(context, request_rows, implicit=True)
    with scope:
        return _batch_with_active_scope(context, request_rows)


def _archive_rpc_test_batch_for_test(
    context: "_ArchiveRpcTestRunContext",
    request_rows: Sequence[Mapping[str, Any]],
) -> Tuple[Mapping[str, Any], ...]:
    return _archive_batch_common(context, request_rows, _ArchiveRpcTestRunContext)


def _production_archive_rpc_batch(
    context: "_ProductionArchiveRpcRunContext",
    request_rows: Sequence[Mapping[str, Any]],
) -> Tuple[Mapping[str, Any], ...]:
    if type(context) is _ProductionArchiveRpcRunContext:
        with context._historical_window_lock:
            if context._historical_window_consumer is not None:
                _raise_archive_error((
                    "authority_mismatch",
                    "historical_window_specialized_batch_required",
                ))
            return _archive_batch_common(
                context, request_rows, _ProductionArchiveRpcRunContext
            )
    return _archive_batch_common(
        context, request_rows, _ProductionArchiveRpcRunContext
    )


def _open_archive_rpc_test_logical_batch_for_test(
    context: "_ArchiveRpcTestRunContext",
    request_rows: Sequence[Mapping[str, Any]],
) -> "_ArchiveRpcTestLogicalBatchScope":
    _require_archive_context(context, _ArchiveRpcTestRunContext)
    return _open_archive_scope(context, request_rows, implicit=False)


def _open_production_archive_rpc_logical_batch(
    context: "_ProductionArchiveRpcRunContext",
    request_rows: Sequence[Mapping[str, Any]],
) -> "_ProductionArchiveRpcLogicalBatchScope":
    if type(context) is _ProductionArchiveRpcRunContext:
        with context._historical_window_lock:
            if context._historical_window_consumer is not None:
                _raise_archive_error((
                    "authority_mismatch",
                    "historical_window_specialized_batch_required",
                ))
            _require_archive_context(context, _ProductionArchiveRpcRunContext)
            return _open_archive_scope(context, request_rows, implicit=False)
    _require_archive_context(context, _ProductionArchiveRpcRunContext)
    return _open_archive_scope(context, request_rows, implicit=False)


def _finalization_projection(context: Any) -> Dict[str, Any]:
    opening = context._opening_identity
    records = tuple(
        _detached_archive_value(record._projection)
        for record in context._records
    )
    summaries = tuple(
        _detached_archive_value(summary)
        for summary in context._logical_summaries
    )
    request_count = sum(len(record["request_ids"]) for record in records)
    response_count = sum(len(record["response_ids"]) for record in records)
    identity = {
        "schema": "historical_foundry_archive_rpc_run_identity/v1",
        "repository_head": opening["repository_head"],
        "python": _detached_archive_value(opening["python"]),
        "configs": _detached_archive_value(opening["configs"]),
        "sources": tuple(_detached_archive_value(row) for row in opening["sources"]),
        "project_inputs": _detached_archive_value(opening["project_inputs"]),
        "toolchain": _detached_archive_value(opening["toolchain"]),
        "executor_artifact": _detached_archive_value(opening["executor_artifact"]),
        "resource_policy": _resource_policy_projection(),
        "endpoint_identity": _detached_archive_value(context._endpoint_identity),
        "collection": {
            "logical_batch_count": len(summaries),
            "successful_exchange_count": len(records),
            "request_count": request_count,
            "response_count": response_count,
            "wire_byte_count": sum(record["wire_byte_count"] for record in records),
            "decoded_byte_count": sum(record["decoded_byte_count"] for record in records),
        },
    }
    return {
        "schema": "historical_foundry_archive_rpc_run_finalization/v1",
        "status": "finalized",
        "identity": identity,
        "logical_batches": summaries,
        "successful_exchanges": records,
    }


def _project_archive_rpc_test_finalization_for_test(
    context: "_ArchiveRpcTestRunContext",
) -> "_ArchiveRpcTestFinalization":
    _require_archive_context(context, _ArchiveRpcTestRunContext)
    if context._active_scope is not None or context._reserved_scope is not None:
        _terminalize_archive_context(context)
        _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
    _require_archive_collection_time(context)
    object.__setattr__(context, "_state", "finalizing")
    try:
        stable = _call_test_checkpoint(context._preflight, "finalize")
    except BaseException:
        _cleanup_archive_context(context, "failed")
        raise
    if not stable:
        _cleanup_archive_context(context, "failed")
        _raise_archive_error(("authority_mismatch", "final_identity_drift"))
    projection_failed = False
    try:
        projection = _finalization_projection(context)
    except BaseException as error:
        _cleanup_archive_context(context, "failed")
        if not isinstance(error, Exception):
            raise
        projection_failed = True
    if projection_failed:
        _raise_archive_error(("authority_mismatch", "final_identity_drift"))
    _require_archive_collection_time(context)
    issue_failed = False
    try:
        finalization = _issue_test_finalization(projection)
    except BaseException as error:
        _cleanup_archive_context(context, "failed")
        if not isinstance(error, Exception):
            raise
        issue_failed = True
    if issue_failed:
        _raise_archive_error(("authority_mismatch", "final_identity_drift"))
    _cleanup_archive_context(context, "finalized")
    return finalization


def _close_archive_rpc_test_run_for_test(
    context: "_ArchiveRpcTestRunContext",
) -> None:
    if type(context) is not _ArchiveRpcTestRunContext:
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    _abandon_archive_context(context)


def _exact_python_projection() -> Dict[str, Any]:
    info = sys.version_info
    if (
        getattr(sys.implementation, "name", None) != "cpython"
        or (info.major, info.minor, info.micro) != (3, 8, 10)
        or info.releaselevel != "final"
        or info.serial != 0
        or getattr(sys.implementation, "cache_tag", None) != "cpython-38"
    ):
        raise ValueError("historical archive Python authority differs")
    return {
        "implementation": "CPython",
        "major": 3,
        "minor": 8,
        "micro": 10,
        "releaselevel": "final",
        "serial": 0,
        "cache_tag": "cpython-38",
    }


def _archive_directory_identity(metadata: os.stat_result) -> Tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        stat.S_IFMT(metadata.st_mode),
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
    )


def _archive_file_identity(metadata: os.stat_result) -> Tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        getattr(metadata, "st_mtime_ns", int(metadata.st_mtime * 1_000_000_000)),
        getattr(metadata, "st_ctime_ns", int(metadata.st_ctime * 1_000_000_000)),
    )


def _archive_directory_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise ValueError("historical archive no-follow directories unavailable")
    return os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0)


def _archive_file_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("historical archive no-follow files unavailable")
    return os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)


def _read_archive_fd(fd: int) -> bytes:
    chunks = []
    offset = 0
    while True:
        chunk = os.pread(fd, min(64 * 1024, _ARCHIVE_MEMBER_BYTES + 1 - offset), offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
        if offset > _ARCHIVE_MEMBER_BYTES:
            raise ValueError("historical archive source member is too large")
    return b"".join(chunks)


class _HeldArchiveSourceAuthority:
    __slots__ = (
        "root", "root_fd", "directories", "files", "projections", "closed",
        "_close_state",
    )

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root_fd = -1
        self.directories = {}
        self.files = {}
        self.projections = ()
        self.closed = False
        self._close_state = {
            "phase": "files",
            "attempted_fds": set(),
            "control": None,
        }
        try:
            before = os.stat(str(root), follow_symlinks=False)
            root_fd = os.open(str(root), _archive_directory_flags())
            descriptor = os.fstat(root_fd)
            identity = _archive_directory_identity(descriptor)
            if (
                _archive_directory_identity(before) != identity
                or not stat.S_ISDIR(descriptor.st_mode)
                or descriptor.st_uid != os.geteuid()
                or stat.S_IMODE(descriptor.st_mode) & 0o022
            ):
                raise ValueError("historical archive root is unsafe")
            self.root_fd = root_fd
            self.directories[()] = (root_fd, None, None, identity)
            self.verify()
        except BaseException:
            self.close()
            raise

    def open_members(self) -> None:
        if self.closed or self.files or self.projections:
            raise ValueError("historical archive source inventory state is invalid")
        try:
            self.verify()
            projections = []
            for role, module_name, relative in _PRODUCTION_SOURCE_MEMBERS:
                components = tuple(relative.split("/"))
                parent_key = ()
                parent_fd = self.root_fd
                for name in components[:-1]:
                    key = parent_key + (name,)
                    if key not in self.directories:
                        path_metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                        child_fd = os.open(name, _archive_directory_flags(), dir_fd=parent_fd)
                        child_metadata = os.fstat(child_fd)
                        child_identity = _archive_directory_identity(child_metadata)
                        if (
                            _archive_directory_identity(path_metadata) != child_identity
                            or not stat.S_ISDIR(child_metadata.st_mode)
                            or child_metadata.st_uid != os.geteuid()
                            or stat.S_IMODE(child_metadata.st_mode) & 0o022
                        ):
                            os.close(child_fd)
                            raise ValueError("historical archive source ancestry is unsafe")
                        self.directories[key] = (
                            child_fd, parent_fd, name, child_identity
                        )
                    parent_fd = self.directories[key][0]
                    parent_key = key
                name = components[-1]
                path_metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                fd = os.open(name, _archive_file_flags(), dir_fd=parent_fd)
                descriptor = os.fstat(fd)
                identity = _archive_file_identity(descriptor)
                if (
                    _archive_file_identity(path_metadata) != identity
                    or not stat.S_ISREG(descriptor.st_mode)
                    or descriptor.st_nlink != 1
                    or descriptor.st_uid != os.geteuid()
                    or stat.S_IMODE(descriptor.st_mode) & 0o022
                ):
                    os.close(fd)
                    raise ValueError("historical archive source member is unsafe")
                payload = _read_archive_fd(fd)
                if _archive_file_identity(os.fstat(fd)) != identity:
                    os.close(fd)
                    raise ValueError("historical archive source member changed")
                digest = hashlib.sha256(payload).hexdigest()
                self.files[role] = (
                    fd, parent_fd, name, relative, module_name, identity,
                    payload, digest,
                )
                projections.append({
                    "role": role,
                    "size_bytes": len(payload),
                    "sha256": digest,
                })
            self.projections = tuple(projections)
            self.verify()
        except BaseException:
            self.close()
            raise

    def verify(self) -> None:
        if self.closed:
            raise ValueError("historical archive source authority is closed")
        for key, (fd, parent_fd, name, identity) in self.directories.items():
            descriptor = os.fstat(fd)
            if key == ():
                path_metadata = os.stat(str(self.root), follow_symlinks=False)
            else:
                path_metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if (
                _archive_directory_identity(descriptor) != identity
                or _archive_directory_identity(path_metadata) != identity
            ):
                raise ValueError("historical archive source ancestry changed")
        for _role, row in self.files.items():
            fd, parent_fd, name, _relative, _module, identity, payload, digest = row
            if (
                _archive_file_identity(os.fstat(fd)) != identity
                or _archive_file_identity(
                    os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                ) != identity
            ):
                raise ValueError("historical archive source member changed")
            observed = _read_archive_fd(fd)
            if observed != payload or hashlib.sha256(observed).hexdigest() != digest:
                raise ValueError("historical archive source bytes changed")
        self._verify_module_origins()

    def _verify_module_origins(self) -> None:
        for _role, row in self.files.items():
            _fd, _parent_fd, _name, relative, module_name, _identity, _payload, _digest = row
            if module_name is None:
                continue
            module = importlib.import_module(module_name)
            spec = getattr(module, "__spec__", None)
            origin = getattr(spec, "origin", None)
            file_name = getattr(module, "__file__", None)
            expected = (self.root / relative).resolve(strict=True)
            if (
                type(origin) is not str
                or type(file_name) is not str
                or Path(origin).resolve(strict=True) != expected
                or Path(file_name).resolve(strict=True) != expected
            ):
                raise ValueError("historical archive module origin differs")

    def member_bytes(self, role: str) -> bytes:
        return bytes(self.files[role][6])

    def member_digest(self, role: str) -> str:
        return self.files[role][7]

    def close(self) -> None:
        state = self._close_state
        while state["phase"] != "done":
            try:
                phase = state["phase"]
                if phase == "files":
                    if self.files:
                        role = next(iter(self.files))
                        fd = self.files[role][0]
                        state["attempted_fds"].add(fd); del self.files[role]; os.close(fd)
                        continue
                    self.projections = ()
                    state["phase"] = "directories"
                    continue
                if phase == "directories":
                    if self.directories:
                        key = tuple(self.directories)[-1]
                        fd = self.directories[key][0]
                        state["attempted_fds"].add(fd); del self.directories[key]; os.close(fd)
                        continue
                    self.root_fd = -1
                    state["phase"] = "finish"
                    continue
                if phase == "finish":
                    state["phase"] = "done"; self.closed = True
                    continue
                raise ValueError("historical archive source cleanup state differs")
            except BaseException as error:
                if not isinstance(error, Exception) and state["control"] is None:
                    state["control"] = error
        control = state["control"]
        state["control"] = None
        if control is not None:
            raise control


def _git_output(root: Path, arguments: Tuple[str, ...], maximum: int = 4 * 1024 * 1024) -> bytes:
    completed = subprocess.run(
        ("/usr/bin/git",) + arguments,
        cwd=str(root),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=30,
        env={"LANG": "C", "LC_ALL": "C"},
    )
    if completed.returncode != 0 or len(completed.stdout) > maximum or len(completed.stderr) > maximum:
        raise ValueError("historical archive Git authority unavailable")
    return completed.stdout


def _git_identity(root: Path) -> str:
    top = _git_output(root, ("rev-parse", "--show-toplevel")).decode("utf-8").strip()
    if Path(top).resolve(strict=True) != root:
        raise ValueError("historical archive Git root differs")
    if _git_output(root, ("status", "--porcelain=v1", "--untracked-files=all")):
        raise ValueError("historical archive Git tree is dirty")
    head = _git_output(root, ("rev-parse", "HEAD")).decode("ascii").strip()
    if re.fullmatch(r"[0-9a-f]{40}", head, re.ASCII) is None:
        raise ValueError("historical archive Git HEAD is invalid")
    paths = tuple(relative for _role, _module, relative in _PRODUCTION_SOURCE_MEMBERS)
    rows = _git_output(root, ("ls-files", "--stage", "-z", "--") + paths).split(b"\0")
    seen = set()
    for row in rows:
        if not row:
            continue
        try:
            prefix, raw_path = row.split(b"\t", 1)
            mode, object_name, stage_value = prefix.decode("ascii").split(" ")
            path = raw_path.decode("utf-8")
        except (UnicodeDecodeError, ValueError):
            raise ValueError("historical archive Git inventory is invalid") from None
        if (
            stage_value != "0"
            or re.fullmatch(r"[0-7]{6}", mode, re.ASCII) is None
            or re.fullmatch(r"[0-9a-f]{40,64}", object_name, re.ASCII) is None
            or path not in paths
            or path in seen
        ):
            raise ValueError("historical archive Git inventory differs")
        seen.add(path)
    if seen != set(paths):
        raise ValueError("historical archive Git stage-zero member is absent")
    return head


def _config_projection(config: Any) -> Dict[str, Any]:
    return {
        "policy_id": config.policy.policy_id,
        "policy_physical_sha256": config.policy.physical_sha256,
        "authority_physical_sha256": config.authority.physical_sha256,
        "toolchain_physical_sha256": config.toolchain.physical_sha256,
    }


def _require_config_source_bytes(config: Any, sources: _HeldArchiveSourceAuthority) -> None:
    if (
        config.policy.physical_bytes != sources.member_bytes("config:replay_policy")
        or config.authority.physical_bytes != sources.member_bytes("config:replay_authority")
        or config.toolchain.physical_bytes != sources.member_bytes("config:replay_toolchain")
    ):
        raise ValueError("historical archive config bytes differ from source authority")


def _replay_toolchain_binaries(
    rows: Any, required_container_type: type
) -> Tuple[Dict[str, str], ...]:
    if type(rows) is not required_container_type or len(rows) != 3:
        raise ValueError("historical archive toolchain binaries are invalid")
    replayed = []
    for expected_name, row in zip(("forge", "cast", "anvil"), rows):
        if (
            type(row) is not dict
            or set(row) != {"name", "sha256", "version"}
            or type(row.get("name")) is not str
            or row.get("name") != expected_name
            or type(row.get("sha256")) is not str
            or re.fullmatch(r"[0-9a-f]{64}", row["sha256"]) is None
            or type(row.get("version")) is not str
            or not row["version"]
        ):
            raise ValueError("historical archive toolchain binary is invalid")
        replayed.append({
            "name": row["name"],
            "sha256": row["sha256"],
            "version": row["version"],
        })
    return tuple(replayed)


def _toolchain_projection(candidate: Mapping[str, Any], config_value: Mapping[str, Any]) -> Dict[str, Any]:
    if type(candidate) is not dict or type(config_value) is not dict:
        raise ValueError("historical archive toolchain identity is invalid")
    release = candidate.get("foundry_release")
    configured_release = config_value.get("foundry_release")
    solc = candidate.get("solc")
    configured_solc = config_value.get("solc")
    forge_std = candidate.get("forge_std")
    configured_forge_std = config_value.get("forge_std")
    candidate_binaries = _replay_toolchain_binaries(
        candidate.get("binaries"), list
    )
    configured_binaries = _replay_toolchain_binaries(
        config_value.get("binaries"), tuple
    )
    if (
        set(candidate) != {
            "schema", "source_lock_sha256", "foundry_release", "binaries",
            "solc", "forge_std", "compiler_settings",
        }
        or set(config_value) != {
            "schema", "foundry_release", "binaries", "solc", "forge_std",
            "compiler_settings", "executor_build",
        }
        or candidate.get("schema") != "historical_foundry_toolchain_candidate/v1"
        or type(release) is not dict
        or type(configured_release) is not dict
        or type(solc) is not dict
        or type(configured_solc) is not dict
        or type(forge_std) is not dict
        or type(configured_forge_std) is not dict
        or set(release) != {
            "archive_sha256", "archive_url", "checksum_sha256",
            "checksum_url", "release_commit", "sigstore_issuer",
            "sigstore_san", "sigstore_sha256", "sigstore_url",
            "spdx_sha256", "spdx_url", "version",
        }
        or set(configured_release) != {
            "archive_sha256", "archive_url", "checksum_sha256",
            "checksum_url", "provenance_sha256", "provenance_url",
            "release_commit", "sigstore_identity", "sigstore_issuer",
            "version",
        }
        or set(solc) != {
            "artifact_sha256", "artifact_url", "sha256", "source_commit",
            "version",
        }
        or set(configured_solc) != {
            "artifact_sha256", "artifact_url", "version",
        }
        or set(forge_std) != {"commit", "repository_url", "version"}
        or set(configured_forge_std) != {
            "commit", "repository_url", "version",
        }
        or candidate_binaries != configured_binaries
        or candidate.get("compiler_settings") != config_value.get("compiler_settings")
        or {
            "version": release.get("version"),
            "archive_url": release.get("archive_url"),
            "archive_sha256": release.get("archive_sha256"),
            "checksum_url": release.get("checksum_url"),
            "checksum_sha256": release.get("checksum_sha256"),
            "provenance_url": release.get("sigstore_url"),
            "provenance_sha256": release.get("sigstore_sha256"),
            "sigstore_issuer": release.get("sigstore_issuer"),
            "sigstore_identity": release.get("sigstore_san"),
            "release_commit": release.get("release_commit"),
        } != configured_release
        or {
            "version": solc.get("version"),
            "artifact_url": solc.get("artifact_url"),
            "artifact_sha256": solc.get("artifact_sha256"),
        } != configured_solc
        or solc.get("sha256") != configured_solc.get("artifact_sha256")
        or {
            "repository_url": forge_std.get("repository_url"),
            "version": forge_std.get("version"),
            "commit": forge_std.get("commit"),
        } != configured_forge_std
    ):
        raise ValueError("historical archive toolchain identity differs")
    return {
        "source_lock_sha256": candidate["source_lock_sha256"],
        "foundry_release": {
            "version": release["version"],
            "archive_sha256": release["archive_sha256"],
            "checksum_sha256": release["checksum_sha256"],
            "sigstore_sha256": release["sigstore_sha256"],
            "sigstore_issuer": release["sigstore_issuer"],
            "sigstore_san": release["sigstore_san"],
            "spdx_sha256": release["spdx_sha256"],
            "release_commit": release["release_commit"],
        },
        "binaries": candidate_binaries,
        "solc": {
            "version": solc["version"],
            "artifact_sha256": solc["artifact_sha256"],
            "sha256": solc["sha256"],
            "source_commit": solc["source_commit"],
        },
        "forge_std": {
            "version": forge_std["version"],
            "commit": forge_std["commit"],
        },
        "compiler_settings": _detached_archive_value(candidate["compiler_settings"]),
    }


def _artifact_projection(artifact: Any, config: Any) -> Tuple[Dict[str, Any], str]:
    identity = artifact.verified_identity
    if type(identity) is not dict or set(identity) != {
        "source_tree_sha256", "constructor_args_sha256",
        "creation_bytecode_sha256", "deployed_runtime_sha256",
        "immutable_references_sha256", "artifact_manifest_sha256",
        "policy_physical_sha256", "authority_physical_sha256",
        "toolchain_physical_sha256",
    }:
        raise ValueError("historical archive executor artifact identity is invalid")
    expected = dict(config.toolchain.value["executor_build"])
    expected.update(_config_projection(config))
    expected.pop("policy_id", None)
    if identity != expected:
        raise ValueError("historical archive executor artifact differs")
    runtime = artifact._deployed_runtime_for_state_override()
    digest = hashlib.sha256(runtime).hexdigest()
    if digest != identity["deployed_runtime_sha256"]:
        raise ValueError("historical archive executor runtime differs")
    return _detached_archive_value(identity), digest


def _project_inputs_equal_sources(
    project_inputs: Mapping[str, Any],
    config_value: Mapping[str, Any],
    sources: _HeldArchiveSourceAuthority,
) -> None:
    if (
        type(project_inputs) is not dict
        or set(project_inputs) != {
            "schema", "foundry_toml_sha256", "foundry_lock_sha256",
            "gitmodules_sha256", "forge_std_commit", "forge_std_tree_sha256",
        }
        or project_inputs.get("schema") != "historical_foundry_project_input_identity/v1"
        or project_inputs.get("foundry_toml_sha256") != sources.member_digest("build:foundry_toml")
        or project_inputs.get("foundry_lock_sha256") != sources.member_digest("build:foundry_lock")
        or project_inputs.get("gitmodules_sha256") != sources.member_digest("build:gitmodules")
        or project_inputs.get("forge_std_commit") != config_value["forge_std"]["commit"]
    ):
        raise ValueError("historical archive project input identity differs")


class _ProductionArchivePreflight:
    __slots__ = (
        "root", "sources", "config", "toolchain", "artifact",
        "identity", "closed", "_close_state",
    )

    def __init__(
        self,
        root: Path,
        sources: _HeldArchiveSourceAuthority,
        config: Any,
        toolchain: Any,
        artifact: Any,
        identity: Mapping[str, Any],
    ) -> None:
        self.root = root
        self.sources = sources
        self.config = config
        self.toolchain = toolchain
        self.artifact = artifact
        self.identity = _frozen_archive_value(dict(identity))
        self.closed = False
        self._close_state = {
            "phase": "toolchain",
            "control": None,
            "ordinary": None,
        }

    def __repr__(self) -> str:
        return "_ProductionArchivePreflight(<sealed>)"

    def close(self) -> None:
        state = self._close_state
        while state["phase"] != "done":
            try:
                phase = state["phase"]
                if phase == "toolchain":
                    state["phase"] = "sources"; self.toolchain._close()
                    continue
                if phase == "sources":
                    state["phase"] = "finish"; self.sources.close()
                    continue
                if phase == "finish":
                    state["phase"] = "done"; self.closed = True
                    continue
                raise ValueError("historical archive preflight cleanup state differs")
            except BaseException as error:
                if isinstance(error, Exception):
                    if state["ordinary"] is None:
                        state["ordinary"] = error
                elif state["control"] is None:
                    state["control"] = error
        control = state["control"]
        ordinary = state["ordinary"]
        state["control"] = None
        state["ordinary"] = None
        if control is not None:
            raise control
        if ordinary is not None:
            raise ordinary


def _perform_production_preflight() -> Optional[_ProductionArchivePreflight]:
    sources = None
    toolchain = None
    try:
        python_identity = _exact_python_projection()
        root = Path(__file__).resolve().parents[1]
        sources = _HeldArchiveSourceAuthority(root)
        head = _git_identity(root)
        sources.open_members()
        config = load_historical_foundry_config_set()
        _require_config_source_bytes(config, sources)
        config_value = _detached_archive_value(config.toolchain.value)
        from scripts.bootstrap_historical_foundry_toolchain import (
            open_reviewed_historical_toolchain,
        )
        toolchain = open_reviewed_historical_toolchain()
        candidate = toolchain.verified_identity
        toolchain_projection = _toolchain_projection(candidate, config_value)
        project_inputs = toolchain.verified_project_input_identity()
        _project_inputs_equal_sources(project_inputs, config_value, sources)
        toolchain._verify_versions_and_hardfork()
        artifact = build_validated_executor_artifact(config)
        artifact_projection, runtime_digest = _artifact_projection(artifact, config)
        toolchain._verify_versions_and_hardfork()
        repeated_project_inputs = toolchain.verified_project_input_identity()
        _project_inputs_equal_sources(
            repeated_project_inputs, config_value, sources
        )
        if repeated_project_inputs != project_inputs:
            raise ValueError("historical archive project input identity changed")
        repeated_toolchain_projection = _toolchain_projection(
            toolchain.verified_identity, config_value
        )
        if repeated_toolchain_projection != toolchain_projection:
            raise ValueError("historical archive toolchain identity changed")
        sources.verify()
        if _git_identity(root) != head or _exact_python_projection() != python_identity:
            raise ValueError("historical archive opening identity changed")
        repeated_config = load_historical_foundry_config_set()
        _require_config_source_bytes(repeated_config, sources)
        if _config_projection(repeated_config) != _config_projection(config):
            raise ValueError("historical archive config identity changed")
        repeated_artifact_projection, repeated_runtime_digest = (
            _artifact_projection(artifact, repeated_config)
        )
        if (
            repeated_artifact_projection != artifact_projection
            or repeated_runtime_digest != runtime_digest
        ):
            raise ValueError("historical archive artifact identity changed")
        identity = {
            "repository_head": head,
            "python": python_identity,
            "configs": _config_projection(config),
            "sources": tuple(_detached_archive_value(row) for row in sources.projections),
            "project_inputs": _detached_archive_value(project_inputs),
            "toolchain": toolchain_projection,
            "executor_artifact": artifact_projection,
            "deployed_runtime_sha256": runtime_digest,
        }
        return _ProductionArchivePreflight(
            root, sources, config, toolchain, artifact, identity
        )
    except BaseException as error:
        cleanup_control = None
        if toolchain is not None:
            try:
                toolchain._close()
            except BaseException as cleanup_error:
                if (
                    not isinstance(cleanup_error, Exception)
                    and cleanup_control is None
                ):
                    cleanup_control = cleanup_error
        if sources is not None:
            try:
                sources.close()
            except BaseException as cleanup_error:
                if (
                    not isinstance(cleanup_error, Exception)
                    and cleanup_control is None
                ):
                    cleanup_control = cleanup_error
        if not isinstance(error, Exception):
            raise
        if cleanup_control is not None:
            raise cleanup_control
        return None


def _recheck_production_preflight(preflight: _ProductionArchivePreflight) -> bool:
    try:
        opening = preflight.identity
        if preflight.closed:
            raise ValueError("historical archive production preflight is closed")
        python_identity = _exact_python_projection()
        head = _git_identity(preflight.root)
        preflight.sources.verify()
        config = load_historical_foundry_config_set()
        _require_config_source_bytes(config, preflight.sources)
        config_value = _detached_archive_value(config.toolchain.value)
        candidate = preflight.toolchain.verified_identity
        toolchain_projection = _toolchain_projection(candidate, config_value)
        project_inputs = preflight.toolchain.verified_project_input_identity()
        _project_inputs_equal_sources(project_inputs, config_value, preflight.sources)
        preflight.toolchain._verify_versions_and_hardfork()
        artifact = build_validated_executor_artifact(config)
        artifact_projection, runtime_digest = _artifact_projection(artifact, config)
        preflight.toolchain._verify_versions_and_hardfork()
        repeated_project_inputs = (
            preflight.toolchain.verified_project_input_identity()
        )
        _project_inputs_equal_sources(
            repeated_project_inputs, config_value, preflight.sources
        )
        if repeated_project_inputs != project_inputs:
            raise ValueError("historical archive final project input changed")
        repeated_toolchain_projection = _toolchain_projection(
            preflight.toolchain.verified_identity, config_value
        )
        if repeated_toolchain_projection != toolchain_projection:
            raise ValueError("historical archive final toolchain identity changed")
        preflight.sources.verify()
        if (
            _git_identity(preflight.root) != head
            or _exact_python_projection() != python_identity
        ):
            raise ValueError("historical archive final runtime identity changed")
        repeated_config = load_historical_foundry_config_set()
        _require_config_source_bytes(repeated_config, preflight.sources)
        if _config_projection(repeated_config) != _config_projection(config):
            raise ValueError("historical archive final config identity changed")
        repeated_artifact_projection, repeated_runtime_digest = (
            _artifact_projection(artifact, repeated_config)
        )
        if (
            repeated_artifact_projection != artifact_projection
            or repeated_runtime_digest != runtime_digest
        ):
            raise ValueError("historical archive final artifact identity changed")
        observed = {
            "repository_head": head,
            "python": python_identity,
            "configs": _config_projection(config),
            "sources": tuple(_detached_archive_value(row) for row in preflight.sources.projections),
            "project_inputs": _detached_archive_value(project_inputs),
            "toolchain": toolchain_projection,
            "executor_artifact": artifact_projection,
            "deployed_runtime_sha256": runtime_digest,
        }
        return observed == opening
    except Exception:
        return False


class _ArchiveNoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def _activate_production_archive_rpc_run(
    preflight: _ProductionArchivePreflight,
) -> "_ProductionArchiveRpcRunContext":
    started = _initial_clock_sample(time.monotonic)
    if started is None:
        preflight.close()
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    entropy_failed = False
    key_bytes = None
    try:
        key_bytes = os.urandom(32)
    except Exception:
        entropy_failed = True
    if entropy_failed or type(key_bytes) is not bytes or len(key_bytes) != 32:
        preflight.close()
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    key = bytearray(key_bytes)
    endpoint_failed = False
    endpoint = None
    try:
        endpoint = os.environ.get("DEX_DEPTH_RPC_ETH")
    except (KeyboardInterrupt, SystemExit):
        _erase_archive_key(key)
        raise
    except Exception:
        endpoint_failed = True
    if endpoint_failed:
        _erase_archive_key(key)
        preflight.close()
        _raise_archive_error(("authority_mismatch", "context_invalid"))
    if endpoint is None or endpoint == "":
        _erase_archive_key(key)
        preflight.close()
        _raise_archive_error(("archive_state_unavailable", "endpoint_missing"))
    endpoint_pair = None
    endpoint_result = None
    try:
        endpoint_result = _canonicalize_archive_endpoint(endpoint)
    except _ArchiveRpcError as error:
        endpoint_pair = (error.reason_code, error.failure_kind)
    if endpoint_pair is not None or endpoint_result is None:
        _erase_archive_key(key)
        preflight.close()
        _raise_archive_error(
            endpoint_pair or ("archive_state_unavailable", "endpoint_invalid")
        )
    projection, endpoint_bytes, connection_url = endpoint_result
    opener_failed = False
    opener = None
    try:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _ArchiveNoRedirectHandler(),
        )
        opener.addheaders = []
    except (KeyboardInterrupt, SystemExit):
        _erase_archive_key(key)
        raise
    except Exception:
        opener_failed = True
    if opener_failed or opener is None:
        _erase_archive_key(key)
        preflight.close()
        _raise_archive_error(("authority_mismatch", "context_invalid"))

    def operation(body: bytes, timeout: float) -> Any:
        request = urllib.request.Request(
            connection_url,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "historical-foundry-archive-rpc/1",
            },
            method="POST",
        )
        try:
            return opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            return error

    def relay_operation(body: bytes, timeout: float) -> bytes:
        response = operation(body, timeout)
        try:
            status = getattr(response, "status", None)
            headers = getattr(response, "headers", None)
            items = tuple(headers.items()) if headers is not None else ()
            header_bytes = sum(
                len(str(name).encode("utf-8"))
                + len(str(value).encode("utf-8")) + 4
                for name, value in items
            )
            if status != 200 or header_bytes > 65_536:
                raise ValueError("historical relay upstream response is invalid")
            encoding = "" if headers is None else headers.get(
                "Content-Encoding", ""
            )
            if str(encoding).strip().lower() not in ("", "identity"):
                raise ValueError("historical relay upstream response is invalid")
            payload = response.read(67_108_865)
            if type(payload) is not bytes or len(payload) > 67_108_864:
                raise ValueError("historical relay upstream response is invalid")
            return payload
        finally:
            closer = getattr(response, "close", None)
            if callable(closer):
                closer()

    digest = hmac.new(key, endpoint_bytes, hashlib.sha256).hexdigest()
    endpoint_identity = {
        "schema": "historical_foundry_rpc_endpoint_identity/v1",
        "scope": "single_run_nonreversible",
        "endpoint_hmac_sha256": digest,
    }
    relay_lease = _issue_historical_relay_lease_from_run(
        key=key,
        endpoint_bytes=endpoint_bytes,
        endpoint_identity=_frozen_archive_value(endpoint_identity),
        connection_url=connection_url,
        operation=relay_operation,
        monotonic=time.monotonic,
        last_clock=started,
    )
    return _issue_production_context(
        _state="active",
        _clock=time.monotonic,
        _last_clock=started,
        _collection_deadline=started + _ARCHIVE_COLLECTION_DEADLINE_SECONDS,
        _key=key,
        _endpoint_projection=_frozen_archive_value(projection),
        _endpoint_bytes=endpoint_bytes,
        _connection_url=connection_url,
        _endpoint_identity=_frozen_archive_value(endpoint_identity),
        _operation=operation,
        _preflight=preflight,
        _opening_identity=_frozen_archive_value(
            _detached_archive_value(preflight.identity)
        ),
        _active_scope=None,
        _reserved_scope=None,
        _logical_summaries=[],
        _records=[],
        _next_logical_batch_index=1,
        _next_exchange_index=1,
        _relay_lease=relay_lease,
        _relay_moved=False,
    )


def _open_production_archive_rpc_run_core() -> "_ProductionArchiveRpcRunContext":
    preflight = _perform_production_preflight()
    if preflight is None:
        _raise_archive_error(("authority_mismatch", "preflight_invalid"))
    try:
        return _activate_production_archive_rpc_run(preflight)
    except BaseException as body_error:
        cleanup_control = None
        try:
            preflight.close()
        except BaseException as cleanup_error:
            if not isinstance(cleanup_error, Exception):
                cleanup_control = cleanup_error
        if not isinstance(body_error, Exception):
            raise
        if cleanup_control is not None:
            raise cleanup_control
        raise


def _make_production_archive_rpc_run_opener(
    core: Callable[[], "_ProductionArchiveRpcRunContext"],
    marker: Callable[
        ["_ProductionArchiveRpcRunContext"],
        "_ProductionArchiveRpcRunContext",
    ],
) -> Callable[[], "_ProductionArchiveRpcRunContext"]:
    def open_run() -> "_ProductionArchiveRpcRunContext":
        return marker(core())

    return open_run


_open_production_archive_rpc_run = _make_production_archive_rpc_run_opener(
    _open_production_archive_rpc_run_core,
    _mark_fresh_production_archive_rpc_run_for_historical_window,
)
del _make_production_archive_rpc_run_opener
del _mark_fresh_production_archive_rpc_run_for_historical_window


def _finalize_production_archive_rpc_run_unlocked(
    context: "_ProductionArchiveRpcRunContext",
) -> "_ProductionArchiveRpcFinalization":
    if type(context) is _ProductionArchiveRpcRunContext:
        with context._historical_window_lock:
            if context._historical_window_consumer is not None:
                _raise_archive_error((
                    "authority_mismatch",
                    "historical_window_specialized_batch_required",
                ))
    _require_archive_context(context, _ProductionArchiveRpcRunContext)
    if context._active_scope is not None or context._reserved_scope is not None:
        _terminalize_archive_context(context)
        _raise_archive_error(("authority_mismatch", "logical_batch_scope_invalid"))
    _require_archive_collection_time(context)
    object.__setattr__(context, "_state", "finalizing")
    try:
        stable = _recheck_production_preflight(context._preflight)
    except BaseException:
        _cleanup_archive_context(context, "failed")
        raise
    if not stable:
        _cleanup_archive_context(context, "failed")
        _raise_archive_error(("authority_mismatch", "final_identity_drift"))
    projection_failed = False
    try:
        projection = _finalization_projection(context)
    except BaseException as error:
        _cleanup_archive_context(context, "failed")
        if not isinstance(error, Exception):
            raise
        projection_failed = True
    if projection_failed:
        _raise_archive_error(("authority_mismatch", "final_identity_drift"))
    _require_archive_collection_time(context)
    issue_failed = False
    try:
        finalization = _issue_production_finalization(projection)
    except BaseException as error:
        _cleanup_archive_context(context, "failed")
        if not isinstance(error, Exception):
            raise
        issue_failed = True
    if issue_failed:
        _raise_archive_error(("authority_mismatch", "final_identity_drift"))
    _cleanup_archive_context(context, "finalized")
    return finalization


def _finalize_production_archive_rpc_run(
    context: "_ProductionArchiveRpcRunContext",
) -> "_ProductionArchiveRpcFinalization":
    if type(context) is _ProductionArchiveRpcRunContext:
        with context._historical_window_lock:
            return _finalize_production_archive_rpc_run_unlocked(context)
    return _finalize_production_archive_rpc_run_unlocked(context)


def _serialize_module_visible_production_cores(
    open_scope_core: Callable[..., Any],
    active_scope_core: Callable[..., Any],
    batch_common_core: Callable[..., Any],
    finalize_core: Callable[..., Any],
    core_gate: Any,
) -> Tuple[Callable[..., Any], ...]:
    def enter_claimed_core(context: Any, kind: Optional[str]) -> Any:
        claimed = core_gate["claimed"].get(id(context))
        if claimed is None or claimed[0] is not context:
            return None
        permit = (
            core_gate[kind].pop(id(context), None)
            if kind is not None else None
        )
        if permit is None or permit[0] is not context:
            _raise_archive_error((
                "authority_mismatch",
                "historical_window_specialized_batch_required",
            ))
        consumer = context._historical_window_consumer
        object.__setattr__(context, "_historical_window_consumer", None)
        return consumer

    def leave_claimed_core(context: Any, consumer: Any) -> None:
        if consumer is not None and context._state == "active":
            object.__setattr__(
                context, "_historical_window_consumer", consumer
            )

    def open_scope(
        context: Any, request_rows: Any, *, implicit: bool
    ) -> Any:
        if type(context) is _ProductionArchiveRpcRunContext:
            with context._historical_window_lock:
                consumer = enter_claimed_core(context, "open")
                try:
                    return open_scope_core(
                        context, request_rows, implicit=implicit
                    )
                finally:
                    leave_claimed_core(context, consumer)
        return open_scope_core(context, request_rows, implicit=implicit)

    def active_scope(context: Any, request_rows: Any) -> Any:
        if type(context) is _ProductionArchiveRpcRunContext:
            with context._historical_window_lock:
                enter_claimed_core(context, None)
                return active_scope_core(context, request_rows)
        return active_scope_core(context, request_rows)

    def batch_common(
        context: Any, request_rows: Any, expected_context_type: type
    ) -> Any:
        if type(context) is _ProductionArchiveRpcRunContext:
            with context._historical_window_lock:
                enter_claimed_core(context, None)
                return batch_common_core(
                    context, request_rows, expected_context_type
                )
        return batch_common_core(
            context, request_rows, expected_context_type
        )

    def finalize(
        context: "_ProductionArchiveRpcRunContext",
    ) -> "_ProductionArchiveRpcFinalization":
        if type(context) is _ProductionArchiveRpcRunContext:
            with context._historical_window_lock:
                consumer = enter_claimed_core(context, "finalize")
                try:
                    return finalize_core(context)
                finally:
                    leave_claimed_core(context, consumer)
        return finalize_core(context)

    return open_scope, active_scope, batch_common, finalize


(
    _open_archive_scope,
    _batch_with_active_scope_unlocked,
    _archive_batch_common,
    _finalize_production_archive_rpc_run_unlocked,
) = _serialize_module_visible_production_cores(
    _open_archive_scope,
    _batch_with_active_scope_unlocked,
    _archive_batch_common,
    _finalize_production_archive_rpc_run_unlocked,
    _historical_window_claimed_core_gate,
)
del _serialize_module_visible_production_cores
del _historical_window_claimed_core_gate
