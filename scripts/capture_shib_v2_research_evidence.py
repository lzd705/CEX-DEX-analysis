"""Capture one bounded, dual-provider SHIB V2/V2 evidence generation."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Callable, List, Sequence, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import shib_v2_research  # noqa: E402
from scripts.shib_v2_research_io import (  # noqa: E402
    atomic_write_canonical_json,
    load_bounded_json,
)


CAPTURE_REASONS = {
    "capture_configuration_invalid",
    "chain_id_mismatch",
    "provider_disagreement",
    "canonical_block_unavailable",
    "eip1898_unavailable",
    "required_call_missing",
    "pool_authority_mismatch",
    "router_authority_mismatch",
    "fee_authority_mismatch",
    "usd_reference_unavailable",
    "registry_invalid",
    "rpc_response_invalid",
    "unsafe_output_path",
}

MAX_RPC_RESPONSE_BYTES = 256 * 1024
MAX_RPC_MEMBERS = 4096
MAX_RPC_DEPTH = 16
MAX_RPC_STRING_BYTES = 192 * 1024
MAX_CALL_RESULT_BYTES = 64 * 1024
MAX_TIMEOUT_SECONDS = 60
_HASH32 = re.compile(r"0x[0-9a-f]{64}$")
_ADDRESS = re.compile(r"0x[0-9a-f]{40}$")
_HEX_DATA = re.compile(r"0x(?:[0-9a-f]{2})*$")
_HEX_QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)$")
_SHA256 = re.compile(r"[0-9a-f]{64}$")


class CaptureError(RuntimeError):
    """A stable, allowlisted failure at the private capture boundary."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in CAPTURE_REASONS:
            reason_code = "rpc_response_invalid"
        self.reason_code = reason_code
        super().__init__(reason_code)


class _RemoteRpcError(Exception):
    pass


def sanitize_capture_failure(error: BaseException) -> str:
    """Project no exception text other than one reviewed reason code."""
    if isinstance(error, CaptureError):
        return error.reason_code
    return "rpc_response_invalid"


@dataclass(frozen=True)
class Provider:
    label: str
    endpoint_identity: str
    rpc: Callable[[str, list], object]


class _RejectRedirects(urllib_request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _reject_float(_token: str) -> float:
    raise ValueError("float token rejected")


def _reject_constant(_token: str) -> object:
    raise ValueError("nonfinite token rejected")


def _reject_duplicate_keys(pairs):
    value = {}
    for key, child in pairs:
        if key in value:
            raise ValueError("duplicate key rejected")
        value[key] = child
    return value


def _bounded_int(token: str) -> int:
    if len(token) > 128:
        raise ValueError("integer token rejected")
    return int(token)


def _check_rpc_shape(value: object, depth: int = 0) -> int:
    if depth > MAX_RPC_DEPTH:
        raise ValueError("RPC depth rejected")
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_RPC_STRING_BYTES:
            raise ValueError("RPC string rejected")
        return 0
    if isinstance(value, dict):
        members = len(value)
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("RPC key rejected")
            members += _check_rpc_shape(child, depth + 1)
        if members > MAX_RPC_MEMBERS:
            raise ValueError("RPC members rejected")
        return members
    if isinstance(value, list):
        members = len(value)
        for child in value:
            members += _check_rpc_shape(child, depth + 1)
        if members > MAX_RPC_MEMBERS:
            raise ValueError("RPC members rejected")
        return members
    if value is None or isinstance(value, bool) or type(value) is int:
        return 0
    raise ValueError("RPC value rejected")


def _parse_rpc_response(raw: bytes) -> object:
    if len(raw) > MAX_RPC_RESPONSE_BYTES:
        raise CaptureError("rpc_response_invalid")
    try:
        response = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_int=_bounded_int,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
        _check_rpc_shape(response)
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        raise CaptureError("rpc_response_invalid")
    if not isinstance(response, dict):
        raise CaptureError("rpc_response_invalid")
    if set(response) == {"jsonrpc", "id", "result"}:
        if response["jsonrpc"] != "2.0" or response["id"] != 1:
            raise CaptureError("rpc_response_invalid")
        return response["result"]
    if set(response) == {"jsonrpc", "id", "error"}:
        rpc_error = response["error"]
        if (
            response["jsonrpc"] != "2.0"
            or response["id"] != 1
            or not isinstance(rpc_error, dict)
            or set(rpc_error) != {"code", "message"}
            or type(rpc_error["code"]) is not int
            or not isinstance(rpc_error["message"], str)
        ):
            raise CaptureError("rpc_response_invalid")
        raise _RemoteRpcError()
    raise CaptureError("rpc_response_invalid")


def _is_eip1898_selector(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"blockHash", "requireCanonical"}
        and isinstance(value["blockHash"], str)
        and _HASH32.fullmatch(value["blockHash"]) is not None
        and value["requireCanonical"] is True
    )


def _validate_rpc_request(method: str, params: list) -> None:
    if method == "eth_chainId" and params == []:
        return
    if method == "eth_getBlockByNumber" and params == ["finalized", False]:
        return
    if (
        method == "eth_getBlockByHash"
        and isinstance(params, list)
        and len(params) == 2
        and isinstance(params[0], str)
        and _HASH32.fullmatch(params[0]) is not None
        and params[1] is False
    ):
        return
    if (
        method == "eth_getCode"
        and isinstance(params, list)
        and len(params) == 2
        and isinstance(params[0], str)
        and _ADDRESS.fullmatch(params[0]) is not None
        and _is_eip1898_selector(params[1])
    ):
        return
    if (
        method == "eth_call"
        and isinstance(params, list)
        and len(params) == 2
        and isinstance(params[0], dict)
        and set(params[0]) == {"to", "data"}
        and isinstance(params[0]["to"], str)
        and _ADDRESS.fullmatch(params[0]["to"]) is not None
        and isinstance(params[0]["data"], str)
        and _HEX_DATA.fullmatch(params[0]["data"]) is not None
        and _is_eip1898_selector(params[1])
    ):
        return
    raise CaptureError("capture_configuration_invalid")


class BoundedJsonRpcTransport:
    """One redirect-rejecting, bounded JSON-RPC POST endpoint."""

    def __init__(self, endpoint: str, timeout_seconds: int = 20) -> None:
        if not isinstance(endpoint, str):
            raise CaptureError("capture_configuration_invalid")
        try:
            parsed = urlsplit(endpoint)
        except ValueError:
            raise CaptureError("capture_configuration_invalid")
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.fragment
            or type(timeout_seconds) is not int
            or not 1 <= timeout_seconds <= MAX_TIMEOUT_SECONDS
        ):
            raise CaptureError("capture_configuration_invalid")
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds
        self._opener = urllib_request.build_opener(_RejectRedirects())

    def __call__(self, method: str, params: list) -> object:
        _validate_rpc_request(method, params)
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        rpc_request = urllib_request.Request(
            self._endpoint,
            data=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self._opener.open(
                rpc_request, timeout=self._timeout_seconds
            ) as response:
                raw = response.read(MAX_RPC_RESPONSE_BYTES + 1)
        except CaptureError:
            raise
        except (OSError, urllib_error.URLError, urllib_error.HTTPError, ValueError):
            raise CaptureError("rpc_response_invalid")
        try:
            return _parse_rpc_response(raw)
        except _RemoteRpcError:
            if method in {"eth_call", "eth_getCode"}:
                raise CaptureError("eip1898_unavailable")
            if method in {"eth_getBlockByNumber", "eth_getBlockByHash"}:
                raise CaptureError("canonical_block_unavailable")
            raise CaptureError("rpc_response_invalid")


def _provider_result(
    provider: Provider, method: str, params: list, failure_reason: str
) -> object:
    try:
        return provider.rpc(method, params)
    except CaptureError:
        raise
    except Exception:
        raise CaptureError(failure_reason)


def _hex_quantity(value: object, reason: str) -> int:
    if not isinstance(value, str) or _HEX_QUANTITY.fullmatch(value) is None:
        raise CaptureError(reason)
    try:
        return int(value[2:], 16)
    except ValueError:
        raise CaptureError(reason)


def _hash32(value: object, reason: str, nonzero: bool = False) -> str:
    if not isinstance(value, str) or _HASH32.fullmatch(value) is None:
        raise CaptureError(reason)
    if nonzero and value == "0x" + "00" * 32:
        raise CaptureError(reason)
    return value


def _load_header(provider: Provider, method: str, params: list) -> dict:
    result = _provider_result(provider, method, params, "rpc_response_invalid")
    if result is None:
        raise CaptureError("canonical_block_unavailable")
    if not isinstance(result, dict):
        raise CaptureError("rpc_response_invalid")
    required = {
        "number", "hash", "parentHash", "timestamp", "stateRoot", "baseFeePerGas"
    }
    if not required.issubset(result):
        raise CaptureError("rpc_response_invalid")
    header = {
        "number": _hex_quantity(result["number"], "rpc_response_invalid"),
        "hash": _hash32(result["hash"], "canonical_block_unavailable", nonzero=True),
        "parent_hash": _hash32(result["parentHash"], "rpc_response_invalid"),
        "timestamp": _hex_quantity(result["timestamp"], "rpc_response_invalid"),
        "state_root": _hash32(result["stateRoot"], "rpc_response_invalid"),
        "base_fee_per_gas": _hex_quantity(
            result["baseFeePerGas"], "rpc_response_invalid"
        ),
    }
    if header["number"] <= 0 or header["timestamp"] <= 0:
        raise CaptureError("rpc_response_invalid")
    return header


def _load_finalized_header(provider: Provider) -> dict:
    return _load_header(provider, "eth_getBlockByNumber", ["finalized", False])


def _require_identical_headers(headers: Sequence[dict]) -> dict:
    if len(headers) != 2 or headers[0] != headers[1]:
        raise CaptureError("provider_disagreement")
    return dict(headers[0])


def _require_header_round_trip(provider: Provider, header: dict) -> None:
    reread = _load_header(
        provider, "eth_getBlockByHash", [header["hash"], False]
    )
    if reread != header:
        raise CaptureError("canonical_block_unavailable")


def _require_chain_ids(providers: Sequence[Provider], expected: int) -> None:
    chain_ids = [
        _hex_quantity(
            _provider_result(provider, "eth_chainId", [], "rpc_response_invalid"),
            "rpc_response_invalid",
        )
        for provider in providers
    ]
    if chain_ids != [expected, expected]:
        raise CaptureError("chain_id_mismatch")


def _bounded_result(value: object) -> str:
    if (
        not isinstance(value, str)
        or _HEX_DATA.fullmatch(value) is None
        or (len(value) - 2) // 2 > MAX_CALL_RESULT_BYTES
    ):
        raise CaptureError("rpc_response_invalid")
    if value == "0x":
        raise CaptureError("required_call_missing")
    return value


def _state_result(provider: Provider, method: str, params: list) -> str:
    for attempt in range(2):
        try:
            result = _provider_result(
                provider, method, params, "rpc_response_invalid"
            )
            return _bounded_result(result)
        except CaptureError as error:
            if attempt == 0 and error.reason_code == "rpc_response_invalid":
                continue
            raise
    raise CaptureError("rpc_response_invalid")


def _persisted_call(inventory_call: dict, result_hex: str) -> dict:
    raw = bytes.fromhex(result_hex[2:])
    return {
        "logical_call_id": inventory_call["logical_call_id"],
        "method": inventory_call["method"],
        "target": inventory_call["target"],
        "calldata": inventory_call["calldata"],
        "calldata_sha256": inventory_call["calldata_sha256"],
        "result_hex": result_hex,
        "result_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _collect_agreed_calls(
    providers: Sequence[Provider], header: dict, calls: Sequence[dict]
) -> Tuple[List[dict], List[dict]]:
    block_selector = {
        "blockHash": header["hash"],
        "requireCanonical": True,
    }
    logical = []
    observations = []
    for inventory_call in calls:
        if inventory_call["method"] == "eth_getCode":
            rpc_method = "eth_getCode"
            params = [inventory_call["target"], block_selector]
        else:
            rpc_method = "eth_call"
            params = [
                {
                    "to": inventory_call["target"],
                    "data": inventory_call["calldata"],
                },
                block_selector,
            ]
        results = [
            _state_result(provider, rpc_method, params) for provider in providers
        ]
        if results[0] != results[1]:
            raise CaptureError("provider_disagreement")
        record = _persisted_call(inventory_call, results[0])
        logical.append(record)
        for provider in providers:
            observations.append({
                "provider_label": provider.label,
                "logical_call_id": record["logical_call_id"],
                "block_hash": header["hash"],
                "result_sha256": record["result_sha256"],
                "status": "observed",
            })
    return logical, observations


def _find_call(
    calls: Sequence[dict], method: str, target: str, calldata: str = ""
) -> dict:
    matches = [
        call for call in calls
        if call["method"] == method
        and call["target"] == target
        and (not calldata or call["calldata"] == calldata)
    ]
    if len(matches) != 1:
        raise CaptureError("required_call_missing")
    return matches[0]


def _call_group_sha256(calls: Sequence[dict]) -> str:
    members = [
        {
            "logical_call_id": call["logical_call_id"],
            "result_sha256": call["result_sha256"],
        }
        for call in sorted(calls, key=lambda item: item["logical_call_id"])
    ]
    return hashlib.sha256(
        b"shib-v2-call-results/v1\n"
        + shib_v2_research.canonical_json_bytes(members)
    ).hexdigest()


def _decode(kind: str, call: dict, reason: str) -> object:
    try:
        return shib_v2_research.abi_decode_result(kind, call["result_hex"])
    except shib_v2_research.ResearchContractError:
        raise CaptureError(reason)


def _code_facts(call: dict, authority: dict, reason: str) -> Tuple[int, str]:
    code = bytes.fromhex(call["result_hex"][2:])
    size = len(code)
    digest = hashlib.sha256(code).hexdigest()
    if (
        size != authority["runtime_code_size_bytes"]
        or digest != authority["runtime_code_sha256"]
    ):
        raise CaptureError(reason)
    return size, digest


def _count_nulls(value: object) -> int:
    if value is None:
        return 1
    if isinstance(value, dict):
        return sum(_count_nulls(child) for child in value.values())
    if isinstance(value, list):
        return sum(_count_nulls(child) for child in value)
    return 0


def _count_numeric_zeroes(value: object) -> int:
    if type(value) is int:
        return int(value == 0)
    if isinstance(value, dict):
        return sum(_count_numeric_zeroes(child) for child in value.values())
    if isinstance(value, list):
        return sum(_count_numeric_zeroes(child) for child in value)
    return 0


def _collection_quality(candidate: dict, expected_count: int) -> dict:
    calls = candidate["logical_calls"]
    observations = candidate["provider_observations"]
    logical_ids = [call["logical_call_id"] for call in calls]
    provider_keys = [
        (observation["provider_label"], observation["logical_call_id"])
        for observation in observations
    ]
    observed_count = sum(
        observation["status"] == "observed" for observation in observations
    )
    observed_values = {
        key: candidate[key] for key in ("block", "tokens", "pools", "usd_reference")
    }
    return {
        "state": "evaluated",
        "expected_logical_call_count": expected_count,
        "observed_logical_call_count": len(calls),
        "usable_logical_call_count": sum(call["result_hex"] != "0x" for call in calls),
        "expected_provider_observation_count": expected_count * 2,
        "observed_provider_observation_count": len(observations),
        "usable_provider_observation_count": observed_count,
        "duplicate_logical_call_key_count": len(logical_ids) - len(set(logical_ids)),
        "duplicate_provider_observation_key_count": (
            len(provider_keys) - len(set(provider_keys))
        ),
        "required_field_null_count": _count_nulls(candidate),
        "measured_zero_count": _count_numeric_zeroes(observed_values),
        "missing_null_count": 0,
        "provider_agreement_count": len(calls),
        "provider_disagreement_count": 0,
        "status_counts": {"observed": observed_count},
    }


def _decode_evidence(
    registry: dict,
    header: dict,
    calls: Sequence[dict],
    observations: Sequence[dict],
) -> dict:
    header_sha256 = hashlib.sha256(
        shib_v2_research.canonical_json_bytes(header)
    ).hexdigest()
    try:
        timestamp_utc = datetime.fromtimestamp(
            header["timestamp"], timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError):
        raise CaptureError("canonical_block_unavailable")
    block = dict(
        header,
        timestamp_utc=timestamp_utc,
        canonical_header_sha256=header_sha256,
        provider_header_observations=[
            {
                "provider_label": label,
                "canonical_header_sha256": header_sha256,
                "status": "observed",
            }
            for label in ("provider_a", "provider_b")
        ],
    )

    tokens = []
    for symbol in ("SHIB", "WETH"):
        authority = registry["tokens"][symbol]
        address = authority["address"]
        code_call = _find_call(calls, "eth_getCode", address)
        decimals_call = _find_call(calls, "erc20.decimals", address)
        code_size, code_hash = _code_facts(
            code_call, authority, "pool_authority_mismatch"
        )
        decimals = _decode(
            "uint8", decimals_call, "pool_authority_mismatch"
        )
        if decimals != authority["decimals"]:
            raise CaptureError("pool_authority_mismatch")
        tokens.append({
            "symbol": symbol,
            "address": address,
            "decimals": decimals,
            "runtime_code_size_bytes": code_size,
            "runtime_code_sha256": code_hash,
            "call_results_sha256": _call_group_sha256((code_call, decimals_call)),
        })

    shib = registry["tokens"]["SHIB"]
    weth = registry["tokens"]["WETH"]
    pools = []
    for authority in registry["pools"]:
        factory = authority["factory"]["address"]
        router = authority["router"]["address"]
        pair = authority["pair"]["address"]
        code_calls = {
            role: _find_call(calls, "eth_getCode", authority[role]["address"])
            for role in ("factory", "router", "pair")
        }
        code_facts = {
            role: _code_facts(
                code_calls[role],
                authority[role],
                (
                    "router_authority_mismatch"
                    if role == "router"
                    else "pool_authority_mismatch"
                ),
            )
            for role in ("factory", "router", "pair")
        }
        get_pair = _find_call(
            calls,
            "factory.getPair",
            factory,
            shib_v2_research.abi_encode_call(
                "getPair(address,address)", (shib["address"], weth["address"])
            ),
        )
        router_factory = _find_call(calls, "router.factory", router)
        router_weth = _find_call(calls, "router.weth", router)
        pair_factory = _find_call(calls, "pair.factory", pair)
        token0_call = _find_call(calls, "pair.token0", pair)
        token1_call = _find_call(calls, "pair.token1", pair)
        reserves_call = _find_call(calls, "pair.getReserves", pair)
        decoded_addresses = (
            _decode("address", get_pair, "pool_authority_mismatch"),
            _decode("address", router_factory, "router_authority_mismatch"),
            _decode("address", router_weth, "router_authority_mismatch"),
            _decode("address", pair_factory, "pool_authority_mismatch"),
            _decode("address", token0_call, "pool_authority_mismatch"),
            _decode("address", token1_call, "pool_authority_mismatch"),
        )
        if decoded_addresses[1:3] != (factory, weth["address"]):
            raise CaptureError("router_authority_mismatch")
        if (
            decoded_addresses[0] != pair
            or decoded_addresses[3:] != (
                factory, shib["address"], weth["address"]
            )
        ):
            raise CaptureError("pool_authority_mismatch")
        reserve0, reserve1, reserve_timestamp = _decode(
            "uint112_tuple", reserves_call, "pool_authority_mismatch"
        )
        if reserve0 <= 0 or reserve1 <= 0 or reserve_timestamp > header["timestamp"]:
            raise CaptureError("pool_authority_mismatch")
        shib_decimals_call = _find_call(
            calls, "erc20.decimals", shib["address"]
        )
        weth_decimals_call = _find_call(
            calls, "erc20.decimals", weth["address"]
        )
        token0_decimals = _decode(
            "uint8", shib_decimals_call, "pool_authority_mismatch"
        )
        token1_decimals = _decode(
            "uint8", weth_decimals_call, "pool_authority_mismatch"
        )
        shib_balance_call = _find_call(
            calls,
            "erc20.balanceOf",
            shib["address"],
            shib_v2_research.abi_encode_call("balanceOf(address)", (pair,)),
        )
        weth_balance_call = _find_call(
            calls,
            "erc20.balanceOf",
            weth["address"],
            shib_v2_research.abi_encode_call("balanceOf(address)", (pair,)),
        )
        token0_balance = _decode(
            "uint256", shib_balance_call, "pool_authority_mismatch"
        )
        token1_balance = _decode(
            "uint256", weth_balance_call, "pool_authority_mismatch"
        )
        if token0_balance != reserve0 or token1_balance != reserve1:
            raise CaptureError("pool_authority_mismatch")

        pool_calls = [
            call for call in calls
            if call["target"] in {factory, router, pair}
            or call["method"] == "erc20.decimals"
            or (
                call["method"] == "erc20.balanceOf"
                and call["calldata"].endswith(pair[2:])
            )
        ]
        fee_model = authority["fee_model"]
        if authority["dex"] == "uniswap_v2":
            fee_parameters = {"kind": "runtime_code_bound"}
            fee_calls = [code_calls["pair"]]
        else:
            total_fee_call = _find_call(calls, "fee.totalFee", pair)
            alpha_call = _find_call(calls, "fee.alpha", pair)
            beta_call = _find_call(calls, "fee.beta", pair)
            total_fee = _decode(
                "uint256", total_fee_call, "fee_authority_mismatch"
            )
            alpha = _decode("uint256", alpha_call, "fee_authority_mismatch")
            beta = _decode("uint256", beta_call, "fee_authority_mismatch")
            native = fee_model["evidence"]
            if (
                total_fee != native["total_fee"]
                or alpha != native["alpha"]
                or beta != native["beta"]
                or fee_model["fee_numerator"]
                != native["native_fee_denominator"] - total_fee
            ):
                raise CaptureError("fee_authority_mismatch")
            fee_parameters = {
                "kind": "pair_native_parameters",
                "native_fee_denominator": native["native_fee_denominator"],
                "total_fee": total_fee,
                "alpha": alpha,
                "beta": beta,
            }
            fee_calls = [total_fee_call, alpha_call, beta_call]
        pools.append({
            "dex": authority["dex"],
            "factory_address": factory,
            "router_address": router,
            "pair_address": pair,
            "factory_runtime_code_size_bytes": code_facts["factory"][0],
            "factory_runtime_code_sha256": code_facts["factory"][1],
            "router_runtime_code_size_bytes": code_facts["router"][0],
            "router_runtime_code_sha256": code_facts["router"][1],
            "pair_runtime_code_size_bytes": code_facts["pair"][0],
            "pair_runtime_code_sha256": code_facts["pair"][1],
            "factory_get_pair_result": decoded_addresses[0],
            "router_factory_result": decoded_addresses[1],
            "router_weth_result": decoded_addresses[2],
            "pair_factory_result": decoded_addresses[3],
            "token0_address": decoded_addresses[4],
            "token1_address": decoded_addresses[5],
            "token0_decimals": token0_decimals,
            "token1_decimals": token1_decimals,
            "reserve0_raw": reserve0,
            "reserve1_raw": reserve1,
            "reserve_timestamp_last_raw": reserve_timestamp,
            "token0_balance_raw": token0_balance,
            "token1_balance_raw": token1_balance,
            "reserve_lag_seconds": header["timestamp"] - reserve_timestamp,
            "fee_bps": fee_model["fee_bps"],
            "fee_numerator": fee_model["fee_numerator"],
            "fee_denominator": fee_model["fee_denominator"],
            "fee_formula": fee_model["formula"],
            "fee_parameters": fee_parameters,
            "fee_evidence_sha256": _call_group_sha256(fee_calls),
            "call_results_sha256": _call_group_sha256(pool_calls),
        })

    reference_authority = registry["usd_reference"]
    proxy = reference_authority["proxy_address"]
    code_call = _find_call(calls, "eth_getCode", proxy)
    decimals_call = _find_call(calls, "feed.decimals", proxy)
    description_call = _find_call(calls, "feed.description", proxy)
    round_call = _find_call(calls, "feed.latestRoundData", proxy)
    code_size, code_hash = _code_facts(
        code_call, reference_authority, "usd_reference_unavailable"
    )
    decimals = _decode("uint8", decimals_call, "usd_reference_unavailable")
    description = _decode("string", description_call, "usd_reference_unavailable")
    round_id, answer, started_at, updated_at, answered_in_round = _decode(
        "chainlink_round", round_call, "usd_reference_unavailable"
    )
    if (
        decimals != reference_authority["decimals"]
        or description != reference_authority["description"]
        or answer <= 0
        or started_at <= 0
        or started_at > updated_at
        or updated_at > header["timestamp"]
        or header["timestamp"] - updated_at > reference_authority["max_age_seconds"]
        or answered_in_round < round_id
    ):
        raise CaptureError("usd_reference_unavailable")
    usd_reference = {
        "kind": reference_authority["kind"],
        "proxy_address": proxy,
        "proxy_runtime_code_size_bytes": code_size,
        "proxy_runtime_code_sha256": code_hash,
        "description": description,
        "decimals": decimals,
        "round_id": round_id,
        "answer": answer,
        "started_at": started_at,
        "updated_at": updated_at,
        "answered_in_round": answered_in_round,
        "freshness_lag_seconds": header["timestamp"] - updated_at,
        "call_results_sha256": _call_group_sha256(
            (code_call, decimals_call, description_call, round_call)
        ),
    }
    candidate = {
        "schema": shib_v2_research.EVIDENCE_SCHEMA,
        "registry_sha256": shib_v2_research.registry_sha256(registry),
        "chain": registry["chain"],
        "block": block,
        "logical_calls": list(calls),
        "provider_observations": list(observations),
        "tokens": tokens,
        "usd_reference": usd_reference,
        "pools": pools,
    }
    candidate["collection_quality"] = _collection_quality(candidate, len(calls))
    return candidate


def _validated_registry(registry: object) -> dict:
    try:
        return shib_v2_research.load_research_registry(registry)
    except shib_v2_research.ResearchContractError:
        raise CaptureError("registry_invalid")


def _map_validation_error(
    error: shib_v2_research.ResearchContractError,
) -> CaptureError:
    message = str(error).lower()
    if "registry" in message:
        return CaptureError("registry_invalid")
    if "router" in message:
        return CaptureError("router_authority_mismatch")
    if "fee" in message:
        return CaptureError("fee_authority_mismatch")
    if "usd" in message or "chainlink" in message or "reference" in message:
        return CaptureError("usd_reference_unavailable")
    if "pool" in message or "token" in message or "runtime code" in message:
        return CaptureError("pool_authority_mismatch")
    if "block" in message or "header" in message:
        return CaptureError("canonical_block_unavailable")
    return CaptureError("rpc_response_invalid")


def _validate_provider_configuration(providers: Sequence[Provider]) -> None:
    if not isinstance(providers, (list, tuple)) or len(providers) != 2:
        raise CaptureError("capture_configuration_invalid")
    if [provider.label for provider in providers] != ["provider_a", "provider_b"]:
        raise CaptureError("capture_configuration_invalid")
    if any(
        not isinstance(provider.endpoint_identity, str)
        or _SHA256.fullmatch(provider.endpoint_identity) is None
        or not callable(provider.rpc)
        for provider in providers
    ):
        raise CaptureError("capture_configuration_invalid")
    if len({provider.endpoint_identity for provider in providers}) != 2:
        raise CaptureError("capture_configuration_invalid")


def capture_research_evidence(
    registry: dict, providers: Sequence[Provider], output_path: Path
) -> dict:
    """Capture, validate, then atomically publish one complete generation."""
    _validate_provider_configuration(providers)
    registry = _validated_registry(registry)
    _require_chain_ids(providers, expected=registry["chain"]["chain_id"])
    headers = [_load_finalized_header(provider) for provider in providers]
    header = _require_identical_headers(headers)
    for provider in providers:
        _require_header_round_trip(provider, header)
    try:
        inventory = shib_v2_research.build_logical_call_inventory(registry)
    except shib_v2_research.ResearchContractError as error:
        raise _map_validation_error(error)
    logical, observations = _collect_agreed_calls(
        providers, header, inventory
    )
    for provider in providers:
        _require_header_round_trip(provider, header)
    candidate = _decode_evidence(
        registry, header, logical, observations
    )
    candidate["evidence_identity"] = shib_v2_research.evidence_identity(candidate)
    try:
        validated = shib_v2_research.validate_research_evidence(
            candidate, registry
        )
    except shib_v2_research.ResearchContractError as error:
        raise _map_validation_error(error)
    try:
        atomic_write_canonical_json(output_path, validated)
    except shib_v2_research.ResearchContractError:
        raise CaptureError("unsafe_output_path")
    return validated


def _endpoint_identity(endpoint: str) -> str:
    return hashlib.sha256(
        b"shib-v2-rpc-endpoint/v1\n" + endpoint.encode("utf-8")
    ).hexdigest()


class _CaptureArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise CaptureError("capture_configuration_invalid")


def _argument_parser() -> argparse.ArgumentParser:
    parser = _CaptureArgumentParser(
        description="Capture fixed-block dual-provider SHIB V2 research evidence."
    )
    parser.add_argument("--registry", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rpc-url-a", required=True)
    parser.add_argument("--rpc-url-b", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    return parser


def main(argv: Sequence[str] = None) -> int:
    try:
        args = _argument_parser().parse_args(argv)
    except CaptureError as error:
        sys.stderr.write(sanitize_capture_failure(error) + "\n")
        return 1
    try:
        if args.rpc_url_a == args.rpc_url_b:
            raise CaptureError("capture_configuration_invalid")
        identities = (
            _endpoint_identity(args.rpc_url_a),
            _endpoint_identity(args.rpc_url_b),
        )
        if identities[0] == identities[1]:
            raise CaptureError("capture_configuration_invalid")
        try:
            registry_payload = load_bounded_json(
                Path(args.registry), "research registry"
            )
        except shib_v2_research.ResearchContractError:
            raise CaptureError("registry_invalid")
        providers = [
            Provider(
                "provider_a",
                identities[0],
                BoundedJsonRpcTransport(args.rpc_url_a, args.timeout_seconds),
            ),
            Provider(
                "provider_b",
                identities[1],
                BoundedJsonRpcTransport(args.rpc_url_b, args.timeout_seconds),
            ),
        ]
        capture_research_evidence(
            registry_payload, providers, Path(args.output)
        )
    except BaseException as error:
        sys.stderr.write(sanitize_capture_failure(error) + "\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
