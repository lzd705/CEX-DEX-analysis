"""Contracts for the bounded SHIB V2/V2 research registry."""

from __future__ import annotations

import ast
import copy
from decimal import Decimal
from fractions import Fraction
import hashlib
import inspect
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from unittest import mock

from scripts import shib_v2_research
from scripts import shib_v2_research_io
from scripts import capture_shib_v2_research_evidence as capture
from scripts.capture_shib_v2_research_evidence import Provider
from scripts.shib_v2_research_io import load_bounded_json
from scripts.route_quantity import (
    CommonTarget,
    MarketRules,
    V2PoolState,
    quote_v2_pool_quantity,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = PROJECT_ROOT / "config/shib_v2_research_pools.json"
EVIDENCE_PATH = (
    PROJECT_ROOT / "data/public/research/shib-v2v2/evidence.json"
)
SNAPSHOT_PATH = (
    PROJECT_ROOT / "data/public/research/shib-v2v2/latest.json"
)
BUILD_SCRIPT = PROJECT_ROOT / "scripts/build_shib_v2_research_snapshot.py"


def valid_registry_payload():
    return {
        "schema": "shib_v2_research_registry/v1",
        "chain": {"name": "eth", "chain_id": 1},
        "tokens": {
            "SHIB": {
                "address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
                "decimals": 18,
                "runtime_code_size_bytes": 4852,
                "runtime_code_sha256": "5c813da8be193a1a33a7533edc758e3ad29f1fa1730cbf2d8c9fc8a7f31c78f3",
            },
            "WETH": {
                "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "decimals": 18,
                "runtime_code_size_bytes": 3124,
                "runtime_code_sha256": "5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739",
            },
        },
        "pools": [
            {
                "dex": "uniswap_v2",
                "factory": {
                    "address": "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
                    "runtime_code_size_bytes": 13859,
                    "runtime_code_sha256": "3abc53f12a9cb8ae37ebfada9efc261c1ab4c2759d161e341a49bf67df3f8321",
                },
                "router": {
                    "address": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
                    "runtime_code_size_bytes": 21943,
                    "runtime_code_sha256": "ccef50da4af021b09ada39d78db5d281fffff81a57969c7028bccc1f50d37854",
                },
                "pair": {
                    "address": "0x811beed0119b4afce20d2583eb608c6f7af1954f",
                    "runtime_code_size_bytes": 11293,
                    "runtime_code_sha256": "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4",
                },
                "token0": "SHIB",
                "token1": "WETH",
                "fee_model": {
                    "formula": "amount_in_with_fee=amount_in*fee_numerator;denominator=reserve_in*fee_denominator+amount_in_with_fee",
                    "fee_bps": 30,
                    "fee_numerator": 997,
                    "fee_denominator": 1000,
                    "evidence": {"kind": "runtime_code_bound"},
                },
            },
            {
                "dex": "shibaswap_v1",
                "factory": {
                    "address": "0x115934131916c8b277dd010ee02de363c09d037c",
                    "runtime_code_size_bytes": 15527,
                    "runtime_code_sha256": "bccd00fecc8d072c7635ef40bd5b7721057975123aa8639d62a37f90f6a45b53",
                },
                "router": {
                    "address": "0x03f7724180aa6b939894b5ca4314783b0b36b329",
                    "runtime_code_size_bytes": 18469,
                    "runtime_code_sha256": "bb5f84ee54eacd3a273b2a3942ad904f8194a999f32394682cda2080b14b0423",
                },
                "pair": {
                    "address": "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
                    "runtime_code_size_bytes": 10654,
                    "runtime_code_sha256": "83589060885cd6b139ce4b4ed723653d124a00b50c0fa203dbd5a425cb272bc7",
                },
                "token0": "SHIB",
                "token1": "WETH",
                "fee_model": {
                    "formula": "amount_in_with_fee=amount_in*fee_numerator;denominator=reserve_in*fee_denominator+amount_in_with_fee",
                    "fee_bps": 30,
                    "fee_numerator": 997,
                    "fee_denominator": 1000,
                    "evidence": {
                        "kind": "pair_native_parameters",
                        "target": "pair",
                        "native_fee_denominator": 1000,
                        "total_fee": 3,
                        "alpha": 1,
                        "beta": 3,
                    },
                },
            },
        ],
        "usd_reference": {
            "kind": "chainlink_aggregator_v3",
            "proxy_address": "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419",
            "runtime_code_size_bytes": 9571,
            "runtime_code_sha256": "ed698309290de3517c7201fcad9a9dbd4b8cde4a72c9add23129201f299c6f2b",
            "description": "ETH / USD",
            "decimals": 8,
            "max_age_seconds": 3600,
        },
        "requested_notionals_usd": ["1000", "5000", "10000", "50000", "100000"],
    }


def fixture_registry_and_code_results():
    registry = valid_registry_payload()
    authorities = [
        registry["pools"][0]["factory"],
        registry["pools"][0]["router"],
        registry["pools"][0]["pair"],
        registry["pools"][1]["factory"],
        registry["pools"][1]["router"],
        registry["pools"][1]["pair"],
        registry["tokens"]["SHIB"],
        registry["tokens"]["WETH"],
        registry["usd_reference"],
    ]
    code_results = {}
    for index, authority in enumerate(authorities):
        code = bytes([index + 1]) * 32
        address = authority.get("address", authority.get("proxy_address"))
        authority["runtime_code_size_bytes"] = len(code)
        authority["runtime_code_sha256"] = hashlib.sha256(code).hexdigest()
        code_results[address] = "0x" + code.hex()
    trust_anchor = (
        tuple(
            (
                symbol,
                registry["tokens"][symbol]["address"],
                registry["tokens"][symbol]["decimals"],
                registry["tokens"][symbol]["runtime_code_size_bytes"],
                registry["tokens"][symbol]["runtime_code_sha256"],
            )
            for symbol in ("SHIB", "WETH")
        ),
        tuple(
            (
                pool["dex"],
                tuple(
                    (
                        pool[role]["address"],
                        pool[role]["runtime_code_size_bytes"],
                        pool[role]["runtime_code_sha256"],
                    )
                    for role in ("factory", "router", "pair")
                ),
                tuple(sorted(pool["fee_model"]["evidence"].items())),
            )
            for pool in registry["pools"]
        ),
        (
            registry["usd_reference"]["proxy_address"],
            registry["usd_reference"]["runtime_code_size_bytes"],
            registry["usd_reference"]["runtime_code_sha256"],
            registry["usd_reference"]["decimals"],
            registry["usd_reference"]["max_age_seconds"],
        ),
    )
    return registry, code_results, trust_anchor


def add_unknown_field(payload):
    payload["unknown"] = "forbidden"
    return payload


def uppercase_shib(payload):
    payload["tokens"]["SHIB"]["address"] = (
        "0x95AD61B0A150D79219DCF64E1E6CC01F0B64C4CE"
    )
    return payload


def duplicate_first_pool(payload):
    payload["pools"].append(copy.deepcopy(payload["pools"][0]))
    return payload


def _abi_word(value):
    if value < 0:
        value += 1 << 256
    return value.to_bytes(32, "big")


def _abi_address(address):
    return b"\x00" * 12 + bytes.fromhex(address[2:])


def _abi_string(value):
    encoded = value.encode("utf-8")
    padding = (-len(encoded)) % 32
    return _abi_word(32) + _abi_word(len(encoded)) + encoded + b"\x00" * padding


def _result_hex(value):
    return "0x" + value.hex()


def _call_group_sha256(calls):
    members = [
        {
            "logical_call_id": call["logical_call_id"],
            "result_sha256": call["result_sha256"],
        }
        for call in sorted(calls, key=lambda item: item["logical_call_id"])
    ]
    return hashlib.sha256(
        b"shib-v2-call-results/v1\n"
        + json.dumps(
            members,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


_REVIEWED_SELECTORS = {
    "getPair(address,address)": "e6a43905",
    "factory()": "c45a0155",
    "WETH()": "ad5c4648",
    "token0()": "0dfe1681",
    "token1()": "d21220a7",
    "getReserves()": "0902f1ac",
    "decimals()": "313ce567",
    "balanceOf(address)": "70a08231",
    "totalFee()": "1df4ccfc",
    "alpha()": "db1d0fd5",
    "beta()": "9faa3c91",
    "description()": "7284e416",
    "latestRoundData()": "feaf968c",
}


def _test_canonical_json_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _test_abi_call(signature, arguments=()):
    encoded = bytearray.fromhex(_REVIEWED_SELECTORS[signature])
    for address in arguments:
        encoded.extend(_abi_address(address))
    return "0x" + bytes(encoded).hex()


def _test_inventory_call(method, target, calldata):
    identity = {
        "method": method,
        "target": target,
        "calldata_sha256": hashlib.sha256(
            bytes.fromhex(calldata[2:])
        ).hexdigest(),
        "block_selector": "eip1898_block_hash_require_canonical",
    }
    record = dict(identity)
    record["logical_call_id"] = "call:" + hashlib.sha256(
        b"shib-v2-logical-call/v1\n" + _test_canonical_json_bytes(identity)
    ).hexdigest()
    record["calldata"] = calldata
    return record


def _expected_logical_call_inventory(registry):
    calls = []
    code_targets = []
    for pool in registry["pools"]:
        code_targets.extend(
            pool[role]["address"] for role in ("factory", "router", "pair")
        )
    code_targets.extend(
        registry["tokens"][symbol]["address"] for symbol in ("SHIB", "WETH")
    )
    code_targets.append(registry["usd_reference"]["proxy_address"])
    for target in code_targets:
        calls.append(_test_inventory_call("eth_getCode", target, "0x"))
    shib = registry["tokens"]["SHIB"]["address"]
    weth = registry["tokens"]["WETH"]["address"]
    for pool in registry["pools"]:
        factory = pool["factory"]["address"]
        router = pool["router"]["address"]
        pair = pool["pair"]["address"]
        calls.extend((
            _test_inventory_call(
                "factory.getPair",
                factory,
                _test_abi_call("getPair(address,address)", (shib, weth)),
            ),
            _test_inventory_call("router.factory", router, _test_abi_call("factory()")),
            _test_inventory_call("router.weth", router, _test_abi_call("WETH()")),
            _test_inventory_call("pair.factory", pair, _test_abi_call("factory()")),
            _test_inventory_call("pair.token0", pair, _test_abi_call("token0()")),
            _test_inventory_call("pair.token1", pair, _test_abi_call("token1()")),
            _test_inventory_call(
                "pair.getReserves", pair, _test_abi_call("getReserves()")
            ),
            _test_inventory_call(
                "erc20.balanceOf", shib, _test_abi_call("balanceOf(address)", (pair,))
            ),
            _test_inventory_call(
                "erc20.balanceOf", weth, _test_abi_call("balanceOf(address)", (pair,))
            ),
        ))
    for token in (shib, weth):
        calls.append(_test_inventory_call(
            "erc20.decimals", token, _test_abi_call("decimals()")
        ))
    shiba_pair = registry["pools"][1]["pair"]["address"]
    calls.extend((
        _test_inventory_call("fee.totalFee", shiba_pair, _test_abi_call("totalFee()")),
        _test_inventory_call("fee.alpha", shiba_pair, _test_abi_call("alpha()")),
        _test_inventory_call("fee.beta", shiba_pair, _test_abi_call("beta()")),
    ))
    feed = registry["usd_reference"]["proxy_address"]
    calls.extend((
        _test_inventory_call("feed.decimals", feed, _test_abi_call("decimals()")),
        _test_inventory_call(
            "feed.description", feed, _test_abi_call("description()")
        ),
        _test_inventory_call(
            "feed.latestRoundData", feed, _test_abi_call("latestRoundData()")
        ),
    ))
    return sorted(calls, key=lambda call: call["logical_call_id"])


def _test_count_nulls(value):
    if value is None:
        return 1
    if isinstance(value, dict):
        return sum(_test_count_nulls(child) for child in value.values())
    if isinstance(value, list):
        return sum(_test_count_nulls(child) for child in value)
    return 0


def _test_count_numeric_zeroes(value):
    if type(value) is int:
        return int(value == 0)
    if isinstance(value, dict):
        return sum(_test_count_numeric_zeroes(child) for child in value.values())
    if isinstance(value, list):
        return sum(_test_count_numeric_zeroes(child) for child in value)
    return 0


def _derive_test_quality(evidence, expected_logical_call_count):
    logical_calls = evidence["logical_calls"]
    observations = evidence["provider_observations"]
    logical_ids = [call["logical_call_id"] for call in logical_calls]
    provider_keys = [
        (observation["provider_label"], observation["logical_call_id"])
        for observation in observations
    ]
    calls_by_id = {call["logical_call_id"]: call for call in logical_calls}
    agreement_count = 0
    disagreement_count = 0
    for logical_call_id in set(logical_ids):
        hashes = [
            observation["result_sha256"]
            for observation in observations
            if observation["logical_call_id"] == logical_call_id
            and observation["status"] == "observed"
        ]
        expected_hash = calls_by_id[logical_call_id]["result_sha256"]
        if len(hashes) == 2 and len(set(hashes)) == 1 and hashes[0] == expected_hash:
            agreement_count += 1
        elif hashes:
            disagreement_count += 1
    required = {
        key: value for key, value in evidence.items()
        if key not in {"collection_quality", "evidence_identity"}
    }
    observed_values = {
        key: evidence[key] for key in ("block", "tokens", "pools", "usd_reference")
    }
    return {
        "state": "evaluated",
        "expected_logical_call_count": expected_logical_call_count,
        "observed_logical_call_count": len(logical_calls),
        "usable_logical_call_count": sum(
            call["result_hex"] != "0x" for call in logical_calls
        ),
        "expected_provider_observation_count": expected_logical_call_count * 2,
        "observed_provider_observation_count": len(observations),
        "usable_provider_observation_count": sum(
            observation["status"] == "observed" for observation in observations
        ),
        "duplicate_logical_call_key_count": len(logical_ids) - len(set(logical_ids)),
        "duplicate_provider_observation_key_count": (
            len(provider_keys) - len(set(provider_keys))
        ),
        "required_field_null_count": _test_count_nulls(required),
        "measured_zero_count": _test_count_numeric_zeroes(observed_values),
        "missing_null_count": 0,
        "provider_agreement_count": agreement_count,
        "provider_disagreement_count": disagreement_count,
        "status_counts": {
            "observed": sum(
                observation["status"] == "observed" for observation in observations
            )
        },
    }


def _fixture_code_results(registry):
    authorities = [
        registry["pools"][0]["factory"],
        registry["pools"][0]["router"],
        registry["pools"][0]["pair"],
        registry["pools"][1]["factory"],
        registry["pools"][1]["router"],
        registry["pools"][1]["pair"],
        registry["tokens"]["SHIB"],
        registry["tokens"]["WETH"],
        registry["usd_reference"],
    ]
    return {
        authority.get("address", authority.get("proxy_address")): (
            "0x" + (bytes([index + 1]) * 32).hex()
        )
        for index, authority in enumerate(authorities)
    }


def _fixture_call_result(call, registry):
    pools_by_factory = {
        pool["factory"]["address"]: pool for pool in registry["pools"]
    }
    pools_by_router = {
        pool["router"]["address"]: pool for pool in registry["pools"]
    }
    pools_by_pair = {pool["pair"]["address"]: pool for pool in registry["pools"]}
    token_by_address = {
        token["address"]: token for token in registry["tokens"].values()
    }
    method = call["method"]
    target = call["target"]
    if method == "eth_getCode":
        return _fixture_code_results(registry)[target]
    if method == "factory.getPair":
        return _result_hex(_abi_address(pools_by_factory[target]["pair"]["address"]))
    if method == "router.factory":
        return _result_hex(_abi_address(pools_by_router[target]["factory"]["address"]))
    if method == "router.weth":
        return _result_hex(_abi_address(registry["tokens"]["WETH"]["address"]))
    if method == "pair.factory":
        return _result_hex(_abi_address(pools_by_pair[target]["factory"]["address"]))
    if method == "pair.token0":
        return _result_hex(_abi_address(registry["tokens"]["SHIB"]["address"]))
    if method == "pair.token1":
        return _result_hex(_abi_address(registry["tokens"]["WETH"]["address"]))
    if method == "pair.getReserves":
        return _result_hex(_abi_word(10 ** 30) + _abi_word(10 ** 24) + _abi_word(1709999990))
    if method == "erc20.decimals":
        return _result_hex(_abi_word(token_by_address[target]["decimals"]))
    if method == "erc20.balanceOf":
        pair_address = "0x" + call["calldata"][-40:]
        if pair_address not in pools_by_pair:
            raise AssertionError("unknown fixture pair balance target")
        value = 10 ** 30 if target == registry["tokens"]["SHIB"]["address"] else 10 ** 24
        return _result_hex(_abi_word(value))
    if method == "fee.totalFee":
        return _result_hex(_abi_word(3))
    if method == "fee.alpha":
        return _result_hex(_abi_word(1))
    if method == "fee.beta":
        return _result_hex(_abi_word(3))
    if method == "feed.decimals":
        return _result_hex(_abi_word(8))
    if method == "feed.description":
        return _result_hex(_abi_string("ETH / USD"))
    if method == "feed.latestRoundData":
        return _result_hex(
            _abi_word(1)
            + _abi_word(200000000000)
            + _abi_word(1709999990)
            + _abi_word(1709999990)
            + _abi_word(1)
        )
    raise AssertionError("unknown fixture method: {}".format(method))


def valid_evidence_payload(registry):
    inventory = _expected_logical_call_inventory(registry)
    logical_calls = []
    for inventory_call in inventory:
        result_hex = _fixture_call_result(inventory_call, registry)
        logical_calls.append({
            "logical_call_id": inventory_call["logical_call_id"],
            "method": inventory_call["method"],
            "target": inventory_call["target"],
            "calldata": inventory_call["calldata"],
            "calldata_sha256": inventory_call["calldata_sha256"],
            "result_hex": result_hex,
            "result_sha256": hashlib.sha256(
                bytes.fromhex(result_hex[2:])
            ).hexdigest(),
        })
    logical_calls.sort(key=lambda item: item["logical_call_id"])
    by_method_target = {
        (call["method"], call["target"]): call for call in logical_calls
    }
    block_hash = "0x" + "11" * 32
    header = {
        "number": 20000000,
        "hash": block_hash,
        "parent_hash": "0x" + "22" * 32,
        "timestamp": 1710000000,
        "state_root": "0x" + "33" * 32,
        "base_fee_per_gas": 0,
    }
    header_sha256 = hashlib.sha256(
        json.dumps(
            header,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    block = dict(
        header,
        timestamp_utc="2024-03-09T16:00:00Z",
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
    provider_observations = [
        {
            "provider_label": label,
            "logical_call_id": call["logical_call_id"],
            "block_hash": block_hash,
            "result_sha256": call["result_sha256"],
            "status": "observed",
        }
        for call in logical_calls
        for label in ("provider_a", "provider_b")
    ]
    tokens = []
    for symbol in ("SHIB", "WETH"):
        authority = registry["tokens"][symbol]
        calls = [
            by_method_target[("eth_getCode", authority["address"])],
            by_method_target[("erc20.decimals", authority["address"])],
        ]
        tokens.append({
            "symbol": symbol,
            "address": authority["address"],
            "decimals": authority["decimals"],
            "runtime_code_size_bytes": authority["runtime_code_size_bytes"],
            "runtime_code_sha256": authority["runtime_code_sha256"],
            "call_results_sha256": _call_group_sha256(calls),
        })
    pools = []
    for pool in registry["pools"]:
        pair_address = pool["pair"]["address"]
        pool_calls = [
            call for call in logical_calls
            if call["target"] in {
                pool["factory"]["address"],
                pool["router"]["address"],
                pair_address,
            }
            or call["method"] == "erc20.decimals"
            or (
                call["method"] == "erc20.balanceOf"
                and call["calldata"].endswith(pair_address[2:])
            )
        ]
        fee_calls = [
            by_method_target[("eth_getCode", pair_address)]
        ] if pool["dex"] == "uniswap_v2" else [
            by_method_target[(method, pair_address)]
            for method in ("fee.totalFee", "fee.alpha", "fee.beta")
        ]
        if pool["dex"] == "uniswap_v2":
            fee_parameters = {"kind": "runtime_code_bound"}
        else:
            fee_parameters = {
                "kind": "pair_native_parameters",
                "native_fee_denominator": 1000,
                "total_fee": 3,
                "alpha": 1,
                "beta": 3,
            }
        pools.append({
            "dex": pool["dex"],
            "factory_address": pool["factory"]["address"],
            "router_address": pool["router"]["address"],
            "pair_address": pair_address,
            "factory_runtime_code_size_bytes": pool["factory"]["runtime_code_size_bytes"],
            "factory_runtime_code_sha256": pool["factory"]["runtime_code_sha256"],
            "router_runtime_code_size_bytes": pool["router"]["runtime_code_size_bytes"],
            "router_runtime_code_sha256": pool["router"]["runtime_code_sha256"],
            "pair_runtime_code_size_bytes": pool["pair"]["runtime_code_size_bytes"],
            "pair_runtime_code_sha256": pool["pair"]["runtime_code_sha256"],
            "factory_get_pair_result": pair_address,
            "router_factory_result": pool["factory"]["address"],
            "router_weth_result": registry["tokens"]["WETH"]["address"],
            "pair_factory_result": pool["factory"]["address"],
            "token0_address": registry["tokens"]["SHIB"]["address"],
            "token1_address": registry["tokens"]["WETH"]["address"],
            "token0_decimals": 18,
            "token1_decimals": 18,
            "reserve0_raw": 10 ** 30,
            "reserve1_raw": 10 ** 24,
            "reserve_timestamp_last_raw": 1709999990,
            "token0_balance_raw": 10 ** 30,
            "token1_balance_raw": 10 ** 24,
            "reserve_lag_seconds": 10,
            "fee_bps": 30,
            "fee_numerator": 997,
            "fee_denominator": 1000,
            "fee_formula": pool["fee_model"]["formula"],
            "fee_parameters": fee_parameters,
            "fee_evidence_sha256": _call_group_sha256(fee_calls),
            "call_results_sha256": _call_group_sha256(pool_calls),
        })
    reference = registry["usd_reference"]
    reference_calls = [
        by_method_target[(method, reference["proxy_address"])]
        for method in (
            "eth_getCode", "feed.decimals", "feed.description",
            "feed.latestRoundData",
        )
    ]
    usd_reference = {
        "kind": "chainlink_aggregator_v3",
        "proxy_address": reference["proxy_address"],
        "proxy_runtime_code_size_bytes": reference["runtime_code_size_bytes"],
        "proxy_runtime_code_sha256": reference["runtime_code_sha256"],
        "description": "ETH / USD",
        "decimals": 8,
        "round_id": 1,
        "answer": 200000000000,
        "started_at": 1709999990,
        "updated_at": 1709999990,
        "answered_in_round": 1,
        "freshness_lag_seconds": 10,
        "call_results_sha256": _call_group_sha256(reference_calls),
    }
    evidence = {
        "schema": "shib_v2_research_evidence/v1",
        "registry_sha256": hashlib.sha256(
            _test_canonical_json_bytes(registry)
        ).hexdigest(),
        "chain": {"name": "eth", "chain_id": 1},
        "block": block,
        "logical_calls": logical_calls,
        "provider_observations": provider_observations,
        "tokens": tokens,
        "usd_reference": usd_reference,
        "pools": pools,
    }
    evidence["collection_quality"] = _derive_test_quality(evidence, len(inventory))
    evidence["evidence_identity"] = _test_evidence_identity(evidence)
    return evidence


def valid_rpc_responses(registry):
    evidence = valid_evidence_payload(registry)
    block = evidence["block"]
    header = {
        "baseFeePerGas": hex(block["base_fee_per_gas"]),
        "blobGasUsed": "0x0",
        "difficulty": "0x0",
        "excessBlobGas": "0x0",
        "extraData": "0x",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x5208",
        "hash": block["hash"],
        "logsBloom": "0x" + "00" * 256,
        "miner": "0x" + "77" * 20,
        "mixHash": "0x" + "88" * 32,
        "nonce": "0x0000000000000000",
        "number": hex(block["number"]),
        "parentHash": block["parent_hash"],
        "parentBeaconBlockRoot": "0x" + "99" * 32,
        "receiptsRoot": "0x" + "aa" * 32,
        "requestsHash": "0x" + "ab" * 32,
        "sha3Uncles": "0x" + "bb" * 32,
        "size": "0x100",
        "timestamp": hex(block["timestamp"]),
        "totalDifficulty": "0x0",
        "transactions": [],
        "transactionsRoot": "0x" + "cc" * 32,
        "stateRoot": block["state_root"],
        "uncles": [],
        "withdrawals": [],
        "withdrawalsRoot": "0x" + "dd" * 32,
    }
    results = {
        (call["target"], call["calldata"]): call["result_hex"]
        for call in evidence["logical_calls"]
    }
    responses = {"chain_id": "0x1", "header": header, "results": results}
    return copy.deepcopy(responses), copy.deepcopy(responses)


def expected_scenario_keys():
    routes = (
        "shib-v2v2:eth:uniswap_v2:"
        "0x811beed0119b4afce20d2583eb608c6f7af1954f:to:shibaswap_v1:"
        "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
        "shib-v2v2:eth:shibaswap_v1:"
        "0xcf6daab95c476106eca715d48de4b13287ffdeaa:to:uniswap_v2:"
        "0x811beed0119b4afce20d2583eb608c6f7af1954f",
    )
    notionals = ("1000", "5000", "10000", "50000", "100000")
    return [
        (route, notional)
        for route in routes
        for notional in notionals
    ]


def _set_fixture_pool_reserves(evidence, reserve_pairs):
    shib_address = next(
        token["address"] for token in evidence["tokens"]
        if token["symbol"] == "SHIB"
    )
    weth_address = next(
        token["address"] for token in evidence["tokens"]
        if token["symbol"] == "WETH"
    )
    for pool, (shib_raw, weth_raw) in zip(evidence["pools"], reserve_pairs):
        if (pool["token0_address"], pool["token1_address"]) != (
            shib_address, weth_address
        ):
            raise AssertionError("fixture pool token order changed")
        pool["reserve0_raw"] = shib_raw
        pool["reserve1_raw"] = weth_raw
        pool["token0_balance_raw"] = shib_raw
        pool["token1_balance_raw"] = weth_raw
        pair = pool["pair_address"]
        _replace_test_call_result(
            evidence,
            "pair.getReserves",
            pair,
            _result_hex(
                _abi_word(shib_raw)
                + _abi_word(weth_raw)
                + _abi_word(pool["reserve_timestamp_last_raw"])
            ),
        )
        _replace_test_call_result(
            evidence,
            "erc20.balanceOf",
            shib_address,
            _result_hex(_abi_word(shib_raw)),
            pair[2:],
        )
        _replace_test_call_result(
            evidence,
            "erc20.balanceOf",
            weth_address,
            _result_hex(_abi_word(weth_raw)),
            pair[2:],
        )
    return _reseal_evidence(evidence)


def build_from_reserves(reserve_pairs):
    registry, _, trust_anchor = fixture_registry_and_code_results()
    with mock.patch.object(
        shib_v2_research, "_AUTHORITY_TRUST_ANCHOR", trust_anchor
    ):
        registry = shib_v2_research.load_research_registry(registry)
        evidence = _set_fixture_pool_reserves(
            valid_evidence_payload(registry), reserve_pairs
        )
        return shib_v2_research.build_research_snapshot(
            evidence, registry, "1" * 40
        )


def positive_edge_reserves():
    reserve_shib = 1_000_000 * 10**18
    return (
        (reserve_shib, 90 * 10**18),
        (reserve_shib, 100 * 10**18),
    )


def v2_quote_oracle(
    reserve_shib_raw,
    reserve_weth_raw,
    target_shib_raw,
    direction,
    shib_is_token0=True,
):
    shib_address = "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce"
    weth_address = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    pool_address = "0x811beed0119b4afce20d2583eb608c6f7af1954f"
    token0_address = shib_address if shib_is_token0 else weth_address
    token1_address = weth_address if shib_is_token0 else shib_address
    reserve0_raw = reserve_shib_raw if shib_is_token0 else reserve_weth_raw
    reserve1_raw = reserve_weth_raw if shib_is_token0 else reserve_shib_raw
    observed_at = "2024-03-09T16:00:00Z"
    state = V2PoolState(
        chain="eth",
        chain_id=1,
        dex="uniswap_v2",
        pool_address=pool_address,
        token0_address=token0_address,
        token1_address=token1_address,
        token0_decimals=18,
        token1_decimals=18,
        reserve0_raw=reserve0_raw,
        reserve1_raw=reserve1_raw,
        reserve_timestamp_last_raw=1709999990,
        fee_bps=30,
        fee_numerator=997,
        fee_denominator=1000,
        fee_formula=(
            "amount_in_with_fee=amount_in*fee_numerator;"
            "denominator=reserve_in*fee_denominator+amount_in_with_fee"
        ),
        fee_proof_sha256="a" * 64,
        block_number=20000000,
        block_hash="0x" + "1" * 64,
        block_header_sha256="b" * 64,
        observed_at=observed_at,
        raw_response_sha256="c" * 64,
    )
    rules = MarketRules(
        market_id="dex:eth:uniswap_v2:{}:SHIB".format(pool_address),
        base_asset="SHIB",
        quote_asset="WETH",
        base_unit_decimals=18,
        quote_unit_decimals=18,
        base_increment=Decimal("0.000000000000000001"),
        quote_increment=Decimal("0.000000000000000001"),
        min_base_quantity=Decimal(0),
        min_quote_notional=Decimal(0),
        observed_at=observed_at,
        valid_until="2024-03-09T16:00:01Z",
        source_record_sha256="c" * 64,
    )
    target = CommonTarget(
        asset="SHIB",
        unit_decimals=18,
        raw_quantity=target_shib_raw,
        lattice_raw=1,
    )
    return quote_v2_pool_quantity(
        state,
        target,
        rules,
        direction=direction,
        target_token_address=shib_address,
        quote_token_address=weth_address,
        cohort_now=observed_at,
    )


def mutate_reserve_and_rebind(evidence):
    changed = copy.deepcopy(evidence)
    pool = changed["pools"][0]
    return _set_fixture_pool_reserves(
        changed,
        (
            (pool["reserve0_raw"] + 1, pool["reserve1_raw"]),
            (
                changed["pools"][1]["reserve0_raw"],
                changed["pools"][1]["reserve1_raw"],
            ),
        ),
    )


def _reseal_snapshot(snapshot):
    snapshot["snapshot_sha256"] = shib_v2_research.snapshot_sha256(snapshot)
    return snapshot


class RecordingRpc:
    def __init__(self, responses):
        self.responses = responses
        self.requests = []

    @property
    def fixed_block_state_requests(self):
        return [
            request for request in self.requests
            if request["method"] in {"eth_call", "eth_getCode"}
        ]

    def __call__(self, method, params):
        request = {"method": method, "params": copy.deepcopy(params)}
        self.requests.append(request)
        if method == "eth_chainId":
            return self.responses["chain_id"]
        if method in {"eth_getBlockByNumber", "eth_getBlockByHash"}:
            return copy.deepcopy(self.responses["header"])
        if method == "eth_getCode":
            return self.responses["results"][(params[0], "0x")]
        if method == "eth_call":
            return self.responses["results"][(params[0]["to"], params[0]["data"])]
        raise AssertionError("unexpected RPC method: {}".format(method))


class ChangedRoundTripRpc(RecordingRpc):
    def __call__(self, method, params):
        if method == "eth_getBlockByHash":
            self.requests.append({"method": method, "params": copy.deepcopy(params)})
            header = copy.deepcopy(self.responses["header"])
            header["stateRoot"] = "0x" + "44" * 32
            return header
        return super().__call__(method, params)


class ChangedFinalRoundTripRpc(RecordingRpc):
    def __init__(self, responses):
        super().__init__(responses)
        self.by_hash_count = 0

    def __call__(self, method, params):
        if method == "eth_getBlockByHash":
            self.by_hash_count += 1
            if self.by_hash_count == 2:
                self.requests.append({
                    "method": method,
                    "params": copy.deepcopy(params),
                })
                header = copy.deepcopy(self.responses["header"])
                header["transactionsRoot"] = "0x" + "66" * 32
                return header
        return super().__call__(method, params)


class Eip1898RejectingRpc(RecordingRpc):
    def __call__(self, method, params):
        if method in {"eth_call", "eth_getCode"}:
            self.requests.append({"method": method, "params": copy.deepcopy(params)})
            raise capture.CaptureError("eip1898_unavailable")
        return super().__call__(method, params)


class TransientStateRpc(RecordingRpc):
    def __init__(self, responses):
        super().__init__(responses)
        self.failed_once = False

    def __call__(self, method, params):
        if method in {"eth_call", "eth_getCode"} and not self.failed_once:
            self.failed_once = True
            self.requests.append({"method": method, "params": copy.deepcopy(params)})
            raise OSError("transient provider failure")
        return super().__call__(method, params)


class StaticHttpResponse:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self, limit):
        return self.body[:limit]


class StaticOpener:
    def __init__(self, body):
        self.body = body
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return StaticHttpResponse(self.body)


def remove_logical_call(payload):
    removed = next(
        call for call in payload["logical_calls"] if call["method"] == "fee.beta"
    )
    payload["logical_calls"].remove(removed)
    payload["provider_observations"] = [
        observation for observation in payload["provider_observations"]
        if observation["logical_call_id"] != removed["logical_call_id"]
    ]
    return _reseal_evidence(payload)


def add_unknown_call(payload):
    call = copy.deepcopy(next(
        item for item in payload["logical_calls"]
        if item["method"] == "erc20.decimals"
    ))
    call["calldata"] += "00"
    _reseal_test_call_identity(call)
    payload["logical_calls"].append(call)
    payload["provider_observations"].extend(
        {
            "provider_label": label,
            "logical_call_id": call["logical_call_id"],
            "block_hash": payload["block"]["hash"],
            "result_sha256": call["result_sha256"],
            "status": "observed",
        }
        for label in ("provider_a", "provider_b")
    )
    _sort_test_ledger(payload)
    return _reseal_evidence(payload)


def duplicate_logical_key(payload):
    duplicate = copy.deepcopy(payload["logical_calls"][-1])
    payload["logical_calls"].append(duplicate)
    duplicate_observations = [
        copy.deepcopy(observation)
        for observation in payload["provider_observations"]
        if observation["logical_call_id"] == duplicate["logical_call_id"]
    ]
    payload["provider_observations"].extend(duplicate_observations)
    _sort_test_ledger(payload)
    return _reseal_evidence(payload)


def remove_provider_observation(payload):
    payload["provider_observations"].pop()
    return _reseal_evidence(payload)


def disagree_provider_result(payload):
    payload["provider_observations"][0]["result_sha256"] = "f" * 64
    return _reseal_evidence(payload)


def change_block_hash(payload):
    payload["provider_observations"][0]["block_hash"] = "0x" + "44" * 32
    return _reseal_evidence(payload, rebind_groups=False, rebind_quality=False)


def change_factory_pair(payload):
    wrong = "0x" + "44" * 20
    pool = payload["pools"][0]
    pool["factory_get_pair_result"] = wrong
    _replace_test_call_result(
        payload, "factory.getPair", pool["factory_address"], _result_hex(_abi_address(wrong))
    )
    return _reseal_evidence(payload)


def change_pair_factory(payload):
    wrong = "0x" + "44" * 20
    pool = payload["pools"][0]
    pool["pair_factory_result"] = wrong
    _replace_test_call_result(
        payload, "pair.factory", pool["pair_address"], _result_hex(_abi_address(wrong))
    )
    return _reseal_evidence(payload)


def change_router_factory(payload):
    wrong = "0x" + "44" * 20
    pool = payload["pools"][0]
    pool["router_factory_result"] = wrong
    _replace_test_call_result(
        payload, "router.factory", pool["router_address"], _result_hex(_abi_address(wrong))
    )
    return _reseal_evidence(payload)


def change_router_weth(payload):
    wrong = "0x" + "44" * 20
    pool = payload["pools"][0]
    pool["router_weth_result"] = wrong
    _replace_test_call_result(
        payload, "router.weth", pool["router_address"], _result_hex(_abi_address(wrong))
    )
    return _reseal_evidence(payload)


def change_token_order(payload):
    pool = payload["pools"][0]
    pool["token0_address"], pool["token1_address"] = (
        pool["token1_address"], pool["token0_address"]
    )
    _replace_test_call_result(
        payload,
        "pair.token0",
        pool["pair_address"],
        _result_hex(_abi_address(pool["token0_address"])),
    )
    _replace_test_call_result(
        payload,
        "pair.token1",
        pool["pair_address"],
        _result_hex(_abi_address(pool["token1_address"])),
    )
    return _reseal_evidence(payload)


def change_runtime_code_hash(payload):
    token = payload["tokens"][0]
    call = _find_test_call(payload, "eth_getCode", token["address"])
    code = bytes.fromhex(call["result_hex"][2:]) + b"\x0a"
    token["runtime_code_size_bytes"] = len(code)
    token["runtime_code_sha256"] = hashlib.sha256(code).hexdigest()
    _replace_test_call_result(
        payload, "eth_getCode", token["address"], _result_hex(code)
    )
    return _reseal_evidence(payload)


def change_balance(payload):
    pool = payload["pools"][0]
    pool["token0_balance_raw"] += 1
    _replace_test_call_result(
        payload,
        "erc20.balanceOf",
        pool["token0_address"],
        _result_hex(_abi_word(pool["token0_balance_raw"])),
        calldata_suffix=pool["pair_address"][2:],
    )
    return _reseal_evidence(payload)


def change_shibaswap_fee(payload):
    pool = payload["pools"][1]
    pool["fee_parameters"]["total_fee"] += 1
    _replace_test_call_result(
        payload,
        "fee.totalFee",
        pool["pair_address"],
        _result_hex(_abi_word(pool["fee_parameters"]["total_fee"])),
    )
    return _reseal_evidence(payload)


def stale_chainlink_round(payload):
    reference = payload["usd_reference"]
    reference["started_at"] = payload["block"]["timestamp"] - 3601
    reference["updated_at"] = payload["block"]["timestamp"] - 3601
    reference["freshness_lag_seconds"] = 3601
    _replace_test_chainlink_round(payload)
    return _reseal_evidence(payload)


def future_chainlink_round(payload):
    reference = payload["usd_reference"]
    reference["started_at"] = payload["block"]["timestamp"] + 1
    reference["updated_at"] = payload["block"]["timestamp"] + 1
    reference["freshness_lag_seconds"] = 0
    _replace_test_chainlink_round(payload)
    return _reseal_evidence(payload)


def forge_quality_summary(payload):
    payload["collection_quality"]["usable_logical_call_count"] -= 1
    payload["evidence_identity"] = _test_evidence_identity(payload)
    return payload


def forge_evidence_identity(payload):
    payload["evidence_identity"] = "f" * 64
    return payload


def _test_evidence_identity(payload):
    body = dict(payload)
    body.pop("evidence_identity", None)
    return hashlib.sha256(
        b"shib-v2-research-evidence/v1\n"
        + json.dumps(
            body,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _find_test_call(payload, method, target, calldata_suffix=None):
    matches = [
        call for call in payload["logical_calls"]
        if call["method"] == method
        and call["target"] == target
        and (calldata_suffix is None or call["calldata"].endswith(calldata_suffix))
    ]
    if len(matches) != 1:
        raise AssertionError("test call lookup is ambiguous: {} {}".format(method, target))
    return matches[0]


def _reseal_test_call_identity(call):
    call["calldata_sha256"] = hashlib.sha256(
        bytes.fromhex(call["calldata"][2:])
    ).hexdigest()
    identity = {
        "method": call["method"],
        "target": call["target"],
        "calldata_sha256": call["calldata_sha256"],
        "block_selector": "eip1898_block_hash_require_canonical",
    }
    call["logical_call_id"] = "call:" + hashlib.sha256(
        b"shib-v2-logical-call/v1\n" + _test_canonical_json_bytes(identity)
    ).hexdigest()


def _replace_test_call_result(
    payload, method, target, result_hex, calldata_suffix=None
):
    call = _find_test_call(payload, method, target, calldata_suffix)
    call["result_hex"] = result_hex
    call["result_sha256"] = hashlib.sha256(
        bytes.fromhex(result_hex[2:])
    ).hexdigest()
    for observation in payload["provider_observations"]:
        if observation["logical_call_id"] == call["logical_call_id"]:
            observation["result_sha256"] = call["result_sha256"]


def _replace_test_chainlink_round(payload):
    reference = payload["usd_reference"]
    _replace_test_call_result(
        payload,
        "feed.latestRoundData",
        reference["proxy_address"],
        _result_hex(
            _abi_word(reference["round_id"])
            + _abi_word(reference["answer"])
            + _abi_word(reference["started_at"])
            + _abi_word(reference["updated_at"])
            + _abi_word(reference["answered_in_round"])
        ),
    )


def _sort_test_ledger(payload):
    payload["logical_calls"].sort(key=lambda call: call["logical_call_id"])
    label_order = {"provider_a": 0, "provider_b": 1}
    payload["provider_observations"].sort(
        key=lambda observation: (
            observation["logical_call_id"],
            label_order[observation["provider_label"]],
        )
    )


def _reseal_test_group_hashes(payload):
    calls = payload["logical_calls"]
    for token in payload["tokens"]:
        token_calls = [
            call for call in calls
            if call["target"] == token["address"]
            and call["method"] in {"eth_getCode", "erc20.decimals"}
        ]
        token["call_results_sha256"] = _call_group_sha256(token_calls)
    for pool in payload["pools"]:
        pair = pool["pair_address"]
        pool_calls = [
            call for call in calls
            if call["target"] in {
                pool["factory_address"], pool["router_address"], pair,
            }
            or call["method"] == "erc20.decimals"
            or (
                call["method"] == "erc20.balanceOf"
                and call["calldata"].endswith(pair[2:])
            )
        ]
        pool["call_results_sha256"] = _call_group_sha256(pool_calls)
        if pool["dex"] == "uniswap_v2":
            fee_calls = [
                call for call in calls
                if call["method"] == "eth_getCode" and call["target"] == pair
            ]
        else:
            fee_calls = [
                call for call in calls
                if call["target"] == pair and call["method"] in {
                    "fee.totalFee", "fee.alpha", "fee.beta",
                }
            ]
        pool["fee_evidence_sha256"] = _call_group_sha256(fee_calls)
    reference = payload["usd_reference"]
    reference_calls = [
        call for call in calls
        if call["target"] == reference["proxy_address"]
        and call["method"] in {
            "eth_getCode", "feed.decimals", "feed.description",
            "feed.latestRoundData",
        }
    ]
    reference["call_results_sha256"] = _call_group_sha256(reference_calls)


def _reseal_evidence(
    payload, rebind_groups=True, rebind_quality=True, expected_call_count=35
):
    if rebind_groups:
        _reseal_test_group_hashes(payload)
    if rebind_quality:
        payload["collection_quality"] = _derive_test_quality(
            payload, expected_call_count
        )
    payload["evidence_identity"] = _test_evidence_identity(payload)
    return payload


def _reseal_block(payload, block_hash):
    payload["block"]["hash"] = block_hash
    header = {
        field: payload["block"][field]
        for field in (
            "number", "hash", "parent_hash", "timestamp", "state_root",
            "base_fee_per_gas",
        )
    }
    header_sha256 = hashlib.sha256(
        json.dumps(
            header,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    payload["block"]["canonical_header_sha256"] = header_sha256
    for observation in payload["block"]["provider_header_observations"]:
        observation["canonical_header_sha256"] = header_sha256
    for observation in payload["provider_observations"]:
        observation["block_hash"] = block_hash
    payload["evidence_identity"] = _test_evidence_identity(payload)
    return payload


class ResearchRegistryTests(unittest.TestCase):
    def test_repository_registry_fixes_exactly_two_shib_weth_pools(self):
        registry = shib_v2_research.load_research_registry(
            json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        )
        self.assertEqual(registry["schema"], "shib_v2_research_registry/v1")
        self.assertEqual(registry["chain"], {"name": "eth", "chain_id": 1})
        self.assertEqual(
            [pool["pair"]["address"] for pool in registry["pools"]],
            [
                "0x811beed0119b4afce20d2583eb608c6f7af1954f",
                "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
            ],
        )
        self.assertEqual(
            registry["requested_notionals_usd"],
            ["1000", "5000", "10000", "50000", "100000"],
        )

    def test_registry_rejects_unknown_fields_case_drift_and_duplicate_pools(self):
        for mutation in (add_unknown_field, uppercase_shib, duplicate_first_pool):
            with self.subTest(mutation=mutation.__name__):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.load_research_registry(
                        mutation(copy.deepcopy(valid_registry_payload()))
                    )

    def test_registry_rejects_fee_drift_that_preserves_native_relationships(self):
        mutations = (
            (0, {"fee_bps": 1, "fee_numerator": 999}),
            (1, {"fee_bps": 15, "fee_denominator": 2000}),
        )
        for pool_index, changes in mutations:
            with self.subTest(pool_index=pool_index, changes=changes):
                payload = valid_registry_payload()
                payload["pools"][pool_index]["fee_model"].update(changes)
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.load_research_registry(payload)

    def test_fixture_authority_patch_is_private_scoped_and_restored(self):
        registry, _, trust_anchor = fixture_registry_and_code_results()
        with mock.patch.object(
            shib_v2_research, "_AUTHORITY_TRUST_ANCHOR", trust_anchor, create=True
        ):
            try:
                loaded = shib_v2_research.load_research_registry(registry)
            except shib_v2_research.ResearchContractError as error:
                self.fail("private fixture anchor was ignored: {}".format(error))
            self.assertEqual(loaded["chain"]["chain_id"], 1)
        with self.assertRaises(shib_v2_research.ResearchContractError):
            shib_v2_research.load_research_registry(registry)
        repository_registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        self.assertEqual(
            shib_v2_research.load_research_registry(repository_registry)["schema"],
            "shib_v2_research_registry/v1",
        )


class ResearchCaptureTests(unittest.TestCase):
    def setUp(self):
        registry, _, trust_anchor = fixture_registry_and_code_results()
        self.authority_patcher = mock.patch.object(
            shib_v2_research, "_AUTHORITY_TRUST_ANCHOR", trust_anchor
        )
        self.authority_patcher.start()
        self.addCleanup(self.authority_patcher.stop)
        self.registry = shib_v2_research.load_research_registry(registry)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.output = Path(self.temporary_directory.name) / "evidence.json"
        self.block_hash = "0x" + "11" * 32

    def test_capture_uses_one_finalized_hash_and_eip1898_for_every_call(self):
        responses_a, responses_b = valid_rpc_responses(self.registry)
        provider_a = RecordingRpc(responses_a)
        provider_b = RecordingRpc(responses_b)
        evidence = capture.capture_research_evidence(
            self.registry,
            [
                Provider("provider_a", "a" * 64, provider_a),
                Provider("provider_b", "b" * 64, provider_b),
            ],
            self.output,
        )
        self.assertEqual(evidence["collection_quality"]["state"], "evaluated")
        self.assertEqual(len(evidence["logical_calls"]), 35)
        self.assertEqual(len(evidence["provider_observations"]), 70)
        for recorder in (provider_a, provider_b):
            self.assertEqual(len(recorder.fixed_block_state_requests), 35)
            for request in recorder.fixed_block_state_requests:
                self.assertEqual(
                    request["params"][1],
                    {"blockHash": self.block_hash, "requireCanonical": True},
                )
        self.assertEqual(
            self.output.read_bytes(),
            shib_v2_research.canonical_json_bytes(evidence) + b"\n",
        )
        rendered = self.output.read_text(encoding="utf-8")
        self.assertNotIn("a" * 64, rendered)
        self.assertNotIn("b" * 64, rendered)
        self.assertNotIn(responses_a["header"]["transactionsRoot"], rendered)
        self.assertNotIn(responses_a["header"]["requestsHash"], rendered)
        for recorder in (provider_a, provider_b):
            methods = [request["method"] for request in recorder.requests]
            self.assertEqual(methods.count("eth_getBlockByNumber"), 1)
            self.assertEqual(methods.count("eth_getBlockByHash"), 2)
            self.assertEqual(
                {request["method"] for request in recorder.fixed_block_state_requests},
                {"eth_call", "eth_getCode"},
            )
            self.assertEqual(
                [
                    request["params"]
                    for request in recorder.requests
                    if request["method"] == "eth_getBlockByNumber"
                ],
                [["finalized", False]],
            )
            self.assertEqual(
                [
                    request["params"]
                    for request in recorder.requests
                    if request["method"] == "eth_getBlockByHash"
                ],
                [[self.block_hash, False], [self.block_hash, False]],
            )

    def _provider_pair(self, rpc_a, rpc_b):
        return [
            Provider("provider_a", "a" * 64, rpc_a),
            Provider("provider_b", "b" * 64, rpc_b),
        ]

    def _failing_providers(self, failure):
        responses_a, responses_b = valid_rpc_responses(self.registry)
        if failure == "wrong_chain_id":
            responses_a["chain_id"] = "0x2"
        elif failure == "different_finalized_hash":
            responses_b["header"]["hash"] = "0x" + "55" * 32
        elif failure == "different_transactions_root":
            responses_b["header"]["transactionsRoot"] = "0x" + "55" * 32
        elif failure == "different_requests_hash":
            responses_b["header"]["requestsHash"] = "0x" + "55" * 32
        elif failure == "different_call_bytes":
            first_key = next(iter(responses_b["results"]))
            responses_b["results"][first_key] = "0x" + "ff" * 32
        elif failure == "missing_state":
            first_key = next(iter(responses_a["results"]))
            responses_a["results"][first_key] = "0x"
        elif failure == "oversized_response":
            first_key = next(iter(responses_a["results"]))
            responses_a["results"][first_key] = (
                "0x" + "00" * (capture.MAX_CALL_RESULT_BYTES + 1)
            )
        rpc_a = RecordingRpc(responses_a)
        rpc_b = RecordingRpc(responses_b)
        if failure == "eip1898_error":
            rpc_a = Eip1898RejectingRpc(responses_a)
        elif failure == "changed_round_trip_header":
            rpc_b = ChangedRoundTripRpc(responses_b)
        elif failure == "changed_final_header":
            rpc_b = ChangedFinalRoundTripRpc(responses_b)
        return self._provider_pair(rpc_a, rpc_b)

    def test_capture_preserves_absent_or_existing_output_on_every_failure(self):
        failures = (
            "wrong_chain_id",
            "different_finalized_hash",
            "different_transactions_root",
            "different_requests_hash",
            "different_call_bytes",
            "eip1898_error",
            "missing_state",
            "changed_round_trip_header",
            "changed_final_header",
            "oversized_response",
        )
        for failure in failures:
            for original in (None, b"old-output\n"):
                with self.subTest(failure=failure, original=original):
                    self.output.unlink(missing_ok=True)
                    if original is not None:
                        self.output.write_bytes(original)
                    with self.assertRaises(capture.CaptureError) as caught:
                        capture.capture_research_evidence(
                            self.registry,
                            self._failing_providers(failure),
                            self.output,
                        )
                    self.assertIn(caught.exception.reason_code, capture.CAPTURE_REASONS)
                    if original is None:
                        self.assertFalse(self.output.exists())
                    else:
                        self.assertEqual(self.output.read_bytes(), original)

    def test_full_block_agreement_rejects_nonprojected_initial_and_final_drift(self):
        for failure, reason in (
            ("different_transactions_root", "provider_disagreement"),
            ("different_requests_hash", "provider_disagreement"),
            ("changed_final_header", "canonical_block_unavailable"),
        ):
            for original in (None, b"old-output\n"):
                with self.subTest(failure=failure, original=original):
                    self.output.unlink(missing_ok=True)
                    if original is not None:
                        self.output.write_bytes(original)
                    with self.assertRaisesRegex(
                        capture.CaptureError, "^{}$".format(reason)
                    ):
                        capture.capture_research_evidence(
                            self.registry,
                            self._failing_providers(failure),
                            self.output,
                        )
                    if original is None:
                        self.assertFalse(self.output.exists())
                    else:
                        self.assertEqual(self.output.read_bytes(), original)

    def test_capture_returns_committed_evidence_after_postcommit_fsync_failure(self):
        real_fsync = shib_v2_research_io.os.fsync
        real_replace = shib_v2_research_io.os.replace

        for original in (None, b"old-output\n"):
            with self.subTest(original=original):
                self.output.unlink(missing_ok=True)
                if original is not None:
                    self.output.write_bytes(original)
                responses_a, responses_b = valid_rpc_responses(self.registry)
                providers = self._provider_pair(
                    RecordingRpc(responses_a), RecordingRpc(responses_b)
                )
                state = {"committed": False, "postcommit_fsync_failures": 0}

                def record_replace(*args, **kwargs):
                    result = real_replace(*args, **kwargs)
                    state["committed"] = True
                    return result

                def fail_postcommit_directory_fsync(descriptor):
                    metadata = os.fstat(descriptor)
                    if stat.S_ISDIR(metadata.st_mode) and state["committed"]:
                        state["postcommit_fsync_failures"] += 1
                        raise OSError("postcommit directory fsync failure")
                    return real_fsync(descriptor)

                with mock.patch.object(
                    shib_v2_research_io.os,
                    "replace",
                    side_effect=record_replace,
                ), mock.patch.object(
                    shib_v2_research_io.os,
                    "fsync",
                    side_effect=fail_postcommit_directory_fsync,
                ):
                    try:
                        evidence = capture.capture_research_evidence(
                            self.registry, providers, self.output
                        )
                    except capture.CaptureError as error:
                        self.fail(
                            "postcommit fsync surfaced as capture failure: {}".format(
                                error.reason_code
                            )
                        )
                self.assertEqual(state["postcommit_fsync_failures"], 1)
                self.assertEqual(
                    self.output.read_bytes(),
                    shib_v2_research.canonical_json_bytes(evidence) + b"\n",
                )
                leaked = [
                    path.name
                    for path in self.output.parent.iterdir()
                    if path.name.startswith("." + self.output.name + ".")
                ]
                self.assertEqual(leaked, [])

    def test_capture_reason_classification_does_not_depend_on_validator_prose(self):
        for message in (
            "router wording changed",
            "fee wording changed",
            "USD reference wording changed",
            "registry wording changed",
            "block header wording changed",
        ):
            with self.subTest(message=message):
                self.output.unlink(missing_ok=True)
                responses_a, responses_b = valid_rpc_responses(self.registry)
                providers = self._provider_pair(
                    RecordingRpc(responses_a), RecordingRpc(responses_b)
                )
                with mock.patch.object(
                    shib_v2_research,
                    "validate_research_evidence",
                    side_effect=shib_v2_research.ResearchContractError(message),
                ):
                    with self.assertRaisesRegex(
                        capture.CaptureError, "^rpc_response_invalid$"
                    ):
                        capture.capture_research_evidence(
                            self.registry, providers, self.output
                        )
                self.assertFalse(self.output.exists())

    def test_configuration_rejects_cardinality_labels_and_endpoint_duplicates_first(
        self,
    ):
        class NoRequestRpc:
            def __call__(self, method, params):
                raise AssertionError("configuration error reached transport")

        rpc = NoRequestRpc()
        invalid_provider_sets = (
            [Provider("provider_a", "a" * 64, rpc)],
            [
                Provider("provider_a", "a" * 64, rpc),
                Provider("provider_a", "b" * 64, rpc),
            ],
            [
                Provider("provider_a", "a" * 64, rpc),
                Provider("provider_b", "a" * 64, rpc),
            ],
            [
                Provider("provider_a", "a" * 64, rpc),
                Provider("provider_b", "b" * 64, rpc),
                Provider("provider_b", "c" * 64, rpc),
            ],
        )
        for providers in invalid_provider_sets:
            for original in (None, b"old-output\n"):
                with self.subTest(count=len(providers), original=original):
                    self.output.unlink(missing_ok=True)
                    if original is not None:
                        self.output.write_bytes(original)
                    with self.assertRaisesRegex(
                        capture.CaptureError, "^capture_configuration_invalid$"
                    ):
                        capture.capture_research_evidence(
                            self.registry, providers, self.output
                        )
                    if original is None:
                        self.assertFalse(self.output.exists())
                    else:
                        self.assertEqual(self.output.read_bytes(), original)

    def test_retry_repeats_the_same_eip1898_hash_without_new_block_selection(self):
        responses_a, responses_b = valid_rpc_responses(self.registry)
        provider_a = TransientStateRpc(responses_a)
        provider_b = RecordingRpc(responses_b)
        capture.capture_research_evidence(
            self.registry,
            self._provider_pair(provider_a, provider_b),
            self.output,
        )
        first, retry = provider_a.fixed_block_state_requests[:2]
        self.assertEqual(first, retry)
        self.assertEqual(
            first["params"][1],
            {"blockHash": self.block_hash, "requireCanonical": True},
        )
        self.assertEqual(
            sum(
                request["method"] == "eth_getBlockByNumber"
                for request in provider_a.requests
            ),
            1,
        )

    def test_capture_failure_never_contains_url_key_or_provider_body(self):
        secret_url = "https://rpc.example/v2/sk-live-private"

        class FailingRpc:
            def __call__(self, method, params):
                raise OSError(
                    secret_url + " provider said account@example.test"
                )

        with self.assertRaises(capture.CaptureError) as caught:
            capture.capture_research_evidence(
                self.registry,
                self._provider_pair(FailingRpc(), FailingRpc()),
                self.output,
            )
        rendered = str(caught.exception)
        self.assertEqual(rendered, "rpc_response_invalid")
        self.assertNotIn(secret_url, rendered)
        self.assertNotIn("account@example.test", rendered)
        self.assertEqual(
            capture.sanitize_capture_failure(
                ValueError(secret_url + " account@example.test")
            ),
            "rpc_response_invalid",
        )
        self.assertEqual(
            str(capture.CaptureError("not_allowlisted")),
            "rpc_response_invalid",
        )

    def test_bounded_transport_uses_fixed_public_user_agent(self):
        expected_user_agent = "CEX-DEX-analysis-research-capture/1"
        expected_headers = {
            "user_agent": expected_user_agent,
            "authorization": None,
            "cookie": None,
            "proxy_authorization": None,
        }

        class UserAgentGateOpener(StaticOpener):
            def __init__(self):
                super().__init__(b'{"id":1,"jsonrpc":"2.0","result":"0x1"}')
                self.observed_headers = []

            def open(self, request, timeout):
                self.requests.append((request, timeout))
                headers = {
                    "user_agent": request.get_header("User-agent"),
                    "authorization": request.get_header("Authorization"),
                    "cookie": request.get_header("Cookie"),
                    "proxy_authorization": request.get_header(
                        "Proxy-authorization"
                    ),
                }
                self.observed_headers.append(headers)
                if headers != expected_headers:
                    raise urllib.error.HTTPError(
                        request.full_url, 403, "Forbidden", None, None
                    )
                return StaticHttpResponse(self.body)

        transport = capture.BoundedJsonRpcTransport("https://rpc.example", 2)
        gate = UserAgentGateOpener()
        transport._opener = gate
        self.assertEqual(transport("eth_chainId", []), "0x1")
        self.assertEqual(gate.observed_headers, [expected_headers])

    def test_bounded_transport_uses_fixed_post_envelope_and_maps_remote_errors(self):
        endpoint = "https://rpc.example/v2/sk-live-private"
        transport = capture.BoundedJsonRpcTransport(endpoint, 7)
        success = StaticOpener(
            b'{"id":1,"jsonrpc":"2.0","result":"0x1"}'
        )
        transport._opener = success
        self.assertEqual(transport("eth_chainId", []), "0x1")
        request, timeout = success.requests[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(timeout, 7)
        self.assertEqual(
            json.loads(request.data.decode("utf-8")),
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_chainId",
                "params": [],
            },
        )

        remote_error = StaticOpener(
            b'{"error":{"code":-32000,"message":"sk-live-private '
            b'account@example.test"},"id":1,"jsonrpc":"2.0"}'
        )
        transport._opener = remote_error
        with self.assertRaisesRegex(
            capture.CaptureError, "^canonical_block_unavailable$"
        ):
            transport("eth_getBlockByNumber", ["finalized", False])
        with self.assertRaisesRegex(
            capture.CaptureError, "^eip1898_unavailable$"
        ):
            transport(
                "eth_getCode",
                [
                    "0x" + "11" * 20,
                    {"blockHash": self.block_hash, "requireCanonical": True},
                ],
            )

    def test_bounded_transport_rejects_oversize_members_and_unreviewed_requests(self):
        transport = capture.BoundedJsonRpcTransport("https://rpc.example", 20)
        transport._opener = StaticOpener(
            b"x" * (capture.MAX_RPC_RESPONSE_BYTES + 1)
        )
        with self.assertRaisesRegex(
            capture.CaptureError, "^rpc_response_invalid$"
        ):
            transport("eth_chainId", [])
        transport._opener = StaticOpener(
            b'{"extra":0,"id":1,"jsonrpc":"2.0","result":"0x1"}'
        )
        with self.assertRaisesRegex(
            capture.CaptureError, "^rpc_response_invalid$"
        ):
            transport("eth_chainId", [])
        with self.assertRaisesRegex(
            capture.CaptureError, "^capture_configuration_invalid$"
        ):
            transport("eth_getBalance", ["0x" + "11" * 20, "latest"])
        with self.assertRaisesRegex(
            capture.CaptureError, "^capture_configuration_invalid$"
        ):
            transport(
                "eth_getCode", ["0x" + "11" * 20, hex(20000000)]
            )

    def test_transport_accepts_bounded_blocks_and_rejects_structural_bombs(self):
        block = {
            "number": "0x1312d00",
            "hash": self.block_hash,
            "parentHash": "0x" + "22" * 32,
            "timestamp": "0x65ec8780",
            "stateRoot": "0x" + "33" * 32,
            "baseFeePerGas": "0x0",
            "transactions": ["0x" + "44" * 32 for _ in range(300)],
        }
        body = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "result": block},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        transport = capture.BoundedJsonRpcTransport("https://rpc.example", 20)
        transport._opener = StaticOpener(body)
        result = transport("eth_getBlockByNumber", ["finalized", False])
        self.assertEqual(len(result["transactions"]), 300)

        too_many = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [0] * (capture.MAX_RPC_MEMBERS + 1),
        }).encode("utf-8")
        transport._opener = StaticOpener(too_many)
        with self.assertRaisesRegex(
            capture.CaptureError, "^rpc_response_invalid$"
        ):
            transport("eth_chainId", [])

        nested = (
            b'{"id":1,"jsonrpc":"2.0","result":'
            + b"[" * 1100
            + b"0"
            + b"]" * 1100
            + b"}"
        )
        transport._opener = StaticOpener(nested)
        with self.assertRaisesRegex(
            capture.CaptureError, "^rpc_response_invalid$"
        ):
            transport("eth_chainId", [])

        with self.assertRaisesRegex(
            capture.CaptureError, "^capture_configuration_invalid$"
        ):
            capture.BoundedJsonRpcTransport("http://[", 20)

    def test_cli_rejects_equal_literal_urls_before_network_and_preserves_output(self):
        registry_path = Path(self.temporary_directory.name) / "registry.json"
        registry_path.write_bytes(REGISTRY_PATH.read_bytes())
        command_prefix = [
            sys.executable,
            str(PROJECT_ROOT / "scripts/capture_shib_v2_research_evidence.py"),
            "--registry",
            str(registry_path),
            "--rpc-url-a",
            "http://127.0.0.1:1/private",
            "--rpc-url-b",
            "http://127.0.0.1:1/private",
            "--output",
            str(self.output),
        ]
        for original in (None, b"old-output\n"):
            with self.subTest(original=original):
                self.output.unlink(missing_ok=True)
                if original is not None:
                    self.output.write_bytes(original)
                completed = subprocess.run(
                    command_prefix,
                    cwd=str(PROJECT_ROOT),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                self.assertNotEqual(completed.returncode, 0)
                self.assertEqual(completed.stdout, b"")
                self.assertEqual(
                    completed.stderr, b"capture_configuration_invalid\n"
                )
                if original is None:
                    self.assertFalse(self.output.exists())
                else:
                    self.assertEqual(self.output.read_bytes(), original)

    def test_cli_argument_errors_emit_only_allowlisted_configuration_reason(self):
        completed = subprocess.run(
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts/capture_shib_v2_research_evidence.py"),
                "--registry",
                str(REGISTRY_PATH),
                "--rpc-url-a",
                "http://127.0.0.1:1/a",
                "--rpc-url-b",
                "http://127.0.0.1:1/b",
                "--output",
                str(self.output),
                "--timeout-seconds",
                "sk-live-private-timeout",
            ],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout, b"")
        self.assertEqual(
            completed.stderr, b"capture_configuration_invalid\n"
        )
        self.assertFalse(self.output.exists())


class RepositoryEvidenceIntegrationTests(unittest.TestCase):
    def test_repository_evidence_is_exact_validated_and_public_safe(self):
        self.assertTrue(EVIDENCE_PATH.is_file(), "tracked evidence file is missing")
        registry = shib_v2_research.load_research_registry(
            load_bounded_json(REGISTRY_PATH, "research registry")
        )
        evidence = shib_v2_research.validate_research_evidence(
            load_bounded_json(EVIDENCE_PATH, "research evidence"),
            registry,
        )

        self.assertEqual(
            evidence["registry_sha256"],
            "1099388921c6dc1a790a9e3e54fd87999a884a6e8eddebe761e1d63256e1c82c",
        )
        self.assertEqual(evidence["block"]["number"], 25860867)
        self.assertEqual(
            evidence["block"]["hash"],
            "0x806fc920f52b11cb56749e7786d176d7d2d21310f145184e83cc5ec5d882d75b",
        )
        self.assertEqual(
            evidence["evidence_identity"],
            "bd87318aa60de73874e05f436ae6f84c66b4b56fd53612929d9cfe7cfa5c7427",
        )

        quality = evidence["collection_quality"]
        self.assertEqual(quality["state"], "evaluated")
        self.assertEqual(quality["expected_logical_call_count"], 35)
        self.assertEqual(quality["observed_logical_call_count"], 35)
        self.assertEqual(quality["usable_logical_call_count"], 35)
        self.assertEqual(quality["expected_provider_observation_count"], 70)
        self.assertEqual(quality["observed_provider_observation_count"], 70)
        self.assertEqual(quality["usable_provider_observation_count"], 70)
        self.assertEqual(quality["duplicate_logical_call_key_count"], 0)
        self.assertEqual(quality["duplicate_provider_observation_key_count"], 0)
        self.assertEqual(quality["required_field_null_count"], 0)
        self.assertEqual(quality["missing_null_count"], 0)
        self.assertEqual(quality["provider_disagreement_count"], 0)

        observations = evidence["provider_observations"]
        for call in evidence["logical_calls"]:
            providers = [
                observation["provider_label"]
                for observation in observations
                if observation["logical_call_id"] == call["logical_call_id"]
            ]
            self.assertEqual(providers, ["provider_a", "provider_b"])

        rendered = EVIDENCE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("http" + "://", rendered.lower())
        self.assertNotIn("wss" + "://", rendered.lower())
        self.assertNotRegex(rendered, r"\?[A-Za-z0-9_.%~-]+(?:=|%3[dD])")
        self.assertNotIn("@", rendered)
        for forbidden in (
            "authorization",
            "cookie",
            "api_key",
            "private_key",
            "provider_error",
            "raw_response",
            "raw_rpc",
            "rpc_url",
            "account",
            "wallet",
            "/Users" + "/",
            "/home" + "/",
            "/root" + "/",
            "/private" + "/",
            "/tmp" + "/",
        ):
            self.assertNotIn(forbidden.lower(), rendered.lower())
        self.assertEqual(
            EVIDENCE_PATH.read_bytes(),
            shib_v2_research.canonical_json_bytes(evidence) + b"\n",
        )


class RepositorySnapshotIntegrationTests(unittest.TestCase):
    APPLICATION_SHA = "31483cbb13c81d2dd193778d14505202ff1be5e7"
    SNAPSHOT_SHA256 = (
        "a109661a50da44e04cb6e8f913319f5fb9b05df363da9c33249d5b7668ae667d"
    )

    def _load_authorities_and_snapshot(self):
        self.assertTrue(
            SNAPSHOT_PATH.is_file(), "tracked research snapshot is missing"
        )
        registry = shib_v2_research.load_research_registry(
            load_bounded_json(REGISTRY_PATH, "research registry")
        )
        evidence = shib_v2_research.validate_research_evidence(
            load_bounded_json(EVIDENCE_PATH, "research evidence"),
            registry,
        )
        snapshot = load_bounded_json(SNAPSHOT_PATH, "research snapshot")
        return registry, evidence, snapshot

    def test_checked_in_snapshot_regenerates_byte_for_byte(self):
        registry, evidence, snapshot = self._load_authorities_and_snapshot()
        expected = SNAPSHOT_PATH.read_bytes()
        rebuilt = shib_v2_research.build_research_snapshot(
            evidence,
            registry,
            self.APPLICATION_SHA,
        )

        self.assertEqual(snapshot["application_sha"], self.APPLICATION_SHA)
        self.assertEqual(
            snapshot["evidence_identity"],
            "bd87318aa60de73874e05f436ae6f84c66b4b56fd53612929d9cfe7cfa5c7427",
        )
        self.assertEqual(snapshot["as_of_block_number"], 25860867)
        self.assertEqual(
            snapshot["as_of_block_hash"],
            "0x806fc920f52b11cb56749e7786d176d7d2d21310f145184e83cc5ec5d882d75b",
        )
        self.assertEqual(snapshot["snapshot_sha256"], self.SNAPSHOT_SHA256)
        self.assertEqual(
            shib_v2_research.validate_research_snapshot(
                snapshot, evidence, registry
            ),
            snapshot,
        )
        self.assertEqual(
            expected,
            shib_v2_research.canonical_json_bytes(rebuilt) + b"\n",
        )

        hash_body = dict(snapshot)
        del hash_body["snapshot_sha256"]
        independent_sha256 = hashlib.sha256(
            b"shib-v2-research-snapshot/v1\n"
            + shib_v2_research.canonical_json_bytes(hash_body)
        ).hexdigest()
        self.assertEqual(independent_sha256, self.SNAPSHOT_SHA256)

    def test_real_scenarios_preserve_classification_and_missing_costs(self):
        _, _, snapshot = self._load_authorities_and_snapshot()
        expected_counts = {
            "non_positive_pool_edge": 10,
            "positive_pool_edge_costs_incomplete": 0,
            "unavailable": 0,
        }
        observed_counts = {classification: 0 for classification in expected_counts}
        missing_cost_fields = (
            "network_gas_usd",
            "router_or_integrator_fee_usd",
            "token_transfer_tax_usd",
            "mev_cost_usd",
            "atomic_execution_cost_usd",
            "net_edge_usd",
            "net_edge_bps",
        )
        edge_fields = (
            "buy_weth_raw",
            "sell_weth_raw",
            "gross_edge_weth_raw",
            "buy_cost_usd",
            "sell_proceeds_usd",
            "gross_edge_usd",
            "gross_edge_bps",
        )
        expected_limitations = [
            "network_gas_not_evaluated",
            "router_fee_not_evaluated",
            "token_transfer_tax_not_evaluated",
            "mev_not_evaluated",
            "atomic_route_simulation_unavailable",
        ]

        self.assertEqual(snapshot["scenario_count"], 10)
        self.assertEqual(len(snapshot["scenarios"]), 10)
        for row in snapshot["scenarios"]:
            observed_counts[row["classification"]] += 1
            self.assertFalse(row["strict_eligible"])
            self.assertFalse(row["executable"])
            self.assertNotIn("opportunity", row["classification"])
            for field in missing_cost_fields:
                self.assertIsNone(row[field], field)

            available = (
                row["buy_quote_status"] == "calculation_complete"
                and row["sell_quote_status"] == "calculation_complete"
            )
            if available:
                self.assertEqual(row["limitations"], expected_limitations)
                if row["gross_edge_weth_raw"] > 0:
                    self.assertEqual(
                        row["classification"],
                        "positive_pool_edge_costs_incomplete",
                    )
                else:
                    self.assertEqual(
                        row["classification"], "non_positive_pool_edge"
                    )
            else:
                self.assertEqual(row["classification"], "unavailable")
                for field in edge_fields:
                    self.assertIsNone(row[field], field)

        self.assertEqual(observed_counts, expected_counts)
        self.assertEqual(
            snapshot["summary"],
            {
                "expected_scenario_count": 10,
                "observed_scenario_count": 10,
                "usable_scenario_count": 10,
                "classification_counts": expected_counts,
                "strict_eligible_count": 0,
                "executable_count": 0,
                "missing_cost_field_count": 70,
            },
        )

    def test_repository_snapshot_is_public_safe(self):
        _, _, snapshot = self._load_authorities_and_snapshot()
        rendered = SNAPSHOT_PATH.read_text(encoding="utf-8")

        self.assertIsNone(shib_v2_research.scan_public_payload(snapshot))
        self.assertNotIn("http" + "://", rendered.lower())
        self.assertNotIn("wss" + "://", rendered.lower())
        self.assertNotRegex(rendered, r"\?[A-Za-z0-9_.%~-]+(?:=|%3[dD])")
        self.assertNotIn("@", rendered)
        for forbidden in (
            "authorization",
            "cookie",
            "api_key",
            "private_key",
            "provider_error",
            "raw_response",
            "raw_rpc",
            "rpc_url",
            "account",
            "wallet",
            "/Users" + "/",
            "/home" + "/",
            "/root" + "/",
            "/private" + "/",
            "/tmp" + "/",
        ):
            self.assertNotIn(forbidden.lower(), rendered.lower())


class ResearchEvidenceTests(unittest.TestCase):
    def setUp(self):
        registry, self.code_results, trust_anchor = (
            fixture_registry_and_code_results()
        )
        self.authority_patcher = mock.patch.object(
            shib_v2_research, "_AUTHORITY_TRUST_ANCHOR", trust_anchor
        )
        self.authority_patcher.start()
        self.addCleanup(self.authority_patcher.stop)
        self.registry = shib_v2_research.load_research_registry(registry)

    def test_inventory_is_closed_unique_and_registry_derived(self):
        try:
            calls = shib_v2_research.build_logical_call_inventory(self.registry)
        except AttributeError as error:
            self.fail("closed inventory is missing: {}".format(error))
        self.assertEqual(calls, _expected_logical_call_inventory(self.registry))
        self.assertEqual(len(calls), 35)
        self.assertEqual(len(calls), len({call["logical_call_id"] for call in calls}))
        self.assertEqual(
            {call["block_selector"] for call in calls},
            {"eip1898_block_hash_require_canonical"},
        )
        self.assertEqual(
            {call["method"] for call in calls},
            {
                "eth_getCode", "factory.getPair", "router.factory",
                "router.weth", "pair.factory", "pair.token0", "pair.token1",
                "pair.getReserves", "erc20.decimals", "erc20.balanceOf",
                "fee.totalFee", "fee.alpha", "fee.beta", "feed.decimals",
                "feed.description", "feed.latestRoundData",
            },
        )
        expected_selectors = {
            "eth_getCode": "0x",
            "factory.getPair": "0xe6a43905",
            "router.factory": "0xc45a0155",
            "router.weth": "0xad5c4648",
            "pair.factory": "0xc45a0155",
            "pair.token0": "0x0dfe1681",
            "pair.token1": "0xd21220a7",
            "pair.getReserves": "0x0902f1ac",
            "erc20.decimals": "0x313ce567",
            "erc20.balanceOf": "0x70a08231",
            "fee.totalFee": "0x1df4ccfc",
            "fee.alpha": "0xdb1d0fd5",
            "fee.beta": "0x9faa3c91",
            "feed.decimals": "0x313ce567",
            "feed.description": "0x7284e416",
            "feed.latestRoundData": "0xfeaf968c",
        }
        for call in calls:
            with self.subTest(method=call["method"], target=call["target"]):
                self.assertTrue(call["calldata"].startswith(
                    expected_selectors[call["method"]]
                ))
                self.assertEqual(
                    call["calldata_sha256"],
                    hashlib.sha256(bytes.fromhex(call["calldata"][2:])).hexdigest(),
                )
                self.assertEqual(
                    set(call),
                    {
                        "logical_call_id", "method", "target", "calldata",
                        "calldata_sha256", "block_selector",
                    },
                )

    def test_valid_evidence_recomputes_complete_quality_and_identity(self):
        try:
            payload = valid_evidence_payload(self.registry)
            evidence = shib_v2_research.validate_research_evidence(
                payload, self.registry
            )
        except AttributeError as error:
            self.fail("evidence validator is missing: {}".format(error))
        quality = evidence["collection_quality"]
        self.assertEqual(quality["state"], "evaluated")
        self.assertEqual(quality["expected_logical_call_count"], 35)
        self.assertEqual(quality["observed_logical_call_count"], 35)
        self.assertEqual(quality["usable_logical_call_count"], 35)
        self.assertEqual(quality["expected_provider_observation_count"], 70)
        self.assertEqual(quality["observed_provider_observation_count"], 70)
        self.assertEqual(quality["usable_provider_observation_count"], 70)
        self.assertEqual(quality["provider_disagreement_count"], 0)

    def test_fully_resealed_zero_canonical_block_hash_is_rejected(self):
        payload = _reseal_block(
            valid_evidence_payload(self.registry), "0x" + "00" * 32
        )
        with self.assertRaisesRegex(
            shib_v2_research.ResearchContractError,
            "^canonical block hash must be nonzero$",
        ):
            shib_v2_research.validate_research_evidence(payload, self.registry)

    def test_valid_fixture_is_independent_of_production_inventory_builder(self):
        with mock.patch.object(
            shib_v2_research,
            "build_logical_call_inventory",
            side_effect=AssertionError("fixture called production inventory"),
        ):
            try:
                payload = valid_evidence_payload(self.registry)
            except AssertionError as error:
                self.fail(str(error))
        self.assertEqual(len(payload["logical_calls"]), 35)

    def test_independent_oracle_detects_broken_production_inventory_identity(self):
        payload = valid_evidence_payload(self.registry)
        broken = _expected_logical_call_inventory(self.registry)
        broken[0] = dict(broken[0], logical_call_id="call:" + "f" * 64)
        with mock.patch.object(
            shib_v2_research,
            "build_logical_call_inventory",
            return_value=broken,
        ):
            with self.assertRaisesRegex(
                shib_v2_research.ResearchContractError,
                "^logical call set is not exact$",
            ):
                shib_v2_research.validate_research_evidence(payload, self.registry)

    def test_evidence_fails_closed_on_each_completeness_and_authority_break(self):
        mutations = (
            (remove_logical_call, "logical call set is not exact"),
            (add_unknown_call, "logical call set is not exact"),
            (duplicate_logical_key, "logical call set is not exact"),
            (remove_provider_observation, "provider observation set is not exact"),
            (disagree_provider_result, "provider results do not agree"),
            (change_block_hash, "provider observation block binding is invalid"),
            (change_factory_pair, "pool identity round trip is invalid"),
            (change_pair_factory, "pool identity round trip is invalid"),
            (change_router_factory, "pool identity round trip is invalid"),
            (change_router_weth, "pool identity round trip is invalid"),
            (change_token_order, "pool identity round trip is invalid"),
            (change_runtime_code_hash, "runtime code authority is invalid"),
            (change_balance, "pool balances do not equal reserves"),
            (change_shibaswap_fee, "ShibaSwap fee authority is invalid"),
            (stale_chainlink_round, "USD reference authority is invalid"),
            (future_chainlink_round, "USD reference authority is invalid"),
            (forge_quality_summary, "collection quality does not recompute"),
            (forge_evidence_identity, "evidence identity does not recompute"),
        )
        for mutation, message in mutations:
            with self.subTest(mutation=mutation.__name__):
                payload = mutation(valid_evidence_payload(self.registry))
                with self.assertRaisesRegex(
                    shib_v2_research.ResearchContractError,
                    "^{}$".format(message),
                ):
                    shib_v2_research.validate_research_evidence(
                        payload, self.registry
                    )

    def test_mutations_reseal_unrelated_identity_and_quality_dependencies(self):
        mutations = (
            remove_logical_call, add_unknown_call, duplicate_logical_key,
            remove_provider_observation, disagree_provider_result,
            change_block_hash, change_factory_pair, change_pair_factory,
            change_router_factory, change_router_weth, change_token_order,
            change_runtime_code_hash, change_balance, change_shibaswap_fee,
            stale_chainlink_round, future_chainlink_round,
            forge_quality_summary, forge_evidence_identity,
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation.__name__):
                payload = mutation(valid_evidence_payload(self.registry))
                recomputed_identity = _test_evidence_identity(payload)
                if mutation is forge_evidence_identity:
                    self.assertNotEqual(
                        payload["evidence_identity"], recomputed_identity
                    )
                else:
                    self.assertEqual(
                        payload["evidence_identity"], recomputed_identity
                    )
                recomputed_quality = _derive_test_quality(payload, 35)
                if mutation is forge_quality_summary:
                    self.assertNotEqual(
                        payload["collection_quality"], recomputed_quality
                    )
                else:
                    self.assertEqual(
                        payload["collection_quality"], recomputed_quality
                    )

    def test_measured_zero_is_preserved_but_missing_base_fee_fails(self):
        evidence = shib_v2_research.validate_research_evidence(
            valid_evidence_payload(self.registry), self.registry
        )
        self.assertIs(type(evidence["block"]["base_fee_per_gas"]), int)
        self.assertEqual(evidence["block"]["base_fee_per_gas"], 0)
        self.assertEqual(evidence["collection_quality"]["measured_zero_count"], 1)
        missing = valid_evidence_payload(self.registry)
        del missing["block"]["base_fee_per_gas"]
        missing["evidence_identity"] = _test_evidence_identity(missing)
        with self.assertRaisesRegex(
            shib_v2_research.ResearchContractError,
            "^evidence block schema is invalid$",
        ):
            shib_v2_research.validate_research_evidence(missing, self.registry)

    def test_abi_codec_rejects_noncanonical_and_oversized_results(self):
        self.assertEqual(
            shib_v2_research.abi_decode_result(
                "string", _result_hex(_abi_string("ETH / USD"))
            ),
            "ETH / USD",
        )
        malformed = (
            ("address", _result_hex(b"\x01" + b"\x00" * 31)),
            (
                "string",
                _result_hex(_abi_word(64) + _abi_word(0)),
            ),
            (
                "string",
                _result_hex(_abi_word(32) + _abi_word(1) + b"A" + b"\x01" * 31),
            ),
            (
                "string",
                _result_hex(_abi_string("ETH / USD") + b"\x00" * 32),
            ),
            (
                "string",
                _result_hex(
                    _abi_word(32) + _abi_word(4097) + b"A" * 4097 + b"\x00" * 31
                ),
            ),
            ("uint256", "0x"),
            ("uint256", "0x" + "00" * 65537),
            ("uint112_tuple", _result_hex(b"\x00" * 128)),
        )
        for kind, result_hex in malformed:
            with self.subTest(kind=kind, result_length=len(result_hex)):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.abi_decode_result(kind, result_hex)
        with self.assertRaises(shib_v2_research.ResearchContractError):
            shib_v2_research.abi_encode_call("feeTo()", ())
        with self.assertRaises(shib_v2_research.ResearchContractError):
            shib_v2_research.abi_encode_call(
                "balanceOf(address)",
                ("0x95AD61B0A150D79219DCF64E1E6CC01F0B64C4CE",),
            )

    def test_persisted_logical_calls_reject_process_only_block_selector(self):
        payload = valid_evidence_payload(self.registry)
        payload["logical_calls"][0]["block_selector"] = (
            "eip1898_block_hash_require_canonical"
        )
        with self.assertRaises(shib_v2_research.ResearchContractError):
            shib_v2_research.validate_research_evidence(payload, self.registry)

    def test_out_of_range_block_timestamp_fails_as_contract_error(self):
        payload = valid_evidence_payload(self.registry)
        payload["block"]["timestamp"] = 1 << 256
        header = {
            field: payload["block"][field]
            for field in (
                "number", "hash", "parent_hash", "timestamp", "state_root",
                "base_fee_per_gas",
            )
        }
        header_sha256 = hashlib.sha256(
            json.dumps(
                header,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        payload["block"]["canonical_header_sha256"] = header_sha256
        for observation in payload["block"]["provider_header_observations"]:
            observation["canonical_header_sha256"] = header_sha256
        try:
            shib_v2_research.validate_research_evidence(payload, self.registry)
        except shib_v2_research.ResearchContractError:
            return
        except Exception as error:
            self.fail("validator leaked non-contract error: {!r}".format(error))
        self.fail("out-of-range block timestamp was accepted")


class ResearchSnapshotTests(unittest.TestCase):
    def setUp(self):
        registry, _, trust_anchor = fixture_registry_and_code_results()
        self.authority_patcher = mock.patch.object(
            shib_v2_research, "_AUTHORITY_TRUST_ANCHOR", trust_anchor
        )
        self.authority_patcher.start()
        self.addCleanup(self.authority_patcher.stop)
        self.registry = shib_v2_research.load_research_registry(registry)
        self.evidence = valid_evidence_payload(self.registry)

    def test_snapshot_has_two_routes_five_notionals_and_no_executable_claim(self):
        snapshot = shib_v2_research.build_research_snapshot(
            self.evidence, self.registry, "1" * 40
        )
        self.assertEqual(snapshot["schema"], "shib_v2_research_snapshot/v1")
        self.assertEqual(snapshot["mode"], "historical_replay")
        self.assertEqual(snapshot["scenario_count"], 10)
        self.assertEqual(
            [
                (row["route_id"], row["requested_notional_usd"])
                for row in snapshot["scenarios"]
            ],
            expected_scenario_keys(),
        )
        for row in snapshot["scenarios"]:
            self.assertFalse(row["strict_eligible"])
            self.assertFalse(row["executable"])
            self.assertIsNone(row["network_gas_usd"])
            self.assertIsNone(row["net_edge_usd"])

    def test_positive_edge_is_cost_incomplete_not_opportunity(self):
        snapshot = build_from_reserves(positive_edge_reserves())
        row = next(
            item for item in snapshot["scenarios"]
            if item["route_id"] == expected_scenario_keys()[0][0]
            and item["requested_notional_usd"] == "1000"
        )
        self.assertEqual(row["common_shib_raw"], 5_000 * 10**18)
        self.assertEqual(row["buy_weth_raw"], 453622173051818773)
        self.assertEqual(row["sell_weth_raw"], 496027303890107812)
        self.assertEqual(row["gross_edge_weth_raw"], 42405130838289039)
        self.assertEqual(
            row["classification"], "positive_pool_edge_costs_incomplete"
        )
        self.assertEqual(row["reason_codes"], [
            "fixed_block_fee_proof_not_authenticated",
            "route_costs_not_evaluated",
        ])
        self.assertNotIn("opportunity", json.dumps(row))
        self.assertIsNone(row["net_edge_usd"])

    def test_v2_integer_rounding_oracles_and_token_order(self):
        shib = 1_000_000 * 10**18
        target = 5_000 * 10**18
        expected = {
            (90 * 10**18, "buy"): 453622173051818773,
            (90 * 10**18, "sell"): 446424573501097031,
            (100 * 10**18, "buy"): 504024636724243082,
            (100 * 10**18, "sell"): 496027303890107812,
        }
        for shib_is_token0 in (True, False):
            for (weth, direction), expected_raw in expected.items():
                with self.subTest(
                    shib_is_token0=shib_is_token0,
                    weth=weth,
                    direction=direction,
                ):
                    quote = v2_quote_oracle(
                        shib,
                        weth,
                        target,
                        direction,
                        shib_is_token0=shib_is_token0,
                    )
                    self.assertEqual(quote.status, "calculation_complete")
                    self.assertEqual(
                        shib_v2_research._quote_weth_raw(quote), expected_raw
                    )

    def test_exact_output_at_reserve_is_unavailable_not_zero(self):
        reserve_shib = 1_000_000 * 10**18
        quote = v2_quote_oracle(
            reserve_shib,
            90 * 10**18,
            reserve_shib,
            "buy",
        )
        self.assertEqual(quote.status, "unavailable")
        self.assertEqual(quote.reason_code, "pool_reserve_insufficient")
        self.assertIsNone(quote.gross_quote_quantity)

    def test_negative_and_measured_zero_edges_remain_distinct_exact_values(self):
        reverse = tuple(reversed(positive_edge_reserves()))
        negative = build_from_reserves(reverse)
        negative_row = negative["scenarios"][0]
        self.assertLess(negative_row["gross_edge_weth_raw"], 0)
        self.assertEqual(
            negative_row["classification"], "non_positive_pool_edge"
        )
        self.assertEqual(negative_row["reason_codes"], [
            "fixed_block_fee_proof_not_authenticated"
        ])

        reserve_shib = 1_000_000 * 10**18
        measured_zero = build_from_reserves((
            (reserve_shib, 90 * 10**18),
            (reserve_shib, 91535531406622513776),
        ))
        zero_row = measured_zero["scenarios"][0]
        self.assertEqual(zero_row["gross_edge_weth_raw"], 0)
        self.assertEqual(
            zero_row["gross_edge_usd"], {"numerator": 0, "denominator": 1}
        )
        self.assertEqual(
            zero_row["gross_edge_bps"], {"numerator": 0, "denominator": 1}
        )
        self.assertEqual(zero_row["classification"], "non_positive_pool_edge")

    def test_unavailable_scenario_keeps_quote_and_edge_values_null(self):
        reserve_shib = 1_000_000 * 10**18
        snapshot = build_from_reserves((
            (reserve_shib, 40 * 10**18),
            (reserve_shib, 40 * 10**18),
        ))
        row = snapshot["scenarios"][4]
        self.assertEqual(row["requested_notional_usd"], "100000")
        self.assertEqual(row["common_shib_raw"], 1_250_000 * 10**18)
        self.assertEqual(row["buy_quote_status"], "unavailable")
        self.assertEqual(row["buy_quote_reason"], "pool_reserve_insufficient")
        self.assertEqual(row["sell_quote_status"], "calculation_complete")
        self.assertEqual(
            row["sell_quote_reason"],
            "fixed_block_fee_proof_not_authenticated",
        )
        self.assertEqual(row["classification"], "unavailable")
        self.assertEqual(row["reason_codes"], [
            "pool_reserve_insufficient",
            "fixed_block_fee_proof_not_authenticated",
        ])
        for field in (
            "buy_weth_raw", "sell_weth_raw", "gross_edge_weth_raw",
            "buy_cost_usd", "sell_proceeds_usd", "gross_edge_usd",
            "gross_edge_bps", "network_gas_usd", "net_edge_usd",
        ):
            self.assertIsNone(row[field], field)
        self.assertGreater(
            snapshot["summary"]["classification_counts"]["unavailable"], 0
        )

    def test_complete_buy_and_unavailable_sell_keep_both_reasons_in_leg_order(self):
        reserve_shib = 1_000_000 * 10**18
        snapshot = build_from_reserves((
            (reserve_shib, 90 * 10**18),
            (reserve_shib, 1),
        ))
        row = snapshot["scenarios"][0]
        self.assertEqual(row["buy_quote_status"], "calculation_complete")
        self.assertEqual(row["sell_quote_status"], "unavailable")
        self.assertEqual(row["sell_quote_reason"], "pool_output_below_one_raw")
        self.assertEqual(row["reason_codes"], [
            "fixed_block_fee_proof_not_authenticated",
            "pool_output_below_one_raw",
        ])

    def test_both_unavailable_reasons_are_unique_in_buy_then_sell_order(self):
        reserve_shib = 1_000_000 * 10**18
        snapshot = build_from_reserves((
            (reserve_shib, 40 * 10**18),
            (reserve_shib, 1),
        ))
        row = snapshot["scenarios"][4]
        self.assertEqual(row["buy_quote_status"], "unavailable")
        self.assertEqual(row["sell_quote_status"], "unavailable")
        self.assertEqual(row["reason_codes"], [
            "pool_reserve_insufficient",
            "pool_output_below_one_raw",
        ])

        duplicate = v2_quote_oracle(
            reserve_shib,
            90 * 10**18,
            reserve_shib,
            "buy",
        )
        self.assertEqual(
            shib_v2_research._scenario_reason_codes(
                duplicate, duplicate, "unavailable"
            ),
            ["pool_reserve_insufficient"],
        )

    def test_same_inputs_are_byte_identical_and_mutation_changes_identity(self):
        first = shib_v2_research.build_research_snapshot(
            self.evidence, self.registry, "1" * 40
        )
        second = shib_v2_research.build_research_snapshot(
            self.evidence, self.registry, "1" * 40
        )
        self.assertEqual(
            shib_v2_research.canonical_json_bytes(first),
            shib_v2_research.canonical_json_bytes(second),
        )
        changed = mutate_reserve_and_rebind(self.evidence)
        self.assertNotEqual(
            shib_v2_research.build_research_snapshot(
                changed, self.registry, "1" * 40
            )["snapshot_sha256"],
            first["snapshot_sha256"],
        )

    def test_public_validator_requires_and_rebuilds_from_authorities(self):
        snapshot = shib_v2_research.build_research_snapshot(
            self.evidence, self.registry, "1" * 40
        )
        self.assertEqual(
            shib_v2_research.validate_research_snapshot(
                snapshot, self.evidence, self.registry
            ),
            snapshot,
        )
        with self.assertRaises(TypeError):
            shib_v2_research.validate_research_snapshot(snapshot)

        changed_evidence = mutate_reserve_and_rebind(self.evidence)
        with self.assertRaises(shib_v2_research.ResearchContractError):
            shib_v2_research.validate_research_snapshot(
                snapshot, changed_evidence, self.registry
            )

    def test_authority_validator_rejects_fully_resealed_forgeries(self):
        snapshot = shib_v2_research.build_research_snapshot(
            self.evidence, self.registry, "1" * 40
        )
        quote_forgery = copy.deepcopy(snapshot)
        row = quote_forgery["scenarios"][0]
        original_buy_cost = Fraction(
            row["buy_cost_usd"]["numerator"],
            row["buy_cost_usd"]["denominator"],
        )
        usd_per_weth_raw = original_buy_cost / row["buy_weth_raw"]
        row["buy_weth_raw"] = row["sell_weth_raw"] - 1
        row["gross_edge_weth_raw"] = 1
        for field, value in (
            ("buy_cost_usd", row["buy_weth_raw"] * usd_per_weth_raw),
            ("gross_edge_usd", usd_per_weth_raw),
            ("gross_edge_bps", Fraction(10000, row["buy_weth_raw"])),
        ):
            row[field] = {
                "numerator": value.numerator,
                "denominator": value.denominator,
            }
        row["classification"] = "positive_pool_edge_costs_incomplete"
        row["reason_codes"] = [
            "fixed_block_fee_proof_not_authenticated",
            "route_costs_not_evaluated",
        ]
        counts = quote_forgery["summary"]["classification_counts"]
        counts["non_positive_pool_edge"] -= 1
        counts["positive_pool_edge_costs_incomplete"] += 1
        quote_forgery = _reseal_snapshot(quote_forgery)

        block_forgery = copy.deepcopy(snapshot)
        block_forgery["as_of_block_number"] += 1
        block_forgery = _reseal_snapshot(block_forgery)

        call_forgery = copy.deepcopy(snapshot)
        call_forgery["pool_identities"][0]["call_results_sha256"] = "d" * 64
        call_forgery = _reseal_snapshot(call_forgery)

        fee_forgery = copy.deepcopy(snapshot)
        fee_forgery["pool_identities"][0]["fee_evidence_sha256"] = "e" * 64
        fee_forgery = _reseal_snapshot(fee_forgery)

        state_forgery = copy.deepcopy(snapshot)
        old_state = state_forgery["pool_identities"][0]["state_id"]
        new_state = "dex-v2-quantity:" + "f" * 64
        state_forgery["pool_identities"][0]["state_id"] = new_state
        for scenario in state_forgery["scenarios"]:
            for field in ("buy_pool_state_id", "sell_pool_state_id"):
                if scenario[field] == old_state:
                    scenario[field] = new_state
        state_forgery = _reseal_snapshot(state_forgery)

        for label, forgery in (
            ("quote_and_dependencies", quote_forgery),
            ("block", block_forgery),
            ("pool_call", call_forgery),
            ("fee", fee_forgery),
            ("state", state_forgery),
        ):
            with self.subTest(label=label):
                self.assertEqual(
                    shib_v2_research._validate_research_snapshot_structure(
                        forgery
                    ),
                    forgery,
                )
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.validate_research_snapshot(
                        forgery, self.evidence, self.registry
                    )

    def test_validator_rejects_forged_scenario_summary_and_self_hash(self):
        snapshot = shib_v2_research.build_research_snapshot(
            self.evidence, self.registry, "1" * 40
        )
        mutations = []

        forged_scenario = copy.deepcopy(snapshot)
        forged_scenario["scenarios"][0]["buy_weth_raw"] += 1
        mutations.append(_reseal_snapshot(forged_scenario))

        forged_summary = copy.deepcopy(snapshot)
        forged_summary["summary"]["usable_scenario_count"] -= 1
        mutations.append(_reseal_snapshot(forged_summary))

        forged_hash = copy.deepcopy(snapshot)
        forged_hash["snapshot_sha256"] = "f" * 64
        mutations.append(forged_hash)

        noncanonical_ratio = copy.deepcopy(snapshot)
        ratio = noncanonical_ratio["scenarios"][0]["buy_cost_usd"]
        ratio["numerator"] *= 2
        ratio["denominator"] *= 2
        mutations.append(_reseal_snapshot(noncanonical_ratio))

        forged_pool_reference = copy.deepcopy(snapshot)
        forged_pool_reference["scenarios"][0][
            "buy_pool_reference_shib_usd"
        ] = {"numerator": 1, "denominator": 1000}
        mutations.append(_reseal_snapshot(forged_pool_reference))

        forged_usd_conversion = copy.deepcopy(snapshot)
        for field in ("buy_cost_usd", "sell_proceeds_usd", "gross_edge_usd"):
            ratio = forged_usd_conversion["scenarios"][0][field]
            doubled = Fraction(ratio["numerator"] * 2, ratio["denominator"])
            forged_usd_conversion["scenarios"][0][field] = {
                "numerator": doubled.numerator,
                "denominator": doubled.denominator,
            }
        mutations.append(_reseal_snapshot(forged_usd_conversion))

        for mutation in mutations:
            with self.subTest(hash=mutation["snapshot_sha256"]):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.validate_research_snapshot(
                        mutation, self.evidence, self.registry
                    )

    def test_validator_rejects_arbitrary_dex_quote_reason_and_reason_order(self):
        positive = build_from_reserves(positive_edge_reserves())
        arbitrary_dex = copy.deepcopy(positive)
        arbitrary_dex["scenarios"][0]["buy_dex"] = "arbitrary_dex"

        arbitrary_complete_reason = copy.deepcopy(positive)
        arbitrary_complete_reason["scenarios"][0]["buy_quote_reason"] = (
            "arbitrary_quote_reason"
        )

        wrong_reason_order = copy.deepcopy(positive)
        wrong_reason_order["scenarios"][0]["reason_codes"].reverse()

        reserve_shib = 1_000_000 * 10**18
        unavailable = build_from_reserves((
            (reserve_shib, 40 * 10**18),
            (reserve_shib, 40 * 10**18),
        ))
        arbitrary_unavailable_reason = copy.deepcopy(unavailable)
        arbitrary_unavailable_reason["scenarios"][4]["buy_quote_reason"] = (
            "source_quote_asset_mismatch"
        )

        for mutation in (
            arbitrary_dex,
            arbitrary_complete_reason,
            wrong_reason_order,
            arbitrary_unavailable_reason,
        ):
            _reseal_snapshot(mutation)
            with self.subTest(reason=mutation["scenarios"][0]["reason_codes"]):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.validate_research_snapshot(
                        mutation, self.evidence, self.registry
                    )

    def test_unavailable_quote_reason_must_be_a_string(self):
        reserve_shib = 1_000_000 * 10**18
        snapshot = build_from_reserves((
            (reserve_shib, 40 * 10**18),
            (reserve_shib, 40 * 10**18),
        ))
        for adversarial_reason in (
            ["pool_reserve_insufficient"],
            {"reason": "pool_reserve_insufficient"},
        ):
            with self.subTest(reason_type=type(adversarial_reason).__name__):
                mutation = copy.deepcopy(snapshot)
                mutation["scenarios"][4]["buy_quote_reason"] = (
                    adversarial_reason
                )
                _reseal_snapshot(mutation)
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.validate_research_snapshot(
                        mutation, self.evidence, self.registry
                    )

    def test_application_sha_and_summary_are_strict_and_recomputed(self):
        with self.assertRaises(shib_v2_research.ResearchContractError):
            shib_v2_research.build_research_snapshot(
                self.evidence, self.registry, "A" * 40
            )
        snapshot = shib_v2_research.build_research_snapshot(
            self.evidence, self.registry, "1" * 40
        )
        self.assertEqual(snapshot["summary"], {
            "expected_scenario_count": 10,
            "observed_scenario_count": 10,
            "usable_scenario_count": 10,
            "classification_counts": {
                "non_positive_pool_edge": 10,
                "positive_pool_edge_costs_incomplete": 0,
                "unavailable": 0,
            },
            "strict_eligible_count": 0,
            "executable_count": 0,
            "missing_cost_field_count": 70,
        })
        self.assertEqual(
            snapshot["snapshot_sha256"],
            shib_v2_research.snapshot_sha256(snapshot),
        )


class ResearchBuildCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.external_cwd = self.root / "external"
        self.external_cwd.mkdir()
        self.registry_path = self.root / "registry.json"
        self.evidence_path = self.root / "evidence.json"
        self.output_path = self.root / "snapshot.json"
        registry, _, self.trust_anchor = fixture_registry_and_code_results()
        self.authority_patcher = mock.patch.object(
            shib_v2_research, "_AUTHORITY_TRUST_ANCHOR", self.trust_anchor
        )
        self.authority_patcher.start()
        self.addCleanup(self.authority_patcher.stop)
        self.registry = shib_v2_research.load_research_registry(registry)
        self.evidence = valid_evidence_payload(self.registry)
        shib_v2_research_io.atomic_write_canonical_json(
            self.registry_path, self.registry
        )
        shib_v2_research_io.atomic_write_canonical_json(
            self.evidence_path, self.evidence
        )
        self.site_directory = self.root / "site"
        self.site_directory.mkdir()
        (self.site_directory / "sitecustomize.py").write_text(
            "import builtins\n"
            "import sys\n"
            "_original_import = builtins.__import__\n"
            "def _patched_import(name, globals=None, locals=None, "
            "fromlist=(), level=0):\n"
            "    module = _original_import(name, globals, locals, fromlist, level)\n"
            "    target = sys.modules.get('scripts.shib_v2_research')\n"
            "    if target is not None:\n"
            "        target._AUTHORITY_TRUST_ANCHOR = {!r}\n"
            "    return module\n"
            "builtins.__import__ = _patched_import\n".format(
                self.trust_anchor
            ),
            encoding="utf-8",
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _fixture_environment(self):
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(self.site_directory)
        return environment

    def _run_cli(
        self,
        *,
        registry_path=None,
        evidence_path=None,
        application_sha="1" * 40,
        output_path=None,
        cwd=None,
    ):
        return subprocess.run(
            [
                sys.executable,
                str(BUILD_SCRIPT),
                "--registry",
                str(registry_path or self.registry_path),
                "--evidence",
                str(evidence_path or self.evidence_path),
                "--application-sha",
                application_sha,
                "--output",
                str(output_path or self.output_path),
            ],
            cwd=str(cwd or self.external_cwd),
            env=self._fixture_environment(),
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

    def test_cli_replays_without_network_from_external_working_directory(self):
        result = self._run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        snapshot = load_bounded_json(self.output_path, "snapshot")
        self.assertEqual(
            self.output_path.read_bytes(),
            shib_v2_research.canonical_json_bytes(
                shib_v2_research.validate_research_snapshot(
                    snapshot, self.evidence, self.registry
                )
            ) + b"\n",
        )

    def test_in_process_build_has_no_socket_or_urlopen_path(self):
        import importlib

        build_cli = importlib.import_module(
            "scripts.build_shib_v2_research_snapshot"
        )
        arguments = [
            str(BUILD_SCRIPT),
            "--registry",
            str(self.registry_path),
            "--evidence",
            str(self.evidence_path),
            "--application-sha",
            "1" * 40,
            "--output",
            str(self.output_path),
        ]
        with mock.patch.object(sys, "argv", arguments), mock.patch(
            "socket.socket", side_effect=AssertionError("network forbidden")
        ), mock.patch.object(
            urllib.request,
            "urlopen",
            side_effect=AssertionError("network forbidden"),
        ):
            self.assertEqual(build_cli.main(), 0)
        snapshot = load_bounded_json(self.output_path, "snapshot")
        self.assertEqual(
            shib_v2_research.validate_research_snapshot(
                snapshot, self.evidence, self.registry
            ),
            snapshot,
        )

    def test_missing_evidence_is_not_evaluated_and_never_writes_output(self):
        missing = self.root / "missing-evidence.json"
        for original in (None, b"old-output\n"):
            with self.subTest(original=original):
                self.output_path.unlink(missing_ok=True)
                if original is not None:
                    self.output_path.write_bytes(original)
                result = self._run_cli(evidence_path=missing)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertEqual(result.stderr, "evidence_not_evaluated\n")
                if original is None:
                    self.assertFalse(self.output_path.exists())
                else:
                    self.assertEqual(self.output_path.read_bytes(), original)

    def test_invalid_present_inputs_and_sha_preserve_absent_or_prior_output(self):
        evidence_symlink = self.root / "evidence-symlink.json"
        evidence_symlink.symlink_to(self.evidence_path)
        oversized_evidence = self.root / "evidence-oversized.json"
        oversized_evidence.write_bytes(
            b" " * (shib_v2_research_io.MAX_JSON_BYTES + 1)
        )
        noncanonical_evidence = self.root / "evidence-noncanonical.json"
        noncanonical_evidence.write_text('{"schema": "invalid"}\n', encoding="utf-8")
        cases = (
            ("symlink", evidence_symlink, "1" * 40),
            ("oversized", oversized_evidence, "1" * 40),
            ("noncanonical", noncanonical_evidence, "1" * 40),
            ("uppercase_sha", self.evidence_path, "A" * 40),
            ("short_sha", self.evidence_path, "1" * 39),
        )
        for label, evidence_path, application_sha in cases:
            for original in (None, b"old-output\n"):
                with self.subTest(label=label, original=original):
                    self.output_path.unlink(missing_ok=True)
                    if original is not None:
                        self.output_path.write_bytes(original)
                    result = self._run_cli(
                        evidence_path=evidence_path,
                        application_sha=application_sha,
                    )
                    self.assertEqual(result.returncode, 1)
                    self.assertEqual(result.stdout, "")
                    self.assertEqual(result.stderr, "evidence_failed\n")
                    if original is None:
                        self.assertFalse(self.output_path.exists())
                    else:
                        self.assertEqual(self.output_path.read_bytes(), original)

    def test_invalid_registry_preserves_absent_or_prior_output(self):
        invalid_registry = self.root / "registry-invalid.json"
        invalid_registry.write_text('{"schema": "invalid"}\n', encoding="utf-8")
        for original in (None, b"old-output\n"):
            with self.subTest(original=original):
                self.output_path.unlink(missing_ok=True)
                if original is not None:
                    self.output_path.write_bytes(original)
                result = self._run_cli(registry_path=invalid_registry)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(result.stderr, "registry_invalid\n")
                if original is None:
                    self.assertFalse(self.output_path.exists())
                else:
                    self.assertEqual(self.output_path.read_bytes(), original)

    def test_help_works_from_repository_and_external_working_directories(self):
        for cwd in (PROJECT_ROOT, self.external_cwd):
            with self.subTest(cwd=cwd):
                result = subprocess.run(
                    [sys.executable, str(BUILD_SCRIPT), "--help"],
                    cwd=str(cwd),
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--registry REGISTRY", result.stdout)
                self.assertIn("--evidence EVIDENCE", result.stdout)
                self.assertIn("--application-sha APPLICATION_SHA", result.stdout)
                self.assertIn("--output OUTPUT", result.stdout)

    def test_public_artifacts_exclude_private_inputs_and_forbidden_imports(self):
        result = self._run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        rendered = self.output_path.read_text(encoding="utf-8")
        slash = chr(47)
        for forbidden in (
            "https://",
            slash + "Users" + slash,
            slash + "private" + slash,
            "rpc_url",
            "cookie",
            "authorization",
        ):
            self.assertNotIn(forbidden, rendered.lower())
        for source_path in (
            PROJECT_ROOT / "scripts/shib_v2_research.py",
            BUILD_SCRIPT,
        ):
            source = source_path.read_text(encoding="utf-8")
            for forbidden in (
                "fetch_cex", "usdt", "uniswap_v3", "connector", "dashboard",
            ):
                self.assertNotIn(forbidden, source.lower())


class SafeJsonBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_bounded_loader_rejects_duplicate_json_keys_and_symlink(self):
        duplicate = self.root / "duplicate.json"
        duplicate.write_text('{"schema":"a","schema":"b"}', encoding="utf-8")
        with self.assertRaisesRegex(
            shib_v2_research.ResearchContractError, "duplicate JSON key"
        ):
            load_bounded_json(duplicate, "registry")
        link = self.root / "link.json"
        link.symlink_to(duplicate)
        with self.assertRaisesRegex(
            shib_v2_research.ResearchContractError, "regular file"
        ):
            load_bounded_json(link, "registry")

    def test_bounded_loader_accepts_canonical_repository_registry(self):
        registry = load_bounded_json(REGISTRY_PATH, "repository registry")
        self.assertEqual(registry["tokens"]["SHIB"]["address"], (
            "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce"
        ))

    def test_public_scan_rejects_url_secret_private_path_and_provider_error(self):
        slash = chr(47)
        for value in (
            "https://rpc.example/key",
            "sk-live-secretmaterial",
            slash + "Users" + slash + "private" + slash + "research",
            {"provider_error": "arbitrary text"},
        ):
            with self.subTest(value=value):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.scan_public_payload({"value": value})

    def test_public_scan_rejects_private_paths_secret_and_key_aliases(self):
        for payload in (
            {"value": "/root/research"},
            {"value": "/etc/passwd"},
            {"value": "ghp_abcdefghijklmnopqrstuvwxyz1234567890"},
            {"privatePath": "hidden"},
            {"providerError": "hidden"},
            {"raw_payload": "hidden"},
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.scan_public_payload(payload)

    def test_public_scan_keeps_legitimate_token_and_evm_address_fields(self):
        self.assertIsNone(shib_v2_research.scan_public_payload({
            "token": "SHIB",
            "address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
            "description": "ETH / USD",
        }))

    def test_bounded_loader_rejects_float_exponent_and_nonfinite_tokens(self):
        for token in ("1.0", "1e3", "NaN", "Infinity", "-Infinity"):
            path = self.root / "numeric.json"
            path.write_text('{"value":' + token + '}', encoding="utf-8")
            with self.subTest(token=token):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    load_bounded_json(path, "numeric fixture")

    def test_bounded_loader_rejects_size_and_parser_bounds(self):
        cases = (
            ("size", b" " * (shib_v2_research_io.MAX_JSON_BYTES + 1)),
            ("nesting", b'{"value":' + b"[" * 65 + b"0" + b"]" * 65 + b"}\n"),
            (
                "members",
                b"{" + b",".join(
                    b'"k%05d":0' % index for index in range(4097)
                ) + b"}\n",
            ),
            (
                "string",
                b'{"value":"' + b"a" * (
                    shib_v2_research_io.MAX_JSON_STRING_TOKEN_BYTES + 1
                ) + b'"}\n',
            ),
            (
                "integer",
                b'{"value":' + b"1" * (
                    shib_v2_research_io.MAX_JSON_INTEGER_TOKEN_BYTES + 1
                ) + b"}\n",
            ),
        )
        path = self.root / "bounded.json"
        for label, raw in cases:
            with self.subTest(label=label):
                path.write_bytes(raw)
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    load_bounded_json(path, label)

    def test_bounded_loader_requires_canonical_bytes_and_writer_round_trips(self):
        from scripts.shib_v2_research_io import atomic_write_canonical_json

        path = self.root / "registry.json"
        path.write_text('{"b":1,"a":2}\n', encoding="utf-8")
        with self.assertRaises(shib_v2_research.ResearchContractError):
            load_bounded_json(path, "registry")
        atomic_write_canonical_json(path, {"b": 1, "a": 2})
        self.assertEqual(path.read_bytes(), b'{"a":2,"b":1}\n')
        self.assertEqual(load_bounded_json(path, "registry"), {"a": 2, "b": 1})

    def test_atomic_writer_preserves_existing_file_on_rejected_payload(self):
        from scripts.shib_v2_research_io import atomic_write_canonical_json

        path = self.root / "registry.json"
        original = b'{"kept":1}\n'
        path.write_bytes(original)
        rejected_payloads = (
            {"value": "a" * (shib_v2_research_io.MAX_JSON_BYTES + 1)},
            {"value": "a" * (
                shib_v2_research_io.MAX_JSON_STRING_TOKEN_BYTES + 1
            )},
        )
        for payload in rejected_payloads:
            with self.subTest(length=len(payload["value"])):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    atomic_write_canonical_json(path, payload)
                self.assertEqual(path.read_bytes(), original)

    def test_atomic_writer_precommit_and_replace_failures_preserve_prior_state(self):
        from scripts.shib_v2_research_io import atomic_write_canonical_json

        path = self.root / "registry.json"
        real_fsync = shib_v2_research_io.os.fsync
        real_replace = shib_v2_research_io.os.replace
        real_stat = shib_v2_research_io.os.stat
        real_recheck = shib_v2_research_io._recheck_directory_chain
        payload = {"new": 2}

        for failure in (
            "serialization",
            "directory_open",
            "target_check",
            "temp_write",
            "file_fsync",
            "precommit_directory_fsync",
            "ancestor_recheck",
            "target_recheck",
            "replace",
        ):
            for original in (None, b'{"old":1}\n'):
                with self.subTest(failure=failure, original=original):
                    path.unlink(missing_ok=True)
                    if original is not None:
                        path.write_bytes(original)
                    state = {"replace_calls": 0, "target_stats": 0}

                    def record_or_fail_replace(*args, **kwargs):
                        state["replace_calls"] += 1
                        if failure == "replace":
                            raise OSError("injected replace failure")
                        return real_replace(*args, **kwargs)

                    def fail_selected_fsync(descriptor):
                        metadata = os.fstat(descriptor)
                        if failure == "file_fsync" and stat.S_ISREG(metadata.st_mode):
                            raise OSError("injected file fsync failure")
                        if (
                            failure == "precommit_directory_fsync"
                            and stat.S_ISDIR(metadata.st_mode)
                        ):
                            raise OSError("injected precommit directory fsync failure")
                        return real_fsync(descriptor)

                    def fail_selected_stat(name, *args, **kwargs):
                        if name == path.name and kwargs.get("dir_fd") is not None:
                            state["target_stats"] += 1
                            if failure == "target_check":
                                raise OSError("injected target check failure")
                            if failure == "target_recheck" and state["target_stats"] == 2:
                                raise OSError("injected target recheck failure")
                        return real_stat(name, *args, **kwargs)

                    def fail_selected_recheck(directory, expected):
                        if failure == "ancestor_recheck":
                            raise shib_v2_research.ResearchContractError(
                                "injected ancestor recheck failure"
                            )
                        return real_recheck(directory, expected)

                    serialization_patcher = mock.patch.object(
                        shib_v2_research_io,
                        "canonical_json_bytes",
                        side_effect=shib_v2_research.ResearchContractError(
                            "injected serialization failure"
                        ),
                    ) if failure == "serialization" else mock.patch.object(
                        shib_v2_research_io,
                        "canonical_json_bytes",
                        wraps=shib_v2_research_io.canonical_json_bytes,
                    )
                    directory_patcher = mock.patch.object(
                        shib_v2_research_io,
                        "_open_directory_chain",
                        side_effect=shib_v2_research.ResearchContractError(
                            "injected directory open failure"
                        ),
                    ) if failure == "directory_open" else mock.patch.object(
                        shib_v2_research_io,
                        "_open_directory_chain",
                        wraps=shib_v2_research_io._open_directory_chain,
                    )
                    write_patcher = mock.patch.object(
                        shib_v2_research_io,
                        "_write_all",
                        side_effect=OSError("injected staged write failure"),
                    ) if failure == "temp_write" else mock.patch.object(
                        shib_v2_research_io,
                        "_write_all",
                        wraps=shib_v2_research_io._write_all,
                    )
                    with serialization_patcher, directory_patcher, write_patcher, mock.patch.object(
                        shib_v2_research_io.os,
                        "fsync",
                        side_effect=fail_selected_fsync,
                    ), mock.patch.object(
                        shib_v2_research_io.os,
                        "stat",
                        side_effect=fail_selected_stat,
                    ), mock.patch.object(
                        shib_v2_research_io,
                        "_recheck_directory_chain",
                        side_effect=fail_selected_recheck,
                    ), mock.patch.object(
                        shib_v2_research_io.os,
                        "replace",
                        side_effect=record_or_fail_replace,
                    ):
                        with self.assertRaises(
                            shib_v2_research.ResearchContractError
                        ):
                            atomic_write_canonical_json(path, payload)
                    if original is None:
                        self.assertFalse(path.exists())
                    else:
                        self.assertEqual(path.read_bytes(), original)
                    expected_replace_calls = 1 if failure == "replace" else 0
                    self.assertEqual(
                        state["replace_calls"], expected_replace_calls
                    )
                    leaked = [
                        child.name
                        for child in self.root.iterdir()
                        if child.name.startswith("." + path.name + ".")
                    ]
                    self.assertEqual(leaked, [])

        real_target = self.root / "real.json"
        original = b'{"kept":1}\n'
        real_target.write_bytes(original)
        path.unlink(missing_ok=True)
        path.symlink_to(real_target)
        with self.assertRaises(shib_v2_research.ResearchContractError):
            atomic_write_canonical_json(path, payload)
        self.assertTrue(path.is_symlink())
        self.assertEqual(real_target.read_bytes(), original)

    def test_atomic_writer_has_one_explicit_nonraising_postcommit_tail(self):
        helper = getattr(shib_v2_research_io, "_commit_staged_json", None)
        self.assertIsNotNone(helper, "single commit helper is missing")
        function = ast.parse(inspect.getsource(helper)).body[0]
        self.assertEqual(len(function.body), 2)
        replace_try, postcommit_try = function.body
        self.assertIsInstance(replace_try, ast.Try)
        replace_call = replace_try.body[0].value.func
        self.assertIsInstance(replace_call, ast.Attribute)
        self.assertEqual(replace_call.attr, "replace")
        self.assertEqual(replace_try.handlers[0].type.id, "OSError")
        self.assertTrue(any(
            isinstance(node, ast.Raise)
            for node in ast.walk(replace_try.handlers[0])
        ))
        self.assertIsInstance(postcommit_try, ast.Try)
        postcommit_call = postcommit_try.body[0].value.func
        self.assertIsInstance(postcommit_call, ast.Attribute)
        self.assertEqual(postcommit_call.attr, "fsync")
        self.assertEqual(postcommit_try.handlers[0].type.id, "BaseException")
        self.assertFalse(any(
            isinstance(node, ast.Raise) for node in ast.walk(postcommit_try)
        ))
        writer_source = inspect.getsource(
            shib_v2_research_io.atomic_write_canonical_json
        )
        self.assertNotIn("rollback", writer_source)
        self.assertNotIn("backup", writer_source)
