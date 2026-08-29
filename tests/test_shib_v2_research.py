"""Contracts for the bounded SHIB V2/V2 research registry."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import shib_v2_research
from scripts import shib_v2_research_io
from scripts.shib_v2_research_io import load_bounded_json


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = PROJECT_ROOT / "config/shib_v2_research_pools.json"


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
