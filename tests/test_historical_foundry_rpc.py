from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping
from contextlib import nullcontext
from decimal import Decimal
import gzip
import hashlib
import hmac
import importlib
import inspect
import json
import linecache
import math
import os
import pickle
import signal
import subprocess
import sys
import tempfile
import threading
import time
from types import MappingProxyType
import unittest
from unittest import mock

import scripts.bootstrap_historical_foundry_toolchain as foundry_toolchain
import scripts.historical_foundry_rpc as rpc
from scripts.historical_foundry_contracts import (
    load_historical_foundry_authority,
    load_historical_foundry_config_set,
    load_historical_foundry_policy,
)
from scripts.historical_foundry_rpc import (
    _archive_rpc_test_batch_for_test,
    _close_archive_rpc_test_run_for_test,
    _issue_archive_rpc_test_preflight_for_test,
    _issue_archive_rpc_test_run_for_test,
    _make_archive_rpc_test_response_for_test,
    _materialize_historical_anchor_stage,
    _open_archive_rpc_test_logical_batch_for_test,
    _open_production_archive_rpc_logical_batch,
    _open_production_archive_rpc_run,
    _production_archive_rpc_batch,
    _project_archive_rpc_test_finalization_for_test,
    _finalize_production_archive_rpc_run,
    _validate_historical_anchor_capture,
    build_historical_anchor_request_plan,
    project_historical_anchor_capture,
)


UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
EXECUTOR = "0x68778b870ceee58d82ba9f97cb4219981fdafa72"
SENDER = "0x5ca9e6c3ed27cc0acfb355061fcab6964d4fc444"
UNI_ROUTER = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
UNI_FACTORY = "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"
SUSHI_ROUTER = "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f"
SUSHI_FACTORY = "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac"
FEED_PROXY = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
UNI_PAIR = "0x1111111111111111111111111111111111111111"
SUSHI_PAIR = "0x2222222222222222222222222222222222222222"
AGGREGATOR = "0x3333333333333333333333333333333333333333"

ANCHOR_HASH = "0x" + "aa" * 32
PARENT_HASH = "0x" + "bb" * 32
STATE_ROOT = "0x" + "cc" * 32
ANCHOR_REFERENCE = {"blockHash": ANCHOR_HASH, "requireCanonical": True}
ZERO_WORD = "0x" + "00" * 32

UNI_BALANCE_KEY = (
    "0x7101778461add6fd4a03a1ab6e1e71f38171a035038a1417d87528facb82b2ca"
)
WETH_BALANCE_KEY = (
    "0x2a21a6c263221a75b9c3271c8cd170c1acead6333ac5a2f1396a52b447acf7d9"
)
UNI_UNI_ALLOWANCE_KEY = (
    "0x24c191bbe5f5284a8318081b3231f130445c513886390ac58e14483a270b8590"
)
UNI_SUSHI_ALLOWANCE_KEY = (
    "0x23735554112a362a63c60262bd1047889eccbc69c39bf5df101690b5d7a3d089"
)
WETH_UNI_ALLOWANCE_KEY = (
    "0x782f604af838722982220f20b4649c36abbef573fe582c49375a8829c17891e9"
)
WETH_SUSHI_ALLOWANCE_KEY = (
    "0xfa119f8599857cc89116e4ff1001428e155c391c7d455150afb87334b076d1b0"
)


def _canonical_bytes(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _typed_hash(domain, value):
    return hashlib.sha256(domain + b"\0" + _canonical_bytes(value)).hexdigest()


def _closure_named_value(function, name):
    pending = [function]
    visited = set()
    while pending:
        candidate = pending.pop()
        if id(candidate) in visited:
            continue
        visited.add(id(candidate))
        closure = getattr(candidate, "__closure__", None) or ()
        code = getattr(candidate, "__code__", None)
        if code is None:
            continue
        values = dict(zip(code.co_freevars, closure))
        if name in values:
            return values[name].cell_contents
        for cell in closure:
            value = cell.cell_contents
            if callable(value) and hasattr(value, "__code__"):
                pending.append(value)
    raise AssertionError("missing closure value: {}".format(name))


def _closure_function(function, name):
    value = _closure_named_value(function, name)
    if not callable(value):
        raise AssertionError("closure value is not callable: {}".format(name))
    return value


def _run_genuine_historical_window_scheduler():
    from tests.test_historical_foundry_scan import (
        HistoricalFoundryScanTask3bIntegratedTests,
    )

    name = "test_scheduler_owns_complete_offline_run_through_capability_delivery"
    case = HistoricalFoundryScanTask3bIntegratedTests(methodName=name)
    return getattr(case, name)()


def _word(value):
    return "0x" + format(value, "064x")


def _address_word(address):
    return "0x" + "0" * 24 + address[2:]


def _address_argument(address):
    return "0" * 24 + address[2:]


def _call(to, data):
    return {"to": to, "data": data}


def _success(request_id, result):
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _abi_string(value):
    payload = value.encode("utf-8")
    padded = payload + b"\0" * ((-len(payload)) % 32)
    return "0x" + (32).to_bytes(32, "big").hex() + (
        len(payload).to_bytes(32, "big").hex()
    ) + padded.hex()


def _round_data(round_id, answer, started_at, updated_at, answered_in_round):
    words = []
    for value in (round_id, answer, started_at, updated_at, answered_in_round):
        if value < 0:
            value += 1 << 256
        words.append(value.to_bytes(32, "big").hex())
    return "0x" + "".join(words)


def _synthetic_responses():
    header = {
        "number": "0x100",
        "hash": ANCHOR_HASH,
        "parentHash": PARENT_HASH,
        "stateRoot": STATE_ROOT,
        "timestamp": "0x65",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x100",
        "baseFeePerGas": "0x1",
    }
    results = {
        1: "0x1",
        2: header,
        3: "0x6003",
        4: _word(18),
        5: ZERO_WORD,
        6: ZERO_WORD,
        7: ZERO_WORD,
        8: ZERO_WORD,
        9: ZERO_WORD,
        10: ZERO_WORD,
        11: "0x6011",
        12: _word(18),
        13: ZERO_WORD,
        14: ZERO_WORD,
        15: ZERO_WORD,
        16: ZERO_WORD,
        17: ZERO_WORD,
        18: ZERO_WORD,
        19: "0x6019",
        20: _address_word(UNI_FACTORY),
        21: _address_word(WETH),
        22: "0x6022",
        23: _address_word(UNI_PAIR),
        24: _address_word(UNI_PAIR),
        25: "0x6025",
        26: _address_word(SUSHI_FACTORY),
        27: _address_word(WETH),
        28: "0x6028",
        29: _address_word(SUSHI_PAIR),
        30: _address_word(SUSHI_PAIR),
        31: "0x6031",
        32: _abi_string("ETH / USD"),
        33: _word(8),
        34: _address_word(AGGREGATOR),
        35: _word(7),
        36: _round_data((7 << 64) + 42, 300_000_000_000, 80, 100,
                        (7 << 64) + 42),
        37: "0x",
        38: "0x0",
        39: "0x0",
        40: "0x6040",
        41: _address_word(UNI_FACTORY),
        42: _address_word(UNI),
        43: _address_word(WETH),
        44: "0x6044",
        45: _address_word(SUSHI_FACTORY),
        46: _address_word(WETH),
        47: _address_word(UNI),
        48: "0x6048",
    }
    return [_success(request_id, results[request_id]) for request_id in range(1, 49)]


def _expected_anchor_rows():
    return [
        {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
        {
            "jsonrpc": "2.0", "id": 2, "method": "eth_getBlockByNumber",
            "params": ["finalized", False],
        },
    ]


def _expected_fixed_rows():
    balance = "0x70a08231" + _address_argument(EXECUTOR)
    uni_allowance = (
        "0xdd62ed3e" + _address_argument(EXECUTOR)
        + _address_argument(UNI_ROUTER)
    )
    sushi_allowance = (
        "0xdd62ed3e" + _address_argument(EXECUTOR)
        + _address_argument(SUSHI_ROUTER)
    )
    get_pair_forward = (
        "0xe6a43905" + _address_argument(UNI) + _address_argument(WETH)
    )
    get_pair_reverse = (
        "0xe6a43905" + _address_argument(WETH) + _address_argument(UNI)
    )
    return [
        {"jsonrpc": "2.0", "id": 3, "method": "eth_getCode", "params": [UNI, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 4, "method": "eth_call", "params": [_call(UNI, "0x313ce567"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 5, "method": "eth_call", "params": [_call(UNI, balance), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 6, "method": "eth_getStorageAt", "params": [UNI, UNI_BALANCE_KEY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 7, "method": "eth_call", "params": [_call(UNI, uni_allowance), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 8, "method": "eth_getStorageAt", "params": [UNI, UNI_UNI_ALLOWANCE_KEY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 9, "method": "eth_call", "params": [_call(UNI, sushi_allowance), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 10, "method": "eth_getStorageAt", "params": [UNI, UNI_SUSHI_ALLOWANCE_KEY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 11, "method": "eth_getCode", "params": [WETH, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 12, "method": "eth_call", "params": [_call(WETH, "0x313ce567"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 13, "method": "eth_call", "params": [_call(WETH, balance), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 14, "method": "eth_getStorageAt", "params": [WETH, WETH_BALANCE_KEY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 15, "method": "eth_call", "params": [_call(WETH, uni_allowance), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 16, "method": "eth_getStorageAt", "params": [WETH, WETH_UNI_ALLOWANCE_KEY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 17, "method": "eth_call", "params": [_call(WETH, sushi_allowance), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 18, "method": "eth_getStorageAt", "params": [WETH, WETH_SUSHI_ALLOWANCE_KEY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 19, "method": "eth_getCode", "params": [UNI_ROUTER, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 20, "method": "eth_call", "params": [_call(UNI_ROUTER, "0xc45a0155"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 21, "method": "eth_call", "params": [_call(UNI_ROUTER, "0xad5c4648"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 22, "method": "eth_getCode", "params": [UNI_FACTORY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 23, "method": "eth_call", "params": [_call(UNI_FACTORY, get_pair_forward), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 24, "method": "eth_call", "params": [_call(UNI_FACTORY, get_pair_reverse), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 25, "method": "eth_getCode", "params": [SUSHI_ROUTER, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 26, "method": "eth_call", "params": [_call(SUSHI_ROUTER, "0xc45a0155"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 27, "method": "eth_call", "params": [_call(SUSHI_ROUTER, "0xad5c4648"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 28, "method": "eth_getCode", "params": [SUSHI_FACTORY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 29, "method": "eth_call", "params": [_call(SUSHI_FACTORY, get_pair_forward), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 30, "method": "eth_call", "params": [_call(SUSHI_FACTORY, get_pair_reverse), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 31, "method": "eth_getCode", "params": [FEED_PROXY, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 32, "method": "eth_call", "params": [_call(FEED_PROXY, "0x7284e416"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 33, "method": "eth_call", "params": [_call(FEED_PROXY, "0x313ce567"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 34, "method": "eth_call", "params": [_call(FEED_PROXY, "0x245a7bfc"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 35, "method": "eth_call", "params": [_call(FEED_PROXY, "0x58303b10"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 36, "method": "eth_call", "params": [_call(FEED_PROXY, "0xfeaf968c"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 37, "method": "eth_getCode", "params": [EXECUTOR, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 38, "method": "eth_getTransactionCount", "params": [EXECUTOR, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 39, "method": "eth_getTransactionCount", "params": [SENDER, ANCHOR_REFERENCE]},
    ]


def _expected_derived_rows():
    return [
        {"jsonrpc": "2.0", "id": 40, "method": "eth_getCode", "params": [UNI_PAIR, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 41, "method": "eth_call", "params": [_call(UNI_PAIR, "0xc45a0155"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 42, "method": "eth_call", "params": [_call(UNI_PAIR, "0x0dfe1681"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 43, "method": "eth_call", "params": [_call(UNI_PAIR, "0xd21220a7"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 44, "method": "eth_getCode", "params": [SUSHI_PAIR, ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 45, "method": "eth_call", "params": [_call(SUSHI_PAIR, "0xc45a0155"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 46, "method": "eth_call", "params": [_call(SUSHI_PAIR, "0x0dfe1681"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 47, "method": "eth_call", "params": [_call(SUSHI_PAIR, "0xd21220a7"), ANCHOR_REFERENCE]},
        {"jsonrpc": "2.0", "id": 48, "method": "eth_getCode", "params": [AGGREGATOR, ANCHOR_REFERENCE]},
    ]


ROLE_METHODS = (
    (1, "chain_id", "eth_chainId"),
    (2, "finalized_anchor", "eth_getBlockByNumber"),
    (3, "uni_runtime", "eth_getCode"),
    (4, "uni_decimals", "eth_call"),
    (5, "uni_executor_balance_getter", "eth_call"),
    (6, "uni_executor_balance_storage", "eth_getStorageAt"),
    (7, "uni_uniswap_v2_allowance_getter", "eth_call"),
    (8, "uni_uniswap_v2_allowance_storage", "eth_getStorageAt"),
    (9, "uni_sushiswap_v2_allowance_getter", "eth_call"),
    (10, "uni_sushiswap_v2_allowance_storage", "eth_getStorageAt"),
    (11, "weth_runtime", "eth_getCode"),
    (12, "weth_decimals", "eth_call"),
    (13, "weth_executor_balance_getter", "eth_call"),
    (14, "weth_executor_balance_storage", "eth_getStorageAt"),
    (15, "weth_uniswap_v2_allowance_getter", "eth_call"),
    (16, "weth_uniswap_v2_allowance_storage", "eth_getStorageAt"),
    (17, "weth_sushiswap_v2_allowance_getter", "eth_call"),
    (18, "weth_sushiswap_v2_allowance_storage", "eth_getStorageAt"),
    (19, "uniswap_v2_router_runtime", "eth_getCode"),
    (20, "uniswap_v2_router_factory", "eth_call"),
    (21, "uniswap_v2_router_weth", "eth_call"),
    (22, "uniswap_v2_factory_runtime", "eth_getCode"),
    (23, "uniswap_v2_pair_forward", "eth_call"),
    (24, "uniswap_v2_pair_reverse", "eth_call"),
    (25, "sushiswap_v2_router_runtime", "eth_getCode"),
    (26, "sushiswap_v2_router_factory", "eth_call"),
    (27, "sushiswap_v2_router_weth", "eth_call"),
    (28, "sushiswap_v2_factory_runtime", "eth_getCode"),
    (29, "sushiswap_v2_pair_forward", "eth_call"),
    (30, "sushiswap_v2_pair_reverse", "eth_call"),
    (31, "chainlink_proxy_runtime", "eth_getCode"),
    (32, "chainlink_description", "eth_call"),
    (33, "chainlink_decimals", "eth_call"),
    (34, "chainlink_aggregator", "eth_call"),
    (35, "chainlink_phase", "eth_call"),
    (36, "chainlink_latest_round", "eth_call"),
    (37, "executor_prior_runtime", "eth_getCode"),
    (38, "executor_prior_nonce", "eth_getTransactionCount"),
    (39, "sender_prior_nonce", "eth_getTransactionCount"),
    (40, "uniswap_v2_pair_runtime", "eth_getCode"),
    (41, "uniswap_v2_pair_factory", "eth_call"),
    (42, "uniswap_v2_pair_token0", "eth_call"),
    (43, "uniswap_v2_pair_token1", "eth_call"),
    (44, "sushiswap_v2_pair_runtime", "eth_getCode"),
    (45, "sushiswap_v2_pair_factory", "eth_call"),
    (46, "sushiswap_v2_pair_token0", "eth_call"),
    (47, "sushiswap_v2_pair_token1", "eth_call"),
    (48, "chainlink_aggregator_runtime", "eth_getCode"),
)


class _MutableClock:
    def __init__(self, value=0.0):
        self.value = value

    def __call__(self):
        return self.value


def _rpc_response(rows, *, status=200, encoding=None, chunks=None):
    body = _canonical_bytes(rows)
    if encoding == "gzip":
        body = gzip.compress(body, mtime=0)
    if chunks is None:
        chunks = (body,)
    headers = [("Content-Length", str(sum(len(chunk) for chunk in chunks)))]
    if encoding is not None:
        headers.append(("Content-Encoding", encoding))
    return _make_archive_rpc_test_response_for_test(
        status=status,
        header_items=tuple(headers),
        body_chunks=tuple(chunks),
    )


def _simple_request_rows(count=1, *, first_id=1):
    return [
        {"jsonrpc": "2.0", "id": first_id + index,
         "method": "eth_chainId", "params": []}
        for index in range(count)
    ]


def _valid_production_transfer_arguments():
    request_bytes = b'{"id":1}'
    decoded_bytes = b'[{"id":1,"result":"0x1"}]'
    wire_bytes = b"sealed-wire-metadata"
    return {
        "exchange_projection": {
            "exchange_index": 1,
            "logical_batch_index": 1,
            "attempt_index": 1,
            "request_byte_count": len(request_bytes),
            "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
            "request_ids": (1,),
            "wire_byte_count": len(wire_bytes),
            "wire_sha256": hashlib.sha256(wire_bytes).hexdigest(),
            "decoded_byte_count": len(decoded_bytes),
            "decoded_sha256": hashlib.sha256(decoded_bytes).hexdigest(),
            "response_ids": (1,),
        },
        "canonical_request_bytes": request_bytes,
        "decoded_response_bytes": decoded_bytes,
    }


class HistoricalFoundryRpcRunBoundaryRedTests(unittest.TestCase):
    def test_task3b_claimed_window_surfaces_are_exact(self):
        expected = {
            "_claim_fresh_production_archive_rpc_run_for_historical_window": (
                "context",
            ),
            "_get_claimed_historical_window_config": ("claim",),
            "_bind_claimed_historical_window_scan_source_module": ("claim",),
            "_bind_claimed_historical_window_storage_source_module": (
                "claim", "module",
            ),
            "_bind_claimed_historical_window_sources_to_spool": (
                "claim", "spool",
            ),
            "_consume_claimed_historical_window_source_capsule_for_storage": (
                "capsule", "expected_claim", "expected_spool",
                "expected_storage_module",
            ),
            "_commit_claimed_historical_window_source_capsule_move": (
                "capsule", "expected_claim", "expected_spool", "binding",
            ),
            "_abort_claimed_historical_window_source_capsule_move": (
                "capsule", "expected_claim", "expected_spool",
            ),
            "_open_production_archive_rpc_historical_window_logical_batch": (
                "claim", "logical_root", "spool",
            ),
            "_production_archive_rpc_historical_window_logical_batch_attempt": (
                "logical_scope", "request_rows",
            ),
            "_finalize_claimed_production_archive_rpc_run_for_historical_window": (
                "claim", "prefinalization",
            ),
            "_verify_claimed_historical_window_finalization": (
                "claim", "finalization", "expected_prefinalization",
                "expected_receipt_inventory_sha256",
            ),
        }
        for name, parameters in expected.items():
            with self.subTest(name=name):
                function = getattr(rpc, name)
                signature = inspect.signature(function)
                self.assertEqual(tuple(signature.parameters), parameters)
                self.assertTrue(all(
                    parameter.kind is inspect.Parameter.KEYWORD_ONLY
                    for parameter in signature.parameters.values()
                ))

        expected_methods = {
            "close": ("self",),
            "__enter__": ("self",),
            "__exit__": ("self", "error_type", "error", "traceback"),
        }
        claim_class = rpc._ProductionHistoricalWindowRunClaim
        for name, parameters in expected_methods.items():
            self.assertEqual(
                tuple(inspect.signature(getattr(claim_class, name)).parameters),
                parameters,
            )

    def test_task3b_rpc_authorities_are_closed_and_sources_are_appended(self):
        classes = (
            rpc._ProductionHistoricalWindowRunClaim,
            rpc._ProductionHistoricalWindowLogicalBatchScope,
            rpc._ClaimedHistoricalWindowSourceCapsule,
        )
        for authority_class in classes:
            with self.subTest(authority=authority_class.__name__):
                with self.assertRaises(rpc._ArchiveRpcError):
                    authority_class()
                with self.assertRaises(TypeError):
                    type("Forbidden", (authority_class,), {})
                clone = object.__new__(authority_class)
                self.assertFalse(hasattr(clone, "__dict__"))
                self.assertEqual(
                    repr(clone), authority_class.__name__ + "(<redacted>)"
                )
                with self.assertRaises(TypeError):
                    copy.copy(clone)
                with self.assertRaises(TypeError):
                    copy.deepcopy(clone)
                with self.assertRaises(TypeError):
                    pickle.dumps(clone)
                with self.assertRaises(TypeError):
                    json.dumps(clone)

        rows = tuple(
            (role, module_name, relative)
            for role, module_name, relative in rpc._PRODUCTION_SOURCE_MEMBERS
        )
        self.assertEqual(rows.count((
            "source:historical_foundry_scan",
            None,
            "scripts/historical_foundry_scan.py",
        )), 1)
        self.assertEqual(rows.count((
            "source:historical_foundry_storage",
            None,
            "scripts/historical_foundry_storage.py",
        )), 1)
        self.assertEqual(rows[-2:], (
            (
                "source:historical_foundry_scan",
                None,
                "scripts/historical_foundry_scan.py",
            ),
            (
                "source:historical_foundry_storage",
                None,
                "scripts/historical_foundry_storage.py",
            ),
        ))

    def test_private_run_boundary_signatures_are_closed(self):
        self.assertEqual(
            tuple(inspect.signature(_open_production_archive_rpc_run).parameters),
            (),
        )
        self.assertEqual(
            tuple(inspect.signature(_production_archive_rpc_batch).parameters),
            ("context", "request_rows"),
        )
        self.assertEqual(
            tuple(inspect.signature(_finalize_production_archive_rpc_run).parameters),
            ("context",),
        )
        self.assertEqual(
            tuple(
                inspect.signature(
                    _open_production_archive_rpc_logical_batch
                ).parameters
            ),
            ("context", "request_rows"),
        )
        self.assertEqual(
            tuple(
                inspect.signature(
                    _issue_archive_rpc_test_preflight_for_test
                ).parameters
            ),
            ("checkpoint",),
        )
        self.assertEqual(
            tuple(inspect.signature(_issue_archive_rpc_test_run_for_test).parameters),
            ("endpoint", "operation", "monotonic", "entropy", "preflight"),
        )
        self.assertEqual(
            tuple(inspect.signature(_archive_rpc_test_batch_for_test).parameters),
            ("context", "request_rows"),
        )
        self.assertEqual(
            tuple(
                inspect.signature(
                    _open_archive_rpc_test_logical_batch_for_test
                ).parameters
            ),
            ("context", "request_rows"),
        )
        self.assertEqual(
            tuple(
                inspect.signature(
                    _project_archive_rpc_test_finalization_for_test
                ).parameters
            ),
            ("context",),
        )
        self.assertEqual(
            tuple(inspect.signature(_close_archive_rpc_test_run_for_test).parameters),
            ("context",),
        )
        self.assertEqual(
            tuple(
                inspect.signature(
                    _make_archive_rpc_test_response_for_test
                ).parameters
            ),
            (
                "status",
                "header_items",
                "body_chunks",
                "before_status",
                "before_headers",
                "before_chunk",
            ),
        )


class HistoricalFoundryRpcTask3bClaimTests(unittest.TestCase):
    class _Preflight:
        def __init__(self):
            self.identity = rpc._test_preflight_identity()
            self.config = object()
            self.closed = False

        def close(self):
            self.closed = True

    class _Environment(dict):
        def get(self, key, default=None):
            if key != "DEX_DEPTH_RPC_ETH" or default is not None:
                raise AssertionError("unexpected environment access")
            return "https://rpc.example.invalid/archive"

    class _Opener:
        def __init__(self, calls):
            self.addheaders = []
            self.calls = calls

        def open(self, _request, _timeout=None, **_kwargs):
            self.calls.append("transport")
            raise AssertionError("transport entered")

    def _open_offline(self):
        calls = []
        preflight = self._Preflight()
        opener = self._Opener(calls)
        with mock.patch.object(
            rpc, "_perform_production_preflight", return_value=preflight
        ), mock.patch.object(
            rpc.time, "monotonic", return_value=10.0
        ), mock.patch.object(
            rpc.os, "urandom", return_value=b"z" * 32
        ), mock.patch.object(
            rpc.os, "environ", self._Environment()
        ), mock.patch.object(
            rpc.urllib.request, "build_opener", return_value=opener
        ):
            context = rpc._open_production_archive_rpc_run()
        return context, preflight, calls

    def test_connected_opener_rewrites_only_finalized_anchor_to_exact_block(self):
        anchor = {
            "number": 123,
            "hash": "0x" + "12" * 32,
            "parent_hash": "0x" + "34" * 32,
            "state_root": "0x" + "56" * 32,
            "timestamp": 1_700_000_000,
            "gas_limit": 30_000_000,
            "gas_used": 15_000_000,
            "base_fee_per_gas": 1_000_000_000,
        }
        calls = []
        preflight = self._Preflight()

        class Response:
            pass

        class Opener:
            addheaders = []

            def open(self, request, timeout):
                calls.append((request.data, timeout))
                return Response()

        with mock.patch.object(
            rpc, "_perform_production_preflight", return_value=preflight
        ), mock.patch.object(
            rpc.time, "monotonic", return_value=10.0
        ), mock.patch.object(
            rpc.os, "urandom", return_value=b"z" * 32
        ), mock.patch.object(
            rpc.os, "environ", self._Environment()
        ), mock.patch.object(
            rpc.urllib.request, "build_opener", return_value=Opener()
        ):
            context = (
                rpc
                ._open_production_archive_rpc_run_for_connected_verification(
                    anchor_header=anchor
                )
            )
        original = rpc._archive_canonical_bytes([
            {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
            {
                "jsonrpc": "2.0", "id": 2,
                "method": "eth_getBlockByNumber",
                "params": ["finalized", False],
            },
        ])
        early_exact = rpc._archive_canonical_bytes({
            "jsonrpc": "2.0", "id": 3,
            "method": "eth_getBlockByNumber",
            "params": [hex(anchor["number"]), False],
        })
        wrong_first_row = rpc._archive_canonical_bytes([
            {"jsonrpc": "2.0", "id": 1, "method": "net_version", "params": []},
            {
                "jsonrpc": "2.0", "id": 2,
                "method": "eth_getBlockByNumber",
                "params": ["finalized", False],
            },
        ])
        for attack in (early_exact, wrong_first_row):
            with self.subTest(attack=json.loads(attack)):
                with self.assertRaises(rpc._ArchiveRpcError):
                    context._operation(attack, 7.0)
        self.assertEqual(calls, [])
        self.assertIsInstance(context._operation(original, 7.0), Response)
        self.assertEqual(len(calls), 1)
        sent = json.loads(calls[0][0])
        self.assertEqual(sent[0]["params"], [])
        self.assertEqual(sent[1]["params"], [hex(anchor["number"]), False])
        self.assertEqual(calls[0][1], 7.0)
        self.assertEqual(json.loads(original)[1]["params"], ["finalized", False])
        with self.assertRaises(rpc._ArchiveRpcError):
            context._operation(original, 7.0)
        rpc._abandon_archive_context(context)

    def assertPair(self, callable_object, expected):
        with self.assertRaises(rpc._ArchiveRpcError) as caught:
            callable_object()
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            expected,
        )

    def test_fresh_opened_context_claims_once_and_close_cleans_without_transport(self):
        context, preflight, calls = self._open_offline()
        claim = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
            context=context
        )
        self.assertIs(type(claim), rpc._ProductionHistoricalWindowRunClaim)
        self.assertIs(
            rpc._get_claimed_historical_window_config(claim=claim),
            preflight.config,
        )
        self.assertPair(
            lambda: rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                context=context
            ),
            ("authority_mismatch", "historical_window_context_not_fresh"),
        )
        self.assertEqual(calls, [])
        self.assertIsNone(claim.close())
        self.assertTrue(preflight.closed)
        self.assertEqual(context._state, "abandoned")
        self.assertIsNone(claim.close())

    def test_claim_core_return_controls_retire_undelivered_authority(self):
        exported = (
            rpc._claim_fresh_production_archive_rpc_run_for_historical_window
        )
        try:
            core = _closure_function(exported, "_claim_fresh_core")
        except AssertionError:
            core = exported
        lines, start = inspect.getsourcelines(core)
        return_line = start + next(
            index for index, line in enumerate(lines)
            if "return claim" in line
        )
        claim_registry = _closure_named_value(exported, "claim_registry")
        control_factories = (
            lambda: KeyboardInterrupt("claim-return-keyboard"),
            lambda: SystemExit("claim-return-system"),
            lambda: GeneratorExit("claim-return-generator"),
            lambda: asyncio.CancelledError("claim-return-cancelled"),
        )
        for control_factory in control_factories:
            control = control_factory()
            with self.subTest(control=type(control).__name__):
                context, preflight, calls = self._open_offline()
                captured = []
                fired = [False]
                prior_trace = sys.gettrace()

                def tracer(frame, event, _argument):
                    if (
                        not fired[0]
                        and frame.f_code is core.__code__
                        and event == "line"
                        and frame.f_lineno == return_line
                    ):
                        fired[0] = True
                        captured.append(frame.f_locals["claim"])
                        sys.settrace(prior_trace)
                        raise control
                    return tracer

                try:
                    sys.settrace(tracer)
                    with self.assertRaises(type(control)) as caught:
                        exported(context=context)
                    self.assertIs(caught.exception, control)
                    self.assertTrue(fired[0])
                    self.assertEqual(len(captured), 1)
                    self.assertEqual(context._state, "failed")
                    self.assertTrue(preflight.closed)
                    self.assertIsNone(context._historical_window_consumer)
                    self.assertNotIn(id(captured[0]), claim_registry)
                    self.assertEqual(calls, [])
                finally:
                    sys.settrace(prior_trace)
                    if id(captured[0]) in claim_registry if captured else False:
                        captured[0].close()

    def test_direct_production_context_issuer_claim_rejection_fail_closes(self):
        calls = []

        class Preflight:
            def __init__(self):
                self.identity = rpc._test_preflight_identity()
                self.config = object()
                self.closed = False

            def close(self):
                self.closed = True

        preflight = Preflight()
        key = bytearray(b"z" * 32)
        context = rpc._issue_production_context(
            _state="active",
            _clock=_MutableClock(),
            _last_clock=0.0,
            _collection_deadline=21_600.0,
            _key=key,
            _endpoint_projection=rpc._frozen_archive_value({}),
            _endpoint_bytes=b"{}",
            _connection_url="https://example.invalid:443/",
            _endpoint_identity=rpc._frozen_archive_value({
                "schema": "historical_foundry_rpc_endpoint_identity/v1",
                "scope": "single_run_nonreversible",
                "endpoint_hmac_sha256": "0" * 64,
            }),
            _operation=lambda _body, _timeout: calls.append("transport"),
            _preflight=preflight,
            _opening_identity=rpc._frozen_archive_value(preflight.identity),
            _active_scope=None,
            _reserved_scope=None,
            _logical_summaries=[],
            _records=[],
            _next_logical_batch_index=1,
            _next_exchange_index=1,
        )
        self.assertPair(
            lambda: rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                context=context
            ),
            ("authority_mismatch", "historical_window_context_not_fresh"),
        )
        self.assertEqual(calls, [])
        self.assertTrue(preflight.closed)
        self.assertEqual(context._state, "abandoned")
        self.assertEqual(bytes(key), b"\0" * 32)

    def test_claim_reserves_generic_routes_before_transport_or_state_mutation(self):
        context, preflight, calls = self._open_offline()
        claim = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
            context=context
        )
        rows = _simple_request_rows()
        initial = (
            context._next_logical_batch_index,
            context._next_exchange_index,
            tuple(context._records),
            tuple(context._logical_summaries),
        )
        pair = (
            "authority_mismatch",
            "historical_window_specialized_batch_required",
        )
        for call in (
            lambda: rpc._production_archive_rpc_batch(context, rows),
            lambda: rpc._open_production_archive_rpc_logical_batch(
                context, rows
            ),
            lambda: rpc._batch_with_active_scope(context, rows),
            lambda: rpc._finalize_production_archive_rpc_run(context),
            lambda: rpc._open_production_archive_rpc_historical_window_logical_batch(
                claim=claim, logical_root={}, spool=object()
            ),
            lambda: rpc._finalize_claimed_production_archive_rpc_run_for_historical_window(
                claim=claim, prefinalization=object()
            ),
        ):
            with self.subTest(call=repr(call)):
                self.assertPair(call, pair)
                self.assertEqual(calls, [])
                self.assertEqual((
                    context._next_logical_batch_index,
                    context._next_exchange_index,
                    tuple(context._records),
                    tuple(context._logical_summaries),
                ), initial)
        claim.close()
        self.assertTrue(preflight.closed)

    def test_simultaneous_claim_has_exactly_one_winner(self):
        context, preflight, calls = self._open_offline()
        barrier = threading.Barrier(3)
        results = []

        def attempt():
            barrier.wait()
            try:
                value = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                    context=context
                )
            except rpc._ArchiveRpcError as error:
                results.append(("error", error.failure_kind))
            else:
                results.append(("claim", value))

        threads = [threading.Thread(target=attempt) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        winners = [row[1] for row in results if row[0] == "claim"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(
            results.count(("error", "historical_window_context_not_fresh")),
            1,
        )
        self.assertEqual(calls, [])
        winners[0].close()
        self.assertTrue(preflight.closed)

    def test_generic_batch_holds_claim_lock_through_core_operation(self):
        context, preflight, calls = self._open_offline()
        core_entered = threading.Event()
        release_core = threading.Event()
        claim_finished = threading.Event()
        order = []
        claimed = []

        def blocked_common(*_arguments):
            core_entered.set()
            release_core.wait(2.0)
            order.append("generic")
            return ()

        def run_generic():
            rpc._production_archive_rpc_batch(context, _simple_request_rows())

        def run_claim():
            try:
                claimed.append(
                    rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                        context=context
                    )
                )
                order.append("claim")
            finally:
                claim_finished.set()

        with mock.patch.object(
            rpc, "_archive_batch_common", side_effect=blocked_common
        ):
            generic_thread = threading.Thread(target=run_generic)
            claim_thread = threading.Thread(target=run_claim)
            generic_thread.start()
            self.assertTrue(core_entered.wait(1.0))
            claim_thread.start()
            self.assertFalse(claim_finished.wait(0.1))
            release_core.set()
            generic_thread.join(2.0)
            claim_thread.join(2.0)
        self.assertEqual(order, ["generic", "claim"])
        self.assertEqual(len(claimed), 1)
        self.assertEqual(calls, [])
        claimed[0].close()
        self.assertTrue(preflight.closed)

    def test_claim_close_preserves_control_after_context_registry_cleanup(self):
        cancellation = KeyboardInterrupt("claim-close-control")

        class Preflight:
            def __init__(self):
                self.identity = rpc._test_preflight_identity()
                self.config = object()
                self.calls = 0

            def close(self):
                self.calls += 1
                raise cancellation

        preflight = Preflight()
        calls = []
        with mock.patch.object(
            rpc, "_perform_production_preflight", return_value=preflight
        ), mock.patch.object(
            rpc.time, "monotonic", return_value=10.0
        ), mock.patch.object(
            rpc.os, "urandom", return_value=b"z" * 32
        ), mock.patch.object(
            rpc.os, "environ", self._Environment()
        ), mock.patch.object(
            rpc.urllib.request, "build_opener", return_value=self._Opener(calls)
        ):
            context = rpc._open_production_archive_rpc_run()
        claim = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
            context=context
        )
        with self.assertRaises(KeyboardInterrupt) as caught:
            claim.close()
        self.assertIs(caught.exception, cancellation)
        self.assertEqual(preflight.calls, 1)
        self.assertEqual(context._state, "abandoned")
        self.assertIsNone(context._preflight)
        self.assertIsNone(context._operation)
        self.assertIsNone(claim.close())
        self.assertEqual(preflight.calls, 1)
        self.assertEqual(calls, [])

    def test_raw_scope_enter_and_context_exit_share_exact_lock(self):
        context, preflight, _calls = self._open_offline()
        scope = rpc._open_production_archive_rpc_logical_batch(
            context, _simple_request_rows()
        )
        original = rpc._enter_archive_scope
        entered = threading.Event()
        release = threading.Event()
        exit_finished = threading.Event()
        errors = []

        def blocked(value):
            entered.set()
            release.wait(2.0)
            return original(value)

        def run_enter():
            try:
                scope.__enter__()
            except BaseException as error:
                errors.append(error)

        def run_exit():
            try:
                context.__exit__(None, None, None)
            finally:
                exit_finished.set()

        with mock.patch.object(rpc, "_enter_archive_scope", side_effect=blocked):
            enter_thread = threading.Thread(target=run_enter)
            exit_thread = threading.Thread(target=run_exit)
            enter_thread.start()
            self.assertTrue(entered.wait(1.0))
            exit_thread.start()
            self.assertFalse(exit_finished.wait(0.1))
            release.set()
            enter_thread.join(2.0)
            exit_thread.join(2.0)
        self.assertEqual(errors, [])
        self.assertTrue(exit_finished.is_set())
        self.assertTrue(preflight.closed)

    def test_direct_claimed_abandon_waits_and_routes_claim_cleanup(self):
        context, preflight, _calls = self._open_offline()
        claim = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
            context=context
        )
        finished = threading.Event()
        errors = []

        def abandon():
            try:
                rpc._abandon_archive_context(context)
            except BaseException as error:
                errors.append(error)
            finally:
                finished.set()

        with context._historical_window_lock:
            thread = threading.Thread(target=abandon)
            thread.start()
            self.assertFalse(finished.wait(0.1))
        thread.join(2.0)
        self.assertEqual(errors, [])
        self.assertTrue(finished.is_set())
        self.assertEqual(context._state, "abandoned")
        self.assertIsNone(context._historical_window_consumer)
        self.assertTrue(preflight.closed)
        self.assertIsNone(claim.close())

    def test_rejected_reuse_claim_terminalizes_the_opened_context(self):
        context, preflight, calls = self._open_offline()
        claim = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
            context=context
        )
        self.assertPair(
            lambda: rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                context=context
            ),
            ("authority_mismatch", "historical_window_context_not_fresh"),
        )
        self.assertEqual(context._state, "abandoned")
        self.assertTrue(preflight.closed)
        self.assertEqual(calls, [])
        self.assertIsNone(claim.close())

    def test_module_visible_raw_core_rejects_claimed_context(self):
        context, preflight, calls = self._open_offline()
        claim = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
            context=context
        )
        initial = (
            context._next_logical_batch_index,
            context._reserved_scope,
            context._active_scope,
        )
        self.assertPair(
            lambda: rpc._open_archive_scope(
                context, _simple_request_rows(), implicit=False
            ),
            (
                "authority_mismatch",
                "historical_window_specialized_batch_required",
            ),
        )
        self.assertEqual(
            (
                context._next_logical_batch_index,
                context._reserved_scope,
                context._active_scope,
            ),
            initial,
        )
        self.assertEqual(calls, [])
        claim.close()
        self.assertTrue(preflight.closed)

    def test_generic_attempt_and_finalizer_preserve_all_control_objects(self):
        for cancellation in (
            GeneratorExit("generic-attempt-control"),
            asyncio.CancelledError("generic-attempt-cancelled"),
        ):
            with self.subTest(attempt=type(cancellation).__name__):
                context, preflight, _calls = self._open_offline()
                scope = rpc._open_production_archive_rpc_logical_batch(
                    context, _simple_request_rows()
                )
                scope.__enter__()
                with mock.patch.object(
                    rpc, "_perform_archive_attempt", side_effect=cancellation
                ):
                    with self.assertRaises(type(cancellation)) as caught:
                        rpc._production_archive_rpc_batch(
                            context, _simple_request_rows()
                        )
                self.assertIs(caught.exception, cancellation)
                self.assertEqual(context._state, "failed")
                self.assertTrue(preflight.closed)

    def test_module_visible_cores_serialize_their_entire_production_use(self):
        context, preflight, _calls = self._open_offline()
        core_entered = threading.Event()
        release_core = threading.Event()
        claim_finished = threading.Event()
        original_freeze = rpc._freeze_archive_request_rows
        results = []

        def blocked_freeze(rows):
            core_entered.set()
            release_core.wait(2.0)
            return original_freeze(rows)

        def open_core():
            try:
                results.append(("scope", rpc._open_archive_scope(
                    context, _simple_request_rows(), implicit=False
                )))
            except BaseException as error:
                results.append(("scope_error", error))

        def claim_context():
            try:
                results.append(("claim", rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                    context=context
                )))
            except BaseException as error:
                results.append(("claim_error", error))
            finally:
                claim_finished.set()

        with mock.patch.object(
            rpc, "_freeze_archive_request_rows", side_effect=blocked_freeze
        ):
            core_thread = threading.Thread(target=open_core)
            claim_thread = threading.Thread(target=claim_context)
            core_thread.start()
            self.assertTrue(core_entered.wait(1.0))
            claim_thread.start()
            self.assertFalse(claim_finished.wait(0.1))
            release_core.set()
            core_thread.join(2.0)
            claim_thread.join(2.0)
        scope = next(value for kind, value in results if kind == "scope")
        self.assertIs(type(scope), rpc._ProductionArchiveRpcLogicalBatchScope)
        self.assertTrue(any(kind == "claim_error" for kind, _ in results))
        self.assertEqual(context._state, "abandoned")
        self.assertTrue(preflight.closed)

        context, preflight, _calls = self._open_offline()
        core_entered = threading.Event()
        release_core = threading.Event()
        claim_finished = threading.Event()
        original_require = rpc._require_archive_context
        results = []

        def blocked_require(value, expected):
            core_entered.set()
            release_core.wait(2.0)
            return original_require(value, expected)

        def finalize_core():
            try:
                with mock.patch.object(
                    rpc, "_recheck_production_preflight", return_value=True
                ):
                    results.append((
                        "finalization",
                        rpc._finalize_production_archive_rpc_run_unlocked(
                            context
                        ),
                    ))
            except BaseException as error:
                results.append(("finalization_error", error))

        def claim_finalizing_context():
            try:
                results.append(("claim", rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                    context=context
                )))
            except BaseException as error:
                results.append(("claim_error", error))
            finally:
                claim_finished.set()

        with mock.patch.object(
            rpc, "_require_archive_context", side_effect=blocked_require
        ):
            core_thread = threading.Thread(target=finalize_core)
            claim_thread = threading.Thread(target=claim_finalizing_context)
            core_thread.start()
            self.assertTrue(core_entered.wait(1.0))
            claim_thread.start()
            self.assertFalse(claim_finished.wait(0.1))
            release_core.set()
            core_thread.join(2.0)
            claim_thread.join(2.0)
        self.assertTrue(any(kind == "finalization" for kind, _ in results))
        self.assertTrue(any(kind == "claim_error" for kind, _ in results))
        self.assertTrue(preflight.closed)

        for cancellation in (
            GeneratorExit("generic-finalize-control"),
            asyncio.CancelledError("generic-finalize-cancelled"),
        ):
            with self.subTest(finalize=type(cancellation).__name__):
                context, preflight, _calls = self._open_offline()
                with mock.patch.object(
                    rpc, "_recheck_production_preflight", return_value=True
                ), mock.patch.object(
                    rpc, "_finalization_projection", side_effect=cancellation
                ):
                    with self.assertRaises(type(cancellation)) as caught:
                        rpc._finalize_production_archive_rpc_run(context)
                self.assertIs(caught.exception, cancellation)
                self.assertEqual(context._state, "failed")
                self.assertTrue(preflight.closed)


class HistoricalFoundryRpcTask3bSourceBindingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = rpc.Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _open_claim(self):
        root = rpc.Path(rpc.__file__).resolve().parents[1]
        sources = rpc._HeldArchiveSourceAuthority(root)
        sources.open_members()

        class Preflight:
            def __init__(self):
                self.identity = rpc._test_preflight_identity()
                self.config = object()
                self.sources = sources
                self.closed = False

            def close(self):
                if not self.closed:
                    self.closed = True
                    self.sources.close()

        class Environment(dict):
            def get(self, key, default=None):
                if key != "DEX_DEPTH_RPC_ETH" or default is not None:
                    raise AssertionError("unexpected environment read")
                return "https://rpc.example.invalid/archive"

        class Opener:
            addheaders = []

            def open(self, *_args, **_kwargs):
                raise AssertionError("transport entered")

        preflight = Preflight()
        with mock.patch.object(
            rpc, "_perform_production_preflight", return_value=preflight
        ), mock.patch.object(
            rpc.time, "monotonic", return_value=10.0
        ), mock.patch.object(
            rpc.os, "urandom", return_value=b"z" * 32
        ), mock.patch.object(
            rpc.os, "environ", Environment()
        ), mock.patch.object(
            rpc.urllib.request, "build_opener", return_value=Opener()
        ):
            context = rpc._open_production_archive_rpc_run()
        claim = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
            context=context
        )
        return claim, context, preflight

    def _install_outstanding_transfer(self, spool, claim, state):
        arguments = _valid_production_transfer_arguments()
        active_registry = _closure_named_value(
            type(spool).append_transfer, "active_registry"
        )
        owner = active_registry[id(spool)][1]
        arguments["exchange_projection"]["exchange_index"] = owner[
            "next_exchange_index"
        ]
        transfer = spool.issue_transfer_from_bound_rpc(
            claim=claim, **arguments
        )
        if state == "issued":
            return transfer, None, None
        pending = spool.append_transfer(transfer=transfer)
        spool.verify_pending_receipt(
            transfer=transfer, pending_receipt=pending
        )
        if state == "pending_verified":
            return transfer, pending, None
        receipt = spool.commit_transfer(
            transfer=transfer, pending_receipt=pending
        )
        if state == "committed_unverified":
            return transfer, pending, receipt
        spool.verify_committed_receipt(
            transfer=transfer, receipt=receipt
        )
        return transfer, pending, receipt

    def test_bind_never_imports_or_reloads_already_loaded_scan(self):
        import scripts.historical_foundry_scan
        import scripts.historical_foundry_storage as storage

        claim, _context, _preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        original_import = importlib.import_module
        calls = []

        def guarded_import(name, package=None):
            calls.append((name, package))
            if name == "scripts.historical_foundry_scan":
                raise AssertionError("binder imported scan")
            return original_import(name, package)

        try:
            with mock.patch.object(
                rpc.importlib, "import_module", side_effect=guarded_import
            ), mock.patch.object(
                rpc.importlib,
                "reload",
                side_effect=AssertionError("binder reloaded a module"),
            ):
                binding = (
                    rpc._bind_claimed_historical_window_sources_to_spool(
                        claim=claim, spool=spool
                    )
                )
            self.assertIsNotNone(binding)
            self.assertFalse(any(
                name == "scripts.historical_foundry_scan"
                for name, _package in calls
            ))
        finally:
            claim.close()
            spool.close()

    def test_bind_rejects_wrong_rpc_spec_name_as_final_identity_drift(self):
        import scripts.historical_foundry_scan
        import scripts.historical_foundry_storage as storage

        claim, _context, _preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        spec = rpc.__spec__
        original_name = spec.name
        try:
            spec.name = "scripts.historical_foundry_rpc_alias"
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                rpc._bind_claimed_historical_window_sources_to_spool(
                    claim=claim, spool=spool
                )
            self.assertEqual(
                (caught.exception.reason_code, caught.exception.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
        finally:
            spec.name = original_name
            claim.close()
            spool.close()

    def test_bind_maps_unresolvable_rpc_origin_to_final_identity_drift(self):
        import scripts.historical_foundry_scan
        import scripts.historical_foundry_storage as storage

        claim, _context, _preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        spec = rpc.__spec__
        original_origin = spec.origin
        try:
            spec.origin = "/private/tmp/historical-foundry-missing-rpc.py"
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                rpc._bind_claimed_historical_window_sources_to_spool(
                    claim=claim, spool=spool
                )
            self.assertEqual(
                (caught.exception.reason_code, caught.exception.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
        finally:
            spec.origin = original_origin
            claim.close()
            spool.close()

    def test_isolated_scan_alias_and_real_reload_matrix(self):
        harness = r'''
import hashlib
import importlib
import importlib.util
import sys
import types

import scripts.historical_foundry_rpc as rpc
import scripts.historical_foundry_storage as storage
from tests.test_historical_foundry_rpc import (
    HistoricalFoundryRpcTask3bSourceBindingTests,
)

case_name = sys.argv[1]
case = HistoricalFoundryRpcTask3bSourceBindingTests(
    methodName="test_bind_never_imports_or_reloads_already_loaded_scan"
)
case.setUp()
claim = None
spool = None
try:
    if case_name == "main_only":
        scan_path = rpc.Path(rpc.__file__).resolve().with_name(
            "historical_foundry_scan.py"
        )
        scan_spec = importlib.util.spec_from_file_location(
            "scripts.historical_foundry_scan", str(scan_path)
        )
        scan = importlib.util.module_from_spec(scan_spec)
        sys.modules.pop("scripts.historical_foundry_scan", None)
        sys.modules["__main__"] = scan
        scan_spec.loader.exec_module(scan)
    elif case_name == "missing_scan":
        scan = None
        sys.modules.pop("scripts.historical_foundry_scan", None)
    else:
        scan = importlib.import_module("scripts.historical_foundry_scan")
        if case_name == "dual_same":
            sys.modules["__main__"] = scan
        elif case_name == "dual_different":
            other = types.ModuleType("__main__")
            other.__spec__ = importlib.util.spec_from_file_location(
                "scripts.historical_foundry_scan", scan.__file__
            )
            other.__file__ = scan.__file__
            sys.modules["__main__"] = other

    claim, context, preflight = case._open_claim()
    spool = storage._open_historical_window_exchange_spool(
        data_dir=case.data_dir
    )
    if case_name == "storage_main_only":
        sys.modules["__main__"] = storage
        sys.modules.pop("scripts.historical_foundry_storage", None)

    expected_failure = case_name in (
        "dual_different", "missing_scan", "storage_main_only"
    )
    if expected_failure:
        try:
            rpc._bind_claimed_historical_window_sources_to_spool(
                claim=claim, spool=spool
            )
        except rpc._ArchiveRpcError as error:
            pair = (error.reason_code, error.failure_kind)
        else:
            raise AssertionError("invalid alias matrix case bound")
        expected = (
            ("authority_mismatch", "historical_window_capability_invalid")
            if case_name == "storage_main_only"
            else ("authority_mismatch", "final_identity_drift")
        )
        assert pair == expected, (case_name, pair)
        if case_name == "missing_scan":
            assert "scripts.historical_foundry_scan" not in sys.modules
    else:
        binding = rpc._bind_claimed_historical_window_sources_to_spool(
            claim=claim, spool=spool
        )
        assert binding is not None
        if case_name == "main_only":
            assert "scripts.historical_foundry_scan" not in sys.modules
finally:
    if claim is not None:
        try:
            claim.close()
        except BaseException:
            pass
    if spool is not None:
        try:
            spool.close()
        except BaseException:
            pass
    case.tearDown()
'''
        root = str(rpc.Path(__file__).resolve().parents[1])
        cases = (
            "canonical",
            "main_only",
            "dual_same",
            "dual_different",
            "missing_scan",
            "storage_main_only",
        )
        for case_name in cases:
            with self.subTest(case=case_name):
                completed = subprocess.run(
                    (sys.executable, "-B", "-c", harness, case_name),
                    cwd=root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    check=False,
                )
                self.assertEqual(
                    completed.returncode,
                    0,
                    completed.stdout + completed.stderr,
                )

    def test_each_bound_module_rejects_wrong_identity_and_real_reload(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        modules = {"rpc": rpc, "scan": scan, "storage": storage}
        for role, module in modules.items():
            for field in ("spec_name", "origin", "file"):
                with self.subTest(role=role, field=field):
                    claim, _context, _preflight = self._open_claim()
                    spool = storage._open_historical_window_exchange_spool(
                        data_dir=self.data_dir
                    )
                    spec = module.__spec__
                    if field == "spec_name":
                        target = spec
                        attribute = "name"
                        replacement = "scripts.historical_foundry_{}_alias".format(
                            role
                        )
                    elif field == "origin":
                        target = spec
                        attribute = "origin"
                        replacement = "/private/tmp/missing-{}-origin.py".format(
                            role
                        )
                    else:
                        target = module
                        attribute = "__file__"
                        replacement = "/private/tmp/missing-{}-file.py".format(
                            role
                        )
                    original = getattr(target, attribute)
                    try:
                        setattr(target, attribute, replacement)
                        with self.assertRaises(rpc._ArchiveRpcError) as caught:
                            rpc._bind_claimed_historical_window_sources_to_spool(
                                claim=claim, spool=spool
                            )
                        self.assertEqual(
                            (
                                caught.exception.reason_code,
                                caught.exception.failure_kind,
                            ),
                            ("authority_mismatch", "final_identity_drift"),
                        )
                    finally:
                        setattr(target, attribute, original)
                        claim.close()
                        spool.close()

        root = str(rpc.Path(__file__).resolve().parents[1])
        reload_harness = r'''
import hashlib
import importlib
import sys

import scripts.historical_foundry_rpc as rpc
import scripts.historical_foundry_scan as scan
import scripts.historical_foundry_storage as storage
from tests.test_historical_foundry_rpc import (
    HistoricalFoundryRpcTask3bSourceBindingTests,
)

role = sys.argv[1]
case = HistoricalFoundryRpcTask3bSourceBindingTests(
    methodName="test_bind_never_imports_or_reloads_already_loaded_scan"
)
case.setUp()
claim = None
spool = None
try:
    claim, context, preflight = case._open_claim()
    old_close = claim.close
    old_error = rpc._ArchiveRpcError
    spool = storage._open_historical_window_exchange_spool(
        data_dir=case.data_dir
    )
    rpc._bind_claimed_historical_window_sources_to_spool(
        claim=claim, spool=spool
    )
    modules = {"rpc": rpc, "scan": scan, "storage": storage}
    module = modules[role]
    generation = module._HISTORICAL_WINDOW_MODULE_GENERATION
    reloaded = importlib.reload(module)
    assert reloaded is module
    assert module._HISTORICAL_WINDOW_MODULE_GENERATION is not generation
    request_bytes = b'{"id":1}'
    decoded_bytes = b'[{"id":1,"result":"0x1"}]'
    wire_bytes = b"sealed-wire-metadata"
    projection = {
        "exchange_index": 1,
        "logical_batch_index": 1,
        "attempt_index": 1,
        "request_byte_count": len(request_bytes),
        "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
        "request_ids": (1,),
        "wire_byte_count": len(wire_bytes),
        "wire_sha256": hashlib.sha256(wire_bytes).hexdigest(),
        "decoded_byte_count": len(decoded_bytes),
        "decoded_sha256": hashlib.sha256(decoded_bytes).hexdigest(),
        "response_ids": (1,),
    }
    try:
        spool.issue_transfer_from_bound_rpc(
            claim=claim,
            exchange_projection=projection,
            canonical_request_bytes=request_bytes,
            decoded_response_bytes=decoded_bytes,
        )
    except old_error as error:
        assert type(error) is old_error
        assert (error.reason_code, error.failure_kind) == (
            "authority_mismatch", "final_identity_drift"
        )
    else:
        raise AssertionError("real reload retained bound authority")
    old_close()
    assert context._state == "abandoned"
    assert preflight.closed
finally:
    if spool is not None:
        try:
            spool.close()
        except BaseException:
            pass
    if claim is not None:
        try:
            claim.close()
        except BaseException:
            pass
    case.tearDown()
'''
        for role in ("rpc", "scan", "storage"):
            with self.subTest(reload=role):
                completed = subprocess.run(
                    (sys.executable, "-B", "-c", reload_harness, role),
                    cwd=root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    check=False,
                )
                self.assertEqual(
                    completed.returncode,
                    0,
                    completed.stdout + completed.stderr,
                )

    def test_outstanding_transfer_blocks_every_specialized_boundary(self):
        states = (
            "issued",
            "pending_verified",
            "committed_unverified",
            "committed_verified",
        )
        expected = (
            "authority_mismatch",
            "historical_window_transfer_outstanding",
        )
        original_bind = (
            rpc._bind_claimed_historical_window_sources_to_spool
        )
        original_open = (
            rpc._open_production_archive_rpc_historical_window_logical_batch
        )
        original_finalize = (
            rpc._finalize_claimed_production_archive_rpc_run_for_historical_window
        )
        for state in states:
            for boundary in ("open", "attempt", "finalize"):
                with self.subTest(state=state, boundary=boundary):
                    captured = {"attempts_before_boundary": None}
                    transport = mock.Mock(
                        wraps=rpc._perform_archive_attempt
                    )

                    def install(claim, spool):
                        self._install_outstanding_transfer(
                            spool, claim, state
                        )
                        captured["attempts_before_boundary"] = (
                            transport.call_count
                        )

                    def bind_wrapper(*, claim, spool):
                        result = original_bind(claim=claim, spool=spool)
                        if boundary == "open":
                            install(claim, spool)
                        return result

                    def open_wrapper(*, claim, logical_root, spool):
                        result = original_open(
                            claim=claim,
                            logical_root=logical_root,
                            spool=spool,
                        )
                        if boundary == "attempt":
                            install(claim, spool)
                        return result

                    def finalize_wrapper(*, claim, prefinalization):
                        claim_registry = _closure_named_value(
                            original_finalize, "claim_registry"
                        )
                        claim_record = claim_registry[id(claim)][1]
                        install(claim, claim_record["spool"])
                        return original_finalize(
                            claim=claim, prefinalization=prefinalization
                        )

                    with mock.patch.object(
                        rpc,
                        "_bind_claimed_historical_window_sources_to_spool",
                        side_effect=bind_wrapper,
                    ), mock.patch.object(
                        rpc,
                        "_open_production_archive_rpc_historical_window_logical_batch",
                        side_effect=open_wrapper,
                    ), mock.patch.object(
                        rpc,
                        "_finalize_claimed_production_archive_rpc_run_for_historical_window",
                        side_effect=finalize_wrapper,
                    ), mock.patch.object(
                        rpc, "_perform_archive_attempt", transport
                    ), mock.patch.object(
                        rpc,
                        "_finalize_production_archive_rpc_run",
                        side_effect=AssertionError("legacy finalizer entered"),
                    ) as legacy_finalizer:
                        with self.assertRaises(rpc._ArchiveRpcError) as caught:
                            _run_genuine_historical_window_scheduler()
                    self.assertEqual(
                        (
                            caught.exception.reason_code,
                            caught.exception.failure_kind,
                        ),
                        expected,
                    )
                    self.assertIsNotNone(
                        captured["attempts_before_boundary"]
                    )
                    self.assertEqual(
                        transport.call_count,
                        captured["attempts_before_boundary"],
                    )
                    self.assertEqual(legacy_finalizer.call_count, 0)

    def test_bind_rejects_spool_lookalike_before_capsule_or_attribute_access(self):
        import scripts.historical_foundry_scan

        calls = []
        capsules = []

        class Probe:
            def __getattribute__(self, name):
                calls.append(name)

                def attack(**keywords):
                    capsules.append(keywords.get("source_capsule"))
                    raise RuntimeError("probe invoked")

                return attack

        claim, _context, _preflight = self._open_claim()
        error = None
        try:
            try:
                rpc._bind_claimed_historical_window_sources_to_spool(
                    claim=claim, spool=Probe()
                )
            except BaseException as observed:
                error = observed
            self.assertIs(type(error), rpc._ArchiveRpcError)
            self.assertEqual(
                (error.reason_code, error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertEqual(calls, [])
            self.assertEqual(capsules, [])
        finally:
            claim.close()

    def test_specialized_opener_rejects_cross_or_lookalike_spool_before_scope(self):
        import scripts.historical_foundry_scan
        import scripts.historical_foundry_storage as storage

        requests = tuple(_simple_request_rows())
        root = {
            "schema": "historical_foundry_anchor_stage_logical_root/v1",
            "segment": "anchor_stage",
            "stage_index": 0,
            "stage_name": "anchor",
            "logical_batch_index": 1,
            "requests": requests,
            "allow_http_413_bisection": False,
        }
        for kind in ("cross", "lookalike"):
            with self.subTest(kind=kind):
                claim, context, _preflight = self._open_claim()
                bound_spool = storage._open_historical_window_exchange_spool(
                    data_dir=self.data_dir
                )
                rpc._bind_claimed_historical_window_sources_to_spool(
                    claim=claim, spool=bound_spool
                )
                other = None
                probe_calls = []
                if kind == "cross":
                    other = storage._open_historical_window_exchange_spool(
                        data_dir=self.data_dir
                    )
                    candidate = other
                else:
                    class Probe:
                        def __getattribute__(self, name):
                            probe_calls.append(name)
                            raise AssertionError("lookalike inspected")

                    candidate = Probe()
                opened = []
                try:
                    with mock.patch.object(
                        rpc,
                        "_open_archive_scope",
                        wraps=rpc._open_archive_scope,
                    ) as scope_core:
                        with self.assertRaises(
                            rpc._ArchiveRpcError
                        ) as caught:
                            opened.append(
                                rpc._open_production_archive_rpc_historical_window_logical_batch(
                                    claim=claim,
                                    logical_root=root,
                                    spool=candidate,
                                )
                            )
                    self.assertEqual(
                        (
                            caught.exception.reason_code,
                            caught.exception.failure_kind,
                        ),
                        (
                            "authority_mismatch",
                            "historical_window_capability_invalid",
                        ),
                    )
                    self.assertEqual(scope_core.call_count, 0)
                    self.assertEqual(probe_calls, [])
                    self.assertIsNone(context._reserved_scope)
                    self.assertIsNone(context._active_scope)
                finally:
                    for logical in opened:
                        try:
                            logical.__exit__(None, None, None)
                        except rpc._ArchiveRpcError:
                            pass
                    claim.close()
                    bound_spool.close()
                    if other is not None:
                        other.close()

    def test_unissued_semantic_roots_reject_before_scope_or_transport(self):
        import scripts.historical_foundry_scan
        import scripts.historical_foundry_storage as storage

        simple_one = tuple(_simple_request_rows())
        cases = {
            "anchor_count": {
                "schema": "historical_foundry_anchor_stage_logical_root/v1",
                "segment": "anchor_stage",
                "stage_index": 0,
                "stage_name": "anchor",
                "logical_batch_index": 1,
                "requests": tuple(_simple_request_rows(2)),
                "allow_http_413_bisection": False,
            },
            "anchor_stage": {
                "schema": "historical_foundry_anchor_stage_logical_root/v1",
                "segment": "anchor_stage",
                "stage_index": 2,
                "stage_name": "derived_authority",
                "logical_batch_index": 1,
                "requests": simple_one,
                "allow_http_413_bisection": False,
            },
            "request_method": {
                "schema": "historical_foundry_anchor_stage_logical_root/v1",
                "segment": "anchor_stage",
                "stage_index": 0,
                "stage_name": "anchor",
                "logical_batch_index": 1,
                "requests": ({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_getBalance",
                    "params": ["0x" + "11" * 20, "latest"],
                },),
                "allow_http_413_bisection": False,
            },
            "request_id": {
                "schema": "historical_foundry_anchor_stage_logical_root/v1",
                "segment": "anchor_stage",
                "stage_index": 0,
                "stage_name": "anchor",
                "logical_batch_index": 1,
                "requests": tuple(_simple_request_rows(first_id=2)),
                "allow_http_413_bisection": False,
            },
            "lower_block": {
                "schema": "historical_foundry_lower_observation_logical_root/v1",
                "segment": "lower_observation",
                "observation_index": 0,
                "observation_kind": "search_probe",
                "kind_index": 0,
                "logical_batch_index": 1,
                "block_number": 5,
                "requests": ({
                    "jsonrpc": "2.0",
                    "id": 49,
                    "method": "eth_getBlockByNumber",
                    "params": ["0x4", False],
                },),
                "allow_http_413_bisection": False,
            },
            "window_order": {
                "schema": "historical_foundry_window_logical_root/v1",
                "segment": "window_root",
                "root_index": 0,
                "kind": "header",
                "block_start": 1,
                "block_stop": 2,
                "logical_batch_index": 1,
                "requests": tuple(reversed(_simple_request_rows(2))),
                "allow_http_413_bisection": True,
            },
        }
        for name, root in cases.items():
            with self.subTest(case=name):
                claim, context, _preflight = self._open_claim()
                spool = storage._open_historical_window_exchange_spool(
                    data_dir=self.data_dir
                )
                rpc._bind_claimed_historical_window_sources_to_spool(
                    claim=claim, spool=spool
                )
                opened = []
                try:
                    with mock.patch.object(
                        rpc, "_open_archive_scope", wraps=rpc._open_archive_scope
                    ) as scope_core, mock.patch.object(
                        rpc,
                        "_perform_archive_attempt",
                        side_effect=AssertionError("transport entered"),
                    ) as transport:
                        with self.assertRaises(
                            rpc._ArchiveRpcError
                        ) as caught:
                            opened.append(
                                rpc._open_production_archive_rpc_historical_window_logical_batch(
                                    claim=claim,
                                    logical_root=root,
                                    spool=spool,
                                )
                            )
                    self.assertEqual(
                        (
                            caught.exception.reason_code,
                            caught.exception.failure_kind,
                        ),
                        ("authority_mismatch", "logical_batch_scope_invalid"),
                    )
                    self.assertEqual(scope_core.call_count, 0)
                    self.assertEqual(transport.call_count, 0)
                    self.assertIsNone(context._reserved_scope)
                finally:
                    for logical in opened:
                        try:
                            logical.__exit__(None, None, None)
                        except rpc._ArchiveRpcError:
                            pass
                    claim.close()
                    spool.close()

    def test_consumed_scheduler_root_nested_mutation_is_pre_scope_rejected(self):
        import scripts.historical_foundry_scan as scan

        consumer = _closure_function(
            scan._capture_production_historical_window,
            "_consume_scheduler_logical_root",
        )

        def mutate_method(root):
            root["requests"][0]["method"] = "eth_getBalance"

        def mutate_params(root):
            root["requests"][0]["params"].append("forged")

        for mutate in (mutate_method, mutate_params):
            with self.subTest(field=mutate.__name__):
                changed = [False]
                previous = sys.gettrace()

                def tracer(frame, event, _argument):
                    if (
                        not changed[0]
                        and frame.f_code is consumer.__code__
                        and event == "return"
                    ):
                        mutate(frame.f_locals["logical_root"])
                        changed[0] = True
                    return tracer

                try:
                    with mock.patch.object(
                        rpc, "_open_archive_scope", wraps=rpc._open_archive_scope
                    ) as scope_core, mock.patch.object(
                        rpc,
                        "_perform_archive_attempt",
                        wraps=rpc._perform_archive_attempt,
                    ) as transport:
                        sys.settrace(tracer)
                        with self.assertRaises(rpc._ArchiveRpcError) as caught:
                            _run_genuine_historical_window_scheduler()
                    self.assertTrue(changed[0])
                    self.assertEqual(
                        (
                            caught.exception.reason_code,
                            caught.exception.failure_kind,
                        ),
                        (
                            "authority_mismatch",
                            "logical_batch_scope_invalid",
                        ),
                    )
                    self.assertEqual(scope_core.call_count, 0)
                    self.assertEqual(transport.call_count, 0)
                finally:
                    sys.settrace(previous)

    def test_real_held_sources_bind_once_and_duplicates_outlive_original_sources(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        claim, _context, preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        binding = rpc._bind_claimed_historical_window_sources_to_spool(
            claim=claim, spool=spool
        )
        self.assertIs(type(binding), storage._HistoricalWindowSpoolSourceBinding)
        self.assertIs(sys.modules["scripts.historical_foundry_scan"], scan)
        self.assertIs(sys.modules["scripts.historical_foundry_storage"], storage)
        with self.assertRaises(rpc._ArchiveRpcError):
            rpc._bind_claimed_historical_window_sources_to_spool(
                claim=claim, spool=spool
            )
        claim.close()
        self.assertTrue(preflight.closed)
        self.assertIsNone(spool.close())
        self.assertEqual(tuple(self.data_dir.iterdir()), ())

    def test_logical_opener_core_return_controls_retire_all_owners(self):
        import scripts.historical_foundry_storage as storage

        exported = (
            rpc._open_production_archive_rpc_historical_window_logical_batch
        )
        try:
            core = _closure_function(exported, "_open_logical_scope_core")
        except AssertionError:
            core = exported
        lines, start = inspect.getsourcelines(core)
        return_line = start + next(
            index for index, line in enumerate(lines)
            if "return logical" in line
        )
        claim_registry = _closure_named_value(
            rpc._claim_fresh_production_archive_rpc_run_for_historical_window,
            "claim_registry",
        )
        logical_registry = _closure_named_value(
            exported, "logical_scope_registry"
        )
        active_registry = _closure_named_value(
            storage._HistoricalWindowExchangeSpool.close, "active_registry"
        )
        control_factories = (
            lambda: KeyboardInterrupt("logical-open-return-keyboard"),
            lambda: SystemExit("logical-open-return-system"),
            lambda: GeneratorExit("logical-open-return-generator"),
            lambda: asyncio.CancelledError("logical-open-return-cancelled"),
        )
        for control_factory in control_factories:
            control = control_factory()
            with self.subTest(control=type(control).__name__):
                captured = {}
                fired = [False]
                prior_trace = sys.gettrace()

                def tracer(frame, event, _argument):
                    if (
                        not fired[0]
                        and frame.f_code is core.__code__
                        and event == "line"
                        and frame.f_lineno == return_line
                    ):
                        fired[0] = True
                        captured["logical"] = frame.f_locals["logical"]
                        captured["claim"] = frame.f_locals["claim"]
                        captured["context"] = frame.f_locals["context"]
                        captured["spool"] = frame.f_locals["spool"]
                        captured["preflight"] = frame.f_locals[
                            "context"
                        ]._preflight
                        sys.settrace(prior_trace)
                        raise control
                    return tracer

                try:
                    sys.settrace(tracer)
                    with self.assertRaises(type(control)) as caught:
                        _run_genuine_historical_window_scheduler()
                    self.assertIs(caught.exception, control)
                    self.assertTrue(fired[0])
                    self.assertEqual(captured["context"]._state, "failed")
                    self.assertTrue(captured["preflight"].closed)
                    self.assertIsNone(
                        captured["context"]._historical_window_consumer
                    )
                    self.assertNotIn(id(captured["claim"]), claim_registry)
                    self.assertNotIn(
                        id(captured["logical"]), logical_registry
                    )
                    self.assertNotIn(id(captured["spool"]), active_registry)
                finally:
                    sys.settrace(prior_trace)

    def test_logical_enter_core_return_controls_retire_all_owners(self):
        import scripts.historical_foundry_storage as storage

        claim_registry = _closure_named_value(
            rpc._claim_fresh_production_archive_rpc_run_for_historical_window,
            "claim_registry",
        )
        logical_registry = _closure_named_value(
            rpc._open_production_archive_rpc_historical_window_logical_batch,
            "logical_scope_registry",
        )
        active_registry = _closure_named_value(
            storage._HistoricalWindowExchangeSpool.close, "active_registry"
        )
        control_factories = (
            lambda: KeyboardInterrupt("logical-enter-return-keyboard"),
            lambda: SystemExit("logical-enter-return-system"),
            lambda: GeneratorExit("logical-enter-return-generator"),
            lambda: asyncio.CancelledError("logical-enter-return-cancelled"),
        )
        for control_factory in control_factories:
            control = control_factory()
            with self.subTest(control=type(control).__name__):
                exported = (
                    rpc._ProductionHistoricalWindowLogicalBatchScope.__enter__
                )
                try:
                    core = _closure_function(
                        exported, "_enter_logical_scope_core"
                    )
                except AssertionError:
                    core = exported
                lines, start = inspect.getsourcelines(core)
                return_line = start + next(
                    index for index, line in enumerate(lines)
                    if "return self" in line
                )
                fired = [False]
                captured = {}
                prior_trace = sys.gettrace()

                def tracer(frame, event, _argument):
                    if (
                        not fired[0]
                        and frame.f_code is core.__code__
                        and event == "line"
                        and frame.f_lineno == return_line
                    ):
                        fired[0] = True
                        record = frame.f_locals["logical_record"]
                        captured["logical"] = frame.f_locals["self"]
                        captured["claim"] = record["claim"]
                        captured["context"] = record["context"]
                        captured["spool"] = record["spool"]
                        captured["preflight"] = record["context"]._preflight
                        sys.settrace(prior_trace)
                        raise control
                    return tracer

                try:
                    sys.settrace(tracer)
                    with self.assertRaises(type(control)) as caught:
                        _run_genuine_historical_window_scheduler()
                    self.assertIs(caught.exception, control)
                    self.assertTrue(fired[0])
                    self.assertEqual(captured["context"]._state, "failed")
                    self.assertTrue(captured["preflight"].closed)
                    self.assertIsNone(
                        captured["context"]._historical_window_consumer
                    )
                    self.assertNotIn(id(captured["claim"]), claim_registry)
                    self.assertNotIn(
                        id(captured["logical"]), logical_registry
                    )
                    self.assertNotIn(id(captured["spool"]), active_registry)
                finally:
                    sys.settrace(prior_trace)

    def test_resolver_substeps_and_private_capsule_calls_are_not_free_authority(self):
        import scripts.historical_foundry_storage as storage

        claim, _context, _preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        for call in (
            lambda: rpc._bind_claimed_historical_window_scan_source_module(
                claim=claim
            ),
            lambda: rpc._bind_claimed_historical_window_storage_source_module(
                claim=claim, module=storage
            ),
            lambda: rpc._consume_claimed_historical_window_source_capsule_for_storage(
                capsule=object(), expected_claim=claim, expected_spool=spool,
                expected_storage_module=storage,
            ),
        ):
            with self.assertRaises(rpc._ArchiveRpcError):
                call()
        claim.close()
        spool.close()

    def test_held_source_close_drains_all_descriptors_after_first_control(self):
        sources = rpc._HeldArchiveSourceAuthority(
            rpc.Path(rpc.__file__).resolve().parents[1]
        )
        sources.open_members()
        expected = tuple(
            row[0] for row in sources.files.values()
        ) + tuple(
            row[0] for _key, row in reversed(tuple(sources.directories.items()))
        )
        cancellation = KeyboardInterrupt("source-close-control")
        original_close = rpc.os.close
        attempted = []

        def interrupt_first(fd):
            original_close(fd)
            attempted.append(fd)
            if len(attempted) == 1:
                raise cancellation

        with mock.patch.object(rpc.os, "close", side_effect=interrupt_first):
            with self.assertRaises(KeyboardInterrupt) as caught:
                sources.close()
        self.assertIs(caught.exception, cancellation)
        self.assertEqual(tuple(attempted), expected)
        self.assertTrue(sources.closed)
        self.assertEqual(sources.files, {})
        self.assertEqual(sources.directories, {})
        self.assertIsNone(sources.close())

    def test_held_source_close_resumes_trace_before_persisted_fd_attempt(self):
        original_close = rpc.os.close
        method_lines, method_start = inspect.getsourcelines(
            rpc._HeldArchiveSourceAuthority.close
        )
        persisted_offsets = tuple(
            index for index, line in enumerate(method_lines)
            if 'state["attempted_fds"].add(fd);' in line
        )
        target_offset = (
            persisted_offsets[0]
            if persisted_offsets
            else next(
                index for index, line in enumerate(method_lines)
                if "for row in file_rows:" in line
            )
        )
        target_line = method_start + target_offset

        for occurrence in (1, 3):
            with self.subTest(occurrence=occurrence):
                sources = rpc._HeldArchiveSourceAuthority(
                    rpc.Path(rpc.__file__).resolve().parents[1]
                )
                sources.open_members()
                expected = tuple(
                    row[0] for row in sources.files.values()
                ) + tuple(
                    row[0] for _key, row in reversed(
                        tuple(sources.directories.items())
                    )
                )
                cancellation = GeneratorExit(
                    "source-close-before-persisted-attempt"
                )
                attempted = []
                visits = [0]
                prior_trace = sys.gettrace()

                def close_fd(fd):
                    attempted.append(fd)
                    return original_close(fd)

                def tracer(frame, event, _arg):
                    if (
                        frame.f_code is rpc._HeldArchiveSourceAuthority.close.__code__
                        and event == "line"
                        and frame.f_lineno == target_line
                    ):
                        visits[0] += 1
                        if visits[0] == occurrence:
                            sys.settrace(prior_trace)
                            raise cancellation
                    return tracer

                try:
                    with mock.patch.object(rpc.os, "close", side_effect=close_fd):
                        sys.settrace(tracer)
                        with self.assertRaises(GeneratorExit) as caught:
                            sources.close()
                        sys.settrace(prior_trace)
                        sources.close()
                    self.assertIs(caught.exception, cancellation)
                    self.assertEqual(tuple(attempted), expected)
                    self.assertEqual(len(attempted), len(set(attempted)))
                    self.assertTrue(sources.closed)
                finally:
                    sys.settrace(prior_trace)
                    for fd in expected:
                        try:
                            original_close(fd)
                        except OSError:
                            pass

    def test_production_preflight_close_drains_source_after_toolchain_control(self):
        cancellation = SystemExit("toolchain-close-control")
        calls = []

        class Toolchain:
            def _close(self):
                calls.append("toolchain")
                raise cancellation

        class Sources:
            def close(self):
                calls.append("sources")

        preflight = rpc._ProductionArchivePreflight(
            rpc.Path("/private/tmp"), Sources(), object(), Toolchain(),
            object(), {},
        )
        with self.assertRaises(SystemExit) as caught:
            preflight.close()
        self.assertIs(caught.exception, cancellation)
        self.assertEqual(calls, ["toolchain", "sources"])
        self.assertTrue(preflight.closed)
        self.assertIsNone(preflight.close())
        self.assertEqual(calls, ["toolchain", "sources"])

    def test_production_preflight_close_preserves_first_nested_control(self):
        toolchain_control = GeneratorExit("toolchain-first-control")
        source_control = asyncio.CancelledError("source-second-control")
        calls = []

        class Toolchain:
            def __init__(self):
                self.closed = False

            def _close(self):
                calls.append("toolchain")
                self.closed = True
                raise toolchain_control

        class Sources:
            def __init__(self):
                self.closed = False

            def close(self):
                calls.append("sources")
                self.closed = True
                raise source_control

        toolchain = Toolchain()
        sources = Sources()
        preflight = rpc._ProductionArchivePreflight(
            rpc.Path("/private/tmp"), sources, object(), toolchain,
            object(), {},
        )
        with self.assertRaises(GeneratorExit) as caught:
            preflight.close()
        self.assertIs(caught.exception, toolchain_control)
        self.assertEqual(calls, ["toolchain", "sources"])
        self.assertTrue(toolchain.closed)
        self.assertTrue(sources.closed)
        self.assertTrue(preflight.closed)
        self.assertIsNone(preflight.close())

    def test_duplicate_source_failure_drains_every_duplicate_after_control(self):
        import scripts.historical_foundry_storage as storage

        claim, _context, _preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        cancellation = GeneratorExit("duplicate-drain-control")
        original_dup = rpc.os.dup
        original_close = rpc.os.close
        duplicated = []
        attempted = []

        def fail_after_three(fd):
            if len(duplicated) == 3:
                raise OSError("forced duplicate failure")
            result = original_dup(fd)
            duplicated.append(result)
            return result

        def interrupt_first(fd):
            original_close(fd)
            if fd in duplicated:
                attempted.append(fd)
                if len(attempted) == 1:
                    raise cancellation

        with mock.patch.object(
            rpc.os, "dup", side_effect=fail_after_three
        ), mock.patch.object(
            rpc.os, "close", side_effect=interrupt_first
        ):
            with self.assertRaises(GeneratorExit) as caught:
                rpc._bind_claimed_historical_window_sources_to_spool(
                    claim=claim, spool=spool
                )
        self.assertIs(caught.exception, cancellation)
        self.assertEqual(tuple(attempted), tuple(reversed(duplicated)))
        claim.close()
        spool.close()

    def test_duplicate_ordinary_failure_terminalizes_authenticated_spool(self):
        import scripts.historical_foundry_storage as storage

        internal = _closure_function(
            storage._HistoricalWindowExchangeSpool
            ._bind_claimed_source_authority_from_rpc,
            "_bind_claimed_source_authority",
        )
        active_registry = _closure_named_value(
            storage._HistoricalWindowExchangeSpool.close,
            "active_registry",
        )
        binding_registry = _closure_named_value(
            internal, "binding_registry"
        )
        capsule_registry = _closure_named_value(
            rpc._bind_claimed_historical_window_sources_to_spool,
            "capsule_registry",
        )
        baseline = (
            len(active_registry), len(binding_registry), len(capsule_registry)
        )
        claim, context, preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        original_dup = rpc.os.dup
        duplicated = []

        def duplicate_then_fail(fd):
            if duplicated:
                raise OSError("forced authenticated duplicate failure")
            result = original_dup(fd)
            duplicated.append(result)
            return result

        observed = None
        try:
            with mock.patch.object(
                rpc.os, "dup", side_effect=duplicate_then_fail
            ):
                try:
                    rpc._bind_claimed_historical_window_sources_to_spool(
                        claim=claim, spool=spool
                    )
                except BaseException as error:
                    observed = error
            self.assertIs(type(observed), rpc._ArchiveRpcError)
            self.assertEqual(
                (observed.reason_code, observed.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_spool_handoff_failed",
                ),
            )
            self.assertNotEqual(context._state, "active")
            self.assertTrue(preflight.closed)
            self.assertEqual(
                (
                    len(active_registry),
                    len(binding_registry),
                    len(capsule_registry),
                ),
                baseline,
            )
            for fd in duplicated:
                with self.assertRaises(OSError):
                    os.fstat(fd)
        finally:
            claim.close()
            spool.close()
            for fd in duplicated:
                try:
                    os.close(fd)
                except OSError:
                    pass

    def test_rpc_binding_delivery_windows_terminalize_every_live_owner(self):
        import scripts.historical_foundry_storage as storage

        bind = rpc._bind_claimed_historical_window_sources_to_spool
        lines, start = inspect.getsourcelines(bind)
        fragments = (
            "capsule_record = capsule_registry",
            'record["transfer_state_checker"] =',
            'record["logical_root_consumer"] =',
            "pending_duplicate_fds[:] = []",
            'record.pop("candidate", None)',
            "capsule_registry.pop(id(capsule), None)",
            "return binding",
        )
        targets = tuple(
            (
                fragment,
                start + next(
                    index for index, line in enumerate(lines)
                    if fragment in line
                ),
            )
            for fragment in fragments
        )
        internal = _closure_function(
            storage._HistoricalWindowExchangeSpool
            ._bind_claimed_source_authority_from_rpc,
            "_bind_claimed_source_authority",
        )
        active_registry = _closure_named_value(
            storage._HistoricalWindowExchangeSpool.close,
            "active_registry",
        )
        binding_registry = _closure_named_value(
            internal, "binding_registry"
        )
        capsule_registry = _closure_named_value(bind, "capsule_registry")
        failures = (
            RuntimeError("rpc-binding-ordinary"),
            KeyboardInterrupt("rpc-binding-keyboard"),
            SystemExit("rpc-binding-system"),
            GeneratorExit("rpc-binding-generator"),
            asyncio.CancelledError("rpc-binding-cancelled"),
        )
        original_dup = rpc.os.dup

        for fragment, target in targets:
            for failure in failures:
                with self.subTest(
                    target=fragment, failure=type(failure).__name__
                ):
                    baseline = (
                        len(active_registry),
                        len(binding_registry),
                        len(capsule_registry),
                    )
                    claim, context, preflight = self._open_claim()
                    spool = storage._open_historical_window_exchange_spool(
                        data_dir=self.data_dir
                    )
                    duplicated = []
                    prior_trace = sys.gettrace()
                    fired = [False]

                    def duplicate(fd):
                        result = original_dup(fd)
                        duplicated.append(result)
                        return result

                    def tracer(frame, event, _argument):
                        if (
                            not fired[0]
                            and frame.f_code is bind.__code__
                            and event == "line"
                            and frame.f_lineno == target
                        ):
                            fired[0] = True
                            sys.settrace(prior_trace)
                            raise failure
                        return tracer

                    try:
                        with mock.patch.object(
                            rpc.os, "dup", side_effect=duplicate
                        ):
                            sys.settrace(tracer)
                            with self.assertRaises(BaseException) as caught:
                                bind(claim=claim, spool=spool)
                        if isinstance(failure, Exception):
                            self.assertIs(
                                type(caught.exception), rpc._ArchiveRpcError
                            )
                            self.assertEqual(
                                (
                                    caught.exception.reason_code,
                                    caught.exception.failure_kind,
                                ),
                                (
                                    "authority_mismatch",
                                    "historical_window_spool_handoff_failed",
                                ),
                            )
                        else:
                            self.assertIs(caught.exception, failure)
                        self.assertTrue(fired[0])
                        self.assertNotEqual(context._state, "active")
                        self.assertTrue(preflight.closed)
                        self.assertEqual(
                            (
                                len(active_registry),
                                len(binding_registry),
                                len(capsule_registry),
                            ),
                            baseline,
                        )
                        for fd in duplicated:
                            with self.assertRaises(OSError):
                                os.fstat(fd)
                    finally:
                        sys.settrace(prior_trace)
                        claim.close()
                        spool.close()
                        for fd in duplicated:
                            try:
                                os.close(fd)
                            except OSError:
                                pass

    def test_duplicate_source_delivery_has_no_unowned_fd_trace_window(self):
        import scripts.historical_foundry_storage as storage

        duplicate_sources = next(
            cell.cell_contents
            for cell in (
                rpc._bind_claimed_historical_window_sources_to_spool.__closure__
                or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "duplicate_claimed_sources"
        )
        duplicate_helper = next(
            (
                cell.cell_contents
                for cell in (duplicate_sources.__closure__ or ())
                if callable(cell.cell_contents)
                and getattr(cell.cell_contents, "__name__", "")
                == "duplicate_into_ledger"
            ),
            None,
        )
        if duplicate_helper is None:
            duplicate_code = duplicate_sources.__code__
            lines, start = inspect.getsourcelines(duplicate_sources)
            duplicate_line = start + next(
                index for index, line in enumerate(lines)
                if "duplicate_fds.append(duplicated)" in line
            )
        else:
            duplicate_code = duplicate_helper.__code__
            lines, start = inspect.getsourcelines(duplicate_helper)
            duplicate_line = start + next(
                index for index, line in enumerate(lines)
                if "return duplicated" in line
            )
        bind_lines, bind_start = inspect.getsourcelines(
            rpc._bind_claimed_historical_window_sources_to_spool
        )
        bind_targets = {
            "returned_payload": bind_start + next(
                index for index, line in enumerate(bind_lines)
                if "capsule = _ClaimedHistoricalWindowSourceCapsule(" in line
            ),
            "constructed_capsule": bind_start + next(
                index for index, line in enumerate(bind_lines)
                if "capsule_registry[id(capsule)] =" in line
            ),
        }
        targets = (("duplicate_return", duplicate_code, duplicate_line),) + tuple(
            (name, rpc._bind_claimed_historical_window_sources_to_spool.__code__, line)
            for name, line in bind_targets.items()
        )
        controls = (
            KeyboardInterrupt("duplicate-window-keyboard"),
            SystemExit("duplicate-window-system-exit"),
            GeneratorExit("duplicate-window-generator"),
            asyncio.CancelledError("duplicate-window-cancelled"),
        )
        original_dup = rpc.os.dup
        original_close = rpc.os.close

        for target_name, target_code, target_line in targets:
            for control in controls:
                with self.subTest(
                    target=target_name, control=type(control).__name__
                ):
                    claim, _context, _preflight = self._open_claim()
                    spool = storage._open_historical_window_exchange_spool(
                        data_dir=self.data_dir
                    )
                    duplicated = []
                    attempted = []
                    prior_trace = sys.gettrace()

                    def duplicate(fd):
                        result = original_dup(fd)
                        duplicated.append(result)
                        return result

                    def close_fd(fd):
                        if fd in duplicated:
                            attempted.append(fd)
                        return original_close(fd)

                    fired = [False]

                    def tracer(frame, event, _arg):
                        if (
                            not fired[0]
                            and frame.f_code is target_code
                            and event == "line"
                            and frame.f_lineno == target_line
                        ):
                            fired[0] = True
                            sys.settrace(prior_trace)
                            raise control
                        return tracer

                    try:
                        with mock.patch.object(
                            rpc.os, "dup", side_effect=duplicate
                        ), mock.patch.object(
                            rpc.os, "close", side_effect=close_fd
                        ):
                            sys.settrace(tracer)
                            with self.assertRaises(type(control)) as caught:
                                rpc._bind_claimed_historical_window_sources_to_spool(
                                    claim=claim, spool=spool
                                )
                        self.assertIs(caught.exception, control)
                        self.assertTrue(fired[0])
                        self.assertGreater(len(duplicated), 0)
                        self.assertEqual(
                            sorted(attempted), sorted(duplicated)
                        )
                        self.assertEqual(len(attempted), len(set(attempted)))
                    finally:
                        sys.settrace(prior_trace)
                        for fd in duplicated:
                            try:
                                original_close(fd)
                            except OSError:
                                pass
                        claim.close()
                        spool.close()

    def test_binding_delivery_control_terminalizes_consumed_half_binding(self):
        import scripts.historical_foundry_storage as storage

        claim, context, preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        internal = next(
            cell.cell_contents
            for cell in (
                storage._HistoricalWindowExchangeSpool
                ._bind_claimed_source_authority_from_rpc.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_bind_claimed_source_authority"
        )
        rollback = next(
            cell.cell_contents
            for cell in (
                storage._HistoricalWindowExchangeSpool
                ._bind_claimed_source_authority_from_rpc.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_rollback_claimed_source_binding_delivery"
        )
        cancellation = GeneratorExit("binding-delivery-control")
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if frame.f_code is internal.__code__ and event == "return":
                sys.settrace(prior_trace)
                raise cancellation
            return tracer

        try:
            sys.settrace(tracer)
            with self.assertRaises(GeneratorExit) as caught:
                rpc._bind_claimed_historical_window_sources_to_spool(
                    claim=claim, spool=spool
                )
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught.exception, cancellation)
        self.assertNotEqual(context._state, "active")
        self.assertTrue(preflight.closed)
        with self.assertRaises(rpc._ArchiveRpcError) as second:
            rpc._bind_claimed_historical_window_sources_to_spool(
                claim=claim, spool=spool
            )
        self.assertEqual(
            (second.exception.reason_code, second.exception.failure_kind),
            ("authority_mismatch", "context_closed"),
        )
        claim.close()
        spool.close()

    def test_binding_rollback_keeps_registry_live_through_fd_revocation(self):
        import scripts.historical_foundry_storage as storage

        internal = next(
            cell.cell_contents
            for cell in (
                storage._HistoricalWindowExchangeSpool
                ._bind_claimed_source_authority_from_rpc.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_bind_claimed_source_authority"
        )
        rollback = next(
            cell.cell_contents
            for cell in (
                storage._HistoricalWindowExchangeSpool
                ._bind_claimed_source_authority_from_rpc.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_rollback_claimed_source_binding_delivery"
        )
        controls = (
            KeyboardInterrupt("binding-revoke-keyboard"),
            SystemExit("binding-revoke-system"),
            GeneratorExit("binding-revoke-generator"),
            asyncio.CancelledError("binding-revoke-cancelled"),
        )
        for cancellation in controls:
            with self.subTest(control=type(cancellation).__name__):
                claim, _context, _preflight = self._open_claim()
                spool = storage._open_historical_window_exchange_spool(
                    data_dir=self.data_dir
                )
                captured_fds = []
                guards = []
                prior_trace = sys.gettrace()

                def capture_guard(frame, event, _argument):
                    if frame.f_code is internal.__code__ and event == "return":
                        guard_cell = frame.f_locals.get("delivery_guard")
                        if type(guard_cell) is list and guard_cell[0] is not None:
                            guards.append(guard_cell[0])
                    return capture_guard

                sys.settrace(capture_guard)
                try:
                    rpc._bind_claimed_historical_window_sources_to_spool(
                        claim=claim, spool=spool
                    )
                finally:
                    sys.settrace(prior_trace)
                self.assertEqual(len(guards), 1)

                def tracer(frame, event, _argument):
                    if frame.f_code is rollback.__code__ and event == "line":
                        local = frame.f_locals
                        binding = local.get("binding")
                        record = local.get("binding_record")
                        registry = local.get("binding_registry")
                        if (
                            binding is not None
                            and type(record) is dict
                            and type(registry) is dict
                            and (
                                id(binding) not in registry
                                or record.get("state") == "closing"
                            )
                            and record.get("state") != "closed"
                        ):
                            captured_fds.extend(
                                row[1]
                                for row in record["ancestry_rows"]
                                + record["source_rows"]
                            )
                            sys.settrace(prior_trace)
                            raise cancellation
                    return tracer

                try:
                    sys.settrace(tracer)
                    with self.assertRaises(type(cancellation)) as caught:
                        rollback(
                            spool,
                            guards[0],
                            RuntimeError("force binding rollback"),
                        )
                finally:
                    sys.settrace(prior_trace)
                self.assertIs(caught.exception, cancellation)
                self.assertTrue(captured_fds)
                spool.close()
                claim.close()
                for fd in captured_fds:
                    with self.assertRaises(OSError):
                        os.fstat(fd)


class HistoricalFoundryRpcTask3bHandoffTests(unittest.TestCase):
    def test_bound_singleton_root_handoffs_before_compact_record_install(self):
        import scripts.historical_foundry_storage as storage
        original_attempt = (
            rpc._production_archive_rpc_historical_window_logical_batch_attempt
        )
        logical_registry = _closure_named_value(
            original_attempt, "logical_scope_registry"
        )
        captured = {}

        def observe(*, logical_scope, request_rows):
            rows, receipt = original_attempt(
                logical_scope=logical_scope, request_rows=request_rows
            )
            if not captured:
                record = logical_registry[id(logical_scope)][1]
                context = record["context"]
                captured["rows"] = rows
                captured["receipt"] = receipt
                captured["compact"] = dict(
                    context._records[-1]._projection
                )
            return rows, receipt

        with mock.patch.object(
            rpc,
            "_production_archive_rpc_historical_window_logical_batch_attempt",
            side_effect=observe,
        ):
            _run_genuine_historical_window_scheduler()
        self.assertTrue(captured["rows"])
        self.assertIs(
            type(captured["receipt"]), storage._HistoricalWindowSpoolReceipt
        )
        compact = captured["compact"]
        self.assertEqual(
            compact["schema"],
            "historical_foundry_archive_rpc_spooled_success_exchange/v1",
        )
        self.assertEqual(len(compact), 16)
        self.assertFalse(any(
            key in compact
            for key in (
                "canonical_request_bytes", "decoded_response_bytes",
                "receipt", "transfer", "body",
            )
        ))

    def test_rpc_attempt_delivery_guard_terminalizes_at_each_handoff_boundary(self):
        import scripts.historical_foundry_storage as storage

        stages = (
            ("after_issue", "pending = record[\"spool\"].append_transfer"),
            ("after_append", "record[\"spool\"].verify_pending_receipt"),
            ("after_pending_verify", "receipt = record[\"spool\"].commit_transfer"),
            ("after_commit", "record[\"spool\"].verify_committed_receipt"),
            ("after_committed_verify", "record[\"spool\"].release_verified_transfer"),
            ("after_release", "compact = dict(receipt)"),
            ("after_compact", "context._records.append"),
            ("after_record_install", "context, \"_next_exchange_index\""),
            ("inner_return", None),
        )
        active_registry = _closure_named_value(
            storage._HistoricalWindowExchangeSpool.close, "active_registry"
        )
        for stage, marker in stages:
            with self.subTest(stage=stage):
                prior_trace = sys.gettrace()
                captured = {}
                cancellation = GeneratorExit(
                    "rpc-handoff-delivery-{}".format(stage)
                )
                fired = [False]

                def tracer(frame, event, argument):
                    if frame.f_code.co_filename != rpc.__file__:
                        return tracer
                    if frame.f_code.co_name not in (
                        "attempt_logical_scope",
                        "_attempt_logical_scope_core",
                    ):
                        return tracer
                    should_fire = (
                        marker is None
                        and frame.f_code.co_name
                        == "_attempt_logical_scope_core"
                        and event == "return"
                    ) or (
                        marker is not None
                        and event == "line"
                        and marker in linecache.getline(
                            frame.f_code.co_filename, frame.f_lineno
                        )
                    )
                    if should_fire and not fired[0]:
                        fired[0] = True
                        record = frame.f_locals["record"]
                        captured["context"] = frame.f_locals["context"]
                        captured["claim"] = record["claim"]
                        captured["spool"] = record["spool"]
                        sys.settrace(prior_trace)
                        raise cancellation
                    return tracer

                try:
                    sys.settrace(tracer)
                    with self.assertRaises(GeneratorExit) as caught:
                        _run_genuine_historical_window_scheduler()
                    self.assertIs(caught.exception, cancellation)
                    self.assertTrue(fired[0])
                    self.assertEqual(captured["context"]._state, "failed")
                    self.assertEqual(captured["context"]._records, [])
                    self.assertNotIn(
                        id(captured["spool"]), active_registry
                    )
                finally:
                    sys.settrace(prior_trace)


class HistoricalFoundryRpcTask3bLogicalScopeTests(unittest.TestCase):
    def test_logical_wrapper_rejects_direct_construction_and_bad_root(self):
        with self.assertRaises(rpc._ArchiveRpcError):
            rpc._ProductionHistoricalWindowLogicalBatchScope()
        with self.assertRaises(rpc._ArchiveRpcError):
            rpc._open_production_archive_rpc_historical_window_logical_batch(
                claim=object(), logical_root={}, spool=object()
            )


class HistoricalFoundryRpcTask3bFinalizationTests(
    HistoricalFoundryRpcTask3bSourceBindingTests
):
    def test_claimed_core_permit_is_one_shot_against_same_thread_reentry(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        original_require = rpc._require_archive_context
        fired = {"open": False, "finalize": False}
        observed = {}

        def require_with_reentry(context, expected):
            frame = sys._getframe()
            callers = []
            while frame is not None:
                callers.append(frame.f_code.co_name)
                frame = frame.f_back
            kind = None
            operation = None
            if "_open_archive_scope" in callers and not fired["open"]:
                kind = "open"
                operation = lambda: rpc._open_archive_scope(
                    context, _simple_request_rows(), implicit=False
                )
            elif (
                "_finalize_production_archive_rpc_run_unlocked" in callers
                and not fired["finalize"]
            ):
                kind = "finalize"
                operation = lambda: rpc._finalize_production_archive_rpc_run_unlocked(
                    context
                )
            if kind is not None:
                fired[kind] = True
                try:
                    operation()
                except rpc._ArchiveRpcError as error:
                    observed[kind] = (
                        error.reason_code, error.failure_kind
                    )
                else:
                    observed[kind] = "entered"
            return original_require(context, expected)

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        with mock.patch.object(
            rpc, "_require_archive_context", side_effect=require_with_reentry
        ):
            case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertEqual(fired, {"open": True, "finalize": True})
        self.assertEqual(observed, {
            "open": (
                "authority_mismatch",
                "historical_window_specialized_batch_required",
            ),
            "finalize": (
                "authority_mismatch",
                "historical_window_specialized_batch_required",
            ),
        })

    def test_bound_source_generation_is_rechecked_at_owner_boundaries(self):
        import scripts.historical_foundry_scan as scan
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        capture_lines, capture_start = inspect.getsourcelines(
            capture_core
        )
        markers = (
            "finalization = (",
            "sealed_spool = spool.seal()",
            "reconciliation = reconcile(",
            "capability = sealed_spool.mint_production_historical_window_capability(",
        )
        targets = {
            marker: capture_start + next(
                index for index, line in enumerate(capture_lines)
                if marker in line
            )
            for marker in markers
        }
        original_generation = rpc._HISTORICAL_WINDOW_MODULE_GENERATION

        for marker, target_line in targets.items():
            with self.subTest(boundary=marker):
                changed = [False]
                prior_trace = sys.gettrace()

                def tracer(frame, event, _arg):
                    if (
                        not changed[0]
                        and frame.f_code is capture_core.__code__
                        and event == "line"
                        and frame.f_lineno == target_line
                    ):
                        changed[0] = True
                        rpc._HISTORICAL_WINDOW_MODULE_GENERATION = object()
                    return tracer

                case = HistoricalFoundryScanTask3bIntegratedTests(
                    methodName=(
                        "test_scheduler_owns_complete_offline_run_through_capability_delivery"
                    )
                )
                try:
                    sys.settrace(tracer)
                    with self.assertRaises(rpc._ArchiveRpcError) as caught:
                        case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                finally:
                    sys.settrace(prior_trace)
                    rpc._HISTORICAL_WINDOW_MODULE_GENERATION = original_generation
                self.assertTrue(changed[0])
                self.assertEqual(
                    (caught.exception.reason_code, caught.exception.failure_kind),
                    ("authority_mismatch", "final_identity_drift"),
                )

    def test_handoff_delivery_controls_terminalize_before_any_rows_escape(self):
        import scripts.historical_foundry_storage as storage
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        methods = (
            "issue_transfer_from_bound_rpc",
            "verify_pending_receipt",
            "commit_transfer",
            "verify_committed_receipt",
            "release_verified_transfer",
        )
        for method_name in methods:
            with self.subTest(boundary=method_name):
                target = getattr(
                    storage._HistoricalWindowExchangeSpool, method_name
                )
                cancellation = GeneratorExit(
                    "handoff-delivery-{}".format(method_name)
                )
                captured = {"context": None, "spool": None}
                fired = [False]
                prior_trace = sys.gettrace()

                def tracer(frame, event, _arg):
                    if (
                        frame.f_code.co_name
                        in ("attempt_logical_scope", "_attempt_logical_scope_core")
                        and frame.f_locals.get("context") is not None
                    ):
                        captured["context"] = frame.f_locals["context"]
                    if (
                        not fired[0]
                        and frame.f_code is target.__code__
                        and event == "return"
                    ):
                        fired[0] = True
                        captured["spool"] = frame.f_locals["self"]
                        sys.settrace(prior_trace)
                        raise cancellation
                    return tracer

                case = HistoricalFoundryScanTask3bIntegratedTests(
                    methodName=(
                        "test_scheduler_owns_complete_offline_run_through_capability_delivery"
                    )
                )
                try:
                    sys.settrace(tracer)
                    with self.assertRaises(GeneratorExit) as caught:
                        case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                finally:
                    sys.settrace(prior_trace)
                self.assertIs(caught.exception, cancellation)
                self.assertTrue(fired[0])
                self.assertIsNotNone(captured["context"])
                self.assertEqual(captured["context"]._state, "failed")
                self.assertEqual(captured["context"]._records, [])
                self.assertEqual(
                    captured["context"]._next_exchange_index, 1
                )
                self.assertIsNone(captured["spool"].close())

    def test_forged_prefinalization_rejects_as_final_identity_drift(self):
        import scripts.historical_foundry_storage as storage

        claim, context, _preflight = self._open_claim()
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        rpc._bind_claimed_historical_window_sources_to_spool(
            claim=claim, spool=spool
        )
        with self.assertRaises(rpc._ArchiveRpcError) as caught:
            rpc._finalize_claimed_production_archive_rpc_run_for_historical_window(
                claim=claim, prefinalization=object()
            )
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "final_identity_drift"),
        )
        self.assertEqual(context._state, "failed")
        claim.close()
        spool.close()

    def test_exact_prefinalization_enters_legacy_finalizer_once_and_seals(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        case.test_scheduler_owns_complete_offline_run_through_capability_delivery()


class HistoricalFoundryRpcTask3bBridgeTests(unittest.TestCase):
    def test_direct_legacy_finalization_clone_is_not_claimed_authority(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        with self.assertRaises(rpc._ArchiveRpcError):
            rpc._verify_claimed_historical_window_finalization(
                claim=object(),
                finalization=object(),
                expected_prefinalization=object(),
                expected_receipt_inventory_sha256="0" * 64,
            )
        with self.assertRaises(storage.HistoricalFoundryStorageError):
            storage.consume_production_historical_window_capability(
                capability=object()
            )


class HistoricalFoundryRpcTask3bIntegratedTests(unittest.TestCase):
    def test_task3b_compact_schema_contains_no_raw_authority_names(self):
        forbidden = (
            "canonical_request_bytes", "decoded_response_bytes", "body",
            "transfer", "pending_receipt", "receipt",
        )
        attempt = (
            rpc._production_archive_rpc_historical_window_logical_batch_attempt
        )
        implementation = next(
            cell.cell_contents
            for cell in attempt.__closure__ or ()
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_attempt_logical_scope_core"
        )
        source = inspect.getsource(implementation)
        self.assertIn(
            "historical_foundry_archive_rpc_spooled_success_exchange/v1",
            source,
        )
        self.assertNotIn("spool_receipt_sha256", source)
        self.assertTrue(all(type(name) is str for name in forbidden))


class HistoricalFoundryRpcRunBoundaryTests(unittest.TestCase):
    def _run(
        self,
        operation=None,
        *,
        endpoint="https://RPC.Example.invalid/archive?key=Opaque%2FValue",
        clock=None,
        entropy=None,
        checkpoint=None,
    ):
        if operation is None:
            operation = lambda _body, _timeout: self.fail("unexpected transport")
        if clock is None:
            clock = _MutableClock()
        if entropy is None:
            entropy = lambda count: b"k" * count
        if checkpoint is None:
            checkpoint = lambda _name: None
        preflight = _issue_archive_rpc_test_preflight_for_test(checkpoint)
        return _issue_archive_rpc_test_run_for_test(
            endpoint=endpoint,
            operation=operation,
            monotonic=clock,
            entropy=entropy,
            preflight=preflight,
        )

    def assertArchiveError(self, callable_value, expected):
        with self.assertRaises(rpc._ArchiveRpcError) as raised:
            callable_value()
        error = raised.exception
        self.assertEqual((error.reason_code, error.failure_kind), expected)
        self.assertEqual(
            str(error),
            "historical archive RPC failure: {}/{}".format(*expected),
        )
        self.assertEqual(error.args, (str(error),))
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__)
        self.assertTrue(error.__suppress_context__)
        self.assertEqual(
            vars(error),
            {"reason_code": expected[0], "failure_kind": expected[1]},
        )
        with self.assertRaises(AttributeError):
            error.reason_code = "changed"
        with self.assertRaises(AttributeError):
            error.detail = "secret"
        return error

    def _endpoint_identity(self, endpoint):
        context = self._run(endpoint=endpoint)
        finalization = _project_archive_rpc_test_finalization_for_test(context)
        return dict(finalization)["identity"]["endpoint_identity"]

    def test_endpoint_hmac_canonical_known_answers_and_closed_grammar(self):
        implicit = self._endpoint_identity("HTTPS://Example.COM")
        explicit = self._endpoint_identity("https://example.com:443/")
        self.assertEqual(implicit, explicit)
        canonical = _canonical_bytes({
            "host": "example.com",
            "path": "/",
            "port": 443,
            "query": "",
            "scheme": "https",
        })
        self.assertEqual(
            implicit,
            {
                "schema": "historical_foundry_rpc_endpoint_identity/v1",
                "scope": "single_run_nonreversible",
                "endpoint_hmac_sha256": hmac.new(
                    b"k" * 32, canonical, hashlib.sha256
                ).hexdigest(),
            },
        )
        self.assertEqual(
            self._endpoint_identity("https://[2001:0DB8:0:0::1]/x?A=%2F"),
            self._endpoint_identity("https://[2001:db8::1]:443/x?A=%2F"),
        )
        self.assertNotEqual(
            self._endpoint_identity("https://127.0.0.1/A?x=1&y=%2F"),
            self._endpoint_identity("https://127.0.0.1/a?y=%2f&x=1"),
        )

        rejected = (
            "", "http://example.com", "https://user@example.com",
            "https://example.com/#frag", "https://example.com/path?",
            "https://example.com/a b", "https://example.com/a\\b",
            "https://example.com/%", "https://example.com/%0g",
            "https://example.com:0", "https://example.com:01",
            "https://example.com:65536", "https://-bad.example",
            "https://bad-.example", "https://bad..example",
            "https://example.com.", "https://éxample.com",
            "https://010.0.0.1", "https://127.1", "https://1.2.3.4.",
            "https://4294967295", "https://256.0.0.1",
            "https://[fe80::1%25en0]", "https://[::ffff:192.0.2.1]",
            "https://2001:db8::1/",
        )
        for endpoint in rejected:
            with self.subTest(endpoint=endpoint):
                entropy_calls = []
                self.assertArchiveError(
                    lambda value=endpoint: self._run(
                        endpoint=value,
                        entropy=lambda count: entropy_calls.append(count) or b"k" * count,
                    ),
                    ("archive_state_unavailable", "endpoint_invalid"),
                )
                self.assertEqual(entropy_calls, [])

    def test_test_types_are_sealed_redacted_noncopyable_and_lifecycle_closed(self):
        preflight = _issue_archive_rpc_test_preflight_for_test(lambda _name: None)
        context = _issue_archive_rpc_test_run_for_test(
            endpoint="https://example.com",
            operation=lambda _body, _timeout: self.fail("unexpected transport"),
            monotonic=_MutableClock(),
            entropy=lambda count: b"q" * count,
            preflight=preflight,
        )
        self.assertEqual(repr(preflight), "_ArchiveRpcTestPreflight(<sealed>)")
        self.assertEqual(repr(context), "_ArchiveRpcTestRunContext(<sealed>)")
        for value in (preflight, context):
            with self.assertRaises((TypeError, ValueError)):
                copy.copy(value)
            with self.assertRaises((TypeError, ValueError)):
                copy.deepcopy(value)
            with self.assertRaises((TypeError, ValueError)):
                pickle.dumps(value)
            with self.assertRaises(AttributeError):
                value.injected = "SECRET"
        with self.assertRaises((TypeError, ValueError)):
            rpc._ArchiveRpcTestRunContext()
        for sealed_type in (
            rpc._ArchiveRpcTestPreflight,
            rpc._ArchiveRpcTestLogicalBatchScope,
            rpc._ArchiveRpcTestSuccessRecord,
            rpc._ArchiveRpcTestResponse,
            rpc._ArchiveRpcTestFinalization,
            rpc._ProductionArchiveRpcRunContext,
            rpc._ProductionArchiveRpcLogicalBatchScope,
            rpc._ProductionArchiveRpcSuccessRecord,
            rpc._ProductionArchiveRpcFinalization,
        ):
            with self.subTest(sealed_type=sealed_type.__name__):
                with self.assertRaises((TypeError, ValueError)):
                    sealed_type()
        self.assertEqual(rpc._ARCHIVE_ERROR_PAIRS, frozenset({
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
        }))
        invalid_responses = (
            dict(status=True, header_items=(), body_chunks=()),
            dict(status=200, header_items=[], body_chunks=()),
            dict(status=200, header_items=(("X", "v"),), body_chunks=[]),
            dict(status=200, header_items=(("X", 1),), body_chunks=()),
            dict(status=200, header_items=(), body_chunks=(bytearray(b"x"),)),
            dict(status=200, header_items=(), body_chunks=(), before_chunk=1),
        )
        for arguments in invalid_responses:
            with self.subTest(response_arguments=arguments):
                self.assertArchiveError(
                    lambda value=arguments: _make_archive_rpc_test_response_for_test(
                        **value
                    ),
                    ("authority_mismatch", "context_invalid"),
                )
        _close_archive_rpc_test_run_for_test(context)
        _close_archive_rpc_test_run_for_test(context)
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(
                context, _simple_request_rows()
            ),
            ("authority_mismatch", "context_closed"),
        )

    def test_preflight_checkpoint_errors_are_sanitized_and_cancellation_propagates(self):
        marker = "SECRET-PREFLIGHT-MARKER"
        calls = []

        def explode(name):
            calls.append(name)
            raise RuntimeError(marker)

        preflight = _issue_archive_rpc_test_preflight_for_test(explode)
        error = self.assertArchiveError(
            lambda: _issue_archive_rpc_test_run_for_test(
                endpoint="https://example.com",
                operation=lambda _body, _timeout: None,
                monotonic=_MutableClock(),
                entropy=lambda count: b"x" * count,
                preflight=preflight,
            ),
            ("authority_mismatch", "preflight_invalid"),
        )
        self.assertNotIn(marker, repr(error))
        self.assertEqual(calls, ["open"])

        for cancellation in (KeyboardInterrupt, SystemExit):
            with self.subTest(cancellation=cancellation.__name__):
                cancel = _issue_archive_rpc_test_preflight_for_test(
                    lambda _name, kind=cancellation: (_ for _ in ()).throw(kind())
                )
                with self.assertRaises(cancellation):
                    _issue_archive_rpc_test_run_for_test(
                        endpoint="https://example.com",
                        operation=lambda _body, _timeout: None,
                        monotonic=_MutableClock(),
                        entropy=lambda count: b"x" * count,
                        preflight=cancel,
                    )

    def test_success_exchange_reorders_rows_and_finalization_schema_is_exact(self):
        requests = [
            {"jsonrpc": "2.0", "id": 7, "method": "eth_chainId", "params": []},
            {"jsonrpc": "2.0", "id": 8, "method": "eth_getBalance",
             "params": [EXECUTOR, "finalized"]},
        ]
        provider_rows = [_success(8, "0x0"), _success(7, "0x1")]
        response = _rpc_response(provider_rows, chunks=(
            _canonical_bytes(provider_rows)[:17],
            _canonical_bytes(provider_rows)[17:],
        ))
        calls = []

        def operation(body, timeout):
            calls.append((body, timeout))
            return response

        context = self._run(operation)
        result = _archive_rpc_test_batch_for_test(context, requests)
        self.assertEqual(result, (_success(7, "0x1"), _success(8, "0x0")))
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], _canonical_bytes(requests))
        self.assertEqual(calls[0][1], 30.0)
        key = context._key
        sealed_record = context._records[0]
        finalization = _project_archive_rpc_test_finalization_for_test(context)
        self.assertEqual(bytes(key), b"\0" * 32)
        self.assertEqual(repr(finalization), "_ArchiveRpcTestFinalization(<sealed>)")
        projected = dict(finalization)
        self.assertEqual(
            set(projected),
            {"schema", "status", "identity", "logical_batches", "successful_exchanges"},
        )
        self.assertEqual(
            projected["schema"],
            "historical_foundry_archive_rpc_run_finalization/v1",
        )
        self.assertEqual(projected["status"], "finalized")
        identity = projected["identity"]
        self.assertEqual(
            set(identity),
            {"schema", "repository_head", "python", "configs", "sources",
             "project_inputs", "toolchain", "executor_artifact",
             "resource_policy", "endpoint_identity", "collection"},
        )
        policy = identity["resource_policy"]
        self.assertEqual(policy, {
            "schema": "historical_foundry_archive_rpc_resource_policy/v1",
            "request_body_bytes": 4_194_304,
            "logical_batch_wire_bytes": 8_388_608,
            "logical_batch_decoded_bytes": 8_388_608,
            "response_header_bytes": 65_536,
            "response_header_rows": 64,
            "json_nodes": 1_048_576,
            "json_scalar_bytes": 8_388_608,
            "json_string_bytes": 262_144,
            "json_depth": 128,
            "json_numeric_token_bytes": 4_096,
            "attempt_deadline_seconds": 30,
            "collection_deadline_seconds": 21_600,
            "request_method": "POST",
            "retry_count": 0,
            "methods": (
                "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash",
                "eth_call", "eth_getCode", "eth_getBalance",
                "eth_getTransactionCount", "eth_getStorageAt", "eth_feeHistory",
            ),
        })
        self.assertEqual(identity["collection"], {
            "logical_batch_count": 1,
            "successful_exchange_count": 1,
            "request_count": 2,
            "response_count": 2,
            "wire_byte_count": len(_canonical_bytes(provider_rows)),
            "decoded_byte_count": len(_canonical_bytes(provider_rows)),
        })
        record = projected["successful_exchanges"][0]
        self.assertEqual(set(record), {
            "schema", "exchange_index", "logical_batch_index", "attempt_index",
            "canonical_request_bytes", "request_byte_count", "request_sha256",
            "request_ids", "wire_byte_count", "wire_sha256",
            "decoded_response_bytes", "decoded_byte_count", "decoded_sha256",
            "response_ids",
        })
        self.assertEqual(record["response_ids"], (8, 7))
        self.assertEqual(record["request_ids"], (7, 8))
        self.assertEqual(
            projected["logical_batches"][0]["success_exchange_indices"],
            (1,),
        )
        secret_text = repr(projected)
        for secret in ("RPC.Example", "Opaque", "/archive", "kkkk"):
            self.assertNotIn(secret, secret_text)
        for value in (finalization,):
            with self.assertRaises((TypeError, ValueError)):
                copy.copy(value)
            with self.assertRaises((TypeError, ValueError)):
                pickle.dumps(value)
            with self.assertRaises(TypeError):
                value["status"] = "changed"
        with self.assertRaises(TypeError):
            finalization._projection["status"] = "changed"
        with self.assertRaises(TypeError):
            sealed_record._projection["request_ids"] = (999,)
        self.assertEqual(context._records, [])
        self.assertEqual(context._logical_summaries, [])
        self.assertIsNone(context._opening_identity)
        self.assertIsNone(context._preflight)

    def test_request_validation_is_closed_and_pretransport(self):
        bad_rows = (
            [],
            [{"jsonrpc": "2.0", "id": True, "method": "eth_chainId", "params": []}],
            [{"jsonrpc": "2.0", "id": 0, "method": "eth_chainId", "params": []}],
            [{"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": []}],
            [{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": [], "x": 1}],
            [{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": {}}],
            [
                {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
                {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
            ],
            [{"jsonrpc": "2.0", "id": 1, "method": "eth_call",
              "params": [{"\ud800": "invalid"}]}],
            [{"jsonrpc": "2.0", "id": 1, "method": "eth_call",
              "params": [10 ** 5000]}],
        )
        for case_index, rows in enumerate(bad_rows):
            with self.subTest(case_index=case_index):
                calls = []
                context = self._run(
                    lambda _body, _timeout: calls.append(True)
                )
                self.assertArchiveError(
                    lambda value=rows: _archive_rpc_test_batch_for_test(
                        context, value
                    ),
                    ("authority_mismatch", "request_invalid"),
                )
                self.assertEqual(calls, [])

        exact = [{
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": ["x" * 262_130 for _index in range(16)],
        }]
        remaining = 4_194_304 - len(_canonical_bytes(exact))
        for offset in range(remaining):
            exact[0]["params"][offset % 16] += "x"
        self.assertEqual(len(_canonical_bytes(exact)), 4_194_304)
        frozen_rows, frozen_bytes, frozen_ids = rpc._freeze_archive_request_rows(exact)
        self.assertEqual(frozen_rows, tuple(exact))
        self.assertEqual(frozen_bytes, _canonical_bytes(exact))
        self.assertEqual(frozen_ids, (1,))
        too_large = copy.deepcopy(exact)
        too_large[0]["params"][0] += "x"
        context = self._run()
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(context, too_large),
            ("authority_mismatch", "request_invalid"),
        )

    def test_response_identity_and_error_classification_are_closed(self):
        request = _simple_request_rows(2)
        cases = (
            ({"jsonrpc": "2.0", "id": 1, "result": "0x1"},
             ("authority_mismatch", "response_identity_invalid")),
            ([_success(1, "0x1")],
             ("authority_mismatch", "response_identity_invalid")),
            ([_success(1, "0x1"), _success(1, "0x1")],
             ("authority_mismatch", "response_identity_invalid")),
            ([_success(True, "0x1"), _success(2, "0x1")],
             ("authority_mismatch", "response_identity_invalid")),
            ([_success(1, "0x1"), _success(3, "0x1")],
             ("authority_mismatch", "response_identity_invalid")),
            ([
                {"jsonrpc": "2.0", "id": 1,
                 "error": {"code": -32000, "message": "closed"}},
                _success(2, "0x1"),
            ], ("archive_state_unavailable", "json_rpc_error")),
            ([
                {"jsonrpc": "2.0", "id": 1,
                 "error": {"code": -32000, "message": "closed", "data": 1.25}},
                _success(2, "0x1"),
            ], ("archive_state_unavailable", "json_rpc_error")),
            ([
                {"jsonrpc": "2.0", "id": 1,
                 "error": {"code": True, "message": "bad"}},
                _success(2, "0x1"),
            ], ("authority_mismatch", "response_identity_invalid")),
            ([
                {"jsonrpc": "2.0", "id": 1, "result": "0x1",
                 "error": {"code": -1, "message": "bad"}},
                _success(2, "0x1"),
            ], ("authority_mismatch", "response_identity_invalid")),
        )
        for body, expected in cases:
            with self.subTest(body=repr(body)[:80]):
                context = self._run(
                    lambda _request, _timeout, value=body: _rpc_response(value)
                )
                self.assertArchiveError(
                    lambda: _archive_rpc_test_batch_for_test(context, request),
                    expected,
                )

    def test_non_200_bodies_are_unread_and_status_mapping_is_exact(self):
        for status, expected in (
            (301, ("archive_state_unavailable", "redirect_forbidden")),
            (413, ("archive_state_unavailable", "http_413")),
            (429, ("archive_state_unavailable", "http_status")),
            (500, ("archive_state_unavailable", "http_status")),
        ):
            with self.subTest(status=status):
                chunks = []
                header_reads = []
                response = _make_archive_rpc_test_response_for_test(
                    status=status,
                    header_items=(("Content-Length", "999"),),
                    body_chunks=(b"SECRET-NON-SUCCESS-BODY",),
                    before_headers=lambda: header_reads.append(True),
                    before_chunk=lambda _index: chunks.append(True),
                )
                context = self._run(lambda _body, _timeout: response)
                error = self.assertArchiveError(
                    lambda: _archive_rpc_test_batch_for_test(
                        context, _simple_request_rows()
                    ),
                    expected,
                )
                self.assertEqual(header_reads, [True])
                self.assertEqual(chunks, [])
                self.assertNotIn("SECRET", repr(error))

    def test_explicit_413_scope_is_left_first_depth_first_and_budgeted(self):
        root = _simple_request_rows(4)
        scripts = [
            (413, None),
            (200, [_success(1, "left-1"), _success(2, "left-2")]),
            (200, [_success(3, "right-3"), _success(4, "right-4")]),
        ]
        seen = []

        def operation(body, _timeout):
            seen.append(json.loads(body))
            status, rows = scripts.pop(0)
            if status == 413:
                return _make_archive_rpc_test_response_for_test(
                    status=413, header_items=(), body_chunks=(b"unread",)
                )
            return _rpc_response(rows)

        context = self._run(operation)
        with _open_archive_rpc_test_logical_batch_for_test(context, root) as scope:
            self.assertEqual(repr(scope), "_ArchiveRpcTestLogicalBatchScope(<sealed>)")
            self.assertArchiveError(
                lambda: _archive_rpc_test_batch_for_test(context, root),
                ("archive_state_unavailable", "http_413"),
            )
            left = _archive_rpc_test_batch_for_test(context, root[:2])
            right = _archive_rpc_test_batch_for_test(context, root[2:])
        self.assertEqual(
            left + right,
            tuple(_success(index, "{}-{}".format(
                "left" if index < 3 else "right", index
            )) for index in range(1, 5)),
        )
        self.assertEqual(seen, [root, root[:2], root[2:]])
        finalization = dict(
            _project_archive_rpc_test_finalization_for_test(context)
        )
        summary = finalization["logical_batches"][0]
        self.assertEqual(summary["attempt_count"], 3)
        self.assertEqual(summary["success_exchange_indices"], (1, 2))
        self.assertEqual(summary["recoverable_failures"], ({
            "attempt_index": 1,
            "reason_code": "archive_state_unavailable",
            "failure_kind": "http_413",
            "request_ids": (1, 2, 3, 4),
        },))

    def test_logical_batch_wire_and_decoded_budgets_are_cumulative_but_reset(self):
        root = _simple_request_rows(48)
        payload = "x" * 180_000
        left_rows = [_success(index, payload) for index in range(1, 25)]
        right_rows = [_success(index, payload) for index in range(25, 49)]
        for encoding in (None, "gzip"):
            with self.subTest(encoding=encoding or "identity"):
                scripted = [
                    _make_archive_rpc_test_response_for_test(
                        status=413, header_items=(), body_chunks=()
                    ),
                    _rpc_response(left_rows, encoding=encoding),
                    _rpc_response(right_rows, encoding=encoding),
                ]
                context = self._run(
                    lambda _body, _timeout: scripted.pop(0)
                )
                with self.assertRaises(rpc._ArchiveRpcError):
                    with _open_archive_rpc_test_logical_batch_for_test(
                        context, root
                    ):
                        self.assertArchiveError(
                            lambda: _archive_rpc_test_batch_for_test(context, root),
                            ("archive_state_unavailable", "http_413"),
                        )
                        _archive_rpc_test_batch_for_test(context, root[:24])
                        self.assertArchiveError(
                            lambda: _archive_rpc_test_batch_for_test(
                                context, root[24:]
                            ),
                            ("archive_state_unavailable", "response_resource_limit"),
                        )

                fresh = [
                    _rpc_response(left_rows, encoding=encoding),
                    _rpc_response(right_rows, encoding=encoding),
                ]
                context = self._run(lambda _body, _timeout: fresh.pop(0))
                self.assertEqual(
                    len(_archive_rpc_test_batch_for_test(context, root[:24])),
                    24,
                )
                self.assertEqual(
                    len(_archive_rpc_test_batch_for_test(context, root[24:])),
                    24,
                )
                finalization = dict(
                    _project_archive_rpc_test_finalization_for_test(context)
                )
                self.assertEqual(len(finalization["logical_batches"]), 2)

    def test_gzip_sidecars_and_decoder_failures_use_one_closed_route(self):
        rows = [_success(1, "0x1")]
        decoded = _canonical_bytes(rows)
        wire = gzip.compress(decoded, mtime=0)
        context = self._run(
            lambda _body, _timeout: _make_archive_rpc_test_response_for_test(
                status=200,
                header_items=(
                    ("Content-Length", str(len(wire))),
                    ("Content-Encoding", "gzip"),
                ),
                body_chunks=(wire[:3], wire[3:]),
            )
        )
        self.assertEqual(
            _archive_rpc_test_batch_for_test(context, _simple_request_rows()),
            tuple(rows),
        )
        finalization = dict(
            _project_archive_rpc_test_finalization_for_test(context)
        )
        record = finalization["successful_exchanges"][0]
        self.assertEqual(record["wire_byte_count"], len(wire))
        self.assertEqual(record["wire_sha256"], hashlib.sha256(wire).hexdigest())
        self.assertEqual(record["decoded_response_bytes"], decoded)
        self.assertEqual(record["decoded_sha256"], hashlib.sha256(decoded).hexdigest())

        cases = (
            (
                (("Content-Encoding", "br"),),
                (b"{}",),
                ("archive_state_unavailable", "response_encoding_unsupported"),
            ),
            (
                (("Content-Encoding", "gzip"),),
                (b"not-gzip",),
                ("archive_state_unavailable", "response_decode_invalid"),
            ),
            (
                tuple(("X-Long-{}".format(index), "v" * 8192) for index in range(9)),
                (decoded,),
                ("archive_state_unavailable", "response_resource_limit"),
            ),
        )
        for headers, chunks, expected in cases:
            with self.subTest(expected=expected):
                context = self._run(
                    lambda _body, _timeout, header_rows=headers, body=chunks:
                    _make_archive_rpc_test_response_for_test(
                        status=200,
                        header_items=header_rows,
                        body_chunks=body,
                    )
                )
                self.assertArchiveError(
                    lambda: _archive_rpc_test_batch_for_test(
                        context, _simple_request_rows()
                    ),
                    expected,
                )

    def test_decoder_receives_only_the_fixed_historical_profile_twice(self):
        observed = []
        decode = rpc.decode_bounded_json_response

        def recording_decode(response, **arguments):
            observed.append(dict(arguments))
            return decode(response, **arguments)

        context = self._run(
            lambda _body, _timeout: _rpc_response([_success(1, "0x1")])
        )
        with mock.patch.object(
            rpc, "decode_bounded_json_response", side_effect=recording_decode
        ):
            self.assertEqual(
                _archive_rpc_test_batch_for_test(
                    context, _simple_request_rows()
                ),
                (_success(1, "0x1"),),
            )
        self.assertEqual(len(observed), 2)
        for index, arguments in enumerate(observed):
            monotonic = arguments.pop("monotonic")
            self.assertTrue(callable(monotonic))
            self.assertEqual(arguments, {
                "header_limit": 65_536,
                "wire_limit": 8_388_608,
                "decoded_limit": 8_388_608,
                "scalar_limit": 8_388_608,
                "node_limit": 1_048_576,
                "ordinary_string_limit": 262_144,
                "require_canonical": False,
                "materialize_exact_floats": False,
                "absolute_deadline": 30.0,
                "return_decoded_bytes": index == 0,
            })

    def test_scope_misuse_terminalizes_the_run(self):
        root = _simple_request_rows(4)
        response_413 = lambda: _make_archive_rpc_test_response_for_test(
            status=413, header_items=(), body_chunks=()
        )
        context = self._run(lambda _body, _timeout: response_413())
        scope = _open_archive_rpc_test_logical_batch_for_test(context, root)
        with self.assertRaises(rpc._ArchiveRpcError):
            with scope:
                self.assertArchiveError(
                    lambda: _archive_rpc_test_batch_for_test(context, root),
                    ("archive_state_unavailable", "http_413"),
                )
                _archive_rpc_test_batch_for_test(context, root[2:])
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(context, root),
            ("authority_mismatch", "context_closed"),
        )

        first = self._run(lambda _body, _timeout: response_413())
        second = self._run(lambda _body, _timeout: response_413())
        transplanted = _open_archive_rpc_test_logical_batch_for_test(first, root)
        object.__setattr__(second, "_active_scope", transplanted)
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(second, root),
            ("authority_mismatch", "logical_batch_scope_invalid"),
        )
        _close_archive_rpc_test_run_for_test(first)

        context = self._run(lambda _body, _timeout: response_413())
        with self.assertRaises(rpc._ArchiveRpcError):
            with _open_archive_rpc_test_logical_batch_for_test(context, root):
                pass
        context = self._run(lambda _body, _timeout: response_413())
        with self.assertRaises(rpc._ArchiveRpcError):
            with _open_archive_rpc_test_logical_batch_for_test(context, root) as outer:
                self.assertArchiveError(
                    lambda: _open_archive_rpc_test_logical_batch_for_test(context, root),
                    ("authority_mismatch", "logical_batch_scope_invalid"),
                )
                self.assertEqual(repr(outer), "_ArchiveRpcTestLogicalBatchScope(<sealed>)")
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(context, root),
            ("authority_mismatch", "context_closed"),
        )

    def test_deadline_guard_interrupts_blocking_transport_and_rejects_prior_alarm(self):
        calls = []

        def interrupted(_body, _timeout):
            calls.append(True)
            os.kill(os.getpid(), signal.SIGALRM)
            self.fail("SIGALRM did not interrupt transport")

        context = self._run(interrupted, clock=time.monotonic)
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(
                context, _simple_request_rows()
            ),
            ("archive_state_unavailable", "attempt_timeout"),
        )
        self.assertEqual(calls, [True])
        self.assertIs(signal.getsignal(signal.SIGALRM), signal.SIG_DFL)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

        body = _canonical_bytes([_success(1, "0x1")])
        for stage in ("status", "headers", "body"):
            with self.subTest(blocking_stage=stage):
                def trigger(*_args):
                    signal.setitimer(signal.ITIMER_REAL, 0.001, 0.0)
                    time.sleep(1.0)
                    self.fail("deadline guard did not interrupt stalled stage")

                response = _make_archive_rpc_test_response_for_test(
                    status=200,
                    header_items=(("Content-Length", str(len(body))),),
                    body_chunks=(body,),
                    before_status=trigger if stage == "status" else None,
                    before_headers=trigger if stage == "headers" else None,
                    before_chunk=trigger if stage == "body" else None,
                )
                context = self._run(
                    lambda _body, _timeout, value=response: value,
                    clock=time.monotonic,
                )
                self.assertArchiveError(
                    lambda: _archive_rpc_test_batch_for_test(
                        context, _simple_request_rows()
                    ),
                    ("archive_state_unavailable", "attempt_timeout"),
                )
                self.assertIs(signal.getsignal(signal.SIGALRM), signal.SIG_DFL)
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

        previous = signal.signal(signal.SIGALRM, lambda *_args: None)
        try:
            calls = []
            context = self._run(
                lambda _body, _timeout: calls.append(True),
                clock=time.monotonic,
            )
            self.assertArchiveError(
                lambda: _archive_rpc_test_batch_for_test(
                    context, _simple_request_rows()
                ),
                ("authority_mismatch", "context_invalid"),
            )
            self.assertEqual(calls, [])
        finally:
            signal.signal(signal.SIGALRM, previous)

        signal.setitimer(signal.ITIMER_REAL, 60.0, 0.0)
        try:
            calls = []
            context = self._run(
                lambda _body, _timeout: calls.append(True),
                clock=time.monotonic,
            )
            self.assertArchiveError(
                lambda: _archive_rpc_test_batch_for_test(
                    context, _simple_request_rows()
                ),
                ("authority_mismatch", "context_invalid"),
            )
            self.assertEqual(calls, [])
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0.0, 0.0)

    def test_clock_boundaries_and_final_drift_are_closed(self):
        collection_clock = _MutableClock(0.0)
        calls = []
        context = self._run(
            lambda _body, _timeout: calls.append(True),
            clock=collection_clock,
        )
        collection_clock.value = 21_600.0
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(
                context, _simple_request_rows()
            ),
            ("archive_state_unavailable", "collection_timeout"),
        )
        self.assertEqual(calls, [])

        clock = _MutableClock(10.0)
        context = self._run(
            lambda _body, _timeout: _rpc_response([_success(1, "0x1")]),
            clock=clock,
        )
        clock.value = 9.0
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(
                context, _simple_request_rows()
            ),
            ("authority_mismatch", "context_invalid"),
        )

        clock = _MutableClock(10.0)
        body = _canonical_bytes([_success(1, "0x1")])
        response = _make_archive_rpc_test_response_for_test(
            status=200,
            header_items=(("Content-Length", str(len(body))),),
            body_chunks=(body,),
            before_headers=lambda: setattr(clock, "value", 9.0),
        )
        context = self._run(
            lambda _body, _timeout: response,
            clock=clock,
        )
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(
                context, _simple_request_rows()
            ),
            ("authority_mismatch", "context_invalid"),
        )

        for invalid in (True, float("nan"), float("inf")):
            self.assertArchiveError(
                lambda value=invalid: self._run(clock=lambda: value),
                ("authority_mismatch", "context_invalid"),
            )

        clock = _MutableClock(0.0)
        response = _make_archive_rpc_test_response_for_test(
            status=200,
            header_items=(("Content-Length", "43"),),
            body_chunks=(b'[{"id":1,"jsonrpc":"2.0","result":"0x1"}]',),
            before_status=lambda: setattr(clock, "value", 30.0),
        )
        context = self._run(lambda _body, _timeout: response, clock=clock)
        self.assertArchiveError(
            lambda: _archive_rpc_test_batch_for_test(
                context, _simple_request_rows()
            ),
            ("archive_state_unavailable", "attempt_timeout"),
        )

        events = []
        context = self._run(checkpoint=lambda name: events.append(name))
        final = _project_archive_rpc_test_finalization_for_test(context)
        self.assertEqual(events, ["open", "finalize"])
        self.assertEqual(dict(final)["status"], "finalized")

        marker = "SECRET-FINAL-DRIFT"
        context = self._run(
            checkpoint=lambda name: (
                (_ for _ in ()).throw(RuntimeError(marker))
                if name == "finalize" else None
            )
        )
        error = self.assertArchiveError(
            lambda: _project_archive_rpc_test_finalization_for_test(context),
            ("authority_mismatch", "final_identity_drift"),
        )
        self.assertNotIn(marker, repr(error))
        self.assertArchiveError(
            lambda: _project_archive_rpc_test_finalization_for_test(context),
            ("authority_mismatch", "context_closed"),
        )

        for cancellation in (KeyboardInterrupt, SystemExit):
            with self.subTest(finalize_cancellation=cancellation.__name__):
                context = self._run(
                    checkpoint=lambda name, kind=cancellation: (
                        (_ for _ in ()).throw(kind())
                        if name == "finalize" else None
                    )
                )
                key = context._key
                with self.assertRaises(cancellation):
                    _project_archive_rpc_test_finalization_for_test(context)
                self.assertEqual(bytes(key), b"\0" * 32)
                self.assertEqual(context._state, "failed")
                self.assertIsNone(context._operation)

    def test_task2a_three_stage_requests_integrate_without_public_api_changes(self):
        policy = json.loads(load_historical_foundry_policy().physical_bytes)
        authority = json.loads(load_historical_foundry_authority().physical_bytes)
        plan = build_historical_anchor_request_plan(
            policy,
            authority,
        )
        expected = _synthetic_responses()
        remaining = list(expected)
        request_bodies = []

        def operation(body, _timeout):
            rows = json.loads(body)
            request_bodies.append(body)
            response_rows = []
            for row in rows:
                for candidate in remaining:
                    if candidate["id"] == row["id"]:
                        response_rows.append(candidate)
                        break
            return _rpc_response(tuple(reversed(response_rows)))

        context = self._run(operation)
        observed = []
        anchor_rows = _materialize_historical_anchor_stage(plan, "anchor", [])
        observed.extend(_archive_rpc_test_batch_for_test(context, anchor_rows))
        fixed_rows = _materialize_historical_anchor_stage(plan, 1, observed)
        observed.extend(_archive_rpc_test_batch_for_test(context, fixed_rows))
        derived_rows = _materialize_historical_anchor_stage(plan, 2, observed)
        observed.extend(_archive_rpc_test_batch_for_test(context, derived_rows))
        capture = project_historical_anchor_capture(plan, observed)
        self.assertEqual(capture["anchor"]["hash"], ANCHOR_HASH)
        self.assertEqual(request_bodies, [
            _canonical_bytes(list(anchor_rows)),
            _canonical_bytes(list(fixed_rows)),
            _canonical_bytes(list(derived_rows)),
        ])
        finalization = dict(
            _project_archive_rpc_test_finalization_for_test(context)
        )
        self.assertEqual(finalization["identity"]["collection"], {
            "logical_batch_count": 3,
            "successful_exchange_count": 3,
            "request_count": 48,
            "response_count": 48,
            "wire_byte_count": sum(
                row["wire_byte_count"]
                for row in finalization["successful_exchanges"]
            ),
            "decoded_byte_count": sum(
                row["decoded_byte_count"]
                for row in finalization["successful_exchanges"]
            ),
        })

    def test_production_apis_reject_test_context_before_transport(self):
        calls = []
        context = self._run(lambda _body, _timeout: calls.append(True))
        rows = _simple_request_rows()
        for operation in (
            lambda: _production_archive_rpc_batch(context, rows),
            lambda: _open_production_archive_rpc_logical_batch(context, rows),
            lambda: _finalize_production_archive_rpc_run(context),
        ):
            self.assertArchiveError(
                operation, ("authority_mismatch", "context_invalid")
            )
        self.assertEqual(calls, [])

    def test_system_runtime_production_preflight_does_not_read_endpoint(self):
        if (
            getattr(rpc.sys.implementation, "name", None) == "cpython"
            and rpc.sys.version_info[:3] == (3, 8, 10)
        ):
            self.skipTest("system runtime is the required production runtime")

        class ExplodingEnvironment(dict):
            def get(self, _key, _default=None):
                raise AssertionError("endpoint was read before Python preflight")

            def __getitem__(self, _key):
                raise AssertionError("endpoint was read before Python preflight")

        with mock.patch.object(rpc.os, "environ", ExplodingEnvironment()):
            self.assertArchiveError(
                _open_production_archive_rpc_run,
                ("authority_mismatch", "preflight_invalid"),
            )

    def test_production_preflight_order_rechecks_every_retained_projection(self):
        events = []

        class Part:
            physical_bytes = b"{}"
            physical_sha256 = "a" * 64
            policy_id = "policy:" + "b" * 64
            value = {}

        class Config:
            policy = Part()
            authority = Part()
            toolchain = Part()

        class Sources:
            projections = ()

            def __init__(self, _root):
                events.append("hold_root")
                self.closed = False

            def open_members(self):
                events.append("open_members")

            def verify(self):
                events.append("verify_sources")

            def close(self):
                self.closed = True
                events.append("close_sources")

        class Toolchain:
            closed = False

            @property
            def verified_identity(self):
                events.append("candidate")
                return {}

            def verified_project_input_identity(self):
                events.append("project_inputs")
                return {"project": "fixed"}

            def _verify_versions_and_hardfork(self):
                events.append("versions")

            def _close(self):
                self.closed = True
                events.append("close_toolchain")

        sources = []
        toolchain = Toolchain()

        def issue_sources(root):
            value = Sources(root)
            sources.append(value)
            return value

        def exact_python():
            events.append("python")
            return {"python": "fixed"}

        def git_identity(_root):
            events.append("git")
            return "1" * 40

        def load_config():
            events.append("config")
            return Config()

        def require_config(_config, _sources):
            events.append("config_bytes")

        def toolchain_projection(_candidate, _config):
            events.append("toolchain_projection")
            return {"toolchain": "fixed"}

        def project_inputs(_project, _config, _sources):
            events.append("project_inputs_vs_sources")

        def build(_config):
            events.append("build")
            return object()

        def artifact_projection(_artifact, _config):
            events.append("artifact_projection")
            return ({"artifact": "fixed"}, "2" * 64)

        with mock.patch.object(rpc, "_exact_python_projection", exact_python), \
                mock.patch.object(rpc, "_git_identity", git_identity), \
                mock.patch.object(rpc, "_HeldArchiveSourceAuthority", issue_sources), \
                mock.patch.object(rpc, "load_historical_foundry_config_set", load_config), \
                mock.patch.object(rpc, "_require_config_source_bytes", require_config), \
                mock.patch(
                    "scripts.bootstrap_historical_foundry_toolchain."
                    "open_reviewed_historical_toolchain",
                    return_value=toolchain,
                ), \
                mock.patch.object(rpc, "_toolchain_projection", toolchain_projection), \
                mock.patch.object(rpc, "_project_inputs_equal_sources", project_inputs), \
                mock.patch.object(rpc, "build_validated_executor_artifact", build), \
                mock.patch.object(rpc, "_artifact_projection", artifact_projection):
            preflight = rpc._perform_production_preflight()

        self.assertIsNotNone(preflight)
        self.assertEqual(events[:4], ["python", "hold_root", "git", "open_members"])
        self.assertEqual(events.count("candidate"), 2)
        self.assertEqual(events.count("toolchain_projection"), 2)
        self.assertEqual(events.count("project_inputs"), 2)
        self.assertEqual(events.count("project_inputs_vs_sources"), 2)
        self.assertEqual(events.count("artifact_projection"), 2)
        self.assertEqual(events.count("git"), 2)
        self.assertEqual(events.count("python"), 2)
        self.assertEqual(events.count("config"), 2)
        self.assertLess(events.index("build"), events.index("verify_sources"))
        preflight.close()
        self.assertTrue(toolchain.closed)
        self.assertTrue(sources[0].closed)

    def test_production_preflight_preserves_all_controls_and_cleanup_priority(self):
        class Part:
            value = {}

        class Config:
            toolchain = Part()

        for body_error in (
            GeneratorExit("preflight-body-generator"),
            asyncio.CancelledError("preflight-body-cancelled"),
            ValueError("preflight-body-ordinary"),
        ):
            with self.subTest(body_error=type(body_error).__name__):
                toolchain_control = SystemExit("preflight-toolchain-cleanup")
                source_control = KeyboardInterrupt("preflight-source-cleanup")
                events = []

                class Sources:
                    projections = ()

                    def __init__(self, _root):
                        self.closed = False

                    def open_members(self):
                        events.append("open_sources")

                    def verify(self):
                        events.append("verify_sources")

                    def close(self):
                        events.append("close_sources")
                        self.closed = True
                        raise source_control

                class Toolchain:
                    verified_identity = {}

                    def __init__(self):
                        self.closed = False

                    def verified_project_input_identity(self):
                        return {}

                    def _verify_versions_and_hardfork(self):
                        events.append("verify_toolchain")

                    def _close(self):
                        events.append("close_toolchain")
                        self.closed = True
                        raise toolchain_control

                sources = []
                toolchain = Toolchain()

                def issue_sources(root):
                    value = Sources(root)
                    sources.append(value)
                    return value

                with mock.patch.object(
                    rpc, "_exact_python_projection", return_value={"python": "fixed"}
                ), mock.patch.object(
                    rpc, "_git_identity", return_value="1" * 40
                ), mock.patch.object(
                    rpc, "_HeldArchiveSourceAuthority", side_effect=issue_sources
                ), mock.patch.object(
                    rpc, "load_historical_foundry_config_set", return_value=Config()
                ), mock.patch.object(
                    rpc, "_require_config_source_bytes", return_value=None
                ), mock.patch(
                    "scripts.bootstrap_historical_foundry_toolchain."
                    "open_reviewed_historical_toolchain",
                    return_value=toolchain,
                ), mock.patch.object(
                    rpc, "_toolchain_projection", return_value={}
                ), mock.patch.object(
                    rpc, "_project_inputs_equal_sources", return_value=None
                ), mock.patch.object(
                    rpc, "build_validated_executor_artifact", return_value=object()
                ), mock.patch.object(
                    rpc, "_artifact_projection", side_effect=body_error
                ):
                    expected = (
                        body_error
                        if not isinstance(body_error, Exception)
                        else toolchain_control
                    )
                    with self.assertRaises(type(expected)) as caught:
                        rpc._perform_production_preflight()
                self.assertIs(caught.exception, expected)
                self.assertEqual(
                    events[-2:], ["close_toolchain", "close_sources"]
                )
                self.assertTrue(toolchain.closed)
                self.assertTrue(sources[0].closed)

    def test_production_opener_preserves_cleanup_control_over_ordinary_activation(self):
        for activation_error in (
            ValueError("activation-ordinary"),
            GeneratorExit("activation-control"),
        ):
            with self.subTest(activation_error=type(activation_error).__name__):
                cleanup_control = asyncio.CancelledError("activation-cleanup")

                class Preflight:
                    def __init__(self):
                        self.closed = False

                    def close(self):
                        self.closed = True
                        raise cleanup_control

                preflight = Preflight()
                with mock.patch.object(
                    rpc, "_perform_production_preflight", return_value=preflight
                ), mock.patch.object(
                    rpc, "_activate_production_archive_rpc_run",
                    side_effect=activation_error,
                ):
                    expected = (
                        activation_error
                        if not isinstance(activation_error, Exception)
                        else cleanup_control
                    )
                    with self.assertRaises(type(expected)) as caught:
                        rpc._open_production_archive_rpc_run_core()
                self.assertIs(caught.exception, expected)
                self.assertTrue(preflight.closed)

    def test_production_opener_is_fixed_offline_and_cancellation_cleans_preflight(self):
        events = []

        class Preflight:
            def __init__(self):
                self.identity = rpc._test_preflight_identity()
                self.closed = False

            def close(self):
                self.closed = True
                events.append("close")

        class Environment:
            def __init__(self, cancellation=None):
                self.calls = []
                self.cancellation = cancellation

            def get(self, key, default=None):
                self.calls.append((key, default))
                events.append("endpoint")
                if self.cancellation is not None:
                    raise self.cancellation()
                return "HTTPS://RPC.Example.invalid/archive?Key=%2F"

        class Opener:
            def __init__(self):
                self.addheaders = ["ambient"]
                self.calls = []

            def open(self, request, timeout):
                self.calls.append((request, timeout))
                return "offline-response"

        preflight = Preflight()
        environment = Environment()
        opener = Opener()

        def build_opener(*handlers):
            events.append("opener")
            self.assertEqual(handlers[0].proxies, {})
            self.assertIsInstance(handlers[1], rpc._ArchiveNoRedirectHandler)
            return opener

        with mock.patch.object(
            rpc, "_perform_production_preflight",
            side_effect=lambda: events.append("preflight") or preflight,
        ), mock.patch.object(
            rpc.time, "monotonic", side_effect=lambda: events.append("clock") or 10.0,
        ), mock.patch.object(
            rpc.os, "urandom", side_effect=lambda count: events.append("entropy") or b"z" * count,
        ), mock.patch.object(
            rpc.os, "environ", environment,
        ), mock.patch.object(
            rpc.urllib.request, "build_opener", side_effect=build_opener,
        ):
            context = _open_production_archive_rpc_run()

        self.assertEqual(events[:5], ["preflight", "clock", "entropy", "endpoint", "opener"])
        self.assertEqual(environment.calls, [("DEX_DEPTH_RPC_ETH", None)])
        self.assertEqual(opener.addheaders, [])
        self.assertEqual(context._operation(b"[]", 7.0), "offline-response")
        request, timeout = opener.calls[0]
        self.assertEqual(timeout, 7.0)
        self.assertEqual(
            request.full_url,
            "https://rpc.example.invalid:443/archive?Key=%2F",
        )
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(dict(request.header_items()), {
            "Accept": "application/json",
            "Content-type": "application/json",
            "User-agent": "historical-foundry-archive-rpc/1",
        })
        key = context._key
        rpc._abandon_archive_context(context)
        self.assertEqual(bytes(key), b"\0" * 32)
        self.assertTrue(preflight.closed)

        for cancellation in (KeyboardInterrupt, SystemExit):
            with self.subTest(cancellation=cancellation.__name__):
                preflight = Preflight()
                environment = Environment(cancellation)
                with mock.patch.object(
                    rpc, "_perform_production_preflight", return_value=preflight,
                ), mock.patch.object(
                    rpc.time, "monotonic", return_value=10.0,
                ), mock.patch.object(
                    rpc.os, "urandom", return_value=b"z" * 32,
                ), mock.patch.object(
                    rpc.os, "environ", environment,
                ):
                    with self.assertRaises(cancellation):
                        _open_production_archive_rpc_run()
                self.assertTrue(preflight.closed)

                preflight = Preflight()
                key = bytearray(b"z" * 32)
                context = rpc._issue_production_context(
                    _state="active",
                    _clock=_MutableClock(),
                    _last_clock=0.0,
                    _collection_deadline=21_600.0,
                    _key=key,
                    _endpoint_projection=rpc._frozen_archive_value({}),
                    _endpoint_bytes=b"{}",
                    _connection_url="https://example.invalid:443/",
                    _endpoint_identity=rpc._frozen_archive_value({
                        "schema": "historical_foundry_rpc_endpoint_identity/v1",
                        "scope": "single_run_nonreversible",
                        "endpoint_hmac_sha256": "0" * 64,
                    }),
                    _operation=lambda _body, _timeout: None,
                    _preflight=preflight,
                    _opening_identity=rpc._frozen_archive_value(preflight.identity),
                    _active_scope=None,
                    _reserved_scope=None,
                    _logical_summaries=[],
                    _records=[],
                    _next_logical_batch_index=1,
                    _next_exchange_index=1,
                )
                with mock.patch.object(
                    rpc, "_recheck_production_preflight",
                    side_effect=cancellation,
                ):
                    with self.assertRaises(cancellation):
                        _finalize_production_archive_rpc_run(context)
                self.assertEqual(bytes(key), b"\0" * 32)
                self.assertTrue(preflight.closed)
                self.assertEqual(context._state, "failed")

    def test_fixed_production_source_inventory_is_descriptor_held_and_ordered(self):
        expected = (
            ("source:atomic_publication", "scripts/atomic_publication.py"),
            ("source:bootstrap_historical_foundry_toolchain", "scripts/bootstrap_historical_foundry_toolchain.py"),
            ("source:bounded_json", "scripts/bounded_json.py"),
            ("source:bounded_snapshot_merge", "scripts/bounded_snapshot_merge.py"),
            ("source:cex_instrument_lifecycle", "scripts/cex_instrument_lifecycle.py"),
            ("source:collection_deadline", "scripts/collection_deadline.py"),
            ("source:execution_cost", "scripts/execution_cost.py"),
            ("source:fact_quality", "scripts/fact_quality.py"),
            ("source:fetch_cex", "scripts/fetch_cex.py"),
            ("source:fetch_cex_depth", "scripts/fetch_cex_depth.py"),
            ("source:historical_foundry_contracts", "scripts/historical_foundry_contracts.py"),
            ("source:historical_foundry_anvil", "scripts/historical_foundry_anvil.py"),
            ("source:historical_foundry_rpc", "scripts/historical_foundry_rpc.py"),
            ("source:market_lifecycle_reviews", "scripts/market_lifecycle_reviews.py"),
            ("source:publication_gate", "scripts/publication_gate.py"),
            ("source:quality_outcomes", "scripts/quality_outcomes.py"),
            ("source:route_cost_evidence", "scripts/route_cost_evidence.py"),
            ("source:route_quantity", "scripts/route_quantity.py"),
            ("source:timestamp_contract", "scripts/timestamp_contract.py"),
            ("config:replay_policy", "config/historical_foundry_replay_policy.json"),
            ("config:replay_authority", "config/historical_foundry_replay_authority.json"),
            ("config:replay_toolchain", "config/historical_foundry_replay_toolchain.json"),
            ("build:foundry_toml", "foundry.toml"),
            ("build:foundry_lock", "foundry.lock"),
            ("build:gitmodules", ".gitmodules"),
            ("build:executor_source", "foundry/src/TwoVenueV2Executor.sol"),
            ("source:historical_foundry_scan", "scripts/historical_foundry_scan.py"),
            ("source:historical_foundry_storage", "scripts/historical_foundry_storage.py"),
        )
        self.assertEqual(
            tuple((role, relative) for role, _module, relative in rpc._PRODUCTION_SOURCE_MEMBERS),
            expected,
        )
        held = rpc._HeldArchiveSourceAuthority(
            rpc.Path(rpc.__file__).resolve().parents[1]
        )
        try:
            held.open_members()
            self.assertEqual(
                tuple(row["role"] for row in held.projections),
                tuple(role for role, _relative in expected),
            )
            self.assertTrue(all(
                set(row) == {"role", "size_bytes", "sha256"}
                and row["size_bytes"] > 0
                and len(row["sha256"]) == 64
                for row in held.projections
            ))
            held.verify()
        finally:
            held.close()
            held.close()

    def test_source_and_git_preflight_fail_closed_on_reviewed_attack_matrix(self):
        member_table = (("source:member", None, "scripts/member.py"),)

        def make_root():
            temporary = tempfile.TemporaryDirectory()
            root = rpc.Path(temporary.name)
            (root / "scripts").mkdir(mode=0o700)
            member = root / "scripts/member.py"
            member.write_bytes(b"reviewed\n")
            member.chmod(0o600)
            return temporary, root, member

        temporary, root, member = make_root()
        try:
            with mock.patch.object(rpc, "_PRODUCTION_SOURCE_MEMBERS", member_table):
                held = rpc._HeldArchiveSourceAuthority(root)
                held.open_members()
                (root / "out").mkdir()
                held.verify()
                member.write_bytes(b"changed\n")
                with self.assertRaises(ValueError):
                    held.verify()
                held.close()
        finally:
            temporary.cleanup()

        for attack in ("symlink", "hardlink", "writable", "oversized"):
            with self.subTest(source_attack=attack):
                temporary, root, member = make_root()
                try:
                    if attack == "symlink":
                        target = root / "target.py"
                        target.write_bytes(b"reviewed\n")
                        member.unlink()
                        member.symlink_to(target)
                    elif attack == "hardlink":
                        os.link(str(member), str(root / "second-link.py"))
                    elif attack == "writable":
                        member.chmod(0o666)
                    else:
                        member.write_bytes(b"x" * (1_048_576 + 1))
                    with mock.patch.object(
                        rpc, "_PRODUCTION_SOURCE_MEMBERS", member_table
                    ):
                        held = rpc._HeldArchiveSourceAuthority(root)
                        with self.assertRaises((OSError, ValueError)):
                            held.open_members()
                        held.close()
                finally:
                    temporary.cleanup()

        temporary, root, _member = make_root()
        try:
            origin_attack = (
                ("source:member", "scripts.historical_foundry_rpc", "scripts/member.py"),
            )
            with mock.patch.object(rpc, "_PRODUCTION_SOURCE_MEMBERS", origin_attack):
                held = rpc._HeldArchiveSourceAuthority(root)
                with self.assertRaises(ValueError):
                    held.open_members()
                held.close()
        finally:
            temporary.cleanup()

        root = rpc.Path(rpc.__file__).resolve().parents[1]
        valid_stage = b"100644 " + b"2" * 40 + b" 0\tscripts/member.py\0"

        def git_output(_root, arguments, maximum=4 * 1024 * 1024):
            del maximum
            if arguments == ("rev-parse", "--show-toplevel"):
                return (str(root) + "\n").encode("utf-8")
            if arguments == ("status", "--porcelain=v1", "--untracked-files=all"):
                return b""
            if arguments == ("rev-parse", "HEAD"):
                return b"1" * 40 + b"\n"
            return valid_stage

        with mock.patch.object(rpc, "_PRODUCTION_SOURCE_MEMBERS", member_table), \
                mock.patch.object(rpc, "_git_output", side_effect=git_output):
            self.assertEqual(rpc._git_identity(root), "1" * 40)

        attacks = {
            "bad_root": (b"/private/tmp/other\n", b"", b"1" * 40 + b"\n", valid_stage),
            "dirty": ((str(root) + "\n").encode(), b"?? local.py\n", b"1" * 40 + b"\n", valid_stage),
            "bad_head": ((str(root) + "\n").encode(), b"", b"A" * 40 + b"\n", valid_stage),
            "stage_one": ((str(root) + "\n").encode(), b"", b"1" * 40 + b"\n", b"100644 " + b"2" * 40 + b" 1\tscripts/member.py\0"),
        }
        for name, scripted in attacks.items():
            with self.subTest(git_attack=name):
                responses = iter(scripted)
                with mock.patch.object(
                    rpc, "_PRODUCTION_SOURCE_MEMBERS", member_table
                ), mock.patch.object(
                    rpc, "_git_output", side_effect=lambda *_args, **_kwargs: next(responses)
                ):
                    with self.assertRaises((OSError, ValueError)):
                        rpc._git_identity(root)

    def test_review_i1_real_toolchain_candidate_replays_frozen_config_binaries(self):
        digests = dict(foundry_toolchain._EXPECTED_BINARY_SHA256)
        digests["solc"] = foundry_toolchain._SOLC_SHA256
        candidate = foundry_toolchain._candidate_identity(digests, None)
        config = rpc._detached_archive_value(
            load_historical_foundry_config_set().toolchain.value
        )

        self.assertIs(type(candidate["binaries"]), list)
        self.assertIs(type(config["binaries"]), tuple)
        projection = rpc._toolchain_projection(candidate, config)
        self.assertEqual(
            projection["binaries"],
            tuple(dict(row) for row in candidate["binaries"]),
        )

        for attack in ("reordered", "extra_field", "candidate_tuple"):
            with self.subTest(attack=attack):
                attacked_candidate = copy.deepcopy(candidate)
                attacked_config = copy.deepcopy(config)
                if attack == "reordered":
                    attacked_candidate["binaries"].reverse()
                    attacked_config["binaries"] = tuple(
                        reversed(attacked_config["binaries"])
                    )
                elif attack == "extra_field":
                    attacked_candidate["binaries"][0]["extra"] = "forbidden"
                    attacked_config["binaries"][0]["extra"] = "forbidden"
                else:
                    attacked_candidate["binaries"] = tuple(
                        attacked_candidate["binaries"]
                    )
                with self.assertRaises(ValueError):
                    rpc._toolchain_projection(attacked_candidate, attacked_config)

    def test_review_i2_exact_fee_history_decimal_is_returned_but_not_projected(self):
        request = [{
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_feeHistory",
            "params": ["0x1", "latest", []],
        }]
        response_rows = [_success(1, {
            "oldestBlock": "0x1",
            "baseFeePerGas": ["0x1", "0x2"],
            "gasUsedRatio": [0.5],
        })]
        context = self._run(
            lambda _body, _timeout: _rpc_response(response_rows)
        )

        returned = _archive_rpc_test_batch_for_test(context, request)
        self.assertEqual(
            returned[0]["result"]["gasUsedRatio"],
            [Decimal("0.5")],
        )
        self.assertIs(type(returned[0]["result"]["gasUsedRatio"][0]), Decimal)

        finalized = dict(_project_archive_rpc_test_finalization_for_test(context))

        def contains_decimal(value):
            if type(value) is dict:
                return any(contains_decimal(item) for item in value.values())
            if type(value) in (list, tuple):
                return any(contains_decimal(item) for item in value)
            return type(value) is Decimal

        self.assertFalse(contains_decimal(finalized))
        self.assertIn(
            b'"gasUsedRatio":[0.5]',
            finalized["successful_exchanges"][0]["decoded_response_bytes"],
        )

    def test_review_i3_archive_error_classification_is_sealed_and_noncopyable(self):
        expected = ("archive_state_unavailable", "attempt_timeout")
        message = "historical archive RPC failure: {}/{}".format(*expected)

        with self.subTest(property="vars_snapshot"):
            error = rpc._ArchiveRpcError(*expected)
            exposed = vars(error)
            exposed["reason_code"] = "authority_mismatch"
            exposed["detail"] = "SECRET"
            self.assertEqual(
                (error.reason_code, error.failure_kind), expected
            )
            self.assertEqual(
                vars(error),
                {"reason_code": expected[0], "failure_kind": expected[1]},
            )
            self.assertEqual(error.args, (message,))
            self.assertEqual(str(error), message)
            self.assertIsNone(error.__cause__)
            self.assertIsNone(error.__context__)
            self.assertTrue(error.__suppress_context__)

        with self.subTest(property="sealed_subclass"):
            with self.assertRaises(TypeError):
                type("ForgedArchiveRpcError", (rpc._ArchiveRpcError,), {})

        for operation in (copy.copy, copy.deepcopy, pickle.dumps):
            with self.subTest(property="noncopyable", operation=operation.__name__):
                with self.assertRaises(TypeError):
                    operation(rpc._ArchiveRpcError(*expected))

    def test_review_i4_deadline_expiry_is_closed_across_whole_attempt(self):
        body = [_success(1, "0x1")]

        class ExpiringClock:
            def __init__(self):
                self.calls = 0

            def __call__(self):
                self.calls += 1
                if self.calls == 3:
                    signal.setitimer(signal.ITIMER_REAL, 0.001, 0.0)
                    time.sleep(1.0)
                    raise AssertionError("deadline did not interrupt clock")
                return time.monotonic()

        def expire(*_args):
            signal.setitimer(signal.ITIMER_REAL, 0.001, 0.0)
            time.sleep(1.0)
            raise AssertionError("deadline did not interrupt guarded point")

        for point in ("hmac", "clock", "response_ids"):
            with self.subTest(point=point):
                clock = ExpiringClock() if point == "clock" else time.monotonic
                context = self._run(
                    lambda _request, _timeout: _rpc_response(body),
                    clock=clock,
                )
                try:
                    patcher = (
                        mock.patch.object(rpc.hmac, "compare_digest", side_effect=expire)
                        if point == "hmac"
                        else mock.patch.object(
                            rpc,
                            "_validate_archive_response_rows",
                            side_effect=expire,
                        )
                        if point == "response_ids"
                        else nullcontext()
                    )
                    with patcher:
                        self.assertArchiveError(
                            lambda: _archive_rpc_test_batch_for_test(
                                context, _simple_request_rows()
                            ),
                            ("archive_state_unavailable", "attempt_timeout"),
                        )
                    self.assertEqual(context._state, "failed")
                    self.assertIsNone(context._operation)
                    self.assertIs(signal.getsignal(signal.SIGALRM), signal.SIG_DFL)
                    self.assertEqual(
                        signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0)
                    )
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0.0, 0.0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
                    _close_archive_rpc_test_run_for_test(context)

    def test_review_i5_close_cancellation_restores_alarm_before_propagating(self):
        cancellation = KeyboardInterrupt("close-cancellation")
        response = _rpc_response([_success(1, "0x1")])
        context = self._run(
            lambda _request, _timeout: response,
            clock=time.monotonic,
        )

        def interrupt_close(_response):
            raise cancellation

        try:
            with mock.patch.object(
                rpc._ArchiveRpcTestResponse, "close", interrupt_close
            ):
                with self.assertRaises(KeyboardInterrupt) as raised:
                    _archive_rpc_test_batch_for_test(
                        context, _simple_request_rows()
                    )
            self.assertIs(raised.exception, cancellation)
            self.assertIs(signal.getsignal(signal.SIGALRM), signal.SIG_DFL)
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))
            self.assertEqual(context._state, "failed")
            self.assertIsNone(context._operation)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0.0, 0.0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            _close_archive_rpc_test_run_for_test(context)

    def test_review_i6_collection_timeout_overrides_attempt_failures_and_close_success(self):
        expected = ("archive_state_unavailable", "collection_timeout")
        for point in ("transport", "status", "decode"):
            with self.subTest(point=point):
                clock = _MutableClock(0.0)

                def expire_and_raise():
                    clock.value = 21_600.0
                    raise OSError("SECRET expired failure")

                if point == "transport":
                    def operation(_request, _timeout):
                        expire_and_raise()
                elif point == "status":
                    response = _make_archive_rpc_test_response_for_test(
                        status=200,
                        header_items=(),
                        body_chunks=(),
                        before_status=expire_and_raise,
                    )
                    operation = lambda _request, _timeout: response
                else:
                    response = _make_archive_rpc_test_response_for_test(
                        status=200,
                        header_items=(("Bad Header", "value"),),
                        body_chunks=(),
                        before_headers=lambda: setattr(
                            clock, "value", 21_600.0
                        ),
                    )
                    operation = lambda _request, _timeout: response
                context = self._run(operation, clock=clock)
                self.assertArchiveError(
                    lambda: _archive_rpc_test_batch_for_test(
                        context, _simple_request_rows()
                    ),
                    expected,
                )
                self.assertEqual(context._state, "failed")

        clock = _MutableClock(0.0)
        response = _rpc_response([_success(1, "0x1")])
        context = self._run(
            lambda _request, _timeout: response,
            clock=clock,
        )
        original_close = rpc._ArchiveRpcTestResponse.close

        def expire_on_close(response_value):
            clock.value = context._collection_deadline
            original_close(response_value)

        try:
            with mock.patch.object(
                rpc._ArchiveRpcTestResponse, "close", expire_on_close
            ):
                self.assertArchiveError(
                    lambda: _archive_rpc_test_batch_for_test(
                        context, _simple_request_rows()
                    ),
                    expected,
                )
            self.assertEqual(context._state, "failed")
            self.assertEqual(context._records, [])
        finally:
            _close_archive_rpc_test_run_for_test(context)

    def test_review_i7_non_200_headers_and_close_override_recoverable_413(self):
        root = _simple_request_rows(2)
        header_name = "X" * 128
        header_rows = tuple(
            (header_name, "a" * 8064) for _index in range(9)
        )
        body_reads = []
        oversized = _make_archive_rpc_test_response_for_test(
            status=413,
            header_items=header_rows,
            body_chunks=(b"SECRET-UNREAD",),
            before_chunk=lambda _index: body_reads.append(True),
        )
        with self.subTest(non_200_boundary="header_limit"):
            context = self._run(lambda _request, _timeout: oversized)
            scope = _open_archive_rpc_test_logical_batch_for_test(context, root)
            scope.__enter__()
            try:
                self.assertArchiveError(
                    lambda: _archive_rpc_test_batch_for_test(context, root),
                    ("archive_state_unavailable", "response_resource_limit"),
                )
                self.assertEqual(body_reads, [])
                self.assertEqual(context._state, "failed")
                self.assertIsNone(context._active_scope)
            finally:
                _close_archive_rpc_test_run_for_test(context)

        def expire(*_args):
            signal.setitimer(signal.ITIMER_REAL, 0.001, 0.0)
            time.sleep(1.0)
            raise AssertionError("deadline did not interrupt 413 boundary")

        for point in ("headers", "close"):
            with self.subTest(deadline_point=point):
                response = _make_archive_rpc_test_response_for_test(
                    status=413,
                    header_items=(),
                    body_chunks=(b"SECRET-UNREAD",),
                    before_headers=expire if point == "headers" else None,
                )
                context = self._run(
                    lambda _request, _timeout, value=response: value,
                    clock=time.monotonic,
                )
                scope = _open_archive_rpc_test_logical_batch_for_test(context, root)
                scope.__enter__()
                try:
                    patcher = (
                        mock.patch.object(
                            rpc._ArchiveRpcTestResponse, "close", expire
                        )
                        if point == "close" else nullcontext()
                    )
                    with patcher:
                        self.assertArchiveError(
                            lambda: _archive_rpc_test_batch_for_test(context, root),
                            ("archive_state_unavailable", "attempt_timeout"),
                        )
                    self.assertEqual(context._state, "failed")
                    self.assertIsNone(context._active_scope)
                    self.assertIs(signal.getsignal(signal.SIGALRM), signal.SIG_DFL)
                    self.assertEqual(
                        signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0)
                    )
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0.0, 0.0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
                    _close_archive_rpc_test_run_for_test(context)

        with self.subTest(non_200_boundary="close_failure"):
            response = _make_archive_rpc_test_response_for_test(
                status=413, header_items=(), body_chunks=()
            )
            context = self._run(lambda _request, _timeout: response)
            scope = _open_archive_rpc_test_logical_batch_for_test(context, root)
            scope.__enter__()

            def fail_close(_response):
                raise OSError("SECRET close failure")

            try:
                with mock.patch.object(
                    rpc._ArchiveRpcTestResponse, "close", fail_close
                ):
                    self.assertArchiveError(
                        lambda: _archive_rpc_test_batch_for_test(context, root),
                        ("archive_state_unavailable", "transport_unavailable"),
                    )
                self.assertEqual(context._state, "failed")
                self.assertIsNone(context._active_scope)
            finally:
                _close_archive_rpc_test_run_for_test(context)
    def test_review_round2_collection_expiry_gates_commit_and_finalization(self):
        expected = ("archive_state_unavailable", "collection_timeout")

        with self.subTest(boundary="batch_commit"):
            clock = _MutableClock(0.0)
            context = self._run(
                lambda _request, _timeout: _rpc_response([_success(1, "0x1")]),
                clock=clock,
            )
            perform_attempt = rpc._perform_archive_attempt

            def expire_after_attempt(*arguments):
                outcome = perform_attempt(*arguments)
                clock.value = context._collection_deadline
                return outcome

            try:
                with mock.patch.object(
                    rpc,
                    "_perform_archive_attempt",
                    side_effect=expire_after_attempt,
                ):
                    self.assertArchiveError(
                        lambda: _archive_rpc_test_batch_for_test(
                            context, _simple_request_rows()
                        ),
                        expected,
                    )
                self.assertEqual(context._state, "failed")
                self.assertEqual(context._records, [])
            finally:
                _close_archive_rpc_test_run_for_test(context)

        with self.subTest(boundary="test_finalization_entry"):
            clock = _MutableClock(0.0)
            checkpoints = []
            context = self._run(
                clock=clock,
                checkpoint=lambda name: checkpoints.append(name),
            )
            clock.value = context._collection_deadline
            self.assertArchiveError(
                lambda: _project_archive_rpc_test_finalization_for_test(context),
                expected,
            )
            self.assertEqual(checkpoints, ["open"])
            self.assertEqual(context._state, "failed")

        with self.subTest(boundary="test_finalization_recheck"):
            clock = _MutableClock(0.0)

            def checkpoint(name):
                if name == "finalize":
                    clock.value = 21_600.0

            context = self._run(clock=clock, checkpoint=checkpoint)
            self.assertArchiveError(
                lambda: _project_archive_rpc_test_finalization_for_test(context),
                expected,
            )
            self.assertEqual(context._state, "failed")

        class ProductionPreflight:
            def __init__(self):
                self.identity = rpc._test_preflight_identity()
                self.closed = False

            def close(self):
                self.closed = True

        def production_context(clock):
            preflight = ProductionPreflight()
            key = bytearray(b"r" * 32)
            context = rpc._issue_production_context(
                _state="active",
                _clock=clock,
                _last_clock=0.0,
                _collection_deadline=21_600.0,
                _key=key,
                _endpoint_projection=rpc._frozen_archive_value({}),
                _endpoint_bytes=b"{}",
                _connection_url="https://example.invalid:443/",
                _endpoint_identity=rpc._frozen_archive_value({
                    "schema": "historical_foundry_rpc_endpoint_identity/v1",
                    "scope": "single_run_nonreversible",
                    "endpoint_hmac_sha256": "0" * 64,
                }),
                _operation=lambda _body, _timeout: None,
                _preflight=preflight,
                _opening_identity=rpc._frozen_archive_value(preflight.identity),
                _active_scope=None,
                _reserved_scope=None,
                _logical_summaries=[],
                _records=[],
                _next_logical_batch_index=1,
                _next_exchange_index=1,
            )
            return context, preflight, key

        for boundary in (
            "production_finalization_entry",
            "production_finalization_recheck",
        ):
            with self.subTest(boundary=boundary):
                clock = _MutableClock(0.0)
                context, preflight, key = production_context(clock)
                rechecks = []
                if boundary.endswith("entry"):
                    clock.value = context._collection_deadline

                def recheck(_preflight):
                    rechecks.append(True)
                    if boundary.endswith("recheck"):
                        clock.value = context._collection_deadline
                    return True

                try:
                    with mock.patch.object(
                        rpc,
                        "_recheck_production_preflight",
                        side_effect=recheck,
                    ):
                        self.assertArchiveError(
                            lambda: _finalize_production_archive_rpc_run(context),
                            expected,
                        )
                    self.assertEqual(
                        rechecks,
                        [] if boundary.endswith("entry") else [True],
                    )
                    self.assertEqual(context._state, "failed")
                    self.assertTrue(preflight.closed)
                    self.assertEqual(bytes(key), b"\0" * 32)
                finally:
                    rpc._abandon_archive_context(context)


class HistoricalFoundryRpcTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = json.loads(load_historical_foundry_policy().physical_bytes)
        cls.authority = json.loads(
            load_historical_foundry_authority().physical_bytes
        )

    def _plan(self):
        return build_historical_anchor_request_plan(
            copy.deepcopy(self.policy), copy.deepcopy(self.authority)
        )

    def _materialized(self, plan=None, responses=None):
        plan = self._plan() if plan is None else plan
        responses = _synthetic_responses() if responses is None else responses
        anchor_rows = _materialize_historical_anchor_stage(plan, "anchor", [])
        fixed_rows = _materialize_historical_anchor_stage(
            plan, 1, responses[:2]
        )
        derived_rows = _materialize_historical_anchor_stage(
            plan, "derived_authority", responses[:39]
        )
        return list(anchor_rows + fixed_rows + derived_rows)

    def assertProjectionRejects(self, mutate):
        plan = self._plan()
        responses = _synthetic_responses()
        mutate(plan, responses)
        with self.assertRaises(ValueError):
            project_historical_anchor_capture(plan, responses)

    def test_public_surface_and_plan_are_closed_symbolic_and_detached(self):
        self.assertEqual(
            list(inspect.signature(build_historical_anchor_request_plan).parameters),
            ["policy", "authority"],
        )
        self.assertEqual(
            list(inspect.signature(project_historical_anchor_capture).parameters),
            ["plan", "responses"],
        )
        with self.assertRaises(TypeError):
            build_historical_anchor_request_plan(
                self.policy, self.authority, endpoint="https://forbidden.invalid"
            )
        with self.assertRaises(TypeError):
            project_historical_anchor_capture(
                self._plan(), _synthetic_responses(), block="0x100"
            )

        policy = copy.deepcopy(self.policy)
        authority = copy.deepcopy(self.authority)
        plan = build_historical_anchor_request_plan(policy, authority)
        self.assertEqual(
            set(plan),
            {"schema", "chain_id", "anchor_tag", "stages", "request_count", "request_ids"},
        )
        self.assertEqual(plan["schema"], "historical_foundry_anchor_request_plan/v1")
        self.assertEqual(plan["chain_id"], 1)
        self.assertEqual(plan["anchor_tag"], "finalized")
        self.assertEqual(plan["request_count"], 48)
        self.assertEqual(plan["request_ids"], list(range(1, 49)))
        self.assertEqual(
            [(stage["index"], stage["name"], len(stage["requests"])) for stage in plan["stages"]],
            [(0, "anchor", 2), (1, "fixed_authority", 37),
             (2, "derived_authority", 9)],
        )
        self.assertTrue(all(
            set(stage) == {"index", "name", "dependencies", "bindings", "requests"}
            for stage in plan["stages"]
        ))
        templates = [row for stage in plan["stages"] for row in stage["requests"]]
        self.assertEqual(
            [(row["id"], row["role"], row["method"]) for row in templates],
            list(ROLE_METHODS),
        )
        self.assertTrue(all(
            set(row) == {
                "id", "role", "method", "dependencies", "bindings",
                "params_template",
            }
            for row in templates
        ))
        self.assertEqual(
            templates[1],
            {
                "id": 2,
                "role": "finalized_anchor",
                "method": "eth_getBlockByNumber",
                "dependencies": [],
                "bindings": ["anchor_header", "anchor_block_reference"],
                "params_template": ["finalized", False],
            },
        )
        self.assertEqual(
            templates[39]["params_template"],
            [{"binding": "uniswap_v2_pair_address"},
             {"binding": "anchor_block_reference"}],
        )
        policy["chain_id"] = 9
        authority["tokens"][0]["address"] = "0x" + "99" * 20
        self.assertEqual(plan["chain_id"], 1)
        self.assertEqual(
            plan["stages"][1]["requests"][0]["params_template"][0], UNI
        )

        for source, mutation in (
            (self.policy, ("chain_id", 2)),
            (self.policy, ("anchor_tag", "latest")),
            (self.policy, ("block", "0x100")),
            (self.authority, ("pair", UNI_PAIR)),
            (self.authority, ("aggregator", AGGREGATOR)),
            (self.authority, ("request_id", 99)),
            (self.authority, ("selector", "0x12345678")),
        ):
            bad_policy = copy.deepcopy(self.policy)
            bad_authority = copy.deepcopy(self.authority)
            target = bad_policy if source is self.policy else bad_authority
            target[mutation[0]] = mutation[1]
            with self.assertRaises(ValueError):
                build_historical_anchor_request_plan(bad_policy, bad_authority)

    def test_planner_closes_mapping_capabilities_and_counts_actual_members(self):
        marker = "UNTRUSTED-MAPPING-CAPABILITY"

        class DelegatingMapping(Mapping):
            def __init__(self, value):
                self.value = value

            def __getitem__(self, key):
                return self.value[key]

            def __iter__(self):
                return iter(self.value)

            def __len__(self):
                return len(self.value)

        class ExplodingLength(DelegatingMapping):
            def __len__(self):
                raise RuntimeError(marker + "-LEN")

        class ExplodingItems(DelegatingMapping):
            def items(self):
                raise RuntimeError(marker + "-ITEMS")

        class ExplodingIterator(DelegatingMapping):
            def items(self):
                class Rows:
                    def __iter__(self):
                        return self

                    def __next__(self):
                        raise RuntimeError(marker + "-ITERATOR")

                return Rows()

        class DishonestLength(DelegatingMapping):
            def __len__(self):
                return 0

        class CancellingItems(DelegatingMapping):
            signal = KeyboardInterrupt

            def items(self):
                raise self.signal(marker)

        class CancellingLength(CancellingItems):
            def __len__(self):
                raise self.signal(marker)

            def items(self):
                return self.value.items()

        class CancellingIterator(CancellingItems):
            def items(self):
                signal = self.signal

                class Rows:
                    def __iter__(self):
                        return self

                    def __next__(self):
                        raise signal(marker)

                return Rows()

        for argument in ("policy", "authority"):
            source = self.policy if argument == "policy" else self.authority
            for mapping_type in (ExplodingLength, ExplodingItems, ExplodingIterator):
                policy = copy.deepcopy(self.policy)
                authority = copy.deepcopy(self.authority)
                if argument == "policy":
                    policy = mapping_type(source)
                else:
                    authority = mapping_type(source)
                with self.subTest(argument=argument, mapping=mapping_type.__name__):
                    with self.assertRaisesRegex(
                        ValueError, "historical anchor config mapping is invalid"
                    ) as caught:
                        build_historical_anchor_request_plan(policy, authority)
                    self.assertNotIn(marker, str(caught.exception))

            oversized = dict(source)
            for index in range(65 - len(oversized)):
                oversized["unexpected_" + str(index)] = index
            policy = copy.deepcopy(self.policy)
            authority = copy.deepcopy(self.authority)
            if argument == "policy":
                policy = DishonestLength(oversized)
            else:
                authority = DishonestLength(oversized)
            with self.subTest(argument=argument, mapping="dishonest_length"):
                with self.assertRaisesRegex(ValueError, "resource limit"):
                    build_historical_anchor_request_plan(policy, authority)

            for signal in (KeyboardInterrupt, SystemExit):
                for capability_type in (
                    CancellingLength, CancellingItems, CancellingIterator
                ):
                    cancelling_type = type(
                        capability_type.__name__ + signal.__name__,
                        (capability_type,),
                        {"signal": signal},
                    )
                    policy = copy.deepcopy(self.policy)
                    authority = copy.deepcopy(self.authority)
                    if argument == "policy":
                        policy = cancelling_type(source)
                    else:
                        authority = cancelling_type(source)
                    with self.subTest(
                        argument=argument, signal=signal.__name__,
                        capability=capability_type.__name__,
                    ):
                        with self.assertRaises(signal):
                            build_historical_anchor_request_plan(policy, authority)

        plan = build_historical_anchor_request_plan(
            MappingProxyType(copy.deepcopy(self.policy)),
            MappingProxyType(copy.deepcopy(self.authority)),
        )
        self.assertEqual(plan["request_ids"], list(range(1, 49)))
        loaded_plan = build_historical_anchor_request_plan(
            load_historical_foundry_policy().value,
            load_historical_foundry_authority().value,
        )
        self.assertEqual(loaded_plan, plan)

    def test_planner_seals_proxy_wrapped_mapping_capabilities(self):
        marker = "UNTRUSTED-PROXY-CAPABILITY"

        class CapabilityMapping(Mapping):
            def __init__(self, value, capability, signal):
                self.value = value
                self.capability = capability
                self.signal = signal

            def __getitem__(self, key):
                return self.value[key]

            def __iter__(self):
                return iter(self.value)

            def __len__(self):
                if self.capability == "len":
                    raise self.signal(marker + "-LEN")
                return len(self.value)

            def items(self):
                if self.capability == "items":
                    raise self.signal(marker + "-ITEMS")
                if self.capability == "iterator_acquisition":
                    signal = self.signal

                    class Rows:
                        def __iter__(self):
                            raise signal(marker + "-ITERATOR-ACQUISITION")

                    return Rows()
                if self.capability == "next":
                    signal = self.signal

                    class Rows:
                        def __iter__(self):
                            return self

                        def __next__(self):
                            raise signal(marker + "-NEXT")

                    return Rows()
                return self.value.items()

        class DishonestLength(Mapping):
            def __init__(self, value):
                self.value = value

            def __getitem__(self, key):
                return self.value[key]

            def __iter__(self):
                return iter(self.value)

            def __len__(self):
                return 0

            def items(self):
                return self.value.items()

        invoked_capabilities = ("items", "iterator_acquisition", "next")
        for argument in ("policy", "authority"):
            source = self.policy if argument == "policy" else self.authority
            policy = copy.deepcopy(self.policy)
            authority = copy.deepcopy(self.authority)
            wrapped = MappingProxyType(
                CapabilityMapping(source, "len", RuntimeError)
            )
            if argument == "policy":
                policy = wrapped
            else:
                authority = wrapped
            with self.subTest(argument=argument, capability="len_not_invoked"):
                plan = build_historical_anchor_request_plan(policy, authority)
                self.assertEqual(plan["request_ids"], list(range(1, 49)))

            for capability in invoked_capabilities:
                policy = copy.deepcopy(self.policy)
                authority = copy.deepcopy(self.authority)
                wrapped = MappingProxyType(
                    CapabilityMapping(source, capability, RuntimeError)
                )
                if argument == "policy":
                    policy = wrapped
                else:
                    authority = wrapped
                with self.subTest(argument=argument, capability=capability):
                    with self.assertRaisesRegex(
                        ValueError, "historical anchor config mapping is invalid"
                    ) as caught:
                        build_historical_anchor_request_plan(policy, authority)
                    self.assertNotIn(marker, str(caught.exception))

            oversized = dict(source)
            for index in range(65 - len(oversized)):
                oversized["unexpected_" + str(index)] = index
            policy = copy.deepcopy(self.policy)
            authority = copy.deepcopy(self.authority)
            wrapped = MappingProxyType(DishonestLength(oversized))
            if argument == "policy":
                policy = wrapped
            else:
                authority = wrapped
            with self.subTest(argument=argument, capability="dishonest_len"):
                with self.assertRaisesRegex(ValueError, "resource limit"):
                    build_historical_anchor_request_plan(policy, authority)

            for signal in (KeyboardInterrupt, SystemExit):
                for capability in invoked_capabilities:
                    policy = copy.deepcopy(self.policy)
                    authority = copy.deepcopy(self.authority)
                    wrapped = MappingProxyType(
                        CapabilityMapping(source, capability, signal)
                    )
                    if argument == "policy":
                        policy = wrapped
                    else:
                        authority = wrapped
                    with self.subTest(
                        argument=argument,
                        signal=signal.__name__,
                        capability=capability,
                    ):
                        with self.assertRaises(signal):
                            build_historical_anchor_request_plan(policy, authority)

        loaded_policy = load_historical_foundry_policy().value
        loaded_authority = load_historical_foundry_authority().value
        self.assertIs(type(loaded_policy), MappingProxyType)
        self.assertIs(type(loaded_authority), MappingProxyType)
        loaded_plan = build_historical_anchor_request_plan(
            loaded_policy, loaded_authority
        )
        self.assertEqual(loaded_plan["request_ids"], list(range(1, 49)))

    def test_materializer_freezes_all_three_canonical_wire_batches(self):
        plan = self._plan()
        responses = _synthetic_responses()
        anchor = _materialize_historical_anchor_stage(plan, 0, ())
        fixed = _materialize_historical_anchor_stage(plan, "fixed_authority", responses[:2])
        derived = _materialize_historical_anchor_stage(plan, 2, responses[:39])
        self.assertEqual(_canonical_bytes(anchor), _canonical_bytes(_expected_anchor_rows()))
        self.assertEqual(_canonical_bytes(fixed), _canonical_bytes(_expected_fixed_rows()))
        self.assertEqual(_canonical_bytes(derived), _canonical_bytes(_expected_derived_rows()))
        self.assertEqual(len(anchor + fixed + derived), 48)
        self.assertTrue(all(
            set(row) == {"jsonrpc", "id", "method", "params"}
            for row in anchor + fixed + derived
        ))
        self.assertTrue(all(
            row["params"][-1] == ANCHOR_REFERENCE
            for row in fixed + derived
        ))

        # Independent known-answer coverage for reused storage/calldata formulas.
        by_id = {row["id"]: row for row in fixed}
        self.assertEqual(by_id[6]["params"][1], UNI_BALANCE_KEY)
        self.assertEqual(by_id[8]["params"][1], UNI_UNI_ALLOWANCE_KEY)
        self.assertEqual(by_id[10]["params"][1], UNI_SUSHI_ALLOWANCE_KEY)
        self.assertEqual(by_id[14]["params"][1], WETH_BALANCE_KEY)
        self.assertEqual(by_id[16]["params"][1], WETH_UNI_ALLOWANCE_KEY)
        self.assertEqual(by_id[18]["params"][1], WETH_SUSHI_ALLOWANCE_KEY)
        self.assertEqual(
            by_id[23]["params"][0]["data"],
            "0xe6a439050000000000000000000000001f9840a85d5af5bf1d1762f925bdaddc4201f984"
            "000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        )
        self.assertEqual(
            by_id[24]["params"][0]["data"],
            "0xe6a43905000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            "0000000000000000000000001f9840a85d5af5bf1d1762f925bdaddc4201f984",
        )

    def test_materializer_rejects_stage_plan_and_prior_binding_attacks(self):
        plan = self._plan()
        responses = _synthetic_responses()
        for stage in ("unknown", -1, 3, True, None):
            with self.assertRaises(ValueError):
                _materialize_historical_anchor_stage(plan, stage, ())
        with self.assertRaises(ValueError):
            _materialize_historical_anchor_stage(plan, "anchor", responses[:1])
        for prior in (
            responses[:1],
            responses[:2] + [_success(49, "0x0")],
            [responses[0], responses[0]],
            [_success(True, "0x1"), responses[1]],
            [{"jsonrpc": "2.0", "id": 1, "error": {}}, responses[1]],
        ):
            with self.assertRaises(ValueError):
                _materialize_historical_anchor_stage(
                    plan, "fixed_authority", prior
                )
        bad_pair = copy.deepcopy(responses[:39])
        bad_pair[23]["result"] = _address_word("0x" + "44" * 20)
        with self.assertRaises(ValueError):
            _materialize_historical_anchor_stage(
                plan, "derived_authority", bad_pair
            )
        bad_aggregator = copy.deepcopy(responses[:39])
        bad_aggregator[33]["result"] = _address_word("0x" + "00" * 20)
        with self.assertRaises(ValueError):
            _materialize_historical_anchor_stage(
                plan, "derived_authority", bad_aggregator
            )
        mutated_plan = copy.deepcopy(plan)
        mutated_plan["stages"][0]["requests"][0]["method"] = "eth_blockNumber"
        with self.assertRaises(ValueError):
            _materialize_historical_anchor_stage(
                mutated_plan, "anchor", ()
            )

    def test_derived_addresses_are_disjoint_without_runtime_authenticity_overclaim(self):
        fixed_addresses = (
            UNI, WETH, UNI_ROUTER, UNI_FACTORY, SUSHI_ROUTER, SUSHI_FACTORY,
            FEED_PROXY, EXECUTOR, SENDER,
        )

        def reject_at_both_boundaries(responses):
            with self.assertRaises(ValueError):
                _materialize_historical_anchor_stage(
                    self._plan(), "derived_authority", responses[:39]
                )
            with self.assertRaises(ValueError):
                project_historical_anchor_capture(self._plan(), responses)

        for fixed_address in fixed_addresses:
            responses = _synthetic_responses()
            responses[22]["result"] = _address_word(fixed_address)
            responses[23]["result"] = _address_word(fixed_address)
            with self.subTest(kind="pair_to_fixed", address=fixed_address):
                reject_at_both_boundaries(responses)

            responses = _synthetic_responses()
            responses[33]["result"] = _address_word(fixed_address)
            with self.subTest(kind="aggregator_to_fixed", address=fixed_address):
                reject_at_both_boundaries(responses)

        for pair_address in (UNI_PAIR, SUSHI_PAIR):
            responses = _synthetic_responses()
            responses[33]["result"] = _address_word(pair_address)
            with self.subTest(kind="aggregator_to_pair", address=pair_address):
                reject_at_both_boundaries(responses)

        responses = _synthetic_responses()
        responses[22]["result"] = _address_word(AGGREGATOR)
        responses[23]["result"] = _address_word(AGGREGATOR)
        with self.subTest(kind="pair_to_aggregator"):
            reject_at_both_boundaries(responses)

        # No reviewed allowlist digests exist: arbitrary correct-ID nonempty
        # runtime observations remain admissible and are retained by digest.
        responses = _synthetic_responses()
        responses[2]["result"] = "0xdeadbeef"
        responses[39]["result"] = "0xcafebabe"
        capture = project_historical_anchor_capture(self._plan(), responses)
        self.assertEqual(
            capture["tokens"][0]["runtime"]["sha256"],
            hashlib.sha256(bytes.fromhex("deadbeef")).hexdigest(),
        )
        self.assertEqual(
            capture["venues"][0]["pair"]["runtime"]["sha256"],
            hashlib.sha256(bytes.fromhex("cafebabe")).hexdigest(),
        )

    def test_projector_accepts_permuted_rows_and_returns_closed_detached_capture(self):
        plan = self._plan()
        responses = _synthetic_responses()
        capture = project_historical_anchor_capture(plan, list(reversed(responses)))
        self.assertEqual(
            set(capture),
            {"schema", "chain_id", "anchor", "tokens", "venues", "price_feed",
             "executor", "sender", "request_inventory"},
        )
        self.assertEqual(capture["schema"], "historical_foundry_anchor_capture/v1")
        self.assertEqual(capture["chain_id"], 1)
        self.assertEqual(
            capture["anchor"],
            {
                "number": "0x100",
                "hash": ANCHOR_HASH,
                "parent_hash": PARENT_HASH,
                "state_root": STATE_ROOT,
                "timestamp": "0x65",
                "gas_limit": "0x1c9c380",
                "gas_used": "0x100",
                "base_fee_per_gas": "0x1",
            },
        )
        self.assertEqual([row["role"] for row in capture["tokens"]], ["uni", "weth"])
        self.assertEqual(
            [row["venue_id"] for row in capture["venues"]],
            ["uniswap_v2", "sushiswap_v2"],
        )
        self.assertEqual(capture["venues"][0]["pair"]["address"], UNI_PAIR)
        self.assertEqual(capture["venues"][1]["pair"]["address"], SUSHI_PAIR)
        self.assertEqual(capture["price_feed"]["aggregator"]["address"], AGGREGATOR)
        self.assertEqual(capture["executor"]["prior_code"], "0x")
        self.assertEqual(capture["executor"]["prior_nonce"], 0)
        self.assertEqual(capture["sender"]["prior_nonce"], 0)
        self.assertEqual(len(capture["request_inventory"]), 48)
        self.assertNotIn("endpoint", _canonical_bytes(capture).decode("utf-8"))
        self.assertNotIn("provider", _canonical_bytes(capture).decode("utf-8"))

        uni_runtime = capture["tokens"][0]["runtime"]
        self.assertEqual(uni_runtime["role"], "uni_runtime")
        self.assertEqual(uni_runtime["address"], UNI)
        self.assertEqual(
            uni_runtime["sha256"], hashlib.sha256(bytes.fromhex("6003")).hexdigest()
        )
        from scripts.route_cost_evidence import keccak256
        self.assertEqual(
            uni_runtime["keccak256"], keccak256(bytes.fromhex("6003")).hex()
        )

        plan["request_ids"][0] = 999
        responses[0]["result"] = "0x2"
        self.assertEqual(capture["request_inventory"][0]["id"], 1)
        self.assertEqual(capture["chain_id"], 1)
        self.assertEqual(
            capture,
            project_historical_anchor_capture(self._plan(), _synthetic_responses()),
        )
        self.assertTrue(_validate_historical_anchor_capture(capture))

        self.assertEqual(
            set(capture["anchor"]),
            {"number", "hash", "parent_hash", "state_root", "timestamp",
             "gas_limit", "gas_used", "base_fee_per_gas"},
        )
        self.assertTrue(all(
            set(token) == {
                "role", "address", "decimals", "prior_balance_raw",
                "allowances", "runtime",
            }
            for token in capture["tokens"]
        ))
        self.assertTrue(all(
            set(token["runtime"]) == {"role", "address", "sha256", "keccak256"}
            for token in capture["tokens"]
        ))
        self.assertTrue(all(
            set(allowance) == {
                "venue_id", "router_address", "storage_key", "prior_value_raw"
            }
            for token in capture["tokens"] for allowance in token["allowances"]
        ))
        self.assertTrue(all(
            set(venue) == {"venue_id", "router", "factory", "pair"}
            for venue in capture["venues"]
        ))
        self.assertTrue(all(
            set(venue["router"]) == {
                "address", "factory_address", "weth_address", "runtime"
            }
            and set(venue["factory"]) == {"address", "runtime"}
            and set(venue["pair"]) == {
                "address", "factory_address", "token0", "token1", "runtime"
            }
            for venue in capture["venues"]
        ))
        self.assertTrue(all(
            set(runtime) == {"role", "address", "sha256", "keccak256"}
            for venue in capture["venues"]
            for runtime in (
                venue["router"]["runtime"], venue["factory"]["runtime"],
                venue["pair"]["runtime"],
            )
        ))
        self.assertEqual(
            set(capture["price_feed"]),
            {"description", "decimals", "phase_id", "latest_round",
             "proxy", "aggregator"},
        )
        self.assertEqual(
            set(capture["price_feed"]["latest_round"]),
            {"round_id", "answer", "started_at", "updated_at",
             "answered_in_round"},
        )
        self.assertEqual(set(capture["price_feed"]["proxy"]), {"address", "runtime"})
        self.assertEqual(
            set(capture["price_feed"]["aggregator"]), {"address", "runtime"}
        )
        self.assertEqual(
            set(capture["price_feed"]["proxy"]["runtime"]),
            {"role", "address", "sha256", "keccak256"},
        )
        self.assertEqual(
            set(capture["price_feed"]["aggregator"]["runtime"]),
            {"role", "address", "sha256", "keccak256"},
        )
        self.assertEqual(
            set(capture["executor"]), {"address", "prior_code", "prior_nonce"}
        )
        self.assertEqual(set(capture["sender"]), {"address", "prior_nonce"})

    def test_projector_accepts_realistic_raw_block_and_large_protocol_arrays(self):
        responses = _synthetic_responses()
        raw_header = responses[1]["result"]
        transactions = ["0x" + format(index, "064x") for index in range(1, 67)]
        raw_header.update({
            "difficulty": "0x0",
            "extraData": "0x",
            "logsBloom": "0x" + "00" * 256,
            "miner": "0x" + "44" * 20,
            "mixHash": "0x" + "55" * 32,
            "nonce": "0x0000000000000000",
            "receiptsRoot": "0x" + "66" * 32,
            "sha3Uncles": "0x" + "77" * 32,
            "size": "0x1",
            "totalDifficulty": "0x0",
            "transactions": transactions,
            "transactionsRoot": "0x" + "88" * 32,
            "uncles": [],
            "withdrawals": [],
            "withdrawalsRoot": "0x" + "99" * 32,
        })
        capture = project_historical_anchor_capture(self._plan(), responses)
        self.assertEqual(
            capture["request_inventory"][1]["response"]["result"], raw_header
        )
        self.assertEqual(len(
            capture["request_inventory"][1]["response"]["result"]["transactions"]
        ), 66)
        self.assertEqual(
            set(capture["anchor"]),
            {"number", "hash", "parent_hash", "state_root", "timestamp",
             "gas_limit", "gas_used", "base_fee_per_gas"},
        )
        self.assertTrue(_validate_historical_anchor_capture(capture))

        unknown_projection = copy.deepcopy(capture)
        unknown_projection["anchor"]["difficulty"] = "0x0"
        with self.assertRaises(ValueError):
            _validate_historical_anchor_capture(unknown_projection)

    def test_request_inventory_binds_exact_requests_responses_and_roles(self):
        plan = self._plan()
        responses = _synthetic_responses()
        capture = project_historical_anchor_capture(plan, responses)
        requests = {row["id"]: row for row in self._materialized(plan, responses)}
        response_by_id = {row["id"]: row for row in responses}
        roles = dict((request_id, role) for request_id, role, _method in ROLE_METHODS)
        for row in capture["request_inventory"]:
            request = requests[row["id"]]
            response = response_by_id[row["id"]]
            self.assertEqual(
                set(row),
                {"id", "role", "method", "request", "response",
                 "params_sha256", "request_sha256", "result_sha256",
                 "response_sha256"},
            )
            self.assertEqual(row["role"], roles[row["id"]])
            self.assertEqual(row["method"], request["method"])
            self.assertEqual(row["request"], request)
            self.assertEqual(row["response"], response)
            self.assertEqual(
                row["params_sha256"],
                _typed_hash(
                    b"historical_foundry_anchor_request_params/v1",
                    request["params"],
                ),
            )
            self.assertEqual(
                row["request_sha256"],
                _typed_hash(b"historical_foundry_anchor_request/v1", request),
            )
            self.assertEqual(
                row["result_sha256"],
                _typed_hash(
                    b"historical_foundry_anchor_response_result/v1",
                    response["result"],
                ),
            )
            self.assertEqual(
                row["response_sha256"],
                _typed_hash(b"historical_foundry_anchor_response/v1", response),
            )
        self.assertTrue(_validate_historical_anchor_capture(capture))

    def test_capture_semantic_replay_rejects_post_capture_observation_transplants(self):
        capture = project_historical_anchor_capture(
            self._plan(), _synthetic_responses()
        )
        transplanted = copy.deepcopy(capture)
        transplanted["tokens"][0]["runtime"], transplanted["tokens"][1]["runtime"] = (
            transplanted["tokens"][1]["runtime"],
            transplanted["tokens"][0]["runtime"],
        )
        with self.assertRaises(ValueError):
            _validate_historical_anchor_capture(transplanted)

        transplanted = copy.deepcopy(capture)
        transplanted["venues"][0]["pair"]["address"] = SUSHI_PAIR
        transplanted["venues"][0]["pair"]["runtime"] = copy.deepcopy(
            transplanted["venues"][1]["pair"]["runtime"]
        )
        with self.assertRaises(ValueError):
            _validate_historical_anchor_capture(transplanted)

        rehashed = copy.deepcopy(capture)
        rehashed["tokens"][0]["runtime"]["sha256"] = hashlib.sha256(
            bytes.fromhex("6011")
        ).hexdigest()
        with self.assertRaises(ValueError):
            _validate_historical_anchor_capture(rehashed)

        # Recomputing the directly adjacent hashes cannot authorize a semantic
        # pair-token transplant in the retained response preimage.
        rehashed = copy.deepcopy(capture)
        inventory_row = rehashed["request_inventory"][41]
        inventory_row["response"]["result"] = _address_word(AGGREGATOR)
        inventory_row["result_sha256"] = _typed_hash(
            b"historical_foundry_anchor_response_result/v1",
            inventory_row["response"]["result"],
        )
        inventory_row["response_sha256"] = _typed_hash(
            b"historical_foundry_anchor_response/v1",
            inventory_row["response"],
        )
        rehashed["venues"][0]["pair"]["token0"] = AGGREGATOR
        with self.assertRaises(ValueError):
            _validate_historical_anchor_capture(rehashed)

    def test_projector_rejects_every_response_id_and_shape_attack(self):
        attacks = []
        attacks.append(lambda rows: rows.pop())
        attacks.append(lambda rows: rows.append(_success(49, "0x0")))
        attacks.append(lambda rows: rows.__setitem__(47, copy.deepcopy(rows[46])))
        attacks.append(lambda rows: rows[0].__setitem__("id", True))
        attacks.append(lambda rows: rows[0].__setitem__("id", 0))
        attacks.append(lambda rows: rows[0].__setitem__("id", 49))
        attacks.append(lambda rows: rows[0].__setitem__("jsonrpc", "1.0"))
        attacks.append(lambda rows: rows[0].__setitem__("extra", None))
        attacks.append(lambda rows: rows[0].pop("result"))
        attacks.append(
            lambda rows: rows.__setitem__(0, {"jsonrpc": "2.0", "id": 1,
                                               "error": {"code": -1}})
        )
        for attack in attacks:
            with self.subTest(attack=attacks.index(attack)):
                rows = _synthetic_responses()
                attack(rows)
                with self.assertRaises(ValueError):
                    project_historical_anchor_capture(self._plan(), rows)
        for not_a_sequence in ("responses", b"responses", {"id": 1}, None):
            with self.assertRaises(ValueError):
                project_historical_anchor_capture(self._plan(), not_a_sequence)

    def test_projector_rebuilds_and_rejects_any_plan_template_mutation(self):
        def mutate(path, value):
            plan = self._plan()
            target = plan
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with self.assertRaises(ValueError):
                project_historical_anchor_capture(plan, _synthetic_responses())

        mutate(("schema",), "historical_foundry_anchor_request_plan/v2")
        mutate(("extra",), None)
        mutate(("request_count",), 47)
        mutate(("request_ids",), list(range(2, 50)))
        mutate(("stages", 0, "name"), "headers")
        mutate(("stages", 0, "extra"), None)
        mutate(("stages", 1, "requests", 0, "id"), 4)
        mutate(("stages", 1, "requests", 0, "role"), "weth_runtime")
        mutate(("stages", 1, "requests", 0, "method"), "eth_call")
        mutate(("stages", 1, "requests", 0, "dependencies"), [])
        mutate(("stages", 1, "requests", 0, "bindings"), ["forged"])
        mutate(("stages", 1, "requests", 0, "params_template"),
               [WETH, {"binding": "anchor_block_reference"}])

    def test_projector_rejects_chain_header_and_quantity_attacks(self):
        cases = (
            (1, "0x2"),
            (2, None),
        )
        for request_id, value in cases:
            self.assertProjectionRejects(
                lambda _plan, rows, i=request_id, v=value:
                rows[i - 1].__setitem__("result", v)
            )
        header_mutations = {
            "number": "0x0100",
            "hash": "0x" + "AA" * 32,
            "parentHash": "0x" + "00" * 31,
            "stateRoot": "0x" + "gg" * 32,
            "timestamp": "0x00",
            "gasLimit": "0x0",
            "gasUsed": "0x1c9c381",
            "baseFeePerGas": "0x01",
        }
        for key, value in header_mutations.items():
            def attack(_plan, rows, field=key, replacement=value):
                rows[1]["result"][field] = replacement
            self.assertProjectionRejects(attack)
        self.assertProjectionRejects(
            lambda _plan, rows: rows[1]["result"].pop("stateRoot")
        )

    def test_projector_rejects_router_pair_token_and_runtime_attacks(self):
        mutations = {
            3: "0x",
            19: "0x",
            20: _address_word(SUSHI_FACTORY),
            21: _address_word(UNI),
            22: "0x",
            24: _address_word("0x" + "44" * 20),
            26: _address_word(UNI_FACTORY),
            27: _address_word(UNI),
            28: "0x",
            30: _address_word("0x" + "55" * 20),
            31: "0x",
            40: "0x",
            41: _address_word(SUSHI_FACTORY),
            42: _address_word(AGGREGATOR),
            43: _address_word(AGGREGATOR),
            44: "0x",
            45: _address_word(UNI_FACTORY),
            46: _address_word(AGGREGATOR),
            47: _address_word(AGGREGATOR),
            48: "0x",
        }
        for request_id, replacement in mutations.items():
            self.assertProjectionRejects(
                lambda _plan, rows, i=request_id, value=replacement:
                rows[i - 1].__setitem__("result", value)
            )
        self.assertProjectionRejects(
            lambda _plan, rows: rows[22].__setitem__(
                "result", _address_word("0x" + "00" * 20)
            )
        )
        def collide_pairs(_plan, rows):
            rows[28]["result"] = _address_word(UNI_PAIR)
            rows[29]["result"] = _address_word(UNI_PAIR)
        self.assertProjectionRejects(collide_pairs)

    def test_projector_rejects_token_storage_executor_and_sender_attacks(self):
        for request_id, replacement in (
            (4, _word(8)),
            (5, _word(1)),
            (6, _word(1)),
            (7, _word(1)),
            (8, _word(1)),
            (9, _word(1)),
            (10, _word(1)),
            (12, _word(8)),
            (13, _word(1)),
            (14, _word(1)),
            (15, _word(1)),
            (16, _word(1)),
            (17, _word(1)),
            (18, _word(1)),
            (37, "0x60"),
            (38, "0x1"),
            (39, "0x1"),
        ):
            self.assertProjectionRejects(
                lambda _plan, rows, i=request_id, value=replacement:
                rows[i - 1].__setitem__("result", value)
            )
        self.assertProjectionRejects(
            lambda _plan, rows: rows[5].__setitem__("result", "0x" + "00" * 31)
        )
        self.assertProjectionRejects(
            lambda _plan, rows: rows[7].__setitem__("result", ZERO_WORD + "00")
        )

    def test_projector_rejects_feed_round_and_strict_abi_attacks(self):
        valid_round = (7 << 64) + 42
        replacements = (
            (32, _abi_string("ETH/USD")),
            (33, _word(18)),
            (34, _address_word("0x" + "00" * 20)),
            (35, _word(0)),
            (35, _word(1 << 16)),
            (36, _round_data((8 << 64) + 1, 300_000_000_000, 80, 100,
                             (8 << 64) + 1)),
            (36, _round_data(7 << 64, 300_000_000_000, 80, 100,
                             7 << 64)),
            (36, _round_data(1 << 80, 300_000_000_000, 80, 100,
                             1 << 80)),
            (36, _round_data(valid_round, 0, 80, 100, valid_round)),
            (36, _round_data(valid_round, -1, 80, 100, valid_round)),
            (36, _round_data(valid_round, 300_000_000_000, 0, 100,
                             valid_round)),
            (36, _round_data(valid_round, 300_000_000_000, 80, 102, valid_round)),
            (36, _round_data(valid_round, 300_000_000_000, 100, 80, valid_round)),
            (36, _round_data(valid_round, 300_000_000_000, 80, 100,
                             valid_round - 1)),
            (36, _round_data(valid_round, 300_000_000_000, 80, 100,
                             1 << 80)),
        )
        for request_id, replacement in replacements:
            self.assertProjectionRejects(
                lambda _plan, rows, i=request_id, value=replacement:
                rows[i - 1].__setitem__("result", value)
            )
        for request_id in (20, 32, 34, 36):
            self.assertProjectionRejects(
                lambda _plan, rows, i=request_id:
                rows[i - 1].__setitem__("result", rows[i - 1]["result"][:-2])
            )
            self.assertProjectionRejects(
                lambda _plan, rows, i=request_id:
                rows[i - 1].__setitem__("result", rows[i - 1]["result"] + "00" * 32)
            )
        malformed_padding = _abi_string("ETH / USD")[:-2] + "01"
        self.assertProjectionRejects(
            lambda _plan, rows: rows[31].__setitem__("result", malformed_padding)
        )

    def test_direct_pure_calls_enforce_closed_resource_limits(self):
        rows = _synthetic_responses()
        rows[2]["result"] = "0x" + "60" * 24_576
        project_historical_anchor_capture(self._plan(), rows)
        rows[2]["result"] += "60"
        with self.assertRaisesRegex(ValueError, "resource limit"):
            project_historical_anchor_capture(self._plan(), rows)

        rows = _synthetic_responses()
        rows[0]["result"] = "x" * 262_144
        with self.assertRaises(ValueError):
            project_historical_anchor_capture(self._plan(), rows)
        rows[0]["result"] += "x"
        with self.assertRaisesRegex(ValueError, "resource limit"):
            project_historical_anchor_capture(self._plan(), rows)

        rows = _synthetic_responses()
        rows[0]["result"] = [0] * 65
        with self.assertRaises(ValueError):
            project_historical_anchor_capture(self._plan(), rows)

    def test_direct_pure_boundary_rejects_value_and_container_subclasses(self):
        class StringAlias(str):
            pass

        class IntegerAlias(int):
            pass

        class MappingAlias(dict):
            pass

        class SequenceAlias(list):
            pass

        for mutate in (
            lambda rows: rows[0].__setitem__("result", StringAlias("0x1")),
            lambda rows: rows[0].__setitem__("id", IntegerAlias(1)),
            lambda rows: rows.__setitem__(0, MappingAlias(rows[0])),
        ):
            rows = _synthetic_responses()
            mutate(rows)
            with self.assertRaises(ValueError):
                project_historical_anchor_capture(self._plan(), rows)
        with self.assertRaises(ValueError):
            project_historical_anchor_capture(
                self._plan(), SequenceAlias(_synthetic_responses())
            )


class HistoricalFoundryRelayNativeTests(unittest.TestCase):
    def test_task6_source_inventory_and_sealed_relay_boundary(self):
        import scripts.historical_foundry_rpc as rpc

        self.assertIn(
            "source:historical_foundry_anvil",
            tuple(row[0] for row in rpc._PRODUCTION_SOURCE_MEMBERS),
        )
        lease = rpc._issue_historical_relay_lease_for_test(
            endpoint="https://fixture.invalid/archive",
            operation=lambda _body, _remaining: (
                b'{"id":1,"jsonrpc":"2.0","result":"0x1"}'
            ),
            monotonic=lambda: 0.0,
            entropy=lambda size: b"q" * size,
        )
        self.assertIs(rpc._require_historical_relay_lease(lease), lease)
        self.assertNotIn("fixture", repr(lease))
        block_hash = "0x" + "4" * 64
        facade = rpc._issue_historical_relay_scenario_facade(
            relay_lease=lease,
            authority={
                "block_number": 1, "block_hash": block_hash,
                "block_tag": {"blockHash": block_hash, "requireCanonical": True},
                "addresses": frozenset(), "calls": frozenset(),
            },
            absolute_deadline=120.0,
        )
        request = b'{"id":1,"jsonrpc":"2.0","method":"eth_chainId","params":[]}'
        self.assertEqual(
            rpc._relay_historical_archive_call(
                relay_lease=facade, canonical_request_bytes=request
            ),
            b'{"id":1,"jsonrpc":"2.0","result":"0x1"}',
        )
        with self.assertRaises(ValueError):
            rpc._relay_historical_archive_call(
                relay_lease=facade,
                canonical_request_bytes=b'[{"id":1,"jsonrpc":"2.0","method":"eth_chainId","params":[]}]',
            )
        facade.close()
        with self.assertRaises(ValueError):
            rpc._require_historical_relay_lease(
                object.__new__(type(lease))
            )
        lease.close()


if __name__ == "__main__":
    unittest.main()
