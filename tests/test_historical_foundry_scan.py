from __future__ import annotations

import asyncio
import ast
import contextvars
import copy
from decimal import Decimal, localcontext
import dis
import gc
import hashlib
import importlib
import inspect
import json
import linecache
import pickle
import os
import sys
import tempfile
import traceback
import types
import unittest
import weakref
from unittest import mock

from scripts.historical_foundry_contracts import load_historical_foundry_config_set
from scripts.historical_foundry_rpc import (
    build_historical_anchor_request_plan,
    project_historical_anchor_capture,
)
from scripts.historical_foundry_scan import (
    HistoricalWindowProjectionError,
    _build_historical_block_header_request,
    _guard_historical_json_value,
    _historical_json_int_token_bytes,
    _preflight_historical_decimal_tuple,
    _project_complete_historical_window_root,
    _project_historical_block_header_success,
    _ratio_decimal_token,
    build_historical_window_request_plan,
    iter_historical_header_request_batches,
    iter_historical_state_request_batches,
    locate_inclusive_lower_bound,
    project_historical_header_inventory,
    project_historical_lower_bound_capture,
    project_historical_window_projection,
)
from tests.test_historical_foundry_rpc import (
    _round_data,
    _synthetic_responses,
)


LOOKBACK = 604_800
PAIR_UNISWAP = "0x1111111111111111111111111111111111111111"
PAIR_SUSHI = "0x2222222222222222222222222222222222222222"
FEED_PROXY = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
PHASE_ID = 7
ANSWER = 300_000_000_000


def _hash_for(number):
    return "0x" + format(number + 1, "064x")


def _state_root_for(number):
    return "0x" + format((1 << 128) + number + 1, "064x")


def _normalized_header(number, timestamp, *, gas_limit=2, gas_used=1, base_fee=0):
    return {
        "number": number,
        "hash": _hash_for(number),
        "parent_hash": _hash_for(number - 1) if number else "0x" + "ff" * 32,
        "state_root": _state_root_for(number),
        "timestamp": timestamp,
        "gas_limit": gas_limit,
        "gas_used": gas_used,
        "base_fee_per_gas": base_fee,
    }


def _raw_header(header, *, extra=True):
    result = {
        "number": hex(header["number"]),
        "hash": header["hash"],
        "parentHash": header["parent_hash"],
        "stateRoot": header["state_root"],
        "timestamp": hex(header["timestamp"]),
        "gasLimit": hex(header["gas_limit"]),
        "gasUsed": hex(header["gas_used"]),
        "baseFeePerGas": hex(header["base_fee_per_gas"]),
    }
    if extra:
        result["transactions"] = []
        result["difficulty"] = "0x0"
    return result


def _capture_for_header(anchor_header):
    config = load_historical_foundry_config_set()
    plan = build_historical_anchor_request_plan(
        config.policy.value,
        config.authority.value,
    )
    responses = copy.deepcopy(_synthetic_responses())
    responses[1]["result"] = _raw_header(anchor_header)
    round_id = (PHASE_ID << 64) + anchor_header["number"] + 1
    responses[35]["result"] = _round_data(
        round_id,
        ANSWER,
        anchor_header["timestamp"],
        anchor_header["timestamp"],
        round_id,
    )
    return project_historical_anchor_capture(plan, responses)


def _observation(block_number, request_id, header):
    request = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "eth_getBlockByNumber",
        "params": [hex(block_number), False],
    }
    return {
        "request": request,
        "response": {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": _raw_header(header),
        },
    }


def _lower_capture(anchor_capture, header_at):
    anchor_number = int(anchor_capture["anchor"]["number"], 16)
    cutoff = int(anchor_capture["anchor"]["timestamp"], 16) - LOOKBACK
    lo = 0
    hi = anchor_number
    request_id = 49
    probes = []
    while lo < hi:
        mid = (lo + hi) // 2
        header = header_at(mid)
        probes.append(_observation(mid, request_id, header))
        request_id += 1
        if header["timestamp"] >= cutoff:
            hi = mid
        else:
            lo = mid + 1
    lower = lo
    numbers = (lower - 1, lower) if lower else (0,)
    witness = []
    for number in numbers:
        witness.append(_observation(number, request_id, header_at(number)))
        request_id += 1
    return project_historical_lower_bound_capture(
        anchor_capture=anchor_capture,
        lookback_seconds=LOOKBACK,
        search_probes=iter(probes),
        boundary_witness=iter(witness),
    )


def _small_context():
    headers = {
        0: _normalized_header(0, 24),
        1: _normalized_header(1, 25),
        2: _normalized_header(2, 604_825),
    }
    capture = _capture_for_header(headers[2])
    lower = _lower_capture(capture, headers.__getitem__)
    plan = build_historical_window_request_plan(
        lower_bound_capture=lower,
        anchor_capture=capture,
    )
    return headers, capture, lower, plan


def _three_block_context():
    headers = {
        0: _normalized_header(0, 24),
        1: _normalized_header(1, 25),
        2: _normalized_header(2, 26),
        3: _normalized_header(3, 604_825),
    }
    capture = _capture_for_header(headers[3])
    lower = _lower_capture(capture, headers.__getitem__)
    plan = build_historical_window_request_plan(
        lower_bound_capture=lower,
        anchor_capture=capture,
    )
    return headers, capture, lower, plan


class _Task4bFormulaicHeaders:
    __slots__ = ("anchor_number",)

    def __init__(self, anchor_number):
        self.anchor_number = anchor_number

    def __getitem__(self, number):
        if (
            type(number) is not int
            or number < 0
            or number > self.anchor_number
        ):
            raise KeyError(number)
        return _normalized_header(number, number * 12 + 1)


def _maximum_task4b_context():
    headers = _Task4bFormulaicHeaders(50_400)
    capture = _capture_for_header(headers[50_400])
    lower = _lower_capture(capture, headers.__getitem__)
    plan = build_historical_window_request_plan(
        lower_bound_capture=lower,
        anchor_capture=capture,
    )
    return headers, capture, lower, plan


def _reserve_result(reserve0, reserve1, timestamp):
    return "0x" + "".join(
        value.to_bytes(32, "big").hex()
        for value in (reserve0, reserve1, timestamp)
    )


def _price_result(number, timestamp, *, age=0):
    round_id = (PHASE_ID << 64) + number + 1
    updated_at = timestamp - age
    return _round_data(round_id, ANSWER, updated_at, updated_at, round_id)


def _responses_for_descriptor(descriptor, header_at):
    rows = []
    kind = descriptor["kind"]
    for offset, request in enumerate(descriptor["requests"]):
        if kind in ("header", "final_anchor"):
            number = int(request["params"][0], 16)
            result = _raw_header(header_at(number))
        elif kind == "reserve":
            number = descriptor["block_start"] + offset // 2
            result = _reserve_result(number + 1, number + 2, number)
        elif kind == "price":
            number = descriptor["block_start"] + offset
            result = _price_result(number, header_at(number)["timestamp"])
        elif kind == "fee_history":
            start = descriptor["block_start"]
            stop = descriptor["block_stop"]
            count = stop - start + 1
            result = {
                "oldestBlock": hex(start),
                "baseFeePerGas": ["0x0"] * (count + 1),
                "gasUsedRatio": [Decimal("0.5")] * count,
                "reward": [["0x1", "0x2"] for _ in range(count)],
            }
        else:
            raise AssertionError("unexpected descriptor kind")
        rows.append({"jsonrpc": "2.0", "id": request["id"], "result": result})
    return tuple(rows)


def _detach_task4b_test_value(value):
    if type(value) in (dict, types.MappingProxyType):
        return {
            key: _detach_task4b_test_value(nested)
            for key, nested in value.items()
        }
    if type(value) in (list, tuple):
        return tuple(_detach_task4b_test_value(nested) for nested in value)
    return value


def _freeze_task4b_test_value(value):
    if type(value) is dict:
        return types.MappingProxyType({
            key: _freeze_task4b_test_value(nested)
            for key, nested in value.items()
        })
    if type(value) in (list, tuple):
        return tuple(_freeze_task4b_test_value(nested) for nested in value)
    return value


def _task4b_executing_python_identity():
    version = sys.version_info
    if sys.implementation.name != "cpython":
        raise AssertionError("Task4b requires CPython")
    return {
        "implementation": "CPython",
        "major": version.major,
        "minor": version.minor,
        "micro": version.micro,
        "releaselevel": version.releaselevel,
        "serial": version.serial,
        "cache_tag": sys.implementation.cache_tag,
    }


def _assert_task4b_offline_preflight_identity(preflight):
    if type(preflight) is not _Task4bOfflineProductionPreflightShim:
        raise AssertionError("Task4b preflight shim type differs")
    if preflight.closed:
        raise AssertionError("Task4b preflight shim is closed")
    preflight.sources.verify()
    identity = preflight.identity
    if type(identity) is not dict or tuple(identity) != (
        "repository_head",
        "python",
        "configs",
        "sources",
        "project_inputs",
        "toolchain",
        "executor_artifact",
        "deployed_runtime_sha256",
    ):
        raise AssertionError("Task4b preflight identity shape differs")
    if identity["repository_head"] != (
        "a6a4a374a333b9ef5387c6a6ff21b653dd4ea725"
    ) or identity["python"] != _task4b_executing_python_identity():
        raise AssertionError("Task4b runtime identity differs")
    current_sources = tuple(
        _detach_task4b_test_value(row)
        for row in preflight.sources.projections
    )
    if identity["sources"] != current_sources:
        raise AssertionError("Task4b held source projections differ")
    projection_by_role = {
        row["role"]: row for row in current_sources
    }
    config_rows = (
        ("policy", "config:replay_policy"),
        ("authority", "config:replay_authority"),
        ("toolchain", "config:replay_toolchain"),
    )
    for attribute, role in config_rows:
        loaded = getattr(preflight.config, attribute)
        if (
            loaded.physical_bytes != preflight.sources.member_bytes(role)
            or loaded.physical_sha256
            != preflight.sources.member_digest(role)
            or projection_by_role[role]["size_bytes"]
            != len(loaded.physical_bytes)
            or projection_by_role[role]["sha256"]
            != loaded.physical_sha256
        ):
            raise AssertionError("Task4b held config identity differs")
    policy_bytes = preflight.sources.member_bytes("config:replay_policy")
    policy = json.loads(policy_bytes.decode("utf-8"))
    authority_sha256 = preflight.sources.member_digest(
        "config:replay_authority"
    )
    toolchain_sha256 = preflight.sources.member_digest(
        "config:replay_toolchain"
    )
    policy_sha256 = hashlib.sha256(policy_bytes).hexdigest()
    if (
        policy["authority_sha256"] != authority_sha256
        or policy["toolchain_sha256"] != toolchain_sha256
        or identity["configs"] != {
            "policy_id": "policy:" + policy_sha256,
            "policy_physical_sha256": policy_sha256,
            "authority_physical_sha256": authority_sha256,
            "toolchain_physical_sha256": toolchain_sha256,
        }
    ):
        raise AssertionError("Task4b config duplicate identity differs")
    project_inputs = identity["project_inputs"]
    if (
        type(project_inputs) is not dict
        or tuple(project_inputs) != (
            "schema",
            "foundry_toml_sha256",
            "foundry_lock_sha256",
            "gitmodules_sha256",
            "forge_std_commit",
            "forge_std_tree_sha256",
        )
        or project_inputs["foundry_toml_sha256"]
        != preflight.sources.member_digest("build:foundry_toml")
        or project_inputs["foundry_lock_sha256"]
        != preflight.sources.member_digest("build:foundry_lock")
        or project_inputs["gitmodules_sha256"]
        != preflight.sources.member_digest("build:gitmodules")
        or project_inputs["forge_std_commit"]
        != identity["toolchain"]["forge_std"]["commit"]
    ):
        raise AssertionError("Task4b project input identity differs")
    artifact = identity["executor_artifact"]
    if (
        type(artifact) is not dict
        or artifact["policy_physical_sha256"] != policy_sha256
        or artifact["authority_physical_sha256"] != authority_sha256
        or artifact["toolchain_physical_sha256"] != toolchain_sha256
        or identity["deployed_runtime_sha256"]
        != artifact["deployed_runtime_sha256"]
    ):
        raise AssertionError("Task4b executor duplicate identity differs")
    if identity != preflight.opening_identity:
        raise AssertionError("Task4b complete preflight identity drifted")


class _Task4bOfflineProductionPreflightShim:
    __slots__ = (
        "root", "sources", "config", "identity", "opening_identity",
        "closed",
    )

    def __init__(self, root, sources, config, identity):
        self.root = root
        self.sources = sources
        self.config = config
        self.identity = _detach_task4b_test_value(identity)
        self.opening_identity = _freeze_task4b_test_value(
            _detach_task4b_test_value(identity)
        )
        self.closed = False

    def close(self):
        if not self.closed:
            self.closed = True
            self.sources.close()


def _recheck_task4b_offline_production_preflight(preflight):
    try:
        _assert_task4b_offline_preflight_identity(preflight)
    except Exception:
        return False
    return True


def _new_task4b_offline_production_preflight(rpc):
    root = rpc.Path(rpc.__file__).resolve().parents[1]
    sources = rpc._HeldArchiveSourceAuthority(root)
    sources.open_members()
    try:
        config = load_historical_foundry_config_set()
        fixture = _detach_task4b_test_value(rpc._test_preflight_identity())
        projection_by_role = {
            row["role"]: row for row in sources.projections
        }
        for attribute, role in (
            ("policy", "config:replay_policy"),
            ("authority", "config:replay_authority"),
            ("toolchain", "config:replay_toolchain"),
        ):
            loaded = getattr(config, attribute)
            if (
                loaded.physical_bytes != sources.member_bytes(role)
                or loaded.physical_sha256 != sources.member_digest(role)
                or projection_by_role[role]["size_bytes"]
                != len(loaded.physical_bytes)
                or projection_by_role[role]["sha256"]
                != loaded.physical_sha256
            ):
                raise AssertionError("Task4b config source recipe differs")
        policy_bytes = sources.member_bytes("config:replay_policy")
        policy = json.loads(policy_bytes.decode("utf-8"))
        policy_sha256 = hashlib.sha256(policy_bytes).hexdigest()
        authority_sha256 = sources.member_digest("config:replay_authority")
        toolchain_sha256 = sources.member_digest("config:replay_toolchain")
        if (
            policy["authority_sha256"] != authority_sha256
            or policy["toolchain_sha256"] != toolchain_sha256
        ):
            raise AssertionError("Task4b policy duplicate recipe differs")
        project_inputs = fixture["project_inputs"]
        project_inputs["foundry_toml_sha256"] = sources.member_digest(
            "build:foundry_toml"
        )
        project_inputs["foundry_lock_sha256"] = sources.member_digest(
            "build:foundry_lock"
        )
        project_inputs["gitmodules_sha256"] = sources.member_digest(
            "build:gitmodules"
        )
        if (
            project_inputs["forge_std_commit"]
            != fixture["toolchain"]["forge_std"]["commit"]
        ):
            raise AssertionError("Task4b forge-std recipe differs")
        artifact = fixture["executor_artifact"]
        artifact["policy_physical_sha256"] = policy_sha256
        artifact["authority_physical_sha256"] = authority_sha256
        artifact["toolchain_physical_sha256"] = toolchain_sha256
        identity = {
            "repository_head": (
                "a6a4a374a333b9ef5387c6a6ff21b653dd4ea725"
            ),
            "python": _task4b_executing_python_identity(),
            "configs": {
                "policy_id": "policy:" + policy_sha256,
                "policy_physical_sha256": policy_sha256,
                "authority_physical_sha256": authority_sha256,
                "toolchain_physical_sha256": toolchain_sha256,
            },
            "sources": tuple(
                _detach_task4b_test_value(row)
                for row in sources.projections
            ),
            "project_inputs": project_inputs,
            "toolchain": fixture["toolchain"],
            "executor_artifact": artifact,
            "deployed_runtime_sha256": artifact[
                "deployed_runtime_sha256"
            ],
        }
        preflight = _Task4bOfflineProductionPreflightShim(
            root, sources, config, identity
        )
        _assert_task4b_offline_preflight_identity(preflight)
        return preflight
    except BaseException:
        sources.close()
        raise


def _initialize_task4b_strict_capture_runner():
    test_module = sys.modules[__name__]
    strict_shim = _Task4bOfflineProductionPreflightShim
    strict_rechecker = _recheck_task4b_offline_production_preflight

    def run(*, rpc, scan, preflight, claim, spool):
        if (
            getattr(
                test_module,
                "_recheck_task4b_offline_production_preflight",
                None,
            )
            is not strict_rechecker
            or type(preflight) is not strict_shim
            or not strict_rechecker(preflight)
        ):
            raise AssertionError("Task4b strict fixture authority drifted")
        original_rpc_rechecker = rpc._recheck_production_preflight
        try:
            with mock.patch.object(
                rpc,
                "_recheck_production_preflight",
                new=strict_rechecker,
            ):
                if rpc._recheck_production_preflight is not strict_rechecker:
                    raise AssertionError(
                        "Task4b strict rechecker installation differs"
                    )
                capability = scan._capture_production_historical_window(
                    claim=claim, spool=spool
                )
        finally:
            if rpc._recheck_production_preflight is not original_rpc_rechecker:
                raise AssertionError(
                    "Task4b strict rechecker restoration differs"
                )
        return capability

    return run


_run_task4b_strict_capture = _initialize_task4b_strict_capture_runner()
del _initialize_task4b_strict_capture_runner


class _Task4bOfflineCapabilityFixture:
    def __init__(
        self,
        *,
        context_factory=_small_context,
        split_reserve_root=True,
        record_calls=True
    ):
        self.context_factory = context_factory
        self.split_reserve_root = split_reserve_root
        self.record_calls = record_calls
        self.temporary = None
        self.data_dir = None
        self.capability = None
        self.context = None
        self.preflight = None
        self.calls = []
        self.transport_call_count = 0
        self.transport_request_count = 0
        self.reserve_attempts = []
        self.response_seed_count = None
        self.context_issuance_assertions = 0

    def mint(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage
        from tests.test_historical_foundry_rpc import _rpc_response

        headers, anchor_capture, lower_capture, plan = self.context_factory()
        anchor_number = plan["anchor_number"]
        scripted = copy.deepcopy(_synthetic_responses())
        scripted[1]["result"] = _raw_header(headers[anchor_number])
        round_id = (PHASE_ID << 64) + headers[anchor_number]["number"] + 1
        scripted[35]["result"] = _round_data(
            round_id,
            ANSWER,
            headers[anchor_number]["timestamp"],
            headers[anchor_number]["timestamp"],
            round_id,
        )
        response_by_id = {row["id"]: row for row in scripted}
        search_probes = lower_capture.get("search_probes")
        boundary_witness = lower_capture.get("boundary_witness")
        if (
            type(search_probes) is not tuple
            or type(boundary_witness) is not tuple
        ):
            raise AssertionError("Task4b lower capture rows differ")
        lower_rows = search_probes + boundary_witness
        if tuple(row.get("request_id") for row in lower_rows) != (
            lower_capture.get("request_ids")
        ):
            raise AssertionError("Task4b lower request ids differ")
        for row in lower_rows:
            if (
                type(row) is not dict
                or tuple(row) != (
                    "request_id", "block_number", "header",
                    "request_sha256", "result_sha256", "response_sha256",
                )
                or type(row["request_id"]) is not int
                or type(row["block_number"]) is not int
                or type(row["header"]) is not dict
                or type(row["header"].get("number")) is not int
                or row["header"]["number"] != row["block_number"]
            ):
                raise AssertionError("Task4b lower capture row differs")
            response_by_id[row["request_id"]] = {
                "jsonrpc": "2.0",
                "id": row["request_id"],
                "result": _raw_header(row["header"]),
            }
        self.response_seed_count = len(response_by_id)
        reserve_request_start = (
            plan["first_request_id"] + plan["block_count"]
        )
        reserve_root = tuple(range(
            reserve_request_start,
            reserve_request_start + min(40, 2 * plan["block_count"]),
        ))
        if not reserve_root:
            raise AssertionError("Task4b reserve root is empty")
        reserve_midpoint = len(reserve_root) // 2
        reserve_left = reserve_root[:reserve_midpoint]
        reserve_right = reserve_root[reserve_midpoint:]

        def response_for(request):
            request_id = request["id"]
            if request_id in response_by_id:
                return copy.deepcopy(response_by_id[request_id])
            method = request["method"]
            if method == "eth_getBlockByNumber":
                number = int(request["params"][0], 16)
                result = _raw_header(headers[number])
            elif method == "eth_call":
                number = int(
                    request["params"][1]["blockHash"], 16
                ) - 1
                target = request["params"][0]["to"].lower()
                if target in (PAIR_UNISWAP, PAIR_SUSHI):
                    result = _reserve_result(
                        number + 1, number + 2, number
                    )
                elif target == FEED_PROXY:
                    result = _price_result(
                        number, headers[number]["timestamp"]
                    )
                else:
                    raise AssertionError(
                        "unexpected Task4b state-call target"
                    )
            elif method == "eth_feeHistory":
                count = int(request["params"][0], 16)
                stop = int(request["params"][1], 16)
                start = stop - count + 1
                result = {
                    "oldestBlock": hex(start),
                    "baseFeePerGas": ["0x0"] * (count + 1),
                    "gasUsedRatio": [0.5] * count,
                    "reward": [["0x1", "0x2"] for _ in range(count)],
                }
            else:
                raise AssertionError("unexpected Task4b RPC method")
            return {
                "jsonrpc": "2.0", "id": request_id, "result": result,
            }

        class Environment(dict):
            def get(self, key, default=None):
                if key != "DEX_DEPTH_RPC_ETH" or default is not None:
                    raise AssertionError("unexpected environment read")
                return "https://rpc.example.invalid/archive"

        calls = self.calls
        fixture = self

        class Opener:
            addheaders = []

            def open(self, request, _timeout=None, **_kwargs):
                rows = json.loads(request.data.decode("utf-8"))
                request_ids = tuple(row["id"] for row in rows)
                fixture.transport_call_count += 1
                fixture.transport_request_count += len(request_ids)
                if fixture.record_calls:
                    calls.append(request_ids)
                if request_ids in (
                    reserve_root, reserve_left, reserve_right
                ):
                    fixture.reserve_attempts.append(request_ids)
                if (
                    fixture.split_reserve_root
                    and request_ids == reserve_root
                    and fixture.reserve_attempts.count(reserve_root) == 1
                ):
                    return _rpc_response((), status=413)
                return _rpc_response(tuple(
                    response_for(row) for row in reversed(rows)
                ))

        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = rpc.Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)
        self.preflight = _new_task4b_offline_production_preflight(rpc)
        original_clock = rpc.time.monotonic
        original_entropy = rpc.os.urandom
        with mock.patch.object(
            rpc,
            "_perform_production_preflight",
            return_value=self.preflight,
        ) as perform, mock.patch.object(
            rpc.time, "monotonic", return_value=10.0
        ) as clock, mock.patch.object(
            rpc.os, "urandom", return_value=b"z" * 32
        ) as entropy, mock.patch.object(
            rpc.os, "environ", Environment()
        ), mock.patch.object(
            rpc.urllib.request, "build_opener", return_value=Opener()
        ):
            self.context = rpc._open_production_archive_rpc_run()
        context_key = self.context._key
        perform.assert_called_once_with()
        clock.assert_called_once_with()
        entropy.assert_called_once_with(32)
        if clock.return_value != 10.0 or entropy.return_value != b"z" * 32:
            raise AssertionError("Task4b context nondeterminism differs")
        if (
            rpc.time.monotonic is not original_clock
            or rpc.os.urandom is not original_entropy
        ):
            raise AssertionError("Task4b nondeterminism patches leaked")
        entropy.return_value = None
        del entropy
        self.context_issuance_assertions += 1
        claim = (
            rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                context=self.context
            )
        )
        spool = storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        self.capability = _run_task4b_strict_capture(
            rpc=rpc,
            scan=scan,
            preflight=self.preflight,
            claim=claim,
            spool=spool,
        )
        if type(self.capability) is not storage._ProductionHistoricalWindowCapability:
            raise AssertionError("Task4b production capability type differs")
        if not self.preflight.closed or self.context._state != "finalized":
            raise AssertionError("Task4b production finalization differs")
        if (
            self.context._key is not None
            or type(context_key) is not bytearray
            or bytes(context_key) != b"\0" * 32
        ):
            raise AssertionError("Task4b endpoint HMAC key was retained")
        del context_key
        if self.split_reserve_root:
            if self.reserve_attempts[:3] != [
                reserve_root, reserve_left, reserve_right,
            ] or self.reserve_attempts.count(reserve_root) != 1:
                raise AssertionError("Task4b reserve split order differs")
        elif self.reserve_attempts != [reserve_root]:
            raise AssertionError("Task4b unsplit reserve order differs")
        return self.capability

    def close(self):
        if self.capability is not None:
            try:
                self.capability.close()
            except BaseException:
                pass
            self.capability = None
        if self.preflight is not None and not self.preflight.closed:
            self.preflight.close()
        if self.temporary is not None:
            self.temporary.cleanup()
            self.temporary = None


def _project_small():
    headers, capture, lower, plan = _small_context()
    header_batches = tuple(iter_historical_header_request_batches(plan))
    header_inventory = project_historical_header_inventory(
        plan=plan,
        anchor_capture=capture,
        lower_bound_capture=lower,
        batch_results=(
            (descriptor, _responses_for_descriptor(descriptor, headers.__getitem__))
            for descriptor in header_batches
        ),
    )
    state_batches = tuple(
        iter_historical_state_request_batches(
            plan=plan,
            header_inventory=header_inventory,
        )
    )
    projection = project_historical_window_projection(
        plan=plan,
        anchor_capture=capture,
        lower_bound_capture=lower,
        header_inventory=header_inventory,
        batch_results=(
            (descriptor, _responses_for_descriptor(descriptor, headers.__getitem__))
            for descriptor in state_batches
        ),
    )
    return headers, capture, lower, plan, header_inventory, state_batches, projection


class HistoricalFoundryScanSurfaceTests(unittest.TestCase):
    def test_task3b_private_surfaces_and_authorities_are_exact(self):
        import scripts.historical_foundry_scan as scan

        expected = {
            "_verify_production_historical_window_prefinalization": (
                "prefinalization", "expected_claim", "expected_spool",
            ),
            "_reconcile_production_historical_window": (
                "claim", "prefinalization", "finalization", "sealed_spool",
                "frozen_pre_ledger", "plan", "compact_projection",
            ),
            "_verify_production_historical_window_reconciliation": (
                "reconciliation", "expected_spool_identity",
                "expected_finalization_identity",
            ),
            "_capture_production_historical_window": ("claim", "spool"),
        }
        for name, parameters in expected.items():
            function = getattr(scan, name)
            signature = inspect.signature(function)
            self.assertEqual(tuple(signature.parameters), parameters)
            self.assertTrue(all(
                parameter.kind is inspect.Parameter.KEYWORD_ONLY
                for parameter in signature.parameters.values()
            ))

        for authority_class in (
            scan._ProductionHistoricalWindowPreFinalization,
            scan._ProductionHistoricalWindowReconciliation,
        ):
            with self.subTest(authority=authority_class.__name__):
                with self.assertRaises(HistoricalWindowProjectionError):
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

        self.assertFalse(any(
            name in vars(scan)
            for name in (
                "issue_production_historical_window_prefinalization",
                "prepare_production_historical_window_prefinalization",
                "build_production_historical_window_prefinalization",
            )
        ))


    def test_public_signatures_and_private_shared_seams_are_exact(self):
        expected = {
            locate_inclusive_lower_bound: (
                "anchor", "header_at_number", "lookback_seconds"
            ),
            project_historical_lower_bound_capture: (
                "anchor_capture", "lookback_seconds", "search_probes",
                "boundary_witness",
            ),
            build_historical_window_request_plan: (
                "lower_bound_capture", "anchor_capture"
            ),
            iter_historical_header_request_batches: ("plan",),
            project_historical_header_inventory: (
                "plan", "anchor_capture", "lower_bound_capture", "batch_results"
            ),
            iter_historical_state_request_batches: ("plan", "header_inventory"),
            project_historical_window_projection: (
                "plan", "anchor_capture", "lower_bound_capture",
                "header_inventory", "batch_results",
            ),
            _build_historical_block_header_request: ("block_number", "request_id"),
            _project_historical_block_header_success: ("request", "response"),
            _project_complete_historical_window_root: (
                "plan", "descriptor", "responses", "header_inventory"
            ),
        }
        for function, names in expected.items():
            with self.subTest(function=function.__name__):
                signature = inspect.signature(function)
                self.assertEqual(tuple(signature.parameters), names)
                if function is not iter_historical_header_request_batches:
                    self.assertTrue(
                        all(
                            parameter.kind is inspect.Parameter.KEYWORD_ONLY
                            for parameter in signature.parameters.values()
                        )
                    )
        import scripts.historical_foundry_scan as scan

        forbidden = (
            "open_production", "finalize", "endpoint", "writer",
            "authorizer", "capability",
        )
        exported = tuple(name.lower() for name in vars(scan) if not name.startswith("__"))
        for marker in forbidden:
            self.assertFalse(any(marker in name for name in exported), marker)

    def test_projection_error_is_closed_immutable_redacted_and_nonserializable(self):
        error = HistoricalWindowProjectionError(
            "block_coverage_incomplete", "lower_bound_invalid"
        )
        self.assertEqual(error.reason_code, "block_coverage_incomplete")
        self.assertEqual(error.failure_kind, "lower_bound_invalid")
        self.assertEqual(str(error), "historical window projection failed")
        self.assertNotIn("SECRET", repr(error))
        with self.assertRaises(AttributeError):
            error.reason_code = "SECRET"
        with self.assertRaises((TypeError, pickle.PicklingError)):
            pickle.dumps(error)
        with self.assertRaises(TypeError):
            class _Child(HistoricalWindowProjectionError):
                pass


class HistoricalFoundryScanTask3bPreFinalizationTests(unittest.TestCase):
    def _fixture(self):
        return (
            {"schema": "prefinalization-plan-fixture/v1", "root_count": 2},
            ({
                "schema": "prefinalization-pre-root-fixture/v1",
                "logical_batch_index": 1,
            },),
            {
                "schema": "prefinalization-compact-fixture/v1",
                "request_ids": (1, 2),
            },
            {
                "number": 1,
                "hash": "0x" + "11" * 32,
                "parent_hash": "0x" + "22" * 32,
                "state_root": "0x" + "33" * 32,
                "timestamp": 2,
                "gas_limit": 3,
                "gas_used": 1,
                "base_fee_per_gas": 4,
            },
        )

    def test_fixed_prefinalization_digest_vector_and_forgery_rejection(self):
        import scripts.historical_foundry_scan as scan

        plan, frozen_pre_ledger, compact_projection, final_anchor = self._fixture()
        self.assertEqual(
            (
                scan._typed_hash(
                    b"historical_foundry_prefinalization_plan/v1", plan
                ),
                scan._typed_hash(
                    b"historical_foundry_prefinalization_pre_ledger/v1",
                    frozen_pre_ledger,
                ),
                scan._typed_hash(
                    b"historical_foundry_prefinalization_compact_projection/v1",
                    compact_projection,
                ),
                scan._typed_hash(
                    b"historical_foundry_prefinalization_final_anchor/v1",
                    final_anchor,
                ),
            ),
            (
                "a0af67675e06bba3577fd9048bcdd277cb2a797405380da974196501b02bf88d",
                "92876c9cacb490e832cf15407e5839a5dc4f74ae235627c572f4a9db22b39726",
                "255b30e12e4b1beed5822a7c643b01b44618767979f8ec58835954d1f15c94a0",
                "6ba9788a64cfd76dbb0efd9fc04e92023491c88f80e636f31dffa14e5a23228e",
            ),
        )
        with self.assertRaises(HistoricalWindowProjectionError):
            scan._verify_production_historical_window_prefinalization(
                prefinalization=object(),
                expected_claim=object(),
                expected_spool=object(),
            )

    def test_scheduler_local_prefinalization_is_exact_and_one_shot(self):
        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        case.test_scheduler_owns_complete_offline_run_through_capability_delivery()

    def test_scheduler_closure_exposes_no_prefinalization_issuer(self):
        import scripts.historical_foundry_scan as scan

        exposed = tuple(
            cell.cell_contents
            for cell in scan._capture_production_historical_window.__closure__
            or ()
            if callable(cell.cell_contents)
            and "issue_prefinalization"
            in getattr(cell.cell_contents, "__name__", "")
        )
        self.assertEqual(exposed, ())


class HistoricalFoundryScanTask3bReconciliationTests(unittest.TestCase):
    def _run_with_preledger_mutation(self, mutate):
        import scripts.historical_foundry_rpc as rpc

        changed = [False]

        def trace(frame, event, _arg):
            if (
                not changed[0]
                and event == "line"
                and frame.f_code.co_name
                == "_capture_production_historical_window_core"
                and "frozen_pre_ledger" in frame.f_locals
                and "digests" not in frame.f_locals
            ):
                mutate(frame.f_locals["frozen_pre_ledger"])
                changed[0] = True
            return trace

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        previous = sys.gettrace()
        sys.settrace(trace)
        try:
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(previous)
        self.assertTrue(changed[0])
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            (
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            ),
        )

    def test_forged_preledger_axes_fail_after_finalization(self):
        def exchange(rows):
            next(row for row in rows if row["schema"].endswith(
                "pre_leaf_ledger/v1"
            ))["predicted_success_exchange_index"] = 999

        def logical(rows):
            rows[0]["logical_batch_index"] = 999

        def interval(rows):
            row = next(
                row for row in rows
                if row.get("schema")
                == "historical_foundry_window_pre_root_ledger/v1"
                and row.get("kind") == "reserve"
            )
            observed = dict(row["observed_http_413_intervals"][0])
            observed["request_count"] += 1
            row["observed_http_413_intervals"] = (observed,)

        def typed(rows):
            next(
                row for row in rows
                if row.get("schema")
                == "historical_foundry_window_pre_root_ledger/v1"
            )["typed_logical_sha256"] = "0" * 64

        for mutate in (exchange, logical, interval, typed):
            with self.subTest(axis=mutate.__name__):
                self._run_with_preledger_mutation(mutate)

    def test_reconcile_recomputes_all_prefinalization_digests(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        lines, start = inspect.getsourcelines(capture_core)
        target = start + next(
            index for index, line in enumerate(lines)
            if "finalization = (" in line
        )
        def pre_root(local_values):
            local_values["frozen_pre_ledger"][0]["stage_name"] = (
                "forged_after_prefinalization"
            )

        def pre_leaf(local_values):
            next(
                row for row in local_values["frozen_pre_ledger"]
                if row.get("schema")
                == "historical_foundry_pre_leaf_ledger/v1"
            )["segment"] = "forged_after_prefinalization"

        def plan(local_values):
            local_values["plan"]["last_request_id"] += 1

        def compact(local_values):
            local_values["compact_projection"]["coverage"][
                "header_count"
            ] += 1

        def final_anchor(local_values):
            local_values["final_anchor"]["timestamp"] += 1

        for mutate in (pre_root, pre_leaf, plan, compact, final_anchor):
            with self.subTest(digest=mutate.__name__):
                changed = [False]
                previous = sys.gettrace()

                def trace(frame, event, _argument):
                    if (
                        not changed[0]
                        and frame.f_code is capture_core.__code__
                        and event == "line"
                        and frame.f_lineno == target
                    ):
                        mutate(frame.f_locals)
                        changed[0] = True
                    return trace

                case = HistoricalFoundryScanTask3bIntegratedTests(
                    methodName=(
                        "test_scheduler_owns_complete_offline_run_through_capability_delivery"
                    )
                )
                try:
                    sys.settrace(trace)
                    with self.assertRaises(rpc._ArchiveRpcError) as caught:
                        case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                finally:
                    sys.settrace(previous)
                self.assertTrue(changed[0])
                self.assertEqual(
                    (caught.exception.reason_code, caught.exception.failure_kind),
                    (
                        "authority_mismatch",
                        "historical_window_reconciliation_mismatch",
                    ),
                )

    def test_frozen_preledger_is_detached_from_mutable_builder_rows(self):
        import scripts.historical_foundry_scan as scan

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        lines, start = inspect.getsourcelines(capture_core)
        target = start + next(
            index for index, line in enumerate(lines)
            if "finalization = (" in line
        )
        observed = [None]
        previous = sys.gettrace()

        def trace(frame, event, _argument):
            if (
                observed[0] is None
                and frame.f_code is capture_core.__code__
                and event == "line"
                and frame.f_lineno == target
            ):
                builder_row = frame.f_locals["frozen_rows"][0]
                frozen_row = frame.f_locals["frozen_pre_ledger"][0]
                original = frozen_row["stage_name"]
                builder_row["stage_name"] = "mutated_builder_alias"
                observed[0] = (
                    builder_row is not frozen_row
                    and frozen_row["stage_name"] == original
                )
            return trace

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        try:
            sys.settrace(trace)
            case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(previous)
        self.assertIs(observed[0], True)

    def test_prefinalization_retains_no_full_window_raw_root_inventory(self):
        retained = []

        def trace(frame, event, _arg):
            if (
                event == "line"
                and frame.f_code.co_name
                == "_capture_production_historical_window_core"
                and "frozen_pre_ledger" in frame.f_locals
            ):
                roots = frame.f_locals.get("all_roots")
                if roots:
                    retained.append(tuple(
                        (
                            root.get("logical_root"),
                            root.get("canonical_request_bytes"),
                        )
                        for root in roots
                    ))
            return trace

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        previous = sys.gettrace()
        sys.settrace(trace)
        try:
            case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(previous)
        self.assertEqual(retained, [])

    def test_arbitrary_prefinalization_fixture_cannot_authorize_reconciliation(self):
        import scripts.historical_foundry_scan as scan

        with self.assertRaises(HistoricalWindowProjectionError):
            scan._ProductionHistoricalWindowPreFinalization()
        exposed = tuple(
            cell.cell_contents
            for cell in scan._capture_production_historical_window.__closure__
            or ()
            if callable(cell.cell_contents)
            and "prefinalization" in getattr(cell.cell_contents, "__name__", "")
        )
        self.assertEqual(exposed, ())

    def test_forged_reconciliation_inputs_are_closed(self):
        import scripts.historical_foundry_scan as scan

        with self.assertRaises(HistoricalWindowProjectionError):
            scan._ProductionHistoricalWindowReconciliation()
        with self.assertRaises(HistoricalWindowProjectionError):
            scan._reconcile_production_historical_window(
                claim=object(),
                prefinalization=object(),
                finalization=object(),
                sealed_spool=object(),
                frozen_pre_ledger=(),
                plan={},
                compact_projection={},
            )

    def test_reconciliation_streams_bound_cursor_once(self):
        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        case.test_scheduler_owns_complete_offline_run_through_capability_delivery()

    def test_reconciliation_retires_raw_member_bytes_before_cursor_advance(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        reconcile = scan._reconcile_production_historical_window
        replay_code = next(
            constant for constant in reconcile.__code__.co_consts
            if inspect.iscode(constant) and constant.co_name == "replay_all"
        )
        cells = dict(zip(
            reconcile.__code__.co_freevars,
            reconcile.__closure__ or (),
        ))
        reparse_code = cells["bounded_reparse"].cell_contents.__code__
        cursor_next_code = (
            storage._HistoricalWindowSpoolReconciliationCursor
            .__next__.__code__
        )
        raw_names = ("request_bytes", "decoded_bytes", "frame_bytes")
        prior_raw_ids = {}
        captured_members = [0]
        advance_count = [0]
        retained = []
        prior_trace = sys.gettrace()

        def trace(frame, event, _argument):
            if event != "call":
                return trace
            caller = frame.f_back
            if caller is None or caller.f_code is not replay_code:
                return trace
            if frame.f_code is reparse_code:
                local_values = caller.f_locals
                prior_raw_ids.clear()
                prior_raw_ids.update(
                    (name, id(local_values[name])) for name in raw_names
                )
                captured_members[0] += 1
            elif frame.f_code is cursor_next_code:
                advance_count[0] += 1
                local_values = caller.f_locals
                live = tuple(
                    name for name in raw_names
                    if name in local_values
                    and id(local_values[name]) == prior_raw_ids.get(name)
                )
                if live and not retained:
                    retained.append((advance_count[0], live))
            return trace

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_"
                "capability_delivery"
            )
        )
        try:
            sys.settrace(trace)
            case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)

        self.assertGreater(captured_members[0], 0)
        self.assertGreater(advance_count[0], 1)
        self.assertEqual(retained, [])

    def test_postfinalization_divergence_uses_exact_frozen_rpc_pair(self):
        self._run_with_preledger_mutation(
            lambda rows: rows[0].__setitem__("logical_batch_index", 999)
        )


class HistoricalFoundryScanTask3bBridgeTests(unittest.TestCase):
    def test_capture_and_reconciliation_lookalikes_are_not_authority(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        with self.assertRaises(rpc._ArchiveRpcError):
            scan._capture_production_historical_window(
                claim=object(), spool=object()
            )
        with self.assertRaises(HistoricalWindowProjectionError):
            scan._verify_production_historical_window_reconciliation(
                reconciliation=object(),
                expected_spool_identity=object(),
                expected_finalization_identity=object(),
            )


class HistoricalFoundryScanTask3bIntegratedTests(unittest.TestCase):
    def test_scheduler_never_closes_an_unauthenticated_spool_argument(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        class Probe:
            def __init__(self):
                self.close_calls = 0

            def close(self):
                self.close_calls += 1

        spool = Probe()
        with self.assertRaises(rpc._ArchiveRpcError):
            scan._capture_production_historical_window(
                claim=object(), spool=spool
            )
        self.assertEqual(spool.close_calls, 0)

    def test_scheduler_scope_control_before_first_attempt_is_preserved(self):
        import scripts.historical_foundry_rpc as rpc

        cancellation = GeneratorExit("logical-scope-before-attempt")
        with mock.patch.object(
            rpc,
            "_production_archive_rpc_historical_window_logical_batch_attempt",
            side_effect=cancellation,
        ):
            with self.assertRaises(GeneratorExit) as caught:
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertIs(caught.exception, cancellation)

    def test_abort_transfer_cleanup_control_has_exact_priority(self):
        import scripts.historical_foundry_storage as storage

        cases = (
            (
                RuntimeError("pending-verifier-ordinary"),
                GeneratorExit("abort-cleanup-control"),
                "cleanup",
            ),
            (
                KeyboardInterrupt("pending-verifier-control"),
                GeneratorExit("abort-cleanup-later-control"),
                "body",
            ),
        )
        for body_error, cleanup_control, winner in cases:
            with self.subTest(winner=winner):
                with mock.patch.object(
                    storage._HistoricalWindowExchangeSpool,
                    "verify_pending_receipt",
                    side_effect=body_error,
                ), mock.patch.object(
                    storage._HistoricalWindowExchangeSpool,
                    "abort_transfer",
                    side_effect=cleanup_control,
                ):
                    with self.assertRaises(BaseException) as caught:
                        self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                self.assertIs(
                    caught.exception,
                    cleanup_control if winner == "cleanup" else body_error,
                )

    def test_claimed_finalizer_close_control_has_exact_priority(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_storage as storage

        original_close = storage._HistoricalWindowExchangeSpool.close
        cases = (
            (
                RuntimeError("claimed-finalizer-ordinary"),
                GeneratorExit("claimed-finalizer-cleanup-control"),
                "cleanup",
            ),
            (
                KeyboardInterrupt("claimed-finalizer-body-control"),
                GeneratorExit("claimed-finalizer-later-control"),
                "body",
            ),
        )
        for body_error, cleanup_control, winner in cases:
            with self.subTest(winner=winner):
                close_calls = [0]

                def close_once(spool):
                    close_calls[0] += 1
                    if close_calls[0] == 1:
                        raise cleanup_control
                    return original_close(spool)

                with mock.patch.object(
                    rpc,
                    "_finalize_production_archive_rpc_run",
                    side_effect=body_error,
                ), mock.patch.object(
                    storage._HistoricalWindowExchangeSpool,
                    "close",
                    new=close_once,
                ):
                    with self.assertRaises(BaseException) as caught:
                        self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                self.assertGreaterEqual(close_calls[0], 2)
                self.assertIs(
                    caught.exception,
                    cleanup_control if winner == "cleanup" else body_error,
                )

    def test_complete_anchor_replay_failure_remains_pure_before_finalization(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        with mock.patch.object(
            rpc,
            "project_historical_anchor_capture",
            side_effect=ValueError("forced complete anchor replay failure"),
        ):
            with self.assertRaises(HistoricalWindowProjectionError) as caught:
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "anchor_authority_invalid"),
        )

    def test_lazy_root_pure_errors_keep_owning_projector_pair(self):
        import scripts.historical_foundry_scan as scan

        original = scan._project_complete_historical_window_root
        cases = (
            (
                "header",
                ("block_coverage_incomplete", "header_invalid"),
            ),
            (
                "reserve",
                ("reserve_snapshot_incomplete", "reserve_abi_invalid"),
            ),
            (
                "price",
                ("price_snapshot_incomplete", "price_abi_invalid"),
            ),
            (
                "fee_history",
                ("fee_history_incomplete", "fee_shape_invalid"),
            ),
            (
                "final_anchor",
                ("anchor_changed", "final_anchor_mismatch"),
            ),
        )
        for kind, pair in cases:
            with self.subTest(kind=kind):
                fired = [False]
                sentinel = HistoricalWindowProjectionError(*pair)

                def project_root(**keywords):
                    if (
                        not fired[0]
                        and keywords["descriptor"].get("kind") == kind
                    ):
                        fired[0] = True
                        raise sentinel
                    return original(**keywords)

                with mock.patch.object(
                    scan,
                    "_project_complete_historical_window_root",
                    side_effect=project_root,
                ):
                    with self.assertRaises(
                        HistoricalWindowProjectionError
                    ) as caught:
                        self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                self.assertTrue(fired[0])
                self.assertEqual(
                    (
                        caught.exception.reason_code,
                        caught.exception.failure_kind,
                    ),
                    pair,
                )

    def test_scheduler_owns_complete_offline_run_through_capability_delivery(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage
        from tests.test_historical_foundry_rpc import _rpc_response

        headers, anchor_capture, lower_capture, plan = _small_context()
        anchor_number = plan["anchor_number"]
        scripted = copy.deepcopy(_synthetic_responses())
        scripted[1]["result"] = _raw_header(headers[anchor_number])
        round_id = (PHASE_ID << 64) + headers[anchor_number]["number"] + 1
        scripted[35]["result"] = _round_data(
            round_id,
            ANSWER,
            headers[anchor_number]["timestamp"],
            headers[anchor_number]["timestamp"],
            round_id,
        )
        response_by_id = {row["id"]: row for row in scripted}
        for row in (
            _observation(1, 49, headers[1]),
            _observation(0, 50, headers[0]),
            _observation(0, 51, headers[0]),
            _observation(1, 52, headers[1]),
        ):
            response_by_id[row["response"]["id"]] = row["response"]
        header_batches = tuple(iter_historical_header_request_batches(plan))
        header_inventory = project_historical_header_inventory(
            plan=plan,
            anchor_capture=anchor_capture,
            lower_bound_capture=lower_capture,
            batch_results=(
                (
                    descriptor,
                    _responses_for_descriptor(
                        descriptor, headers.__getitem__
                    ),
                )
                for descriptor in header_batches
            ),
        )
        state_batches = tuple(iter_historical_state_request_batches(
            plan=plan, header_inventory=header_inventory
        ))
        for descriptor in header_batches + state_batches:
            for row in _responses_for_descriptor(
                descriptor, headers.__getitem__
            ):
                detached = copy.deepcopy(row)
                result = detached.get("result")
                if type(result) is dict and type(result.get("gasUsedRatio")) is list:
                    result["gasUsedRatio"] = [
                        float(value) for value in result["gasUsedRatio"]
                    ]
                response_by_id[detached["id"]] = detached

        root = rpc.Path(rpc.__file__).resolve().parents[1]
        sources = rpc._HeldArchiveSourceAuthority(root)
        sources.open_members()
        config = load_historical_foundry_config_set()

        class Preflight:
            def __init__(self):
                self.identity = rpc._test_preflight_identity()
                self.config = config
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

        calls = []
        reserve_root = next(
            tuple(row["id"] for row in descriptor["requests"])
            for descriptor in state_batches
            if descriptor["kind"] == "reserve"
        )
        reserve_midpoint = len(reserve_root) // 2
        reserve_left = reserve_root[:reserve_midpoint]
        reserve_right = reserve_root[reserve_midpoint:]

        class Opener:
            addheaders = []

            def open(self, request, _timeout=None, **_kwargs):
                rows = json.loads(request.data.decode("utf-8"))
                request_ids = tuple(row["id"] for row in rows)
                calls.append(request_ids)
                if request_ids == reserve_root and calls.count(reserve_root) == 1:
                    return _rpc_response((), status=413)
                return _rpc_response(tuple(
                    copy.deepcopy(response_by_id[request_id])
                    for request_id in reversed(request_ids)
                ))

        temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        data_dir = rpc.Path(temporary.name)
        os.chmod(str(data_dir), 0o700)
        preflight = Preflight()
        try:
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
            spool = storage._open_historical_window_exchange_spool(
                data_dir=data_dir
            )
            with mock.patch.object(
                rpc, "_recheck_production_preflight", return_value=True
            ):
                capability = scan._capture_production_historical_window(
                    claim=claim, spool=spool
                )
            self.assertIs(
                type(capability),
                storage._ProductionHistoricalWindowCapability,
            )
            self.assertEqual(context._state, "finalized")
            self.assertTrue(preflight.closed)
            self.assertEqual(calls.count(reserve_root), 1)
            reserve_position = calls.index(reserve_root)
            self.assertEqual(
                calls[reserve_position:reserve_position + 3],
                [reserve_root, reserve_left, reserve_right],
            )
            self.assertEqual(
                tuple(sorted(
                    request_id
                    for request_ids in calls
                    if request_ids != reserve_root
                    for request_id in request_ids
                )),
                tuple(range(1, plan["last_request_id"] + 1)),
            )
            view = storage.consume_production_historical_window_capability(
                capability=capability
            )
            view.close()
            self.assertEqual(tuple(data_dir.iterdir()), ())
        finally:
            temporary.cleanup()

    def test_real_scheduler_splits_six_row_reserve_three_plus_three_left_first(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        active_registry = dict(zip(
            storage._HistoricalWindowExchangeSpool.close.__code__.co_freevars,
            storage._HistoricalWindowExchangeSpool.close.__closure__ or (),
        ))["active_registry"].cell_contents
        original_issue = (
            storage._HistoricalWindowExchangeSpool
            .issue_transfer_from_bound_rpc
        )
        original_project = scan._project_complete_historical_window_root
        max_live = [0]
        reserve_typed = []

        def issue(spool, *args, **kwargs):
            entry = active_registry[id(spool)]
            self.assertIs(entry[0], spool)
            self.assertIsNone(entry[1]["live_transfer"])
            transfer = original_issue(spool, *args, **kwargs)
            current = active_registry[id(spool)]
            max_live[0] = max(
                max_live[0],
                int(current[1]["live_transfer"] is not None),
            )
            return transfer

        def project(**keywords):
            result = original_project(**keywords)
            if keywords["descriptor"]["kind"] == "reserve":
                reserve_typed.append((
                    keywords["descriptor"]["request_count"],
                    result["typed_row_count"],
                ))
            return result

        with mock.patch.object(
            sys.modules[__name__],
            "_small_context",
            side_effect=_three_block_context,
        ), mock.patch.object(
            storage._HistoricalWindowExchangeSpool,
            "issue_transfer_from_bound_rpc",
            new=issue,
        ), mock.patch.object(
            scan,
            "_project_complete_historical_window_root",
            side_effect=project,
        ):
            self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertEqual(max_live[0], 1)
        self.assertTrue(reserve_typed)
        self.assertEqual(set(reserve_typed), {(6, 6)})

    def _task3b_i3_exact_authority_trace(
        self,
        *,
        rpc,
        scan,
        storage,
        rpc_tests,
        tracked_request_ids=None,
        stop_request_ids=None,
        stop_error=None,
    ):
        original_attempt = (
            rpc._production_archive_rpc_historical_window_logical_batch_attempt
        )
        original_issue = (
            storage._HistoricalWindowExchangeSpool
            .issue_transfer_from_bound_rpc
        )
        original_capture = scan._capture_production_historical_window
        attempt_core = rpc_tests._closure_named_value(
            original_attempt, "_attempt_logical_scope_core"
        )
        issue_core = rpc_tests._closure_named_value(
            original_issue, "_issue_transfer_from_bound_rpc"
        )
        logical_scope_registry = rpc_tests._closure_named_value(
            original_attempt, "logical_scope_registry"
        )
        active_registry = rpc_tests._closure_named_value(
            original_issue, "active_registry"
        )
        binding_registry = rpc_tests._closure_named_value(
            original_issue, "binding_registry"
        )
        bound_object_names = rpc_tests._closure_named_value(
            original_issue, "_bound_object_names"
        )
        resolve_bound_object = rpc_tests._closure_named_value(
            original_issue, "_resolve_bound_object"
        )
        tracked = (
            None
            if tracked_request_ids is None
            else set(tracked_request_ids)
        )
        state = {
            "attempts": [],
            "errors": [],
            "issued": [],
            "stopped": [],
            "scope_ids": [],
            "underlying_scopes": [],
            "active_underlying": [],
            "authority_checks": [],
        }

        def is_tracked(request_ids):
            return (
                tracked is None
                or (
                    request_ids
                    and set(request_ids).issubset(tracked)
                )
            )

        def observe_bound_authority(spool):
            try:
                owner_entry = active_registry[id(spool)]
                binding = owner_entry[1]["binding"]
                binding_record = binding_registry[id(binding)][1]
                rows = binding_record["bound_module_rows"]
                roles = tuple(row[0] for row in rows)
                generations_current = all(
                    row[4] is getattr(
                        row[3], "_HISTORICAL_WINDOW_MODULE_GENERATION"
                    )
                    for row in rows
                )
                objects_current = all(
                    all(
                        saved is resolve_bound_object(module, name)
                        for name, saved in zip(
                            bound_object_names[role], expected_objects
                        )
                    )
                    for (
                        role, _canonical, _actual, module, _generation,
                        _spec_name, _origin, _file_name, expected_objects,
                    ) in rows
                )
                by_role = {row[0]: row for row in rows}
                named_originals = (
                    by_role["rpc"][8][
                        bound_object_names["rpc"].index(
                            "_production_archive_rpc_historical_window_"
                            "logical_batch_attempt"
                        )
                    ] is original_attempt
                    and by_role["scan"][8][
                        bound_object_names["scan"].index(
                            "_capture_production_historical_window"
                        )
                    ] is original_capture
                    and by_role["storage"][8][
                        bound_object_names["storage"].index(
                            "_HistoricalWindowExchangeSpool."
                            "issue_transfer_from_bound_rpc"
                        )
                    ] is original_issue
                )
                no_mocks = not any(
                    isinstance(value, mock.Mock)
                    for row in rows
                    for value in row[8]
                )
                observed = (
                    roles,
                    generations_current,
                    objects_current,
                    named_originals,
                    no_mocks,
                )
            except Exception:
                observed = ((), False, False, False, False)
            state["authority_checks"].append(observed)

        def trace(frame, event, argument):
            if frame.f_code is attempt_core.__code__:
                request_ids = tuple(
                    row["id"] for row in frame.f_locals["request_rows"]
                )
                if is_tracked(request_ids):
                    if event == "call":
                        state["attempts"].append(request_ids)
                        logical_scope = frame.f_locals["logical_scope"]
                        entry = logical_scope_registry[id(logical_scope)]
                        record = entry[1]
                        state["scope_ids"].append(id(logical_scope))
                        state["underlying_scopes"].append(
                            record["underlying"]
                        )
                        state["active_underlying"].append(
                            record["context"]._active_scope
                            is record["underlying"]
                        )
                        if request_ids == stop_request_ids:
                            state["stopped"].append(request_ids)
                            raise stop_error
                    elif event == "exception":
                        error = argument[1]
                        if type(error) is rpc._ArchiveRpcError:
                            state["errors"].append((
                                request_ids,
                                (error.reason_code, error.failure_kind),
                            ))
            elif frame.f_code is issue_core.__code__ and event == "call":
                projection = frame.f_locals["exchange_projection"]
                request_ids = tuple(projection["request_ids"])
                observe_bound_authority(frame.f_locals["spool"])
                if is_tracked(request_ids):
                    state["issued"].append(request_ids)
            return trace

        return trace, state

    def _assert_task3b_i3_exact_authority(self, state):
        self.assertTrue(state["authority_checks"])
        self.assertTrue(all(
            observation == (
                ("rpc", "scan", "storage"), True, True, True, True
            )
            for observation in state["authority_checks"]
        ))

    def test_claimed_scheduler_uses_nested_left_first_depth_first_413_splits(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage
        import tests.test_historical_foundry_rpc as rpc_tests

        headers, capture, lower, plan = _three_block_context()
        header_batches = tuple(iter_historical_header_request_batches(plan))
        header_inventory = project_historical_header_inventory(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            batch_results=(
                (
                    descriptor,
                    _responses_for_descriptor(
                        descriptor, headers.__getitem__
                    ),
                )
                for descriptor in header_batches
            ),
        )
        reserve_root = next(
            tuple(row["id"] for row in descriptor["requests"])
            for descriptor in iter_historical_state_request_batches(
                plan=plan, header_inventory=header_inventory
            )
            if descriptor["kind"] == "reserve"
        )
        self.assertEqual(reserve_root, (56, 57, 58, 59, 60, 61))
        midpoint = len(reserve_root) // 2
        left = reserve_root[:midpoint]
        right = reserve_root[midpoint:]
        left_midpoint = len(left) // 2
        left_head = left[:left_midpoint]
        left_tail = left[left_midpoint:]
        tail_midpoint = len(left_tail) // 2
        tail_head = left_tail[:tail_midpoint]
        tail_tail = left_tail[tail_midpoint:]
        forced_413 = {left, left_tail}
        original_response = rpc_tests._rpc_response
        root_ids = set(reserve_root)
        bounded_stop = GeneratorExit("nested-413-before-right-half")
        trace, state = self._task3b_i3_exact_authority_trace(
            rpc=rpc,
            scan=scan,
            storage=storage,
            rpc_tests=rpc_tests,
            tracked_request_ids=root_ids,
            stop_request_ids=right,
            stop_error=bounded_stop,
        )
        prior_trace = sys.gettrace()

        def response(rows, *, status=200, encoding=None, chunks=None):
            detached = tuple(copy.deepcopy(tuple(rows)))
            request_ids = tuple(sorted(
                row["id"] for row in detached
            ))
            if status == 200 and request_ids in forced_413:
                return original_response((), status=413)
            return original_response(
                detached,
                status=status,
                encoding=encoding,
                chunks=chunks,
            )

        try:
            with mock.patch.object(
                sys.modules[__name__],
                "_small_context",
                side_effect=_three_block_context,
            ), mock.patch.object(
                rpc_tests, "_rpc_response", side_effect=response
            ):
                sys.settrace(trace)
                with self.assertRaises(GeneratorExit) as caught:
                    self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught.exception, bounded_stop)
        self._assert_task3b_i3_exact_authority(state)
        self.assertEqual(len(set(state["scope_ids"])), 1)
        self.assertTrue(state["underlying_scopes"])
        self.assertTrue(all(
            scope is state["underlying_scopes"][0]
            for scope in state["underlying_scopes"]
        ))
        self.assertEqual(state["active_underlying"], [True] * 7)
        self.assertEqual(state["attempts"], [
            reserve_root,
            left,
            left_head,
            left_tail,
            tail_head,
            tail_tail,
            right,
        ])
        self.assertEqual(state["errors"], [
            (
                reserve_root,
                ("archive_state_unavailable", "http_413"),
            ),
            (
                left,
                ("archive_state_unavailable", "http_413"),
            ),
            (
                left_tail,
                ("archive_state_unavailable", "http_413"),
            ),
        ])
        self.assertEqual(state["issued"], [
            left_head, tail_head, tail_tail,
        ])
        self.assertEqual(state["stopped"], [right])

    def test_claimed_scheduler_terminalizes_exact_disallowed_413_matrix(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage
        import tests.test_historical_foundry_rpc as rpc_tests

        headers, capture, lower, plan, header_inventory, state_batches, _ = (
            _project_small()
        )
        del headers, capture, lower
        anchor_stage = tuple(range(3, 40))
        header_root = tuple(
            row["id"]
            for row in next(iter_historical_header_request_batches(plan))[
                "requests"
            ]
        )
        fee_root = next(
            tuple(row["id"] for row in descriptor["requests"])
            for descriptor in state_batches
            if descriptor["kind"] == "fee_history"
        )
        final_root = next(
            tuple(row["id"] for row in descriptor["requests"])
            for descriptor in state_batches
            if descriptor["kind"] == "final_anchor"
        )
        self.assertIsNotNone(header_inventory)
        self.assertEqual(header_root, (53, 54))
        self.assertEqual(fee_root, (61,))
        self.assertEqual(final_root, (62,))
        cases = (
            ("anchor_disallowed", (anchor_stage,)),
            ("lower_disallowed", ((49,),)),
            ("header_singleton", (header_root, header_root[:1])),
            ("fee_history", (fee_root,)),
            ("final_anchor", (final_root,)),
        )
        original_response = rpc_tests._rpc_response

        for label, forced_sequence in cases:
            with self.subTest(label=label):
                forced = set(forced_sequence)
                trace, state = self._task3b_i3_exact_authority_trace(
                    rpc=rpc,
                    scan=scan,
                    storage=storage,
                    rpc_tests=rpc_tests,
                )
                prior_trace = sys.gettrace()

                def response(rows, *, status=200, encoding=None, chunks=None):
                    detached = tuple(copy.deepcopy(tuple(rows)))
                    request_ids = tuple(sorted(
                        row["id"] for row in detached
                    ))
                    if status == 200 and request_ids in forced:
                        return original_response((), status=413)
                    return original_response(
                        detached,
                        status=status,
                        encoding=encoding,
                        chunks=chunks,
                    )

                try:
                    with mock.patch.object(
                        rpc_tests, "_rpc_response", side_effect=response
                    ):
                        sys.settrace(trace)
                        with self.assertRaises(
                            rpc._ArchiveRpcError
                        ) as caught:
                            self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                finally:
                    sys.settrace(prior_trace)
                self._assert_task3b_i3_exact_authority(state)
                self.assertEqual(
                    (caught.exception.reason_code, caught.exception.failure_kind),
                    ("archive_state_unavailable", "http_413"),
                )
                self.assertEqual(
                    state["attempts"][-len(forced_sequence):],
                    list(forced_sequence),
                )
                self.assertEqual(
                    state["errors"][-len(forced_sequence):],
                    [
                        (
                            request_ids,
                            ("archive_state_unavailable", "http_413"),
                        )
                        for request_ids in forced_sequence
                    ],
                )
                self.assertEqual(
                    state["attempts"][-1], forced_sequence[-1]
                )
                self.assertTrue(all(
                    request_ids not in state["issued"]
                    for request_ids in forced_sequence
                ))

    def test_claimed_scheduler_shares_cumulative_budget_across_413_leaves(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage
        import tests.test_historical_foundry_rpc as rpc_tests

        headers, capture, lower, plan = _three_block_context()
        header_batches = tuple(iter_historical_header_request_batches(plan))
        header_inventory = project_historical_header_inventory(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            batch_results=(
                (
                    descriptor,
                    _responses_for_descriptor(
                        descriptor, headers.__getitem__
                    ),
                )
                for descriptor in header_batches
            ),
        )
        reserve_root = next(
            tuple(row["id"] for row in descriptor["requests"])
            for descriptor in iter_historical_state_request_batches(
                plan=plan, header_inventory=header_inventory
            )
            if descriptor["kind"] == "reserve"
        )
        self.assertEqual(reserve_root, (56, 57, 58, 59, 60, 61))
        midpoint = len(reserve_root) // 2
        left = reserve_root[:midpoint]
        right = reserve_root[midpoint:]
        original_response = rpc_tests._rpc_response
        root_ids = set(reserve_root)
        padding = b" " * 4_250_000
        trace, state = self._task3b_i3_exact_authority_trace(
            rpc=rpc,
            scan=scan,
            storage=storage,
            rpc_tests=rpc_tests,
            tracked_request_ids=root_ids,
        )
        prior_trace = sys.gettrace()

        def response(rows, *, status=200, encoding=None, chunks=None):
            detached = tuple(copy.deepcopy(tuple(rows)))
            request_ids = tuple(sorted(
                row["id"] for row in detached
            ))
            if (
                status == 200
                and request_ids
                and set(request_ids).issubset(root_ids)
            ):
                body = padding + rpc_tests._canonical_bytes(detached)
                return original_response(detached, chunks=(body,))
            return original_response(
                detached,
                status=status,
                encoding=encoding,
                chunks=chunks,
            )

        try:
            with mock.patch.object(
                sys.modules[__name__],
                "_small_context",
                side_effect=_three_block_context,
            ), mock.patch.object(
                rpc_tests, "_rpc_response", side_effect=response
            ):
                sys.settrace(trace)
                with self.assertRaises(rpc._ArchiveRpcError) as caught:
                    self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self._assert_task3b_i3_exact_authority(state)
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("archive_state_unavailable", "response_resource_limit"),
        )
        self.assertEqual(state["attempts"], [reserve_root, left, right])
        self.assertEqual(state["errors"], [
            (
                reserve_root,
                ("archive_state_unavailable", "http_413"),
            ),
            (
                right,
                ("archive_state_unavailable", "response_resource_limit"),
            ),
        ])
        self.assertEqual(state["issued"], [left])

    def test_poisoned_right_half_never_types_or_escapes_left_half(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage
        import tests.test_historical_foundry_rpc as rpc_tests

        original_response = rpc_tests._rpc_response
        original_open = rpc._open_production_archive_rpc_run
        original_spool_open = storage._open_historical_window_exchange_spool
        original_project = scan._project_complete_historical_window_root
        observed = {}
        reserve_typed = []
        poisoned_ids = (59, 60, 61)

        def response(rows, *, status=200, encoding=None, chunks=None):
            rows = copy.deepcopy(rows)
            if (
                status == 200
                and tuple(sorted(row["id"] for row in rows)) == poisoned_ids
            ):
                rows[-1]["id"] = rows[0]["id"]
            return original_response(
                rows, status=status, encoding=encoding, chunks=chunks
            )

        def open_context(*args, **kwargs):
            context = original_open(*args, **kwargs)
            observed["context"] = context
            return context

        def open_spool(*args, **kwargs):
            spool = original_spool_open(*args, **kwargs)
            observed["spool"] = spool
            return spool

        def project(**keywords):
            if keywords["descriptor"]["kind"] == "reserve":
                reserve_typed.append(keywords["descriptor"])
            return original_project(**keywords)

        active_registry = dict(zip(
            storage._HistoricalWindowExchangeSpool.close.__code__.co_freevars,
            storage._HistoricalWindowExchangeSpool.close.__closure__ or (),
        ))["active_registry"].cell_contents
        with mock.patch.object(
            sys.modules[__name__],
            "_small_context",
            side_effect=_three_block_context,
        ), mock.patch.object(
            rpc_tests, "_rpc_response", side_effect=response
        ), mock.patch.object(
            rpc,
            "_open_production_archive_rpc_run",
            side_effect=open_context,
        ), mock.patch.object(
            storage,
            "_open_historical_window_exchange_spool",
            side_effect=open_spool,
        ), mock.patch.object(
            scan,
            "_project_complete_historical_window_root",
            side_effect=project,
        ):
            with self.assertRaises(rpc._ArchiveRpcError):
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertEqual(reserve_typed, [])
        self.assertEqual(observed["context"]._records, [])
        self.assertEqual(observed["context"]._state, "failed")
        self.assertNotIn(id(observed["spool"]), active_registry)

    def test_scheduler_preserves_each_lazy_root_rpc_error_and_stops(self):
        import scripts.historical_foundry_rpc as rpc

        original = (
            rpc._production_archive_rpc_historical_window_logical_batch_attempt
        )
        target_first_ids = {
            "header": 53,
            "reserve": 55,
            "price": 59,
            "fee_history": 61,
            "final_anchor": 62,
        }
        for kind, target_first_id in target_first_ids.items():
            with self.subTest(kind=kind):
                exact = rpc._ArchiveRpcError(
                    "archive_state_unavailable", "transport_unavailable"
                )
                attempts = []

                def attempt(*, logical_scope, request_rows):
                    request_ids = tuple(row["id"] for row in request_rows)
                    attempts.append(request_ids)
                    if request_ids[0] == target_first_id:
                        raise exact
                    return original(
                        logical_scope=logical_scope,
                        request_rows=request_rows,
                    )

                with mock.patch.object(
                    rpc,
                    "_production_archive_rpc_historical_window_logical_batch_attempt",
                    side_effect=attempt,
                ):
                    with self.assertRaises(rpc._ArchiveRpcError) as caught:
                        self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                self.assertIs(caught.exception, exact)
                self.assertEqual(attempts[-1][0], target_first_id)

    def test_offline_owner_lifecycle_reaches_consumed_view_cleanup(self):
        self.test_scheduler_owns_complete_offline_run_through_capability_delivery()

    def test_scheduler_tracks_moved_owner_before_next_trace_boundary(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        case_method = (
            HistoricalFoundryScanTask3bIntegratedTests
            .test_scheduler_owns_complete_offline_run_through_capability_delivery
        )
        original_cleanup = tempfile.TemporaryDirectory.cleanup
        for stage in ("sealed", "capability"):
            with self.subTest(stage=stage):
                cancellation = GeneratorExit(
                    "scheduler-owner-assignment-{}".format(stage)
                )
                captured = {
                    "owner": None,
                    "temporary": None,
                    "data_dir": None,
                }
                prior_trace = sys.gettrace()

                def tracer(frame, event, _argument):
                    if (
                        frame.f_code.co_filename == scan.__file__
                        and frame.f_code.co_name
                        == "_capture_production_historical_window_core"
                        and event == "line"
                    ):
                        locals_ = frame.f_locals
                        ready = (
                            stage == "sealed"
                            and "sealed_spool" in locals_
                            and "reconciliation" not in locals_
                        ) or (
                            stage == "capability"
                            and "capability" in locals_
                            and not locals_.get("delivered", False)
                        )
                        if ready:
                            captured["owner"] = locals_[
                                "sealed_spool"
                                if stage == "sealed"
                                else "capability"
                            ]
                            caller = frame.f_back
                            while caller is not None:
                                if caller.f_code is case_method.__code__:
                                    captured["temporary"] = caller.f_locals[
                                        "temporary"
                                    ]
                                    captured["data_dir"] = caller.f_locals[
                                        "data_dir"
                                    ]
                                    break
                                caller = caller.f_back
                            sys.settrace(prior_trace)
                            raise cancellation
                    return tracer

                case = HistoricalFoundryScanTask3bIntegratedTests(
                    methodName=case_method.__name__
                )
                observed_files = None
                try:
                    with mock.patch.object(
                        tempfile.TemporaryDirectory,
                        "cleanup",
                        autospec=True,
                        return_value=None,
                    ):
                        try:
                            sys.settrace(tracer)
                            with self.assertRaises(GeneratorExit) as caught:
                                case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                        finally:
                            sys.settrace(prior_trace)
                    self.assertIs(caught.exception, cancellation)
                    self.assertIsNotNone(captured["owner"])
                    observed_files = tuple(captured["data_dir"].iterdir())
                finally:
                    owner = captured["owner"]
                    if owner is not None:
                        try:
                            owner.close()
                        except storage.HistoricalFoundryStorageError:
                            pass
                    temporary = captured["temporary"]
                    if temporary is not None:
                        original_cleanup(temporary)
                self.assertEqual(observed_files, ())

    def test_scheduler_has_no_unguarded_line_after_delivery_publication(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        case_method = (
            HistoricalFoundryScanTask3bIntegratedTests
            .test_scheduler_owns_complete_offline_run_through_capability_delivery
        )
        cancellation = GeneratorExit("scheduler-post-delivery-line-control")
        captured = {
            "owner": None,
            "temporary": None,
            "data_dir": None,
            "fired": False,
        }
        prior_trace = sys.gettrace()
        original_cleanup = tempfile.TemporaryDirectory.cleanup

        def tracer(frame, event, _argument):
            if (
                frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name
                == "_capture_production_historical_window_core"
                and event == "line"
            ):
                caller = frame.f_back
                while caller is not None:
                    if caller.f_code is case_method.__code__:
                        captured["temporary"] = caller.f_locals.get("temporary")
                        captured["data_dir"] = caller.f_locals.get("data_dir")
                        break
                    caller = caller.f_back
                if frame.f_locals.get("delivered") is True:
                    captured["fired"] = True
                    captured["owner"] = frame.f_locals["capability"]
                    sys.settrace(prior_trace)
                    raise cancellation
            return tracer

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=case_method.__name__
        )
        caught = None
        observed_files = None
        try:
            with mock.patch.object(
                tempfile.TemporaryDirectory,
                "cleanup",
                autospec=True,
                return_value=None,
            ):
                try:
                    sys.settrace(tracer)
                    case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                except GeneratorExit as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
            if captured["fired"]:
                self.assertIs(caught, cancellation)
            else:
                self.assertIsNone(caught)
            observed_files = tuple(captured["data_dir"].iterdir())
        finally:
            owner = captured["owner"]
            if owner is not None:
                try:
                    owner.close()
                except storage.HistoricalFoundryStorageError:
                    pass
            temporary = captured["temporary"]
            if temporary is not None:
                original_cleanup(temporary)
        self.assertEqual(observed_files, ())

    def test_scheduler_cleanup_control_overrides_ordinary_body_failure(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        capture_lines, capture_start = inspect.getsourcelines(capture_core)
        capture_target = capture_start + next(
            index for index, line in enumerate(capture_lines)
            if "digests = prefinalization_digests(" in line
        )
        claim_close = rpc._ProductionHistoricalWindowRunClaim.close
        close_lines, close_start = inspect.getsourcelines(claim_close)
        close_target = close_start + next(
            index for index, line in enumerate(close_lines)
            if "if control is not None:" in line
        )
        cleanup_control = asyncio.CancelledError(
            "scheduler-cleanup-control-over-ordinary"
        )
        prior_trace = sys.gettrace()
        patcher = [None]
        fired = {"body": False, "cleanup": False}

        def cleanup_tracer(frame, event, _argument):
            if (
                frame.f_code is claim_close.__code__
                and event == "line"
                and frame.f_lineno == close_target
            ):
                fired["cleanup"] = True
                sys.settrace(prior_trace)
                raise cleanup_control
            return cleanup_tracer

        def fail_typed_hash(*_args, **_kwargs):
            fired["body"] = True
            raise ValueError("scheduler-body-ordinary")

        def install(frame, event, _argument):
            if (
                patcher[0] is None
                and
                frame.f_code is capture_core.__code__
                and event == "line"
                and frame.f_lineno == capture_target
            ):
                patcher[0] = mock.patch.object(
                    scan, "_typed_hash", side_effect=fail_typed_hash
                )
                patcher[0].start()
                sys.settrace(cleanup_tracer)
            return install

        try:
            sys.settrace(install)
            caught = None
            try:
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
            except BaseException as error:
                caught = error
        finally:
            sys.settrace(prior_trace)
            if patcher[0] is not None:
                patcher[0].stop()
        self.assertTrue(fired["body"])
        self.assertTrue(fired["cleanup"])
        self.assertIs(caught, cleanup_control)

    def test_reconciliation_replays_each_state_root_typed_ledger(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        lines, start = inspect.getsourcelines(capture_core)
        target = start + next(
            index for index, line in enumerate(lines)
            if "reconciliation = reconcile(" in line
        )
        for kind in ("reserve", "price", "fee_history", "final_anchor"):
            with self.subTest(kind=kind):
                changed = [False]
                prior_trace = sys.gettrace()

                def tracer(frame, event, _argument):
                    if (
                        not changed[0]
                        and frame.f_code is capture_core.__code__
                        and event == "line"
                        and frame.f_lineno == target
                    ):
                        for row in frame.f_locals["frozen_pre_ledger"]:
                            if (
                                row.get("schema")
                                == "historical_foundry_window_pre_root_ledger/v1"
                                and row.get("kind") == kind
                            ):
                                row["typed_logical_sha256"] = "0" * 64
                                changed[0] = True
                                break
                    return tracer

                try:
                    sys.settrace(tracer)
                    with self.assertRaises(rpc._ArchiveRpcError) as caught:
                        self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                finally:
                    sys.settrace(prior_trace)
                self.assertTrue(changed[0])
                self.assertEqual(
                    (caught.exception.reason_code, caught.exception.failure_kind),
                    (
                        "authority_mismatch",
                        "historical_window_reconciliation_mismatch",
                    ),
                )

    def test_reconciliation_invokes_both_global_projectors_once_again(self):
        import scripts.historical_foundry_scan as scan

        original_headers = scan.project_historical_header_inventory
        original_window = scan.project_historical_window_projection
        with mock.patch.object(
            scan,
            "project_historical_header_inventory",
            wraps=original_headers,
        ) as header_projector, mock.patch.object(
            scan,
            "project_historical_window_projection",
            wraps=original_window,
        ) as window_projector:
            self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertEqual(header_projector.call_count, 2)
        self.assertEqual(window_projector.call_count, 2)

    def test_mint_rejects_reconciliation_with_detached_post_ledger(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        verifier = scan._verify_production_historical_window_reconciliation
        registry = dict(
            zip(
                verifier.__code__.co_freevars,
                (
                    cell.cell_contents
                    for cell in verifier.__closure__ or ()
                ),
            )
        )["reconciliation_registry"]
        lines, start = inspect.getsourcelines(capture_core)
        target = start + next(
            index for index, line in enumerate(lines)
            if "current_owner = capability =" in line
        )
        changed = [False]
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                not changed[0]
                and frame.f_code is capture_core.__code__
                and event == "line"
                and frame.f_lineno == target
            ):
                reconciliation = frame.f_locals["reconciliation"]
                record = registry[id(reconciliation)][1]
                record["post_root_ledger"] = ()
                changed[0] = True
            return tracer

        try:
            sys.settrace(tracer)
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertTrue(changed[0])
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            (
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            ),
        )

    def test_completed_runs_retire_all_scan_and_claim_registry_records(self):
        import gc
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        def closure_values(function):
            return {
                name: cell.cell_contents
                for name, cell in zip(
                    function.__code__.co_freevars,
                    function.__closure__ or (),
                )
            }

        prefinalizations = closure_values(
            scan._verify_production_historical_window_prefinalization
        )["prefinalization_registry"]
        reconciliations = closure_values(
            scan._verify_production_historical_window_reconciliation
        )["reconciliation_registry"]
        claim_authorities = closure_values(
            rpc._claim_fresh_production_archive_rpc_run_for_historical_window
        )
        claims = closure_values(
            claim_authorities["register_claim"]
        )["claim_registry"]
        before = (
            len(prefinalizations),
            len(reconciliations),
            len(claims),
        )
        for _unused in range(2):
            self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
            gc.collect()
        after = (
            len(prefinalizations),
            len(reconciliations),
            len(claims),
        )
        self.assertTrue(all(
            after_count <= before_count
            for after_count, before_count in zip(after, before)
        ))

    def test_completed_runs_retire_all_storage_registries_and_handles(self):
        import gc
        import weakref
        import scripts.historical_foundry_storage as storage

        roots = (
            storage._open_historical_window_exchange_spool,
            storage._HistoricalWindowExchangeSpool.close,
            storage._HistoricalWindowExchangeSpool.seal,
            storage._SealedHistoricalWindowExchangeSpool.close,
            storage._SealedHistoricalWindowExchangeSpool
            ._open_reconciliation_cursor_from_bound_scan,
            storage._SealedHistoricalWindowExchangeSpool
            .mint_production_historical_window_capability,
            storage._HistoricalWindowSpoolReconciliationCursor.__next__,
            storage._ProductionHistoricalWindowCapability.close,
            storage.consume_production_historical_window_capability,
            storage._ConsumedProductionHistoricalWindowCapabilityView.close,
        )
        pending = list(roots)
        visited = set()
        registries = {}
        while pending:
            function = pending.pop()
            if (
                not callable(function)
                or not hasattr(function, "__code__")
                or id(function) in visited
            ):
                continue
            visited.add(id(function))
            for name, cell in zip(
                function.__code__.co_freevars,
                function.__closure__ or (),
            ):
                value = cell.cell_contents
                if name.endswith("_registry") and type(value) is dict:
                    registries[name] = value
                if callable(value) and hasattr(value, "__code__"):
                    pending.append(value)
        expected_names = {
            "active_registry",
            "sealed_registry",
            "cursor_registry",
            "capability_registry",
            "consumed_view_registry",
            "staging_snapshot_registry",
            "replay_source_registry",
            "binding_registry",
            "task4b_checker_registry",
            "transfer_registry",
            "pending_registry",
            "receipt_registry",
            "quota_registry",
        }
        self.assertEqual(set(registries), expected_names)
        baseline = {
            name: len(registry)
            for name, registry in registries.items()
        }

        references = []
        original_open = storage._open_historical_window_exchange_spool
        original_seal = storage._HistoricalWindowExchangeSpool.seal
        original_cursor = (
            storage._SealedHistoricalWindowExchangeSpool
            ._open_reconciliation_cursor_from_bound_scan
        )
        original_mint = (
            storage._SealedHistoricalWindowExchangeSpool
            .mint_production_historical_window_capability
        )
        original_consume = (
            storage.consume_production_historical_window_capability
        )

        def capture_open(*args, **kwargs):
            handle = original_open(*args, **kwargs)
            references.append(weakref.ref(handle))
            return handle

        def capture_seal(handle, *args, **kwargs):
            moved = original_seal(handle, *args, **kwargs)
            references.append(weakref.ref(moved))
            return moved

        def capture_cursor(handle, *args, **kwargs):
            cursor = original_cursor(handle, *args, **kwargs)
            references.append(weakref.ref(cursor))
            return cursor

        def capture_mint(handle, *args, **kwargs):
            moved = original_mint(handle, *args, **kwargs)
            references.append(weakref.ref(moved))
            return moved

        def capture_consume(*args, **kwargs):
            moved = original_consume(*args, **kwargs)
            references.append(weakref.ref(moved))
            return moved

        with mock.patch.object(
            storage,
            "_open_historical_window_exchange_spool",
            new=capture_open,
        ), mock.patch.object(
            storage._HistoricalWindowExchangeSpool,
            "seal",
            new=capture_seal,
        ), mock.patch.object(
            storage._SealedHistoricalWindowExchangeSpool,
            "_open_reconciliation_cursor_from_bound_scan",
            new=capture_cursor,
        ), mock.patch.object(
            storage._SealedHistoricalWindowExchangeSpool,
            "mint_production_historical_window_capability",
            new=capture_mint,
        ), mock.patch.object(
            storage,
            "consume_production_historical_window_capability",
            new=capture_consume,
        ):
            for _unused in range(2):
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        gc.collect()

        self.assertEqual(len(references), 10)
        self.assertTrue(all(reference() is None for reference in references))
        self.assertEqual(
            {
                name: len(registry)
                for name, registry in registries.items()
            },
            baseline,
        )

    def test_scheduler_and_reconciliation_validate_header_inventory_constant_times(self):
        import scripts.historical_foundry_scan as scan

        original = scan._validate_header_inventory
        with mock.patch.object(
            scan, "_validate_header_inventory", wraps=original
        ) as validator:
            self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertLessEqual(validator.call_count, 10)

    def test_mint_rechecks_post_ledgers_and_global_projection_digests(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        verifier_cells = {
            name: cell.cell_contents
            for name, cell in zip(
                scan._verify_production_historical_window_reconciliation.__code__.co_freevars,
                scan._verify_production_historical_window_reconciliation.__closure__
                or (),
            )
        }
        registry = verifier_cells["reconciliation_registry"]
        lines, start = inspect.getsourcelines(capture_core)
        target = start + next(
            index for index, line in enumerate(lines)
            if "current_owner = capability =" in line
        )

        def mutate_root(record):
            record["post_root_ledger"][0]["attempt_count"] += 1

        def mutate_leaf(record):
            record["post_leaf_ledger"][0]["wire_byte_count"] += 1

        def mutate_global(record):
            record["compact_projection"]["coverage"]["header_count"] += 1

        for label, mutate in (
            ("post_root", mutate_root),
            ("post_leaf", mutate_leaf),
            ("global", mutate_global),
        ):
            with self.subTest(label=label):
                changed = [False]
                prior_trace = sys.gettrace()

                def tracer(frame, event, _argument):
                    if (
                        not changed[0]
                        and frame.f_code is capture_core.__code__
                        and event == "line"
                        and frame.f_lineno == target
                    ):
                        reconciliation = frame.f_locals["reconciliation"]
                        mutate(registry[id(reconciliation)][1])
                        changed[0] = True
                    return tracer

                try:
                    sys.settrace(tracer)
                    with self.assertRaises(rpc._ArchiveRpcError) as caught:
                        self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
                finally:
                    sys.settrace(prior_trace)
                self.assertTrue(changed[0])
                self.assertEqual(
                    (
                        caught.exception.reason_code,
                        caught.exception.failure_kind,
                    ),
                    (
                        "authority_mismatch",
                        "historical_window_reconciliation_mismatch",
                    ),
                )

    def test_reconciliation_verifier_is_consumed_by_mint_once(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        captured = {}
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                frame.f_code is capture_core.__code__
                and event == "line"
                and "reconciliation" in frame.f_locals
                and "capability" in frame.f_locals
                and not captured
            ):
                captured.update({
                    "reconciliation": frame.f_locals["reconciliation"],
                    "sealed": frame.f_locals["sealed_spool"],
                    "finalization": frame.f_locals["finalization"],
                })
            return tracer

        try:
            sys.settrace(tracer)
            self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        with self.assertRaises(rpc._ArchiveRpcError) as caught:
            scan._verify_production_historical_window_reconciliation(
                reconciliation=captured["reconciliation"],
                expected_spool_identity=captured["sealed"],
                expected_finalization_identity=captured["finalization"],
            )
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            (
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            ),
        )

    def test_first_failed_cursor_open_terminalizes_and_consumes_slot(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_storage as storage

        original = (
            storage._SealedHistoricalWindowExchangeSpool
            ._open_reconciliation_cursor_from_bound_scan
        )
        closure_values = {
            name: cell.cell_contents
            for name, cell in zip(
                original.__code__.co_freevars,
                original.__closure__ or (),
            )
        }
        core = closure_values["_open_reconciliation_cursor_core"]
        core_values = {
            name: cell.cell_contents
            for name, cell in zip(
                core.__code__.co_freevars,
                core.__closure__ or (),
            )
        }
        sealed_registry = core_values["sealed_registry"]
        cursor_registry = core_values["cursor_registry"]
        cursor_baseline = set(cursor_registry)
        observed = []

        def fail_then_retry(sealed, *, claim, finalization):
            try:
                original(
                    sealed,
                    claim=claim,
                    finalization=object(),
                )
            except BaseException as error:
                observed.append(error)
            else:
                self.fail("invalid first cursor open unexpectedly succeeded")
            entry = sealed_registry.get(id(sealed))
            observed.append(
                entry is None
                or entry[0] is not sealed
                or entry[1].get("state") != "sealed"
            )
            try:
                return original(
                    sealed,
                    claim=claim,
                    finalization=finalization,
                )
            except BaseException as error:
                observed.append(error)
                raise

        with mock.patch.object(
            storage._SealedHistoricalWindowExchangeSpool,
            "_open_reconciliation_cursor_from_bound_scan",
            new=fail_then_retry,
        ):
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertIs(type(observed[0]), storage.HistoricalFoundryStorageError)
        self.assertIs(observed[1], True)
        self.assertIs(type(observed[2]), storage.HistoricalFoundryStorageError)
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            (
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            ),
        )
        self.assertEqual(set(cursor_registry), cursor_baseline)

    def test_mint_rechecks_live_claimed_finalization_before_owner_move(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        capture_core = next(
            cell.cell_contents
            for cell in (
                scan._capture_production_historical_window.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_capture_production_historical_window_core"
        )
        mint = (
            storage._SealedHistoricalWindowExchangeSpool
            .mint_production_historical_window_capability
        )
        mint_core = dict(zip(
            mint.__code__.co_freevars, mint.__closure__ or (),
        ))[
            "_mint_production_historical_window_capability_core"
        ].cell_contents
        mint_state = {
            name: cell.cell_contents
            for name, cell in zip(
                mint_core.__code__.co_freevars,
                mint_core.__closure__ or (),
            )
        }
        sealed_registry = mint_state["sealed_registry"]
        capability_registry = mint_state["capability_registry"]
        capability_baseline = set(capability_registry)
        lines, start = inspect.getsourcelines(capture_core)
        target = start + next(
            index for index, line in enumerate(lines)
            if "current_owner = capability = sealed_spool.mint" in line
        )
        observed = {}
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                not observed
                and frame.f_code is capture_core.__code__
                and event == "line"
                and frame.f_lineno == target
            ):
                observed["sealed"] = frame.f_locals["sealed_spool"]
                sys.settrace(prior_trace)
                frame.f_locals["claim"].close()
            return tracer

        try:
            sys.settrace(tracer)
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "final_identity_drift"),
        )
        self.assertNotIn(id(observed["sealed"]), sealed_registry)
        self.assertEqual(set(capability_registry), capability_baseline)
        self.assertIsNone(observed["sealed"].close())

    def test_first_failed_mint_attempt_terminalizes_and_consumes_slot(self):
        import scripts.historical_foundry_storage as storage

        original = (
            storage._SealedHistoricalWindowExchangeSpool
            .mint_production_historical_window_capability
        )
        closure_values = {
            name: cell.cell_contents
            for name, cell in zip(
                original.__code__.co_freevars,
                original.__closure__ or (),
            )
        }
        core = closure_values[
            "_mint_production_historical_window_capability_core"
        ]
        core_values = {
            name: cell.cell_contents
            for name, cell in zip(
                core.__code__.co_freevars,
                core.__closure__ or (),
            )
        }
        sealed_registry = core_values["sealed_registry"]
        observed = []

        def fail_then_retry(
            sealed, *, claim, finalization, reconciliation
        ):
            try:
                original(
                    sealed,
                    claim=claim,
                    finalization=finalization,
                    reconciliation=object(),
                )
            except BaseException as error:
                observed.append(error)
            else:
                self.fail("invalid first mint unexpectedly succeeded")
            entry = sealed_registry.get(id(sealed))
            observed.append(
                entry is None
                or entry[0] is not sealed
                or entry[1].get("state") != "sealed"
            )
            try:
                return original(
                    sealed,
                    claim=claim,
                    finalization=finalization,
                    reconciliation=reconciliation,
                )
            except BaseException as error:
                observed.append(error)
                raise

        with mock.patch.object(
            storage._SealedHistoricalWindowExchangeSpool,
            "mint_production_historical_window_capability",
            new=fail_then_retry,
        ):
            with self.assertRaises(
                storage.HistoricalFoundryStorageError
            ) as caught:
                self.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        self.assertIs(type(observed[0]), storage.HistoricalFoundryStorageError)
        self.assertIs(observed[1], True)
        self.assertIs(observed[2], caught.exception)


class HistoricalFoundryLowerAndPlanTests(unittest.TestCase):
    def test_mathematical_lower_bound_handles_equality_genesis_and_duplicates(self):
        timestamps = (1, 10, 10, 15, 20)

        def header_at(number):
            return _normalized_header(number, timestamps[number])

        self.assertEqual(
            locate_inclusive_lower_bound(
                anchor=header_at(4),
                header_at_number=header_at,
                lookback_seconds=10,
            ),
            1,
        )
        self.assertEqual(
            locate_inclusive_lower_bound(
                anchor=header_at(0),
                header_at_number=header_at,
                lookback_seconds=1,
            ),
            0,
        )

    def test_lower_capture_binds_exact_order_ids_and_fresh_boundary(self):
        _headers, capture, lower, plan = _small_context()
        self.assertEqual(lower["schema"], "historical_foundry_lower_bound_capture/v1")
        self.assertEqual(lower["cutoff_timestamp"], 25)
        self.assertEqual(lower["lower_bound_number"], 1)
        self.assertEqual(
            tuple(row["block_number"] for row in lower["search_probes"]),
            (1, 0),
        )
        self.assertEqual(
            tuple(row["block_number"] for row in lower["boundary_witness"]),
            (0, 1),
        )
        self.assertEqual(lower["request_ids"], (49, 50, 51, 52))
        self.assertEqual(lower["next_request_id"], 53)
        self.assertEqual(plan["block_count"], 2)
        self.assertEqual(plan["first_request_id"], 53)
        self.assertEqual(plan["last_request_id"], 62)
        self.assertEqual(plan["request_count"], 10)
        self.assertEqual(plan["fee_chunk_count"], 1)
        self.assertEqual(
            plan["pair_addresses"],
            {"uniswap_v2": PAIR_UNISWAP, "sushiswap_v2": PAIR_SUSHI},
        )
        self.assertEqual(plan["price_feed_proxy"], FEED_PROXY)
        self.assertNotIn("request_ids", plan)
        self.assertNotIn("requests", plan)
        self.assertEqual(capture["anchor"]["number"], "0x2")

    def test_lower_transcript_rejects_missing_extra_reordered_and_transplanted_rows(self):
        headers, capture, _lower, _plan = _small_context()
        valid = []
        request_id = 49
        for number in (1, 0):
            valid.append(_observation(number, request_id, headers[number]))
            request_id += 1
        witness = [
            _observation(0, 51, headers[0]),
            _observation(1, 52, headers[1]),
        ]
        attacks = (
            (valid[:1], witness),
            (valid + [_observation(2, 51, headers[2])], witness),
            (list(reversed(valid)), witness),
            (valid, list(reversed(witness))),
        )
        for probes, boundary in attacks:
            with self.subTest(probes=len(probes), boundary=len(boundary)):
                with self.assertRaises(HistoricalWindowProjectionError):
                    project_historical_lower_bound_capture(
                        anchor_capture=capture,
                        lookback_seconds=LOOKBACK,
                        search_probes=iter(probes),
                        boundary_witness=iter(boundary),
                    )
        transplanted = copy.deepcopy(valid)
        transplanted[0]["response"]["id"] = 50
        with self.assertRaises(HistoricalWindowProjectionError):
            project_historical_lower_bound_capture(
                anchor_capture=capture,
                lookback_seconds=LOOKBACK,
                search_probes=iter(transplanted),
                boundary_witness=iter(witness),
            )

    def test_plan_rejects_maximum_plus_one_early(self):
        def make(block_count):
            anchor_number = block_count - 1
            anchor = _normalized_header(anchor_number, LOOKBACK + 1)
            capture = _capture_for_header(anchor)

            def header_at(number):
                return _normalized_header(number, number * 12 + 1)

            lower = _lower_capture(capture, header_at)
            return build_historical_window_request_plan(
                lower_bound_capture=lower,
                anchor_capture=capture,
            )

        with self.assertRaises(HistoricalWindowProjectionError) as caught:
            make(50_402)
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("block_coverage_incomplete", "window_resource_limit"),
        )

    def test_staged_factories_are_fresh_deterministic_one_shot_and_exact(self):
        headers, _capture, _lower, plan = _small_context()
        first = iter_historical_header_request_batches(plan)
        descriptor = next(first)
        with self.assertRaises(StopIteration):
            next(first)
        with self.assertRaises(HistoricalWindowProjectionError):
            tuple(first)
        second = tuple(iter_historical_header_request_batches(plan))
        self.assertEqual(second, (descriptor,))
        self.assertEqual(
            descriptor,
            {
                "schema": "historical_foundry_window_batch/v1",
                "kind": "header",
                "root_index": 0,
                "block_start": 1,
                "block_stop": 2,
                "request_id_start": 53,
                "request_id_stop": 54,
                "request_count": 2,
                "requests": (
                    {"jsonrpc": "2.0", "id": 53,
                     "method": "eth_getBlockByNumber", "params": ["0x1", False]},
                    {"jsonrpc": "2.0", "id": 54,
                     "method": "eth_getBlockByNumber", "params": ["0x2", False]},
                ),
                "allow_http_413_bisection": True,
            },
        )
        header_inventory = project_historical_header_inventory(
            plan=plan,
            anchor_capture=_small_context()[1],
            lower_bound_capture=_small_context()[2],
            batch_results=((descriptor, _responses_for_descriptor(
                descriptor, headers.__getitem__)),),
        )
        state = tuple(iter_historical_state_request_batches(
            plan=plan, header_inventory=header_inventory
        ))
        self.assertEqual(tuple(row["kind"] for row in state), (
            "reserve", "price", "fee_history", "final_anchor"
        ))
        self.assertEqual(tuple(row["root_index"] for row in state), (1, 2, 3, 4))
        self.assertEqual(state[0]["request_count"], 4)
        self.assertEqual(state[0]["requests"][0]["params"][1], {
            "blockHash": headers[1]["hash"], "requireCanonical": True
        })
        self.assertFalse(state[2]["allow_http_413_bisection"])
        self.assertFalse(state[3]["allow_http_413_bisection"])

    def test_factory_iterators_support_normal_consumers_then_reject_reuse(self):
        _headers, _capture, _lower, plan = _small_context()
        consumers = (
            lambda source: tuple(source),
            lambda source: list(source),
            lambda source: tuple(row for row in source),
        )
        for consume in consumers:
            source = iter_historical_header_request_batches(plan)
            with self.subTest(consumer=consume):
                rows = consume(source)
                self.assertEqual(tuple(row["kind"] for row in rows), ("header",))
                with self.assertRaises(HistoricalWindowProjectionError) as caught:
                    consume(source)
                self.assertEqual(
                    (caught.exception.reason_code, caught.exception.failure_kind),
                    ("authority_mismatch", "window_plan_invalid"),
                )


class HistoricalFoundryProjectionTests(unittest.TestCase):
    def test_small_window_projects_exact_compact_fixture_only_output(self):
        headers, _capture, lower, plan, header_inventory, _state, projection = (
            _project_small()
        )
        self.assertEqual(set(header_inventory), {
            "schema", "anchor_capture_sha256", "lower_bound_capture_sha256",
            "anchor_header_sha256", "lower_header_sha256",
            "lower_bound_number", "anchor_number", "row_count", "rows",
            "logical_sha256",
        })
        self.assertEqual(header_inventory["row_count"], 2)
        self.assertEqual(tuple(row["number"] for row in header_inventory["rows"]), (1, 2))
        self.assertEqual(set(projection), {
            "schema", "authority", "chain_id", "anchor_capture_sha256",
            "lower_bound_capture_sha256", "range", "role_inventories",
            "boundaries", "request_ledger", "coverage",
        })
        self.assertEqual(projection["schema"], "historical_foundry_window_projection/v1")
        self.assertEqual(projection["authority"], "fixture_only_nonauthorizing")
        self.assertEqual(projection["range"], {
            "lower_bound_number": 1,
            "anchor_number": 2,
            "cutoff_timestamp": 25,
            "block_count": 2,
        })
        self.assertEqual(projection["coverage"], {
            "header_count": 2,
            "reserve_count": 4,
            "price_count": 2,
            "fee_count": 2,
        })
        self.assertEqual(set(projection["role_inventories"]), {
            "headers", "reserves", "prices", "fees"
        })
        self.assertEqual(projection["role_inventories"]["headers"]["row_count"], 2)
        self.assertEqual(projection["request_ledger"]["first_request_id"], 1)
        self.assertEqual(projection["request_ledger"]["last_request_id"], 62)
        self.assertEqual(projection["request_ledger"]["request_count"], 62)
        self.assertEqual(
            tuple(row["role"] for row in projection["request_ledger"]["stage_ranges"]),
            ("anchor", "lower_bound", "headers", "reserves", "prices",
             "fee_history", "final_anchor"),
        )
        self.assertEqual(projection["boundaries"]["predecessor_header"], headers[0])
        self.assertEqual(projection["boundaries"]["lower_header"], headers[1])
        self.assertEqual(projection["boundaries"]["anchor_header"], headers[2])
        self.assertEqual(projection["boundaries"]["final_anchor_header"], headers[2])
        self.assertNotIn("rows", projection)
        self.assertNotIn("final_anchor", projection["role_inventories"])
        self.assertEqual(plan["lower_bound_number"], lower["lower_bound_number"])

    def test_shared_header_projector_accepts_extra_raw_fields_but_closes_ranges(self):
        request = _build_historical_block_header_request(block_number=5, request_id=49)
        header = _normalized_header(5, 10)
        projected = _project_historical_block_header_success(
            request=request,
            response={"jsonrpc": "2.0", "id": 49, "result": _raw_header(header)},
        )
        self.assertEqual(projected["header"], header)
        bad = _raw_header(header)
        bad["gasLimit"] = hex(1 << 64)
        with self.assertRaises(HistoricalWindowProjectionError):
            _project_historical_block_header_success(
                request=request,
                response={"jsonrpc": "2.0", "id": 49, "result": bad},
            )

    def test_descriptor_mutation_and_wrong_header_inventory_reject_before_projection(self):
        headers, capture, lower, plan, inventory, state, _projection = _project_small()
        mutated = copy.deepcopy(state[0])
        mutated["requests"][0]["params"][0]["to"] = FEED_PROXY
        with self.assertRaises(HistoricalWindowProjectionError):
            project_historical_window_projection(
                plan=plan,
                anchor_capture=capture,
                lower_bound_capture=lower,
                header_inventory=inventory,
                batch_results=((mutated, _responses_for_descriptor(
                    mutated, headers.__getitem__)),),
            )
        wrong = copy.deepcopy(inventory)
        wrong["rows"][0]["hash"] = "0x" + "ee" * 32
        with self.assertRaises(HistoricalWindowProjectionError):
            iter_historical_state_request_batches(plan=plan, header_inventory=wrong)

    def test_hostile_descriptor_kind_never_runs_rejected_equality(self):
        class HostileKind:
            def __init__(self):
                self.calls = 0

            def __eq__(self, _other):
                self.calls += 1
                raise RuntimeError("/private/SECRET-KIND")

        class HostileString(str):
            def __new__(cls):
                value = str.__new__(cls, "reserve")
                value.calls = 0
                return value

            def __eq__(self, _other):
                self.calls += 1
                raise RuntimeError("/private/SECRET-STRING-KIND")

        class HostileKey(str):
            __hash__ = str.__hash__

            def __new__(cls):
                value = str.__new__(cls, "kind")
                value.calls = 0
                return value

            def __eq__(self, _other):
                self.calls += 1
                raise RuntimeError("/private/SECRET-KIND-KEY")

        for kind in (HostileKind(), HostileString()):
            with self.subTest(kind=type(kind).__name__):
                with self.assertRaises(HistoricalWindowProjectionError) as caught:
                    _project_complete_historical_window_root(
                        plan={},
                        descriptor={"kind": kind},
                        responses=(),
                        header_inventory=None,
                    )
                self.assertEqual(kind.calls, 0)
                self.assertEqual(
                    (caught.exception.reason_code, caught.exception.failure_kind),
                    ("block_coverage_incomplete", "header_invalid"),
                )
                self.assertNotIn("SECRET", str(caught.exception))
                self.assertNotIn("SECRET", repr(caught.exception))

        key = HostileKey()
        with self.assertRaises(HistoricalWindowProjectionError) as caught:
            _project_complete_historical_window_root(
                plan={},
                descriptor={key: "reserve"},
                responses=(),
                header_inventory=None,
            )
        self.assertEqual(key.calls, 0)
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("block_coverage_incomplete", "header_invalid"),
        )
        self.assertIsNone(caught.exception.__context__)
        self.assertNotIn("SECRET", repr(caught.exception))

    def test_reversible_header_mutation_cannot_authorize_a_price_row(self):
        headers, capture, lower, plan, inventory, state, _projection = (
            _project_small()
        )
        original_timestamp = inventory["rows"][0]["timestamp"]

        def batch_results():
            for descriptor in state:
                responses = list(
                    _responses_for_descriptor(descriptor, headers.__getitem__)
                )
                if descriptor["kind"] == "price":
                    inventory["rows"][0]["timestamp"] = 100
                    responses[0]["result"] = _price_result(1, 100)
                yield descriptor, tuple(responses)
                inventory["rows"][0]["timestamp"] = original_timestamp

        try:
            with self.assertRaises(HistoricalWindowProjectionError):
                project_historical_window_projection(
                    plan=plan,
                    anchor_capture=capture,
                    lower_bound_capture=lower,
                    header_inventory=inventory,
                    batch_results=batch_results(),
                )
        finally:
            inventory["rows"][0]["timestamp"] = original_timestamp

    def test_reserve_price_fee_and_final_anchor_failures_are_closed(self):
        headers, capture, lower, plan, inventory, state, _projection = _project_small()
        expected_pairs = {
            "reserve": ("reserve_snapshot_incomplete", "reserve_abi_invalid"),
            "price": ("price_snapshot_incomplete", "price_freshness_invalid"),
            "fee_history": ("fee_history_incomplete", "fee_shape_invalid"),
            "final_anchor": ("anchor_changed", "final_anchor_mismatch"),
        }
        for target_kind, pair in expected_pairs.items():
            batches = []
            for descriptor in state:
                responses = list(_responses_for_descriptor(descriptor, headers.__getitem__))
                if descriptor["kind"] == target_kind:
                    if target_kind == "reserve":
                        responses[0]["result"] += "00"
                    elif target_kind == "price":
                        number = descriptor["block_stop"]
                        responses[-1]["result"] = _price_result(
                            number, headers[number]["timestamp"], age=3601
                        )
                    elif target_kind == "fee_history":
                        responses[0]["result"]["reward"][0] = ["0x2", "0x1"]
                    else:
                        responses[0]["result"]["hash"] = "0x" + "ee" * 32
                batches.append((descriptor, tuple(responses)))
            with self.subTest(kind=target_kind):
                with self.assertRaises(HistoricalWindowProjectionError) as caught:
                    project_historical_window_projection(
                        plan=plan,
                        anchor_capture=capture,
                        lower_bound_capture=lower,
                        header_inventory=inventory,
                        batch_results=iter(batches),
                    )
                self.assertEqual(
                    (caught.exception.reason_code, caught.exception.failure_kind), pair
                )

    def test_ordinary_iterator_errors_sanitize_and_cancellation_propagates(self):
        _headers, capture, _lower, _plan = _small_context()

        class Exploding:
            def __iter__(self):
                return self

            def __next__(self):
                raise RuntimeError("/private/SECRET-RPC-BODY")

        with self.assertRaises(HistoricalWindowProjectionError) as caught:
            project_historical_lower_bound_capture(
                anchor_capture=capture,
                lookback_seconds=LOOKBACK,
                search_probes=Exploding(),
                boundary_witness=(),
            )
        self.assertNotIn("SECRET", str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)
        self.assertIsNone(caught.exception.__context__)

        for exception in (
            KeyboardInterrupt(), SystemExit(), GeneratorExit(), asyncio.CancelledError()
        ):
            class Cancelled:
                def __iter__(self):
                    return self

                def __next__(self):
                    raise exception

            with self.subTest(exception=type(exception).__name__):
                with self.assertRaises(type(exception)) as propagated:
                    project_historical_lower_bound_capture(
                        anchor_capture=capture,
                        lookback_seconds=LOOKBACK,
                        search_probes=Cancelled(),
                        boundary_witness=(),
                    )
                self.assertIs(propagated.exception, exception)

    def test_sanitized_conversion_and_projector_boundaries_clear_context(self):
        import scripts.historical_foundry_scan as scan

        marker = "/private/SECRET-CONTEXT"

        def assert_sanitized(error):
            self.assertIsNone(error.__cause__)
            self.assertIsNone(error.__context__)
            self.assertNotIn(marker, str(error))
            self.assertNotIn(marker, repr(error))
            rendered = "".join(traceback.format_exception(
                type(error), error, error.__traceback__
            ))
            self.assertNotIn(marker, rendered)

        _headers, _capture, _lower, _plan = _small_context()
        callback_error = RuntimeError(marker)

        def exploding_callback(_number):
            raise callback_error

        with self.assertRaises(HistoricalWindowProjectionError) as callback:
            locate_inclusive_lower_bound(
                anchor=_normalized_header(2, LOOKBACK + 25),
                header_at_number=exploding_callback,
                lookback_seconds=LOOKBACK,
            )
        assert_sanitized(callback.exception)

        issued = []

        def poisoned_closed_callback(_number):
            try:
                raise RuntimeError(marker)
            except RuntimeError:
                error = HistoricalWindowProjectionError(
                    "block_coverage_incomplete", "lower_bound_invalid"
                )
                issued.append(error)
                raise error

        with self.assertRaises(HistoricalWindowProjectionError) as closed_callback:
            locate_inclusive_lower_bound(
                anchor=_normalized_header(2, LOOKBACK + 25),
                header_at_number=poisoned_closed_callback,
                lookback_seconds=LOOKBACK,
            )
        self.assertIsNot(closed_callback.exception, issued[0])
        self.assertEqual(
            (
                closed_callback.exception.reason_code,
                closed_callback.exception.failure_kind,
            ),
            ("block_coverage_incomplete", "lower_bound_invalid"),
        )
        assert_sanitized(closed_callback.exception)

        request = _build_historical_block_header_request(
            block_number=2, request_id=49
        )
        response = {
            "jsonrpc": "2.0",
            "id": 49,
            "result": _raw_header(_normalized_header(2, LOOKBACK + 25)),
        }
        with mock.patch.object(
            scan,
            "_normalized_from_raw",
            side_effect=RuntimeError(marker),
        ):
            with self.assertRaises(HistoricalWindowProjectionError) as projector:
                _project_historical_block_header_success(
                    request=request, response=response
                )
        assert_sanitized(projector.exception)

        with mock.patch.object(
            scan.json,
            "dumps",
            side_effect=RuntimeError(marker),
        ):
            with self.assertRaises(ValueError) as canonical:
                scan._canonical_json_bytes({"fixture": 1})
        assert_sanitized(canonical.exception)

        _ratio_decimal_token(Decimal("0.5"))
        decimal_error = RuntimeError(marker)

        class ExplodingDecimal:
            def __sizeof__(self):
                raise decimal_error

        with mock.patch.object(scan, "Decimal", ExplodingDecimal):
            with self.assertRaises(ValueError) as decimal_failure:
                _preflight_historical_decimal_tuple(ExplodingDecimal())
        assert_sanitized(decimal_failure.exception)


class HistoricalFoundryResourceAndDecimalTests(unittest.TestCase):
    def test_resource_guard_exact_and_plus_one_boundaries(self):
        _guard_historical_json_value([None] * (1_048_576 - 1))
        with self.assertRaises(ValueError):
            _guard_historical_json_value([None] * 1_048_576)

        string = "x" * 262_144
        _guard_historical_json_value(string)
        with self.assertRaises(ValueError):
            _guard_historical_json_value(string + "x")

        _guard_historical_json_value([string] * 32)
        with self.assertRaises(ValueError):
            _guard_historical_json_value([string] * 32 + ["x"])

        value = None
        for _ in range(128):
            value = [value]
        _guard_historical_json_value(value)
        value = [value]
        with self.assertRaises(ValueError):
            _guard_historical_json_value(value)

    def test_integer_preflight_accepts_exact_tokens_and_rejects_before_stringifying(self):
        self.assertEqual(len(_historical_json_int_token_bytes(10 ** 4095)), 4096)
        self.assertEqual(
            len(_historical_json_int_token_bytes(-(10 ** 4095 - 1))), 4096
        )
        for value in (10 ** 4096, -(10 ** 4095), 1 << 1_000_000, True):
            with self.subTest(bits=getattr(value, "bit_length", lambda: 0)()):
                with self.assertRaises(ValueError):
                    _historical_json_int_token_bytes(value)

    def test_decimal_layout_bounds_tokens_endpoints_and_quantum_are_exact(self):
        expected_sizes = {
            4095: 1832,
            4096: 1832,
            4097: 1832,
            4500: 2000,
            4617: 2048,
            4618: 2056,
        }
        for digits, size in expected_sizes.items():
            value = Decimal("9" * digits)
            self.assertEqual(Decimal.__sizeof__(value), size)
            if digits <= 4617:
                self.assertEqual(len(_preflight_historical_decimal_tuple(value)[1]), digits)
            else:
                with self.assertRaises(ValueError):
                    _preflight_historical_decimal_tuple(value)

        self.assertEqual(_ratio_decimal_token(Decimal("0")), "0")
        self.assertEqual(_ratio_decimal_token(Decimal("1.0")), "1")
        self.assertEqual(_ratio_decimal_token(Decimal("0.50")), "5e-1")
        with self.assertRaises(ValueError):
            _ratio_decimal_token(Decimal("-0"))
        with self.assertRaises(ValueError):
            _ratio_decimal_token(Decimal("NaN"))
        with self.assertRaises(ValueError):
            _ratio_decimal_token(Decimal("9" * 4097))

        self.assertEqual(
            _ratio_decimal_token(
                Decimal("0.6666666666666666"), gas_used=2, gas_limit=3
            ),
            "6.666666666666666e-1",
        )
        self.assertEqual(
            _ratio_decimal_token(
                Decimal("0.14285714285714285"), gas_used=1, gas_limit=7
            ),
            "1.4285714285714285e-1",
        )
        with self.assertRaises(ValueError):
            _ratio_decimal_token(Decimal("0.4"), gas_used=1, gas_limit=2)
        self.assertEqual(_ratio_decimal_token(0, gas_used=0, gas_limit=2), "0")
        self.assertEqual(_ratio_decimal_token(1, gas_used=2, gas_limit=2), "1")
        for bad in (True, 0.5, "0.5", Decimal("1.1")):
            with self.assertRaises(ValueError):
                _ratio_decimal_token(bad)

        with localcontext() as context:
            context.prec = 2
            context.Emax = 9
            context.Emin = -9
            self.assertEqual(
                _ratio_decimal_token(
                    Decimal("0.6666666666666666"), gas_used=2, gas_limit=3
                ),
                "6.666666666666666e-1",
            )

    def test_fee_root_preflights_each_decimal_object_exactly_once(self):
        import scripts.historical_foundry_scan as scan

        headers, _capture, _lower, plan, inventory, state, _projection = (
            _project_small()
        )
        descriptor = next(row for row in state if row["kind"] == "fee_history")
        responses = list(_responses_for_descriptor(descriptor, headers.__getitem__))
        gas_first = Decimal("0.5")
        gas_second = Decimal("0.5")
        shared_blob = Decimal("0.5")
        self.assertIsNot(gas_first, gas_second)
        result = responses[0]["result"]
        result["gasUsedRatio"] = [gas_first, gas_second]
        result["baseFeePerBlobGas"] = ["0x0", "0x0", "0x0"]
        result["blobGasUsedRatio"] = [shared_blob, shared_blob]

        original = scan._preflight_historical_decimal_tuple
        with mock.patch.object(
            scan,
            "_preflight_historical_decimal_tuple",
            wraps=original,
        ) as preflight:
            projected = _project_complete_historical_window_root(
                plan=plan,
                descriptor=descriptor,
                responses=tuple(responses),
                header_inventory=inventory,
            )
        self.assertEqual(projected["typed_row_count"], 2)
        self.assertEqual(preflight.call_count, 3)
        self.assertEqual(
            {id(call.args[0]) for call in preflight.call_args_list},
            {id(gas_first), id(gas_second), id(shared_blob)},
        )


class HistoricalFoundryScanTask3bSchedulingTests(unittest.TestCase):
    def test_locator_propagates_exact_bound_rpc_error_unchanged(self):
        import scripts.historical_foundry_rpc as rpc

        exact = rpc._ArchiveRpcError(
            "archive_state_unavailable", "transport_unavailable"
        )

        def fail(_number):
            raise exact

        with self.assertRaises(rpc._ArchiveRpcError) as caught:
            locate_inclusive_lower_bound(
                anchor=_normalized_header(2, LOOKBACK + 25),
                header_at_number=fail,
                lookback_seconds=LOOKBACK,
            )
        self.assertIs(caught.exception, exact)

class HistoricalFoundryScanTask4bBridgeTests(unittest.TestCase):
    def test_task4b_strict_fixture_rejects_rechecker_replaced_with_true(self):
        module = sys.modules[__name__]
        original = module._recheck_task4b_offline_production_preflight
        fixture = _Task4bOfflineCapabilityFixture()
        try:
            module._recheck_task4b_offline_production_preflight = (
                lambda _preflight: True
            )
            with self.assertRaises(AssertionError):
                fixture.mint()
        finally:
            module._recheck_task4b_offline_production_preflight = original
            fixture.close()

    def test_task4b_copied_context_cannot_reach_storage_source_bind(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        source_class = storage._HistoricalWindowCaptureReplaySource
        original_method = source_class._bind_reconciliation_from_bound_scan
        original_exported = storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS
        interception_exported = list(original_exported)
        observation = {
            "interception_calls": 0,
            "copied_error": None,
            "origin_bind_succeeded": False,
        }

        def interception(
            self, *, expected_view, expected_reconciliation
        ):
            observation["interception_calls"] += 1
            source_class._bind_reconciliation_from_bound_scan = (
                original_method
            )
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                original_exported
            )
            copied = contextvars.copy_context()
            try:
                copied.run(
                    scan._bind_production_historical_window_capture_replay_source_from_bound_storage,
                    reconciliation=expected_reconciliation,
                    source=self,
                )
            except BaseException as caught:
                observation["copied_error"] = caught
            try:
                original_method(
                    self,
                    expected_view=expected_view,
                    expected_reconciliation=expected_reconciliation,
                )
            except rpc._ArchiveRpcError:
                observation["origin_bind_succeeded"] = False
            else:
                observation["origin_bind_succeeded"] = True
            raise RuntimeError("stop after copied-context probe")

        interception_exported[2] = interception
        error = None
        capability = None
        prior_trace = sys.gettrace()
        armed = [False]

        def tracer(frame, event, argument):
            if (
                not armed[0]
                and frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "_prepare_handle"
                and event == "return"
                and type(argument) is source_class
            ):
                armed[0] = True
                source_class._bind_reconciliation_from_bound_scan = (
                    interception
                )
                storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = tuple(
                    interception_exported
                )
            return tracer

        try:
            capability = fixture.mint()
            sys.settrace(tracer)
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                error = caught
        finally:
            sys.settrace(prior_trace)
            source_class._bind_reconciliation_from_bound_scan = (
                original_method
            )
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = original_exported
        try:
            copied_error = observation["copied_error"]
            self.assertEqual(observation["interception_calls"], 1)
            self.assertIs(type(copied_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (copied_error.reason_code, copied_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIs(observation["origin_bind_succeeded"], True)
            self.assertIs(type(error), rpc._ArchiveRpcError)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            observation["copied_error"] = None
            error = None
            capability = None
            fixture.capability = None
            fixture.close()

    def test_task4b_post_bind_scan_surface_drift_precedes_association(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        for surface_index, surface_name in enumerate(
            scan._TASK4B_SCAN_LOCAL_SURFACE_NAMES
        ):
            with self.subTest(surface=surface_name):
                fixture = _Task4bOfflineCapabilityFixture()
                source_class = storage._HistoricalWindowCaptureReplaySource
                original_method = (
                    source_class._bind_reconciliation_from_bound_scan
                )
                original_storage_exported = (
                    storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS
                )
                original_scan_object = getattr(scan, surface_name)
                original_scan_exported = (
                    scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS
                )

                if surface_index == 1:
                    class ReplacementCaptureReplayEvent:
                        pass

                    replacement_scan_object = (
                        ReplacementCaptureReplayEvent
                    )
                else:
                    def replacement_scan_object(*_args, **_kwargs):
                        raise AssertionError(
                            "synchronized scan replacement was invoked"
                        )

                replacement_scan_exported = list(
                    original_scan_exported
                )
                replacement_scan_exported[surface_index] = (
                    replacement_scan_object
                )
                observation = {
                    "interception_calls": 0,
                    "association_calls": 0,
                }

                def interception(
                    self, *, expected_view, expected_reconciliation
                ):
                    observation["interception_calls"] += 1
                    source_class._bind_reconciliation_from_bound_scan = (
                        original_method
                    )
                    storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                        original_storage_exported
                    )
                    original_method(
                        self,
                        expected_view=expected_view,
                        expected_reconciliation=expected_reconciliation,
                    )
                    source_class._bind_reconciliation_from_bound_scan = (
                        interception
                    )
                    storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                        interception_storage_exported
                    )
                    setattr(
                        scan, surface_name, replacement_scan_object
                    )
                    scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS = tuple(
                        replacement_scan_exported
                    )
                    return None

                interception_storage_exported = list(
                    original_storage_exported
                )
                interception_storage_exported[2] = interception
                interception_storage_exported = tuple(
                    interception_storage_exported
                )
                error = None
                capability = None
                prior_trace = sys.gettrace()
                armed = [False]

                def tracer(frame, event, argument):
                    if (
                        frame.f_code.co_filename == scan.__file__
                        and frame.f_code.co_name
                        == "_install_task4b_capture_replay_association"
                        and event == "call"
                    ):
                        observation["association_calls"] += 1
                    if (
                        not armed[0]
                        and frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name == "_prepare_handle"
                        and event == "return"
                        and type(argument) is source_class
                    ):
                        armed[0] = True
                        source_class._bind_reconciliation_from_bound_scan = (
                            interception
                        )
                        storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                            interception_storage_exported
                        )
                    return tracer

                try:
                    capability = fixture.mint()
                    sys.settrace(tracer)
                    try:
                        scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                    except BaseException as caught:
                        error = caught
                finally:
                    sys.settrace(prior_trace)
                    source_class._bind_reconciliation_from_bound_scan = (
                        original_method
                    )
                    storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                        original_storage_exported
                    )
                    setattr(scan, surface_name, original_scan_object)
                    scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS = (
                        original_scan_exported
                    )
                try:
                    self.assertEqual(
                        observation["interception_calls"], 1
                    )
                    self.assertEqual(
                        observation["association_calls"], 0
                    )
                    self.assertIs(type(error), rpc._ArchiveRpcError)
                    self.assertEqual(
                        (error.reason_code, error.failure_kind),
                        ("authority_mismatch", "final_identity_drift"),
                    )
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    error = None
                    capability = None
                    fixture.capability = None
                    fixture.close()

    def test_task4b_association_uses_initializer_weakref_anchor(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        fixture = _Task4bOfflineCapabilityFixture()
        original_weakref_module = scan.weakref
        original_ref = weakref.ref
        target_references = []
        strong_ref_calls = [0]

        class StrongReference:
            def __init__(self, target):
                self.target = target

            def __call__(self):
                return self.target

        def forged_ref(target):
            strong_ref_calls[0] += 1
            target_references.append(original_ref(target))
            return StrongReference(target)

        error = None
        capability = None
        try:
            capability = fixture.mint()
            scan.weakref = types.SimpleNamespace(ref=forged_ref)
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                error = caught
        finally:
            scan.weakref = original_weakref_module
        try:
            self.assertIs(type(error), rpc._ArchiveRpcError)
            self.assertEqual(
                (error.reason_code, error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertEqual(strong_ref_calls[0], 0)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            error = None
            capability = None
            fixture.capability = None
            fixture.close()
            gc.collect()
            self.assertTrue(all(
                reference() is None for reference in target_references
            ))
        finally:
            fixture.close()

    def _run_task4b_injected_materialization(self, stage, injected):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        context_before = tuple(contextvars.copy_context().items())
        observation = {
            "fired": False,
            "caught": None,
            "pair": None,
            "same": False,
            "partial_publication_reached": False,
            "directory_empty": False,
            "context_restored": False,
        }
        prior_trace = sys.gettrace()

        def fire():
            observation["fired"] = True
            sys.settrace(prior_trace)
            raise injected

        def tracer(frame, event, _argument):
            name = frame.f_code.co_name
            if observation["fired"]:
                return tracer
            if (
                frame.f_code.co_filename == storage.__file__
                and name == "_bind_reconciliation_from_bound_scan"
                and (
                    (stage == "before_storage_verification" and event == "call")
                    or (
                        stage == "after_storage_verification"
                        and event == "return"
                    )
                )
            ):
                fire()
            if (
                frame.f_code.co_filename == scan.__file__
                and name == "_install_task4b_capture_replay_association"
                and event == "line"
            ):
                record = frame.f_locals.get("record")
                if (
                    stage == "during_scan_publication"
                    and type(record) is dict
                    and record.get("state") == "capture_replay_binding"
                ):
                    observation["partial_publication_reached"] = True
                    fire()
            return tracer

        error = None
        capability = None
        try:
            capability = fixture.mint()
            sys.settrace(tracer)
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                error = caught
        finally:
            sys.settrace(prior_trace)
        try:
            observation["caught"] = error
            observation["same"] = error is injected
            if hasattr(error, "reason_code") and hasattr(
                error, "failure_kind"
            ):
                observation["pair"] = (
                    error.reason_code, error.failure_kind
                )
            observation["directory_empty"] = (
                tuple(fixture.data_dir.iterdir()) == ()
            )
            observation["context_restored"] = (
                tuple(contextvars.copy_context().items()) == context_before
            )
            capability = None
            fixture.capability = None
        finally:
            fixture.close()
        return observation

    def test_task4b_strict_offline_preflight_recipe_and_direct_drift_matrix(self):
        import scripts.historical_foundry_rpc as rpc

        preflight = _new_task4b_offline_production_preflight(rpc)
        try:
            self.assertTrue(
                _recheck_task4b_offline_production_preflight(preflight)
            )
            mutations = (
                lambda: preflight.identity.__setitem__(
                    "repository_head", "0" * 40
                ),
                lambda: preflight.identity["python"].__setitem__(
                    "cache_tag", "forged"
                ),
                lambda: preflight.identity["configs"].__setitem__(
                    "policy_id", "policy:" + "0" * 64
                ),
                lambda: preflight.identity["sources"][0].__setitem__(
                    "size_bytes",
                    preflight.identity["sources"][0]["size_bytes"] + 1,
                ),
                lambda: preflight.identity["project_inputs"].__setitem__(
                    "foundry_toml_sha256", "0" * 64
                ),
                lambda: preflight.identity["toolchain"][
                    "forge_std"
                ].__setitem__("commit", "0" * 40),
                lambda: preflight.identity["executor_artifact"].__setitem__(
                    "authority_physical_sha256", "0" * 64
                ),
                lambda: preflight.identity.__setitem__(
                    "deployed_runtime_sha256", "0" * 64
                ),
            )
            for mutate in mutations:
                with self.subTest(mutate=mutate):
                    opening = _detach_task4b_test_value(
                        preflight.opening_identity
                    )
                    mutate()
                    self.assertFalse(
                        _recheck_task4b_offline_production_preflight(
                            preflight
                        )
                    )
                    preflight.identity = opening
                    self.assertTrue(
                        _recheck_task4b_offline_production_preflight(
                            preflight
                        )
                    )
            preflight.closed = True
            self.assertFalse(
                _recheck_task4b_offline_production_preflight(preflight)
            )
            preflight.closed = False
            self.assertFalse(
                _recheck_task4b_offline_production_preflight(object())
            )
        finally:
            preflight.close()

    def test_task4b_strict_harness_mints_real_production_capability(self):
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        try:
            capability = fixture.mint()
            self.assertIs(
                type(capability),
                storage._ProductionHistoricalWindowCapability,
            )
            self.assertEqual(len(tuple(fixture.data_dir.iterdir())), 1)
            self.assertIsNone(capability.close())
            fixture.capability = None
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_task4b_pre_mint_scan_surface_drift_matrix_is_terminal(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        for surface_index, surface_name in enumerate(
            scan._TASK4B_SCAN_LOCAL_SURFACE_NAMES
        ):
            with self.subTest(surface=surface_name):
                fixture = _Task4bOfflineCapabilityFixture()
                original_object = getattr(scan, surface_name)
                original_exported = (
                    scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS
                )
                if surface_index == 1:
                    replacement = type(
                        "ReplacementCaptureReplayEvent", (), {}
                    )
                else:
                    def replacement(*_args, **_kwargs):
                        raise AssertionError(
                            "synchronized scan replacement was invoked"
                        )
                replacement_exported = list(original_exported)
                replacement_exported[surface_index] = replacement
                observation = {
                    "mint_calls": 0,
                    "capability_allocations": 0,
                }
                prior_trace = sys.gettrace()

                def tracer(frame, event, argument):
                    if frame.f_code.co_filename != storage.__file__:
                        return None
                    if (
                        frame.f_code.co_name
                        == "_mint_production_historical_window_capability_core"
                        and event == "call"
                    ):
                        observation["mint_calls"] += 1
                    elif (
                        frame.f_code.co_name == "_prepare_handle"
                        and event == "return"
                        and type(argument)
                        is storage._ProductionHistoricalWindowCapability
                    ):
                        observation["capability_allocations"] += 1
                    return tracer

                try:
                    setattr(scan, surface_name, replacement)
                    scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS = tuple(
                        replacement_exported
                    )
                    sys.settrace(tracer)
                    with self.assertRaises(rpc._ArchiveRpcError) as caught:
                        fixture.mint()
                finally:
                    sys.settrace(prior_trace)
                    setattr(scan, surface_name, original_object)
                    scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS = (
                        original_exported
                    )
                try:
                    self.assertEqual(
                        (
                            caught.exception.reason_code,
                            caught.exception.failure_kind,
                        ),
                        ("authority_mismatch", "final_identity_drift"),
                    )
                    self.assertEqual(observation["mint_calls"], 1)
                    self.assertEqual(
                        observation["capability_allocations"], 0
                    )
                    self.assertIsNone(fixture.capability)
                    self.assertTrue(fixture.preflight.closed)
                    self.assertEqual(fixture.context._state, "finalized")
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.close()

    def test_task4b_ingress_replays_exchange_events_then_fails_closed(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        with open(scan.__file__, "r", encoding="utf-8") as source_file:
            source_tree = ast.parse(
                source_file.read(), filename=scan.__file__
            )
        function_nodes = [
            node for node in ast.walk(source_tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        issuer_nodes = [
            node for node in function_nodes
            if node.name == "_issue_task4b_capture_replay_event"
        ]
        self.assertEqual(len(issuer_nodes), 1)
        issuer_node = issuer_nodes[0]
        allocation_calls = [
            node for node in ast.walk(source_tree)
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "__new__"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "object"
                and node.args
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id
                == "_ProductionHistoricalWindowCaptureReplayEvent"
            )
        ]
        self.assertEqual(len(allocation_calls), 1)
        allocation_call = allocation_calls[0]
        allocation_owners = [
            node for node in function_nodes
            if (
                node.lineno <= allocation_call.lineno
                and allocation_call.end_lineno <= node.end_lineno
            )
        ]
        self.assertIs(
            max(allocation_owners, key=lambda node: node.lineno),
            issuer_node,
        )
        registry_publications = [
            node for node in ast.walk(source_tree)
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "capture_replay_event_registry"
                    for target in node.targets
                )
            )
        ]
        self.assertEqual(len(registry_publications), 1)
        publication_line = registry_publications[0].lineno

        fixture = _Task4bOfflineCapabilityFixture()
        context_before = tuple(contextvars.copy_context().items())
        observation = {
            "binder_calls": 0,
            "source_bind_calls": 0,
            "association_returns": 0,
            "source_allocations": 0,
            "snapshot_allocations": 0,
            "event_registry_sizes": [],
            "event_issuer_calls": 0,
            "event_registry_publications": 0,
            "event_payload_tags": [],
        }
        prior_trace = sys.gettrace()

        def tracer(frame, event, argument):
            if frame.f_code.co_filename == storage.__file__:
                if (
                    frame.f_code.co_name == "_prepare_handle"
                    and event == "return"
                ):
                    if type(argument) is storage._HistoricalWindowCaptureReplaySource:
                        observation["source_allocations"] += 1
                    elif type(argument) is storage.HistoricalRunStagingSnapshot:
                        observation["snapshot_allocations"] += 1
                if (
                    frame.f_code.co_name
                    == "_bind_reconciliation_from_bound_scan"
                    and event == "call"
                ):
                    observation["source_bind_calls"] += 1
                return tracer
            if frame.f_code.co_filename != scan.__file__:
                return tracer
            name = frame.f_code.co_name
            if name == "_issue_task4b_capture_replay_event":
                if event == "call":
                    observation["event_issuer_calls"] += 1
                    payload = frame.f_locals.get("payload")
                    if type(payload) is tuple and payload:
                        observation["event_payload_tags"].append(payload[0])
                elif event == "line" and frame.f_lineno == publication_line:
                    observation["event_registry_publications"] += 1
            if (
                name
                == "_bind_production_historical_window_capture_replay_source_from_bound_storage"
                and event == "call"
            ):
                observation["binder_calls"] += 1
            if (
                name == "_install_task4b_capture_replay_association"
                and event == "return"
            ):
                observation["association_returns"] += 1
            event_registry = frame.f_locals.get("event_registry")
            if type(event_registry) is dict:
                observation["event_registry_sizes"].append(
                    len(event_registry)
                )
            return tracer

        result = None
        error = None
        try:
            capability = fixture.mint()
            sys.settrace(tracer)
            try:
                result = scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                error = caught
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertIs(
                type(result), storage.HistoricalRunStagingSnapshot
            )
            self.assertIsNone(error)
            self.assertEqual(observation["binder_calls"], 1)
            self.assertEqual(observation["source_bind_calls"], 1)
            self.assertEqual(observation["association_returns"], 1)
            self.assertEqual(observation["source_allocations"], 1)
            self.assertEqual(observation["snapshot_allocations"], 1)
            self.assertTrue(observation["event_registry_sizes"])
            self.assertEqual(set(observation["event_registry_sizes"]), {0})
            legal_event_count = 2 * (len(fixture.calls) - 1)
            self.assertEqual(
                observation["event_issuer_calls"], legal_event_count
            )
            self.assertEqual(
                observation["event_registry_publications"],
                legal_event_count,
            )
            self.assertEqual(
                len(observation["event_payload_tags"]), legal_event_count
            )
            self.assertEqual(
                observation["event_payload_tags"].count("exchange"),
                len(fixture.calls) - 1,
            )
            self.assertIn("root", observation["event_payload_tags"])
            self.assertIn("finish", observation["event_payload_tags"])
            self.assertEqual(observation["event_payload_tags"][-1], "finish")
            self.assertEqual(
                tuple(contextvars.copy_context().items()), context_before
            )
            with self.assertRaises(rpc._ArchiveRpcError) as reset_rejection:
                scan._bind_production_historical_window_capture_replay_source_from_bound_storage(
                    reconciliation=object(), source=object()
                )
            self.assertEqual(
                (
                    reset_rejection.exception.reason_code,
                    reset_rejection.exception.failure_kind,
                ),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            forged = object.__new__(
                scan._ProductionHistoricalWindowCaptureReplayEvent
            )
            with self.assertRaises(rpc._ArchiveRpcError) as event_rejection:
                scan._consume_production_historical_window_capture_replay_event_for_storage(
                    event=forged,
                    expected_source=object(),
                    expected_event_index=0,
                )
            self.assertEqual(
                (
                    event_rejection.exception.reason_code,
                    event_rejection.exception.failure_kind,
                ),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            forged = None
            event_rejection = None
            error = None
            capability = None
            fixture.capability = None
            self.assertIsNone(result.close())
            self.assertIsNone(result.close())
            result = None
            gc.collect()
            self.assertFalse(any(
                type(value)
                is scan._ProductionHistoricalWindowCaptureReplayEvent
                for value in gc.get_objects()
            ))
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if result is not None:
                result.close()
            fixture.close()

    def test_task4b_three_transaction_boundaries_sanitize_ordinary_failure(self):
        expected_pair = (
            "authority_mismatch",
            "historical_window_spool_handoff_failed",
        )
        for stage in (
            "before_storage_verification",
            "after_storage_verification",
            "during_scan_publication",
        ):
            with self.subTest(stage=stage):
                observation = self._run_task4b_injected_materialization(
                    stage, RuntimeError("Task4b ordinary " + stage)
                )
                self.assertTrue(observation["fired"])
                self.assertEqual(observation["pair"], expected_pair)
                self.assertTrue(observation["directory_empty"])
                self.assertTrue(observation["context_restored"])
                if stage == "during_scan_publication":
                    self.assertTrue(
                        observation["partial_publication_reached"]
                    )
                observation["caught"] = None

    def test_task4b_three_transaction_boundaries_preserve_all_controls(self):
        for stage in (
            "before_storage_verification",
            "after_storage_verification",
            "during_scan_publication",
        ):
            for control_type in (
                KeyboardInterrupt,
                SystemExit,
                GeneratorExit,
                asyncio.CancelledError,
            ):
                with self.subTest(stage=stage, control=control_type.__name__):
                    control = control_type(
                        "Task4b control {} {}".format(
                            stage, control_type.__name__
                        )
                    )
                    observation = self._run_task4b_injected_materialization(
                        stage, control
                    )
                    self.assertTrue(observation["fired"])
                    self.assertTrue(observation["same"])
                    self.assertIs(observation["caught"], control)
                    self.assertIsNone(control.__context__)
                    self.assertTrue(observation["directory_empty"])
                    self.assertTrue(observation["context_restored"])
                    observation["caught"] = None
                    control.__traceback__ = None

    def test_task4b_real_owner_cleanup_priority_has_no_exception_context(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        cases = (
            (
                RuntimeError("Task4b body ordinary"),
                GeneratorExit("Task4b cleanup control"),
                "cleanup",
            ),
            (
                asyncio.CancelledError("Task4b body control"),
                GeneratorExit("Task4b later cleanup control"),
                "body",
            ),
        )
        for body_error, cleanup_control, selected in cases:
            with self.subTest(selected=selected):
                fixture = _Task4bOfflineCapabilityFixture()
                prior_trace = sys.gettrace()
                original_close = storage.os.close
                patcher = [None]
                state = {"body_fired": False, "cleanup_fired": False}

                def close_then_control(fd):
                    original_close(fd)
                    if not state["cleanup_fired"]:
                        state["cleanup_fired"] = True
                        raise cleanup_control

                def tracer(frame, event, _argument):
                    if (
                        not state["body_fired"]
                        and frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name
                        == "_bind_reconciliation_from_bound_scan"
                        and event == "call"
                    ):
                        state["body_fired"] = True
                        patcher[0] = mock.patch.object(
                            storage.os, "close", new=close_then_control
                        )
                        patcher[0].start()
                        sys.settrace(prior_trace)
                        raise body_error
                    return tracer

                caught = None
                capability = None
                try:
                    capability = fixture.mint()
                    sys.settrace(tracer)
                    try:
                        scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                    except BaseException as observed:
                        caught = observed
                finally:
                    sys.settrace(prior_trace)
                    if patcher[0] is not None:
                        patcher[0].stop()
                try:
                    expected = (
                        cleanup_control if selected == "cleanup"
                        else body_error
                    )
                    self.assertTrue(state["body_fired"])
                    self.assertTrue(state["cleanup_fired"])
                    self.assertIs(caught, expected)
                    self.assertIsNone(caught.__context__)
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    caught = None
                    capability = None
                    fixture.capability = None
                    body_error.__traceback__ = None
                    cleanup_control.__traceback__ = None
                    fixture.close()

    def test_task4b_post_storage_verification_drift_blocks_scan_publication(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        original_method = (
            storage._HistoricalWindowCaptureReplaySource
            ._bind_reconciliation_from_bound_scan
        )
        original_exported = storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS
        replacement_exported = list(original_exported)

        def replacement(
            self, *, expected_view, expected_reconciliation
        ):
            return original_method(
                self,
                expected_view=expected_view,
                expected_reconciliation=expected_reconciliation,
            )

        replacement_exported[2] = replacement
        replacement_exported = tuple(replacement_exported)
        observation = {
            "fired": False,
            "association_calls": 0,
        }
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name
                == "_install_task4b_capture_replay_association"
                and event == "call"
            ):
                observation["association_calls"] += 1
            if (
                not observation["fired"]
                and frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name
                == "_bind_reconciliation_from_bound_scan"
                and event == "return"
            ):
                observation["fired"] = True
                storage._HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan = replacement
                storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                    replacement_exported
                )
            return tracer

        error = None
        capability = None
        try:
            capability = fixture.mint()
            sys.settrace(tracer)
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                error = caught
        finally:
            sys.settrace(prior_trace)
            storage._HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan = original_method
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = original_exported
        try:
            self.assertTrue(observation["fired"])
            self.assertIs(type(error), rpc._ArchiveRpcError)
            self.assertEqual(
                (error.reason_code, error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertEqual(observation["association_calls"], 0)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            error = None
            capability = None
            fixture.capability = None
            fixture.close()


class HistoricalFoundryScanTask4bLeafJoinTests(unittest.TestCase):
    _COMPACT_KEYS = (
        "schema", "exchange_index", "logical_batch_index", "attempt_index",
        "request_byte_count", "request_sha256", "request_ids",
        "wire_byte_count", "wire_sha256", "decoded_byte_count",
        "decoded_sha256", "response_ids", "spool_member_index",
        "spool_offset", "spool_length", "spool_member_sha256",
    )
    _POST_LEAF_KEYS = (
        "schema", "segment", "segment_local_index", "leaf_index",
        "request_ids", "request_count", "canonical_request_sha256",
        "response_ids", "exchange_index", "logical_batch_index",
        "attempt_index", "request_byte_count", "decoded_byte_count",
        "decoded_sha256", "wire_byte_count", "wire_sha256",
        "wire_hash_authority", "spool_member_index", "spool_offset",
        "spool_length", "spool_member_sha256",
    )

    def test_slice4_replays_exact_compact_post_leaf_joins_once(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        payloads = []
        consumer_calls = []
        protocol_actions = []
        event_registry_counts = []
        snapshot_allocations = []
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "_prepare_handle"
                and event == "return"
                and type(_argument)
                is storage.HistoricalRunStagingSnapshot
            ):
                snapshot_allocations.append(weakref.ref(_argument))
                return tracer
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "__next__"
                and event == "return"
                and type(_argument) is tuple
                and len(_argument) == 3
            ):
                protocol_actions.append("source")
                return tracer
            if frame.f_code.co_filename != scan.__file__:
                return tracer
            if frame.f_code.co_name == "_issue_task4b_capture_replay_event":
                if event == "call":
                    payload = frame.f_locals.get("payload")
                    if type(payload) is tuple:
                        payloads.append(copy.deepcopy(payload))
                        protocol_actions.append("issue")
                elif event == "return":
                    registry = frame.f_locals.get(
                        "capture_replay_event_registry"
                    )
                    if type(registry) is dict:
                        event_registry_counts.append(
                            ("issuer_return", len(registry))
                        )
            elif (
                frame.f_code.co_name
                == "_consume_production_historical_window_capture_replay_event_for_storage"
            ):
                if event == "call":
                    consumer_calls.append(
                        frame.f_locals.get("expected_event_index")
                    )
                    protocol_actions.append("consume")
                elif event == "return":
                    registry = frame.f_locals.get(
                        "capture_replay_event_registry"
                    )
                    if type(registry) is dict:
                        event_registry_counts.append(
                            ("consumer_return", len(registry))
                        )
            return tracer

        result = None
        error = None
        try:
            capability = fixture.mint()
            sys.settrace(tracer)
            try:
                result = scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                error = caught
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertIsNone(error)
            self.assertIs(
                type(result), storage.HistoricalRunStagingSnapshot
            )
            self.assertEqual(len(snapshot_allocations), 1)
            self.assertTrue(payloads)
            self.assertEqual(
                consumer_calls, list(range(len(payloads)))
            )
            self.assertEqual(
                event_registry_counts,
                [
                    observation
                    for _event_index in range(len(payloads))
                    for observation in (
                        ("issuer_return", 1),
                        ("consumer_return", 0),
                    )
                ],
            )
            self.assertEqual(
                max(count for _boundary, count in event_registry_counts), 1
            )
            self.assertEqual(
                protocol_actions,
                [
                    action
                    for payload in payloads
                    for action in (
                        ("source", "issue", "consume")
                        if payload[0] == "exchange"
                        else ("issue", "consume")
                    )
                ],
            )
            exchange_payloads = [
                payload for payload in payloads if payload[0] == "exchange"
            ]
            self.assertEqual(
                len(exchange_payloads), len(fixture.calls) - 1
            )
            self.assertEqual(
                {payload[0] for payload in exchange_payloads}, {"exchange"}
            )
            for expected_index, payload in enumerate(exchange_payloads, 1):
                self.assertIs(type(payload), tuple)
                self.assertEqual(len(payload), 3)
                compact, leaf = payload[1:]
                self.assertIs(type(compact), dict)
                self.assertIs(type(leaf), dict)
                self.assertEqual(tuple(compact), self._COMPACT_KEYS)
                self.assertEqual(tuple(leaf), self._POST_LEAF_KEYS)
                self.assertEqual(
                    compact["schema"],
                    "historical_foundry_archive_rpc_spooled_success_exchange/v1",
                )
                self.assertEqual(
                    leaf["schema"],
                    "historical_foundry_leaf_ledger/v1",
                )
                self.assertEqual(compact["exchange_index"], expected_index)
                for compact_key, leaf_key in (
                    ("exchange_index", "exchange_index"),
                    ("logical_batch_index", "logical_batch_index"),
                    ("attempt_index", "attempt_index"),
                    ("request_ids", "request_ids"),
                    ("response_ids", "response_ids"),
                    ("request_sha256", "canonical_request_sha256"),
                    ("request_byte_count", "request_byte_count"),
                    ("decoded_byte_count", "decoded_byte_count"),
                    ("decoded_sha256", "decoded_sha256"),
                    ("wire_byte_count", "wire_byte_count"),
                    ("wire_sha256", "wire_sha256"),
                    ("spool_member_index", "spool_member_index"),
                    ("spool_offset", "spool_offset"),
                    ("spool_length", "spool_length"),
                    ("spool_member_sha256", "spool_member_sha256"),
                ):
                    self.assertEqual(
                        compact[compact_key], leaf[leaf_key]
                    )
                self.assertEqual(
                    leaf["request_count"], len(compact["request_ids"])
                )
                self.assertEqual(
                    leaf["wire_hash_authority"],
                    "task2b_sealed_not_rehashed",
                )
                reconstructed_leaf = {
                    "schema": "historical_foundry_leaf_ledger/v1",
                    "segment": leaf["segment"],
                    "segment_local_index": leaf["segment_local_index"],
                    "leaf_index": leaf["leaf_index"],
                    "request_ids": compact["request_ids"],
                    "request_count": len(compact["request_ids"]),
                    "canonical_request_sha256": compact["request_sha256"],
                    "response_ids": compact["response_ids"],
                    "exchange_index": compact["exchange_index"],
                    "logical_batch_index": compact["logical_batch_index"],
                    "attempt_index": compact["attempt_index"],
                    "request_byte_count": compact["request_byte_count"],
                    "decoded_byte_count": compact["decoded_byte_count"],
                    "decoded_sha256": compact["decoded_sha256"],
                    "wire_byte_count": compact["wire_byte_count"],
                    "wire_sha256": compact["wire_sha256"],
                    "wire_hash_authority": "task2b_sealed_not_rehashed",
                    "spool_member_index": compact["spool_member_index"],
                    "spool_offset": compact["spool_offset"],
                    "spool_length": compact["spool_length"],
                    "spool_member_sha256": compact[
                        "spool_member_sha256"
                    ],
                }
                self.assertEqual(reconstructed_leaf, leaf)

            split_positions = [
                index for index in range(len(fixture.calls) - 2)
                if fixture.calls[index]
                == fixture.calls[index + 1] + fixture.calls[index + 2]
            ]
            self.assertEqual(len(split_positions), 1)
            split = split_positions[0]
            split_payloads = [
                payload for payload in exchange_payloads
                if payload[1]["request_ids"]
                in (fixture.calls[split + 1], fixture.calls[split + 2])
            ]
            self.assertEqual(len(split_payloads), 2)
            self.assertEqual(
                tuple(payload[2]["leaf_index"] for payload in split_payloads),
                (0, 1),
            )
            self.assertEqual(
                tuple(payload[1]["attempt_index"] for payload in split_payloads),
                (2, 3),
            )
            self.assertEqual(
                len({
                    (
                        payload[2]["segment"],
                        payload[2]["segment_local_index"],
                        payload[1]["logical_batch_index"],
                    )
                    for payload in split_payloads
                }),
                1,
            )
            self.assertIsNone(result.close())
            self.assertIsNone(result.close())
            result = None
            gc.collect()
            self.assertIsNone(snapshot_allocations[0]())
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if result is not None:
                result.close()
            fixture.capability = None
            fixture.close()

    def test_slice4_unconsumed_event_is_revoked_and_controls_preserve_identity(
        self,
    ):
        import scripts.historical_foundry_scan as scan

        for control_type in (
            KeyboardInterrupt,
            SystemExit,
            GeneratorExit,
            asyncio.CancelledError,
        ):
            with self.subTest(control=control_type.__name__):
                fixture = _Task4bOfflineCapabilityFixture()
                control = control_type(
                    "slice4 unconsumed event " + control_type.__name__
                )
                observation = {
                    "issuer_calls": 0,
                    "consumer_calls": 0,
                    "fired": False,
                }
                prior_trace = sys.gettrace()

                def tracer(frame, event, _argument):
                    if frame.f_code.co_filename != scan.__file__:
                        return tracer
                    if (
                        frame.f_code.co_name
                        == "_issue_task4b_capture_replay_event"
                        and event == "return"
                    ):
                        observation["issuer_calls"] += 1
                        if not observation["fired"]:
                            observation["fired"] = True
                            raise control
                    elif (
                        frame.f_code.co_name
                        == "_consume_production_historical_window_capture_replay_event_for_storage"
                        and event == "call"
                    ):
                        observation["consumer_calls"] += 1
                    return tracer

                escaped = None
                try:
                    capability = fixture.mint()
                    sys.settrace(tracer)
                    try:
                        scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                    except BaseException as caught:
                        escaped = caught
                finally:
                    sys.settrace(prior_trace)
                try:
                    self.assertTrue(observation["fired"])
                    self.assertEqual(observation["issuer_calls"], 1)
                    self.assertEqual(observation["consumer_calls"], 0)
                    self.assertIs(escaped, control)
                    self.assertIsNone(control.__context__)
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                    escaped = None
                    control.__traceback__ = None
                    capability = None
                    fixture.capability = None
                    gc.collect()
                    self.assertFalse(any(
                        type(value)
                        is scan._ProductionHistoricalWindowCaptureReplayEvent
                        for value in gc.get_objects()
                    ))
                finally:
                    fixture.close()

    def test_slice4_detached_source_axis_mutations_reject_before_event_issue(
        self,
    ):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        mutations = (
            (
                "exchange_index",
                lambda compact: compact.__setitem__(
                    "exchange_index", compact["exchange_index"] + 1
                ),
            ),
            (
                "request_ids",
                lambda compact: compact.__setitem__(
                    "request_ids", (compact["request_ids"][0] + 1,)
                ),
            ),
            (
                "request_sha256",
                lambda compact: compact.__setitem__(
                    "request_sha256", "0" * 64
                ),
            ),
            (
                "response_ids",
                lambda compact: compact.__setitem__(
                    "response_ids",
                    tuple(
                        response_id + 100_000
                        for response_id in compact["response_ids"]
                    ),
                ),
            ),
            (
                "decoded_sha256",
                lambda compact: compact.__setitem__(
                    "decoded_sha256", "0" * 64
                ),
            ),
            (
                "attempt_order",
                lambda compact: compact.__setitem__(
                    "attempt_index", compact["attempt_index"] + 1
                ),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(axis=label):
                fixture = _Task4bOfflineCapabilityFixture()
                observation = {"mutated": False, "issuer_calls": 0}
                prior_trace = sys.gettrace()

                def tracer(frame, event, argument):
                    if (
                        frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name == "__next__"
                        and event == "return"
                        and not observation["mutated"]
                        and type(argument) is tuple
                        and len(argument) == 3
                        and type(argument[0]) is dict
                    ):
                        mutate(argument[0])
                        observation["mutated"] = True
                    elif (
                        frame.f_code.co_filename == scan.__file__
                        and frame.f_code.co_name
                        == "_issue_task4b_capture_replay_event"
                        and event == "call"
                    ):
                        observation["issuer_calls"] += 1
                    return tracer

                error = None
                try:
                    capability = fixture.mint()
                    sys.settrace(tracer)
                    try:
                        scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                    except BaseException as caught:
                        error = caught
                finally:
                    sys.settrace(prior_trace)
                try:
                    self.assertTrue(observation["mutated"])
                    self.assertEqual(observation["issuer_calls"], 0)
                    self.assertIs(type(error), rpc._ArchiveRpcError)
                    self.assertEqual(
                        (error.reason_code, error.failure_kind),
                        (
                            "authority_mismatch",
                            "historical_window_reconciliation_mismatch",
                        ),
                    )
                    self.assertIsNone(error.__context__)
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.capability = None
                    fixture.close()


class HistoricalFoundryScanTask4bReplayTests(unittest.TestCase):
    _SEMANTIC_ROOT_NAMES = (
        "_materialize_historical_anchor_stage",
        "project_historical_anchor_capture",
        "project_historical_lower_bound_capture",
        "build_historical_window_request_plan",
        "iter_historical_header_request_batches",
        "iter_historical_state_request_batches",
        "_project_complete_historical_window_root",
    )

    def _run_replay(
        self,
        *,
        context_factory=_small_context,
        before_materialize=None,
        callback=None,
        trace_mint_callback=False,
    ):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture(
            context_factory=context_factory
        )
        observation = {
            "payloads": [],
            "consumer_payloads": [],
            "consumer_return_observations": [],
            "replay_unwind_states": [],
            "malformed_event_index": None,
            "registry_counts": [],
            "actions": [],
            "source_rows": [],
            "outstanding_source_frames": 0,
            "source_frame_state": "empty",
            "max_outstanding_source_frames": 0,
            "source_frame_protocol_violations": [],
            "post_roots": (),
            "authenticated_header_mapping": None,
            "authenticated_window_mapping": None,
            "prefinalization_digests": None,
            "reconciliation_digests": None,
            "semantic_calls": [],
            "semantic_root_calls": [],
            "manifest_checks": 0,
            "error": None,
            "archive_error": None,
            "error_pair": None,
            "snapshot_allocations": 0,
            "snapshot_type": None,
            "live_data_entries": None,
            "data_entries": None,
            "fixture_calls": None,
        }
        prior_trace = sys.gettrace()

        def tracer(frame, event, argument):
            filename = frame.f_code.co_filename
            name = frame.f_code.co_name
            if (
                filename == storage.__file__
                and name == "_prepare_handle"
                and event == "return"
                and type(argument)
                is storage.HistoricalRunStagingSnapshot
            ):
                observation["snapshot_allocations"] += 1
            if (
                filename == storage.__file__
                and name == "__next__"
                and event == "return"
                and type(argument) is tuple
                and len(argument) == 3
                and type(argument[0]) is dict
                and type(argument[1]) is bytes
                and type(argument[2]) is bytes
            ):
                try:
                    decoded = json.loads(argument[2].decode("utf-8"))
                except (UnicodeDecodeError, ValueError):
                    decoded = None
                row = (
                    copy.deepcopy(argument[0]),
                    len(argument[1]),
                    len(argument[2]),
                    len(decoded) if type(decoded) is list else None,
                )
                observation["source_rows"].append(row)
                if observation["outstanding_source_frames"] != 0:
                    observation["source_frame_protocol_violations"].append(
                        "next_before_consume"
                    )
                observation["outstanding_source_frames"] += 1
                if observation["source_frame_state"] != "empty":
                    observation["source_frame_protocol_violations"].append(
                        "next_before_prior_frame_terminal"
                    )
                observation["source_frame_state"] = "returned"
                observation["max_outstanding_source_frames"] = max(
                    observation["max_outstanding_source_frames"],
                    observation["outstanding_source_frames"],
                )
                observation["actions"].append((
                    "source",
                    row[0].get("logical_batch_index"),
                    row[0].get("exchange_index"),
                ))
            if (
                filename in (rpc.__file__, scan.__file__)
                and name in self._SEMANTIC_ROOT_NAMES
                and event == "call"
            ):
                observation["semantic_calls"].append(name)
                caller = frame.f_back
                if (
                    caller is not None
                    and caller.f_code.co_filename == scan.__file__
                    and caller.f_code.co_name == "_call_task4b_semantic_root"
                ):
                    observation["semantic_root_calls"].append(name)
            if filename == scan.__file__:
                if (
                    name == "drive"
                    and event == "return"
                    and argument is None
                    and type(frame.f_locals.get("record")) is dict
                    and type(frame.f_locals.get(
                        "capture_replay_event_registry"
                    )) is dict
                    and frame.f_locals.get("source") is not None
                ):
                    association = frame.f_locals["record"]
                    registry = frame.f_locals[
                        "capture_replay_event_registry"
                    ]
                    observation["replay_unwind_states"].append((
                        len(registry),
                        association.get("state"),
                        association.get("event_issuer_state"),
                        association.get("live_event") is None,
                    ))
                if (
                    name == "drive"
                    and event == "return"
                    and frame.f_back is not None
                    and frame.f_back.f_code.co_name == "drive"
                    and type(frame.f_back.f_locals.get("record")) is dict
                    and frame.f_back.f_locals.get("source") is not None
                    and type(argument) is dict
                ):
                    schema = argument.get("schema")
                    if schema == "historical_foundry_header_inventory/v1":
                        observation["authenticated_header_mapping"] = (
                            copy.deepcopy(argument)
                        )
                    elif schema == "historical_foundry_window_projection/v1":
                        observation["authenticated_window_mapping"] = (
                            copy.deepcopy(argument)
                        )
                if name == "_new_task4b_exchange_replay" and event == "return":
                    roots = frame.f_locals.get("post_roots")
                    record = frame.f_locals.get("record")
                    if type(roots) is tuple:
                        observation["post_roots"] = copy.deepcopy(roots)
                    if type(record) is dict:
                        pre = record.get("prefinalization_digests")
                        replay = record.get("replay_digests")
                        if type(pre) is tuple:
                            observation["prefinalization_digests"] = tuple(pre)
                        if type(replay) is tuple:
                            observation["reconciliation_digests"] = tuple(replay)
                elif (
                    name == "_issue_task4b_capture_replay_event"
                    and event == "call"
                ):
                    payload = frame.f_locals.get("payload")
                    if type(payload) is tuple:
                        detached = copy.deepcopy(payload)
                        if (
                            detached[0] == "exchange"
                            and (
                                observation["outstanding_source_frames"] != 1
                                or observation["source_frame_state"]
                                != "returned"
                            )
                        ):
                            observation[
                                "source_frame_protocol_violations"
                            ].append("issue_without_one_frame")
                        elif detached[0] == "exchange":
                            observation["source_frame_state"] = "issued"
                        observation["payloads"].append(detached)
                        observation["actions"].append((
                            "issue",
                            detached[0],
                            (
                                detached[1].get("logical_batch_index")
                                if len(detached) > 1
                                and type(detached[1]) is dict
                                else None
                            ),
                        ))
                elif (
                    name == "_issue_task4b_capture_replay_event"
                    and event == "return"
                ):
                    registry = frame.f_locals.get(
                        "capture_replay_event_registry"
                    )
                    if type(registry) is dict:
                        observation["registry_counts"].append(
                            ("issuer_return", len(registry))
                        )
                elif (
                    name
                    == "_consume_production_historical_window_capture_replay_event_for_storage"
                    and event == "return"
                    and argument is not None
                ):
                    expected_event_index = frame.f_locals.get(
                        "expected_event_index"
                    )
                    exact_tuple = type(argument) is tuple
                    tag = (
                        argument[0]
                        if exact_tuple
                        and len(argument) > 0
                        and type(argument[0]) is str
                        else None
                    )
                    observation["consumer_return_observations"].append((
                        expected_event_index,
                        exact_tuple,
                        type(argument).__name__,
                        tag,
                    ))
                    if exact_tuple:
                        detached = copy.deepcopy(argument)
                    else:
                        detached = None
                    if detached is not None and detached[0] == "exchange":
                        if (
                            observation["outstanding_source_frames"] != 1
                            or observation["source_frame_state"] != "issued"
                        ):
                            observation[
                                "source_frame_protocol_violations"
                            ].append("consume_without_one_frame")
                        else:
                            observation["outstanding_source_frames"] = 0
                            observation["source_frame_state"] = "empty"
                    if detached is not None:
                        observation["consumer_payloads"].append(detached)
                        observation["actions"].append((
                            "consume",
                            detached[0],
                            (
                                detached[1].get("logical_batch_index")
                                if len(detached) > 1
                                and type(detached[1]) is dict
                                else None
                            ),
                        ))
                    registry = frame.f_locals.get(
                        "capture_replay_event_registry"
                    )
                    if type(registry) is dict:
                        observation["registry_counts"].append(
                            ("consumer_return", len(registry))
                        )
                elif (
                    name == "_verify_task4b_semantic_dependency_manifest"
                    and event == "call"
                ):
                    observation["manifest_checks"] += 1
            if callback is not None:
                callback(frame, event, argument, observation)
            return tracer

        def mint_tracer(frame, event, argument):
            if (
                callback is not None
                and frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name in (
                    "project_historical_lower_bound_capture",
                    "_project_lower_observation",
                )
            ):
                callback(frame, event, argument, observation)
            return mint_tracer

        restore = None
        result = None
        try:
            if trace_mint_callback:
                sys.settrace(mint_tracer)
                try:
                    capability = fixture.mint()
                finally:
                    sys.settrace(prior_trace)
            else:
                capability = fixture.mint()
            if before_materialize is not None:
                restore = before_materialize(
                    rpc, scan, storage, observation
                )
            sys.settrace(tracer)
            try:
                result = scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as error:
                if type(error) is rpc._ArchiveRpcError:
                    observation["archive_error"] = error
                    observation["error_pair"] = (
                        error.reason_code, error.failure_kind
                    )
                else:
                    observation["error"] = error
        finally:
            sys.settrace(prior_trace)
            if restore is not None:
                restore()
            observation["fixture_calls"] = tuple(fixture.calls)
            if fixture.data_dir is not None and fixture.data_dir.exists():
                observation["live_data_entries"] = tuple(
                    path.name for path in fixture.data_dir.iterdir()
                )
            if result is not None:
                observation["snapshot_type"] = type(result)
                result.close()
                result.close()
                result = None
            if fixture.data_dir is not None and fixture.data_dir.exists():
                observation["data_entries"] = tuple(
                    path.name for path in fixture.data_dir.iterdir()
                )
            fixture.capability = None
            fixture.close()
        return observation

    @staticmethod
    def _root_role(root):
        segment = root.get("segment")
        if segment in ("anchor_stage", "lower_observation"):
            return segment
        if segment == "window_root":
            role = root.get("typed_role")
            if role in (
                "headers", "reserves", "prices", "fees", "final_anchor"
            ):
                return role
        raise AssertionError("unexpected Task4b post-root role")

    def test_slice5_authenticated_transaction_emits_exchange_root_finish_in_exact_order(
        self,
    ):
        import scripts.historical_foundry_storage as storage

        headers, _anchor, _lower, plan = _three_block_context()
        height = plan["block_count"]
        self.assertEqual(height, len(headers) - 1)
        observed = self._run_replay(context_factory=_three_block_context)

        self.assertIsNone(observed["error_pair"])
        self.assertIsNone(observed["error"])
        self.assertEqual(observed["snapshot_allocations"], 1)
        self.assertIs(
            observed["snapshot_type"],
            storage.HistoricalRunStagingSnapshot,
        )
        roots = observed["post_roots"]
        self.assertTrue(roots)
        self.assertNotEqual(len(roots), 7)
        exchange_count = sum(root["leaf_count"] for root in roots)
        payloads = observed["payloads"]
        self.assertEqual(
            len(payloads), exchange_count + len(roots) + 1
        )
        self.assertEqual(payloads, observed["consumer_payloads"])
        self.assertEqual(
            observed["registry_counts"],
            [
                boundary
                for _payload in payloads
                for boundary in (
                    ("issuer_return", 1),
                    ("consumer_return", 0),
                )
            ],
        )

        roles = []
        cursor = 0
        for root in roots:
            for exchange_index in root["success_exchange_indices"]:
                payload = payloads[cursor]
                self.assertIs(type(payload), tuple)
                self.assertEqual(len(payload), 3)
                self.assertEqual(payload[0], "exchange")
                self.assertEqual(payload[1]["exchange_index"], exchange_index)
                self.assertEqual(payload[2]["exchange_index"], exchange_index)
                cursor += 1
            payload = payloads[cursor]
            self.assertIs(type(payload), tuple)
            self.assertEqual(len(payload), 6)
            self.assertEqual(payload[0], "root")
            self.assertEqual(payload[1], root)
            role = self._root_role(root)
            roles.append(role)
            self.assertEqual(payload[2], role)
            if role in ("headers", "reserves", "prices", "fees"):
                self.assertIs(type(payload[3]), bytes)
                typed_rows = json.loads(payload[3].decode("utf-8"))
                self.assertIs(type(typed_rows), list)
                self.assertEqual(len(typed_rows), payload[4])
                self.assertEqual(payload[4], root["typed_row_count"])
                self.assertEqual(payload[5], root["typed_logical_sha256"])
                self.assertNotEqual(
                    payload[5], hashlib.sha256(payload[3]).hexdigest()
                )
            else:
                self.assertEqual(payload[3:], (None, 0, None))
            cursor += 1

        self.assertEqual(
            set(roles),
            {
                "anchor_stage", "lower_observation", "headers",
                "reserves", "prices", "fees", "final_anchor",
            },
        )
        finish = payloads[cursor]
        self.assertIs(type(finish), tuple)
        self.assertEqual(len(finish), 5)
        self.assertEqual(finish[0], "finish")
        self.assertEqual(finish[1], exchange_count)
        self.assertIs(type(finish[2]), tuple)
        self.assertEqual(
            finish[2],
            (
                ("headers", height),
                ("reserves", 2 * height),
                ("prices", height),
                ("fees", height),
            ),
        )
        self.assertTrue(all(
            type(row) is tuple
            and len(row) == 2
            and type(row[0]) is str
            and type(row[1]) is int
            for row in finish[2]
        ))
        self.assertEqual(
            finish[3], observed["prefinalization_digests"]
        )
        self.assertEqual(
            finish[4], observed["reconciliation_digests"]
        )

        reserve_splits = [
            index
            for index in range(len(observed["fixture_calls"]) - 2)
            if observed["fixture_calls"][index]
            == observed["fixture_calls"][index + 1]
            + observed["fixture_calls"][index + 2]
        ]
        self.assertEqual(len(reserve_splits), 1)
        split = reserve_splits[0]
        self.assertEqual(
            tuple(
                len(observed["fixture_calls"][split + offset])
                for offset in (1, 2)
            ),
            (3, 3),
        )
        self.assertEqual(observed["data_entries"], ())

    def test_slice5_public_driver_parity_backpressure_and_wrapper_metadata(
        self,
    ):
        import scripts.historical_foundry_scan as scan

        headers, capture, lower, plan = _three_block_context()
        header_batches = tuple(iter_historical_header_request_batches(plan))
        header_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(
                    descriptor, headers.__getitem__
                ),
            )
            for descriptor in header_batches
        )
        ordinary_headers = project_historical_header_inventory(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            batch_results=iter(header_pairs),
        )
        state_batches = tuple(iter_historical_state_request_batches(
            plan=plan, header_inventory=ordinary_headers
        ))
        state_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(
                    descriptor, headers.__getitem__
                ),
            )
            for descriptor in state_batches
        )
        ordinary_window = project_historical_window_projection(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            header_inventory=ordinary_headers,
            batch_results=iter(state_pairs),
        )
        self.assertEqual(
            ordinary_window["role_inventories"]["headers"]["row_count"],
            ordinary_headers["row_count"],
        )
        self.assertEqual(
            ordinary_window["role_inventories"]["headers"][
                "logical_sha256"
            ],
            ordinary_headers["logical_sha256"],
        )

        for projector in (
            scan.project_historical_header_inventory,
            scan.project_historical_window_projection,
        ):
            self.assertEqual(projector.__name__, projector.__qualname__)
            self.assertEqual(projector.__module__, scan.__name__)
            self.assertIs(pickle.loads(pickle.dumps(projector)), projector)
        module_tree = ast.parse(inspect.getsource(scan))
        call_counts = {
            name: sum(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == name
                for node in ast.walk(module_tree)
            )
            for name in (
                "project_historical_header_inventory",
                "project_historical_window_projection",
            )
        }
        self.assertEqual(
            call_counts,
            {
                "project_historical_header_inventory": 2,
                "project_historical_window_projection": 2,
            },
        )

        observed = self._run_replay(context_factory=_three_block_context)
        self.assertEqual(
            observed["authenticated_header_mapping"], ordinary_headers
        )
        self.assertEqual(
            observed["authenticated_window_mapping"], ordinary_window
        )
        roots = observed["post_roots"]
        root_payloads = [
            payload for payload in observed["payloads"]
            if payload[0] == "root"
        ]
        self.assertEqual(len(root_payloads), len(roots))
        anchor_roots = [
            root for root in roots if root.get("segment") == "anchor_stage"
        ]
        lower_roots = [
            root
            for root in roots if root.get("segment") == "lower_observation"
        ]
        window_roots = [
            root for root in roots if root.get("segment") == "window_root"
        ]
        self.assertEqual(
            observed["semantic_root_calls"].count(
                "_project_complete_historical_window_root"
            ),
            len(window_roots),
        )
        self.assertEqual(
            observed["semantic_root_calls"].count(
                "_materialize_historical_anchor_stage"
            ),
            len(anchor_roots),
        )
        self.assertEqual(
            observed["semantic_root_calls"].count(
                "project_historical_anchor_capture"
            ),
            1,
        )
        self.assertEqual(
            observed["semantic_root_calls"].count(
                "project_historical_lower_bound_capture"
            ),
            1,
        )
        self.assertEqual(len(lower_roots), len(lower["request_ids"]))
        for name in (
            "build_historical_window_request_plan",
            "iter_historical_header_request_batches",
            "iter_historical_state_request_batches",
        ):
            self.assertEqual(observed["semantic_root_calls"].count(name), 1)

        typed_rows_by_role = {
            "headers": [], "reserves": [], "prices": [], "fees": []
        }
        for payload in root_payloads:
            role = payload[2]
            if role not in typed_rows_by_role:
                self.assertEqual(payload[3:], (None, 0, None))
                continue
            decoded = json.loads(payload[3].decode("utf-8"))
            self.assertEqual(len(decoded), payload[4])
            typed_rows_by_role[role].extend(decoded)
            self.assertEqual(
                payload[5], payload[1]["typed_logical_sha256"]
            )
        self.assertEqual(
            tuple(typed_rows_by_role["headers"]),
            ordinary_headers["rows"],
        )
        for role, rows in typed_rows_by_role.items():
            expected = ordinary_window["role_inventories"][role]
            self.assertEqual(len(rows), expected["row_count"])
            matching = [
                payload for payload in root_payloads
                if payload[2] == role
            ]
            self.assertEqual(
                matching[-1][5], expected["logical_sha256"]
            )

        actions = observed["actions"]
        for position, root in enumerate(window_roots):
            logical_index = root["logical_batch_index"]
            source_positions = [
                index for index, action in enumerate(actions)
                if action[0] == "source" and action[1] == logical_index
            ]
            consume_positions = [
                index for index, action in enumerate(actions)
                if action == ("consume", "root", logical_index)
            ]
            self.assertTrue(source_positions)
            self.assertEqual(len(consume_positions), 1)
            self.assertLess(max(source_positions), consume_positions[0])
            if position + 1 < len(window_roots):
                next_logical = window_roots[position + 1][
                    "logical_batch_index"
                ]
                next_sources = [
                    index for index, action in enumerate(actions)
                    if action[0] == "source" and action[1] == next_logical
                ]
                self.assertTrue(next_sources)
                self.assertLess(consume_positions[0], min(next_sources))
        finish_positions = [
            index for index, action in enumerate(actions)
            if action[0] == "consume" and action[1] == "finish"
        ]
        source_positions = [
            index for index, action in enumerate(actions)
            if action[0] == "source"
        ]
        self.assertEqual(len(finish_positions), 1)
        self.assertLess(max(source_positions), finish_positions[0])

    def test_slice5_semantic_poison_matrix_fails_closed(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        class TupleSubclass(tuple):
            pass

        class IntSubclass(int):
            pass

        class StrSubclass(str):
            pass

        def one_block_context():
            headers = {
                0: _normalized_header(0, 24),
                1: _normalized_header(1, 604_825),
            }
            capture = _capture_for_header(headers[1])
            lower = _lower_capture(capture, headers.__getitem__)
            plan = build_historical_window_request_plan(
                lower_bound_capture=lower,
                anchor_capture=capture,
            )
            if plan["block_count"] != 1:
                raise AssertionError("Task4b one-block context differs")
            return headers, capture, lower, plan

        def mutate_root(frame, role, field, value):
            roots = frame.f_locals.get("post_roots")
            if type(roots) is not tuple:
                return False
            for root in roots:
                observed_role = self._root_role(root)
                if observed_role == role:
                    root[field] = value(root[field])
                    return True
            return False

        cases = (
            ("typed_digest", "reconciliation"),
            ("raw_bytes_with_copied_hashes", "handoff"),
            ("response_order", "reconciliation"),
            ("final_anchor", "reconciliation"),
            ("omit_root", "reconciliation"),
            ("duplicate_root", "reconciliation"),
            ("header_coverage", "reconciliation"),
            ("state_coverage", "reconciliation"),
            ("finish_outer_mapping", "reconciliation"),
            ("finish_outer_list", "reconciliation"),
            ("finish_outer_tuple_subclass", "reconciliation"),
            ("finish_pair_list", "reconciliation"),
            ("finish_pair_tuple_subclass", "reconciliation"),
            ("finish_pairs_list", "reconciliation"),
            ("finish_pairs_mapping", "reconciliation"),
            ("finish_pairs_tuple_subclass", "reconciliation"),
            ("finish_pair_mapping", "reconciliation"),
            ("finish_role_str_subclass", "reconciliation"),
            ("finish_pair_count_int_subclass", "reconciliation"),
            ("finish_pair_count_bool_h1", "reconciliation"),
            ("finish_bool_count", "reconciliation"),
            ("finish_int_subclass_count", "reconciliation"),
            ("finish_reordered_pairs", "reconciliation"),
            ("finish_missing_pair", "reconciliation"),
            ("finish_extra_pair", "reconciliation"),
            ("finish_nontyped_role", "reconciliation"),
        )
        for label, expected_kind in cases:
            with self.subTest(axis=label):
                fired = [False]

                def poison(frame, event, argument, _observation):
                    if fired[0]:
                        return
                    filename = frame.f_code.co_filename
                    name = frame.f_code.co_name
                    if (
                        filename == scan.__file__
                        and name == "_new_task4b_exchange_replay"
                        and event == "return"
                    ):
                        if label == "typed_digest":
                            fired[0] = mutate_root(
                                frame,
                                "headers",
                                "typed_logical_sha256",
                                lambda _value: "0" * 64,
                            )
                        elif label == "final_anchor":
                            fired[0] = mutate_root(
                                frame,
                                "final_anchor",
                                "typed_row_count",
                                lambda value: value + 1,
                            )
                        elif label in ("omit_root", "duplicate_root"):
                            record = frame.f_locals.get("record")
                            roots = (
                                record.get("post_root_ledger")
                                if type(record) is dict else None
                            )
                            if type(roots) is tuple and roots:
                                record["post_root_ledger"] = (
                                    roots[:-1]
                                    if label == "omit_root"
                                    else roots + (copy.deepcopy(roots[-1]),)
                                )
                                fired[0] = True
                        elif label == "header_coverage":
                            fired[0] = mutate_root(
                                frame,
                                "headers",
                                "typed_row_count",
                                lambda value: value + 1,
                            )
                        elif label == "state_coverage":
                            fired[0] = mutate_root(
                                frame,
                                "reserves",
                                "typed_row_count",
                                lambda value: value + 1,
                            )
                    elif (
                        label == "raw_bytes_with_copied_hashes"
                        and filename == storage.__file__
                        and name == "_task4b_next_replay_source_frame"
                        and event == "return"
                    ):
                        builder = frame.f_locals.get("builder")
                        if type(builder) is bytearray and builder:
                            builder[-1] ^= 1
                            fired[0] = True
                    elif (
                        label == "response_order"
                        and filename == storage.__file__
                        and name == "__next__"
                        and event == "return"
                        and type(argument) is tuple
                        and len(argument) == 3
                        and type(argument[0]) is dict
                    ):
                        response_ids = argument[0].get("response_ids")
                        if type(response_ids) is tuple and len(response_ids) > 1:
                            argument[0]["response_ids"] = tuple(
                                reversed(response_ids)
                            )
                            fired[0] = True
                    elif (
                        label.startswith("finish_")
                        and filename == scan.__file__
                        and name == "_issue_task4b_capture_replay_event"
                        and event == "return"
                    ):
                        payload = frame.f_locals.get("payload")
                        registry = frame.f_locals.get(
                            "capture_replay_event_registry"
                        )
                        association = frame.f_locals.get("record")
                        source = frame.f_locals.get("source")
                        event_entry = (
                            registry.get(id(argument))
                            if type(registry) is dict else None
                        )
                        if (
                            type(payload) is tuple
                            and len(payload) == 5
                            and payload[0] == "finish"
                            and type(event_entry) is tuple
                            and len(event_entry) == 2
                            and event_entry[0] is argument
                            and type(event_entry[1]) is dict
                            and type(association) is dict
                            and event_entry[1].get("state") == "live"
                            and event_entry[1].get("source") is source
                            and event_entry[1].get("association")
                            is association
                            and event_entry[1].get("payload") is payload
                            and type(event_entry[1].get("event_index")) is int
                            and event_entry[1].get("event_index")
                            == association.get("next_event_index")
                            and association.get("live_event") is argument
                            and association.get("event_issuer_state")
                            == "awaiting_consume"
                        ):
                            pairs = payload[2]
                            if label == "finish_outer_mapping":
                                malformed = dict(enumerate(payload))
                            elif label == "finish_outer_list":
                                malformed = list(payload)
                            elif label == "finish_outer_tuple_subclass":
                                malformed = TupleSubclass(payload)
                            elif label == "finish_pair_list":
                                malformed = payload[:2] + ((
                                    list(pairs[0]),
                                ) + pairs[1:],) + payload[3:]
                            elif label == "finish_pair_tuple_subclass":
                                malformed = payload[:2] + ((
                                    TupleSubclass(pairs[0]),
                                ) + pairs[1:],) + payload[3:]
                            elif label == "finish_pairs_list":
                                malformed = payload[:2] + (
                                    list(pairs),
                                ) + payload[3:]
                            elif label == "finish_pairs_mapping":
                                malformed = payload[:2] + (
                                    dict(pairs),
                                ) + payload[3:]
                            elif label == "finish_pairs_tuple_subclass":
                                malformed = payload[:2] + (
                                    TupleSubclass(pairs),
                                ) + payload[3:]
                            elif label == "finish_pair_mapping":
                                malformed = payload[:2] + ((
                                    {pairs[0][0]: pairs[0][1]},
                                ) + pairs[1:],) + payload[3:]
                            elif label == "finish_role_str_subclass":
                                malformed = payload[:2] + ((
                                    (StrSubclass(pairs[0][0]), pairs[0][1]),
                                ) + pairs[1:],) + payload[3:]
                            elif label == "finish_pair_count_int_subclass":
                                malformed = payload[:2] + ((
                                    (pairs[0][0], IntSubclass(pairs[0][1])),
                                ) + pairs[1:],) + payload[3:]
                            elif label == "finish_pair_count_bool_h1":
                                if pairs[0] != ("headers", 1):
                                    raise AssertionError(
                                        "Task4b bool-count fixture is not H=1"
                                    )
                                malformed = payload[:2] + ((
                                    (pairs[0][0], True),
                                ) + pairs[1:],) + payload[3:]
                            elif label == "finish_bool_count":
                                malformed = (
                                    payload[0], True, payload[2],
                                    payload[3], payload[4],
                                )
                            elif label == "finish_int_subclass_count":
                                malformed = (
                                    payload[0], IntSubclass(payload[1]),
                                    payload[2], payload[3], payload[4],
                                )
                            elif label == "finish_reordered_pairs":
                                malformed = payload[:2] + (
                                    tuple(reversed(pairs)),
                                ) + payload[3:]
                            elif label == "finish_missing_pair":
                                malformed = payload[:2] + (
                                    pairs[:-1],
                                ) + payload[3:]
                            elif label == "finish_extra_pair":
                                malformed = payload[:2] + (
                                    pairs + (("headers", 0),),
                                ) + payload[3:]
                            elif label == "finish_nontyped_role":
                                malformed = payload[:2] + ((
                                    ("anchor_stage", pairs[0][1]),
                                ) + pairs[1:],) + payload[3:]
                            else:
                                raise AssertionError(
                                    "unexpected Task4b finish poison"
                                )
                            _observation["malformed_event_index"] = (
                                event_entry[1]["event_index"]
                            )
                            event_entry[1]["payload"] = malformed
                            fired[0] = True

                observed = self._run_replay(
                    context_factory=(
                        one_block_context
                        if label == "finish_pair_count_bool_h1"
                        else _small_context
                    ),
                    callback=poison,
                )
                self.assertTrue(fired[0])
                expected_failure = (
                    "historical_window_spool_handoff_failed"
                    if expected_kind == "handoff"
                    else "historical_window_reconciliation_mismatch"
                )
                self.assertEqual(
                    observed["error_pair"],
                    ("authority_mismatch", expected_failure),
                )
                if label.startswith("finish_"):
                    malformed_event_index = observed[
                        "malformed_event_index"
                    ]
                    self.assertIs(type(malformed_event_index), int)
                    self.assertFalse(any(
                        payload[0] == "finish"
                        for payload in observed["consumer_payloads"]
                    ))
                    self.assertFalse(any(
                        row[0] == malformed_event_index
                        for row in observed[
                            "consumer_return_observations"
                        ]
                    ))
                    self.assertTrue(observed["replay_unwind_states"])
                    self.assertEqual(
                        observed["replay_unwind_states"][-1],
                        (0, "consumed_failed", "failed", True),
                    )
                self.assertEqual(observed["data_entries"], ())

    def test_slice5_final_identity_translators_clear_exception_context(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        for site in ("materializer_precheck", "replay_precheck"):
            with self.subTest(site=site):
                state = {"drifted": False, "original": None}

                def install_drift(current_scan):
                    state["original"] = current_scan._parse_quantity
                    current_scan._parse_quantity = lambda _value: 0
                    state["drifted"] = True

                def before(_rpc, current_scan, _storage, _observation):
                    if site == "materializer_precheck":
                        install_drift(current_scan)

                    def restore():
                        if state["drifted"]:
                            current_scan._parse_quantity = state["original"]

                    return restore

                def callback(frame, event, _argument, _observation):
                    if (
                        site == "replay_precheck"
                        and not state["drifted"]
                        and frame.f_code.co_filename == scan.__file__
                        and frame.f_code.co_name == "_new_task4b_exchange_replay"
                        and event == "call"
                    ):
                        install_drift(scan)

                observed = self._run_replay(
                    before_materialize=before,
                    callback=callback,
                )
                error = observed["archive_error"]
                try:
                    self.assertTrue(state["drifted"])
                    self.assertIs(type(error), rpc._ArchiveRpcError)
                    self.assertEqual(
                        (error.reason_code, error.failure_kind),
                        ("authority_mismatch", "final_identity_drift"),
                    )
                    self.assertIsNone(error.__cause__)
                    self.assertIsNone(error.__context__)
                    self.assertEqual(observed["data_entries"], ())
                finally:
                    observed["archive_error"] = None

    def test_slice5_precall_manifest_drift_delivers_zero_events(self):
        import scripts.historical_foundry_scan as scan

        surface = HistoricalFoundryScanTask4bSurfaceTests
        modules = {
            "contracts": importlib.import_module(
                "scripts.historical_foundry_contracts"
            ),
            "rpc": importlib.import_module(
                "scripts.historical_foundry_rpc"
            ),
            "scan": scan,
            "route": importlib.import_module("scripts.route_cost_evidence"),
        }
        callable_groups = (
            ("contracts", surface._CONTRACTS_CALLABLE_NAMES),
            ("rpc", surface._RPC_CALLABLE_NAMES),
            ("scan", surface._SCAN_CALLABLE_NAMES),
            ("route", surface._ROUTE_CALLABLE_NAMES),
        )
        constant_groups = (
            ("rpc", surface._RPC_CONSTANT_NAMES),
            ("scan", surface._SCAN_CONSTANT_NAMES),
            ("route", surface._ROUTE_CONSTANT_NAMES),
        )
        cases = []
        cases.extend(
            ("callable", role, name)
            for role, names in callable_groups for name in names
        )
        cases.extend(
            ("class_callable", role, name)
            for role, name in surface._CLASS_CALLABLE_BINDINGS
        )
        cases.extend(
            ("module_attribute", role, module_name + "." + name)
            for role, module_name, name
            in surface._MODULE_ATTRIBUTE_CALLABLES
        )
        cases.extend(
            ("class_surface", role, name)
            for role, name in surface._CLASS_SURFACE_NAMES
        )
        cases.extend(
            ("constant", role, name)
            for role, names in constant_groups for name in names
        )
        canonical_names = {
            "scan": "scripts.historical_foundry_scan",
            "rpc": "scripts.historical_foundry_rpc",
            "contracts": "scripts.historical_foundry_contracts",
            "route": "scripts.route_cost_evidence",
        }
        cases.extend(
            ("canonical_module", role, canonical_name)
            for role, canonical_name in canonical_names.items()
        )
        self.assertEqual(
            sum(len(names) for _role, names in callable_groups), 120
        )
        self.assertEqual(len(surface._CLASS_CALLABLE_BINDINGS), 6)
        self.assertEqual(len(surface._MODULE_ATTRIBUTE_CALLABLES), 6)
        self.assertEqual(len(surface._CLASS_SURFACE_NAMES), 9)
        self.assertEqual(
            sum(len(names) for _role, names in constant_groups), 94
        )
        semantic_roots = {
            ("rpc", "_materialize_historical_anchor_stage"),
            ("rpc", "project_historical_anchor_capture"),
            ("scan", "project_historical_lower_bound_capture"),
            ("scan", "build_historical_window_request_plan"),
            ("scan", "iter_historical_header_request_batches"),
            ("scan", "iter_historical_state_request_batches"),
            ("scan", "_project_complete_historical_window_root"),
        }
        self.assertTrue(semantic_roots.issubset({
            (role, name)
            for category, role, name in cases
            if category == "callable"
        }))
        self.assertEqual(len(cases), 239)
        base_context = _small_context()

        def fresh_manifest_context():
            return copy.deepcopy(base_context)

        def resolve_parent(module, qualified_name):
            components = qualified_name.split(".")
            parent = module
            for component in components[:-1]:
                parent = getattr(parent, component)
            return parent, components[-1]

        def drifted_constant(current_scan, value):
            if type(value) is dict:
                return None
            if type(value) is tuple:
                return value + ("__task4b_drift__",)
            if type(value) is frozenset:
                return value | frozenset(("__task4b_drift__",))
            if type(value) is bytes:
                return value + b"x"
            if type(value) is str:
                return value + "x"
            if type(value) is bool:
                return not value
            if type(value) is int:
                return value + 1
            if type(value) is contextvars.ContextVar:
                return contextvars.ContextVar(
                    "task4b_precall_manifest_drift", default=None
                )
            if (
                hasattr(value, "pattern")
                and hasattr(value, "flags")
                and type(value.pattern) is str
            ):
                return current_scan.re.compile(
                    value.pattern + "(?:)", value.flags
                )
            return object()

        mismatches = []
        for category, role, qualified_name in cases:
            with self.subTest(
                category=category, role=role, name=qualified_name
            ):
                def before(_rpc, current_scan, _storage, state):
                    state["replacement_calls"] = 0
                    if category == "canonical_module":
                        original = sys.modules[qualified_name]
                        sys.modules[qualified_name] = types.ModuleType(
                            qualified_name
                        )
                        return lambda: sys.modules.__setitem__(
                            qualified_name, original
                        )
                    module = modules[role]
                    parent, attribute = resolve_parent(
                        module, qualified_name
                    )
                    original = getattr(parent, attribute)
                    if category == "constant" and type(original) is dict:
                        detached = dict(original)
                        original["__task4b_drift__"] = 1

                        def restore_mapping():
                            original.clear()
                            original.update(detached)

                        return restore_mapping
                    if category == "constant":
                        replacement = drifted_constant(
                            current_scan, original
                        )
                    else:
                        def replacement(*_args, **_kwargs):
                            state["replacement_calls"] += 1
                            raise AssertionError(
                                "pre-call manifest replacement was invoked"
                            )

                    setattr(parent, attribute, replacement)
                    return lambda: setattr(parent, attribute, original)

                observed = self._run_replay(
                    context_factory=fresh_manifest_context,
                    before_materialize=before,
                )
                actual = (
                    observed["error_pair"],
                    len(observed["payloads"]),
                    len(observed["semantic_calls"]),
                    observed["replacement_calls"],
                    observed["data_entries"],
                )
                expected = (
                    ("authority_mismatch", "final_identity_drift"),
                    0, 0, 0, (),
                )
                if actual != expected:
                    mismatches.append((
                        category, role, qualified_name, actual
                    ))
        self.assertFalse(
            mismatches,
            "Task4b pre-call manifest gaps: count={} first={}".format(
                len(mismatches), mismatches[:5]
            ),
        )
        self.assertIs(scan._DECIMAL_LAYOUT_VERIFIED, True)

    def test_slice5_midcall_outcome_and_control_priority(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_storage as storage

        body_controls = (
            KeyboardInterrupt("slice5 body keyboard"),
            SystemExit("slice5 body system"),
            GeneratorExit("slice5 body generator"),
            asyncio.CancelledError("slice5 body cancelled"),
        )
        cleanup_a = asyncio.CancelledError("slice5 cleanup after return")
        cleanup_b = KeyboardInterrupt("slice5 cleanup after ordinary")
        cleanup_c = SystemExit("slice5 cleanup below body control")
        cases = (
            ("ordinary_return_drift", None, True, None, "drift"),
            (
                "ordinary_body_drift", RuntimeError("slice5 ordinary"),
                True, None, "drift",
            ),
            (
                "ordinary_body_no_drift", ValueError("slice5 projector"),
                False, None, "mapping",
            ),
            *((
                "body_control_" + type(control).__name__,
                control,
                True,
                None,
                "body_control",
            ) for control in body_controls),
            (
                "cleanup_after_return", None, True, cleanup_a,
                "cleanup_control",
            ),
            (
                "cleanup_after_ordinary", RuntimeError("slice5 body"),
                True, cleanup_b, "cleanup_control",
            ),
            (
                "body_control_priority", body_controls[0], True,
                cleanup_c, "body_control",
            ),
        )
        for label, body_outcome, drift, cleanup_outcome, expected in cases:
            with self.subTest(case=label):
                state = {
                    "root_called": False,
                    "postchecks_before": None,
                    "dependency_calls": 0,
                    "cleanup_called": False,
                    "dependency_original": None,
                    "dependency_installed": False,
                    "ordinary_injected": False,
                    "cleanup_completed": False,
                    "root_original": None,
                }

                def callback(frame, event, _argument, observation):
                    filename = frame.f_code.co_filename
                    name = frame.f_code.co_name
                    if (
                        not state["root_called"]
                        and filename == rpc.__file__
                        and name == "_materialize_historical_anchor_stage"
                        and event == "call"
                    ):
                        state["root_called"] = True
                        state["root_original"] = getattr(rpc, name)
                        state["postchecks_before"] = observation[
                            "manifest_checks"
                        ]
                        state["dependency_original"] = (
                            rpc._validate_closed_plan
                        )
                        if drift:
                            original_dependency = state[
                                "dependency_original"
                            ]

                            def rebound_dependency(value):
                                state["dependency_calls"] += 1
                                if body_outcome is not None:
                                    raise body_outcome
                                return original_dependency(value)

                            rpc._validate_closed_plan = rebound_dependency
                            state["dependency_installed"] = True
                    elif (
                        not drift
                        and body_outcome is not None
                        and not state["ordinary_injected"]
                        and filename == rpc.__file__
                        and name == "_validate_closed_plan"
                        and event == "call"
                    ):
                        state["ordinary_injected"] = True
                        state["dependency_calls"] += 1
                        raise body_outcome
                    elif (
                        cleanup_outcome is not None
                        and not state["cleanup_called"]
                        and filename == storage.__file__
                        and name == "_cleanup_task4b_capture_staging"
                        and event == "return"
                    ):
                        state["cleanup_called"] = True
                        ledger = frame.f_locals.get("ledger")
                        cleanup_state = (
                            ledger.get("cleanup_state")
                            if type(ledger) is dict else None
                        )
                        state["cleanup_completed"] = (
                            type(cleanup_state) is dict
                            and cleanup_state.get("phase") == "done"
                        )
                        raise cleanup_outcome

                def before(_rpc, _scan, _storage, _observation):
                    def restore():
                        if state["dependency_installed"]:
                            rpc._validate_closed_plan = state[
                                "dependency_original"
                            ]

                    return restore

                observed = self._run_replay(
                    before_materialize=before, callback=callback
                )
                self.assertTrue(state["root_called"])
                self.assertIs(
                    rpc._materialize_historical_anchor_stage,
                    state["root_original"],
                )
                self.assertEqual(observed["payloads"], [])
                self.assertEqual(state["dependency_calls"], 1)
                if drift:
                    self.assertGreater(
                        observed["manifest_checks"],
                        state["postchecks_before"],
                    )
                if cleanup_outcome is not None:
                    self.assertTrue(state["cleanup_called"])
                    self.assertTrue(state["cleanup_completed"])
                if expected == "body_control":
                    self.assertIs(observed["error"], body_outcome)
                elif expected == "cleanup_control":
                    self.assertIs(observed["error"], cleanup_outcome)
                elif expected == "drift":
                    self.assertEqual(
                        observed["error_pair"],
                        ("authority_mismatch", "final_identity_drift"),
                    )
                else:
                    self.assertEqual(
                        observed["error_pair"],
                        (
                            "authority_mismatch",
                            "historical_window_reconciliation_mismatch",
                        ),
                    )
                escaped = observed["error"]
                if escaped is not None:
                    self.assertIsNone(escaped.__context__)
                    self.assertIsNone(escaped.__cause__)
                    escaped.__traceback__ = None
                self.assertEqual(observed["data_entries"], ())

    def test_slice5_source_buffer_phase_bounds(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        _headers, anchor_capture, lower_capture, _plan = _three_block_context()
        phase = {"active": None, "sequence": 0}
        anchor_counts = []
        anchor_capture_counts = []
        anchor_raw_at_lower = []
        lower_envelopes = {
            "capture": [],
            "reconciliation": [],
            "capture_replay": [],
        }
        window_envelopes = []
        storage_frames = []
        phase_boundaries = []

        def sequence(label, state):
            phase["sequence"] += 1
            phase_boundaries.append((phase["sequence"], label, state))

        def lower_path(frame):
            active = frame.f_back
            while active is not None:
                if active.f_code.co_filename == scan.__file__:
                    name = active.f_code.co_name
                    if name == "_capture_production_historical_window_core":
                        return "capture"
                    if name == "replay_all":
                        return "reconciliation"
                    if name == "_call_task4b_semantic_root":
                        return "capture_replay"
                active = active.f_back
            return None

        def observe_buffers(frame, event, _argument, _observation):
            filename = frame.f_code.co_filename
            name = frame.f_code.co_name
            if (
                filename == rpc.__file__
                and name == "project_historical_anchor_capture"
            ):
                if event == "call":
                    self.assertIsNone(phase["active"])
                    phase["active"] = "anchor"
                    responses = frame.f_locals.get("responses")
                    anchor_counts.append((
                        type(responses) is tuple,
                        len(responses) if type(responses) is tuple else None,
                    ))
                    sequence("anchor", "call")
                elif event == "return":
                    self.assertEqual(phase["active"], "anchor")
                    capture = _argument
                    inventory = (
                        capture.get("request_inventory")
                        if type(capture) is dict else None
                    )
                    anchor_capture_counts.append(
                        len(inventory) if type(inventory) is list else None
                    )
                    sequence("anchor", event)
                    phase["active"] = None
            elif (
                filename == scan.__file__
                and name == "project_historical_lower_bound_capture"
            ):
                if event == "call":
                    self.assertIsNone(phase["active"])
                    phase["active"] = "lower"
                    raw_container_ids = set()
                    active_frame = frame
                    while active_frame is not None:
                        for value in active_frame.f_locals.values():
                            if (
                                type(value) in (list, tuple)
                                and len(value) == 48
                                and all(
                                    type(row) is dict
                                    and set(row) == {
                                        "jsonrpc", "id", "result"
                                    }
                                    for row in value
                                )
                            ):
                                raw_container_ids.add(id(value))
                        active_frame = active_frame.f_back
                    if lower_path(frame) == "capture_replay":
                        anchor_raw_at_lower.append(
                            len(raw_container_ids)
                        )
                    sequence("lower", "call")
                elif event == "return":
                    self.assertEqual(phase["active"], "lower")
                    sequence("lower", event)
                    phase["active"] = None
            elif (
                filename == scan.__file__
                and name == "_project_lower_observation"
                and event == "call"
            ):
                self.assertEqual(phase["active"], "lower")
                path = lower_path(frame)
                projector = frame.f_back
                while projector is not None and not (
                    projector.f_code.co_filename == scan.__file__
                    and projector.f_code.co_name
                    == "project_historical_lower_bound_capture"
                ):
                    projector = projector.f_back
                probes = (
                    projector.f_locals.get("compact_probes")
                    if projector is not None else None
                )
                witness = (
                    projector.f_locals.get("compact_witness")
                    if projector is not None else None
                )
                live_raw_ids = set()
                active_frame = frame
                while active_frame is not None:
                    for value in active_frame.f_locals.values():
                        candidates = (
                            value
                            if type(value) in (list, tuple)
                            else (value,)
                        )
                        for candidate in candidates:
                            if (
                                type(candidate) is dict
                                and set(candidate) == {"request", "response"}
                                and type(candidate.get("request")) is dict
                                and type(candidate.get("response")) is dict
                            ):
                                live_raw_ids.add(id(candidate))
                    active_frame = active_frame.f_back
                if path is not None:
                    lower_envelopes[path].append((
                        len(live_raw_ids),
                        len(probes) if type(probes) is list else 0,
                        len(witness) if type(witness) is list else 0,
                    ))
            elif (
                filename == scan.__file__
                and name == "_project_complete_historical_window_root"
            ):
                if event == "call":
                    self.assertIsNone(phase["active"])
                    phase["active"] = "window"
                    responses = frame.f_locals.get("responses")
                    descriptor = frame.f_locals.get("descriptor")
                    live_jsonrpc_ids = set()
                    live_lower_ids = set()
                    active_frame = frame
                    while active_frame is not None:
                        for value in active_frame.f_locals.values():
                            candidates = (
                                value
                                if type(value) in (list, tuple)
                                else (value,)
                            )
                            for candidate in candidates:
                                if (
                                    type(candidate) is dict
                                    and set(candidate)
                                    == {"jsonrpc", "id", "result"}
                                ):
                                    live_jsonrpc_ids.add(id(candidate))
                                elif (
                                    type(candidate) is dict
                                    and set(candidate)
                                    == {"request", "response"}
                                    and type(candidate.get("request")) is dict
                                    and type(candidate.get("response")) is dict
                                ):
                                    live_lower_ids.add(id(candidate))
                        active_frame = active_frame.f_back
                    window_envelopes.append((
                        descriptor.get("kind")
                        if type(descriptor) is dict else None,
                        len(responses)
                        if type(responses) is tuple else None,
                        len(live_jsonrpc_ids),
                        len(live_lower_ids),
                    ))
                    sequence("window", "call")
                elif event == "return":
                    self.assertEqual(phase["active"], "window")
                    sequence("window", event)
                    phase["active"] = None
            if (
                filename == storage.__file__
                and name == "_task4b_next_replay_source_frame"
                and event == "return"
            ):
                builder = frame.f_locals.get("builder")
                builder_rows = frame.f_locals.get("builder_rows")
                if type(builder) is bytearray and type(builder_rows) is list:
                    storage_frames.append(
                        (len(builder), len(builder_rows), phase["active"])
                    )

        observed = self._run_replay(
            context_factory=_three_block_context,
            callback=observe_buffers,
            trace_mint_callback=True,
        )
        self.assertIsNone(observed["error_pair"])
        self.assertIsNone(observed["error"])
        self.assertEqual(observed["snapshot_allocations"], 1)
        self.assertIs(
            observed["snapshot_type"],
            storage.HistoricalRunStagingSnapshot,
        )
        roots = observed["post_roots"]
        self.assertTrue(roots)
        by_logical = {
            root["logical_batch_index"]: root for root in roots
        }
        source_rows = observed["source_rows"]
        logical_order = [row[0]["logical_batch_index"] for row in source_rows]
        self.assertEqual(logical_order, sorted(logical_order))
        for logical_index in set(logical_order):
            positions = [
                index for index, value in enumerate(logical_order)
                if value == logical_index
            ]
            self.assertEqual(
                positions, list(range(min(positions), max(positions) + 1))
            )

        counts = {}
        for compact, _request_size, _decoded_size, response_count in source_rows:
            self.assertIs(type(response_count), int)
            logical_index = compact["logical_batch_index"]
            counts[logical_index] = counts.get(logical_index, 0) + response_count
        self.assertTrue(anchor_counts)
        self.assertEqual(set(anchor_counts), {(True, 48)})
        self.assertEqual(set(anchor_capture_counts), {48})
        self.assertEqual(len(anchor_capture["request_inventory"]), 48)
        self.assertTrue(anchor_raw_at_lower)
        self.assertTrue(all(
            count == 0 for count in anchor_raw_at_lower
        ), anchor_raw_at_lower)
        lower_indices = {
            root["logical_batch_index"]
            for root in roots if root["segment"] == "lower_observation"
        }
        lower_response_ids = {
            response_id
            for compact, _request_size, _decoded_size, _count in source_rows
            if compact["logical_batch_index"] in lower_indices
            for response_id in compact["response_ids"]
        }
        lower_n = len(lower_capture["request_ids"])
        self.assertEqual(len(lower_response_ids), lower_n)
        self.assertTrue(1 <= lower_n <= 66)
        for path in ("capture", "reconciliation", "capture_replay"):
            points = lower_envelopes[path]
            self.assertTrue(points, path)
            self.assertEqual(len(points), lower_n, path)
            self.assertTrue(all(
                raw_count >= 1
                and compact_probes >= 0
                and compact_witness >= 0
                and raw_count + compact_probes + compact_witness <= lower_n
                for raw_count, compact_probes, compact_witness in points
            ), path)
        for root in roots:
            if root["segment"] == "window_root":
                self.assertIn(
                    counts[root["logical_batch_index"]], range(1, 41)
                )
        expected_window_envelopes = (
            ("header", 3), ("reserve", 6), ("price", 3),
            ("fee_history", 1), ("final_anchor", 1),
        )
        self.assertTrue(window_envelopes)
        self.assertEqual(
            len(window_envelopes) % len(expected_window_envelopes), 0
        )
        for start in range(
            0, len(window_envelopes), len(expected_window_envelopes)
        ):
            group = window_envelopes[
                start:start + len(expected_window_envelopes)
            ]
            self.assertEqual(
                tuple(
                    (kind, count)
                    for kind, count, _jsonrpc_count, _lower_count in group
                ),
                expected_window_envelopes,
            )
            self.assertTrue(all(
                jsonrpc_count == count and lower_count == 0
                for _kind, count, jsonrpc_count, lower_count in group
            ))
        self.assertEqual(len(storage_frames), len(source_rows))
        self.assertTrue(all(
            byte_count > 0
            and 0 < frame_count <= len(source_rows)
            and active_phase is None
            for byte_count, frame_count, active_phase in storage_frames
        ))
        self.assertEqual(observed["max_outstanding_source_frames"], 1)
        self.assertEqual(observed["outstanding_source_frames"], 0)
        self.assertEqual(observed["source_frame_state"], "empty")
        self.assertEqual(
            observed["source_frame_protocol_violations"], []
        )
        self.assertIsNone(phase["active"])
        active = None
        for _position, label, state in phase_boundaries:
            if state == "call":
                self.assertIsNone(active)
                active = label
            else:
                self.assertEqual(active, label)
                active = None
        self.assertIsNone(active)
        root_consumes = {
            action[2]
            for action in observed["actions"]
            if action[:2] == ("consume", "root")
        }
        self.assertEqual(root_consumes, set(by_logical))
        self.assertIn("raw", observed["live_data_entries"])
        self.assertFalse(any(
            name.startswith(".historical-foundry-exchange-spool-")
            and name.endswith(".bin")
            for name in observed["live_data_entries"]
        ))
        self.assertEqual(observed["data_entries"], ())


class HistoricalFoundryScanTask4bSurfaceTests(unittest.TestCase):
    _CONTRACTS_CALLABLE_NAMES = (
        "_next_base_fee", "_nonnegative_int", "_positive_int",
        "next_historical_base_fee",
    )
    _RPC_CALLABLE_NAMES = (
        "_abi_string", "_address_argument", "_address_word",
        "_allowance_calldata", "_anchor_bindings", "_anchor_projection",
        "_balance_calldata", "_binding", "_build_closed_plan", "_call",
        "_canonical_bytes", "_copy_json", "_derived_bindings",
        "_derived_templates", "_feed_projection", "_fixed_templates",
        "_guard_exact_json", "_hash32", "_hex_bytes", "_inventory",
        "_latest_round_projection", "_materialize_historical_anchor_stage",
        "_project_capture", "_quantity", "_require_derived_authority_addresses",
        "_resolve_template", "_resource_error", "_runtime_projection",
        "_stage_identity", "_template", "_token_projection", "_typed_hash",
        "_uint_word", "_validate_closed_plan",
        "_validate_historical_anchor_capture", "_validate_success_rows",
        "_venue_projection", "_zero_word", "build_factory_get_pair_calldata",
        "keccak256", "project_historical_anchor_capture",
        "solidity_allowance_storage_key", "solidity_balance_storage_key",
    )
    _SCAN_CALLABLE_NAMES = (
        "_anchor_state_authority", "_build_historical_block_header_request",
        "_cached_decimal_projection", "_canonical_hash_value",
        "_canonical_json_bytes", "_captured_failure_pair",
        "_coefficient_from_digits", "_decode_price_result",
        "_descriptor_root_failure_pair", "_expected_descriptor", "_failure",
        "_fee_quantity_list", "_frame", "_guard_historical_json_value",
        "_header_descriptor_rows", "_header_from_inventory_row",
        "_header_hash_at", "_header_root_count", "_header_row_from_projection",
        "_hex_payload", "_historical_json_int_token_bytes", "_inventory_hasher",
        "_inventory_update", "_iterator_once", "_make_descriptor",
        "_next_input", "_normalized_anchor_from_capture", "_normalized_from_raw",
        "_parse_quantity", "_preflight_historical_decimal_tuple",
        "_project_complete_historical_window_root", "_project_fee_root",
        "_project_final_anchor_root", "_project_header_root",
        "_project_historical_block_header_success", "_project_lower_observation",
        "_project_price_root", "_project_reserve_root", "_ratio_decimal_token",
        "_require_hash32", "_require_hash64", "_require_raw_json_containers",
        "_require_uint", "_response_result", "_root_header_rows",
        "_state_descriptor_rows", "_typed_hash", "_validate_anchor_capture",
        "_validate_compact_observation", "_validate_descriptor",
        "_validate_header_inventory", "_validate_historical_anchor_capture",
        "_validate_lower_capture", "_validate_normalized_header",
        "_validate_plan_shape", "_verify_decimal_layout",
        "build_historical_window_request_plan",
        "iter_historical_header_request_batches",
        "iter_historical_state_request_batches", "next_historical_base_fee",
        "project_historical_lower_bound_capture",
    )
    _ROUTE_CALLABLE_NAMES = (
        "_abi_address_word", "_address", "_exact_int", "_keccak_f1600",
        "_pad_address", "_pad_slot", "_rotate_left_64", "_uint256",
        "build_factory_get_pair_calldata", "keccak256",
        "solidity_allowance_storage_key", "solidity_balance_storage_key",
    )
    _CLASS_CALLABLE_BINDINGS = (
        ("scan", "Decimal"),
        ("scan", "HistoricalWindowProjectionError"),
        ("scan", "MappingProxyType"),
        ("scan", "_ArchiveRpcError"),
        ("scan", "_OneShotIterator"),
        ("route", "RouteCostEvidenceError"),
    )
    _MODULE_ATTRIBUTE_CALLABLES = (
        ("rpc", "hashlib", "sha256"),
        ("rpc", "json", "dumps"),
        ("rpc", "json", "loads"),
        ("scan", "hashlib", "sha256"),
        ("scan", "json", "dumps"),
        ("scan", "platform", "python_implementation"),
    )
    _CLASS_SURFACE_NAMES = (
        ("scan", "_OneShotCursor"),
        ("scan", "_OneShotCursor.__init__"),
        ("scan", "_OneShotCursor.__iter__"),
        ("scan", "_OneShotCursor.__next__"),
        ("scan", "_OneShotIterator.__init__"),
        ("scan", "_OneShotIterator.__iter__"),
        ("scan", "_OneShotIterator.__next__"),
        ("scan", "_OneShotIterator._advance"),
        ("scan", "HistoricalWindowProjectionError.__init__"),
    )
    _RPC_CONSTANT_NAMES = (
        "_ADDRESS", "_AGGREGATOR", "_ALLOWANCE", "_ANCHOR_RESULT_FIELDS",
        "_BALANCE_OF", "_CAPTURE_FIELDS", "_CAPTURE_SCHEMA", "_DECIMALS",
        "_DESCRIPTION", "_EXECUTOR", "_FACTORY", "_FEED_PROXY",
        "_FIXED_AUTHORITY_ADDRESSES", "_HASH32", "_HEX_BYTES",
        "_INVENTORY_FIELDS", "_LATEST_ROUND", "_MAX_ABI_BYTES",
        "_MAX_JSON_NODES", "_MAX_NESTING_DEPTH", "_MAX_ORDINARY_STRING_BYTES",
        "_MAX_RUNTIME_BYTES", "_MAX_SCALAR_BYTES", "_PARAMS_HASH_DOMAIN",
        "_PHASE", "_PLAN_FIELDS", "_PLAN_SCHEMA", "_QUANTITY",
        "_REQUEST_HASH_DOMAIN", "_RESPONSE_FIELDS", "_RESPONSE_HASH_DOMAIN",
        "_RESULT_HASH_DOMAIN", "_SENDER", "_STAGE_FIELDS", "_TEMPLATE_FIELDS",
        "_TOKEN0", "_TOKEN1", "_UNI", "_VENUES", "_WETH", "_WETH_GETTER",
        "_WIRE_FIELDS",
    )
    _SCAN_CONSTANT_NAMES = (
        "_ACTIVE_HEADER_VALIDATION", "_ADDRESS", "_ANCHOR_CAPTURE_DOMAIN",
        "_DECIMAL_LAYOUT_VERIFIED", "_DESCRIPTOR_FIELDS", "_END_OF_INPUT",
        "_ERROR_PAIRS", "_FEE_INVENTORY_DOMAIN", "_FINAL_ANCHOR_DOMAIN",
        "_GET_RESERVES_SELECTOR", "_HASH32", "_HASH64",
        "_HEADER_INVENTORY_DOMAIN", "_HEADER_INVENTORY_FIELDS",
        "_HEADER_ROW_FIELDS", "_LATEST_ROUND_SELECTOR", "_LOOKBACK_SECONDS",
        "_LOWER_CAPTURE_DOMAIN", "_LOWER_FIELDS", "_MAX_BLOCK_COUNT",
        "_MAX_DEPTH", "_MAX_JSON_NODES", "_MAX_NUMERIC_TOKEN_BYTES",
        "_MAX_RATIO_DECIMAL_OBJECT_BYTES", "_MAX_RATIO_TOKEN_BYTES",
        "_MAX_SCALAR_BYTES", "_MAX_STRING_BYTES", "_MAX_UINT112",
        "_MAX_UINT256", "_MAX_UINT64", "_MAX_UINT80",
        "_NEGATIVE_JSON_INT_MAGNITUDE_EXCLUSIVE",
        "_NONNEGATIVE_JSON_INT_EXCLUSIVE", "_NORMALIZED_HEADER_DOMAIN",
        "_NORMALIZED_HEADER_FIELDS", "_OBSERVATION_FIELDS", "_PLAN_FIELDS",
        "_PRICE_INVENTORY_DOMAIN", "_QUANTITY", "_RAW_HEADER_FIELDS",
        "_REQUEST_DOMAIN", "_RESERVE_INVENTORY_DOMAIN", "_RESPONSE_DOMAIN",
        "_RESULT_DOMAIN", "_ROOT_BATCH_POLICY", "_SUCCESS_FIELDS",
        "_VENUE_ORDER", "_WIRE_FIELDS",
    )
    _ROUTE_CONSTANT_NAMES = (
        "_ADDRESS", "_KECCAK_ROTATION", "_KECCAK_ROUND_CONSTANTS", "_MASK64",
    )

    @staticmethod
    def _nested_load_globals(code):
        names = []
        for instruction in dis.get_instructions(code):
            if instruction.opname == "LOAD_GLOBAL":
                names.append(instruction.argval)
        for value in code.co_consts:
            if type(value) is types.CodeType:
                names.extend(
                    HistoricalFoundryScanTask4bSurfaceTests
                    ._nested_load_globals(value)
                )
        return tuple(names)

    @staticmethod
    def _nested_instructions(code):
        rows = [tuple(dis.get_instructions(code))]
        for value in code.co_consts:
            if type(value) is types.CodeType:
                rows.extend(
                    HistoricalFoundryScanTask4bSurfaceTests
                    ._nested_instructions(value)
                )
        return tuple(rows)

    def _walk_semantic_bindings(self):
        scan = importlib.import_module("scripts.historical_foundry_scan")
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        contracts = importlib.import_module("scripts.historical_foundry_contracts")
        route = importlib.import_module("scripts.route_cost_evidence")
        modules = {
            "scan": scan, "rpc": rpc, "contracts": contracts, "route": route,
        }
        role_by_name = {module.__name__: role for role, module in modules.items()}
        roots = (
            ("rpc", "_materialize_historical_anchor_stage"),
            ("rpc", "project_historical_anchor_capture"),
            ("scan", "project_historical_lower_bound_capture"),
            ("scan", "build_historical_window_request_plan"),
            ("scan", "iter_historical_header_request_batches"),
            ("scan", "iter_historical_state_request_batches"),
            ("scan", "_project_complete_historical_window_root"),
        )
        pending = [(role, name, getattr(modules[role], name)) for role, name in roots]
        walked = set()
        function_bindings = set()
        class_callables = set()
        module_attributes = set()
        constants = set()
        while pending:
            binding_role, binding_name, function = pending.pop()
            function_bindings.add((binding_role, binding_name))
            function_key = id(function)
            if function_key in walked:
                continue
            walked.add(function_key)
            self.assertIs(type(function), types.FunctionType)
            globals_role = role_by_name[function.__globals__["__name__"]]
            function_bindings.add((globals_role, function.__name__))
            for name in self._nested_load_globals(function.__code__):
                if name not in function.__globals__:
                    continue
                value = function.__globals__[name]
                if type(value) is types.FunctionType and value.__module__ in role_by_name:
                    function_bindings.add((globals_role, name))
                    pending.append((globals_role, name, value))
                elif isinstance(value, types.ModuleType):
                    continue
                elif callable(value):
                    class_callables.add((globals_role, name))
                else:
                    constants.add((globals_role, name))
            for instructions in self._nested_instructions(function.__code__):
                for index, instruction in enumerate(instructions[:-1]):
                    if instruction.opname != "LOAD_GLOBAL":
                        continue
                    value = function.__globals__.get(instruction.argval)
                    following = instructions[index + 1]
                    if (
                        isinstance(value, types.ModuleType)
                        and following.opname in ("LOAD_ATTR", "LOAD_METHOD")
                        and callable(getattr(value, following.argval, None))
                    ):
                        module_attributes.add(
                            (globals_role, instruction.argval, following.argval)
                        )
        return roots, function_bindings, class_callables, module_attributes, constants

    @staticmethod
    def _private_replay_closure_values(scan):
        values = []
        pending = [
            scan._replay_production_historical_window_capture_from_bound_storage
        ]
        walked = set()
        while pending:
            value = pending.pop()
            if id(value) in walked:
                continue
            walked.add(id(value))
            if type(value) is types.FunctionType:
                for cell in value.__closure__ or ():
                    nested = cell.cell_contents
                    values.append(nested)
                    pending.append(nested)
        return tuple(values)

    @staticmethod
    def _unique_private_closure_value(scan, target_name):
        replay = (
            scan._replay_production_historical_window_capture_from_bound_storage
        )
        pending = [replay]
        walked = set()
        values = {}
        while pending:
            function = pending.pop()
            if type(function) is not types.FunctionType or id(function) in walked:
                continue
            walked.add(id(function))
            for name, cell in zip(
                function.__code__.co_freevars, function.__closure__ or (),
            ):
                value = cell.cell_contents
                if name == target_name:
                    values[id(value)] = value
                if type(value) is types.FunctionType:
                    pending.append(value)
        if len(values) != 1:
            raise AssertionError((target_name, len(values)))
        return next(iter(values.values()))

    def test_task4b_scan_surface_tuple_is_exact(self):
        import scripts.historical_foundry_scan as scan

        expected_names = (
            "_materialize_historical_window_staging_snapshot",
            "_ProductionHistoricalWindowCaptureReplayEvent",
            "_bind_production_historical_window_capture_replay_source_from_bound_storage",
            "_replay_production_historical_window_capture_from_bound_storage",
            "_consume_production_historical_window_capture_replay_event_for_storage",
        )
        self.assertEqual(scan._TASK4B_SCAN_LOCAL_SURFACE_NAMES, expected_names)
        self.assertEqual(len(scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS), 5)
        self.assertEqual(
            tuple(inspect.signature(
                scan._materialize_historical_window_staging_snapshot
            ).parameters),
            ("capability",),
        )
        self.assertEqual(
            tuple(inspect.signature(
                scan._bind_production_historical_window_capture_replay_source_from_bound_storage
            ).parameters),
            ("reconciliation", "source"),
        )
        self.assertEqual(
            tuple(inspect.signature(
                scan._replay_production_historical_window_capture_from_bound_storage
            ).parameters),
            ("source",),
        )
        self.assertEqual(
            tuple(inspect.signature(
                scan._consume_production_historical_window_capture_replay_event_for_storage
            ).parameters),
            ("event", "expected_source", "expected_event_index"),
        )
        for function in (
            scan._materialize_historical_window_staging_snapshot,
            scan._bind_production_historical_window_capture_replay_source_from_bound_storage,
            scan._replay_production_historical_window_capture_from_bound_storage,
            scan._consume_production_historical_window_capture_replay_event_for_storage,
        ):
            self.assertTrue(all(
                parameter.kind is inspect.Parameter.KEYWORD_ONLY
                for parameter in inspect.signature(function).parameters.values()
            ))
        event_signature = inspect.signature(
            scan._ProductionHistoricalWindowCaptureReplayEvent
        )
        self.assertEqual(tuple(event_signature.parameters), ("args", "kwargs"))
        self.assertEqual(
            tuple(
                parameter.kind
                for parameter in event_signature.parameters.values()
            ),
            (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ),
        )
        with self.assertRaises(scan.HistoricalWindowProjectionError):
            scan._ProductionHistoricalWindowCaptureReplayEvent()
        with self.assertRaises(TypeError):
            type(
                "ForbiddenReplayEvent",
                (scan._ProductionHistoricalWindowCaptureReplayEvent,),
                {},
            )
        clone = object.__new__(
            scan._ProductionHistoricalWindowCaptureReplayEvent
        )
        self.assertFalse(hasattr(clone, "__dict__"))
        self.assertEqual(
            repr(clone),
            "_ProductionHistoricalWindowCaptureReplayEvent(<redacted>)",
        )
        with self.assertRaises(TypeError):
            copy.copy(clone)
        with self.assertRaises(TypeError):
            copy.deepcopy(clone)
        with self.assertRaises(TypeError):
            pickle.dumps(clone)

    def test_task4b_cross_module_tuple_is_exact(self):
        import scripts.historical_foundry_storage as storage

        self.assertEqual(
            storage._TASK4B_BOUND_OBJECT_NAMES,
            (
                ("scan", "_ProductionHistoricalWindowCaptureReplayEvent"),
                ("scan", "_bind_production_historical_window_capture_replay_source_from_bound_storage"),
                ("scan", "_replay_production_historical_window_capture_from_bound_storage"),
                ("scan", "_consume_production_historical_window_capture_replay_event_for_storage"),
                ("storage", "_HistoricalWindowCaptureReplaySource"),
                ("storage", "_HistoricalWindowCaptureReplaySource.__enter__"),
                ("storage", "_HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan"),
                ("storage", "_HistoricalWindowCaptureReplaySource.__iter__"),
                ("storage", "_HistoricalWindowCaptureReplaySource.__next__"),
                ("storage", "_HistoricalWindowCaptureReplaySource.__exit__"),
                ("storage", "_HistoricalWindowCaptureReplaySource.close"),
                ("storage", "_ConsumedProductionHistoricalWindowCapabilityView._materialize_staging_snapshot_from_bound_scan"),
            ),
        )

    def test_task4b_manifest_is_not_exported(self):
        import scripts.historical_foundry_scan as scan

        self.assertFalse(hasattr(scan, "_TASK4B_SEMANTIC_DEPENDENCY_MANIFEST"))

    def test_task4b_replay_is_only_module_callable_with_driver_cells(self):
        import scripts.historical_foundry_scan as scan

        replay = (
            scan._replay_production_historical_window_capture_from_bound_storage
        )
        replay_cells = dict(zip(
            replay.__code__.co_freevars, replay.__closure__ or (),
        ))
        self.assertEqual(
            set(replay_cells), {"new_header_driver", "new_window_driver"}
        )
        constructors = {
            cell.cell_contents for cell in replay_cells.values()
        }
        self.assertEqual(len(constructors), 2)
        holders = []
        for name, value in vars(scan).items():
            if type(value) is not types.FunctionType:
                continue
            direct = tuple(
                cell.cell_contents for cell in value.__closure__ or ()
            )
            if any(
                cell_value is constructor
                for cell_value in direct
                for constructor in constructors
            ):
                holders.append(name)
        self.assertEqual(
            holders,
            ["_replay_production_historical_window_capture_from_bound_storage"],
        )
        with self.assertRaises(scan._ArchiveRpcError) as rejected:
            replay(source=object())
        self.assertEqual(
            (rejected.exception.reason_code, rejected.exception.failure_kind),
            (
                "authority_mismatch",
                "historical_window_capability_invalid",
            ),
        )

    def test_task4b_nested_load_global_walk_matches_literal_groups(self):
        (
            roots, callables, classes, module_calls, constants,
        ) = self._walk_semantic_bindings()
        self.assertEqual(len(roots), 7)
        self.assertEqual(
            callables,
            set(("contracts", name) for name in self._CONTRACTS_CALLABLE_NAMES)
            | set(("rpc", name) for name in self._RPC_CALLABLE_NAMES)
            | set(("scan", name) for name in self._SCAN_CALLABLE_NAMES)
            | set(("route", name) for name in self._ROUTE_CALLABLE_NAMES),
        )
        self.assertEqual(classes, set(self._CLASS_CALLABLE_BINDINGS))
        self.assertEqual(module_calls, set(self._MODULE_ATTRIBUTE_CALLABLES))
        self.assertEqual(
            constants,
            set(("rpc", name) for name in self._RPC_CONSTANT_NAMES)
            | set(("scan", name) for name in self._SCAN_CONSTANT_NAMES)
            | set(("route", name) for name in self._ROUTE_CONSTANT_NAMES),
        )

    def test_task4b_replay_closure_contains_exact_private_manifest(self):
        import scripts.historical_foundry_scan as scan

        closure_values = self._private_replay_closure_values(scan)
        manifests = [
            value for value in closure_values
            if (
                type(value) is tuple
                and len(value) == 5
                and all(type(group) is tuple for group in value)
                and len(value[0]) == 7
                and len(value[1]) == 120
                and len(value[2]) == 6
                and len(value[3]) == 6
                and len(value[4]) == 94
            )
        ]
        self.assertEqual(len(manifests), 1)
        roots, callables, classes, module_calls, constants = manifests[0]
        self.assertEqual(len(roots), 7)
        self.assertEqual(len(callables), 120)
        self.assertEqual(len(classes), 6)
        self.assertEqual(len(module_calls), 6)
        self.assertEqual(len(constants), 94)
        self.assertEqual(
            set((row[0], row[1]) for row in callables),
            set(("contracts", name) for name in self._CONTRACTS_CALLABLE_NAMES)
            | set(("rpc", name) for name in self._RPC_CALLABLE_NAMES)
            | set(("scan", name) for name in self._SCAN_CALLABLE_NAMES)
            | set(("route", name) for name in self._ROUTE_CALLABLE_NAMES),
        )
        self.assertEqual(
            set((row[0], row[1]) for row in classes),
            set(self._CLASS_CALLABLE_BINDINGS),
        )
        self.assertEqual(
            set((row[0], row[1], row[2]) for row in module_calls),
            set(self._MODULE_ATTRIBUTE_CALLABLES),
        )
        self.assertEqual(
            set((row[0], row[1]) for row in constants),
            set(("rpc", name) for name in self._RPC_CONSTANT_NAMES)
            | set(("scan", name) for name in self._SCAN_CONSTANT_NAMES)
            | set(("route", name) for name in self._ROUTE_CONSTANT_NAMES),
        )
        self.assertEqual(
            tuple(row[:2] for row in roots),
            (
                ("rpc", "_materialize_historical_anchor_stage"),
                ("rpc", "project_historical_anchor_capture"),
                ("scan", "project_historical_lower_bound_capture"),
                ("scan", "build_historical_window_request_plan"),
                ("scan", "iter_historical_header_request_batches"),
                ("scan", "iter_historical_state_request_batches"),
                ("scan", "_project_complete_historical_window_root"),
            ),
        )

        class_surfaces = [
            value for value in closure_values
            if type(value) is tuple
            and len(value) == len(self._CLASS_SURFACE_NAMES)
            and all(type(row) is tuple and len(row) == 3 for row in value)
            and tuple(row[:2] for row in value) == self._CLASS_SURFACE_NAMES
        ]
        self.assertEqual(len(class_surfaces), 1)
        modules = [
            value for value in closure_values
            if type(value) is tuple and tuple(
                row[:2] for row in value
                if type(row) is tuple and len(row) == 3
            ) == (
                ("scan", "scripts.historical_foundry_scan"),
                ("rpc", "scripts.historical_foundry_rpc"),
                ("contracts", "scripts.historical_foundry_contracts"),
                ("route", "scripts.route_cost_evidence"),
            )
        ]
        unique_modules = {id(value): value for value in modules}
        self.assertEqual(len(unique_modules), 1)
        module_rows = next(iter(unique_modules.values()))
        for role, canonical_name, original_module in module_rows:
            del role
            self.assertIs(sys.modules[canonical_name], original_module)

        regex_names = {
            ("rpc", "_ADDRESS"), ("rpc", "_HASH32"),
            ("rpc", "_HEX_BYTES"), ("rpc", "_QUANTITY"),
            ("scan", "_ADDRESS"), ("scan", "_HASH32"),
            ("scan", "_HASH64"), ("scan", "_QUANTITY"),
            ("route", "_ADDRESS"),
        }
        module_by_role = {row[0]: row[2] for row in module_rows}
        for role, name, original, projection in constants:
            current = getattr(module_by_role[role], name)
            self.assertIs(current, original)
            if (role, name) in regex_names:
                self.assertEqual(
                    projection,
                    ("regex", type(current), current.pattern, current.flags),
                )
            elif (role, name) == ("scan", "_ROOT_BATCH_POLICY"):
                self.assertEqual(
                    projection,
                    (
                        "dict", dict,
                        (
                            ("fee_blocks", 1024),
                            ("header_requests", 40),
                            ("price_requests", 40),
                            ("reserve_blocks", 20),
                        ),
                    ),
                )
            elif (role, name) == ("scan", "_ACTIVE_HEADER_VALIDATION"):
                self.assertEqual(projection, ("contextvar", type(current)))
                self.assertIsNone(current.get())
            elif (role, name) == ("scan", "_END_OF_INPUT"):
                self.assertEqual(projection, ("identity_only", type(current)))
            else:
                self.assertEqual(projection, ("value", type(current), current))
        self.assertIs(scan._DECIMAL_LAYOUT_VERIFIED, True)

    def test_task4b_private_manifest_verifier_detects_in_place_drift(self):
        import scripts.historical_foundry_scan as scan

        closure_values = self._private_replay_closure_values(scan)
        verifiers = [
            value for value in closure_values
            if type(value) is types.FunctionType
            and value.__name__ == "_verify_task4b_semantic_dependency_manifest"
        ]
        unique_verifiers = {id(value): value for value in verifiers}
        self.assertEqual(len(unique_verifiers), 1)
        verifier = next(iter(unique_verifiers.values()))
        self.assertIsNone(verifier())

        original_next = scan._OneShotIterator.__next__
        try:
            scan._OneShotIterator.__next__ = lambda self: next(iter(()))
            with self.assertRaises(scan.HistoricalWindowProjectionError) as caught:
                verifier()
        finally:
            scan._OneShotIterator.__next__ = original_next
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "final_identity_drift"),
        )

        original_policy = dict(scan._ROOT_BATCH_POLICY)
        try:
            scan._ROOT_BATCH_POLICY["header_requests"] = 41
            with self.assertRaises(scan.HistoricalWindowProjectionError) as caught:
                verifier()
        finally:
            scan._ROOT_BATCH_POLICY.clear()
            scan._ROOT_BATCH_POLICY.update(original_policy)
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "final_identity_drift"),
        )

        token = scan._ACTIVE_HEADER_VALIDATION.set((object(), object()))
        try:
            with self.assertRaises(scan.HistoricalWindowProjectionError) as caught:
                verifier()
        finally:
            scan._ACTIVE_HEADER_VALIDATION.reset(token)
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "final_identity_drift"),
        )

    def test_task4b_incremental_driver_protocol_matches_legacy_aggregates(self):
        import scripts.historical_foundry_scan as scan

        replay = (
            scan._replay_production_historical_window_capture_from_bound_storage
        )
        replay_cells = {
            name: cell.cell_contents for name, cell in zip(
                replay.__code__.co_freevars, replay.__closure__ or (),
            )
        }
        header_constructor = replay_cells["new_header_driver"]
        window_constructor = replay_cells["new_window_driver"]
        self.assertNotIn(
            "authenticated", inspect.signature(header_constructor).parameters
        )
        self.assertNotIn(
            "authenticated", inspect.signature(window_constructor).parameters
        )
        driver_ack = self._unique_private_closure_value(scan, "driver_ack")
        driver_expect_eof = self._unique_private_closure_value(
            scan, "driver_expect_eof"
        )
        driver_eof = self._unique_private_closure_value(scan, "driver_eof")
        header_declaration = self._unique_private_closure_value(
            scan, "header_declaration"
        )
        window_declaration = self._unique_private_closure_value(
            scan, "window_declaration"
        )

        headers, capture, lower, plan = _small_context()
        header_descriptors = tuple(iter_historical_header_request_batches(plan))
        header_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in header_descriptors
        )
        expected_inventory = header_declaration(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            batch_results=iter(header_pairs),
        )
        driver = header_constructor(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
        )
        self.assertEqual(next(driver), header_descriptors[0])
        root = driver.send(header_pairs[0])
        self.assertEqual(root["typed_role"], "headers")
        self.assertIs(driver.send(driver_ack), driver_expect_eof)
        with self.assertRaises(StopIteration) as completed:
            driver.send(driver_eof)
        self.assertEqual(completed.exception.value, expected_inventory)

        state_descriptors = tuple(iter_historical_state_request_batches(
            plan=plan, header_inventory=expected_inventory,
        ))
        state_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in state_descriptors
        )
        expected_projection = window_declaration(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            header_inventory=expected_inventory,
            batch_results=iter(state_pairs),
        )
        driver = window_constructor(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            header_inventory=expected_inventory,
        )
        for index, pair in enumerate(state_pairs):
            self.assertEqual(next(driver) if index == 0 else request, pair[0])
            root = driver.send(pair)
            self.assertIsNone(scan._ACTIVE_HEADER_VALIDATION.get())
            request = driver.send(driver_ack)
        self.assertIs(request, driver_expect_eof)
        with self.assertRaises(StopIteration) as completed:
            driver.send(driver_eof)
        self.assertEqual(completed.exception.value, expected_projection)

    def test_task4b_drivers_reject_protocol_misuse_and_preserve_controls(self):
        import scripts.historical_foundry_scan as scan

        replay = (
            scan._replay_production_historical_window_capture_from_bound_storage
        )
        replay_cells = {
            name: cell.cell_contents for name, cell in zip(
                replay.__code__.co_freevars, replay.__closure__ or (),
            )
        }
        header_constructor = replay_cells["new_header_driver"]
        window_constructor = replay_cells["new_window_driver"]
        driver_ack = self._unique_private_closure_value(scan, "driver_ack")
        driver_expect_eof = self._unique_private_closure_value(
            scan, "driver_expect_eof"
        )
        driver_eof = self._unique_private_closure_value(scan, "driver_eof")

        headers, capture, lower, plan = _small_context()
        header_descriptor = next(iter_historical_header_request_batches(plan))
        header_pair = (
            header_descriptor,
            _responses_for_descriptor(header_descriptor, headers.__getitem__),
        )
        header_arguments = {
            "plan": plan,
            "anchor_capture": capture,
            "lower_bound_capture": lower,
        }
        expected_pair = (
            "block_coverage_incomplete", "header_coverage_invalid"
        )

        driver = header_constructor(**header_arguments)
        self.assertEqual(next(driver), header_descriptor)
        with self.assertRaises(HistoricalWindowProjectionError) as early_eof:
            driver.send(driver_eof)
        self.assertEqual(
            (early_eof.exception.reason_code, early_eof.exception.failure_kind),
            expected_pair,
        )

        driver = header_constructor(**header_arguments)
        self.assertEqual(next(driver), header_descriptor)
        with self.assertRaises(HistoricalWindowProjectionError) as repeated_step:
            next(driver)
        self.assertEqual(
            (
                repeated_step.exception.reason_code,
                repeated_step.exception.failure_kind,
            ),
            expected_pair,
        )

        driver = header_constructor(**header_arguments)
        self.assertEqual(next(driver), header_descriptor)
        driver.send(header_pair)
        with self.assertRaises(HistoricalWindowProjectionError) as wrong_ack:
            driver.send(object())
        self.assertEqual(
            (wrong_ack.exception.reason_code, wrong_ack.exception.failure_kind),
            expected_pair,
        )

        driver = header_constructor(**header_arguments)
        self.assertEqual(next(driver), header_descriptor)
        driver.send(header_pair)
        self.assertIs(driver.send(driver_ack), driver_expect_eof)
        with self.assertRaises(HistoricalWindowProjectionError) as wrong_eof:
            driver.send(driver_ack)
        self.assertEqual(
            (wrong_eof.exception.reason_code, wrong_eof.exception.failure_kind),
            expected_pair,
        )

        driver = header_constructor(**header_arguments)
        self.assertEqual(next(driver), header_descriptor)
        driver.send(header_pair)
        self.assertIs(driver.send(driver_ack), driver_expect_eof)
        with self.assertRaises(StopIteration) as completed:
            driver.send(driver_eof)
        self.assertEqual(completed.exception.value["row_count"], 2)
        with self.assertRaises(StopIteration):
            driver.send(driver_eof)

        inventory = scan.project_historical_header_inventory(
            **header_arguments,
            batch_results=iter((header_pair,)),
        )
        constructor_rows = (
            (header_constructor, header_arguments),
            (
                window_constructor,
                dict(header_arguments, header_inventory=inventory),
            ),
        )
        for constructor, arguments in constructor_rows:
            for control_type in (
                KeyboardInterrupt,
                SystemExit,
                GeneratorExit,
                asyncio.CancelledError,
            ):
                with self.subTest(
                    driver=constructor.__name__, control=control_type.__name__
                ):
                    driver = constructor(**arguments)
                    next(driver)
                    control = control_type("driver-control-identity")
                    with self.assertRaises(BaseException) as caught:
                        driver.throw(control)
                    self.assertIs(caught.exception, control)

    def test_task4b_drivers_release_roots_and_drains_do_not_prefetch(self):
        import scripts.historical_foundry_scan as scan

        replay = (
            scan._replay_production_historical_window_capture_from_bound_storage
        )
        replay_cells = {
            name: cell.cell_contents for name, cell in zip(
                replay.__code__.co_freevars, replay.__closure__ or (),
            )
        }
        header_constructor = replay_cells["new_header_driver"]
        window_constructor = replay_cells["new_window_driver"]
        driver_ack = self._unique_private_closure_value(scan, "driver_ack")
        driver_eof = self._unique_private_closure_value(scan, "driver_eof")

        headers, capture, lower, plan = _small_context()
        header_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in iter_historical_header_request_batches(plan)
        )
        inventory = scan.project_historical_header_inventory(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            batch_results=iter(header_pairs),
        )
        state_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in iter_historical_state_request_batches(
                plan=plan, header_inventory=inventory,
            )
        )

        driver_rows = (
            (
                header_constructor,
                {
                    "plan": plan,
                    "anchor_capture": capture,
                    "lower_bound_capture": lower,
                },
                header_pairs[0],
            ),
            (
                window_constructor,
                {
                    "plan": plan,
                    "anchor_capture": capture,
                    "lower_bound_capture": lower,
                    "header_inventory": inventory,
                },
                state_pairs[0],
            ),
        )
        for constructor, arguments, pair in driver_rows:
            with self.subTest(driver=constructor.__name__):
                driver = constructor(**arguments)
                self.assertEqual(next(driver), pair[0])
                root = driver.send(pair)
                self.assertIs(driver.gi_frame.f_locals["root"], root)
                driver.send(driver_ack)
                self.assertNotIn("root", driver.gi_frame.f_locals)
                self.assertNotIn("raw_pair", driver.gi_frame.f_locals)
                driver.close()

        class CountingResults:
            def __init__(self, pairs):
                self._pairs = iter(pairs)
                self.iter_calls = 0
                self.next_calls = 0

            def __iter__(self):
                self.iter_calls += 1
                return self

            def __next__(self):
                self.next_calls += 1
                return next(self._pairs)

        source = CountingResults(state_pairs)
        projected_next_counts = []
        original_projector = scan._project_complete_historical_window_root

        def observing_projector(
            *, plan, descriptor, responses, header_inventory
        ):
            projected_next_counts.append(source.next_calls)
            return original_projector(
                plan=plan,
                descriptor=descriptor,
                responses=responses,
                header_inventory=header_inventory,
            )

        try:
            scan._project_complete_historical_window_root = observing_projector
            projection = scan.project_historical_window_projection(
                plan=plan,
                anchor_capture=capture,
                lower_bound_capture=lower,
                header_inventory=inventory,
                batch_results=source,
            )
        finally:
            scan._project_complete_historical_window_root = original_projector
        self.assertEqual(
            projected_next_counts,
            list(range(1, len(state_pairs) + 1)),
        )
        self.assertEqual(source.iter_calls, 1)
        self.assertEqual(source.next_calls, len(state_pairs) + 1)
        self.assertEqual(projection["coverage"]["header_count"], 2)

    def test_task4b_public_drains_release_raw_pair_before_next_input(self):
        import scripts.historical_foundry_scan as scan

        headers, capture, lower, plan = _small_context()
        header_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in iter_historical_header_request_batches(plan)
        )
        observations = []
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name == "_next_input"
                and event == "call"
                and frame.f_back is not None
                and frame.f_back.f_code.co_name
                in ("header_drain", "window_drain")
            ):
                caller = frame.f_back
                observations.append((
                    caller.f_code.co_name,
                    "raw_pair" in caller.f_locals,
                ))
            return tracer

        try:
            sys.settrace(tracer)
            inventory = scan.project_historical_header_inventory(
                plan=plan,
                anchor_capture=capture,
                lower_bound_capture=lower,
                batch_results=iter(header_pairs),
            )
            state_pairs = tuple(
                (
                    descriptor,
                    _responses_for_descriptor(
                        descriptor, headers.__getitem__
                    ),
                )
                for descriptor in iter_historical_state_request_batches(
                    plan=plan, header_inventory=inventory,
                )
            )
            scan.project_historical_window_projection(
                plan=plan,
                anchor_capture=capture,
                lower_bound_capture=lower,
                header_inventory=inventory,
                batch_results=iter(state_pairs),
            )
        finally:
            sys.settrace(prior_trace)
        self.assertGreaterEqual(
            sum(name == "header_drain" for name, _retained in observations),
            2,
        )
        self.assertGreaterEqual(
            sum(name == "window_drain" for name, _retained in observations),
            2,
        )
        self.assertFalse(
            any(retained for _name, retained in observations),
            observations,
        )

    def test_task4b_public_wrappers_close_only_over_ordinary_drains(self):
        import scripts.historical_foundry_scan as scan

        for public_name, drain_name, constructor_name in (
            (
                "project_historical_header_inventory", "header_drain",
                "new_header_driver",
            ),
            (
                "project_historical_window_projection", "window_drain",
                "new_window_driver",
            ),
        ):
            wrapper = getattr(scan, public_name)
            bindings = {
                name: cell.cell_contents for name, cell in zip(
                    wrapper.__code__.co_freevars, wrapper.__closure__ or (),
                )
            }
            self.assertEqual(tuple(bindings), (drain_name,))
            self.assertNotIn(constructor_name, bindings)
            declaration = self._unique_private_closure_value(
                scan,
                "header_declaration" if public_name.endswith("inventory")
                else "window_declaration",
            )
            self.assertEqual(inspect.signature(wrapper), inspect.signature(declaration))
            self.assertEqual(wrapper.__annotations__, declaration.__annotations__)
            self.assertEqual(wrapper.__name__, public_name)
            self.assertEqual(wrapper.__qualname__, public_name)
            self.assertEqual(wrapper.__module__, scan.__name__)
            self.assertIs(pickle.loads(pickle.dumps(wrapper)), wrapper)

        headers, capture, lower, plan = _small_context()
        header_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in iter_historical_header_request_batches(plan)
        )
        inventory = scan.project_historical_header_inventory(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            batch_results=iter(header_pairs),
        )
        self.assertEqual(inventory["row_count"], plan["block_count"])

        state_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in iter_historical_state_request_batches(
                plan=plan, header_inventory=inventory,
            )
        )
        ambient = object()
        token = scan._ACTIVE_HEADER_VALIDATION.set(ambient)
        try:
            projection = scan.project_historical_window_projection(
                plan=plan,
                anchor_capture=capture,
                lower_bound_capture=lower,
                header_inventory=inventory,
                batch_results=iter(state_pairs),
            )
            self.assertIs(scan._ACTIVE_HEADER_VALIDATION.get(), ambient)
        finally:
            scan._ACTIVE_HEADER_VALIDATION.reset(token)
        self.assertEqual(
            projection["coverage"]["header_count"], plan["block_count"]
        )

    def test_task4b_ingress_control_priority_matrix(self):
        case = HistoricalFoundryScanTask4bBridgeTests(
            methodName=(
                "test_task4b_real_owner_cleanup_priority_has_no_exception_context"
            )
        )
        case.test_task4b_real_owner_cleanup_priority_has_no_exception_context()

    def test_task4b_ingress_guards_success_assignment_until_delivery(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        for control_class in (
            KeyboardInterrupt, SystemExit, GeneratorExit,
            asyncio.CancelledError,
        ):
            with self.subTest(control=control_class.__name__):
                fixture = _Task4bOfflineCapabilityFixture()
                control = control_class(
                    "Task4b pre-assignment " + control_class.__name__
                )
                snapshot_reference = [None]
                fired = [False]
                result = None
                escaped = None
                opcode_cache = {}
                prior_trace = sys.gettrace()

                def instruction(frame):
                    rows = opcode_cache.get(frame.f_code)
                    if rows is None:
                        rows = {
                            row.offset: (row.opname, row.argval)
                            for row in dis.get_instructions(frame.f_code)
                        }
                        opcode_cache[frame.f_code] = rows
                    return rows.get(frame.f_lasti)

                def tracer(frame, event, argument):
                    if (
                        frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name == "_prepare_handle"
                        and event == "return"
                        and type(argument)
                        is storage.HistoricalRunStagingSnapshot
                    ):
                        snapshot_reference[0] = weakref.ref(argument)
                    if (
                        frame.f_code.co_filename == scan.__file__
                        and frame.f_code.co_name
                        == "_materialize_historical_window_staging_snapshot"
                    ):
                        frame.f_trace_opcodes = True
                        if (
                            not fired[0]
                            and snapshot_reference[0] is not None
                            and event == "opcode"
                            and instruction(frame)
                            == ("STORE_FAST", "snapshot")
                        ):
                            fired[0] = True
                            raise control
                    return tracer

                try:
                    capability = fixture.mint()
                    sys.settrace(tracer)
                    try:
                        result = scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                    except BaseException as caught:
                        escaped = caught
                finally:
                    sys.settrace(prior_trace)
                    fixture.capability = None
                try:
                    self.assertTrue(fired[0])
                    self.assertIsNone(result)
                    self.assertIs(escaped, control)
                    self.assertIsNone(control.__context__)
                    escaped.__traceback__ = None
                    escaped = None
                    control = None
                    capability = None
                    gc.collect()
                    self.assertIsNotNone(snapshot_reference[0])
                    self.assertIsNone(snapshot_reference[0]())
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    leaked = (
                        snapshot_reference[0]()
                        if snapshot_reference[0] is not None else None
                    )
                    if leaked is not None:
                        leaked.close()
                    fixture.close()

    def test_task4b_ingress_uses_bound_consume_after_export_replacement(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        original_consume = (
            storage.consume_production_historical_window_capability
        )
        fake_calls = []
        first_error = None
        retry_error = None
        retry_snapshot = None

        def fake_consume(*, capability):
            fake_calls.append(capability)
            raise AssertionError("replacement consume was invoked")

        try:
            storage.consume_production_historical_window_capability = (
                fake_consume
            )
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                first_error = caught
        finally:
            storage.consume_production_historical_window_capability = (
                original_consume
            )
        try:
            try:
                retry_snapshot = (
                    scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
                )
            except BaseException as caught:
                retry_error = caught
            self.assertEqual(fake_calls, [])
            self.assertIs(type(first_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (first_error.reason_code, first_error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertIs(type(retry_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (retry_error.reason_code, retry_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(retry_snapshot)
        finally:
            if retry_snapshot is not None:
                retry_snapshot.close()
            fixture.capability = None
            try:
                capability.close()
            except BaseException:
                pass
            fixture.close()

    def test_task4b_ingress_uses_bound_consume_after_storage_alias_replacement(self):
        import scripts
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        fake_calls = []
        first_error = None
        retry_error = None
        retry_snapshot = None

        def fake_consume(*, capability):
            fake_calls.append(capability)
            raise AssertionError("replacement storage module was invoked")

        fake_storage = types.SimpleNamespace(
            consume_production_historical_window_capability=fake_consume
        )
        try:
            sys.modules["scripts.historical_foundry_storage"] = fake_storage
            scripts.historical_foundry_storage = fake_storage
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                first_error = caught
        finally:
            sys.modules["scripts.historical_foundry_storage"] = storage
            scripts.historical_foundry_storage = storage
        try:
            try:
                retry_snapshot = (
                    scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
                )
            except BaseException as caught:
                retry_error = caught
            self.assertEqual(fake_calls, [])
            self.assertIs(type(first_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (first_error.reason_code, first_error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertIs(type(retry_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (retry_error.reason_code, retry_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(retry_snapshot)
        finally:
            if retry_snapshot is not None:
                retry_snapshot.close()
            fixture.capability = None
            try:
                capability.close()
            except BaseException:
                pass
            fixture.close()

    def test_task4b_ingress_never_executes_in_place_consume_code_drift(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        consume = storage.consume_production_historical_window_capability
        original_code = consume.__code__
        first_error = None
        retry_error = None
        retry_snapshot = None

        def replacement_factory():
            free_a = free_b = free_c = free_d = None

            def replacement(*, capability):
                (free_a, free_b, free_c, free_d)
                return capability

            return replacement.__code__

        replacement_code = replacement_factory()
        self.assertEqual(
            len(replacement_code.co_freevars),
            len(original_code.co_freevars),
        )
        try:
            consume.__code__ = replacement_code
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                first_error = caught
        finally:
            consume.__code__ = original_code
        try:
            try:
                retry_snapshot = (
                    scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
                )
            except BaseException as caught:
                retry_error = caught
            self.assertIs(type(first_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (first_error.reason_code, first_error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertIs(type(retry_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (retry_error.reason_code, retry_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(retry_snapshot)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if retry_snapshot is not None:
                retry_snapshot.close()
            fixture.capability = None
            try:
                capability.close()
            except BaseException:
                pass
            fixture.close()

    def test_task4b_ingress_never_executes_private_runner_code_drift(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        materialize = scan._materialize_historical_window_staging_snapshot
        closure = dict(zip(
            materialize.__code__.co_freevars,
            materialize.__closure__,
        ))
        runner = closure[
            "task4b_storage_consume_runner"
        ].cell_contents
        original_code = runner.__code__
        first_error = None
        retry_error = None
        retry_snapshot = None

        def replacement_factory():
            free_a = free_b = free_c = free_d = None

            def replacement(*, capability):
                (free_a, free_b, free_c, free_d, capability)
                raise GeneratorExit("private runner code executed")

            return replacement.__code__

        replacement_code = replacement_factory()
        self.assertEqual(
            len(replacement_code.co_freevars),
            len(original_code.co_freevars),
        )
        try:
            runner.__code__ = replacement_code
            try:
                materialize(capability=capability)
            except BaseException as caught:
                first_error = caught
        finally:
            runner.__code__ = original_code
        try:
            try:
                retry_snapshot = materialize(capability=capability)
            except BaseException as caught:
                retry_error = caught
            self.assertIs(type(first_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (first_error.reason_code, first_error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertIs(type(retry_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (retry_error.reason_code, retry_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(retry_snapshot)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if retry_snapshot is not None:
                retry_snapshot.close()
            fixture.capability = None
            try:
                capability.close()
            except BaseException:
                pass
            fixture.close()

    def test_task4b_ingress_never_executes_consume_closure_cell_drift(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        consume = storage.consume_production_historical_window_capability
        core_name = "_consume_production_historical_window_capability_core"
        core_index = consume.__code__.co_freevars.index(core_name)
        core_cell = consume.__closure__[core_index]
        original_core = core_cell.cell_contents
        fake_calls = []
        first_error = None
        retry_error = None
        retry_snapshot = None

        def fake_core(candidate, delivery_guard):
            fake_calls.append((candidate, delivery_guard))
            return original_core(candidate, delivery_guard)

        try:
            core_cell.cell_contents = fake_core
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                first_error = caught
        finally:
            core_cell.cell_contents = original_core
        try:
            try:
                retry_snapshot = (
                    scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
                )
            except BaseException as caught:
                retry_error = caught
            self.assertEqual(fake_calls, [])
            self.assertIs(type(first_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (first_error.reason_code, first_error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertIs(type(retry_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (retry_error.reason_code, retry_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(retry_snapshot)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if retry_snapshot is not None:
                retry_snapshot.close()
            fixture.capability = None
            try:
                capability.close()
            except BaseException:
                pass
            fixture.close()

    def test_task4b_ingress_rejects_nested_kwdefault_content_drift(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        root = (
            storage._ConsumedProductionHistoricalWindowCapabilityView
            ._materialize_staging_snapshot_from_bound_scan
        )
        function_type = type(root)
        pending = [root]
        seen = set()
        target = None
        while pending:
            function = pending.pop()
            if id(function) in seen:
                continue
            seen.add(id(function))
            if (
                function.__name__ == "_task4b_open_registered_slot"
                and function.__kwdefaults__ == {"mode": None}
            ):
                target = function
                break
            for cell in function.__closure__ or ():
                value = cell.cell_contents
                if type(value) is function_type:
                    pending.append(value)
        self.assertIsNotNone(target)
        original_mode = target.__kwdefaults__["mode"]
        first_error = None
        retry_error = None
        retry_snapshot = None
        try:
            target.__kwdefaults__["mode"] = object()
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                first_error = caught
        finally:
            target.__kwdefaults__["mode"] = original_mode
        try:
            try:
                retry_snapshot = (
                    scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
                )
            except BaseException as caught:
                retry_error = caught
            self.assertIs(type(first_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (first_error.reason_code, first_error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertIs(type(retry_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (retry_error.reason_code, retry_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(retry_snapshot)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if retry_snapshot is not None:
                retry_snapshot.close()
            fixture.capability = None
            try:
                capability.close()
            except BaseException:
                pass
            fixture.close()

    def test_task4b_manifest_control_consumes_owner_before_exact_reraise(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        view_class = storage._ConsumedProductionHistoricalWindowCapabilityView
        original_close = view_class.close
        public_close_calls = []
        control = GeneratorExit("Task4b manifest control")
        escaped = None
        retry_error = None
        retry_snapshot = None
        fired = [False]
        close_replaced = [False]
        prior_trace = sys.gettrace()
        prior_profile = sys.getprofile()

        def no_op_close(self):
            public_close_calls.append(self)
            return None

        def tracer(frame, event, _argument):
            if (
                not fired[0]
                and frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name
                == "_verify_task4b_semantic_dependency_manifest"
                and event == "call"
            ):
                fired[0] = True
                raise control
            return tracer

        def profiler(frame, event, _argument):
            if (
                not close_replaced[0]
                and frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name
                == "consume_production_historical_window_capability"
                and event == "return"
            ):
                close_replaced[0] = True
                view_class.close = no_op_close

        try:
            sys.setprofile(profiler)
            sys.settrace(tracer)
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                escaped = caught
        finally:
            sys.settrace(prior_trace)
            sys.setprofile(prior_profile)
            view_class.close = original_close
        try:
            self.assertTrue(fired[0])
            self.assertTrue(close_replaced[0])
            self.assertIs(escaped, control)
            self.assertIsNone(control.__cause__)
            self.assertIsNone(control.__context__)
            self.assertEqual(public_close_calls, [])
            escaped.__traceback__ = None
            escaped = None
            try:
                retry_snapshot = (
                    scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
                )
            except BaseException as caught:
                retry_error = caught
            self.assertIs(type(retry_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (retry_error.reason_code, retry_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(retry_snapshot)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if retry_snapshot is not None:
                retry_snapshot.close()
            fixture.capability = None
            try:
                capability.close()
            except BaseException:
                pass
            fixture.close()

    def test_task4b_ingress_rejects_global_sys_and_package_alias_drift(self):
        import scripts
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        calls = []

        class FakeView:
            def _materialize_staging_snapshot_from_bound_scan(self):
                return object()

        def fake_consume(*, capability):
            calls.append(capability)
            return FakeView()

        fake_storage = types.SimpleNamespace(
            consume_production_historical_window_capability=fake_consume
        )
        fake_sys = types.SimpleNamespace(modules={
            "scripts.historical_foundry_storage": fake_storage,
        })
        original_sys = scan.sys
        original_storage_alias = storage
        try:
            scan.sys = fake_sys
            scripts.historical_foundry_storage = fake_storage
            with self.assertRaises(HistoricalWindowProjectionError) as caught:
                scan._materialize_historical_window_staging_snapshot(
                    capability=object()
                )
        finally:
            scan.sys = original_sys
            scripts.historical_foundry_storage = original_storage_alias
        self.assertEqual(calls, [])
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "fixture_input_invalid"),
        )

    def test_task4b_manifest_drift_never_invokes_unvalidated_close_callback(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        calls = []

        class FakeCapability:
            def close(self):
                calls.append("close")

        original = scan._parse_quantity
        error = None
        try:
            scan._parse_quantity = lambda _value: 0
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=FakeCapability()
                )
            except BaseException as caught:
                error = caught
        finally:
            scan._parse_quantity = original
        self.assertEqual(calls, [])
        self.assertIs(type(error), rpc._ArchiveRpcError)
        self.assertEqual(
            (error.reason_code, error.failure_kind),
            (
                "authority_mismatch",
                "historical_window_capability_invalid",
            ),
        )

    def test_task4b_manifest_drift_terminalizes_real_capability_without_public_close(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        capability_class = storage._ProductionHistoricalWindowCapability
        original_parse = scan._parse_quantity
        original_close = capability_class.close
        close_calls = []
        first_error = None
        retry_error = None
        retry_snapshot = None

        def no_op_close(self):
            close_calls.append(self)
            return None

        try:
            scan._parse_quantity = lambda _value: 0
            capability_class.close = no_op_close
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                first_error = caught
        finally:
            scan._parse_quantity = original_parse
            capability_class.close = original_close
        try:
            try:
                retry_snapshot = (
                    scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
                )
            except BaseException as caught:
                retry_error = caught
            self.assertEqual(close_calls, [])
            self.assertIs(type(first_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (first_error.reason_code, first_error.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertIs(type(retry_error), rpc._ArchiveRpcError)
            self.assertEqual(
                (retry_error.reason_code, retry_error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(retry_snapshot)
        finally:
            if retry_snapshot is not None:
                retry_snapshot.close()
            fixture.capability = None
            try:
                capability.close()
            except BaseException:
                pass
            fixture.close()

    def test_task4b_drains_validate_before_touching_batch_iterable(self):
        import scripts.historical_foundry_scan as scan

        class HostileIterable:
            def __init__(self):
                self.touches = 0

            def __iter__(self):
                self.touches += 1
                raise RuntimeError("batch iterable must remain untouched")

        common = {
            "plan": {},
            "anchor_capture": {},
            "lower_bound_capture": {},
        }
        for public_name, declaration_name, extra in (
            (
                "project_historical_header_inventory",
                "header_declaration", {},
            ),
            (
                "project_historical_window_projection",
                "window_declaration", {"header_inventory": {}},
            ),
        ):
            declaration = self._unique_private_closure_value(
                scan, declaration_name
            )
            expected_source = HostileIterable()
            actual_source = HostileIterable()
            with self.assertRaises(HistoricalWindowProjectionError) as expected:
                declaration(
                    **common, **extra, batch_results=expected_source
                )
            with self.assertRaises(HistoricalWindowProjectionError) as actual:
                getattr(scan, public_name)(
                    **common, **extra, batch_results=actual_source
                )
            self.assertEqual(expected_source.touches, 0)
            self.assertEqual(actual_source.touches, 0)
            self.assertEqual(
                (actual.exception.reason_code, actual.exception.failure_kind),
                (
                    expected.exception.reason_code,
                    expected.exception.failure_kind,
                ),
            )

    def test_task4b_window_drain_input_failures_match_legacy_per_root(self):
        import scripts.historical_foundry_scan as scan

        headers, capture, lower, plan = _small_context()
        header_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in iter_historical_header_request_batches(plan)
        )
        inventory = scan.project_historical_header_inventory(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            batch_results=iter(header_pairs),
        )
        state_pairs = tuple(
            (
                descriptor,
                _responses_for_descriptor(descriptor, headers.__getitem__),
            )
            for descriptor in iter_historical_state_request_batches(
                plan=plan, header_inventory=inventory,
            )
        )
        expected_pairs = {
            "reserve": (
                "reserve_snapshot_incomplete", "reserve_coverage_invalid"
            ),
            "price": (
                "price_snapshot_incomplete", "price_coverage_invalid"
            ),
            "fee_history": (
                "fee_history_incomplete", "fee_coverage_invalid"
            ),
            "final_anchor": ("anchor_changed", "final_anchor_mismatch"),
        }
        target_by_kind = {
            kind: next(
                index for index, pair in enumerate(state_pairs)
                if pair[0]["kind"] == kind
            )
            for kind in expected_pairs
        }

        class FailingResults:
            def __init__(self, target, ordinary):
                self._position = 0
                self._target = target
                self._ordinary = ordinary

            def __iter__(self):
                return self

            def __next__(self):
                if self._position == self._target:
                    if self._ordinary:
                        raise RuntimeError("ordinary input failure")
                    raise StopIteration
                pair = state_pairs[self._position]
                self._position += 1
                return pair

        declaration = self._unique_private_closure_value(
            scan, "window_declaration"
        )
        common = {
            "plan": plan,
            "anchor_capture": capture,
            "lower_bound_capture": lower,
            "header_inventory": inventory,
        }
        for kind, expected_pair in expected_pairs.items():
            for ordinary in (False, True):
                with self.subTest(kind=kind, ordinary=ordinary):
                    target = target_by_kind[kind]
                    with self.assertRaises(
                        HistoricalWindowProjectionError
                    ) as legacy:
                        declaration(
                            **common,
                            batch_results=FailingResults(target, ordinary),
                        )
                    with self.assertRaises(
                        HistoricalWindowProjectionError
                    ) as actual:
                        scan.project_historical_window_projection(
                            **common,
                            batch_results=FailingResults(target, ordinary),
                        )
                    self.assertEqual(
                        (
                            legacy.exception.reason_code,
                            legacy.exception.failure_kind,
                        ),
                        expected_pair,
                    )
                    self.assertEqual(
                        (
                            actual.exception.reason_code,
                            actual.exception.failure_kind,
                        ),
                        expected_pair,
                    )

    def test_task4b_public_projectors_remain_ordinary_pickleable_drains(self):
        import scripts.historical_foundry_scan as scan

        for projector in (
            scan.project_historical_header_inventory,
            scan.project_historical_window_projection,
        ):
            self.assertIsNotNone(projector.__closure__)
            self.assertEqual(projector.__module__, scan.__name__)
            self.assertEqual(projector.__qualname__, projector.__name__)
            self.assertIs(pickle.loads(pickle.dumps(projector)), projector)

    def test_task4b_ingress_delegates_and_preserves_close_control(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        with self.assertRaises(rpc._ArchiveRpcError) as caught:
            scan._materialize_historical_window_staging_snapshot(
                capability=object()
            )
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            (
                "authority_mismatch",
                "historical_window_capability_invalid",
            ),
        )


if __name__ == "__main__":
    unittest.main()
