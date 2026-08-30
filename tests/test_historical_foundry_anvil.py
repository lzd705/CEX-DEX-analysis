from __future__ import annotations

import copy
import gzip
import hashlib
import http.server
import inspect
import io
import json
import os
import pickle
import socket
import subprocess
import sys
import tempfile
import types
import unittest
from decimal import Decimal, localcontext
from unittest import mock

from scripts.historical_foundry_contracts import (
    build_validated_executor_artifact,
    load_historical_foundry_config_set,
)
from scripts.route_cost_evidence import (
    keccak256,
    solidity_allowance_storage_key,
    solidity_balance_storage_key,
)
from tests import test_historical_foundry_scan as scan_fixtures


class _Clock:
    def __init__(self, values=None):
        self.values = list(values or [0.0])
        self.last = self.values[-1]

    def __call__(self):
        if self.values:
            self.last = self.values.pop(0)
        return self.last


class HistoricalFoundryRelayTests(unittest.TestCase):
    """R6.2 and the fork-relay method/resource/deadline boundary."""

    def test_relay_allowlist_hmac_and_single_object_contract(self):
        import scripts.historical_foundry_rpc as rpc

        calls = []

        def operation(body, remaining):
            calls.append((body, remaining))
            return b'{"id":7,"jsonrpc":"2.0","result":"0x1"}'

        lease = rpc._issue_historical_relay_lease_for_test(
            endpoint="https://fixture.invalid/archive?key=secret",
            operation=operation,
            monotonic=_Clock([0.0, 1.0, 2.0, 3.0, 4.0]),
            entropy=lambda size: b"k" * size,
        )
        address = "0x" + "11" * 20
        slot = "0x" + "22" * 32
        calldata = "0x" + "33" * 4
        block_hash = "0x" + "44" * 32
        facade = rpc._issue_historical_relay_scenario_facade(
            relay_lease=lease,
            authority={
                "block_number": 1,
                "block_hash": block_hash,
                "block_tag": {
                    "blockHash": block_hash,
                    "requireCanonical": True,
                },
                "addresses": frozenset((address,)),
                "calls": frozenset(((address, calldata),)),
            },
            absolute_deadline=120.0,
        )
        try:
            self.assertNotIn("fixture.invalid", repr(lease))
            self.assertNotIn("secret", repr(lease))
            requests = (
                ("eth_chainId", []),
                ("eth_getBlockByNumber", ["0x1", False]),
                ("eth_getBlockByHash", [block_hash, False]),
                ("eth_getCode", [address, "0x1"]),
                ("eth_getBalance", [address, "0x1"]),
                ("eth_getTransactionCount", [address, "0x1"]),
                ("eth_getStorageAt", [address, slot, "0x1"]),
                ("eth_call", [{"to": address, "data": calldata}, "0x1"]),
                ("eth_getProof", [address, [slot], "0x1"]),
            )
            for method, params in requests:
                with self.subTest(method=method):
                    request = json.dumps({
                        "id": 7, "jsonrpc": "2.0", "method": method,
                        "params": params,
                    }, sort_keys=True, separators=(",", ":")).encode("ascii")
                    self.assertEqual(
                        rpc._relay_historical_archive_call(
                            relay_lease=facade,
                            canonical_request_bytes=request,
                        ),
                        b'{"id":7,"jsonrpc":"2.0","result":"0x1"}',
                    )
            for body in (
                b'[{"id":1,"jsonrpc":"2.0","method":"eth_chainId","params":[]}]',
                b'{"id":true,"jsonrpc":"2.0","method":"eth_chainId","params":[]}',
                b'{"id":1,"jsonrpc":"2.0","method":"eth_feeHistory","params":[]}',
                b'{"id":1,"jsonrpc":"2.0","method":"anvil_mine","params":[]}',
            ):
                with self.subTest(body=body[:24]):
                    with self.assertRaises(ValueError):
                        rpc._relay_historical_archive_call(
                            relay_lease=facade,
                            canonical_request_bytes=body,
                        )
            key = object.__getattribute__(lease, "_key")
            key[0] ^= 1
            with self.assertRaises(ValueError):
                rpc._relay_historical_archive_call(
                    relay_lease=facade,
                    canonical_request_bytes=(
                        b'{"id":7,"jsonrpc":"2.0","method":"eth_chainId",'
                        b'"params":[]}'
                    ),
                )
        finally:
            facade.close()
            lease.close()
        self.assertTrue(all(value == 0 for value in key))
        self.assertEqual(lease.close(), None)
        self.assertGreaterEqual(len(calls), 9)

    def test_relay_resource_boundaries_are_inclusive_and_deadline_is_absolute(self):
        import scripts.historical_foundry_rpc as rpc

        limits = {
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
        self.assertIsNone(rpc._validate_historical_relay_resource_counts(
            **limits, elapsed_seconds=29.999999
        ))
        for key, value in limits.items():
            changed = dict(limits)
            changed[key] = value + 1
            with self.subTest(boundary=key):
                with self.assertRaises(ValueError):
                    rpc._validate_historical_relay_resource_counts(
                        **changed, elapsed_seconds=0.0
                    )
        for elapsed in (30.0, 30.000001):
            with self.subTest(elapsed=elapsed):
                with self.assertRaises(TimeoutError):
                    rpc._validate_historical_relay_resource_counts(
                        **limits, elapsed_seconds=elapsed
                    )


class _Process:
    def __init__(self, wait_effects):
        self.wait_effects = list(wait_effects)
        self.calls = []
        self.returncode = None

    def terminate(self):
        self.calls.append(("terminate",))

    def kill(self):
        self.calls.append(("kill",))

    def wait(self, timeout):
        self.calls.append(("wait", timeout))
        effect = self.wait_effects.pop(0)
        if isinstance(effect, BaseException):
            raise effect
        self.returncode = effect
        return effect


def _serve_historical_anvil_fixture() -> None:
    """Serve one stateful local JSON-RPC fixture in a real child process."""
    port = int(sys.argv[1])
    with open(sys.argv[2], "r", encoding="utf-8") as handle:
        config = json.load(handle)
    native_balances = {}
    nonces = {}
    codes = {}
    storage = {}
    state = {"sent": False, "transaction": None}

    def quantity(value):
        return hex(value)

    def word(value):
        return "0x" + int(value).to_bytes(32, "big").hex()

    def result(method, params):
        if method == "eth_chainId":
            return "0x1"
        if method == "eth_getBlockByNumber":
            if params[0] == quantity(config["header"]["number"]):
                header = config["header"]
                return {
                    "number": quantity(header["number"]),
                    "hash": header["hash"],
                    "parentHash": header["parent_hash"],
                    "stateRoot": header["state_root"],
                    "timestamp": quantity(header["timestamp"]),
                    "gasLimit": quantity(header["gas_limit"]),
                    "gasUsed": quantity(header["gas_used"]),
                    "baseFeePerGas": quantity(header["base_fee_per_gas"]),
                    "transactions": [],
                }
            return {
                "number": quantity(config["synthetic_number"]),
                "hash": config["child_hash"],
                "parentHash": config["header"]["hash"],
                "stateRoot": config["header"]["state_root"],
                "timestamp": quantity(config["synthetic_timestamp"]),
                "gasLimit": quantity(config["transaction"]["gas"]),
                "gasUsed": quantity(config["gas_used"]),
                "baseFeePerGas": quantity(config["synthetic_base_fee"]),
                "transactions": [config["transaction_hash"]],
            }
        if method == "eth_call":
            target = params[0]["to"].lower()
            data = params[0]["data"].lower()
            pair = config["pairs"].get(target)
            if pair is not None and data == "0x0902f1ac":
                return "0x" + "".join(
                    int(value).to_bytes(32, "big").hex()
                    for value in (pair["word0"], pair["word1"], pair["timestamp"])
                )
            if data.startswith("0x70a08231") and len(data) == 74:
                owner = "0x" + data[-40:]
                role = config["token_roles"].get(target)
                if role is None:
                    return word(0)
                owner_pair = config["pairs"].get(owner)
                if owner_pair is not None:
                    return word(owner_pair[role])
                if owner == config["executor"]:
                    if role == "weth":
                        return word(
                            config["final_weth"] if state["sent"]
                            else config["initial_weth"]
                        )
                    return word(config["residual_uni"] if state["sent"] else 0)
            return word(0)
        if method == "eth_getBalance":
            return quantity(native_balances.get(params[0].lower(), 0))
        if method == "eth_getTransactionCount":
            return quantity(nonces.get(params[0].lower(), 0))
        if method == "eth_getCode":
            return codes.get(params[0].lower(), "0x")
        if method == "eth_getStorageAt":
            return storage.get((params[0].lower(), params[1].lower()), word(0))
        if method == "anvil_setBalance":
            native_balances[params[0].lower()] = int(params[1], 16); return True
        if method == "anvil_setNonce":
            nonces[params[0].lower()] = int(params[1], 16); return True
        if method == "anvil_setCode":
            codes[params[0].lower()] = params[1].lower(); return True
        if method == "anvil_setStorageAt":
            storage[(params[0].lower(), params[1].lower())] = params[2].lower(); return True
        if method in (
            "evm_setNextBlockTimestamp", "anvil_setNextBlockBaseFeePerGas",
            "anvil_impersonateAccount", "anvil_stopImpersonatingAccount",
            "anvil_mine",
        ):
            return True
        if method == "eth_sendTransaction":
            state["transaction"] = params[0]
            state["sent"] = True
            return config["transaction_hash"]
        if method == "eth_getTransactionReceipt":
            pair = config["first_pair"]
            executor_topic = "0x" + "0" * 24 + config["executor"][2:]
            pair_topic = "0x" + "0" * 24 + pair[2:]
            return {
                "status": "0x1", "blockNumber": quantity(config["synthetic_number"]),
                "blockHash": config["child_hash"], "transactionIndex": "0x0",
                "gasUsed": quantity(config["gas_used"]),
                "effectiveGasPrice": quantity(config["effective_gas_price"]),
                "transactionHash": config["transaction_hash"],
                "logs": [{
                    "address": config["uni"],
                    "topics": [config["transfer_topic"], pair_topic, executor_topic],
                    "data": word(config["first_uni"]), "logIndex": "0x0",
                    "transactionIndex": "0x0", "removed": False,
                }],
            }
        if method == "debug_traceTransaction":
            if len(params) == 2 and params[1] == {"tracer": "callTracer"}:
                tx = state["transaction"]
                return {
                    "type": "CALL", "from": tx["from"], "to": tx["to"],
                    "input": tx["input"], "output": "0x", "value": tx["value"],
                    "gas": tx["gas"], "gasUsed": quantity(config["gas_used"]),
                    "calls": [],
                }
            return {
                "gas": config["gas_used"], "failed": False, "returnValue": "0x",
                "structLogs": [{
                    "pc": 0, "op": "STOP", "gas": 1, "gasCost": 0,
                    "depth": 1, "stack": [], "memory": [], "storage": {},
                }],
            }
        if method == "eth_getTransactionByHash":
            tx = state["transaction"]
            return dict(tx, chainId="0x1", hash=config["transaction_hash"],
                        blockHash=config["child_hash"],
                        blockNumber=quantity(config["synthetic_number"]),
                        transactionIndex="0x0")
        raise RuntimeError("unsupported fixture method")

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format, *args):
            del args

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            try:
                payload = {"id": request["id"], "jsonrpc": "2.0",
                           "result": result(request["method"], request["params"])}
            except Exception:
                payload = {"id": request["id"], "jsonrpc": "2.0",
                           "error": {"code": -32000, "message": "fixture failed"}}
            body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers(); self.wfile.write(body); self.wfile.flush()

    http.server.HTTPServer(("127.0.0.1", port), Handler).serve_forever()


class HistoricalFoundryProcessLeaseTests(unittest.TestCase):
    """R6.3: reviewed toolchain owns spawn through exact bounded reap."""

    def test_production_spawn_signature_has_only_dynamic_sealed_inputs(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        signature = inspect.signature(
            toolchain.ReviewedHistoricalToolchain._spawn_historical_anvil_process
        )
        self.assertEqual(
            tuple(signature.parameters),
            ("self", "selected_block", "hardfork", "relay_port", "anvil_port"),
        )
        self.assertFalse({
            "binary", "binary_path", "flags", "arguments", "environment",
            "cwd", "endpoint", "private_key", "popen", "timeout",
        }.intersection(signature.parameters))

    def test_term_success_and_term_timeout_kill_are_exact(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        cases = (
            ("term", [0], [("terminate",), ("wait", 5.0)]),
            (
                "kill",
                [subprocess.TimeoutExpired("anvil", 5), -9],
                [("terminate",), ("wait", 5.0), ("kill",), ("wait", 5.0)],
            ),
        )
        for axis, effects, expected in cases:
            with self.subTest(axis=axis):
                process = _Process(effects)
                cleanup = mock.Mock()
                lease = toolchain._issue_historical_process_lease_for_test(
                    process=process,
                    cleanup=cleanup,
                    binary_sha256="1" * 64,
                    selected_block=123,
                    hardfork="osaka",
                )
                projection = lease.redacted_argv_projection()
                self.assertEqual(
                    set(projection),
                    {"schema", "binary_sha256", "fixed_arguments",
                     "selected_block", "hardfork", "fork_url_kind"},
                )
                self.assertEqual(
                    projection["schema"], "historical_foundry_anvil_argv/v1"
                )
                self.assertEqual(projection["fork_url_kind"], "loopback_relay")
                self.assertNotIn("port", repr(projection).lower())
                self.assertNotIn("http", repr(projection).lower())
                self.assertEqual(lease.close(), None)
                self.assertEqual(process.calls, expected)
                cleanup.assert_called_once_with()
                self.assertEqual(lease.close(), None)

    def test_process_output_limits_are_inclusive_and_plus_one_closes(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        self.assertIsNone(
            toolchain._validate_historical_process_output_counts(
                stdout_bytes=32_768, stderr_bytes=32_768
            )
        )
        for stream in ("stdout", "stderr"):
            values = {"stdout_bytes": 32_768, "stderr_bytes": 32_768}
            values[stream + "_bytes"] += 1
            with self.subTest(stream=stream):
                with self.assertRaises(ValueError):
                    toolchain._validate_historical_process_output_counts(
                        **values
                    )
        process = _Process([0])
        process.stdout = io.BytesIO(b"x" * 65_537)
        process.stderr = io.BytesIO(b"")
        cleanup = mock.Mock()
        lease = toolchain._issue_historical_process_lease_for_test(
            process=process, cleanup=cleanup,
            binary_sha256="4" * 64,
            selected_block=123, hardfork="osaka",
        )
        with self.assertRaises(ValueError):
            lease.close()
        self.assertEqual(process.calls, [("terminate",), ("wait", 5.0)])
        cleanup.assert_called_once_with()

    def test_unreaped_child_fails_after_cleanup_and_control_flow_propagates(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        timeout = subprocess.TimeoutExpired("anvil", 5)
        process = _Process([timeout, timeout])
        cleanup = mock.Mock()
        lease = toolchain._issue_historical_process_lease_for_test(
            process=process,
            cleanup=cleanup,
            binary_sha256="2" * 64,
            selected_block=123,
            hardfork="osaka",
        )
        with self.assertRaises(ValueError):
            lease.close()
        cleanup.assert_not_called()
        self.assertIs(object.__getattribute__(lease, "_process"), process)
        self.assertFalse(object.__getattribute__(lease, "_closed"))
        self.assertEqual(
            process.calls,
            [("terminate",), ("wait", 5.0), ("kill",), ("wait", 5.0)],
        )

        controlled = _Process([KeyboardInterrupt(), 0])
        cleanup_controlled = mock.Mock()
        lease = toolchain._issue_historical_process_lease_for_test(
            process=controlled,
            cleanup=cleanup_controlled,
            binary_sha256="3" * 64,
            selected_block=123,
            hardfork="osaka",
        )
        with self.assertRaises(KeyboardInterrupt):
            lease.close()
        cleanup_controlled.assert_called_once_with()

    def test_blocked_drainer_is_joined_again_after_pipe_close(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        class Thread:
            def __init__(self):
                self.alive = True
                self.joins = []

            def join(self, timeout):
                self.joins.append(timeout)

            def is_alive(self):
                return self.alive

        thread = Thread()
        process = _Process([0])

        class Stream:
            def close(self):
                thread.alive = False

        process.stdout = Stream()
        process.stderr = Stream()
        lease = toolchain._issue_historical_process_lease_for_test(
            process=process, cleanup=mock.Mock(),
            binary_sha256="5" * 64, selected_block=123,
            hardfork="osaka",
        )
        object.__setattr__(lease, "_output_threads", (thread,))
        self.assertIsNone(lease.close())
        self.assertEqual(thread.joins, [5.0, 5.0])

    def test_process_reap_uses_remaining_absolute_budget_for_each_block(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        timeout = subprocess.TimeoutExpired("anvil", 2.0)
        process = _Process([timeout, -9])
        lease = toolchain._issue_historical_process_lease_for_test(
            process=process, cleanup=mock.Mock(),
            binary_sha256="6" * 64, selected_block=123,
            hardfork="osaka",
        )
        budgets = iter((3.0, 2.0, 1.0, 0.5))

        self.assertIsNone(lease._close_with_budget(
            lambda cap: min(cap, next(budgets))
        ))
        self.assertEqual(process.calls, [
            ("terminate",), ("wait", 2.0),
            ("kill",), ("wait", 0.5),
        ])


class HistoricalFoundryScenarioAuthorityTests(unittest.TestCase):
    """R6.1: only the exact current grid lineage can issue a scenario."""

    @staticmethod
    def _prepared(fixture):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        capture = scan_fixtures.HistoricalPrefilterGridTests._capture_snapshot(
            fixture
        )
        config = load_historical_foundry_config_set()
        window = scan.open_validated_historical_window(
            config=config, staging=capture
        )
        rows = scan.build_historical_prefilter_grid(
            config=config, window=window
        )
        prefilter = storage._freeze_historical_prefilter_grid(
            staging=capture, rows=rows
        )
        grid = scan.validate_historical_prefilter_grid(
            config=config, window=window, staging=prefilter
        )
        return config, capture, prefilter, window, grid, rows

    def test_exact_signatures_and_sealed_capabilities(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan

        self.assertEqual(
            tuple(inspect.signature(
                anvil.open_historical_replay_context
            ).parameters),
            ("config", "staging", "window", "grid", "executor_artifact"),
        )
        self.assertEqual(
            tuple(inspect.signature(
                anvil.build_historical_state_override
            ).parameters),
            ("context", "scenario"),
        )
        self.assertEqual(
            tuple(inspect.signature(
                anvil._replay_historical_scenario
            ).parameters),
            ("context", "scenario", "sink"),
        )
        self.assertEqual(
            tuple(inspect.signature(
                scan._issue_validated_replay_scenario
            ).parameters),
            ("staging", "window", "grid", "scenario_key"),
        )
        forbidden = {
            "endpoint", "private_key", "binary_path", "argv", "router",
            "token", "pair", "sender", "executor", "slot", "value",
            "timestamp", "gas", "calldata", "direction", "row", "mapping",
        }
        for function in (
            anvil.open_historical_replay_context,
            anvil.build_historical_state_override,
            anvil._replay_historical_scenario,
            scan._issue_validated_replay_scenario,
        ):
            self.assertTrue(all(
                parameter.kind is inspect.Parameter.KEYWORD_ONLY
                for parameter in inspect.signature(function).parameters.values()
            ))
            self.assertFalse(
                forbidden.intersection(inspect.signature(function).parameters)
            )
        with self.assertRaises((TypeError, ValueError)):
            scan.ValidatedReplayScenario()
        forged = object.__new__(scan.ValidatedReplayScenario)
        self.assertNotIn("object at", repr(forged))
        with self.assertRaises(TypeError):
            copy.copy(forged)
        with self.assertRaises(TypeError):
            copy.deepcopy(forged)
        with self.assertRaises(TypeError):
            pickle.dumps(forged)

    def test_scenario_is_issued_from_exact_grid_row_and_cross_lineage_fails(self):
        import scripts.historical_foundry_scan as scan

        fixture_a = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
        fixture_b = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
        values_a = values_b = None
        try:
            values_a = self._prepared(fixture_a)
            values_b = self._prepared(fixture_b)
            _, _, prefilter_a, window_a, grid_a, rows_a = values_a
            _, _, prefilter_b, window_b, grid_b, rows_b = values_b
            self.assertEqual(rows_a, rows_b)
            scenario = scan._issue_validated_replay_scenario(
                staging=prefilter_a,
                window=window_a,
                grid=grid_a,
                scenario_key=rows_a[0]["scenario_key"],
            )
            self.assertIs(type(scenario), scan.ValidatedReplayScenario)
            self.assertEqual(scenario.scenario_key, rows_a[0]["scenario_key"])
            projection = scan._validated_replay_scenario_projection(
                scenario=scenario
            )
            self.assertEqual(projection["block_hash"], rows_a[0]["block_hash"])
            self.assertEqual(projection["direction"], rows_a[0]["direction"])
            self.assertEqual(
                projection["requested_notional_usd"],
                rows_a[0]["requested_notional_usd"],
            )
            with self.assertRaises(ValueError):
                scan._issue_validated_replay_scenario(
                    staging=prefilter_b,
                    window=window_a,
                    grid=grid_a,
                    scenario_key=rows_a[0]["scenario_key"],
                )
            with self.assertRaises(ValueError):
                scan._issue_validated_replay_scenario(
                    staging=prefilter_a,
                    window=window_b,
                    grid=grid_a,
                    scenario_key=rows_a[0]["scenario_key"],
                )
            with self.assertRaises(ValueError):
                scan._issue_validated_replay_scenario(
                    staging=prefilter_a,
                    window=window_a,
                    grid=grid_b,
                    scenario_key=rows_a[0]["scenario_key"],
                )
            with self.assertRaises(ValueError):
                scan._issue_validated_replay_scenario(
                    staging=prefilter_a,
                    window=window_a,
                    grid=grid_a,
                    scenario_key="2:uniswap_to_sushiswap:1001",
                )
        finally:
            for fixture, values in ((fixture_b, values_b), (fixture_a, values_a)):
                if values is not None:
                    _, capture, prefilter, _, _, _ = values
                    scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                        fixture, capture, prefilter
                    )
                else:
                    fixture.close()

    def test_relay_facade_rejects_non_scenario_requests_before_upstream(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
        values = lease = facade = None
        calls = []
        try:
            values = self._prepared(fixture)
            config, capture, prefilter, window, grid, rows = values
            row = rows[0]
            scenario = scan._issue_validated_replay_scenario(
                staging=prefilter, window=window, grid=grid,
                scenario_key=row["scenario_key"],
            )

            def operation(body, _remaining):
                calls.append(body)
                identifier = json.loads(body.decode("utf-8"))["id"]
                return json.dumps({
                    "id": identifier, "jsonrpc": "2.0", "result": "0x1",
                }, sort_keys=True, separators=(",", ":")).encode("ascii")

            lease = rpc._issue_historical_relay_lease_for_test(
                endpoint="https://fixture.invalid/archive",
                operation=operation,
                monotonic=_Clock([float(value) for value in range(200)]),
                entropy=lambda size: b"r" * size,
            )
            facade = rpc._bind_historical_relay_scenario(
                relay_lease=lease, config=config, scenario=scenario,
                absolute_deadline=120.0,
            )
            tag = {
                "blockHash": row["block_hash"],
                "requireCanonical": True,
            }
            token = config.authority.value["tokens"][0]["address"]
            executor = config.authority.value["executor"]["address"]
            calldata = anvil._balance_of_calldata(executor)

            valid = (
                ("eth_chainId", []),
                ("eth_getBlockByNumber", [hex(row["block_number"]), False]),
                ("eth_getBlockByHash", [row["block_hash"], False]),
                ("eth_getCode", [token, tag]),
                ("eth_call", [{"to": token, "data": calldata}, tag]),
            )
            for identifier, (method, params) in enumerate(valid, 1):
                body = json.dumps({
                    "id": identifier, "jsonrpc": "2.0",
                    "method": method, "params": params,
                }, sort_keys=True, separators=(",", ":")).encode("ascii")
                rpc._relay_historical_archive_call(
                    relay_lease=facade, canonical_request_bytes=body
                )
            accepted = len(calls)
            invalid = (
                {"id": 20, "jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": [hex(row["block_number"] - 1), False]},
                {"id": 21, "jsonrpc": "2.0", "method": "eth_getBlockByHash", "params": ["0x" + "00" * 32, False]},
                {"id": 22, "jsonrpc": "2.0", "method": "eth_getCode", "params": ["0x" + "00" * 20, tag]},
                {"id": 23, "jsonrpc": "2.0", "method": "eth_call", "params": [{"to": token, "data": "0x00000000"}, tag]},
                {"id": 24, "jsonrpc": "2.0", "method": "eth_chainId", "params": [1]},
            )
            bad_bodies = [
                json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
                for value in invalid
            ]
            bad_bodies.extend((
                b'[{"id":25,"jsonrpc":"2.0","method":"eth_chainId","params":[]}]',
                b'{"id":26,"id":27,"jsonrpc":"2.0","method":"eth_chainId","params":[]}',
                b'{"id":0,"jsonrpc":"2.0","method":"eth_chainId","params":[]}',
                b"{" + b"x" * 4_194_304,
            ))
            for body in bad_bodies:
                with self.subTest(body_sha256=hashlib.sha256(body).hexdigest()):
                    with self.assertRaises(ValueError):
                        rpc._relay_historical_archive_call(
                            relay_lease=facade,
                            canonical_request_bytes=body,
                        )
                    self.assertEqual(len(calls), accepted)
        finally:
            if facade is not None:
                facade.close()
            elif lease is not None:
                lease.close()
            if values is not None:
                _, capture, prefilter, _, _, _ = values
                scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                    fixture, capture, prefilter
                )
            else:
                fixture.close()

    def test_relay_close_unblocks_and_joins_non_daemon_handler(self):
        import scripts.historical_foundry_anvil as anvil

        class Thread:
            daemon = False

            def __init__(self, alive=True):
                self.alive = alive
                self.joins = []

            def join(self, timeout):
                self.joins.append(timeout)

            def is_alive(self):
                return self.alive

        handler = Thread()
        main = Thread()

        class Request:
            def shutdown(self, _direction):
                handler.alive = False

            def close(self):
                return None

        class Server:
            daemon_threads = False
            block_on_close = False

            def shutdown(self):
                main.alive = False

            def server_close(self):
                return None

        state = {
            "server": Server(), "thread": main,
            "handlers": {handler}, "requests": {Request()},
        }
        budgets = iter((4.0, 3.0, 2.0, 1.0))
        self.assertIsNone(anvil._close_historical_relay_server(
            state=state, remaining=lambda cap: min(cap, next(budgets))
        ))
        self.assertEqual(main.joins, [2.0])
        self.assertEqual(handler.joins, [1.0])
        self.assertFalse(main.is_alive())
        self.assertFalse(handler.is_alive())

    def test_replay_context_requires_held_anvil_module_origin_and_identity(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_storage as storage

        fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
        values = None
        try:
            values = self._prepared(fixture)
            _, capture, prefilter, _, _, _ = values
            self.assertIsNone(storage._verify_historical_replay_module_source(
                staging=prefilter,
                module_name="scripts.historical_foundry_anvil",
                module=anvil,
            ))
            origin = anvil.__spec__.origin
            file_name = anvil.__file__
            for fake in (
                types.SimpleNamespace(
                    __spec__=types.SimpleNamespace(
                        name="scripts.historical_foundry_anvil_alias",
                        origin=origin,
                    ),
                    __file__=file_name,
                ),
                types.SimpleNamespace(
                    __spec__=types.SimpleNamespace(
                        name="scripts.historical_foundry_anvil",
                        origin=origin + ".alias",
                    ),
                    __file__=file_name,
                ),
            ):
                with self.subTest(spec_name=fake.__spec__.name):
                    with self.assertRaises(ValueError):
                        storage._verify_historical_replay_module_source(
                            staging=prefilter,
                            module_name="scripts.historical_foundry_anvil",
                            module=fake,
                        )
        finally:
            if values is not None:
                _, capture, prefilter, _, _, _ = values
                scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                    fixture, capture, prefilter
                )
            else:
                fixture.close()


class HistoricalFoundryOverlayTests(unittest.TestCase):
    """The KAT catches a wrong account, slot, amount, nonce, or fee envelope."""

    @classmethod
    def setUpClass(cls):
        cls.config = load_historical_foundry_config_set()
        cls.artifact = build_validated_executor_artifact(cls.config)

    def setUp(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        self.fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
        (
            self.config,
            self.capture,
            self.prefilter,
            self.window,
            self.grid,
            self.rows,
        ) = HistoricalFoundryScenarioAuthorityTests._prepared(self.fixture)
        self.scenario = scan._issue_validated_replay_scenario(
            staging=self.prefilter,
            window=self.window,
            grid=self.grid,
            scenario_key=self.rows[0]["scenario_key"],
        )
        self.relay = None
        self.context = None
        self.storage = storage

    def tearDown(self):
        if self.context is not None:
            self.context.close()
        elif self.relay is not None:
            try:
                self.relay.close()
            except BaseException:
                pass
        scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
            self.fixture, self.capture, self.prefilter
        )

    def _open_context(self):
        import scripts.historical_foundry_anvil as anvil
        hostile = tempfile.TemporaryDirectory()
        self.addCleanup(hostile.cleanup)
        for name in ("anvil", "forge", "cast", "solc"):
            path = os.path.join(hostile.name, name)
            with open(path, "wb") as handle:
                handle.write(b"#!/bin/sh\nexit 99\n")
            os.chmod(path, 0o700)
        with mock.patch.dict(os.environ, {"PATH": hostile.name}, clear=False):
            self.context = anvil.open_historical_replay_context(
                config=self.config,
                staging=self.prefilter,
                window=self.window,
                grid=self.grid,
                executor_artifact=self.artifact,
            )
        return anvil

    def test_overlay_known_answer_and_sender_funding_are_internal(self):
        anvil = self._open_context()
        override = anvil.build_historical_state_override(
            context=self.context, scenario=self.scenario
        )
        authority = self.config.authority.value
        policy = self.config.policy.value
        executor = authority["executor"]["address"]
        sender = authority["sender"]["address"]
        weth = next(row for row in authority["tokens"] if row["role"] == "weth")
        uni = next(row for row in authority["tokens"] if row["role"] == "uni")
        venues = {row["venue_id"]: row for row in authority["venues"]}
        amount_in = self.rows[0]["amount_weth_in_wei"]
        predicted_uni = self.rows[0]["first_amount_out_raw"]
        max_fee = (
            policy["fees"]["max_fee_multiplier"]
            * self.rows[0]["child_base_fee_wei"]
            + self.rows[0]["fee"]["p50_priority_fee_per_gas"]
        )
        self.assertEqual(
            override["accounts"][sender]["balance"],
            policy["execution"]["transaction_gas_limit"] * max_fee,
        )
        self.assertEqual(override["accounts"][sender]["nonce"], 0)
        self.assertEqual(override["accounts"][executor]["balance"], 0)
        self.assertEqual(override["accounts"][executor]["nonce"], 0)
        self.assertEqual(
            override["accounts"][executor]["code_sha256"],
            self.artifact.verified_identity["deployed_runtime_sha256"],
        )
        storage = override["accounts"]
        weth_balance_slot = solidity_balance_storage_key(
            executor, weth["balance_descriptor"]["slot"]
        )
        self.assertEqual(storage[weth["address"]]["storage"][weth_balance_slot], amount_in)
        uni_balance_slot = solidity_balance_storage_key(
            executor, uni["balance_descriptor"]["slot"]
        )
        self.assertEqual(storage[uni["address"]]["storage"][uni_balance_slot], 0)
        expected_allowances = (
            (weth, venues["uniswap_v2"], amount_in),
            (weth, venues["sushiswap_v2"], 0),
            (uni, venues["uniswap_v2"], 0),
            (uni, venues["sushiswap_v2"], predicted_uni),
        )
        for token, venue, expected in expected_allowances:
            slot = solidity_allowance_storage_key(
                executor,
                venue["router_address"],
                token["allowance_descriptor"]["slot"],
            )
            self.assertEqual(
                storage[token["address"]]["storage"][slot], expected
            )
        self.assertEqual(override["transaction"]["type"], "0x2")
        self.assertEqual(override["transaction"]["accessList"], [])
        self.assertEqual(override["transaction"]["nonce"], 0)
        self.assertEqual(override["transaction"]["value"], 0)
        self.assertEqual(
            override["synthetic_block"]["timestamp"],
            self.rows[0]["header"]["timestamp"] + 12,
        )
        self.assertEqual(
            override["synthetic_block"]["base_fee_per_gas"],
            self.rows[0]["child_base_fee_wei"],
        )
        self.assertEqual(
            tuple(override["changed_accounts"]),
            tuple(sorted(override["accounts"])),
        )
        self.assertNotIn("path", repr(override).lower())
        self.assertNotIn("fixture.invalid", repr(override))

    def test_runtime_byte_flip_fails_before_process_spawn(self):
        import scripts.historical_foundry_anvil as anvil

        runtime = self.artifact._deployed_runtime_for_state_override()
        object.__setattr__(
            self.artifact,
            "_deployed_runtime",
            runtime[:-1] + bytes((runtime[-1] ^ 1,)),
        )
        try:
            with mock.patch.object(
                anvil, "_start_historical_relay", side_effect=AssertionError
            ) as start:
                with self.assertRaises(ValueError):
                    self._open_context()
                start.assert_not_called()
        finally:
            object.__setattr__(self.artifact, "_deployed_runtime", runtime)

    @staticmethod
    def _canonical(value):
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")

    def _proof(
        self, scenario_key, receipt_sha256, trace_sha256, receipt, row=None
    ):
        row = self.rows[0] if row is None else row
        price = row["price"]
        with localcontext() as decimal_context:
            decimal_context.prec = 200
            gas_amount = format(
                Decimal(
                    receipt["gasUsed"] * receipt["effectiveGasPrice"]
                    * price["answer"]
                ) / Decimal(10 ** (18 + price["feed_decimals"])),
                "f",
            ).rstrip("0").rstrip(".") or "0"
        with localcontext() as decimal_context:
            decimal_context.prec = 200
            pool_amount = format(
                Decimal(
                    row["amount_weth_in_wei"] * price["answer"] * 3
                ) / Decimal(10 ** (18 + price["feed_decimals"]) * 1000),
                "f",
            ).rstrip("0").rstrip(".") or "0"
        second_venue = (
            "sushiswap_v2"
            if row["direction"] == "uniswap_to_sushiswap"
            else "uniswap_v2"
        )
        second_reserves = row["reserves"][second_venue]
        with localcontext() as decimal_context:
            decimal_context.prec = 200
            second_pool_amount = format(
                Decimal(
                    row["first_amount_out_raw"]
                    * second_reserves["reserve_weth_raw"]
                    * price["answer"] * 3
                ) / Decimal(
                    second_reserves["reserve_uni_raw"]
                    * 10 ** (18 + price["feed_decimals"]) * 1000
                ),
                "f",
            ).rstrip("0").rstrip(".") or "0"
        mev_amount = str(row["requested_notional_usd"] * 10 // 10_000)
        row_specs = (
            ("buy", "pool_swap_fee", "bounded_estimate", True, pool_amount, "30", "receipt"),
            ("buy", "router_or_integrator_fee", "bounded_estimate", False, "0", "0", "receipt"),
            ("buy", "token_transfer_tax", "bounded_estimate", False, "0", "0", "receipt"),
            ("sell", "pool_swap_fee", "bounded_estimate", True, second_pool_amount, "30", "receipt"),
            ("sell", "router_or_integrator_fee", "bounded_estimate", False, "0", "0", "receipt"),
            ("sell", "token_transfer_tax", "bounded_estimate", False, "0", "0", "receipt"),
            ("route", "network_gas", "assumed", False, gas_amount, None, "receipt"),
            ("route", "rebalancing_or_transfer", "not_applicable", False, None, None, "trace"),
            ("route", "mev_buffer", "assumed", False, mev_amount, "10", "policy"),
        )
        role_hash = {
            "receipt": receipt_sha256,
            "trace": trace_sha256,
            "policy": self.config.policy.physical_sha256,
        }
        rows = [{
            "grain": grain,
            "component": component,
            "value_status": status,
            "embedded": embedded,
            "amount_usd_exact": amount,
            "rate_bps_exact": rate,
            "proof_role": role,
            "proof_sha256": role_hash[role],
        } for grain, component, status, embedded, amount, rate, role in row_specs]
        proof = {
            "schema": "historical_foundry_cost_proof_inputs/v1",
            "scenario_key": scenario_key,
            "policy_sha256": self.config.policy.physical_sha256,
            "receipt_sha256": receipt_sha256,
            "trace_sha256": trace_sha256,
            "adapter_proof_sha256": self.artifact.verified_identity[
                "creation_bytecode_sha256"
            ],
            "rows": rows,
        }
        proof["proof_inputs_hash"] = hashlib.sha256(
            b"historical_foundry_cost_proof_inputs/v1\0"
            + self._canonical(proof)
        ).hexdigest()
        return proof

    def _quartet(self, override, row=None):
        row = self.rows[0] if row is None else row
        scenario_key = override["scenario_key"]
        overlay_bytes = self._canonical(override)
        receipt = {
            "schema": "historical_foundry_receipt/v1",
            "scenario_key": scenario_key,
            "status": 1,
            "blockNumber": override["synthetic_block"]["number"],
            "blockHash": "0x" + "b" * 64,
            "transactionIndex": 0,
            "gasUsed": 123456,
            "effectiveGasPrice": 7,
            "transactionHash": "0x" + "c" * 64,
        }
        receipt_bytes = self._canonical(receipt)
        trace = {
            "schema": "historical_foundry_trace/v1",
            "scenario_key": scenario_key,
            "failed": False,
            "gasprice_opcode_addresses": [],
            "calls": [],
        }
        pair_closure = {
            venue_id: {
                "pair_address": row["reserves"][venue_id]["pair_address"],
                "reserve_uni_raw": row["reserves"][venue_id]["reserve_uni_raw"],
                "reserve_weth_raw": row["reserves"][venue_id]["reserve_weth_raw"],
                "pair_uni_balance_raw": row["reserves"][venue_id]["reserve_uni_raw"],
                "pair_weth_balance_raw": row["reserves"][venue_id]["reserve_weth_raw"],
            } for venue_id in ("uniswap_v2", "sushiswap_v2")
        }
        second_venue = (
            "sushiswap_v2"
            if row["direction"] == "uniswap_to_sushiswap"
            else "uniswap_v2"
        )
        second_reserves = row["reserves"][second_venue]
        balances = {
            "initial_weth_raw": row["amount_weth_in_wei"],
            "initial_uni_raw": 0,
            "final_weth_raw": row["second_amount_out_raw"],
            "final_uni_raw": 0,
        }
        actual_deltas = {
            "first_leg_uni_raw": row["first_amount_out_raw"],
            "weth_raw": row["second_amount_out_raw"] - row["amount_weth_in_wei"],
            "residual_uni_raw": 0,
        }
        trace.update({
            "fork_header": dict(row["header"]),
            "pair_closure": pair_closure,
            "balances": balances,
            "actual_deltas": actual_deltas,
        })
        trace_decoded = self._canonical(trace)
        trace_bytes = gzip.compress(trace_decoded, mtime=0)
        receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
        trace_sha = hashlib.sha256(trace_bytes).hexdigest()
        result = {
            "schema": "historical_foundry_replay_result/v1",
            "scenario_key": scenario_key,
            "status": 1,
            "classification": "replay_success",
            "overlay_sha256": hashlib.sha256(overlay_bytes).hexdigest(),
            "receipt_sha256": receipt_sha,
            "trace_sha256": trace_sha,
            "fork_header": dict(row["header"]),
            "pair_closure": pair_closure,
            "balances": balances,
            "actual_deltas": actual_deltas,
            "gas": {
                "gas_used": receipt["gasUsed"],
                "effective_gas_price": receipt["effectiveGasPrice"],
                "gas_cost_wei": receipt["gasUsed"] * receipt["effectiveGasPrice"],
            },
            "receipt_closure": {
                "status": receipt["status"],
                "block_number": receipt["blockNumber"],
                "block_hash": receipt["blockHash"],
                "transaction_index": receipt["transactionIndex"],
                "transaction_hash": receipt["transactionHash"],
            },
            "trace_closure": {
                "failed": trace["failed"],
                "gasprice_opcode_addresses": trace["gasprice_opcode_addresses"],
                "calls": trace["calls"],
            },
            "proof_authority": {
                "policy_sha256": self.config.policy.physical_sha256,
                "authority_sha256": self.config.authority.physical_sha256,
                "toolchain_sha256": self.config.toolchain.physical_sha256,
                "adapter_proof_sha256": self.artifact.verified_identity[
                    "creation_bytecode_sha256"
                ],
                "executor_runtime_sha256": self.artifact.verified_identity[
                    "deployed_runtime_sha256"
                ],
                "requested_notional_usd": row["requested_notional_usd"],
                "amount_weth_in_wei": row["amount_weth_in_wei"],
                "actual_first_leg_uni_raw": row["first_amount_out_raw"],
                "direction": row["direction"],
                "second_leg_pair_address": second_reserves["pair_address"],
                "second_leg_reserve_uni_raw": second_reserves["reserve_uni_raw"],
                "second_leg_reserve_weth_raw": second_reserves["reserve_weth_raw"],
                "eth_usd_answer": row["price"]["answer"],
                "feed_decimals": row["price"]["feed_decimals"],
                "v2_fee_numerator": 997,
                "v2_fee_denominator": 1000,
                "acceptance_mev_bps": "10",
            },
            "cost_proof_inputs": self._proof(
                scenario_key, receipt_sha, trace_sha, receipt, row=row
            ),
        }
        return (
            ("overlay", overlay_bytes),
            ("receipt", receipt_bytes),
            ("trace", trace_bytes),
            ("result", self._canonical(result)),
        )

    def test_quartet_is_one_no_replace_transaction_and_status_one_proof_is_frozen(self):
        anvil = self._open_context()
        override = anvil.build_historical_state_override(
            context=self.context, scenario=self.scenario
        )
        before = self.prefilter.frozen_identity_projection()
        sink = anvil._open_scenario_evidence_sink(
            context=self.context, scenario=self.scenario
        )
        self.assertEqual(
            tuple(inspect.signature(type(sink).write_member).parameters),
            ("self", "role", "canonical_bytes"),
        )
        with self.assertRaises(ValueError):
            sink.write_member(role="receipt", canonical_bytes=b"{}")
        quartet = self._quartet(override)
        receipt = json.loads(quartet[1][1])
        proof = anvil._build_cost_proof_inputs(
            context=self.context,
            row=self.rows[0],
            receipt=receipt,
            token_deltas={
                "initial_weth_raw": self.rows[0]["amount_weth_in_wei"],
                "initial_uni_raw": 0,
                "actual_first_leg_uni_raw": self.rows[0]["first_amount_out_raw"],
                "final_weth_raw": self.rows[0]["second_amount_out_raw"],
                "residual_uni_raw": 0,
            },
            receipt_sha256=hashlib.sha256(quartet[1][1]).hexdigest(),
            trace_sha256=hashlib.sha256(quartet[2][1]).hexdigest(),
        )
        proof_rows = proof["rows"]
        second_venue = (
            "sushiswap_v2"
            if self.rows[0]["direction"] == "uniswap_to_sushiswap"
            else "uniswap_v2"
        )
        second_reserves = self.rows[0]["reserves"][second_venue]
        with localcontext() as decimal_context:
            decimal_context.prec = 200
            expected_second_pool = format(
                Decimal(
                    self.rows[0]["first_amount_out_raw"]
                    * second_reserves["reserve_weth_raw"]
                    * self.rows[0]["price"]["answer"] * 3
                ) / Decimal(
                    second_reserves["reserve_uni_raw"]
                    * 10 ** (18 + self.rows[0]["price"]["feed_decimals"])
                    * 1000
                ), "f",
            ).rstrip("0").rstrip(".") or "0"
        self.assertEqual(
            [row["amount_usd_exact"] for row in proof_rows[:6]],
            [
                "2.999999999999999997", "0", "0",
                expected_second_pool, "0", "0",
            ],
        )
        price = self.rows[0]["price"]
        gas_numerator = (
            receipt["gasUsed"] * receipt["effectiveGasPrice"]
            * price["answer"]
        )
        with localcontext() as decimal_context:
            decimal_context.prec = 200
            expected_gas = format(
                Decimal(gas_numerator)
                / Decimal(10 ** (18 + price["feed_decimals"])),
                "f",
            ).rstrip("0").rstrip(".") or "0"
        self.assertEqual(proof_rows[6]["amount_usd_exact"], expected_gas)
        self.assertEqual(proof_rows[7]["amount_usd_exact"], None)
        self.assertEqual(proof_rows[8]["amount_usd_exact"], "1")
        changed = dict(self.rows[0])
        changed["requested_notional_usd"] += 1
        with self.assertRaises(ValueError):
            anvil._build_cost_proof_inputs(
                context=self.context, row=changed, receipt=receipt,
                token_deltas={
                    "initial_weth_raw": self.rows[0]["amount_weth_in_wei"],
                    "initial_uni_raw": 0,
                    "actual_first_leg_uni_raw": self.rows[0]["first_amount_out_raw"],
                    "final_weth_raw": self.rows[0]["second_amount_out_raw"],
                    "residual_uni_raw": 0,
                },
                receipt_sha256=hashlib.sha256(quartet[1][1]).hexdigest(),
                trace_sha256=hashlib.sha256(quartet[2][1]).hexdigest(),
            )
        for role, payload in quartet[:3]:
            projection = sink.write_member(
                role=role, canonical_bytes=payload
            )
            self.assertEqual(projection["role"], role)
            self.assertEqual(projection["sha256"], hashlib.sha256(payload).hexdigest())
            self.assertNotIn("path", projection)
            self.assertNotIn(payload, projection.values())
            self.assertEqual(
                self.prefilter.frozen_identity_projection()["generation"], 2
            )
            with self.assertRaises(ValueError):
                sink.validated_ledger()
        final_projection = sink.write_member(
            role=quartet[3][0], canonical_bytes=quartet[3][1]
        )
        ledger = sink.validated_ledger()
        self.assertEqual(final_projection["role"], "result")
        self.assertEqual(ledger.generation, 3)
        self.assertEqual(ledger.scenario_count, 1)
        self.assertEqual(ledger.scenario_key, self.scenario.scenario_key)
        self.assertEqual(
            ledger.proof_inputs_hash,
            json.loads(quartet[3][1])["cost_proof_inputs"]["proof_inputs_hash"],
        )
        successor = ledger.staging_snapshot()
        after = successor.frozen_identity_projection()
        self.assertEqual(after["stage"], "replay_frozen")
        self.assertEqual(after["generation"], 3)
        self.assertEqual(
            after["quota_committed_member_count"],
            before["quota_committed_member_count"] + 4,
        )
        self.assertGreater(
            after["quota_committed_physical_bytes"],
            before["quota_committed_physical_bytes"],
        )
        successor.reread_frozen_members_unchanged()
        with self.assertRaises(ValueError):
            sink.write_member(role="result", canonical_bytes=quartet[3][1])
        self.prefilter = successor

    def test_scenario_member_exact_size_boundaries(self):
        import scripts.historical_foundry_storage as storage

        for role, limit in (
            ("overlay", 8_388_608),
            ("receipt", 8_388_608),
            ("result", 8_388_608),
            ("trace", 16_777_216),
        ):
            with self.subTest(role=role):
                self.assertIsNone(storage._validate_historical_scenario_member_size(
                    role=role, byte_count=limit
                ))
                with self.assertRaises(ValueError):
                    storage._validate_historical_scenario_member_size(
                        role=role, byte_count=limit + 1
                    )

    def test_quartet_failure_before_single_rename_exposes_no_formal_scenario(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_storage as storage

        self._open_context()
        override = anvil.build_historical_state_override(
            context=self.context, scenario=self.scenario
        )
        sink = anvil._open_scenario_evidence_sink(
            context=self.context, scenario=self.scenario
        )
        quartet = self._quartet(override)
        for role, payload in quartet[:3]:
            sink.write_member(role=role, canonical_bytes=payload)
        observed = []

        def fail_before_rename(phase):
            observed.append(phase)
            if phase == "pre_rename":
                self.assertEqual(
                    list(self.fixture.data_dir.rglob(self.scenario.scenario_key)),
                    [],
                )
                raise OSError("injected rename boundary")

        with mock.patch.object(
            storage, "_task6_commit_checkpoint",
            side_effect=fail_before_rename,
        ):
            with self.assertRaises(Exception):
                sink.write_member(
                    role=quartet[3][0], canonical_bytes=quartet[3][1]
                )
        self.assertIn("pre_rename", observed)
        self.assertEqual(
            list(self.fixture.data_dir.rglob(self.scenario.scenario_key)), []
        )

    def test_storage_recomputes_result_closure_and_proof_authority(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_storage as storage

        self._open_context()
        override = anvil.build_historical_state_override(
            context=self.context, scenario=self.scenario
        )
        quartet = self._quartet(override)
        members = dict(quartet)
        baseline = storage._validate_historical_quartet_for_test(
            staging=self.prefilter,
            scenario_key=self.scenario.scenario_key,
            members=members,
        )
        self.assertEqual(baseline["block_number"], self.rows[0]["block_number"])
        for axis, mutate in (
            ("header", lambda value: value["fork_header"].__setitem__("timestamp", value["fork_header"]["timestamp"] + 1)),
            ("initial_balance", lambda value: value["balances"].__setitem__("initial_weth_raw", value["balances"]["initial_weth_raw"] + 1)),
            ("delta", lambda value: value["actual_deltas"].__setitem__("weth_raw", value["actual_deltas"]["weth_raw"] + 1)),
            ("gas", lambda value: value["gas"].__setitem__("gas_cost_wei", value["gas"]["gas_cost_wei"] + 1)),
            ("receipt", lambda value: value["receipt_closure"].__setitem__("transaction_index", 1)),
            ("trace", lambda value: value["trace_closure"].__setitem__("failed", True)),
            ("policy", lambda value: value["proof_authority"].__setitem__("policy_sha256", "0" * 64)),
            ("artifact", lambda value: value["proof_authority"].__setitem__("adapter_proof_sha256", "0" * 64)),
            ("fee", lambda value: value["proof_authority"].__setitem__("v2_fee_numerator", 996)),
            ("notional", lambda value: value["proof_authority"].__setitem__("requested_notional_usd", value["proof_authority"]["requested_notional_usd"] + 1)),
            ("leg_input", lambda value: value["proof_authority"].__setitem__("actual_first_leg_uni_raw", value["proof_authority"]["actual_first_leg_uni_raw"] + 1)),
            ("second_reserve", lambda value: value["proof_authority"].__setitem__("second_leg_reserve_uni_raw", value["proof_authority"]["second_leg_reserve_uni_raw"] + 1)),
        ):
            changed = json.loads(members["result"])
            mutate(changed)
            changed_members = dict(members)
            changed_members["result"] = self._canonical(changed)
            with self.subTest(axis=axis):
                with self.assertRaises(ValueError):
                    storage._validate_historical_quartet_for_test(
                        staging=self.prefilter,
                        scenario_key=self.scenario.scenario_key,
                        members=changed_members,
                    )
        changed = json.loads(members["result"])
        changed_proof = changed["cost_proof_inputs"]
        changed_proof["rows"][3]["amount_usd_exact"] = (
            changed_proof["rows"][0]["amount_usd_exact"]
        )
        unhashed = dict(changed_proof)
        unhashed.pop("proof_inputs_hash")
        changed_proof["proof_inputs_hash"] = hashlib.sha256(
            b"historical_foundry_cost_proof_inputs/v1\0"
            + self._canonical(unhashed)
        ).hexdigest()
        changed_members = dict(members)
        changed_members["result"] = self._canonical(changed)
        with self.assertRaises(ValueError):
            storage._validate_historical_quartet_for_test(
                staging=self.prefilter,
                scenario_key=self.scenario.scenario_key,
                members=changed_members,
            )

    def test_successor_authority_advances_context_and_rejects_stale_handles(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan

        self._open_context()
        run_lease = self.context._relay_lease
        run_key = object.__getattribute__(run_lease, "_key")
        run_deadline = object.__getattribute__(run_lease, "_run_deadline")
        self.assertIs(self.context._clock, run_lease._clock)
        self.assertEqual(self.context._run_deadline, run_deadline)
        override = anvil.build_historical_state_override(
            context=self.context, scenario=self.scenario
        )
        sink = anvil._open_scenario_evidence_sink(
            context=self.context, scenario=self.scenario
        )
        for role, payload in self._quartet(override):
            sink.write_member(role=role, canonical_bytes=payload)
        ledger = sink.validated_ledger()
        old_snapshot = self.prefilter
        old_window = self.window
        old_grid = self.grid
        self.assertIsNone(anvil._advance_historical_replay_context(
            context=self.context, ledger=ledger
        ))
        successor = ledger.staging_snapshot()
        self.assertEqual(
            successor.frozen_identity_projection()["generation"], 3
        )
        with self.assertRaises(Exception):
            scan._issue_validated_replay_scenario(
                staging=old_snapshot, window=old_window, grid=old_grid,
                scenario_key=self.rows[1]["scenario_key"],
            )
        second = anvil._issue_next_historical_replay_scenario(
            context=self.context, scenario_key=self.rows[1]["scenario_key"]
        )
        anchor = anvil._bind_historical_final_anchor_relay(
            context=self.context, scenario=second
        )
        self.assertIs(anchor._lease, run_lease)
        self.assertIs(object.__getattribute__(run_lease, "_key"), run_key)
        anchor.close()
        self.assertIs(object.__getattribute__(run_lease, "_key"), run_key)
        with self.assertRaises(ValueError):
            anvil._open_scenario_evidence_sink(
                context=self.context, scenario=self.scenario
            )
        second_override = anvil.build_historical_state_override(
            context=self.context, scenario=second
        )
        self.assertEqual(second_override["scenario_key"], self.rows[1]["scenario_key"])
        second_sink = anvil._open_scenario_evidence_sink(
            context=self.context, scenario=second
        )
        for role, payload in self._quartet(second_override, row=self.rows[1]):
            second_sink.write_member(role=role, canonical_bytes=payload)
        second_ledger = second_sink.validated_ledger()
        self.assertEqual(second_ledger.generation, 4)
        self.assertEqual(second_ledger.scenario_count, 2)
        self.assertIsNone(anvil._advance_historical_replay_context(
            context=self.context, ledger=second_ledger
        ))
        self.assertIs(self.context._relay_lease, run_lease)
        self.assertIs(object.__getattribute__(run_lease, "_key"), run_key)
        self.assertEqual(self.context._run_deadline, run_deadline)
        self.prefilter = second_ledger.staging_snapshot()


class HistoricalFoundryClosedRevertTests(unittest.TestCase):
    def test_exact_outer_and_inner_revert_axes_close_only_the_allowlisted_case(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        unit = 10 ** 18
        fixture = scan_fixtures._Task4bOfflineCapabilityFixture(
            split_reserve_root=False,
            record_calls=False,
            reserve_by_target={
                scan_fixtures.PAIR_UNISWAP: (0, unit),
                scan_fixtures.PAIR_SUSHI: (unit, unit),
            },
        )
        capture = prefilter = context = relay = None
        try:
            config, capture, prefilter, window, grid, rows = (
                HistoricalFoundryScenarioAuthorityTests._prepared(fixture)
            )
            row = rows[0]
            self.assertEqual(row["reason"], "first_leg_zero_output")
            scenario = scan._issue_validated_replay_scenario(
                staging=prefilter,
                window=window,
                grid=grid,
                scenario_key=row["scenario_key"],
            )
            context = anvil.open_historical_replay_context(
                config=config,
                staging=prefilter,
                window=window,
                grid=grid,
                executor_artifact=build_validated_executor_artifact(config),
            )
            matrix = config.policy.value["closed_revert_matrix"][0]
            router = next(
                venue["router_address"]
                for venue in config.authority.value["venues"]
                if venue["venue_id"] == "uniswap_v2"
            )
            outer = "0x" + keccak256(b"ExternalCallFailed()")[:4].hex()
            receipt = {"status": 0, "revert_data": outer}
            inner = {
                "call_path": [0],
                "leg": "first_leg",
                "router": router,
                "revert_selector": matrix["revert_selector"],
                "revert_data_sha256": matrix["revert_data_sha256"],
            }
            trace = {"failed": True, "calls": [inner]}
            self.assertEqual(
                anvil._classify_historical_revert(
                    context=context, scenario=scenario,
                    receipt=receipt, trace=trace,
                ),
                "closed_revert",
            )
            self.assertEqual(
                anvil._classify_historical_outcome(
                    context=context, scenario=scenario,
                    receipt=receipt, trace=trace,
                ),
                "closed_revert",
            )
            mutations = {
                "outer": (receipt, {"status": 0, "revert_data": "0x00000000"}),
                "selector": (inner, dict(inner, revert_selector="0x00000000")),
                "data_hash": (inner, dict(inner, revert_data_sha256="0" * 64)),
                "router": (inner, dict(inner, router="0x" + "0" * 40)),
                "leg": (inner, dict(inner, leg="second_leg")),
                "call_path": (inner, dict(inner, call_path=[1])),
            }
            for axis, (original, changed) in mutations.items():
                with self.subTest(axis=axis):
                    changed_receipt = changed if original is receipt else receipt
                    changed_trace = (
                        trace if original is receipt
                        else {"failed": True, "calls": [changed]}
                    )
                    self.assertEqual(
                        anvil._classify_historical_revert(
                            context=context, scenario=scenario,
                            receipt=changed_receipt, trace=changed_trace,
                        ),
                        "unresolved",
                    )
            second_leg = next(
                candidate for candidate in rows
                if candidate["reason"] == "second_leg_zero_output"
            )
            second_scenario = scan._issue_validated_replay_scenario(
                staging=prefilter, window=window, grid=grid,
                scenario_key=second_leg["scenario_key"],
            )
            self.assertEqual(
                anvil._classify_historical_revert(
                    context=context, scenario=second_scenario,
                    receipt=receipt, trace=trace,
                ),
                "unresolved",
            )
        finally:
            if context is not None:
                context.close()
            elif relay is not None:
                relay.close()
            scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                fixture, capture, prefilter
            )


class HistoricalFoundryOfflineRepeatTests(unittest.TestCase):
    def test_pair_closure_and_full_type2_envelope_reject_one_field_drift(self):
        import scripts.historical_foundry_anvil as anvil

        pair_state = {
            "uniswap_v2": {
                "pair_address": "0x" + "11" * 20,
                "reserve_uni_raw": 10,
                "reserve_weth_raw": 20,
                "pair_uni_balance_raw": 10,
                "pair_weth_balance_raw": 20,
            },
            "sushiswap_v2": {
                "pair_address": "0x" + "22" * 20,
                "reserve_uni_raw": 30,
                "reserve_weth_raw": 40,
                "pair_uni_balance_raw": 30,
                "pair_weth_balance_raw": 40,
            },
        }
        self.assertEqual(
            anvil._validate_historical_pair_closure(
                expected=pair_state, before=pair_state, after=pair_state
            ),
            pair_state,
        )
        for phase in ("before", "after"):
            changed = copy.deepcopy(pair_state)
            changed["uniswap_v2"]["pair_uni_balance_raw"] += 1
            arguments = {
                "expected": pair_state, "before": pair_state,
                "after": pair_state,
            }
            arguments[phase] = changed
            with self.subTest(pair_phase=phase):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_pair_closure(**arguments)

        tx_hash = "0x" + "33" * 32
        block_hash = "0x" + "44" * 32
        expected = {
            "type": "0x2", "from": "0x" + "55" * 20,
            "to": "0x" + "66" * 20, "nonce": 0, "gas": 100,
            "maxPriorityFeePerGas": 2, "maxFeePerGas": 9,
            "accessList": [], "value": 0, "input": "0x12345678",
        }
        raw = {
            "type": "0x2", "from": expected["from"], "to": expected["to"],
            "chainId": "0x1", "nonce": "0x0", "gas": "0x64",
            "maxPriorityFeePerGas": "0x2", "maxFeePerGas": "0x9",
            "value": "0x0", "input": expected["input"], "accessList": [],
            "hash": tx_hash, "blockHash": block_hash,
            "blockNumber": "0x3", "transactionIndex": "0x0",
        }
        self.assertIsNone(anvil._validate_historical_transaction_envelope(
            raw_transaction=raw, expected_transaction=expected,
            transaction_hash=tx_hash, block_hash=block_hash,
            block_number=3, transaction_index=0, chain_id=1,
        ))
        mutations = {
            "type": "0x1", "from": "0x" + "77" * 20,
            "to": "0x" + "77" * 20, "chainId": "0x2",
            "nonce": "0x1", "gas": "0x65",
            "maxPriorityFeePerGas": "0x3", "maxFeePerGas": "0xa",
            "value": "0x1", "input": "0x12345679",
            "accessList": [{"address": expected["to"], "storageKeys": []}],
            "hash": "0x" + "88" * 32, "blockHash": "0x" + "99" * 32,
            "blockNumber": "0x4", "transactionIndex": "0x1",
        }
        for field, value in mutations.items():
            changed = dict(raw); changed[field] = value
            with self.subTest(envelope_field=field):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_transaction_envelope(
                        raw_transaction=changed, expected_transaction=expected,
                        transaction_hash=tx_hash, block_hash=block_hash,
                        block_number=3, transaction_index=0, chain_id=1,
                    )

    def test_trace_transfer_and_real_revert_paths_are_strict(self):
        import scripts.historical_foundry_anvil as anvil

        sender = "0x" + "10" * 20
        executor = "0x" + "20" * 20
        first_router = "0x" + "30" * 20
        second_router = "0x" + "40" * 20
        pair = "0x" + "50" * 20
        uni = "0x" + "60" * 20
        calldata = "0x12345678"
        raw_trace = {
            "gas": 100, "failed": False, "returnValue": "",
            "structLogs": [{
                "pc": 0, "op": "PUSH1", "gas": 100, "gasCost": 3,
                "depth": 1, "stack": [], "memory": [], "storage": {},
            }],
        }
        self.assertEqual(
            anvil._validate_historical_raw_trace(
                raw_trace=raw_trace, expected_failed=False
            ),
            [],
        )
        for missing in ("gas", "failed", "returnValue", "structLogs"):
            changed = dict(raw_trace); changed.pop(missing)
            with self.subTest(missing_trace=missing):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_raw_trace(
                        raw_trace=changed, expected_failed=False
                    )
        for missing in ("pc", "op", "gas", "gasCost", "depth", "stack", "memory", "storage"):
            changed = copy.deepcopy(raw_trace)
            changed["structLogs"][0].pop(missing)
            with self.subTest(missing_step=missing):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_raw_trace(
                        raw_trace=changed, expected_failed=False
                    )
        changed = copy.deepcopy(raw_trace)
        changed["structLogs"][0]["op"] = "GASPRICE"
        self.assertEqual(anvil._validate_historical_raw_trace(
            raw_trace=changed, expected_failed=False
        ), ["GASPRICE"])

        transfer_topic = "0x" + keccak256(
            b"Transfer(address,address,uint256)"
        ).hex()
        log = {
            "address": uni,
            "topics": [
                transfer_topic, "0x" + "00" * 12 + pair[2:],
                "0x" + "00" * 12 + executor[2:],
            ],
            "data": "0x" + (123).to_bytes(32, "big").hex(),
            "logIndex": "0x2", "transactionIndex": "0x0",
            "removed": False,
        }
        self.assertEqual(anvil._extract_actual_first_leg_uni_raw(
            raw_receipt={"logs": [log]}, uni_address=uni,
            executor_address=executor, pair_address=pair,
        ), 123)
        for axis, changed_log in (
            ("sender", dict(log, topics=[transfer_topic, "0x" + "00" * 12 + sender[2:], log["topics"][2]])),
        ):
            with self.subTest(transfer_axis=axis):
                with self.assertRaises(ValueError):
                    anvil._extract_actual_first_leg_uni_raw(
                        raw_receipt={"logs": [changed_log]}, uni_address=uni,
                        executor_address=executor, pair_address=pair,
                    )
        unordered = {
            "logs": [
                dict(log, address="0x" + "61" * 20, logIndex="0x3"),
                log,
            ]
        }
        with self.assertRaises(ValueError):
            anvil._extract_actual_first_leg_uni_raw(
                raw_receipt=unordered, uni_address=uni,
                executor_address=executor, pair_address=pair,
            )
        with self.assertRaises(ValueError):
            anvil._extract_actual_first_leg_uni_raw(
                raw_receipt={"logs": [log, dict(log, logIndex="0x3")]},
                uni_address=uni, executor_address=executor, pair_address=pair,
            )

        revert_data = "0xdeadbeef01"
        call_trace = {
            "type": "CALL", "from": sender, "to": executor,
            "input": calldata, "output": "0x", "value": "0x0",
            "gas": "0x100", "gasUsed": "0x80", "error": "execution reverted",
            "calls": [{
                "type": "CALL", "from": executor, "to": first_router,
                "input": "0xabcdef01", "output": revert_data,
                "value": "0x0", "gas": "0x80", "gasUsed": "0x40",
                "error": "execution reverted", "calls": [],
            }],
        }
        normalized = anvil._normalized_failed_router_calls(
            call_trace=call_trace,
            router_order=(first_router, second_router),
            root_sender=sender, root_executor=executor,
            root_input=calldata, root_failed=True,
        )
        self.assertEqual(normalized[0]["call_path"], [0])
        nested = copy.deepcopy(call_trace)
        nested_child = copy.deepcopy(call_trace["calls"][0])
        nested_child["from"] = "0x" + "70" * 20
        nested["calls"] = [{
            "type": "CALL", "from": executor, "to": "0x" + "70" * 20,
            "input": "0x", "output": "0x", "value": "0x0",
            "gas": "0x80", "gasUsed": "0x40", "calls": [nested_child],
        }]
        self.assertEqual(anvil._normalized_failed_router_calls(
            call_trace=nested, router_order=(first_router, second_router),
            root_sender=sender, root_executor=executor,
            root_input=calldata, root_failed=True,
        )[0]["call_path"], [0, 0])
        for axis, mutate in (
            ("root_sender", lambda value: value.__setitem__("from", pair)),
            ("root_input", lambda value: value.__setitem__("input", "0x")),
            ("wrong_parent", lambda value: value["calls"][0].__setitem__("from", pair)),
            ("duplicate", lambda value: value["calls"].append(copy.deepcopy(value["calls"][0]))),
        ):
            changed = copy.deepcopy(call_trace); mutate(changed)
            with self.subTest(call_axis=axis):
                with self.assertRaises(ValueError):
                    anvil._normalized_failed_router_calls(
                        call_trace=changed,
                        router_order=(first_router, second_router),
                        root_sender=sender, root_executor=executor,
                        root_input=calldata, root_failed=True,
                    )

    def test_status_one_proof_row_semantics_are_closed(self):
        import scripts.historical_foundry_storage as storage

        specs = (
            ("buy", "pool_swap_fee", "bounded_estimate", True, "3", "30", "receipt"),
            ("buy", "router_or_integrator_fee", "bounded_estimate", False, "0", "0", "receipt"),
            ("buy", "token_transfer_tax", "bounded_estimate", False, "0", "0", "receipt"),
            ("sell", "pool_swap_fee", "bounded_estimate", True, "3", "30", "receipt"),
            ("sell", "router_or_integrator_fee", "bounded_estimate", False, "0", "0", "receipt"),
            ("sell", "token_transfer_tax", "bounded_estimate", False, "0", "0", "receipt"),
            ("route", "network_gas", "assumed", False, "0.0001", None, "receipt"),
            ("route", "rebalancing_or_transfer", "not_applicable", False, None, None, "trace"),
            ("route", "mev_buffer", "assumed", False, "1", "10", "policy"),
        )
        rows = [{
            "grain": grain, "component": component,
            "value_status": status, "embedded": embedded,
            "amount_usd_exact": amount, "rate_bps_exact": rate,
            "proof_role": role, "proof_sha256": "a" * 64,
        } for grain, component, status, embedded, amount, rate, role in specs]
        self.assertIsNone(storage._validate_historical_cost_proof_rows(rows))
        for axis, index, key, value in (
            ("order", 0, "grain", "sell"),
            ("status", 0, "value_status", "assumed"),
            ("embedded", 0, "embedded", False),
            ("pool_rate", 0, "rate_bps_exact", "29"),
            ("zero_fee", 1, "amount_usd_exact", "0.1"),
            ("gas_rate", 6, "rate_bps_exact", "0"),
            ("transfer_amount", 7, "amount_usd_exact", "0"),
            ("mev_rate", 8, "rate_bps_exact", "11"),
            ("decimal", 8, "amount_usd_exact", "1.0"),
        ):
            changed = [dict(row) for row in rows]
            changed[index][key] = value
            with self.subTest(axis=axis):
                with self.assertRaises(ValueError):
                    storage._validate_historical_cost_proof_rows(changed)

    def test_scenario_deadline_and_actual_transfer_delta_boundaries(self):
        import scripts.historical_foundry_anvil as anvil

        self.assertIsNone(anvil._validate_historical_scenario_elapsed(119.999999))
        for elapsed in (120.0, 120.000001):
            with self.subTest(elapsed=elapsed):
                with self.assertRaises(TimeoutError):
                    anvil._validate_historical_scenario_elapsed(elapsed)

        executor = "0x" + "12" * 20
        uni = "0x" + "34" * 20
        pair = "0x" + "56" * 20
        transfer_topic = "0x" + keccak256(
            b"Transfer(address,address,uint256)"
        ).hex()
        receipt = {"logs": [{
            "address": uni,
            "topics": [
                transfer_topic,
                "0x" + "00" * 12 + pair[2:],
                "0x" + "00" * 12 + executor[2:],
            ],
            "data": "0x" + (123).to_bytes(32, "big").hex(),
            "logIndex": "0x0",
            "transactionIndex": "0x0",
            "removed": False,
        }]}
        self.assertEqual(
            anvil._extract_actual_first_leg_uni_raw(
                raw_receipt=receipt, uni_address=uni,
                executor_address=executor, pair_address=pair,
            ),
            123,
        )
        for changed in (
            {"logs": []},
            {"logs": [dict(receipt["logs"][0], data="0x01")]},
            {"logs": [dict(receipt["logs"][0], address="0x" + "ff" * 20)]},
        ):
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    anvil._extract_actual_first_leg_uni_raw(
                        raw_receipt=changed, uni_address=uni,
                        executor_address=executor, pair_address=pair,
                    )

        expected_header = {
            "number": 123,
            "hash": "0x" + "11" * 32,
            "parent_hash": "0x" + "22" * 32,
            "state_root": "0x" + "33" * 32,
            "timestamp": 1_700_000_000,
            "gas_limit": 30_000_000,
            "gas_used": 15_000_000,
            "base_fee_per_gas": 1_000_000_000,
        }
        raw_header = {
            "number": hex(expected_header["number"]),
            "hash": expected_header["hash"],
            "parentHash": expected_header["parent_hash"],
            "stateRoot": expected_header["state_root"],
            "timestamp": hex(expected_header["timestamp"]),
            "gasLimit": hex(expected_header["gas_limit"]),
            "gasUsed": hex(expected_header["gas_used"]),
            "baseFeePerGas": hex(expected_header["base_fee_per_gas"]),
            "transactions": [],
        }
        self.assertIsNone(anvil._validate_historical_fork_base_header(
            raw_header=raw_header, expected_header=expected_header
        ))
        raw_names = {
            "number": "number", "hash": "hash",
            "parent_hash": "parentHash", "state_root": "stateRoot",
            "timestamp": "timestamp", "gas_limit": "gasLimit",
            "gas_used": "gasUsed", "base_fee_per_gas": "baseFeePerGas",
        }
        for normalized_name, raw_name in raw_names.items():
            changed = dict(raw_header)
            changed[raw_name] = (
                "0x" + "44" * 32
                if normalized_name in ("hash", "parent_hash", "state_root")
                else hex(expected_header[normalized_name] + 1)
            )
            with self.subTest(header_axis=normalized_name):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_fork_base_header(
                        raw_header=changed, expected_header=expected_header
                    )

    def test_local_rpc_exact_limits_and_deadline(self):
        import scripts.historical_foundry_anvil as anvil

        allowed = (
            "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash",
            "eth_getTransactionByHash",
            "anvil_setBalance", "anvil_setNonce", "anvil_setCode",
            "anvil_setStorageAt", "eth_getBalance", "eth_getTransactionCount",
            "eth_getCode", "eth_getStorageAt", "evm_setNextBlockTimestamp",
            "anvil_setNextBlockBaseFeePerGas", "eth_sendTransaction",
            "anvil_mine", "eth_getTransactionReceipt", "debug_traceTransaction",
            "eth_call", "evm_setAutomine", "anvil_impersonateAccount",
            "anvil_stopImpersonatingAccount",
        )
        self.assertEqual(anvil._HISTORICAL_LOCAL_RPC_METHODS, frozenset(allowed))
        for method in allowed:
            with self.subTest(method=method):
                self.assertIsNone(anvil._validate_historical_local_rpc_call(
                    method=method,
                    request_byte_count=4_194_304,
                    decoded_response_byte_count=67_108_864,
                    elapsed_seconds=29.999999,
                ))
        for axis, request_size, response_size, elapsed in (
            ("request", 4_194_305, 0, 0.0),
            ("response", 0, 67_108_865, 0.0),
            ("deadline_equal", 0, 0, 30.0),
            ("deadline_plus", 0, 0, 30.000001),
        ):
            with self.subTest(axis=axis):
                error = TimeoutError if axis.startswith("deadline") else ValueError
                with self.assertRaises(error):
                    anvil._validate_historical_local_rpc_call(
                        method="eth_chainId",
                        request_byte_count=request_size,
                        decoded_response_byte_count=response_size,
                        elapsed_seconds=elapsed,
                    )
        with self.assertRaises(ValueError):
            anvil._validate_historical_local_rpc_call(
                method="eth_sign", request_byte_count=1,
                decoded_response_byte_count=1, elapsed_seconds=0.0,
            )

    def test_run_and_scenario_absolute_deadlines_bound_every_own_cap(self):
        import scripts.historical_foundry_anvil as anvil

        self.assertEqual(anvil._remaining_historical_deadline(
            run_deadline=21_600.0, scenario_deadline=120.0,
            now=90.0, own_cap=30.0,
        ), 30.0)
        self.assertEqual(anvil._remaining_historical_deadline(
            run_deadline=100.0, scenario_deadline=120.0,
            now=90.0, own_cap=30.0,
        ), 10.0)
        for axis, values in (
            ("scenario_equal", (21_600.0, 120.0, 120.0, 30.0)),
            ("scenario_plus", (21_600.0, 120.0, 120.000001, 30.0)),
            ("run_equal", (100.0, 120.0, 100.0, 30.0)),
            ("run_plus", (100.0, 120.0, 100.000001, 30.0)),
        ):
            with self.subTest(axis=axis):
                with self.assertRaises(TimeoutError):
                    anvil._remaining_historical_deadline(
                        run_deadline=values[0], scenario_deadline=values[1],
                        now=values[2], own_cap=values[3],
                    )

    def test_replay_error_categories_are_closed_and_sanitized(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_rpc as rpc

        cases = (
            (ValueError("historical fork base differs"), "fork_window_mixed"),
            (toolchain._error("fork_window_mixed"), "fork_window_mixed"),
            (toolchain._error("fork_hardfork_unsupported"), "fork_hardfork_unsupported"),
            (ValueError("historical scenario lineage differs"), "authority"),
            (rpc._archive_error(("archive_state_unavailable", "transport_unavailable")), "archive"),
            (TimeoutError("secret endpoint /tmp/private --fork-url"), "foundry_replay_failed"),
            (RuntimeError("secret endpoint /tmp/private --fork-url"), "foundry_replay_failed"),
        )
        for error, category in cases:
            with self.subTest(category=category):
                typed = anvil._typed_historical_replay_error(error)
                self.assertIs(type(typed), anvil.HistoricalReplayError)
                self.assertEqual(typed.category, category)
                rendered = repr(typed) + str(typed)
                self.assertNotIn("secret", rendered)
                self.assertNotIn("/tmp", rendered)
                self.assertNotIn("--fork-url", rendered)

    def test_two_repeats_use_independent_real_local_rpc_processes(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan

        projections = []
        processes = []

        for repeat in range(2):
            fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
            capture = prefilter = context = successor = None
            fixture_directory = tempfile.TemporaryDirectory()
            try:
                config, capture, prefilter, window, grid, rows = (
                    HistoricalFoundryScenarioAuthorityTests._prepared(fixture)
                )
                scenario = scan._issue_validated_replay_scenario(
                    staging=prefilter, window=window, grid=grid,
                    scenario_key=rows[0]["scenario_key"],
                )
                context = anvil.open_historical_replay_context(
                    config=config, staging=prefilter, window=window, grid=grid,
                    executor_artifact=build_validated_executor_artifact(config),
                )
                sink = anvil._open_scenario_evidence_sink(
                    context=context, scenario=scenario
                )
                override = anvil.build_historical_state_override(
                    context=context, scenario=scenario
                )
                authority = config.authority.value
                tokens = {row["role"]: row for row in authority["tokens"]}
                ordered_roles = sorted(
                    ("uni", "weth"), key=lambda role: tokens[role]["address"]
                )
                pairs = {}
                for venue_id in ("uniswap_v2", "sushiswap_v2"):
                    reserve = rows[0]["reserves"][venue_id]
                    by_role = {
                        "uni": reserve["reserve_uni_raw"],
                        "weth": reserve["reserve_weth_raw"],
                    }
                    pairs[reserve["pair_address"]] = {
                        "word0": by_role[ordered_roles[0]],
                        "word1": by_role[ordered_roles[1]],
                        "timestamp": reserve["pair_timestamp"],
                        "uni": by_role["uni"], "weth": by_role["weth"],
                    }
                first_venue = (
                    "uniswap_v2" if rows[0]["direction"] == "uniswap_to_sushiswap"
                    else "sushiswap_v2"
                )
                fixture_config = {
                    "header": dict(rows[0]["header"]),
                    "synthetic_number": override["synthetic_block"]["number"],
                    "synthetic_timestamp": override["synthetic_block"]["timestamp"],
                    "synthetic_base_fee": override["synthetic_block"]["base_fee_per_gas"],
                    "child_hash": "0x" + "b" * 64,
                    "transaction_hash": "0x" + "c" * 64,
                    "gas_used": 123456, "effective_gas_price": 1,
                    "transaction": dict(override["transaction"]),
                    "pairs": pairs,
                    "token_roles": {
                        tokens["uni"]["address"]: "uni",
                        tokens["weth"]["address"]: "weth",
                    },
                    "executor": authority["executor"]["address"],
                    "uni": tokens["uni"]["address"],
                    "initial_weth": rows[0]["amount_weth_in_wei"],
                    "first_uni": rows[0]["first_amount_out_raw"],
                    "final_weth": rows[0]["second_amount_out_raw"],
                    "residual_uni": 0,
                    "first_pair": rows[0]["reserves"][first_venue]["pair_address"],
                    "transfer_topic": "0x" + keccak256(
                        b"Transfer(address,address,uint256)"
                    ).hex(),
                }
                config_path = os.path.join(fixture_directory.name, "fixture.json")
                with open(config_path, "w", encoding="utf-8") as handle:
                    json.dump(fixture_config, handle, sort_keys=True)

                def spawn(*, selected_block, hardfork, relay_port, anvil_port):
                    del relay_port
                    process = subprocess.Popen(
                        [
                            sys.executable, "-c",
                            "from tests.test_historical_foundry_anvil import "
                            "_serve_historical_anvil_fixture as serve; serve()",
                            str(anvil_port), config_path,
                        ],
                        cwd=os.getcwd(), stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        env={"LANG": "C", "LC_ALL": "C"},
                    )
                    processes.append(process)
                    return toolchain._issue_historical_process_lease_for_test(
                        process=process, cleanup=mock.Mock(),
                        binary_sha256="9" * 64,
                        selected_block=selected_block, hardfork=hardfork,
                    )

                original_getaddrinfo = socket.getaddrinfo

                def loopback_only(host, *args, **kwargs):
                    if host != "127.0.0.1":
                        raise AssertionError("external network forbidden")
                    return original_getaddrinfo(host, *args, **kwargs)

                with mock.patch.object(
                    type(context._toolchain),
                    "_spawn_historical_anvil_process",
                    side_effect=spawn,
                ), mock.patch.object(
                    socket, "getaddrinfo", side_effect=loopback_only
                ), mock.patch.dict(os.environ, {}, clear=True):
                    projections.append(anvil._replay_historical_scenario(
                        context=context, scenario=scenario, sink=sink
                    ))
                ledger = sink.validated_ledger()
                self.assertEqual(ledger.generation, 3)
                successor = ledger.staging_snapshot()
            finally:
                if context is not None:
                    context.close()
                if successor is not None:
                    successor.close()
                fixture_directory.cleanup()
                scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                    fixture, capture, prefilter
                )
        self.assertEqual(projections[0], projections[1])
        self.assertIsNot(processes[0], processes[1])
        self.assertNotEqual(processes[0].pid, processes[1].pid)
        self.assertTrue(all(process.poll() is not None for process in processes))
        for projection in projections:
            self.assertNotIn("path", projection)
            self.assertNotIn("endpoint", repr(projection).lower())
            self.assertEqual(projection["gas_used"], 123456)


if __name__ == "__main__":
    unittest.main()
