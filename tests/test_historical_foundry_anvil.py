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
import signal
import socket
import subprocess
import sys
import tempfile
import time
import types
import unittest
from decimal import Decimal, localcontext
from types import MappingProxyType
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
        block_hash = "0x" + "44" * 32
        fork_header = {
            "number": 1, "hash": block_hash,
            "parent_hash": "0x" + "43" * 32,
            "state_root": "0x" + "45" * 32,
            "timestamp": 2, "gas_limit": 30_000_000,
            "gas_used": 15_000_000, "base_fee_per_gas": 7,
        }
        raw_block = {
            "number": "0x1", "hash": block_hash,
            "parentHash": fork_header["parent_hash"],
            "stateRoot": fork_header["state_root"],
            "timestamp": "0x2", "gasLimit": hex(30_000_000),
            "gasUsed": hex(15_000_000), "baseFeePerGas": "0x7",
            "transactions": [],
        }

        def operation(body, remaining):
            calls.append((body, remaining))
            request = json.loads(body)
            result = (
                raw_block if request["method"] in (
                    "eth_getBlockByNumber", "eth_getBlockByHash"
                ) else "0x1"
            )
            if request["method"] == "eth_chainId":
                return b'{"jsonrpc":"2.0", "result":"0x1", "id":7}'
            return json.dumps({
                "id": 7, "jsonrpc": "2.0", "result": result,
            }, sort_keys=True, separators=(",", ":")).encode("ascii")

        lease = rpc._issue_historical_relay_lease_for_test(
            endpoint="https://fixture.invalid/archive?key=secret",
            operation=operation,
            monotonic=_Clock([0.0, 1.0, 2.0, 3.0, 4.0]),
            entropy=lambda size: b"k" * size,
        )
        address = "0x" + "11" * 20
        slot = "0x" + "22" * 32
        calldata = "0x" + "33" * 4
        facade = rpc._issue_historical_relay_scenario_facade(
            relay_lease=lease,
            authority={
                "block_number": 1,
                "block_hash": block_hash,
                "fork_header": fork_header,
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
                    response = rpc._relay_historical_archive_call(
                            relay_lease=facade,
                            canonical_request_bytes=request,
                        )
                    self.assertEqual(json.loads(response)["id"], 7)
                    if method == "eth_chainId":
                        self.assertEqual(
                            response,
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

    def test_upstream_response_after_absolute_deadline_is_rejected(self):
        import scripts.historical_foundry_rpc as rpc

        request = (
            b'{"id":1,"jsonrpc":"2.0","method":"eth_chainId",'
            b'"params":[]}'
        )
        for axis, clock_values, scenario_deadline in (
            ("scenario_equal", [0.0, 1.0, 119.0, 120.0], 120.0),
            ("run_equal", [0.0, 1.0, 21_599.0, 21_600.0], 21_600.0),
        ):
            with self.subTest(axis=axis):
                lease = rpc._issue_historical_relay_lease_for_test(
                    endpoint="https://fixture.invalid/hidden",
                    operation=lambda _body, _remaining: (
                        b'{"id":1,"jsonrpc":"2.0","result":"0x1"}'
                    ),
                    monotonic=_Clock(clock_values),
                    entropy=lambda size: b"k" * size,
                )
                facade = rpc._issue_historical_relay_scenario_facade(
                    relay_lease=lease,
                    authority={
                        "block_number": 1,
                        "block_hash": "0x" + "1" * 64,
                        "block_tag": {
                            "blockHash": "0x" + "1" * 64,
                            "requireCanonical": True,
                        },
                        "addresses": frozenset(), "calls": frozenset(),
                    },
                    absolute_deadline=scenario_deadline,
                )
                try:
                    with self.assertRaises(TimeoutError):
                        rpc._relay_historical_archive_call(
                            relay_lease=facade,
                            canonical_request_bytes=request,
                        )
                finally:
                    facade.close()
                    lease.close()


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


def _serve_historical_archive_fixture() -> None:
    """Serve the fixed fork state used by the real reviewed-Anvil KAT."""
    port = int(sys.argv[1])
    with open(sys.argv[2], "r", encoding="utf-8") as handle:
        config = json.load(handle)

    def word(value):
        return "0x" + int(value).to_bytes(32, "big").hex()

    def result(method, params):
        if method == "eth_chainId":
            return "0x1"
        if method in ("eth_getBlockByNumber", "eth_getBlockByHash"):
            return config["block"]
        if method == "eth_getCode":
            return config["codes"].get(params[0].lower(), "0x")
        if method in ("eth_getBalance", "eth_getTransactionCount"):
            return "0x0"
        if method == "eth_getStorageAt":
            return config["storage"].get(
                params[0].lower() + ":" + params[1].lower(), word(0)
            )
        if method == "eth_call":
            target = params[0]["to"].lower()
            data = params[0]["data"].lower()
            pair = config["pairs"].get(target)
            if pair is not None and data == "0x0902f1ac":
                return "0x" + "".join(
                    int(value).to_bytes(32, "big").hex()
                    for value in (
                        pair["reserve0"], pair["reserve1"],
                        pair["timestamp"],
                    )
                )
            if data.startswith("0x70a08231") and len(data) == 74:
                owner = "0x" + data[-40:]
                return word(config["balances"].get(
                    target + ":" + owner, 0
                ))
            return word(0)
        raise RuntimeError("unsupported archive fixture method")

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format, *args):
            del args

        def do_POST(self):
            request = json.loads(self.rfile.read(
                int(self.headers["Content-Length"])
            ))
            with open(config["log_path"], "a", encoding="utf-8") as log:
                log.write(json.dumps(request, sort_keys=True) + "\n")
            try:
                payload = {
                    "id": request["id"], "jsonrpc": "2.0",
                    "result": result(request["method"], request["params"]),
                }
            except Exception:
                payload = {
                    "error": {"code": -32000, "message": "fixture failed"},
                    "id": request.get("id"), "jsonrpc": "2.0",
                }
            body = json.dumps(
                payload, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()

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
                self.assertEqual(
                    projection["fixed_arguments"],
                    (
                        "--chain-id", "1", "--fork-chain-id", "1",
                        "--accounts", "0", "--gas-price", "0",
                        "--disable-default-create2-deployer",
                        "--host", "127.0.0.1", "--no-mining", "--no-cors",
                        "--silent", "--order", "fifo", "--steps-tracing",
                        "--retries", "0", "--timeout", "30000",
                        "--no-storage-caching",
                    ),
                )
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

    def test_reaped_child_retains_cleanup_authority_until_retry_succeeds(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        process = _Process([0])
        cleanup = mock.Mock(side_effect=(ValueError("identity drift"), None))
        lease = toolchain._issue_historical_process_lease_for_test(
            process=process, cleanup=cleanup,
            binary_sha256="7" * 64, selected_block=123,
            hardfork="osaka",
        )
        with self.assertRaises(ValueError):
            lease.close()
        self.assertFalse(lease._closed)
        self.assertIs(lease._process, process)
        self.assertIs(lease._cleanup, cleanup)
        self.assertEqual(process.calls, [("terminate",), ("wait", 5.0)])
        self.assertIsNone(lease.close())
        self.assertTrue(lease._closed)
        self.assertEqual(process.calls, [("terminate",), ("wait", 5.0)])
        self.assertEqual(cleanup.call_count, 2)

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

    def test_process_output_drainers_are_tracked_non_daemon_threads(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        created = []

        class Thread:
            def __init__(self, **kwargs):
                created.append(kwargs)
                self.daemon = kwargs.get("daemon")

            def start(self):
                return None

            def join(self, timeout):
                del timeout

            def is_alive(self):
                return False

        process = _Process([0])
        process.stdout = io.BytesIO(b"")
        process.stderr = io.BytesIO(b"")
        with mock.patch.object(toolchain.threading, "Thread", Thread):
            lease = toolchain._issue_historical_process_lease_for_test(
                process=process, cleanup=mock.Mock(),
                binary_sha256="8" * 64, selected_block=123,
                hardfork="osaka",
            )
        self.assertEqual(len(created), 2)
        self.assertTrue(all(row["daemon"] is False for row in created))
        lease.close()

    def test_drainer_control_is_rethrown_by_identity_after_bounded_cleanup(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        control = KeyboardInterrupt()

        class Stream:
            def read(self, _size):
                raise control

            def close(self):
                return None

        process = _Process([0])
        process.stdout = Stream()
        process.stderr = io.BytesIO(b"")
        cleanup = mock.Mock()
        lease = toolchain._issue_historical_process_lease_for_test(
            process=process, cleanup=cleanup,
            binary_sha256="9" * 64, selected_block=123,
            hardfork="osaka",
        )
        with self.assertRaises(KeyboardInterrupt) as raised:
            lease.close()
        self.assertIs(raised.exception, control)
        cleanup.assert_called_once_with()
        self.assertTrue(lease._closed)

    def test_reaped_child_with_live_drainer_retains_every_cleanup_reference(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        class Thread:
            alive = True

            def join(self, timeout):
                del timeout

            def is_alive(self):
                return self.alive

        thread = Thread()
        process = _Process([0])
        cleanup = mock.Mock()
        lease = toolchain._issue_historical_process_lease_for_test(
            process=process, cleanup=cleanup,
            binary_sha256="a" * 64, selected_block=123,
            hardfork="osaka",
        )
        object.__setattr__(lease, "_output_threads", (thread,))
        with self.assertRaises(ValueError):
            lease.close()
        cleanup.assert_not_called()
        self.assertIs(lease._process, process)
        self.assertIs(lease._cleanup, cleanup)
        self.assertFalse(lease._closed)
        thread.alive = False
        self.assertIsNone(lease.close())
        cleanup.assert_called_once_with()

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

    def test_private_materialization_rejects_symlink_and_post_spawn_substitution(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        reviewed = toolchain.open_reviewed_historical_toolchain()
        private_parent = os.getcwd()
        try:
            symlink_directory = tempfile.mkdtemp(
                prefix="materialization-symlink-", dir=private_parent
            )
            os.symlink("/bin/true", os.path.join(
                symlink_directory, ".reviewed-anvil"
            ))
            with mock.patch.object(
                toolchain.tempfile, "mkdtemp",
                return_value=symlink_directory,
            ), mock.patch.object(
                toolchain, "_darwin_spawn_suspended"
            ) as spawn:
                with self.assertRaises(Exception) as raised:
                    reviewed._spawn_historical_anvil_process(
                        selected_block=1, hardfork="osaka",
                        relay_port=31001, anvil_port=31002,
                    )
                spawn.assert_not_called()
            self.assertFalse(
                os.path.exists(symlink_directory), repr(raised.exception)
            )

            substitution_directory = tempfile.mkdtemp(
                prefix="materialization-substitution-", dir=private_parent
            )
            real_stat = toolchain.os.stat
            executable_stats = []

            def substituted_stat(path, *args, **kwargs):
                observed = real_stat(path, *args, **kwargs)
                if path == ".reviewed-anvil":
                    executable_stats.append(observed)
                    if len(executable_stats) == 2:
                        values = list(observed)
                        values[1] += 1
                        return os.stat_result(values)
                return observed

            process = mock.Mock()
            process.poll.return_value = None
            process.wait.return_value = 0
            identity = {
                "schema": "historical_foundry_anvil_launch_identity/v1",
                "binary_sha256": "5c9f9aad323062b1c0421a63595741430acaea150da3611e38c45071e4cf4e28",
                "cdhash": "561b69d0257e574c3438465eb55cf4cef6852abc",
                "main_image_matches_materialized_inode": True,
                "resumed_after_identity_verification": True,
            }
            with mock.patch.object(
                toolchain.tempfile, "mkdtemp",
                return_value=substitution_directory,
            ), mock.patch.object(
                toolchain.os, "stat", side_effect=substituted_stat,
            ), mock.patch.object(
                toolchain, "_darwin_spawn_suspended",
                return_value=(process, identity),
            ):
                with self.assertRaises(Exception):
                    reviewed._spawn_historical_anvil_process(
                        selected_block=1, hardfork="osaka",
                        relay_port=31003, anvil_port=31004,
                    )
            process.terminate.assert_called_once_with()
            process.wait.assert_called_once()
            self.assertFalse(os.path.exists(substitution_directory))

            before = {
                name for name in os.listdir(private_parent)
                if name.startswith(".historical-anvil-scenario-")
            }
            reaped = _Process([0])
            with mock.patch.object(
                toolchain, "_darwin_spawn_suspended",
                return_value=(reaped, identity),
            ):
                lease = reviewed._spawn_historical_anvil_process(
                    selected_block=1, hardfork="osaka",
                    relay_port=31005, anvil_port=31006,
                )
            after = {
                name for name in os.listdir(private_parent)
                if name.startswith(".historical-anvil-scenario-")
            }
            created = after - before
            self.assertEqual(len(created), 1)
            materialized_directory = os.path.join(
                private_parent, created.pop()
            )
            executable = os.path.join(
                materialized_directory, ".reviewed-anvil"
            )
            held = os.path.join(materialized_directory, ".held-anvil")
            os.rename(executable, held)
            with open(executable, "wb") as handle:
                handle.write(b"substituted")
            os.chmod(executable, 0o700)
            with self.assertRaises(ValueError):
                lease.close()
            self.assertFalse(lease._closed)
            self.assertTrue(os.path.isdir(materialized_directory))
            os.unlink(executable)
            os.rename(held, executable)
            self.assertIsNone(lease.close())
            self.assertFalse(os.path.exists(materialized_directory))
        finally:
            reviewed._close()

    def test_production_spawn_uses_suspended_darwin_identity_gate_and_no_popen(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        source = inspect.getsource(
            toolchain.ReviewedHistoricalToolchain._spawn_historical_anvil_process
        )
        self.assertNotIn("Popen", source)
        self.assertEqual(toolchain._DARWIN_POSIX_SPAWN_START_SUSPENDED, 0x0080)
        self.assertEqual(toolchain._DARWIN_POSIX_SPAWN_CLOEXEC_DEFAULT, 0x4000)
        self.assertEqual(
            toolchain._DARWIN_EXPECTED_ANVIL_CDHASH,
            "561b69d0257e574c3438465eb55cf4cef6852abc",
        )

    def test_identity_failure_never_resumes_and_emergency_lease_reaps(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        reviewed = toolchain.open_reviewed_historical_toolchain()
        observed_signals = []
        original_kill = toolchain.os.kill

        def observe_kill(pid, selected_signal):
            observed_signals.append(selected_signal)
            return original_kill(pid, selected_signal)

        try:
            with mock.patch.object(
                toolchain, "_darwin_verified_launch_identity",
                side_effect=toolchain.HistoricalFoundryToolchainError(
                    "toolchain_process_identity_mismatch"
                ),
            ), mock.patch.object(
                toolchain.os, "kill", side_effect=observe_kill,
            ):
                with self.assertRaises(
                    toolchain.HistoricalFoundryToolchainError
                ):
                    reviewed._spawn_historical_anvil_process(
                        selected_block=1, hardfork="osaka",
                        relay_port=31101, anvil_port=31102,
                    )
            self.assertNotIn(signal.SIGCONT, observed_signals)
            self.assertIn(signal.SIGKILL, observed_signals)
            self.assertEqual(reviewed._process_leases, {})
        finally:
            reviewed._close()

    def test_pending_emergency_lease_is_registered_before_spawn_call(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain

        reviewed = toolchain.open_reviewed_historical_toolchain()
        observed = []

        def fail_inside_spawn(**_values):
            leases = tuple(reviewed._process_leases.values())
            self.assertEqual(len(leases), 1)
            self.assertIsInstance(
                leases[0], toolchain._PendingHistoricalSpawnLease
            )
            observed.append(True)
            raise OSError("injected posix_spawn failure")

        try:
            with mock.patch.object(
                toolchain, "_darwin_spawn_suspended",
                side_effect=fail_inside_spawn,
            ):
                with self.assertRaises(Exception):
                    reviewed._spawn_historical_anvil_process(
                        selected_block=1, hardfork="osaka",
                        relay_port=31103, anvil_port=31104,
                    )
            self.assertEqual(observed, [True])
            self.assertEqual(reviewed._process_leases, {})
        finally:
            reviewed._close()


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

            raw_block = {
                "number": hex(row["header"]["number"]),
                "hash": row["header"]["hash"],
                "parentHash": row["header"]["parent_hash"],
                "stateRoot": row["header"]["state_root"],
                "timestamp": hex(row["header"]["timestamp"]),
                "gasLimit": hex(row["header"]["gas_limit"]),
                "gasUsed": hex(row["header"]["gas_used"]),
                "baseFeePerGas": hex(row["header"]["base_fee_per_gas"]),
                "transactions": [],
            }
            self.assertEqual(
                scan._normalized_from_raw(raw_block), dict(row["header"])
            )

            def operation(body, _remaining):
                calls.append(body)
                request = json.loads(body.decode("utf-8"))
                identifier = request["id"]
                result = (
                    raw_block if request["method"] in (
                        "eth_getBlockByNumber", "eth_getBlockByHash"
                    ) else "0x1"
                )
                return json.dumps({
                    "id": identifier, "jsonrpc": "2.0", "result": result,
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
            pair = row["reserves"]["uniswap_v2"]["pair_address"]
            osaka_history_storage = (
                "0x0000f90827f1c53a10cb7a02335b175320002935"
            )

            valid = (
                ("eth_chainId", []),
                ("eth_getBlockByNumber", [hex(row["block_number"]), False]),
                ("eth_getBlockByNumber", [hex(row["block_number"]), True]),
                ("eth_getBlockByHash", [row["block_hash"], False]),
                ("eth_getCode", [token, tag]),
                ("eth_getCode", [osaka_history_storage, hex(row["block_number"])]),
                ("eth_getStorageAt", [pair, "0x0", hex(row["block_number"])]),
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
            anvil_wire_body = (
                b'{"method":"eth_chainId","params":[],"id":0,'
                b'"jsonrpc":"2.0"}'
            )
            rpc._relay_historical_archive_call(
                relay_lease=facade, canonical_request_bytes=anvil_wire_body
            )
            self.assertEqual(
                calls[-1],
                b'{"id":0,"jsonrpc":"2.0","method":"eth_chainId",'
                b'"params":[]}',
            )
            full_transaction = {
                "hash": "0x" + "12" * 32,
                "blockHash": row["block_hash"],
                "blockNumber": hex(row["block_number"]),
                "transactionIndex": "0x0",
            }
            full_block = dict(raw_block, transactions=[full_transaction])
            full_request = {
                "id": 40, "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(row["block_number"]), True],
            }
            rpc._validate_historical_relay_response(
                authority=facade._authority, request=full_request,
                response={"id": 40, "jsonrpc": "2.0", "result": full_block},
            )
            for axis, mutate in (
                ("block_hash", lambda value: value.__setitem__(
                    "hash", "0x" + "00" * 32
                )),
                ("tx_block", lambda value: value["transactions"][0].__setitem__(
                    "blockHash", "0x" + "00" * 32
                )),
                ("tx_number", lambda value: value["transactions"][0].__setitem__(
                    "blockNumber", "0x0"
                )),
                ("tx_index", lambda value: value["transactions"][0].__setitem__(
                    "transactionIndex", "0x1"
                )),
                ("tx_hash", lambda value: value["transactions"][0].__setitem__(
                    "hash", "0x1"
                )),
                ("duplicate", lambda value: value["transactions"].append(
                    copy.deepcopy(value["transactions"][0])
                )),
            ):
                changed = copy.deepcopy(full_block)
                mutate(changed)
                with self.subTest(response_axis=axis):
                    with self.assertRaises(ValueError):
                        rpc._validate_historical_relay_response(
                            authority=facade._authority,
                            request=full_request,
                            response={
                                "id": 40, "jsonrpc": "2.0",
                                "result": changed,
                            },
                        )
            accepted = len(calls)
            invalid = (
                {"id": 20, "jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": [hex(row["block_number"] - 1), False]},
                {"id": 21, "jsonrpc": "2.0", "method": "eth_getBlockByHash", "params": ["0x" + "00" * 32, False]},
                {"id": 22, "jsonrpc": "2.0", "method": "eth_getCode", "params": ["0x" + "00" * 20, tag]},
                {"id": 23, "jsonrpc": "2.0", "method": "eth_call", "params": [{"to": token, "data": "0x00000000"}, tag]},
                {"id": 24, "jsonrpc": "2.0", "method": "eth_chainId", "params": [1]},
                {"id": 25, "jsonrpc": "2.0", "method": "eth_getStorageAt", "params": [pair, "0x00", hex(row["block_number"])]},
            )
            bad_bodies = [
                json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
                for value in invalid
            ]
            bad_bodies.extend((
                b'[{"id":25,"jsonrpc":"2.0","method":"eth_chainId","params":[]}]',
                b'{"id":26,"id":27,"jsonrpc":"2.0","method":"eth_chainId","params":[]}',
                b'{"id":-1,"jsonrpc":"2.0","method":"eth_chainId","params":[]}',
                b'{"id":true,"jsonrpc":"2.0","method":"eth_chainId","params":[]}',
                b'{"id":0.0,"jsonrpc":"2.0","method":"eth_chainId","params":[]}',
                b'{"id":"0","jsonrpc":"2.0","method":"eth_chainId","params":[]}',
                b'{"id":null,"jsonrpc":"2.0","method":"eth_chainId","params":[]}',
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

    def test_relay_handler_control_is_rethrown_by_context_owner_after_cleanup(self):
        anvil = self._open_context()
        closed_authorities = []

        class Authority:
            def close(self):
                closed_authorities.append(self)

        class ServerBase:
            def __init__(self, _address, _handler):
                self.server_address = ("127.0.0.1", 1)

            def serve_forever(self, **_kwargs):
                return None

        class Thread:
            def start(self):
                return None

        control = KeyboardInterrupt("private endpoint and path")
        with mock.patch.object(
            anvil.http.server, "ThreadingHTTPServer", ServerBase
        ), mock.patch.object(
            anvil.threading, "Thread", return_value=Thread()
        ), mock.patch.object(
            anvil, "_close_historical_relay_server", return_value=None
        ), mock.patch(
            "scripts.historical_foundry_rpc._bind_historical_relay_scenario",
            side_effect=lambda **_kwargs: Authority(),
        ):
            ordinary = anvil._start_historical_relay(
                context=self.context, scenario=self.scenario
            )
            ordinary._record_handler_failure_for_test(
                RuntimeError("private endpoint and path")
            )
            self.assertEqual(
                ordinary._diagnostics_for_test(),
                (("handler_error", "ordinary"),),
            )
            ordinary.close()
            relay = anvil._start_historical_relay(
                context=self.context, scenario=self.scenario
            )
            object.__setattr__(self.context, "_active_relay_lease", relay)
            relay._record_handler_failure_for_test(control)
            with self.assertRaises(KeyboardInterrupt) as raised:
                self.context.close()
        self.assertIs(raised.exception, control)
        self.assertTrue(relay._is_closed())
        self.assertIsNone(self.context._active_relay_lease)
        self.assertTrue(self.context._closed)
        self.assertEqual(len(closed_authorities), 2)

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

    def _quartet(self, override, row=None, *, revert_call=None):
        row = self.rows[0] if row is None else row
        scenario_key = override["scenario_key"]
        reverted = revert_call is not None
        receipt = {
            "schema": "historical_foundry_receipt/v1",
            "scenario_key": scenario_key,
            "status": 0 if reverted else 1,
            "blockNumber": override["synthetic_block"]["number"],
            "blockHash": "0x" + "b" * 64,
            "transactionIndex": 0,
            "gasUsed": 123456,
            "effectiveGasPrice": 7,
            "transactionHash": "0x" + "c" * 64,
        }
        if reverted:
            receipt["revert_data"] = "0x" + keccak256(
                b"ExternalCallFailed()"
            )[:4].hex()
        receipt_bytes = self._canonical(receipt)
        trace = {
            "schema": "historical_foundry_trace/v1",
            "scenario_key": scenario_key,
            "failed": reverted,
            "gasprice_opcode_addresses": [],
            "calls": [dict(revert_call)] if reverted else [],
        }
        trace_config = {
            "disableStack": False,
            "disableStorage": False,
            "enableMemory": True,
            "enableReturnData": True,
        }
        trace["struct_logs"] = [
            {
                "pc": 0, "op": "PUSH1", "gas": 100,
                "gasCost": 3, "depth": 1, "stack": [], "memory": [],
                "refund": 0, "returnData": "0x",
            },
            {
                "pc": 2, "op": "STOP", "gas": 97,
                "gasCost": 0, "depth": 1, "stack": [], "memory": [],
                "refund": 0, "returnData": "0x",
                "storage": {},
            },
        ]
        trace["raw_trace_closure"] = {
            "gas": receipt["gasUsed"],
            "failed": reverted,
            "return_value": "0xdeadbeef" if reverted else "0x",
        }
        trace["struct_log_storage"] = {
            "schema": "historical_foundry_sparse_storage_trace/v1",
            "anvil_binary_sha256": next(
                value["sha256"] for value in self.config.toolchain.value["binaries"]
                if value["name"] == "anvil"
            ),
            "trace_config_sha256": hashlib.sha256(
                self._canonical(trace_config)
            ).hexdigest(),
            "storage_omitted_step_count": 1,
            "storage_explicit_step_count": 1,
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
        override["pair_balance_baseline"] = {
            venue_id: {
                "pair_address": value["pair_address"],
                "pair_uni_balance_raw": value["pair_uni_balance_raw"],
                "pair_weth_balance_raw": value["pair_weth_balance_raw"],
            }
            for venue_id, value in pair_closure.items()
        }
        overlay_bytes = self._canonical(override)
        second_venue = (
            "sushiswap_v2"
            if row["direction"] == "uniswap_to_sushiswap"
            else "uniswap_v2"
        )
        second_reserves = row["reserves"][second_venue]
        balances = {
            "initial_weth_raw": row["amount_weth_in_wei"],
            "initial_uni_raw": 0,
            "final_weth_raw": (
                row["amount_weth_in_wei"]
                if reverted else row["second_amount_out_raw"]
            ),
            "final_uni_raw": 0,
        }
        actual_deltas = {
            "first_leg_uni_raw": 0 if reverted else row["first_amount_out_raw"],
            "weth_raw": (
                0 if reverted else row["second_amount_out_raw"]
                - row["amount_weth_in_wei"]
            ),
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
            "status": receipt["status"],
            "classification": "closed_revert" if reverted else "replay_success",
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
                "raw_trace_closure": trace["raw_trace_closure"],
                "struct_log_storage": trace["struct_log_storage"],
            },
            "proof_authority": {
                "policy_sha256": self.config.policy.physical_sha256,
                "authority_sha256": self.config.authority.physical_sha256,
                "toolchain_sha256": self.config.toolchain.physical_sha256,
                "executor_source_tree_sha256": self.artifact.verified_identity[
                    "source_tree_sha256"
                ],
                "executor_constructor_args_sha256": self.artifact.verified_identity[
                    "constructor_args_sha256"
                ],
                "anvil_binary_sha256": trace["struct_log_storage"][
                    "anvil_binary_sha256"
                ],
                "trace_config_sha256": trace["struct_log_storage"][
                    "trace_config_sha256"
                ],
                "adapter_proof_sha256": self.artifact.verified_identity[
                    "creation_bytecode_sha256"
                ],
                "executor_runtime_sha256": self.artifact.verified_identity[
                    "deployed_runtime_sha256"
                ],
                "executor_immutable_references_sha256": self.artifact.verified_identity[
                    "immutable_references_sha256"
                ],
                "executor_artifact_manifest_sha256": self.artifact.verified_identity[
                    "artifact_manifest_sha256"
                ],
                "requested_notional_usd": row["requested_notional_usd"],
                "amount_weth_in_wei": row["amount_weth_in_wei"],
                "actual_first_leg_uni_raw": (
                    0 if reverted else row["first_amount_out_raw"]
                ),
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
        }
        if not reverted:
            result["cost_proof_inputs"] = self._proof(
                scenario_key, receipt_sha, trace_sha, receipt, row=row
            )
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

    def test_quartet_precommit_rename_failure_restores_exact_retryable_predecessor(self):
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
        predecessor = self.prefilter.frozen_identity_projection()
        observed = []

        def fail_after_rename(phase):
            observed.append(phase)
            if phase == "after_formal_directory_rename":
                self.assertEqual(
                    len(storage.task6_transaction_registry), 1
                )
                transaction = next(iter(
                    storage.task6_transaction_registry.values()
                ))
                transaction["formal_installed"] = False
                raise OSError("injected post-rename boundary")

        with mock.patch.object(
            storage, "_task6_commit_checkpoint",
            side_effect=fail_after_rename,
        ):
            with self.assertRaises(Exception):
                sink.write_member(
                    role=quartet[3][0], canonical_bytes=quartet[3][1]
                )
        self.assertIn("after_formal_directory_rename", observed)
        self.assertEqual(
            list(self.fixture.data_dir.rglob(self.scenario.scenario_key)), []
        )
        self.assertEqual(
            self.prefilter.frozen_identity_projection(), predecessor
        )
        sink.write_member(
            role=quartet[3][0], canonical_bytes=quartet[3][1]
        )
        self.assertEqual(sink.validated_ledger().generation, 3)
        self.prefilter = sink.validated_ledger().staging_snapshot()

    def test_quartet_rename_return_before_hint_is_recovered_from_disk_identity(self):
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
        predecessor = self.prefilter.frozen_identity_projection()
        original = storage._task6_rename_directory_noreplace
        returned = [False]

        def fail_after_formal_rename(**values):
            original(**values)
            if values["destination_name"] == self.scenario.scenario_key:
                returned[0] = True
                raise OSError("injected after rename syscall return")

        with mock.patch.object(
            storage, "_task6_rename_directory_noreplace",
            side_effect=fail_after_formal_rename,
        ):
            with self.assertRaises(Exception):
                sink.write_member(
                    role=quartet[3][0], canonical_bytes=quartet[3][1]
                )
        self.assertTrue(returned[0])
        self.assertEqual(
            self.prefilter.frozen_identity_projection(), predecessor
        )
        self.assertEqual(
            list(self.fixture.data_dir.rglob(self.scenario.scenario_key)), []
        )
        sink.write_member(
            role=quartet[3][0], canonical_bytes=quartet[3][1]
        )
        self.prefilter = sink.validated_ledger().staging_snapshot()

    def test_quartet_journal_recovers_every_publication_boundary(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        phases = (
            "after_prepare_fsync",
            "after_owner_transaction_install",
            "after_journal_authority_install",
            "after_owner_materializing",
            "after_quota_owner_install",
            "after_foundry_directory_open",
            "after_block_directory_open",
            "after_scenario_directory_create",
            "after_member_directory_map_install",
            "after_member_file_overlay", "after_member_map_overlay",
            "after_member_file_receipt", "after_member_map_receipt",
            "after_member_file_trace", "after_member_map_trace",
            "after_member_file_result", "after_member_map_result",
            "after_formal_directory_rename",
            "after_formal_directory_name_install",
            "after_formal_directory_identity_install",
            "after_formal_quartet_install",
            "after_quota_install",
            "after_owner_scenarios_install",
            "after_owner_members_install",
            "after_owner_generation_install",
            "after_owner_state_install",
            "after_owner_projection_install",
            "after_owner_generation_counter_install",
            "after_owner_snapshot_handle_install",
            "after_owner_successor_install",
            "after_successor_registry_install",
            "after_ledger_registry_install",
            "after_sink_state_target_ready",
            "after_sink_target_ready",
            "after_target_audit",
            "after_commit_marker_rename",
        )
        for phase in phases:
            fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
            capture = prefilter = context = None
            with self.subTest(phase=phase):
                try:
                    config, capture, prefilter, window, grid, rows = (
                        HistoricalFoundryScenarioAuthorityTests._prepared(
                            fixture
                        )
                    )
                    scenario = scan._issue_validated_replay_scenario(
                        staging=prefilter, window=window, grid=grid,
                        scenario_key=rows[0]["scenario_key"],
                    )
                    context = anvil.open_historical_replay_context(
                        config=config, staging=prefilter, window=window,
                        grid=grid,
                        executor_artifact=build_validated_executor_artifact(
                            config
                        ),
                    )
                    helper = HistoricalFoundryOverlayTests(
                        "test_overlay_known_answer_and_sender_funding_are_internal"
                    )
                    helper.config = config
                    helper.artifact = context._artifact
                    helper.rows = rows
                    override = anvil.build_historical_state_override(
                        context=context, scenario=scenario
                    )
                    quartet = helper._quartet(override)
                    sink = anvil._open_scenario_evidence_sink(
                        context=context, scenario=scenario
                    )
                    for role, payload in quartet[:3]:
                        sink.write_member(
                            role=role, canonical_bytes=payload
                        )
                    predecessor = prefilter.frozen_identity_projection()

                    def fail_selected(observed):
                        if observed == phase:
                            raise OSError("injected transaction boundary")

                    with mock.patch.object(
                        storage, "_task6_commit_checkpoint",
                        side_effect=fail_selected,
                    ):
                        with self.assertRaises(Exception):
                            sink.write_member(
                                role=quartet[3][0],
                                canonical_bytes=quartet[3][1],
                            )
                    self.assertEqual(
                        prefilter.frozen_identity_projection(), predecessor
                    )
                    self.assertEqual(
                        list(fixture.data_dir.rglob(scenario.scenario_key)),
                        [],
                    )
                    sink.write_member(
                        role=quartet[3][0],
                        canonical_bytes=quartet[3][1],
                    )
                    self.assertEqual(sink.validated_ledger().generation, 3)
                    prefilter = sink.validated_ledger().staging_snapshot()
                finally:
                    if context is not None:
                        context.close()
                    scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                        fixture, capture, prefilter
                    )

    def test_journal_v2_prepare_is_durable_before_first_domain_mutation(self):
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
        predecessor = self.prefilter.frozen_identity_projection()
        observed = []

        def stop_after_prepare(phase):
            observed.append(phase)
            if phase != "after_prepare_fsync":
                return
            journals = list(
                self.fixture.data_dir.rglob(".transaction-*.PREPARE.json")
            )
            self.assertEqual(len(journals), 1)
            document = json.loads(journals[0].read_bytes())
            self.assertEqual(
                document["schema"],
                "historical_foundry_replay_transaction/v2",
            )
            self.assertEqual(document["state"], "PREPARED")
            self.assertEqual(document["predecessor"]["generation"], 2)
            self.assertEqual(document["target"]["generation"], 3)
            self.assertEqual(
                set(document["predecessor"]),
                {
                    "generation", "state", "owner_generation",
                    "projection", "members", "scenarios", "quota",
                    "opened_scenario_keys",
                },
            )
            self.assertEqual(
                set(document["target"]), set(document["predecessor"])
            )
            self.assertEqual(
                set(document["predecessor"]["quota"]),
                {
                    "committed_physical_bytes", "committed_members",
                    "provisional_physical_bytes", "provisional_members",
                    "reservation",
                },
            )
            self.assertIsNone(
                document["predecessor"]["quota"]["reservation"]
            )
            self.assertIsNone(document["target"]["quota"]["reservation"])
            self.assertEqual(
                document["predecessor"]["projection"], predecessor
            )
            self.assertEqual(
                self.prefilter.frozen_identity_projection(), predecessor
            )
            self.assertEqual(
                list(self.fixture.data_dir.rglob(self.scenario.scenario_key)),
                [],
            )
            raise OSError("injected after durable prepare")

        with mock.patch.object(
            storage, "_task6_commit_checkpoint", side_effect=stop_after_prepare
        ):
            with self.assertRaises(Exception):
                sink.write_member(
                    role=quartet[3][0], canonical_bytes=quartet[3][1]
                )
        self.assertIn("after_prepare_fsync", observed)
        self.assertEqual(
            self.prefilter.frozen_identity_projection(), predecessor
        )
        self.assertEqual(
            list(self.fixture.data_dir.rglob(self.scenario.scenario_key)), []
        )
        sink.write_member(
            role=quartet[3][0], canonical_bytes=quartet[3][1]
        )
        self.prefilter = sink.validated_ledger().staging_snapshot()

    def test_gen3_post_ledger_mutation_rolls_back_every_owner_and_handle(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_storage as storage

        self._open_context()
        first_override = anvil.build_historical_state_override(
            context=self.context, scenario=self.scenario
        )
        first_sink = anvil._open_scenario_evidence_sink(
            context=self.context, scenario=self.scenario
        )
        for role, payload in self._quartet(first_override):
            first_sink.write_member(role=role, canonical_bytes=payload)
        first_ledger = first_sink.validated_ledger()
        self.assertIsNone(anvil._advance_historical_replay_context(
            context=self.context, ledger=first_ledger
        ))
        self.prefilter = first_ledger.staging_snapshot()
        predecessor = self.prefilter.frozen_identity_projection()
        self.assertEqual(predecessor["generation"], 3)

        second = anvil._issue_next_historical_replay_scenario(
            context=self.context, scenario_key=self.rows[1]["scenario_key"]
        )
        second_override = anvil.build_historical_state_override(
            context=self.context, scenario=second
        )
        second_sink = anvil._open_scenario_evidence_sink(
            context=self.context, scenario=second
        )
        quartet = self._quartet(second_override, row=self.rows[1])
        for role, payload in quartet[:3]:
            second_sink.write_member(role=role, canonical_bytes=payload)
        observed = []

        def fail_after_real_ledger_mutation(phase):
            observed.append(phase)
            if phase == "after_ledger_registry_install":
                raise OSError("injected after ledger registry mutation")

        with mock.patch.object(
            storage, "_task6_commit_checkpoint",
            side_effect=fail_after_real_ledger_mutation,
        ):
            with self.assertRaises(Exception):
                second_sink.write_member(
                    role=quartet[3][0], canonical_bytes=quartet[3][1]
                )
        self.assertIn("after_ledger_registry_install", observed)
        self.assertEqual(
            self.prefilter.frozen_identity_projection(), predecessor
        )
        self.assertEqual(
            list(self.fixture.data_dir.rglob(second.scenario_key)), []
        )
        second_sink.write_member(
            role=quartet[3][0], canonical_bytes=quartet[3][1]
        )
        self.assertEqual(second_sink.validated_ledger().generation, 4)
        self.prefilter = second_sink.validated_ledger().staging_snapshot()

    def test_quartet_failed_rollback_retains_journal_and_blocks_consumers(self):
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
        predecessor = self.prefilter.frozen_identity_projection()
        failed_publication = [False]

        def fail_publication_and_rollback(phase):
            if phase == "after_formal_quartet_install":
                failed_publication[0] = True
                raise OSError("injected publication failure")
            if phase == "rollback" and failed_publication[0]:
                raise OSError("injected rollback failure")

        with mock.patch.object(
            storage, "_task6_commit_checkpoint",
            side_effect=fail_publication_and_rollback,
        ):
            with self.assertRaises(Exception):
                sink.write_member(
                    role=quartet[3][0], canonical_bytes=quartet[3][1]
                )
            with self.assertRaises(Exception):
                self.prefilter.frozen_identity_projection()
            with self.assertRaises(Exception):
                sink.validated_ledger()
        self.assertEqual(
            self.prefilter.frozen_identity_projection(), predecessor
        )
        self.assertEqual(
            list(self.fixture.data_dir.rglob(self.scenario.scenario_key)), []
        )

    def _retain_precommit_transaction(self):
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
        predecessor = self.prefilter.frozen_identity_projection()
        publication_failed = [False]

        def retain_journal(phase):
            if phase == "after_formal_quartet_install":
                publication_failed[0] = True
                raise OSError("injected publication failure")
            if phase == "rollback" and publication_failed[0]:
                raise OSError("injected rollback failure")

        with mock.patch.object(
            storage, "_task6_commit_checkpoint", side_effect=retain_journal
        ):
            with self.assertRaises(Exception):
                sink.write_member(
                    role=quartet[3][0], canonical_bytes=quartet[3][1]
                )
        journals = list(
            self.fixture.data_dir.rglob(".transaction-*.PREPARE.json")
        )
        self.assertEqual(len(journals), 1)
        storage._drop_historical_quartet_transaction_memory_for_test(
            self.prefilter
        )
        return sink, quartet, predecessor, journals[0]

    def test_quartet_disk_journal_recovers_after_transaction_registry_loss(self):
        sink, quartet, predecessor, journal = (
            self._retain_precommit_transaction()
        )
        self.assertTrue(journal.is_file())
        self.assertEqual(
            self.prefilter.frozen_identity_projection(), predecessor
        )
        self.assertFalse(journal.exists())
        self.assertEqual(
            list(self.fixture.data_dir.rglob(self.scenario.scenario_key)), []
        )
        sink.write_member(
            role=quartet[3][0], canonical_bytes=quartet[3][1]
        )
        self.assertEqual(sink.validated_ledger().generation, 3)
        self.prefilter = sink.validated_ledger().staging_snapshot()

    def test_committed_journal_completes_after_transaction_registry_loss(self):
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
        commit_seen = [False]

        def retain_committed_journal(phase):
            if phase == "after_commit_marker_fsync":
                commit_seen[0] = True
                raise OSError("injected after durable commit")
            if phase == "after_old_snapshot_retire" and commit_seen[0]:
                raise OSError("injected completion failure")

        with mock.patch.object(
            storage, "_task6_commit_checkpoint",
            side_effect=retain_committed_journal,
        ):
            with self.assertRaises(Exception):
                sink.write_member(
                    role=quartet[3][0], canonical_bytes=quartet[3][1]
                )
        journals = list(
            self.fixture.data_dir.rglob(".transaction-*.COMMITTED.json")
        )
        self.assertEqual(len(journals), 1)
        storage._drop_historical_quartet_transaction_memory_for_test(
            self.prefilter
        )
        self.assertEqual(sink.validated_ledger().generation, 3)
        successor = sink.validated_ledger().staging_snapshot()
        self.assertFalse(journals[0].exists())
        self.assertEqual(
            successor.frozen_identity_projection()["generation"], 3
        )
        self.prefilter = successor

    def test_every_postcommit_boundary_completes_without_rollback(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        phases = (
            "after_commit_marker_fsync",
            "after_old_snapshot_retire",
            "after_sink_result_install",
            "after_sink_ledger_install",
            "after_sink_commit",
        )
        for selected in phases:
            fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
            capture = prefilter = context = None
            with self.subTest(phase=selected):
                try:
                    config, capture, prefilter, window, grid, rows = (
                        HistoricalFoundryScenarioAuthorityTests._prepared(
                            fixture
                        )
                    )
                    scenario = scan._issue_validated_replay_scenario(
                        staging=prefilter, window=window, grid=grid,
                        scenario_key=rows[0]["scenario_key"],
                    )
                    context = anvil.open_historical_replay_context(
                        config=config, staging=prefilter, window=window,
                        grid=grid,
                        executor_artifact=build_validated_executor_artifact(
                            config
                        ),
                    )
                    helper = HistoricalFoundryOverlayTests(
                        "test_overlay_known_answer_and_sender_funding_are_internal"
                    )
                    helper.config = config
                    helper.artifact = context._artifact
                    helper.rows = rows
                    override = anvil.build_historical_state_override(
                        context=context, scenario=scenario
                    )
                    quartet = helper._quartet(override)
                    sink = anvil._open_scenario_evidence_sink(
                        context=context, scenario=scenario
                    )
                    for role, payload in quartet[:3]:
                        sink.write_member(role=role, canonical_bytes=payload)
                    failed = [False]

                    def fail_once_after_commit(phase):
                        if phase == selected and not failed[0]:
                            failed[0] = True
                            raise OSError("injected committed completion failure")

                    with mock.patch.object(
                        storage, "_task6_commit_checkpoint",
                        side_effect=fail_once_after_commit,
                    ):
                        with self.assertRaises(Exception):
                            sink.write_member(
                                role=quartet[3][0],
                                canonical_bytes=quartet[3][1],
                            )
                    self.assertTrue(failed[0])
                    ledger = sink.validated_ledger()
                    self.assertEqual(ledger.generation, 3)
                    self.assertEqual(
                        len(list(fixture.data_dir.rglob(
                            scenario.scenario_key
                        ))),
                        1,
                    )
                    self.assertEqual(
                        list(fixture.data_dir.rglob(".transaction-*.json")),
                        [],
                    )
                    prefilter = ledger.staging_snapshot()
                finally:
                    if context is not None:
                        context.close()
                    scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                        fixture, capture, prefilter
                    )

    def test_quartet_disk_journal_tamper_fails_closed_without_cross_delete(self):
        mutation_names = (
            "raw_byte", "transaction_id", "member_digest",
            "predecessor_quota", "predecessor_generation",
        )
        for axis in mutation_names:
            fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
            capture = prefilter = context = None
            with self.subTest(axis=axis):
                try:
                    import scripts.historical_foundry_anvil as anvil
                    import scripts.historical_foundry_scan as scan
                    import scripts.historical_foundry_storage as storage

                    config, capture, prefilter, window, grid, rows = (
                        HistoricalFoundryScenarioAuthorityTests._prepared(
                            fixture
                        )
                    )
                    scenario = scan._issue_validated_replay_scenario(
                        staging=prefilter, window=window, grid=grid,
                        scenario_key=rows[0]["scenario_key"],
                    )
                    context = anvil.open_historical_replay_context(
                        config=config, staging=prefilter, window=window,
                        grid=grid,
                        executor_artifact=build_validated_executor_artifact(
                            config
                        ),
                    )
                    helper = HistoricalFoundryOverlayTests(
                        "test_overlay_known_answer_and_sender_funding_are_internal"
                    )
                    helper.config = config
                    helper.artifact = context._artifact
                    helper.rows = rows
                    override = anvil.build_historical_state_override(
                        context=context, scenario=scenario
                    )
                    quartet = helper._quartet(override)
                    sink = anvil._open_scenario_evidence_sink(
                        context=context, scenario=scenario
                    )
                    for role, payload in quartet[:3]:
                        sink.write_member(role=role, canonical_bytes=payload)
                    publication_failed = [False]

                    def retain_journal(phase):
                        if phase == "after_formal_quartet_install":
                            publication_failed[0] = True
                            raise OSError("injected publication failure")
                        if phase == "rollback" and publication_failed[0]:
                            raise OSError("injected rollback failure")

                    with mock.patch.object(
                        storage, "_task6_commit_checkpoint",
                        side_effect=retain_journal,
                    ):
                        with self.assertRaises(Exception):
                            sink.write_member(
                                role=quartet[3][0],
                                canonical_bytes=quartet[3][1],
                            )
                    journal = next(
                        fixture.data_dir.rglob(
                            ".transaction-*.PREPARE.json"
                        )
                    )
                    original = journal.read_bytes()
                    document = json.loads(original)
                    if axis == "raw_byte":
                        mutated = original[:-2] + b"0}"
                    else:
                        if axis == "transaction_id":
                            document["transaction_id"] = "0" * 32
                        elif axis == "member_digest":
                            document["members"][0]["sha256"] = "0" * 64
                        elif axis == "predecessor_quota":
                            document["predecessor"]["quota"][
                                "committed_physical_bytes"
                            ] += 1
                        else:
                            document["predecessor"]["generation"] = 3
                        mutated = json.dumps(
                            document, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8") + b"\n"
                    storage._drop_historical_quartet_transaction_memory_for_test(
                        prefilter
                    )
                    sentinel = journal.parent / "unrelated-scenario"
                    sentinel.mkdir()
                    (sentinel / "sentinel").write_bytes(b"unrelated")
                    journal.write_bytes(mutated)
                    with self.assertRaises(Exception):
                        prefilter.frozen_identity_projection()
                    self.assertTrue((sentinel / "sentinel").is_file())
                    self.assertTrue(journal.is_file())
                finally:
                    if context is not None:
                        context.close()
                    scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                        fixture, capture, prefilter
                    )

    def test_replay_boundary_types_and_sanitizes_all_ordinary_validation(self):
        import scripts.historical_foundry_anvil as anvil

        self._open_context()
        sink = anvil._open_scenario_evidence_sink(
            context=self.context, scenario=self.scenario
        )
        cases = (
            ("context", object(), self.scenario, sink, "authority"),
            ("sink", self.context, self.scenario, object(), "authority"),
            ("scenario", self.context, object(), sink, "authority"),
        )
        for axis, context, scenario, selected_sink, category in cases:
            with self.subTest(axis=axis):
                with self.assertRaises(anvil.HistoricalReplayError) as raised:
                    anvil._replay_historical_scenario(
                        context=context, scenario=scenario, sink=selected_sink
                    )
                self.assertEqual(raised.exception.category, category)
        for error in (
            RuntimeError("secret endpoint /tmp/private --fork-url"),
            ValueError("secret argv /Users/private/key"),
        ):
            with self.subTest(error=type(error).__name__):
                with mock.patch.object(
                    anvil, "build_historical_state_override",
                    side_effect=error,
                ):
                    with self.assertRaises(
                        anvil.HistoricalReplayError
                    ) as raised:
                        anvil._replay_historical_scenario(
                            context=self.context, scenario=self.scenario,
                            sink=sink,
                        )
                self.assertEqual(
                    raised.exception.category, "foundry_replay_failed"
                )
                rendered = repr(raised.exception) + str(raised.exception)
                self.assertNotIn("secret", rendered)
                self.assertNotIn("/tmp", rendered)
                self.assertNotIn("/Users", rendered)
                self.assertNotIn("--fork-url", rendered)
        for control in (KeyboardInterrupt(), SystemExit()):
            with self.subTest(control=type(control).__name__):
                with mock.patch.object(
                    anvil, "build_historical_state_override",
                    side_effect=control,
                ):
                    with self.assertRaises(type(control)):
                        anvil._replay_historical_scenario(
                            context=self.context, scenario=self.scenario,
                            sink=sink,
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
            ("artifact_source", lambda value: value["proof_authority"].__setitem__("executor_source_tree_sha256", "0" * 64)),
            ("artifact_constructor", lambda value: value["proof_authority"].__setitem__("executor_constructor_args_sha256", "0" * 64)),
            ("artifact_immutable", lambda value: value["proof_authority"].__setitem__("executor_immutable_references_sha256", "0" * 64)),
            ("artifact_manifest", lambda value: value["proof_authority"].__setitem__("executor_artifact_manifest_sha256", "0" * 64)),
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
                "call_path": [2],
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
                "extra_preceding_call": (inner, dict(inner, call_path=[3])),
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
            original_identity = context._artifact._verified_identity
            changed_identity = dict(context._artifact.verified_identity)
            changed_identity["source_tree_sha256"] = "0" * 64
            object.__setattr__(
                context._artifact,
                "_verified_identity",
                MappingProxyType(changed_identity),
            )
            try:
                self.assertEqual(
                    anvil._classify_historical_revert(
                        context=context, scenario=scenario,
                        receipt=receipt, trace=trace,
                    ),
                    "unresolved",
                )
            finally:
                object.__setattr__(
                    context._artifact, "_verified_identity", original_identity
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

    def test_protocol_system_addresses_are_exact_hardfork_authority(self):
        import scripts.historical_foundry_rpc as rpc

        address = "0x0000f90827f1c53a10cb7a02335b175320002935"
        self.assertEqual(
            rpc._historical_protocol_system_addresses(hardfork="osaka"),
            frozenset((address,)),
        )
        for hardfork in ("prague", "cancun", "", None):
            with self.subTest(hardfork=hardfork):
                self.assertEqual(
                    rpc._historical_protocol_system_addresses(
                        hardfork=hardfork
                    ),
                    frozenset(),
                )

    def test_local_view_call_requires_sealed_executor_and_no_value(self):
        import scripts.historical_foundry_anvil as anvil

        executor = "0x" + "11" * 20
        target = "0x" + "22" * 20
        request = {"from": executor, "to": target, "data": "0x0902f1ac"}
        self.assertEqual(
            anvil._validate_historical_local_read_request(
                request=request, expected_executor=executor
            ),
            request,
        )
        for axis, changed in (
            ("missing_from", {"to": target, "data": "0x0902f1ac"}),
            ("wrong_from", dict(request, **{"from": "0x" + "33" * 20})),
            ("value", dict(request, value="0x0")),
            ("caller_field", dict(request, caller=executor)),
        ):
            with self.subTest(axis=axis):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_local_read_request(
                        request=changed, expected_executor=executor
                    )

    def test_status_zero_quartet_freezes_only_exact_atomic_rollback(self):
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
        capture = prefilter = context = None
        try:
            config, capture, prefilter, window, grid, rows = (
                HistoricalFoundryScenarioAuthorityTests._prepared(fixture)
            )
            row = rows[0]
            self.assertEqual(row["reason"], "first_leg_zero_output")
            scenario = scan._issue_validated_replay_scenario(
                staging=prefilter, window=window, grid=grid,
                scenario_key=row["scenario_key"],
            )
            artifact = build_validated_executor_artifact(config)
            context = anvil.open_historical_replay_context(
                config=config, staging=prefilter, window=window, grid=grid,
                executor_artifact=artifact,
            )
            override = anvil.build_historical_state_override(
                context=context, scenario=scenario
            )
            matrix = config.policy.value["closed_revert_matrix"][0]
            router = next(
                venue["router_address"]
                for venue in config.authority.value["venues"]
                if venue["venue_id"] == "uniswap_v2"
            )
            call = {
                "call_path": [2],
                "leg": "first_leg",
                "router": router,
                "revert_selector": matrix["revert_selector"],
                "revert_data_sha256": matrix["revert_data_sha256"],
            }
            helper = HistoricalFoundryOverlayTests(
                "test_overlay_known_answer_and_sender_funding_are_internal"
            )
            helper.config = config
            helper.artifact = artifact
            helper.rows = rows
            quartet = helper._quartet(
                override, row=row, revert_call=call
            )
            members = dict(quartet)
            projection = storage._validate_historical_quartet_for_test(
                staging=prefilter, scenario_key=row["scenario_key"],
                members=members,
            )
            self.assertIsNone(projection["proof_inputs_hash"])
            for axis, section, field in (
                ("final_weth", "balances", "final_weth_raw"),
                ("final_uni", "balances", "final_uni_raw"),
                ("first_leg", "actual_deltas", "first_leg_uni_raw"),
                ("weth_delta", "actual_deltas", "weth_raw"),
                ("residual", "actual_deltas", "residual_uni_raw"),
            ):
                changed = json.loads(members["result"])
                changed[section][field] += 1
                changed_members = dict(members)
                changed_members["result"] = helper._canonical(changed)
                with self.subTest(axis=axis):
                    with self.assertRaises(ValueError):
                        storage._validate_historical_quartet_for_test(
                            staging=prefilter,
                            scenario_key=row["scenario_key"],
                            members=changed_members,
                        )
            for axis, mutate in (
                ("omitted_count", lambda value: value["struct_log_storage"].__setitem__(
                    "storage_omitted_step_count", 2
                )),
                ("explicit_count", lambda value: value["struct_log_storage"].__setitem__(
                    "storage_explicit_step_count", 2
                )),
                ("anvil_sha", lambda value: value["struct_log_storage"].__setitem__(
                    "anvil_binary_sha256", "0" * 64
                )),
                ("trace_config", lambda value: value["struct_log_storage"].__setitem__(
                    "trace_config_sha256", "0" * 64
                )),
                ("missing_other_field", lambda value: value["struct_logs"][0].pop(
                    "refund"
                )),
                ("explicit_storage", lambda value: value["struct_logs"][1].__setitem__(
                    "storage", {"0x1": "0x" + "00" * 32}
                )),
                ("raw_failed", lambda value: value["raw_trace_closure"].__setitem__(
                    "failed", False
                )),
            ):
                changed_trace = json.loads(gzip.decompress(members["trace"]))
                mutate(changed_trace)
                changed_trace_bytes = gzip.compress(
                    helper._canonical(changed_trace), mtime=0
                )
                changed_result = json.loads(members["result"])
                changed_result["trace_sha256"] = hashlib.sha256(
                    changed_trace_bytes
                ).hexdigest()
                changed_result["trace_closure"]["raw_trace_closure"] = (
                    changed_trace["raw_trace_closure"]
                )
                changed_result["trace_closure"]["struct_log_storage"] = (
                    changed_trace["struct_log_storage"]
                )
                changed_result["proof_authority"]["anvil_binary_sha256"] = (
                    changed_trace["struct_log_storage"]["anvil_binary_sha256"]
                )
                changed_result["proof_authority"]["trace_config_sha256"] = (
                    changed_trace["struct_log_storage"]["trace_config_sha256"]
                )
                changed_members = dict(members)
                changed_members["trace"] = changed_trace_bytes
                changed_members["result"] = helper._canonical(changed_result)
                with self.subTest(trace_axis=axis):
                    with self.assertRaises(ValueError):
                        storage._validate_historical_quartet_for_test(
                            staging=prefilter,
                            scenario_key=row["scenario_key"],
                            members=changed_members,
                        )
            sink = anvil._open_scenario_evidence_sink(
                context=context, scenario=scenario
            )
            for role, payload in quartet:
                sink.write_member(role=role, canonical_bytes=payload)
            self.assertEqual(sink.validated_ledger().scenario_key, row[
                "scenario_key"
            ])
            prefilter = sink.validated_ledger().staging_snapshot()
        finally:
            if context is not None:
                context.close()
            scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                fixture, capture, prefilter
            )


class HistoricalFoundryOfflineRepeatTests(unittest.TestCase):
    def test_cleanup_failure_before_freeze_leaves_zero_evidence_and_successor(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan

        for axis in ("late_output", "relay_handler"):
            fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
            capture = prefilter = context = None
            with self.subTest(axis=axis):
                try:
                    config, capture, prefilter, window, grid, rows = (
                        HistoricalFoundryScenarioAuthorityTests._prepared(
                            fixture
                        )
                    )
                    scenario = scan._issue_validated_replay_scenario(
                        staging=prefilter, window=window, grid=grid,
                        scenario_key=rows[0]["scenario_key"],
                    )
                    context = anvil.open_historical_replay_context(
                        config=config, staging=prefilter, window=window,
                        grid=grid,
                        executor_artifact=build_validated_executor_artifact(
                            config
                        ),
                    )
                    override = anvil.build_historical_state_override(
                        context=context, scenario=scenario
                    )
                    helper = HistoricalFoundryOverlayTests(
                        "test_overlay_known_answer_and_sender_funding_are_internal"
                    )
                    helper.config = config
                    helper.artifact = context._artifact
                    helper.rows = rows
                    quartet = dict(helper._quartet(override))
                    receipt = json.loads(quartet["receipt"])
                    trace = json.loads(gzip.decompress(quartet["trace"]))
                    frozen_result = json.loads(quartet["result"])
                    outcome = {
                        "receipt": receipt, "trace": trace,
                        "selected_state": {"closed": True},
                        "token_deltas": {
                            "initial_weth_raw": frozen_result["balances"][
                                "initial_weth_raw"
                            ],
                            "initial_uni_raw": frozen_result["balances"][
                                "initial_uni_raw"
                            ],
                            "actual_first_leg_uni_raw": frozen_result[
                                "actual_deltas"
                            ]["first_leg_uni_raw"],
                            "final_weth_raw": frozen_result["balances"][
                                "final_weth_raw"
                            ],
                            "residual_uni_raw": frozen_result[
                                "actual_deltas"
                            ]["residual_uni_raw"],
                        },
                    }

                    class Process:
                        _closed = False

                        def __init__(self):
                            self.attempts = 0

                        def _assert_output_within_limit(self):
                            return None

                        def _close_with_budget(self, remaining):
                            remaining(5.0)
                            self.attempts += 1
                            self._closed = True
                            if axis == "late_output" and self.attempts == 1:
                                raise ValueError("late output plus one")

                    class Relay:
                        port = 31201

                        def __init__(self):
                            self.attempts = 0
                            self.closed = False

                        def close(self):
                            self.attempts += 1
                            if axis == "relay_handler" and self.attempts == 1:
                                raise ValueError("handler remains alive")
                            self.closed = True

                        def _is_closed(self):
                            return self.closed

                    process = Process()
                    relay = Relay()
                    sink = anvil._open_scenario_evidence_sink(
                        context=context, scenario=scenario
                    )
                    with mock.patch.object(
                        type(context._toolchain),
                        "_spawn_historical_anvil_process",
                        return_value=process,
                    ), mock.patch.object(
                        anvil, "_start_historical_relay",
                        return_value=relay,
                    ), mock.patch.object(
                        anvil, "_reserve_historical_anvil_port",
                        return_value=31202,
                    ), mock.patch.object(
                        anvil, "_execute_historical_local_rpc",
                        return_value=outcome,
                    ):
                        with self.assertRaises(anvil.HistoricalReplayError):
                            anvil._replay_historical_scenario(
                                context=context, scenario=scenario, sink=sink
                            )
                    self.assertEqual(
                        prefilter.frozen_identity_projection()["generation"], 2
                    )
                    with self.assertRaises(ValueError):
                        sink.validated_ledger()
                    self.assertEqual(
                        list(fixture.data_dir.rglob(scenario.scenario_key)), []
                    )
                finally:
                    if context is not None:
                        context.close()
                    scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                        fixture, capture, prefilter
                    )

    def test_context_retains_unreaped_process_and_live_relay_until_retry(self):
        import scripts.historical_foundry_anvil as anvil

        class BaseLease:
            def __init__(self):
                self.closes = 0

            def close(self):
                self.closes += 1

            _close = close

        class ProcessLease:
            def __init__(self):
                self.attempts = 0
                self._closed = False

            def _close_with_budget(self, remaining):
                self.attempts += 1
                self.asserted_budget = remaining(5.0)
                if self.attempts == 1:
                    raise ValueError("kill/wait left child alive")
                self._closed = True

        class RelayLease:
            def __init__(self):
                self.attempts = 0
                self.closed = False

            def close(self):
                self.attempts += 1
                if self.attempts == 1:
                    raise ValueError("handler remains alive")
                self.closed = True

            def _is_closed(self):
                return self.closed

        for axis in ("process", "relay"):
            with self.subTest(axis=axis):
                base_relay = BaseLease()
                toolchain = BaseLease()
                process = ProcessLease() if axis == "process" else None
                relay = RelayLease() if axis == "relay" else None
                context = anvil._issue_replay_context(
                    _config=None, _staging=None, _window=None, _grid=None,
                    _artifact=None, _runtime=b"runtime",
                    _runtime_sha256="1" * 64, _toolchain=toolchain,
                    _relay_lease=base_relay,
                    _active_process_lease=process,
                    _active_relay_lease=relay,
                    _clock=lambda: 10.0, _run_deadline=10.0,
                    _scenario_deadline=10.0, _closed=False,
                )
                with self.assertRaises(ValueError):
                    context.close()
                self.assertFalse(context._closed)
                self.assertIs(context._active_process_lease, process)
                self.assertIs(context._active_relay_lease, relay)
                self.assertEqual(base_relay.closes, 0)
                self.assertEqual(toolchain.closes, 0)
                object.__setattr__(context, "_run_deadline", 20.0)
                object.__setattr__(context, "_scenario_deadline", 20.0)
                context.close()
                self.assertTrue(context._closed)
                self.assertIsNone(context._active_process_lease)
                self.assertIsNone(context._active_relay_lease)
                self.assertEqual(base_relay.closes, 1)
                self.assertEqual(toolchain.closes, 1)

    def test_pair_closure_and_full_type2_envelope_reject_one_field_drift(self):
        import scripts.historical_foundry_anvil as anvil

        reserve_authority = {
            "uniswap_v2": {
                "pair_address": "0x" + "11" * 20,
                "reserve_uni_raw": 10,
                "reserve_weth_raw": 20,
            },
            "sushiswap_v2": {
                "pair_address": "0x" + "22" * 20,
                "reserve_uni_raw": 30,
                "reserve_weth_raw": 40,
            },
        }
        pair_state = {
            "uniswap_v2": dict(
                reserve_authority["uniswap_v2"],
                pair_uni_balance_raw=17,
                pair_weth_balance_raw=29,
            ),
            "sushiswap_v2": dict(
                reserve_authority["sushiswap_v2"],
                pair_uni_balance_raw=41,
                pair_weth_balance_raw=53,
            ),
        }
        self.assertEqual(
            anvil._validate_historical_pair_closure(
                expected=reserve_authority, before=pair_state,
                after=pair_state,
            ),
            pair_state,
        )
        for phase in ("expected", "after"):
            changed = copy.deepcopy(pair_state)
            if phase == "expected":
                changed = copy.deepcopy(reserve_authority)
                changed["uniswap_v2"]["reserve_uni_raw"] += 1
            else:
                changed["uniswap_v2"]["pair_uni_balance_raw"] += 1
            arguments = {
                "expected": reserve_authority, "before": pair_state,
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
                "refund": 0, "returnData": "0x",
            }],
        }
        trace_authority = {
            "anvil_binary_sha256": (
                "5c9f9aad323062b1c0421a63595741430acaea150da3611e38c45071e4cf4e28"
            ),
            "trace_config": anvil._historical_struct_trace_config(),
        }
        complete = anvil._validate_historical_raw_trace(
            raw_trace=raw_trace, expected_failed=False, **trace_authority
        )
        self.assertEqual(complete["gasprice_operations"], [])
        self.assertEqual(complete["storage_explicit_step_count"], 1)
        self.assertEqual(complete["storage_omitted_step_count"], 0)
        prefixed_storage = copy.deepcopy(raw_trace)
        prefixed_storage["structLogs"][0]["storage"] = {
            "0x" + "01" * 32: "0x" + "02" * 32,
        }
        self.assertEqual(
            anvil._validate_historical_raw_trace(
                raw_trace=prefixed_storage, expected_failed=False,
                **trace_authority
            )["storage_explicit_step_count"],
            1,
        )
        for invalid_storage in (
            {"0x1": "0x" + "02" * 32},
            {"0x" + "01" * 32: "0x2"},
            {"0X" + "01" * 32: "0x" + "02" * 32},
        ):
            changed = copy.deepcopy(raw_trace)
            changed["structLogs"][0]["storage"] = invalid_storage
            with self.subTest(invalid_storage=invalid_storage):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_raw_trace(
                        raw_trace=changed, expected_failed=False,
                        **trace_authority
                    )
        for missing in ("gas", "failed", "returnValue", "structLogs"):
            changed = dict(raw_trace); changed.pop(missing)
            with self.subTest(missing_trace=missing):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_raw_trace(
                        raw_trace=changed, expected_failed=False,
                        **trace_authority
                    )
        for missing in (
            "pc", "op", "gas", "gasCost", "depth", "stack", "memory",
            "refund", "returnData", "storage",
        ):
            changed = copy.deepcopy(raw_trace)
            changed["structLogs"][0].pop(missing)
            with self.subTest(missing_step=missing):
                if missing == "storage":
                    sparse = anvil._validate_historical_raw_trace(
                        raw_trace=changed, expected_failed=False,
                        **trace_authority
                    )
                    self.assertEqual(
                        sparse["storage_omitted_step_count"], 1
                    )
                else:
                    with self.assertRaises(ValueError):
                        anvil._validate_historical_raw_trace(
                            raw_trace=changed, expected_failed=False,
                            **trace_authority
                        )
        sparse = copy.deepcopy(raw_trace)
        sparse["structLogs"][0].pop("storage")
        for axis, authority_value in (
            ("wrong_binary", dict(trace_authority, anvil_binary_sha256="0" * 64)),
            ("wrong_config", dict(trace_authority, trace_config={})),
        ):
            with self.subTest(axis=axis):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_raw_trace(
                        raw_trace=sparse, expected_failed=False,
                        **authority_value
                    )
        changed = copy.deepcopy(raw_trace)
        changed["structLogs"][0]["op"] = "GASPRICE"
        self.assertEqual(anvil._validate_historical_raw_trace(
            raw_trace=changed, expected_failed=False, **trace_authority
        )["gasprice_operations"], ["GASPRICE"])

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
        call_trace_authority = {
            "anvil_binary_sha256": trace_authority["anvil_binary_sha256"],
            "call_trace_config": {"tracer": "callTracer"},
        }
        normalized = anvil._normalized_failed_router_calls(
            call_trace=call_trace,
            router_order=(first_router, second_router),
            root_sender=sender, root_executor=executor,
            root_input=calldata, root_failed=True,
            **call_trace_authority
        )
        self.assertEqual(normalized[0]["call_path"], [0])
        successful_sparse_leaf = {
            "type": "CALL", "from": sender, "to": executor,
            "input": calldata, "output": "0x", "value": "0x0",
            "gas": "0x100", "gasUsed": "0x80", "calls": [{
                "type": "STATICCALL", "from": executor, "to": pair,
                "input": "0x0902f1ac", "output": "0x" + "00" * 96,
                "gas": "0x80", "gasUsed": "0x40",
            }],
        }
        self.assertEqual(anvil._normalized_failed_router_calls(
            call_trace=successful_sparse_leaf,
            router_order=(first_router, second_router),
            root_sender=sender, root_executor=executor,
            root_input=calldata, root_failed=False,
            **call_trace_authority
        ), [])
        for axis, mutate in (
            ("call_missing_value", lambda value: value["calls"][0].update(
                {"type": "CALL"}
            )),
            ("wrong_type", lambda value: value["calls"][0].update(
                {"type": "CREATE"}
            )),
            ("static_nonzero_value", lambda value: value["calls"][0].update(
                {"value": "0x1"}
            )),
        ):
            changed = copy.deepcopy(successful_sparse_leaf)
            mutate(changed)
            with self.subTest(sparse_child_axis=axis):
                with self.assertRaises(ValueError):
                    anvil._normalized_failed_router_calls(
                        call_trace=changed,
                        router_order=(first_router, second_router),
                        root_sender=sender, root_executor=executor,
                        root_input=calldata, root_failed=False,
                        **call_trace_authority
                    )
        for missing_root in ("value", "calls"):
            changed = copy.deepcopy(successful_sparse_leaf)
            changed.pop(missing_root)
            with self.subTest(missing_root=missing_root):
                with self.assertRaises(ValueError):
                    anvil._normalized_failed_router_calls(
                        call_trace=changed,
                        router_order=(first_router, second_router),
                        root_sender=sender, root_executor=executor,
                        root_input=calldata, root_failed=False,
                        **call_trace_authority
                    )
        for wrong_authority in (
            dict(call_trace_authority, anvil_binary_sha256="0" * 64),
            dict(call_trace_authority, call_trace_config={}),
        ):
            with self.assertRaises(ValueError):
                anvil._normalized_failed_router_calls(
                    call_trace=successful_sparse_leaf,
                    router_order=(first_router, second_router),
                    root_sender=sender, root_executor=executor,
                    root_input=calldata, root_failed=False,
                    **wrong_authority
                )
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
            **call_trace_authority
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
                        **call_trace_authority
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
            "anvil_setCoinbase",
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

    def test_local_coinbase_initialization_is_exact_once_and_precedes_reads(self):
        import scripts.historical_foundry_anvil as anvil

        executor = "0x" + "12" * 20
        self.assertIsNone(
            anvil._validate_historical_local_coinbase_initialization(
                params=[executor], expected_executor=executor,
                already_initialized=False, read_started=False,
            )
        )
        for axis, params, already, read_started in (
            ("missing", [], False, False),
            ("wrong", ["0x" + "34" * 20], False, False),
            ("caller_extra", [executor, "0x" + "56" * 20], False, False),
            ("second", [executor], True, False),
            ("late", [executor], False, True),
        ):
            with self.subTest(axis=axis):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_local_coinbase_initialization(
                        params=params, expected_executor=executor,
                        already_initialized=already, read_started=read_started,
                    )

    def test_struct_trace_request_configuration_is_fixed_and_complete(self):
        import scripts.historical_foundry_anvil as anvil

        expected = {
            "disableStack": False,
            "disableStorage": False,
            "enableMemory": True,
            "enableReturnData": True,
        }
        self.assertEqual(anvil._historical_struct_trace_config(), expected)
        self.assertIsNone(
            anvil._validate_historical_struct_trace_config(expected)
        )
        invalid = ({}, {key: value for key, value in expected.items() if key != "enableMemory"})
        invalid += tuple(
            dict(expected, **{key: not value}) for key, value in expected.items()
        )
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_struct_trace_config(value)

    def test_local_rpc_uses_bounded_decoder_and_canonicalizes_before_semantics(self):
        import scripts.historical_foundry_anvil as anvil

        for valid in (
            b'{"id":7,"jsonrpc":"2.0","result":"0x1"}',
            b'{"jsonrpc":"2.0", "id":7, "result":"0x1"}',
        ):
            self.assertEqual(
                anvil._decode_historical_local_rpc_response(
                    payload=valid, identifier=7
                ),
                "0x1",
            )
        cases = {
            "oversize": b"x" * 67_108_865,
            "deep": (
                b'{"id":7,"jsonrpc":"2.0","result":'
                + b"[" * 130 + b"0" + b"]" * 130 + b"}"
            ),
            "wide": (
                b'{"id":7,"jsonrpc":"2.0","result":['
                + b"0," * 1_048_576 + b"0]}"
            ),
            "long_string": (
                b'{"id":7,"jsonrpc":"2.0","result":"'
                + b"x" * 262_145 + b'"}'
            ),
            "duplicate": b'{"id":7,"id":7,"jsonrpc":"2.0","result":0}',
        }
        for axis, payload in cases.items():
            with self.subTest(axis=axis):
                with self.assertRaises(ValueError):
                    anvil._decode_historical_local_rpc_response(
                        payload=payload, identifier=7
                    )

    def test_child_receipt_and_raw_transaction_identity_closure(self):
        import scripts.historical_foundry_anvil as anvil

        base_hash = "0x" + "1" * 64
        child_hash = "0x" + "2" * 64
        transaction_hash = "0x" + "3" * 64
        child = {
            "number": "0x65", "hash": child_hash,
            "parentHash": base_hash, "transactions": [transaction_hash],
        }
        receipt = {
            "transactionHash": transaction_hash,
            "blockHash": child_hash, "blockNumber": "0x65",
            "transactionIndex": "0x0",
        }
        transaction = {
            "hash": transaction_hash, "blockHash": child_hash,
            "blockNumber": "0x65", "transactionIndex": "0x0",
        }
        arguments = {
            "raw_child_block": child, "raw_receipt": receipt,
            "raw_transaction": transaction,
            "submitted_transaction_hash": transaction_hash,
            "base_block_number": 100, "base_block_hash": base_hash,
        }
        self.assertEqual(
            anvil._validate_historical_child_transaction_closure(**arguments),
            child_hash,
        )
        axes = (
            ("child_number", "raw_child_block", "number", "0x66"),
            ("child_hash", "raw_child_block", "hash", "0x" + "4" * 64),
            ("parent_hash", "raw_child_block", "parentHash", "0x" + "4" * 64),
            ("child_transactions", "raw_child_block", "transactions", []),
            ("receipt_hash", "raw_receipt", "transactionHash", "0x" + "4" * 64),
            ("receipt_block_hash", "raw_receipt", "blockHash", "0x" + "4" * 64),
            ("receipt_number", "raw_receipt", "blockNumber", "0x66"),
            ("receipt_index", "raw_receipt", "transactionIndex", "0x1"),
            ("raw_hash", "raw_transaction", "hash", "0x" + "4" * 64),
            ("raw_block_hash", "raw_transaction", "blockHash", "0x" + "4" * 64),
            ("raw_number", "raw_transaction", "blockNumber", "0x66"),
            ("raw_index", "raw_transaction", "transactionIndex", "0x1"),
        )
        for axis, container, field, value in axes:
            changed = copy.deepcopy(arguments)
            changed[container][field] = value
            with self.subTest(axis=axis):
                with self.assertRaises(ValueError):
                    anvil._validate_historical_child_transaction_closure(
                        **changed
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
            (anvil._HistoricalReplayBoundaryError("authority"), "authority"),
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

    def test_two_status_one_repeats_and_fresh_status_zero_use_real_anvil(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan

        source = r'''
pragma solidity 0.8.36;
interface T { function transferFrom(address,address,uint256) external returns (bool); }
contract UniToken {
    uint256 p0; uint256 p1; uint256 p2;
    mapping(address => mapping(address => uint256)) public allowance;
    mapping(address => uint256) public balanceOf;
    event Transfer(address indexed from,address indexed to,uint256 value);
    function transferFrom(address from,address to,uint256 value) external returns(bool) {
        uint256 approved=allowance[from][msg.sender];
        if(approved!=type(uint256).max) allowance[from][msg.sender]=approved-value;
        balanceOf[from]-=value; balanceOf[to]+=value; emit Transfer(from,to,value); return true;
    }
}
contract WethToken {
    uint256 p0; uint256 p1; uint256 p2;
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;
    event Transfer(address indexed from,address indexed to,uint256 value);
    function transferFrom(address from,address to,uint256 value) external returns(bool) {
        uint256 approved=allowance[from][msg.sender];
        if(approved!=type(uint256).max) allowance[from][msg.sender]=approved-value;
        balanceOf[from]-=value; balanceOf[to]+=value; emit Transfer(from,to,value); return true;
    }
}
contract FixtureRouter {
    address constant UNI=0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984;
    address constant WETH_TOKEN=0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address constant UNI_ROUTER=0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D;
    address constant UNI_FACTORY=0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f;
    address constant SUSHI_FACTORY=0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac;
    uint256 public amountOut; address public pair;
    function factory() external view returns(address) { return address(this)==UNI_ROUTER?UNI_FACTORY:SUSHI_FACTORY; }
    function WETH() external pure returns(address) { return WETH_TOKEN; }
    function swapExactTokensForTokens(uint256 amountIn,uint256,address[] calldata path,address recipient,uint256)
        external returns(uint256[] memory amounts) {
        require(path.length==2);
        require(amountOut!=0,"UniswapV2: INSUFFICIENT_OUTPUT_AMOUNT");
        require(T(path[0]).transferFrom(msg.sender,pair,amountIn));
        require(T(path[1]).transferFrom(pair,recipient,amountOut));
        amounts=new uint256[](2); amounts[0]=amountIn; amounts[1]=amountOut;
    }
}
contract FixturePair {
    uint256 r0; uint256 r1; uint256 ts;
    function getReserves() external view returns(uint112,uint112,uint32) {
        return (uint112(r0),uint112(r1),uint32(ts));
    }
}
'''
        standard_input = {
            "language": "Solidity",
            "sources": {"Fixture.sol": {"content": source}},
            "settings": {
                "optimizer": {"enabled": True, "runs": 200},
                "outputSelection": {
                    "*": {"*": ["evm.deployedBytecode.object"]}
                },
            },
        }
        compiler = toolchain._sealed_solc_argument()
        compiled = subprocess.run(
            [compiler, "--standard-json"],
            input=json.dumps(standard_input).encode("utf-8"),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            check=True, timeout=30,
        )
        output = json.loads(compiled.stdout[compiled.stdout.find(b"{"):])
        contracts = output["contracts"]["Fixture.sol"]
        runtimes = {
            name: "0x" + contracts[name]["evm"]["deployedBytecode"]["object"]
            for name in ("UniToken", "WethToken", "FixtureRouter", "FixturePair")
        }
        projections = []
        processes = []
        launch_identities = []

        def realistic_anvil_context():
            headers = {
                0: scan_fixtures._normalized_header(
                    0, 24, gas_limit=30_000_000, gas_used=15_000_000
                ),
                1: scan_fixtures._normalized_header(
                    1, 25, gas_limit=30_000_000, gas_used=15_000_000
                ),
                2: scan_fixtures._normalized_header(
                    2, 604_825,
                    gas_limit=30_000_000, gas_used=15_000_000,
                ),
            }
            capture = scan_fixtures._capture_for_header(headers[2])
            lower = scan_fixtures._lower_capture(
                capture, headers.__getitem__
            )
            plan = scan_fixtures.build_historical_window_request_plan(
                lower_bound_capture=lower, anchor_capture=capture
            )
            return headers, capture, lower, plan

        for repeat in range(3):
            closed_revert = repeat == 2
            unit = 10 ** 18
            fixture = scan_fixtures._Task4bOfflineCapabilityFixture(
                context_factory=realistic_anvil_context,
                split_reserve_root=False,
                record_calls=False,
                reserve_by_target={
                    scan_fixtures.PAIR_UNISWAP: (
                        (1, 10 ** 30) if closed_revert
                        else (4000 * unit, 1000 * unit)
                    ),
                    scan_fixtures.PAIR_SUSHI: (1000 * unit, 1000 * unit),
                },
            )
            capture = prefilter = context = successor = None
            fixture_directory = tempfile.TemporaryDirectory()
            archive_process = None
            try:
                config, capture, prefilter, window, grid, rows = (
                    HistoricalFoundryScenarioAuthorityTests._prepared(fixture)
                )
                self.assertEqual(
                    rows[0].get("reason"),
                    "first_leg_zero_output" if closed_revert else None,
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
                storage_values = {}
                balance_values = {}
                codes = {
                    tokens["uni"]["address"]: runtimes["UniToken"],
                    tokens["weth"]["address"]: runtimes["WethToken"],
                }
                for venue_id in ("uniswap_v2", "sushiswap_v2"):
                    reserve = rows[0]["reserves"][venue_id]
                    by_role = {
                        "uni": reserve["reserve_uni_raw"],
                        "weth": reserve["reserve_weth_raw"],
                    }
                    pair = reserve["pair_address"]
                    pairs[reserve["pair_address"]] = {
                        "reserve0": by_role[ordered_roles[0]],
                        "reserve1": by_role[ordered_roles[1]],
                        "timestamp": reserve["pair_timestamp"],
                    }
                    codes[pair] = runtimes["FixturePair"]
                    for slot, value in enumerate((
                        by_role[ordered_roles[0]],
                        by_role[ordered_roles[1]], reserve["pair_timestamp"],
                    )):
                        storage_values[
                            pair + ":" + "0x" + slot.to_bytes(32, "big").hex()
                        ] = "0x" + value.to_bytes(32, "big").hex()
                    for role in ("uni", "weth"):
                        balance_values[
                            tokens[role]["address"] + ":" + pair
                        ] = by_role[role]
                        key = solidity_balance_storage_key(
                            pair, tokens[role]["balance_descriptor"]["slot"]
                        )
                        storage_values[
                            tokens[role]["address"] + ":" + key
                        ] = "0x" + by_role[role].to_bytes(32, "big").hex()
                first_venue = (
                    "uniswap_v2" if rows[0]["direction"] == "uniswap_to_sushiswap"
                    else "sushiswap_v2"
                )
                second_venue = (
                    "sushiswap_v2" if first_venue == "uniswap_v2"
                    else "uniswap_v2"
                )
                venues = {row["venue_id"]: row for row in authority["venues"]}
                for venue_id, amount_out in (
                    (first_venue, rows[0]["first_amount_out_raw"]),
                    (second_venue, rows[0]["second_amount_out_raw"]),
                ):
                    router = venues[venue_id]["router_address"]
                    pair = rows[0]["reserves"][venue_id]["pair_address"]
                    codes[router] = runtimes["FixtureRouter"]
                    storage_values[
                        router + ":" + "0x" + (0).to_bytes(32, "big").hex()
                    ] = "0x" + amount_out.to_bytes(32, "big").hex()
                    storage_values[
                        router + ":" + "0x" + (1).to_bytes(32, "big").hex()
                    ] = "0x" + int(pair, 16).to_bytes(32, "big").hex()
                max_uint = (1 << 256) - 1
                for role, venue_id in (
                    ("uni", first_venue), ("weth", second_venue),
                ):
                    pair = rows[0]["reserves"][venue_id]["pair_address"]
                    router = venues[venue_id]["router_address"]
                    key = solidity_allowance_storage_key(
                        pair, router,
                        tokens[role]["allowance_descriptor"]["slot"],
                    )
                    storage_values[
                        tokens[role]["address"] + ":" + key
                    ] = "0x" + max_uint.to_bytes(32, "big").hex()
                header = rows[0]["header"]
                block = {
                    "number": hex(header["number"]), "hash": header["hash"],
                    "parentHash": header["parent_hash"],
                    "stateRoot": header["state_root"],
                    "timestamp": hex(header["timestamp"]),
                    "gasLimit": hex(header["gas_limit"]),
                    "gasUsed": hex(header["gas_used"]),
                    "baseFeePerGas": hex(header["base_fee_per_gas"]),
                    "difficulty": "0x0", "totalDifficulty": "0x0",
                    "extraData": "0x", "logsBloom": "0x" + "0" * 512,
                    "miner": authority["executor"]["address"],
                    "nonce": "0x" + "0" * 16,
                    "mixHash": "0x" + "0" * 64,
                    "receiptsRoot": "0x" + "0" * 64,
                    "sha3Uncles": "0x" + "0" * 64,
                    "transactionsRoot": "0x" + "0" * 64,
                    "transactions": [], "uncles": [],
                }
                fixture_config = {
                    "block": block, "codes": codes,
                    "storage": storage_values, "balances": balance_values,
                    "pairs": pairs,
                    "log_path": os.path.join(
                        fixture_directory.name, "archive-requests.jsonl"
                    ),
                }
                config_path = os.path.join(fixture_directory.name, "fixture.json")
                with open(config_path, "w", encoding="utf-8") as handle:
                    json.dump(fixture_config, handle, sort_keys=True)
                archive_port = anvil._reserve_historical_anvil_port()
                archive_process = subprocess.Popen(
                    [
                        sys.executable, "-c",
                        "from tests.test_historical_foundry_anvil import "
                        "_serve_historical_archive_fixture as serve; serve()",
                        str(archive_port), config_path,
                    ],
                    cwd=os.getcwd(), stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    env={"LANG": "C", "LC_ALL": "C"},
                )

                def local_archive(body, timeout):
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", archive_port, timeout=timeout
                    )
                    try:
                        connection.request(
                            "POST", "/", body=body,
                            headers={"Content-Type": "application/json"},
                        )
                        response = connection.getresponse()
                        if response.status != 200:
                            raise ValueError("archive fixture failed")
                        return response.read(67_108_865)
                    finally:
                        connection.close()

                object.__setattr__(
                    context._relay_lease, "_operation", local_archive
                )
                live_started = time.monotonic()
                object.__setattr__(
                    context._relay_lease, "_clock", time.monotonic
                )
                object.__setattr__(
                    context._relay_lease, "_last_clock", live_started
                )
                object.__setattr__(
                    context._relay_lease, "_run_deadline",
                    live_started + 21_600.0,
                )
                object.__setattr__(context, "_clock", time.monotonic)
                object.__setattr__(
                    context, "_run_deadline", live_started + 21_600.0
                )

                original_getaddrinfo = socket.getaddrinfo
                original_execute = anvil._execute_historical_local_rpc
                original_decode = anvil._decode_historical_local_rpc_response
                original_canonical_json = anvil._canonical_json
                original_raw_trace_validator = anvil._validate_historical_raw_trace
                original_call_trace_validator = anvil._normalized_failed_router_calls
                original_typed = anvil._typed_historical_replay_error
                original_relay_call = rpc._relay_historical_archive_call
                diagnostics = []

                def loopback_only(host, *args, **kwargs):
                    if host != "127.0.0.1":
                        raise AssertionError("external network forbidden")
                    return original_getaddrinfo(host, *args, **kwargs)

                def observe_real_process(**arguments):
                    lease = context._active_process_lease
                    process = lease._process
                    processes.append(process)
                    launch_identity = lease.verified_launch_identity_projection()
                    self.assertEqual(
                        launch_identity,
                        {
                            "schema": "historical_foundry_anvil_launch_identity/v1",
                            "binary_sha256": (
                                "5c9f9aad323062b1c0421a63595741430acaea150"
                                "da3611e38c45071e4cf4e28"
                            ),
                            "cdhash": (
                                "561b69d0257e574c3438465eb55cf4cef6852abc"
                            ),
                            "main_image_matches_materialized_inode": True,
                            "resumed_after_identity_verification": True,
                        },
                    )
                    launch_identities.append(launch_identity)
                    try:
                        observed_outcome = original_execute(**arguments)
                        if closed_revert:
                            diagnostics.append((
                                "closed_revert_outcome",
                                {
                                    "receipt": observed_outcome.get("receipt"),
                                    "calls": observed_outcome.get(
                                        "trace", {}
                                    ).get("calls"),
                                    "failed": observed_outcome.get(
                                        "trace", {}
                                    ).get("failed"),
                                },
                            ))
                        return observed_outcome
                    except BaseException:
                        diagnostics.append((
                            "anvil_output_poll_{}".format(process.poll()),
                            tuple(
                                value.decode("utf-8", "replace")
                                for value in lease._captured_output_for_test()
                            ),
                        ))
                        diagnostics.append((
                            "relay_server",
                            context._active_relay_lease._diagnostics_for_test(),
                        ))
                        raise

                def observe_local_response(**arguments):
                    try:
                        return original_decode(**arguments)
                    except BaseException as error:
                        diagnostics.append((
                            "local_response", arguments["payload"][:4096],
                            type(error).__name__, str(error),
                        ))
                        raise

                def observe_local_request(value):
                    encoded = original_canonical_json(value)
                    if (
                        type(value) is dict
                        and value.get("method") in anvil._HISTORICAL_LOCAL_RPC_METHODS
                    ):
                        diagnostics.append((
                            "local_request", value.get("id"),
                            value.get("method"), value.get("params"),
                        ))
                    return encoded

                def observe_raw_trace(**arguments):
                    try:
                        return original_raw_trace_validator(**arguments)
                    except BaseException as error:
                        raw = arguments.get("raw_trace")
                        logs = raw.get("structLogs") if type(raw) is dict else None
                        shapes = {}
                        if type(logs) is list:
                            for step in logs:
                                shape = tuple(step) if type(step) is dict else (type(step).__name__,)
                                shapes[shape] = shapes.get(shape, 0) + 1
                        diagnostics.append((
                            "raw_trace_shape",
                            tuple(raw) if type(raw) is dict else type(raw).__name__,
                            tuple(shapes.items()),
                            next((step.get("storage") for step in logs if type(step) is dict and "storage" in step), None) if type(logs) is list else None,
                            type(error).__name__, str(error),
                        ))
                        raise

                def observe_call_trace(**arguments):
                    try:
                        return original_call_trace_validator(**arguments)
                    except BaseException as error:
                        diagnostics.append((
                            "call_trace_shape", arguments.get("call_trace"),
                            type(error).__name__, str(error),
                        ))
                        raise

                def record_error(error):
                    diagnostics.append((type(error).__name__, str(error)))
                    return original_typed(error)

                def record_relay(**arguments):
                    try:
                        return original_relay_call(**arguments)
                    except BaseException as error:
                        diagnostics.append((
                            "relay_request",
                            arguments["canonical_request_bytes"][:512],
                            type(error).__name__, str(error),
                        ))
                        raise

                with mock.patch.object(
                    anvil, "_execute_historical_local_rpc",
                    side_effect=observe_real_process,
                ), mock.patch.object(
                    anvil, "_decode_historical_local_rpc_response",
                    side_effect=observe_local_response,
                ), mock.patch.object(
                    anvil, "_canonical_json",
                    side_effect=observe_local_request,
                ), mock.patch.object(
                    anvil, "_validate_historical_raw_trace",
                    side_effect=observe_raw_trace,
                ), mock.patch.object(
                    anvil, "_normalized_failed_router_calls",
                    side_effect=observe_call_trace,
                ), mock.patch.object(
                    anvil, "_typed_historical_replay_error",
                    side_effect=record_error,
                ), mock.patch.object(
                    rpc, "_relay_historical_archive_call",
                    side_effect=record_relay,
                ), mock.patch.object(
                    socket, "getaddrinfo", side_effect=loopback_only
                ), mock.patch.dict(os.environ, {}, clear=True):
                    try:
                        projections.append(anvil._replay_historical_scenario(
                            context=context, scenario=scenario, sink=sink
                        ))
                    except anvil.HistoricalReplayError:
                        if os.path.exists(fixture_config["log_path"]):
                            with open(
                                fixture_config["log_path"], "r",
                                encoding="utf-8",
                            ) as log:
                                diagnostics.append(("archive", log.read()))
                        self.fail("diagnostics={!r}".format([
                            item for item in diagnostics
                            if item[0] not in ("local_request", "archive")
                        ]))
                ledger = sink.validated_ledger()
                self.assertEqual(ledger.generation, 3)
                successor = ledger.staging_snapshot()
            finally:
                if context is not None:
                    context.close()
                if successor is not None:
                    successor.close()
                if archive_process is not None:
                    archive_process.terminate()
                    try:
                        archive_output = archive_process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        archive_process.kill()
                        archive_output = archive_process.communicate(timeout=5)
                    self.assertLessEqual(
                        sum(len(value) for value in archive_output), 65_536
                    )
                    for stream in (
                        archive_process.stdout, archive_process.stderr
                    ):
                        if stream is not None:
                            stream.close()
                fixture_directory.cleanup()
                scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                    fixture, capture, prefilter
                )
        self.assertEqual(projections[0], projections[1])
        self.assertEqual(len(launch_identities), 3)
        self.assertIsNot(processes[0], processes[1])
        self.assertNotEqual(processes[0].pid, processes[1].pid)
        self.assertEqual(projections[2]["classification"], "closed_revert")
        self.assertIsNone(projections[2]["proof_inputs_hash"])
        self.assertEqual(len({process.pid for process in processes}), 3)
        self.assertTrue(all(process.poll() is not None for process in processes))
        for projection in projections:
            self.assertNotIn("path", projection)
            self.assertNotIn("endpoint", repr(projection).lower())
            self.assertGreater(projection["gas_used"], 0)
            self.assertGreater(
                projection["trace_storage_omitted_step_count"], 0
            )
            self.assertGreater(
                projection["trace_storage_explicit_step_count"], 0
            )


if __name__ == "__main__":
    unittest.main()
