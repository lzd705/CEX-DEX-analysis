from __future__ import annotations

import copy
import gzip
import hashlib
import inspect
import io
import json
import os
import pickle
import socket
import subprocess
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
        try:
            self.assertNotIn("fixture.invalid", repr(lease))
            self.assertNotIn("secret", repr(lease))
            for method in (
                "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash",
                "eth_getCode", "eth_getBalance", "eth_getTransactionCount",
                "eth_getStorageAt", "eth_call", "eth_getProof",
            ):
                with self.subTest(method=method):
                    request = (
                        '{{"id":7,"jsonrpc":"2.0","method":"{}",'
                        '"params":[]}}'.format(method)
                    ).encode("ascii")
                    self.assertEqual(
                        rpc._relay_historical_archive_call(
                            relay_lease=lease,
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
                            relay_lease=lease,
                            canonical_request_bytes=body,
                        )
            key = object.__getattribute__(lease, "_key")
            key[0] ^= 1
            with self.assertRaises(ValueError):
                rpc._relay_historical_archive_call(
                    relay_lease=lease,
                    canonical_request_bytes=(
                        b'{"id":7,"jsonrpc":"2.0","method":"eth_chainId",'
                        b'"params":[]}'
                    ),
                )
        finally:
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
        cleanup.assert_called_once_with()
        self.assertEqual(
            process.calls,
            [("terminate",), ("wait", 5.0), ("kill",), ("wait", 5.0)],
        )

        controlled = _Process([KeyboardInterrupt()])
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

    def _proof(self, scenario_key, receipt_sha256, trace_sha256, receipt):
        row = self.rows[0]
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
        pool_amount = str(row["requested_notional_usd"] * 30 // 10_000)
        mev_amount = str(row["requested_notional_usd"] * 10 // 10_000)
        row_specs = (
            ("buy", "pool_swap_fee", "bounded_estimate", True, pool_amount, "30", "receipt"),
            ("buy", "router_or_integrator_fee", "bounded_estimate", False, "0", "0", "receipt"),
            ("buy", "token_transfer_tax", "bounded_estimate", False, "0", "0", "receipt"),
            ("sell", "pool_swap_fee", "bounded_estimate", True, pool_amount, "30", "receipt"),
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
            "adapter_proof_sha256": "a" * 64,
            "rows": rows,
        }
        proof["proof_inputs_hash"] = hashlib.sha256(
            b"historical_foundry_cost_proof_inputs/v1\0"
            + self._canonical(proof)
        ).hexdigest()
        return proof

    def _quartet(self, override):
        scenario_key = self.scenario.scenario_key
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
            "cost_proof_inputs": self._proof(
                scenario_key, receipt_sha, trace_sha, receipt
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
            receipt_sha256=hashlib.sha256(quartet[1][1]).hexdigest(),
            trace_sha256=hashlib.sha256(quartet[2][1]).hexdigest(),
        )
        proof_rows = proof["rows"]
        self.assertEqual(
            [row["amount_usd_exact"] for row in proof_rows[:6]],
            ["3", "0", "0", "3", "0", "0"],
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
        transfer_topic = "0x" + keccak256(
            b"Transfer(address,address,uint256)"
        ).hex()
        receipt = {"logs": [{
            "address": uni,
            "topics": [
                transfer_topic,
                "0x" + "00" * 12 + "56" * 20,
                "0x" + "00" * 12 + executor[2:],
            ],
            "data": "0x" + (123).to_bytes(32, "big").hex(),
        }]}
        self.assertEqual(
            anvil._extract_actual_first_leg_uni_raw(
                raw_receipt=receipt, uni_address=uni,
                executor_address=executor,
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
                        executor_address=executor,
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

    def test_two_repeats_use_independent_test_processes_and_zero_network(self):
        import scripts.bootstrap_historical_foundry_toolchain as toolchain
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        projections = []
        processes = []

        def execute(*, context, scenario, override, anvil_port):
            del context, scenario, anvil_port
            return {
                "selected_state": {
                    "block_number": override["block_number"],
                    "block_hash": override["block_hash"],
                    "state_root": override["state_root"],
                },
                "token_deltas": {
                    "actual_first_leg_uni_raw": 1328891698325589794,
                    "final_weth_raw": 1323151972535702977,
                    "residual_uni_raw": 0,
                },
                "receipt": {
                    "schema": "historical_foundry_receipt/v1",
                    "scenario_key": override["scenario_key"],
                    "status": 1,
                    "blockNumber": override["synthetic_block"]["number"],
                    "blockHash": "0x" + "b" * 64,
                    "transactionIndex": 0,
                    "gasUsed": 123456,
                    "effectiveGasPrice": 7,
                    "transactionHash": "0x" + "c" * 64,
                },
                "trace": {
                    "schema": "historical_foundry_trace/v1",
                    "scenario_key": override["scenario_key"],
                    "failed": False,
                    "gasprice_opcode_addresses": [],
                    "calls": [],
                },
            }

        for repeat in range(2):
            fixture = scan_fixtures.HistoricalPrefilterGridTests._new_fixture()
            capture = prefilter = context = relay = successor = None
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
                process = _Process([0])
                processes.append(process)
                process_lease = toolchain._issue_historical_process_lease_for_test(
                    process=process, cleanup=mock.Mock(),
                    binary_sha256="9" * 64,
                    selected_block=rows[0]["block_number"],
                    hardfork=config.toolchain.value["compiler_settings"]["fork_hardfork"],
                )
                fake_relay = mock.Mock()
                fake_relay.port = 43100 + repeat
                fake_relay.close = mock.Mock()
                with mock.patch.object(
                    anvil, "_start_historical_relay", return_value=fake_relay
                ), mock.patch.object(
                    anvil, "_reserve_historical_anvil_port",
                    return_value=44100 + repeat,
                ), mock.patch.object(
                    type(context._toolchain),
                    "_spawn_historical_anvil_process",
                    return_value=process_lease,
                ), mock.patch.object(
                    anvil, "_execute_historical_local_rpc", side_effect=execute
                ), mock.patch.object(
                    socket, "socket", side_effect=AssertionError("network forbidden")
                ), mock.patch.object(
                    socket, "getaddrinfo", side_effect=AssertionError("DNS forbidden")
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
                elif relay is not None:
                    relay.close()
                if successor is not None:
                    successor.close()
                scan_fixtures.HistoricalPrefilterGridTests._close_fixture(
                    fixture, capture, prefilter
                )
        self.assertEqual(projections[0], projections[1])
        self.assertIsNot(processes[0], processes[1])
        self.assertEqual(
            [process.calls for process in processes],
            [[("terminate",), ("wait", 5.0)]] * 2,
        )
        for projection in projections:
            self.assertNotIn("path", projection)
            self.assertNotIn("endpoint", repr(projection).lower())
            self.assertEqual(projection["gas_used"], 123456)


if __name__ == "__main__":
    unittest.main()
