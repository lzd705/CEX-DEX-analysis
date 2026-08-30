from __future__ import annotations

import asyncio
import copy
from decimal import Decimal, localcontext
import hashlib
import inspect
import json
import linecache
import pickle
import os
import sys
import tempfile
import traceback
import unittest
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

    def test_maximum_scheduler_uses_real_scope_transfer_and_cleans_after_first_state_root(self):
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage
        from tests.test_historical_foundry_rpc import _rpc_response

        anchor_number = 50_400

        def header_at(number):
            return _normalized_header(number, number * 12 + 1)

        anchor_capture = _capture_for_header(header_at(anchor_number))
        lower_capture = _lower_capture(anchor_capture, header_at)
        plan = build_historical_window_request_plan(
            lower_bound_capture=lower_capture,
            anchor_capture=anchor_capture,
        )
        original_header_projector = project_historical_header_inventory
        header_inventory = original_header_projector(
            plan=plan,
            anchor_capture=anchor_capture,
            lower_bound_capture=lower_capture,
            batch_results=(
                (
                    descriptor,
                    _responses_for_descriptor(descriptor, header_at),
                )
                for descriptor in iter_historical_header_request_batches(plan)
            ),
        )
        state_counts = {
            "reserve": 0,
            "price": 0,
            "fee_history": 0,
            "final_anchor": 0,
        }
        first_state_request_ids = None
        for descriptor in iter_historical_state_request_batches(
            plan=plan, header_inventory=header_inventory
        ):
            state_counts[descriptor["kind"]] += 1
            if first_state_request_ids is None:
                first_state_request_ids = tuple(
                    row["id"] for row in descriptor["requests"]
                )
        self.assertEqual(plan["block_count"], 50_401)
        self.assertEqual(state_counts, {
            "reserve": 2_521,
            "price": 1_261,
            "fee_history": 50,
            "final_anchor": 1,
        })
        self.assertIsNotNone(first_state_request_ids)
        anchor_rows = copy.deepcopy(_synthetic_responses())
        anchor_rows[1]["result"] = _raw_header(header_at(anchor_number))
        round_id = (PHASE_ID << 64) + anchor_number + 1
        anchor_timestamp = header_at(anchor_number)["timestamp"]
        anchor_rows[35]["result"] = _round_data(
            round_id, ANSWER, anchor_timestamp, anchor_timestamp, round_id
        )
        anchor_by_id = {row["id"]: row for row in anchor_rows}

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

        transport_calls = []

        def response_for(request):
            request_id = request["id"]
            if request_id <= 48:
                return copy.deepcopy(anchor_by_id[request_id])
            method = request["method"]
            if method == "eth_getBlockByNumber":
                number = int(request["params"][0], 16)
                result = _raw_header(header_at(number))
            elif method == "eth_call":
                block_reference = request["params"][1]
                number = int(block_reference["blockHash"], 16) - 1
                target = request["params"][0]["to"].lower()
                if target in (PAIR_UNISWAP, PAIR_SUSHI):
                    result = _reserve_result(
                        number + 1, number + 2, number
                    )
                elif target == FEED_PROXY:
                    result = _price_result(
                        number, header_at(number)["timestamp"]
                    )
                else:
                    raise AssertionError("unexpected state call target")
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
                raise AssertionError("unexpected scheduler RPC method")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}

        class OfflineOpener:
            addheaders = []

            def open(self, request, _timeout=None, **_kwargs):
                requests = json.loads(request.data.decode("utf-8"))
                transport_calls.append(tuple(row["id"] for row in requests))
                return _rpc_response(tuple(
                    response_for(row) for row in reversed(requests)
                ))

        active_registry = dict(zip(
            storage._HistoricalWindowExchangeSpool.close.__code__.co_freevars,
            storage._HistoricalWindowExchangeSpool.close.__closure__ or (),
        ))["active_registry"].cell_contents
        active_baseline = set(active_registry)
        original_issue = (
            storage._HistoricalWindowExchangeSpool
            .issue_transfer_from_bound_rpc
        )
        issued_request_ids = []
        max_live_transfers = [0]

        def issue(spool, *args, **kwargs):
            entry = active_registry[id(spool)]
            self.assertIs(entry[0], spool)
            self.assertIsNone(entry[1]["live_transfer"])
            transfer = original_issue(spool, *args, **kwargs)
            current = active_registry[id(spool)]
            max_live_transfers[0] = max(
                max_live_transfers[0],
                int(current[1]["live_transfer"] is not None),
            )
            issued_request_ids.append(tuple(
                kwargs["exchange_projection"]["request_ids"]
            ))
            return transfer

        bounded_stop = GeneratorExit("maximum-first-state-root-stop")
        stopped_state_roots = []
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                not stopped_state_roots
                and frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name == "state_results"
                and event == "line"
                and "del responses, typed, executed" in linecache.getline(
                    frame.f_code.co_filename, frame.f_lineno
                )
                and frame.f_locals["descriptor"]["kind"] == "reserve"
            ):
                stopped_state_roots.append(tuple(
                    row["id"]
                    for row in frame.f_locals["descriptor"]["requests"]
                ))
                sys.settrace(prior_trace)
                raise bounded_stop
            return tracer

        temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        data_dir = rpc.Path(temporary.name)
        os.chmod(str(data_dir), 0o700)
        preflight = Preflight()
        context = None
        claim = None
        spool = None
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
                rpc.urllib.request,
                "build_opener",
                return_value=OfflineOpener(),
            ):
                context = rpc._open_production_archive_rpc_run()
            claim = rpc._claim_fresh_production_archive_rpc_run_for_historical_window(
                context=context
            )
            spool = storage._open_historical_window_exchange_spool(
                data_dir=data_dir
            )
            try:
                with mock.patch.object(
                    storage._HistoricalWindowExchangeSpool,
                    "issue_transfer_from_bound_rpc",
                    new=issue,
                ), mock.patch.object(
                    rpc, "_recheck_production_preflight", return_value=True
                ):
                    sys.settrace(tracer)
                    with self.assertRaises(GeneratorExit) as caught:
                        scan._capture_production_historical_window(
                            claim=claim, spool=spool
                        )
                self.assertIs(caught.exception, bounded_stop)
            finally:
                sys.settrace(prior_trace)
            self.assertGreater(len(transport_calls), 1_261)
            self.assertEqual(max_live_transfers[0], 1)
            self.assertEqual(stopped_state_roots, [first_state_request_ids])
            self.assertEqual(issued_request_ids[-1], first_state_request_ids)
            self.assertEqual(context._state, "abandoned")
            self.assertTrue(preflight.closed)
            self.assertEqual(set(active_registry), active_baseline)
            self.assertEqual(tuple(data_dir.iterdir()), ())
        finally:
            sys.settrace(prior_trace)
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
            if not preflight.closed:
                preflight.close()
            temporary.cleanup()

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
            "binding_registry",
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

    def test_plan_accepts_maximum_and_rejects_maximum_plus_one_early(self):
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

        maximum = make(50_401)
        self.assertEqual(maximum["block_count"], 50_401)
        self.assertEqual(maximum["fee_chunk_count"], 50)
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


class HistoricalFoundryMaximumStressTests(unittest.TestCase):
    def test_generated_maximum_window_is_streamed_and_gapless(self):
        import scripts.historical_foundry_scan as scan

        anchor_number = 50_400

        def header_at(number):
            return _normalized_header(number, number * 12 + 1)

        capture = _capture_for_header(header_at(anchor_number))
        lower = _lower_capture(capture, header_at)
        plan = build_historical_window_request_plan(
            lower_bound_capture=lower,
            anchor_capture=capture,
        )
        header_batches = iter_historical_header_request_batches(plan)
        header_root_count = 0

        def header_results():
            nonlocal header_root_count
            for descriptor in header_batches:
                header_root_count += 1
                yield descriptor, _responses_for_descriptor(descriptor, header_at)

        inventory = project_historical_header_inventory(
            plan=plan,
            anchor_capture=capture,
            lower_bound_capture=lower,
            batch_results=header_results(),
        )
        self.assertEqual(header_root_count, 1_261)
        self.assertEqual(inventory["row_count"], 50_401)

        root_counts = {
            "reserve": 0, "price": 0, "fee_history": 0, "final_anchor": 0
        }

        def state_results():
            for descriptor in iter_historical_state_request_batches(
                plan=plan, header_inventory=inventory
            ):
                root_counts[descriptor["kind"]] += 1
                yield descriptor, _responses_for_descriptor(descriptor, header_at)

        original_validator = scan._validate_header_inventory
        with mock.patch.object(
            scan, "_validate_header_inventory", wraps=original_validator
        ) as validator:
            projection = project_historical_window_projection(
                plan=plan,
                anchor_capture=capture,
                lower_bound_capture=lower,
                header_inventory=inventory,
                batch_results=state_results(),
            )
        self.assertEqual(validator.call_count, 3)
        self.assertEqual(root_counts, {
            "reserve": 2_521,
            "price": 1_261,
            "fee_history": 50,
            "final_anchor": 1,
        })
        self.assertEqual(projection["coverage"], {
            "header_count": 50_401,
            "reserve_count": 100_802,
            "price_count": 50_401,
            "fee_count": 50_401,
        })
        self.assertEqual(
            projection["request_ledger"]["last_request_id"],
            plan["last_request_id"],
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


if __name__ == "__main__":
    unittest.main()
