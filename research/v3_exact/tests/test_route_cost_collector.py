import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import traceback
import unittest
import zlib
import copy
import hashlib
import inspect
import urllib.error
from datetime import datetime, timezone
from unittest.mock import patch

from scripts import route_cost_collector, route_cost_evidence
from scripts.route_cost_collector import (
    CONNECTOR_PROFILE_ENV,
    TRACE_PROFILE_ENV,
    RouteCostCollectorError,
    _NoRedirectHandler,
    _decode_bounded_json_response,
    _read_private_profile,
    load_route_cost_profile_capture,
)


_COLLECTOR_GENERATION = "a" * 64
_COLLECTOR_COHORT_ID = "cohort:" + "c" * 64


def _phase_a_multimarket_universe(retained_members):
    from tests.test_route_cost_evidence import universe_for

    base_leg = universe_for()["selected_legs"][0]
    legs = []
    for index, market_id in enumerate(sorted(retained_members), start=1):
        state = json.loads(retained_members[market_id]["payload"])
        leg = copy.deepcopy(base_leg)
        leg.update({
            "market_id": market_id,
            "token_symbol": market_id.rsplit(":", 1)[1],
            "selection_rank": index,
            "target_token_address": state["token0_address"],
        })
        leg["collector_context"] = copy.deepcopy(leg["collector_context"])
        leg["collector_context"].update({
            "base_token_id": "eth_" + state["token0_address"],
            "quote_token_id": "eth_" + state["token1_address"],
        })
        legs.append(leg)
    return universe_for(markets=legs, routes=[])


def _phase_a_lineage(universe):
    return {
        "universe": universe,
        "run_id": "collector-run",
        "route_cohort_id": _COLLECTOR_COHORT_ID,
        "candidate_source_generation": universe["candidate_source_generation"],
        "route_universe_sha256": route_cost_evidence.physical_sha256(universe),
        "capture_utc_anchor": "2026-08-01T12:00:02Z",
    }


def _collector_cex_leg(market_id, *, selection_rank=1):
    token = market_id.rsplit(":", 1)[1].split("/", 1)[0]
    return {
        "market_id": market_id,
        "market_type": "cex",
        "token_symbol": token,
        "candidate_source_generation": _COLLECTOR_GENERATION,
        "selection_window": {"start": "2026-07-03", "end": "2026-08-01"},
        "selection_inputs": {
            "execution_capability": "supported",
            "proved_execution_capacity_usd": None,
            "observed_100bps_depth_usd": "2000",
            "cex_selected_window_usd": "2000",
            "dex_24h_usd": None,
            "dex_tvl_usd": None,
        },
        "selection_rank": selection_rank,
    }


def _collector_universe(markets=None, routes=None):
    if markets is None:
        markets = [
            _collector_cex_leg("cex:alpha:AAA/USDT"),
            _collector_cex_leg("cex:beta:AAA/USDT", selection_rank=2),
        ]
    if routes is None:
        routes = [{
            "route_id": (
                "route:AAA:cex:alpha:AAA/USDT->"
                "cex:beta:AAA/USDT:prepositioned_inventory"
            ),
            "token_symbol": "AAA",
            "buy_market_id": "cex:alpha:AAA/USDT",
            "sell_market_id": "cex:beta:AAA/USDT",
            "route_mode": "prepositioned_inventory",
            "route_class": "candidate",
            "settlement_reason": None,
            "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
            "candidate_source_generation": _COLLECTOR_GENERATION,
            "buy_reference_volume_usd": "2000",
            "sell_reference_volume_usd": "2000",
            "route_volume_usd": "2000",
            "route_volume_basis": "minimum_leg_source_horizon_usd",
        }]
    return {
        "schema": "route_universe/v1",
        "candidate_source_generation": _COLLECTOR_GENERATION,
        "selection_window": {"start": "2026-07-03", "end": "2026-08-01"},
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "selected_legs": markets,
        "routes": routes,
    }


def _collector_core_leg(leg):
    market_id = leg["market_id"]
    result = {
        "leg_id": market_id,
        "market_id": market_id,
        "market_type": leg["market_type"],
        "token_symbol": leg["token_symbol"],
        "status": "failed",
        "available": False,
        "reason_code": "source_unavailable" if leg["market_type"] == "cex" else "collection_failed",
        "state_observed_at": None,
        "snapshot_id": None,
        "source_endpoint": None,
        "raw_response_sha256": None,
        "typed_source_lineage": {
            "schema": "route_leg_typed_source_lineage/v1",
            "members": [
                {
                    "role": role,
                    "status": "unavailable",
                    "reason_code": "typed_source_missing",
                    "filename": None,
                    "sha256": None,
                    "size": None,
                    "logical_generation": None,
                    "adapter_id": route_cost_evidence_adapter,
                    "content_schema": content_schema,
                }
                for role, route_cost_evidence_adapter, content_schema in (
                    (
                        "cex_market_rules",
                        "route_quantity_quote_for_book/v1",
                        "route_market_rules_source/v1",
                    ),
                    (
                        "cex_raw_book_response",
                        "fetch_cex_depth/parse_book/v1",
                        "route_bytes/v1",
                    ),
                    (
                        "quote_usd_conversion",
                        "route_usd_conversion_source/v1",
                        "route_usd_conversion_source/v1",
                    ),
                )
            ],
        },
    }
    if leg["market_type"] == "dex":
        result["collector_context"] = copy.deepcopy(leg["collector_context"])
    return result


def _collector_cohort(universe, *, raw_run_id="collector-raw-run"):
    routes = copy.deepcopy(universe["routes"])
    legs = [_collector_core_leg(leg) for leg in universe["selected_legs"]]
    route_rows = [{
        **route,
        "validated_at": "2026-08-01T12:00:03Z",
        "skew_seconds": None,
        "timing_status": "unavailable",
        "reason_code": "buy_leg_unavailable",
    } for route in routes]
    cohort = {
        "schema": "route_cohort_collection/v1",
        "candidate_source_generation": _COLLECTOR_GENERATION,
        "collection_input_generation": "b" * 64,
        "source_state": {
            "candidate_source_generation": _COLLECTOR_GENERATION,
            "collection_input_generation": "b" * 64,
        },
        "raw_evidence_run_id": raw_run_id,
        "target_observed_at": "2026-08-01T12:00:00Z",
        "collection_started_at": "2026-08-01T12:00:00Z",
        "collection_completed_at": "2026-08-01T12:00:03Z",
        "collection_deadline_at": "2026-08-01T12:01:00Z",
        "skew_sla_seconds": "60",
        "route_age_sla_seconds": "120",
        "selection_window": copy.deepcopy(universe["selection_window"]),
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "legs": sorted(legs, key=lambda row: row["market_id"]),
        "routes": sorted(routes, key=lambda row: row["route_id"]),
        "route_rows": sorted(route_rows, key=lambda row: row["route_id"]),
    }
    without_hashes = copy.deepcopy(cohort)
    cohort["route_cohort_id"] = "cohort:" + hashlib.sha256(
        route_cost_evidence.canonical_json_bytes(without_hashes)
    ).hexdigest()
    cohort["fingerprint"] = hashlib.sha256(
        route_cost_evidence.canonical_json_bytes(cohort)
    ).hexdigest()
    return cohort


def _canonical_private_json(path: Path, value):
    path.write_bytes(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    path.chmod(0o600)


def _trace_profile():
    return {
        "schema": "route_cost_trace_rpc_profile/v1",
        "profile_id": "trace-mainnet-a",
        "endpoint_id": "ethereum-trace-a",
        "rpc_url": "https://rpc.example.invalid/trace",
        "authorization": "Bearer TRACE-SECRET",
    }


def _connector_profile():
    return {
        "schema": "route_cost_submission_connector_profile/v1",
        "profile_id": "connector-primary-a",
        "connector_id": "route_connector_a",
        "endpoint_url": "https://connector.example.invalid",
        "authorization": "Bearer CONNECTOR-SECRET",
    }


class _Headers:
    def __init__(self, rows):
        self._rows = list(rows)

    def raw_items(self):
        return iter(self._rows)

    def get_all(self, name):
        lowered = name.lower()
        return [value for key, value in self._rows if key.lower() == lowered]


class _Response:
    def __init__(self, body, *, headers=(), status=200, chunk_size=7):
        self._body = body
        self._offset = 0
        self._chunk_size = chunk_size
        self.headers = _Headers(headers)
        self.status = status
        self.read_calls = 0

    def read(self, maximum):
        self.read_calls += 1
        amount = min(maximum, self._chunk_size)
        result = self._body[self._offset:self._offset + amount]
        self._offset += len(result)
        return result

    def read1(self, maximum):
        return self.read(maximum)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _RouteCostCapability:
    def __init__(self, rpc_batch, *, monotonic_values=None, utc_values=None):
        self._rpc_batch = rpc_batch
        self.rpc_requests = []
        self.rpc_timeouts = []
        self.time_calls = 0
        self._monotonic_values = iter(monotonic_values or [0.0] * 32)
        self._utc_values = None if utc_values is None else iter(utc_values)

    def __call__(self):
        self.time_calls += 1
        if self._utc_values is not None:
            return next(self._utc_values)
        return "2026-08-01T12:00:04Z"

    def monotonic(self):
        return next(self._monotonic_values)

    def rpc_batch(self, request_bytes, timeout_seconds=None):
        self.rpc_requests.append(request_bytes)
        self.rpc_timeouts.append(timeout_seconds)
        return self._rpc_batch(request_bytes)


def _phase_b_one_market_wire_fixture(
    *, partial_estimates=False, include_unsupported=False,
    include_missing_supported=False
):
    """Build external RPC/native fixtures for one real collector success."""
    from tests.test_route_cost_evidence import (
        MARKET_ID, adapter_registry, connector_registry, funding_descriptor,
        pair_descriptor,
        native_price_captured_bytes, phase_a_rpc_responses,
        retained_v2_pool_state, universe_for,
    )
    from scripts.route_cost_collector import _NativePriceCaptureResult

    universe = universe_for()
    missing_supported_id = None
    if include_missing_supported:
        pool2 = "0x" + "4" * 40
        token2 = "0x" + "5" * 40
        token3 = "0x" + "6" * 40
        missing_supported_id = (
            "dex:eth:uniswap_v2:{}:BBB".format(pool2)
        )
        missing_supported = copy.deepcopy(universe["selected_legs"][0])
        missing_supported.update({
            "market_id": missing_supported_id,
            "token_symbol": "BBB",
            "selection_rank": 2,
            "target_token_address": token2,
        })
        missing_supported["collector_context"] = copy.deepcopy(
            missing_supported["collector_context"]
        )
        missing_supported["collector_context"].update({
            "base_token_id": "eth_" + token2,
            "quote_token_id": "eth_" + token3,
        })
        universe["selected_legs"].append(missing_supported)
    if include_unsupported:
        unsupported = copy.deepcopy(universe["selected_legs"][0])
        unsupported.update({
            "market_id": (
                "dex:eth:uniswap_v2:"
                "0x4444444444444444444444444444444444444444:BBB"
            ),
            "token_symbol": "BBB",
            "selection_rank": 2,
            "target_token_address": "0x" + "5" * 40,
        })
        unsupported["collector_context"] = copy.deepcopy(
            unsupported["collector_context"]
        )
        unsupported["collector_context"].update({
            "base_token_id": "eth_" + "0x" + "5" * 40,
            "quote_token_id": "eth_" + "0x" + "6" * 40,
        })
        universe["selected_legs"].append(unsupported)
    retained = retained_v2_pool_state(
        block_number=20_000_000,
        reserve0_raw=10 ** 24,
        reserve1_raw=2 * 10 ** 24,
    )
    retained_members = {MARKET_ID: retained}
    registry = adapter_registry(supported=True)
    if include_missing_supported:
        registry["adapters"][0]["pair_descriptors"].append(
            pair_descriptor(
                "0x" + "4" * 40,
                token0="0x" + "5" * 40,
                token1="0x" + "6" * 40,
            )
        )
        registry["adapters"][0]["token_funding_descriptors"].extend([
            funding_descriptor("0x" + "5" * 40),
            funding_descriptor("0x" + "6" * 40),
        ])
    keys = connector_registry()
    trace_identity = route_cost_evidence.trace_profile_identity(
        _trace_profile()
    )[0]
    connector_identity = (
        route_cost_evidence.submission_connector_profile_identity(None)[0]
    )
    universe_sha = route_cost_evidence.physical_sha256(universe)
    phase_a_plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
        universe=universe,
        adapter_registry=registry,
        retained_typed_pool_state_members=retained_members,
    )
    state = json.loads(retained["payload"])
    phase_a_rows = phase_a_rpc_responses(phase_a_plan)
    role_by_id = {
        row["id"]: row["role"] for row in phase_a_plan["request_roles"]
    }
    for row in phase_a_rows:
        role = role_by_id[row["id"]]
        if role == "block_header":
            row["result"].update({
                "number": phase_a_plan["block_tag"],
                "hash": state["block_hash"],
            })
        elif role == "fee_history":
            row["result"]["oldestBlock"] = phase_a_plan["block_tag"]
    phase_a_capture = route_cost_evidence.project_fixed_block_phase_a_capture(
        universe=universe,
        plan=phase_a_plan,
        responses=phase_a_rows,
        run_id="collector-run",
        route_cohort_id=_COLLECTOR_COHORT_ID,
        candidate_source_generation=universe["candidate_source_generation"],
        route_universe_sha256=universe_sha,
        trace_profile_identity=trace_identity,
        adapter_registry=registry,
        retained_typed_pool_state_members=retained_members,
        captured_started_at="2026-08-01T12:00:02Z",
        captured_finished_at="2026-08-01T12:00:04Z",
    )
    book_raw, rules_raw = native_price_captured_bytes()
    native = route_cost_evidence.build_native_price_evidence_from_captured(
        run_id="collector-run",
        route_cohort_id=_COLLECTOR_COHORT_ID,
        candidate_source_generation=universe["candidate_source_generation"],
        book_raw_response=book_raw,
        book_observed_at="2026-08-01T12:00:05Z",
        market_rules_raw_response=rules_raw,
        market_rules_observed_at="2026-08-01T12:00:04Z",
    )
    bound = route_cost_evidence.bind_native_price_to_phase_a_capture(
        universe=universe,
        phase_a_capture=phase_a_capture,
        native_price_evidence=native,
        run_id="collector-run",
        route_cohort_id=_COLLECTOR_COHORT_ID,
        candidate_source_generation=universe["candidate_source_generation"],
        route_universe_sha256=universe_sha,
        trace_profile_identity=trace_identity,
        adapter_registry=registry,
        retained_typed_pool_state_members=retained_members,
    )
    snapshot = route_cost_evidence.build_terminal_submission_policy_snapshot(
        universe=universe,
        run_id="collector-run",
        route_cohort_id=_COLLECTOR_COHORT_ID,
        candidate_source_generation=universe["candidate_source_generation"],
        route_universe_sha256=universe_sha,
        adapter_registry=registry,
        connector_key_registry=keys,
        trace_profile_identity=trace_identity,
        submission_connector_profile_identity=connector_identity,
        reason_code="submission_connector_missing",
    )
    scenario_plan = route_cost_evidence.build_phase_b_scenario_request_plan(
        universe=universe,
        run_id="collector-run",
        route_cohort_id=_COLLECTOR_COHORT_ID,
        candidate_source_generation=universe["candidate_source_generation"],
        route_universe_sha256=universe_sha,
        adapter_registry=registry,
        trace_profile_identity=trace_identity,
        submission_connector_profile_identity=connector_identity,
        retained_typed_pool_state_members=retained_members,
        native_price_evidence=native,
        submission_policy_snapshot=snapshot,
        native_bound_phase_a_capture=bound,
        terminal_reason_by_market=(
            {missing_supported_id: "core_pool_state_unavailable"}
            if missing_supported_id is not None else None
        ),
    )
    estimate_rows = [{
        "jsonrpc": "2.0", "id": row["id"], "result": "0x5208",
    } for row in scenario_plan["estimate_requests"]]
    trace_plan = route_cost_evidence.build_phase_b_trace_request_plan(
        scenario_plan=scenario_plan,
        estimate_responses=estimate_rows,
    )
    current_adapter = registry["adapters"][0]
    spec_by_trace_id = {
        row["trace_request_id"]: row
        for row in scenario_plan["scenario_specs"]
    }

    def raw_trace_result(spec):
        decoded = route_cost_evidence.decode_v2_swap_calldata(
            spec["calldata_hex"]
        )
        token_in, token_out = decoded["path"]
        amount_in = int(spec["quoted_amount_in_raw"])
        amount_out = int(spec["quoted_amount_out_raw"])
        funded = decoded.get(
            "amount_in_raw", decoded.get("amount_in_max_raw")
        )
        descriptor_in = next(
            row for row in current_adapter["token_funding_descriptors"]
            if row["token_address"] == token_in
        )
        descriptor_out = next(
            row for row in current_adapter["token_funding_descriptors"]
            if row["token_address"] == token_out
        )
        sender_key = route_cost_evidence.solidity_balance_storage_key(
            current_adapter["simulation_sender_address"],
            int(descriptor_in["balance_mapping_slot"]),
        )
        allowance_key = route_cost_evidence.solidity_allowance_storage_key(
            current_adapter["simulation_sender_address"],
            current_adapter["router_address"],
            int(descriptor_in["allowance_mapping_slot"]),
        )
        pair_in_key = route_cost_evidence.solidity_balance_storage_key(
            state["pool_address"], int(descriptor_in["balance_mapping_slot"])
        )
        pair_out_key = route_cost_evidence.solidity_balance_storage_key(
            state["pool_address"], int(descriptor_out["balance_mapping_slot"])
        )
        recipient_key = route_cost_evidence.solidity_balance_storage_key(
            current_adapter["simulation_sender_address"],
            int(descriptor_out["balance_mapping_slot"]),
        )
        word = lambda number: "0x{:064x}".format(number)
        return {
            "pre": {
                token_in: {"storage": {
                    sender_key: word(amount_in),
                    allowance_key: word(funded),
                    pair_in_key: word(10 ** 30),
                }},
                token_out: {"storage": {
                    pair_out_key: word(10 ** 30),
                }},
            },
            "post": {
                token_in: {"storage": {
                    sender_key: word(0),
                    allowance_key: word(funded - amount_in),
                    pair_in_key: word(10 ** 30 + amount_in),
                }},
                token_out: {"storage": {
                    pair_out_key: word(10 ** 30 - amount_out),
                    recipient_key: word(amount_out),
                }},
            },
        }

    def rpc(request_bytes):
        requests = json.loads(request_bytes.decode("utf-8"))
        methods = {row["method"] for row in requests}
        wanted = {row["id"] for row in requests}
        if "eth_estimateGas" in methods:
            rows = [row for row in estimate_rows if row["id"] in wanted]
            return route_cost_evidence.canonical_json_bytes(
                rows[:-1] if partial_estimates else rows
            )
        if "debug_traceCall" in methods:
            return route_cost_evidence.canonical_json_bytes([{
                "jsonrpc": "2.0",
                "id": row["id"],
                "result": raw_trace_result(spec_by_trace_id[row["id"]]),
            } for row in requests])
        return route_cost_evidence.canonical_json_bytes([
            row for row in phase_a_rows if row["id"] in wanted
        ])

    normalized = {
        "route_cohort_id": _COLLECTOR_COHORT_ID,
        "raw_evidence_run_id": "collector-raw-run",
        "candidate_source_generation": universe["candidate_source_generation"],
        "legs": [_collector_core_leg(row) for row in universe["selected_legs"]],
        "routes": copy.deepcopy(universe["routes"]),
    }
    return {
        "universe": universe,
        "universe_sha": universe_sha,
        "retained": retained,
        "registry": registry,
        "keys": keys,
        "normalized": normalized,
        "native_result": _NativePriceCaptureResult(
            status="observed", reason_code=None, evidence=native
        ),
        "phase_a_plan": phase_a_plan,
        "scenario_plan": scenario_plan,
        "trace_plan": trace_plan,
        "rpc": rpc,
        "missing_supported_id": missing_supported_id,
    }


class RouteCostProfileCaptureTests(unittest.TestCase):
    def test_missing_profiles_have_fixed_secret_free_identities(self):
        with patch.dict(os.environ, {}, clear=True):
            capture = load_route_cost_profile_capture()

        self.assertIsNone(capture.trace_profile)
        self.assertIsNone(capture.connector_profile)
        self.assertEqual(
            capture.trace_profile_identity,
            route_cost_evidence.trace_profile_identity(None)[0],
        )
        self.assertEqual(
            capture.submission_connector_profile_identity,
            route_cost_evidence.submission_connector_profile_identity(None)[0],
        )
        rendered = repr(capture)
        self.assertNotIn("SECRET", rendered)
        self.assertNotIn("rpc_url", rendered)
        self.assertNotIn("endpoint_url", rendered)

    def test_owner_only_profiles_are_captured_once_and_project_only_identity(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            trace_path = root / "trace.json"
            connector_path = root / "connector.json"
            _canonical_private_json(trace_path, _trace_profile())
            _canonical_private_json(connector_path, _connector_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(trace_path),
                CONNECTOR_PROFILE_ENV: str(connector_path),
            }, clear=True):
                capture = load_route_cost_profile_capture()

        self.assertEqual(capture.trace_profile, _trace_profile())
        self.assertEqual(capture.connector_profile, _connector_profile())
        self.assertEqual(
            capture.trace_profile_identity,
            {
                "schema": "route_cost_trace_profile_identity/v1",
                "status": "available",
                "profile_id": "trace-mainnet-a",
                "endpoint_id": "ethereum-trace-a",
            },
        )
        self.assertEqual(
            capture.submission_connector_profile_identity,
            {
                "schema": "route_cost_submission_connector_identity/v1",
                "status": "available",
                "profile_id": "connector-primary-a",
                "connector_id": "route_connector_a",
            },
        )
        projected = json.dumps(capture.public_projection(), sort_keys=True)
        self.assertNotIn("SECRET", projected)
        self.assertNotIn("example.invalid", projected)

        for mapping, key in (
            (capture.trace_profile, "endpoint_id"),
            (capture.connector_profile, "connector_id"),
            (capture.trace_profile_identity, "endpoint_id"),
            (
                capture.submission_connector_profile_identity,
                "connector_id",
            ),
        ):
            with self.subTest(key=key), self.assertRaises(TypeError):
                mapping[key] = "mutated"
        self.assertEqual(
            route_cost_evidence.trace_profile_identity(
                dict(capture.trace_profile)
            )[1],
            capture.trace_profile_generation,
        )
        self.assertEqual(
            route_cost_evidence.submission_connector_profile_identity(
                dict(capture.connector_profile)
            )[1],
            capture.submission_connector_profile_generation,
        )

    def test_configured_profile_requires_absolute_owner_only_canonical_file(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            valid = root / "trace.json"
            _canonical_private_json(valid, _trace_profile())
            hardlink = root / "trace-hardlink.json"
            os.link(str(valid), str(hardlink))
            symlink = root / "trace-symlink.json"
            symlink.symlink_to(valid)
            public = root / "trace-public.json"
            _canonical_private_json(public, _trace_profile())
            public.chmod(0o644)
            noncanonical = root / "trace-noncanonical.json"
            noncanonical.write_text(
                json.dumps(_trace_profile(), indent=2), encoding="utf-8"
            )
            noncanonical.chmod(0o600)
            surrogate = root / "trace-surrogate.json"
            surrogate.write_text(
                json.dumps(
                    {**_trace_profile(), "authorization": "x\ud800"},
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ) + "\n",
                encoding="ascii",
            )
            surrogate.chmod(0o600)
            nonfinite = root / "trace-nonfinite.json"
            nonfinite.write_text(
                json.dumps(
                    {**_trace_profile(), "authorization": float("inf")},
                    sort_keys=True,
                    separators=(",", ":"),
                ) + "\n",
                encoding="ascii",
            )
            nonfinite.chmod(0o600)
            extra = root / "trace-extra.json"
            _canonical_private_json(extra, {**_trace_profile(), "extra": True})

            cases = (
                ("relative", "trace.json"),
                ("hardlink", str(hardlink)),
                ("symlink", str(symlink)),
                ("mode", str(public)),
                ("noncanonical", str(noncanonical)),
                ("surrogate", str(surrogate)),
                ("nonfinite", str(nonfinite)),
                ("extra", str(extra)),
            )
            for name, path in cases:
                with self.subTest(name=name), patch.dict(
                    os.environ, {TRACE_PROFILE_ENV: path}, clear=True
                ):
                    with self.assertRaises(RouteCostCollectorError):
                        load_route_cost_profile_capture()

    def test_empty_environment_value_is_invalid_not_missing(self):
        with patch.dict(os.environ, {TRACE_PROFILE_ENV: ""}, clear=True):
            with self.assertRaisesRegex(
                RouteCostCollectorError, "configured.*path"
            ):
                load_route_cost_profile_capture()

    def test_profile_exception_chain_never_contains_private_path_or_bytes(self):
        private_marker = "TRACE-PATH-SECRET"
        path = Path(tempfile.gettempdir()) / private_marker / "missing.json"
        with patch.dict(
            os.environ, {TRACE_PROFILE_ENV: str(path)}, clear=True
        ):
            try:
                load_route_cost_profile_capture()
            except RouteCostCollectorError as error:
                rendered = "".join(traceback.format_exception(
                    type(error), error, error.__traceback__
                ))
            else:  # pragma: no cover - the path is deliberately absent
                self.fail("missing private profile unexpectedly loaded")
        self.assertNotIn(private_marker, rendered)
        self.assertNotIn(str(path), rendered)
        self.assertNotIn("TRACE-SECRET", rendered)

    def test_profile_ancestor_symlink_and_limit_plus_one_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            real = root / "real"
            real.mkdir()
            profile = real / "trace.json"
            _canonical_private_json(profile, _trace_profile())
            linked = root / "linked"
            linked.symlink_to(real, target_is_directory=True)
            oversized = root / "oversized.json"
            oversized.write_bytes(
                b"x" * (route_cost_evidence.MAX_PROFILE_BYTES + 1)
            )
            oversized.chmod(0o600)

            for name, path in (
                ("ancestor symlink", linked / "trace.json"),
                ("limit plus one", oversized),
            ):
                with self.subTest(name=name), patch.dict(
                    os.environ, {TRACE_PROFILE_ENV: str(path)}, clear=True
                ):
                    with self.assertRaises(RouteCostCollectorError) as caught:
                        load_route_cost_profile_capture()
                    self.assertNotIn("TRACE-SECRET", str(caught.exception))

    def test_configured_fifo_fails_without_blocking_on_open(self):
        with tempfile.TemporaryDirectory() as directory_name:
            fifo = Path(directory_name) / "trace.json"
            os.mkfifo(str(fifo), 0o600)
            command = (
                "from scripts.route_cost_collector import "
                "load_route_cost_profile_capture; "
                "load_route_cost_profile_capture()"
            )
            environment = dict(os.environ)
            environment[TRACE_PROFILE_ENV] = str(fifo)
            started = __import__("time").monotonic()
            completed = subprocess.run(
                [sys.executable, "-c", command],
                cwd=str(Path(__file__).resolve().parents[1]),
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=1,
                check=False,
            )
            self.assertLess(__import__("time").monotonic() - started, 1)
            self.assertNotEqual(completed.returncode, 0)
            self.assertNotIn(b"TRACE-SECRET", completed.stderr)

    def test_profile_ancestor_replacement_during_read_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            profiles = root / "profiles"
            profiles.mkdir()
            profile = profiles / "trace.json"
            _canonical_private_json(profile, _trace_profile())
            replacement = root / "replacement"
            replacement.mkdir()
            _canonical_private_json(
                replacement / "trace.json",
                {**_trace_profile(), "authorization": "Bearer OTHER-SECRET"},
            )
            displaced = root / "displaced"
            real_stat = os.stat
            profile_component_stats = 0

            def replacing_stat(path, *args, **kwargs):
                nonlocal profile_component_stats
                if path == "profiles" and kwargs.get("dir_fd") is not None:
                    profile_component_stats += 1
                    if profile_component_stats == 2:
                        profiles.rename(displaced)
                        replacement.rename(profiles)
                return real_stat(path, *args, **kwargs)

            with patch.dict(
                os.environ, {TRACE_PROFILE_ENV: str(profile)}, clear=True
            ), patch("scripts.route_cost_collector.os.stat", replacing_stat):
                with self.assertRaisesRegex(
                    RouteCostCollectorError, "ancestry changed"
                ):
                    load_route_cost_profile_capture()

    def test_profile_child_descriptor_closes_when_metadata_check_fails(self):
        for failing_call in ("fstat", "stat"):
            with self.subTest(failing_call=failing_call), tempfile.TemporaryDirectory() as directory_name:
                profile = (
                    Path(os.path.realpath(directory_name))
                    / "nested" / "trace.json"
                )
                profile.parent.mkdir()
                _canonical_private_json(profile, _trace_profile())
                real_open = os.open
                real_fstat = os.fstat
                real_stat = os.stat
                opened = []

                def tracking_open(*args, **kwargs):
                    descriptor = real_open(*args, **kwargs)
                    opened.append(descriptor)
                    return descriptor

                def failing_fstat(descriptor):
                    if failing_call == "fstat" and len(opened) >= 2 and descriptor == opened[1]:
                        raise OSError("fixture metadata failure")
                    return real_fstat(descriptor)

                def failing_stat(*args, **kwargs):
                    if failing_call == "stat" and kwargs.get("dir_fd") == opened[0]:
                        raise OSError("fixture path metadata failure")
                    return real_stat(*args, **kwargs)

                with patch(
                    "scripts.route_cost_collector.os.open", tracking_open
                ), patch(
                    "scripts.route_cost_collector.os.fstat", failing_fstat
                ), patch(
                    "scripts.route_cost_collector.os.stat", failing_stat
                ):
                    with self.assertRaises(RouteCostCollectorError):
                        _read_private_profile(profile, "trace RPC")

                self.assertGreaterEqual(len(opened), 2)
                for descriptor in opened:
                    with self.assertRaises(OSError):
                        real_fstat(descriptor)

    def test_profile_cleanup_attempts_every_descriptor_and_redacts_close_error(self):
        with tempfile.TemporaryDirectory() as directory_name:
            profile = Path(os.path.realpath(directory_name)) / "trace.json"
            _canonical_private_json(profile, _trace_profile())
            real_close = os.close
            close_calls = []

            def failing_first_close(descriptor):
                close_calls.append(descriptor)
                if len(close_calls) == 1:
                    raise OSError("/private/CLOSE-SECRET")
                return real_close(descriptor)

            with patch(
                "scripts.route_cost_collector.os.close", failing_first_close
            ):
                try:
                    _read_private_profile(profile, "trace RPC")
                except RouteCostCollectorError as error:
                    rendered = "".join(traceback.format_exception(
                        type(error), error, error.__traceback__
                    ))
                else:
                    self.fail("profile cleanup failure unexpectedly succeeded")
            self.assertGreaterEqual(len(close_calls), 2)
            self.assertNotIn("CLOSE-SECRET", rendered)


class BoundedRouteCostWireTests(unittest.TestCase):
    def _decode(self, response, **overrides):
        options = {
            "wire_limit": 1024,
            "decoded_limit": 1024,
            "scalar_limit": 1024,
            "node_limit": 100,
            "ordinary_string_limit": 64,
        }
        options.update(overrides)
        return _decode_bounded_json_response(response, **options)

    def test_identity_body_exact_limit_passes_and_plus_one_rejects(self):
        body = b'{"ok":true}'
        response = _Response(body, headers=(
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(body))),
        ))
        self.assertEqual(
            self._decode(
                response, wire_limit=len(body), decoded_limit=len(body)
            ),
            {"ok": True},
        )
        with self.assertRaisesRegex(RouteCostCollectorError, "resource limit"):
            self._decode(
                _Response(body),
                wire_limit=len(body) - 1,
                decoded_limit=len(body),
            )

    def test_absolute_deadline_stops_a_slow_fragment_stream(self):
        class SocketSpy:
            def __init__(self):
                self.timeouts = []

            def settimeout(self, value):
                self.timeouts.append(value)

        class SlowFragmentResponse(_Response):
            def __init__(self, body):
                super().__init__(body, chunk_size=1)
                self.socket = SocketSpy()
                self.fp = type("Buffered", (), {
                    "raw": type("Raw", (), {"_sock": self.socket})(),
                })()

            def read1(self, maximum):
                return super().read(maximum)

            def read(self, _maximum):
                raise AssertionError("deadline-bound stream must use read1")

        response = SlowFragmentResponse(b'{"ok":true}')
        with patch(
            "scripts.route_cost_collector.time.monotonic",
            side_effect=[0.0, 1.0, 2.0, 3.0],
        ), self.assertRaisesRegex(
            RouteCostCollectorError, "deadline|unavailable"
        ):
            self._decode(response, absolute_deadline=2.5)
        self.assertEqual(response.read_calls, 3)
        self.assertEqual(response.socket.timeouts, [2.5, 1.5, 0.5])

    def test_absolute_deadline_rejects_unbounded_response_structure(self):
        with self.assertRaisesRegex(
            RouteCostCollectorError, "deadline|stream.*invalid"
        ):
            self._decode(
                _Response(b'{"ok":true}'), absolute_deadline=2.5
            )

    def test_content_length_is_only_an_early_bound_and_must_match(self):
        body = b'{"ok":true}'
        oversized = _Response(
            body, headers=(("Content-Length", "9999"),)
        )
        with self.assertRaisesRegex(RouteCostCollectorError, "resource limit"):
            self._decode(oversized)
        self.assertEqual(oversized.read_calls, 0)
        for declared in (str(len(body) - 1), str(len(body) + 1), "01", "x"):
            with self.subTest(declared=declared):
                with self.assertRaises(RouteCostCollectorError):
                    self._decode(_Response(
                        body, headers=(("Content-Length", declared),)
                    ))

        for length in (1025, 4301, 8192):
            declared = "9" * length
            with self.subTest(length=length), self.assertRaisesRegex(
                RouteCostCollectorError, "resource limit"
            ):
                self._decode(_Response(
                    body, headers=(("Content-Length", declared),)
                ))

    def test_gzip_is_streamed_and_expansion_is_bounded(self):
        raw = b'{"value":"' + (b"a" * 200) + b'"}'
        encoder = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
        compressed = encoder.compress(raw) + encoder.flush()
        response = _Response(compressed, headers=(
            ("Content-Encoding", "gzip"),
            ("Content-Length", str(len(compressed))),
        ))
        with self.assertRaisesRegex(RouteCostCollectorError, "resource limit"):
            self._decode(
                response,
                wire_limit=len(compressed),
                decoded_limit=len(raw) - 1,
                ordinary_string_limit=512,
            )
        self.assertEqual(
            self._decode(
                _Response(compressed, headers=(("Content-Encoding", "gzip"),)),
                wire_limit=len(compressed),
                decoded_limit=len(raw),
                scalar_limit=len(raw),
                ordinary_string_limit=512,
            )["value"],
            "a" * 200,
        )

    def test_bad_or_concatenated_gzip_and_unknown_encoding_reject(self):
        compressor = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
        first = compressor.compress(b'{"ok":true}') + compressor.flush()
        compressor = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
        second = compressor.compress(b'{"other":true}') + compressor.flush()
        cases = (
            (b"not-gzip", "gzip"),
            (first[:-1], "gzip"),
            (first + second, "gzip"),
            (b'{"ok":true}', "br"),
        )
        for body, encoding in cases:
            with self.subTest(encoding=encoding, size=len(body)):
                with self.assertRaises(RouteCostCollectorError):
                    self._decode(_Response(
                        body, headers=(("Content-Encoding", encoding),)
                    ))

    def test_response_stream_errors_are_normalized_without_private_details(self):
        class BrokenResponse(_Response):
            def read(self, maximum):
                raise OSError("/private/SECRET-WIRE-PATH")

        try:
            self._decode(BrokenResponse(b""))
        except RouteCostCollectorError as error:
            rendered = "".join(traceback.format_exception(
                type(error), error, error.__traceback__
            ))
        else:
            self.fail("broken response stream unexpectedly decoded")
        self.assertNotIn("SECRET-WIRE-PATH", rendered)

    def test_header_and_json_shape_limits_fail_closed(self):
        body = b'{"ok":true}'
        cases = (
            ((('X-A', 'a'),) * 65, {}),
            ((("X-" + "a" * 127, "a"),), {}),
            ((("X-A", "a" * 8193),), {}),
            ((), {"node_limit": 1}),
        )
        for headers, overrides in cases:
            with self.subTest(headers=len(headers), overrides=overrides):
                with self.assertRaisesRegex(
                    RouteCostCollectorError, "resource limit"
                ):
                    self._decode(_Response(body, headers=headers), **overrides)
        with self.assertRaisesRegex(RouteCostCollectorError, "resource limit"):
            self._decode(
                _Response(b'{"value":"12345"}'), ordinary_string_limit=4
            )
        deep = b"[" * 1100 + b"0" + b"]" * 1100
        with self.assertRaisesRegex(RouteCostCollectorError, "resource limit"):
            self._decode(
                _Response(deep, chunk_size=4096),
                wire_limit=len(deep),
                decoded_limit=len(deep),
                scalar_limit=10,
                node_limit=10,
                ordinary_string_limit=10,
            )

    def test_node_and_string_limits_reject_before_json_materialization(self):
        cases = (
            (
                b"[" + b",".join(b"0" for _ in range(1000)) + b"]",
                {"node_limit": 2},
            ),
            (
                b'{"value":"' + (b"a" * 1000) + b'"}',
                {"ordinary_string_limit": 4},
            ),
        )
        for body, overrides in cases:
            with self.subTest(overrides=overrides), patch(
                "scripts.route_cost_collector.json.loads",
                side_effect=AssertionError("JSON tree was materialized"),
            ) as parser:
                with self.assertRaisesRegex(
                    RouteCostCollectorError, "resource limit"
                ):
                    self._decode(
                        _Response(body, chunk_size=len(body)),
                        wire_limit=len(body),
                        decoded_limit=len(body),
                        scalar_limit=len(body),
                        **overrides
                    )
                parser.assert_not_called()

    def test_json_nesting_depth_is_explicit_and_cross_runtime_stable(self):
        allowed = (b"[" * 128) + b"0" + (b"]" * 128)
        rejected = (b"[" * 129) + b"0" + (b"]" * 129)
        self.assertIsNotNone(self._decode(
            _Response(allowed, chunk_size=len(allowed)),
            wire_limit=len(allowed),
            decoded_limit=len(allowed),
            scalar_limit=16,
            node_limit=256,
        ))
        with patch(
            "scripts.route_cost_collector.json.loads",
            side_effect=AssertionError("overdeep JSON was materialized"),
        ) as parser, self.assertRaisesRegex(
            RouteCostCollectorError, "resource limit"
        ):
            self._decode(
                _Response(rejected, chunk_size=len(rejected)),
                wire_limit=len(rejected),
                decoded_limit=len(rejected),
                scalar_limit=16,
                node_limit=512,
            )
        parser.assert_not_called()

    def test_http_header_names_and_values_use_exact_field_grammar(self):
        for name in ("Bad:Name", "Bad,Name", "Bad(Name)", "Bad@Name"):
            with self.subTest(name=name), self.assertRaises(
                RouteCostCollectorError
            ):
                self._decode(_Response(
                    b'{"ok":true}', headers=((name, "value"),)
                ))

        for byte in tuple(range(0, 9)) + tuple(range(10, 32)) + (127,):
            value = "ok{}bad".format(chr(byte))
            with self.subTest(value_byte=byte), self.assertRaises(
                RouteCostCollectorError
            ):
                self._decode(_Response(
                    b'{"ok":true}', headers=(("X-Test", value),)
                ))

        self.assertEqual(
            self._decode(_Response(
                b'{"ok":true}',
                headers=(("X!#$%&'*+.^_`|~-9", "ok\tvalue\x80"),),
            )),
            {"ok": True},
        )

    def test_header_iteration_does_not_swallow_process_control_exceptions(self):
        class RaisingHeaders:
            def __init__(self, exception):
                self._exception = exception

            def raw_items(self):
                raise self._exception

        for exception_type in (KeyboardInterrupt, SystemExit):
            response = _Response(b'{"ok":true}')
            response.headers = RaisingHeaders(exception_type())
            with self.subTest(exception=exception_type.__name__), self.assertRaises(
                exception_type
            ):
                self._decode(response)

    def test_header_accessor_errors_are_redacted_and_process_control_propagates(self):
        class BrokenResponse:
            @property
            def headers(self):
                raise OSError("/private/SECRET-HEADER")

        class BrokenHeaders:
            @property
            def raw_items(self):
                raise OSError("/private/SECRET-RAW-ITEMS")

        for response in (BrokenResponse(), _Response(b'{"ok":true}')):
            if isinstance(response, _Response):
                response.headers = BrokenHeaders()
            try:
                self._decode(response)
            except RouteCostCollectorError as error:
                rendered = "".join(traceback.format_exception(
                    type(error), error, error.__traceback__
                ))
            else:
                self.fail("broken header accessor unexpectedly succeeded")
            self.assertNotIn("SECRET-", rendered)

    def test_json_surrogates_are_rejected_as_invalid_scalars(self):
        bodies = (
            b'"\\ud800"',
            b'"\\udfff"',
            b'{"\\ud800":1}',
            b'{"value":"\\udfff"}',
        )
        for body in bodies:
            with self.subTest(body=body), self.assertRaises(
                RouteCostCollectorError
            ):
                self._decode(_Response(body))

    def test_unicode_escapes_require_exactly_four_hexadecimal_digits(self):
        bodies = (
            b'"\\u-001"',
            b'"\\u+001"',
            b'"\\u 001"',
            b'"\\u0_01"',
            b'"\\ud800\\u-001"',
        )
        for body in bodies:
            with self.subTest(body=body), patch(
                "scripts.route_cost_collector.json.loads",
                side_effect=AssertionError("invalid escape was materialized"),
            ) as parser:
                with self.assertRaisesRegex(
                    RouteCostCollectorError, "JSON response is invalid"
                ) as raised:
                    self._decode(_Response(body))
            parser.assert_not_called()
            self.assertIsNone(raised.exception.__cause__)

    def test_integer_tokens_are_bounded_before_big_integer_materialization(self):
        exact = b"9" * 4096
        value = self._decode(
            _Response(exact, chunk_size=len(exact)),
            wire_limit=len(exact),
            decoded_limit=len(exact),
            scalar_limit=len(exact),
            node_limit=1,
        )
        self.assertEqual(len(str(value)), 4096)

        for body in (b"9" * 4097, b"-" + (b"9" * 4096), b"-0"):
            with self.subTest(size=len(body)), self.assertRaises(
                RouteCostCollectorError
            ):
                self._decode(
                    _Response(body, chunk_size=len(body)),
                    wire_limit=len(body),
                    decoded_limit=len(body),
                    scalar_limit=4096,
                    node_limit=1,
                )

    def test_numeric_scalar_limit_uses_the_preflight_lexical_width(self):
        body = b"[" + b",".join([b"1e2"] * 10) + b"]"
        value = self._decode(
            _Response(body, chunk_size=len(body)),
            wire_limit=len(body),
            decoded_limit=len(body),
            scalar_limit=30,
            node_limit=11,
        )
        self.assertEqual(value, [__import__("decimal").Decimal("1e2")] * 10)

        with patch(
            "scripts.route_cost_collector.json.loads",
            side_effect=AssertionError("over-limit numbers were materialized"),
        ) as parser, self.assertRaisesRegex(
            RouteCostCollectorError, "resource limit"
        ):
            self._decode(
                _Response(body, chunk_size=len(body)),
                wire_limit=len(body),
                decoded_limit=len(body),
                scalar_limit=29,
                node_limit=11,
            )
        parser.assert_not_called()

        for body in (b"9" * 4097, b"-" + (b"9" * 4097)):
            with self.subTest(independent_token_cap=len(body)), self.assertRaisesRegex(
                RouteCostCollectorError, "resource limit"
            ):
                self._decode(
                    _Response(body, chunk_size=len(body)),
                    wire_limit=len(body),
                    decoded_limit=len(body),
                    scalar_limit=6000,
                    node_limit=1,
                )

    def test_duplicate_keys_nan_noncanonical_body_and_redirect_reject(self):
        for body in (
            b'{"a":1,"a":2}', b'{"a":NaN}', b'1e1000000',
            b'1e-9999999', b'-0.0', b'1.234567890123456789',
        ):
            with self.subTest(body=body):
                if body == b'1.234567890123456789':
                    value = self._decode(_Response(body))
                    self.assertEqual(str(value), "1.234567890123456789")
                else:
                    with self.assertRaises(RouteCostCollectorError):
                        self._decode(_Response(body))
        with self.assertRaisesRegex(RouteCostCollectorError, "noncanonical"):
            self._decode(_Response(b"0.1"), require_canonical=True)
        with self.assertRaisesRegex(RouteCostCollectorError, "canonical"):
            self._decode(
                _Response(b'{ "a": 1 }'), require_canonical=True
            )
        handler = _NoRedirectHandler()
        self.assertIsNone(handler.redirect_request(
            None, None, 302, "Found", {}, "https://example.invalid/new"
        ))

    def test_profile_metadata_change_after_read_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory_name:
            profile = Path(directory_name) / "trace.json"
            _canonical_private_json(profile, _trace_profile())
            initial_metadata = profile.stat()
            original_read = os.read
            mutated = False

            def mutating_read(descriptor, maximum):
                nonlocal mutated
                data = original_read(descriptor, maximum)
                if data and not mutated:
                    mutated = True
                    replacement = data.replace(b"TRACE-SECRET", b"OTHER-SECRET")
                    self.assertEqual(len(replacement), len(data))
                    profile.write_bytes(replacement)
                    profile.chmod(0o600)
                    os.utime(
                        str(profile),
                        ns=(
                            initial_metadata.st_atime_ns,
                            initial_metadata.st_mtime_ns + 10_000_000_000,
                        ),
                    )
                    changed_metadata = os.fstat(descriptor)
                    self.assertEqual(
                        (
                            changed_metadata.st_dev,
                            changed_metadata.st_ino,
                            changed_metadata.st_size,
                        ),
                        (
                            initial_metadata.st_dev,
                            initial_metadata.st_ino,
                            initial_metadata.st_size,
                        ),
                    )
                    self.assertNotEqual(
                        changed_metadata.st_mtime_ns,
                        initial_metadata.st_mtime_ns,
                    )
                return data

            with patch.dict(
                os.environ, {TRACE_PROFILE_ENV: str(profile)}, clear=True
            ), patch("scripts.route_cost_collector.os.read", mutating_read):
                with self.assertRaisesRegex(
                    RouteCostCollectorError, "changed while reading"
                ):
                    load_route_cost_profile_capture()


if __name__ == "__main__":
    unittest.main()


def _retained_typed_loader_fixture(root):
    """Publish one real five-role typed inventory and its exact core lineage."""
    import hashlib

    from scripts.collect_route_cohort import publish_typed_source_manifest
    from scripts.route_quantity import V2PoolState
    from scripts.route_shadow_inputs import TYPED_SOURCE_ROLE_CONTRACTS

    cex_market = "cex:binance:AAA/USDT"
    pool = "0x" + "a" * 40
    dex_market = "dex:eth:uniswap_v2:{}:AAA".format(pool)
    state = V2PoolState(
        chain="eth",
        chain_id=1,
        dex="uniswap_v2",
        pool_address=pool,
        token0_address="0x" + "1" * 40,
        token1_address="0x" + "2" * 40,
        token0_decimals=18,
        token1_decimals=6,
        reserve0_raw=10**18,
        reserve1_raw=2_000_000,
        reserve_timestamp_last_raw=12_000,
        fee_bps=30,
        fee_numerator=9_970,
        fee_denominator=10_000,
        fee_formula=(
            "amount_in_with_fee=amount_in*fee_numerator;"
            "denominator=reserve_in*fee_denominator+amount_in_with_fee"
        ),
        fee_proof_sha256="6" * 64,
        block_number=100,
        block_hash="0x" + "7" * 64,
        block_header_sha256="8" * 64,
        observed_at="2026-08-01T12:00:00Z",
        raw_response_sha256="9" * 64,
    )
    integer_fields = {
        "chain_id", "token0_decimals", "token1_decimals", "reserve0_raw",
        "reserve1_raw", "reserve_timestamp_last_raw", "fee_bps",
        "fee_numerator", "fee_denominator", "block_number",
    }
    state_payload = {
        "schema": "route_v2_pool_state/v1",
        **{
            field: (
                str(getattr(state, field))
                if field in integer_fields else getattr(state, field)
            )
            for field in (
                "chain", "chain_id", "dex", "pool_address",
                "token0_address", "token1_address", "token0_decimals",
                "token1_decimals", "reserve0_raw", "reserve1_raw",
                "reserve_timestamp_last_raw", "fee_bps", "fee_numerator",
                "fee_denominator", "fee_formula", "fee_proof_sha256",
                "block_number", "block_hash", "block_header_sha256",
                "observed_at", "raw_response_sha256", "state_id",
            )
        },
    }
    state_bytes = route_cost_evidence.canonical_json_bytes(state_payload)
    payloads = {
        (cex_market, "cex_raw_book_response"): b'{"asks":[["2","3"]]}',
        (cex_market, "cex_market_rules"): b'{"market":"AAA/USDT"}',
        (cex_market, "quote_usd_conversion"): b'{"rate":"1"}',
        (dex_market, "dex_pool_state"): state_bytes,
        (dex_market, "dex_usd_price_context"): b'{"token0":"2"}',
    }
    producers = []
    for (market_id, role), payload in payloads.items():
        contract = TYPED_SOURCE_ROLE_CONTRACTS[role]
        producers.append({
            "market_id": market_id,
            "role": role,
            "payload": payload,
            "logical_generation": (
                state.state_id.split(":", 1)[1]
                if role == "dex_pool_state"
                else hashlib.sha256(payload).hexdigest()
            ),
            "adapter_id": contract["adapter_id"],
            "content_schema": contract["content_schema"],
        })
    run_id = "retained-loader-run"
    run_root = root / "raw" / "route-cohort" / run_id
    publication = publish_typed_source_manifest(
        run_root, raw_evidence_run_id=run_id, members=producers
    )
    descriptors = {
        (item["market_id"], item["role"]): item
        for item in publication["manifest"]["members"]
    }

    def lineage(market_id, roles):
        return {
            "schema": "route_leg_typed_source_lineage/v1",
            "members": [
                {
                    "role": role,
                    "status": "observed",
                    "reason_code": None,
                    **{
                        field: descriptors[(market_id, role)][field]
                        for field in (
                            "filename", "sha256", "size",
                            "logical_generation", "adapter_id",
                            "content_schema",
                        )
                    },
                }
                for role in roles
            ],
        }

    cohort = {
        "raw_evidence_run_id": run_id,
        "legs": [
            {
                "market_id": cex_market,
                "market_type": "cex",
                "typed_source_lineage": lineage(
                    cex_market,
                    ("cex_market_rules", "cex_raw_book_response",
                     "quote_usd_conversion"),
                ),
            },
            {
                "market_id": dex_market,
                "market_type": "dex",
                "typed_source_lineage": lineage(
                    dex_market,
                    ("dex_pool_state", "dex_usd_price_context"),
                ),
            },
        ],
    }
    return cohort, payloads, publication


class RetainedRouteCostTypedMemberLoaderTests(unittest.TestCase):
    def _load(self, root, cohort):
        from scripts.route_cost_collector import (
            _load_retained_route_cost_typed_members,
        )
        return _load_retained_route_cost_typed_members(root, cohort)

    def test_real_published_inventory_returns_all_roles_and_filterable_dex_state(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            cohort, payloads, publication = _retained_typed_loader_fixture(root)

            retained = self._load(root, cohort)

            self.assertEqual(set(retained), set(payloads))
            self.assertEqual(
                {key: item["payload"] for key, item in retained.items()},
                payloads,
            )
            self.assertEqual(
                [
                    item["descriptor"]["market_id"]
                    for key, item in retained.items()
                    if key[1] == "dex_pool_state"
                ],
                [cohort["legs"][1]["market_id"]],
            )
            self.assertEqual(
                publication["manifest"]["members"],
                [retained[key]["descriptor"] for key in sorted(retained)],
            )

    def test_market_id_prefix_must_match_the_declared_leg_type(self):
        cases = (
            (0, "dex:eth:counterfeit-cex-market"),
            (1, "cex:counterfeit:dex-market"),
            (0, "cex:garbage"),
            (0, "cex::AAA/USDT"),
            (0, "cex:binance:aaa/USDT"),
            (0, "cex:binance:AAA/USDT:extra"),
        )
        for leg_index, relabeled_market_id in cases:
            with self.subTest(market_id=relabeled_market_id), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                cohort, _payloads, publication = _retained_typed_loader_fixture(root)
                original_market_id = cohort["legs"][leg_index]["market_id"]
                cohort["legs"][leg_index]["market_id"] = relabeled_market_id
                for member in publication["manifest"]["members"]:
                    if member["market_id"] == original_market_id:
                        member["market_id"] = relabeled_market_id
                manifest_path = (
                    root / "raw" / "route-cohort"
                    / cohort["raw_evidence_run_id"] / "typed-manifest.json"
                )
                manifest_path.write_bytes(
                    route_cost_evidence.canonical_json_bytes(
                        publication["manifest"]
                    )
                )

                self._assert_rejected(root, cohort)

    def _assert_rejected(self, root, cohort):
        with self.assertRaisesRegex(
            RouteCostCollectorError, "retained route-cost typed evidence"
        ):
            self._load(root, cohort)

    def test_missing_extra_symlink_hardlink_and_hash_mutation_fail_closed(self):
        import shutil

        cases = ("missing", "extra", "symlink", "hardlink", "hash")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                cohort, _payloads, publication = _retained_typed_loader_fixture(root)
                run_root = (
                    root / "raw" / "route-cohort"
                    / cohort["raw_evidence_run_id"]
                )
                typed = run_root / "typed"
                filename = publication["manifest"]["members"][0]["filename"]
                member = typed / filename
                if case == "missing":
                    member.unlink()
                elif case == "extra":
                    (typed / "extra.json").write_bytes(b"{}")
                elif case == "symlink":
                    original = typed / "original.json"
                    member.rename(original)
                    member.symlink_to(original.name)
                elif case == "hardlink":
                    os.link(str(member), str(root / "external-hardlink.json"))
                else:
                    payload = member.read_bytes()
                    replacement = bytes([payload[0] ^ 1]) + payload[1:]
                    self.assertEqual(len(payload), len(replacement))
                    member.write_bytes(replacement)
                self._assert_rejected(root, cohort)
                shutil.rmtree(run_root, ignore_errors=True)

    def test_role_cap_plus_one_rejects_even_when_lineage_and_manifest_agree(self):
        import hashlib

        from scripts.route_shadow_inputs import TYPED_SOURCE_ROLE_CONTRACTS

        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            cohort, _payloads, publication = _retained_typed_loader_fixture(root)
            run_root = root / "raw/route-cohort" / cohort["raw_evidence_run_id"]
            record = next(
                item for item in publication["manifest"]["members"]
                if item["role"] == "cex_market_rules"
            )
            payload = b"x" * (
                TYPED_SOURCE_ROLE_CONTRACTS["cex_market_rules"]["max_bytes"] + 1
            )
            (run_root / "typed" / record["filename"]).write_bytes(payload)
            record["size"] = len(payload)
            record["sha256"] = hashlib.sha256(payload).hexdigest()
            lineage = cohort["legs"][0]["typed_source_lineage"]["members"]
            core_record = next(
                item for item in lineage if item["role"] == "cex_market_rules"
            )
            core_record["size"] = record["size"]
            core_record["sha256"] = record["sha256"]
            (run_root / "typed-manifest.json").write_bytes(
                route_cost_evidence.canonical_json_bytes(publication["manifest"])
            )

            self._assert_rejected(root, cohort)

    def test_manifest_exact_schema_bytes_count_and_members_are_enforced(self):
        mutations = ("extra-field", "noncanonical", "count", "members")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                cohort, _payloads, publication = _retained_typed_loader_fixture(root)
                manifest_path = (
                    root / "raw/route-cohort" / cohort["raw_evidence_run_id"]
                    / "typed-manifest.json"
                )
                manifest = publication["manifest"]
                if mutation == "extra-field":
                    manifest["extra"] = True
                elif mutation == "noncanonical":
                    manifest_path.write_text(
                        json.dumps(manifest, indent=2), encoding="utf-8"
                    )
                    self._assert_rejected(root, cohort)
                    continue
                elif mutation == "count":
                    manifest["member_count"] += 1
                else:
                    manifest["members"] = list(reversed(manifest["members"]))
                manifest_path.write_bytes(
                    route_cost_evidence.canonical_json_bytes(manifest)
                )
                self._assert_rejected(root, cohort)

    def test_invalid_pool_state_rejects_the_whole_five_role_result(self):
        import hashlib

        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            cohort, _payloads, publication = _retained_typed_loader_fixture(root)
            run_root = root / "raw/route-cohort" / cohort["raw_evidence_run_id"]
            record = next(
                item for item in publication["manifest"]["members"]
                if item["role"] == "dex_pool_state"
            )
            member_path = run_root / "typed" / record["filename"]
            value = json.loads(member_path.read_bytes())
            value["pool_address"] = "0x" + "b" * 40
            payload = route_cost_evidence.canonical_json_bytes(value)
            member_path.write_bytes(payload)
            record["size"] = len(payload)
            record["sha256"] = hashlib.sha256(payload).hexdigest()
            core_record = next(
                item
                for item in cohort["legs"][1]["typed_source_lineage"]["members"]
                if item["role"] == "dex_pool_state"
            )
            core_record["size"] = record["size"]
            core_record["sha256"] = record["sha256"]
            (run_root / "typed-manifest.json").write_bytes(
                route_cost_evidence.canonical_json_bytes(publication["manifest"])
            )

            self._assert_rejected(root, cohort)

    def test_non_pool_logical_generation_must_equal_the_retained_bytes_hash(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            cohort, _payloads, publication = _retained_typed_loader_fixture(root)
            record = next(
                item for item in publication["manifest"]["members"]
                if item["role"] == "cex_market_rules"
            )
            record["logical_generation"] = "f" * 64
            core_record = next(
                item
                for item in cohort["legs"][0]["typed_source_lineage"]["members"]
                if item["role"] == "cex_market_rules"
            )
            core_record["logical_generation"] = record["logical_generation"]
            manifest_path = (
                root / "raw" / "route-cohort"
                / cohort["raw_evidence_run_id"] / "typed-manifest.json"
            )
            manifest_path.write_bytes(
                route_cost_evidence.canonical_json_bytes(
                    publication["manifest"]
                )
            )

            self._assert_rejected(root, cohort)

    def test_private_retain_keys_still_validate_all_bytes_but_return_only_selected_pool(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            cohort, payloads, _publication = _retained_typed_loader_fixture(root)
            dex_market = cohort["legs"][1]["market_id"]
            retained = self._load_with_keys(
                root, cohort,
                frozenset({(dex_market, "dex_pool_state")}),
            )
            self.assertEqual(set(retained), {(dex_market, "dex_pool_state")})
            self.assertEqual(
                retained[(dex_market, "dex_pool_state")]["payload"],
                payloads[(dex_market, "dex_pool_state")],
            )

    def _load_with_keys(self, root, cohort, retain_keys):
        from scripts.route_cost_collector import (
            _load_retained_route_cost_typed_members,
        )
        return _load_retained_route_cost_typed_members(
            root, cohort, retain_keys=retain_keys
        )

    def test_manifest_member_and_ancestor_swaps_during_reread_are_rejected(self):
        import shutil

        for target in ("manifest", "member", "typed-dir", "ancestor"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                cohort, _payloads, publication = _retained_typed_loader_fixture(root)
                run_root = root / "raw/route-cohort" / cohort["raw_evidence_run_id"]
                manifest_path = run_root / "typed-manifest.json"
                typed = run_root / "typed"
                member_name = publication["manifest"]["members"][0]["filename"]
                member_path = typed / member_name
                original_open = os.open
                original_listdir = os.listdir
                open_hits = 0
                list_hits = 0

                def swapping_open(path, flags, *args, **kwargs):
                    nonlocal open_hits
                    if path == (
                        "typed-manifest.json" if target == "manifest" else member_name
                    ):
                        open_hits += 1
                        if open_hits == 2:
                            selected = (
                                manifest_path
                                if target == "manifest" else member_path
                            )
                            payload = selected.read_bytes()
                            replacement = selected.with_name(
                                selected.name + ".replacement"
                            )
                            replacement.write_bytes(payload)
                            os.replace(str(replacement), str(selected))
                    return original_open(path, flags, *args, **kwargs)

                def swapping_listdir(path):
                    nonlocal list_hits
                    result = original_listdir(path)
                    list_hits += 1
                    if list_hits == 2 and target in {"typed-dir", "ancestor"}:
                        if target == "typed-dir":
                            moved = run_root / "typed-old"
                            typed.rename(moved)
                            shutil.copytree(moved, typed)
                        else:
                            raw = root / "raw"
                            moved = root / "raw-old"
                            raw.rename(moved)
                            shutil.copytree(moved, raw)
                    return result

                patches = []
                if target in {"manifest", "member"}:
                    patches.append(patch(
                        "scripts.route_cost_collector.os.open", swapping_open
                    ))
                else:
                    patches.append(patch(
                        "scripts.route_cost_collector.os.listdir", swapping_listdir
                    ))
                with patches[0]:
                    self._assert_rejected(root, cohort)

    def test_symlinked_data_ancestor_and_raw_run_are_rejected(self):
        import shutil

        for target in ("data-ancestor", "run"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as name:
                container = Path(name)
                real = container / "real"
                real.mkdir()
                cohort, _payloads, _publication = _retained_typed_loader_fixture(real)
                if target == "data-ancestor":
                    linked = container / "linked"
                    linked.symlink_to(real, target_is_directory=True)
                    root = linked
                else:
                    run = real / "raw/route-cohort" / cohort["raw_evidence_run_id"]
                    moved = run.with_name(run.name + "-real")
                    run.rename(moved)
                    run.symlink_to(moved.name, target_is_directory=True)
                    root = real
                self._assert_rejected(root, cohort)
                shutil.rmtree(real, ignore_errors=True)

    def test_manifest_and_member_close_failures_are_redacted(self):
        for target in ("typed-manifest.json", "0000-cex_market_rules.json"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                cohort, _payloads, publication = _retained_typed_loader_fixture(root)
                if target != "typed-manifest.json":
                    target = next(
                        item["filename"]
                        for item in publication["manifest"]["members"]
                        if item["role"] == "cex_market_rules"
                    )
                original_open = os.open
                original_close = os.close
                target_fd = None
                successful_closes = []

                def tracking_open(path, flags, *args, **kwargs):
                    nonlocal target_fd
                    descriptor = original_open(path, flags, *args, **kwargs)
                    if path == target and target_fd is None:
                        target_fd = descriptor
                    return descriptor

                def failing_close(descriptor):
                    original_close(descriptor)
                    if descriptor == target_fd:
                        raise OSError("/private/SECRET-TYPED-CLOSE")
                    successful_closes.append(descriptor)

                with patch(
                    "scripts.route_cost_collector.os.open", tracking_open
                ), patch(
                    "scripts.route_cost_collector.os.close", failing_close
                ):
                    try:
                        self._load(root, cohort)
                    except RouteCostCollectorError as error:
                        rendered = "".join(traceback.format_exception(
                            type(error), error, error.__traceback__
                        ))
                    else:
                        self.fail("typed close failure unexpectedly succeeded")
                self.assertIsNotNone(target_fd)
                self.assertGreater(len(successful_closes), 1)
                self.assertNotIn("SECRET-TYPED-CLOSE", rendered)

    def test_directory_check_close_failure_does_not_mask_process_control(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            cohort, _payloads, _publication = _retained_typed_loader_fixture(root)
            original_open = os.open
            original_close = os.close
            original_fstat = os.fstat
            typed_fd = None
            successful_closes = []

            def tracking_open(path, flags, *args, **kwargs):
                nonlocal typed_fd
                descriptor = original_open(path, flags, *args, **kwargs)
                if path == "typed":
                    typed_fd = descriptor
                return descriptor

            def interrupting_fstat(descriptor):
                if descriptor == typed_fd:
                    raise KeyboardInterrupt()
                return original_fstat(descriptor)

            def failing_close(descriptor):
                original_close(descriptor)
                if descriptor == typed_fd:
                    raise OSError("/private/SECRET-TYPED-DIRECTORY-CLOSE")
                successful_closes.append(descriptor)

            with patch(
                "scripts.route_cost_collector.os.open", tracking_open
            ), patch(
                "scripts.route_cost_collector.os.fstat", interrupting_fstat
            ), patch(
                "scripts.route_cost_collector.os.close", failing_close
            ), self.assertRaises(KeyboardInterrupt):
                self._load(root, cohort)
            self.assertIsNotNone(typed_fd)
            self.assertGreater(len(successful_closes), 1)


class ProductionRouteCostCollectorTests(unittest.TestCase):
    def test_configured_connector_hardstop_precedes_phase_a_network_work(self):
        from tests.test_route_cost_evidence import (
            MARKET_ID,
            adapter_registry,
            connector_registry,
            retained_v2_pool_state,
            universe_for,
        )
        from scripts.route_cost_collector import (
            _terminal_manifest,
            load_route_cost_profile_capture,
        )

        universe = universe_for()
        retained = {MARKET_ID: retained_v2_pool_state(block_number=20_000_000)}
        base = load_route_cost_profile_capture()
        trace_identity, trace_generation = route_cost_evidence.trace_profile_identity(
            _trace_profile()
        )
        connector_identity, connector_generation = (
            route_cost_evidence.submission_connector_profile_identity(
                _connector_profile()
            )
        )
        profiles = type(base)(
            trace_profile=_trace_profile(),
            connector_profile=_connector_profile(),
            trace_profile_identity=trace_identity,
            trace_profile_generation=trace_generation,
            submission_connector_profile_identity=connector_identity,
            submission_connector_profile_generation=connector_generation,
        )
        capability = _RouteCostCapability(
            lambda _body: (_ for _ in ()).throw(
                AssertionError("configured connector released trace RPC")
            )
        )
        with patch(
            "scripts.route_cost_collector._capture_phase_a_result",
            side_effect=AssertionError("configured connector started Phase A"),
        ) as phase_a, self.assertRaisesRegex(
            RouteCostCollectorError,
            "configured submission connector collection is not implemented",
        ):
            _terminal_manifest(
                universe=universe,
                run_id="collector-run",
                route_cohort_id=_COLLECTOR_COHORT_ID,
                phase="canary",
                candidate_source_generation=universe[
                    "candidate_source_generation"
                ],
                route_universe_sha256=route_cost_evidence.physical_sha256(
                    universe
                ),
                adapter_registry=adapter_registry(supported=True),
                connector_key_registry=connector_registry(),
                profiles=profiles,
                retained_pool_members=retained,
                supported=(MARKET_ID,),
                capability=capability,
            )
        phase_a.assert_not_called()
        self.assertEqual(capability.rpc_requests, [])

    def test_public_signature_is_closed_and_all_unsupported_preserves_profiles(self):
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
            collect_route_cost_evidence_manifest,
        )

        self.assertEqual(
            tuple(inspect.signature(collect_route_cost_evidence_manifest).parameters),
            (
                "data_dir", "universe", "cohort", "run_id", "phase",
                "route_universe_sha256",
            ),
        )
        universe = _collector_universe()
        cohort = _collector_cohort(universe)
        evaluated_at = "2026-08-01T12:00:04Z"
        with tempfile.TemporaryDirectory() as name, patch.dict(
            os.environ, {}, clear=True
        ), patch(
            "scripts.route_cost_collector._load_retained_route_cost_typed_members",
            return_value={},
        ) as retained_loader, patch(
            "scripts.route_cost_collector.urllib.request.urlopen",
            side_effect=AssertionError("unsupported terminal path performed network I/O"),
        ):
            manifest = _collect_route_cost_evidence_manifest_with_capability(
                Path(name), universe=universe, cohort=cohort,
                run_id="collector-run", phase="canary",
                route_universe_sha256=(
                    hashlib.sha256(
                        route_cost_evidence.canonical_json_bytes(universe)
                    ).hexdigest()
                ),
                capability=lambda: evaluated_at,
            )

        retained_loader.assert_called_once_with(
            Path(name), unittest.mock.ANY, retain_keys=frozenset()
        )
        self.assertEqual(manifest["evaluated_at"], evaluated_at)
        self.assertEqual(manifest["route_cohort_id"], cohort["route_cohort_id"])
        self.assertEqual(manifest["trace_profile_identity"]["status"], "missing")
        self.assertEqual(
            manifest["submission_connector_profile_identity"]["status"],
            "missing",
        )
        self.assertEqual(manifest["submission_policy_snapshot"]["reason_code"], "scope_empty")
        self.assertEqual(manifest["transcript_count"], 0)

    def test_all_unsupported_available_profiles_remain_identity_only_and_zero_network(self):
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = _collector_universe()
        cohort = _collector_cohort(universe)
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            trace_path = root / "trace.json"
            connector_path = root / "connector.json"
            _canonical_private_json(trace_path, _trace_profile())
            _canonical_private_json(connector_path, _connector_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(trace_path),
                CONNECTOR_PROFILE_ENV: str(connector_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value={},
            ), patch(
                "scripts.route_cost_collector.urllib.request.urlopen",
                side_effect=AssertionError("empty cost scope performed network I/O"),
            ):
                manifest = _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=universe, cohort=cohort,
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=hashlib.sha256(
                        route_cost_evidence.canonical_json_bytes(universe)
                    ).hexdigest(),
                    capability=lambda: "2026-08-01T12:00:04Z",
                )

        self.assertEqual(manifest["trace_profile_identity"]["status"], "available")
        self.assertEqual(
            manifest["submission_connector_profile_identity"]["status"],
            "available",
        )
        self.assertEqual(manifest["submission_policy_snapshot"]["reason_code"], "scope_empty")
        serialized = route_cost_evidence.canonical_json_bytes(manifest)
        self.assertNotIn(b"SECRET", serialized)
        self.assertNotIn(b"example.invalid", serialized)

    def test_selected_supported_without_pool_state_keeps_ten_terminal_rows(self):
        from tests.test_route_cost_evidence import (
            MARKET_ID,
            adapter_registry,
            universe_for,
        )
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = universe_for()
        normalized = {
            "route_cohort_id": _COLLECTOR_COHORT_ID,
            "raw_evidence_run_id": "collector-raw-run",
            "candidate_source_generation": universe[
                "candidate_source_generation"
            ],
            "legs": [_collector_core_leg(universe["selected_legs"][0])],
            "routes": copy.deepcopy(universe["routes"]),
        }
        with tempfile.TemporaryDirectory() as name, patch.dict(
            os.environ, {}, clear=True
        ), patch(
            "scripts.route_cost_collector._validate_route_cost_universe",
            return_value=universe,
        ), patch(
            "scripts.route_cost_collector._normalize_route_cost_cohort",
            return_value=normalized,
        ), patch(
            "scripts.route_cost_collector._load_retained_route_cost_typed_members",
            return_value={},
        ), patch(
            "scripts.route_cost_collector.load_route_cost_adapter_registry",
            return_value=adapter_registry(supported=True),
        ), patch(
            "scripts.route_cost_collector.urllib.request.urlopen",
            side_effect=AssertionError("missing core path performed network I/O"),
        ):
            manifest = _collect_route_cost_evidence_manifest_with_capability(
                Path(name), universe=universe, cohort={},
                run_id="collector-run", phase="canary",
                route_universe_sha256=hashlib.sha256(
                    route_cost_evidence.canonical_json_bytes(universe)
                ).hexdigest(),
                capability=lambda: "2026-08-01T12:00:04Z",
            )

        self.assertEqual(manifest["transcript_count"], 10)
        self.assertEqual(
            {(row["reason_code"], row["core_pool_state_id"],
              row["simulation_target_sha256"])
             for row in manifest["transcripts"]},
            {("core_pool_state_unavailable", None, None)},
        )
        self.assertEqual(manifest["selected_market_count"], 1)

    def test_configured_trace_missing_core_is_terminal_without_rpc(self):
        from tests.test_route_cost_evidence import (
            adapter_registry,
            universe_for,
        )
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = universe_for()
        normalized = {
            "route_cohort_id": _COLLECTOR_COHORT_ID,
            "raw_evidence_run_id": "collector-raw-run",
            "candidate_source_generation": universe[
                "candidate_source_generation"
            ],
            "legs": [_collector_core_leg(universe["selected_legs"][0])],
            "routes": copy.deepcopy(universe["routes"]),
        }
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            profile_path = root / "trace.json"
            _canonical_private_json(profile_path, _trace_profile())
            capability = _RouteCostCapability(
                lambda _request: self.fail("missing core attempted RPC")
            )
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(profile_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._validate_route_cost_universe",
                return_value=universe,
            ), patch(
                "scripts.route_cost_collector._normalize_route_cost_cohort",
                return_value=normalized,
            ), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value={},
            ), patch(
                "scripts.route_cost_collector.load_route_cost_adapter_registry",
                return_value=adapter_registry(supported=True),
            ):
                manifest = _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=universe, cohort={},
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=hashlib.sha256(
                        route_cost_evidence.canonical_json_bytes(universe)
                    ).hexdigest(),
                    capability=capability,
                )

        self.assertEqual(capability.rpc_requests, [])
        self.assertEqual(manifest["transcript_count"], 10)
        self.assertEqual(
            {(row["status"], row["reason_code"])
             for row in manifest["transcripts"]},
            {("unavailable", "core_pool_state_unavailable")},
        )

    def test_configured_trace_timeout_is_one_attempt_and_rpc_unavailable(self):
        from tests.test_route_cost_evidence import (
            MARKET_ID,
            adapter_registry,
            retained_v2_pool_state,
            universe_for,
        )
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = universe_for()
        retained = retained_v2_pool_state(block_number=20_000_000)
        normalized = {
            "route_cohort_id": _COLLECTOR_COHORT_ID,
            "raw_evidence_run_id": "collector-raw-run",
            "candidate_source_generation": universe[
                "candidate_source_generation"
            ],
            "legs": [_collector_core_leg(universe["selected_legs"][0])],
            "routes": copy.deepcopy(universe["routes"]),
        }
        capability = _RouteCostCapability(
            lambda _request: (_ for _ in ()).throw(TimeoutError("TRACE-SECRET"))
        )
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            profile_path = root / "trace.json"
            _canonical_private_json(profile_path, _trace_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(profile_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._validate_route_cost_universe",
                return_value=universe,
            ), patch(
                "scripts.route_cost_collector._normalize_route_cost_cohort",
                return_value=normalized,
            ), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value={(MARKET_ID, "dex_pool_state"): retained},
            ), patch(
                "scripts.route_cost_collector.load_route_cost_adapter_registry",
                return_value=adapter_registry(supported=True),
            ):
                manifest = _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=universe, cohort={},
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=hashlib.sha256(
                        route_cost_evidence.canonical_json_bytes(universe)
                    ).hexdigest(),
                    capability=capability,
                )

        self.assertEqual(len(capability.rpc_requests), 1)
        requests = json.loads(capability.rpc_requests[0].decode("utf-8"))
        self.assertEqual([row["id"] for row in requests], list(range(1, 12)))
        self.assertEqual(
            [row["method"] for row in requests[:3]],
            ["eth_chainId", "eth_getBlockByNumber", "eth_feeHistory"],
        )
        block_tag = hex(20_000_000)
        adapter = adapter_registry(supported=True)["adapters"][0]
        state = json.loads(retained["payload"])
        pair = adapter["pair_descriptors"][0]
        self.assertEqual(
            [(row["method"], row["params"]) for row in requests],
            [
                ("eth_chainId", []),
                ("eth_getBlockByNumber", [block_tag, False]),
                ("eth_feeHistory", ["0x1", block_tag, [50]]),
                ("eth_getCode", [adapter["router_address"], block_tag]),
                ("eth_getCode", [adapter["factory_address"], block_tag]),
                ("eth_call", [{
                    "to": adapter["factory_address"],
                    "data": route_cost_evidence.build_factory_get_pair_calldata(
                        state["token0_address"], state["token1_address"]
                    ),
                }, block_tag]),
                ("eth_getCode", [pair["pair_address"], block_tag]),
                ("eth_call", [{
                    "to": pair["pair_address"], "data": "0x0dfe1681",
                }, block_tag]),
                ("eth_call", [{
                    "to": pair["pair_address"], "data": "0xd21220a7",
                }, block_tag]),
                ("eth_getCode", [state["token0_address"], block_tag]),
                ("eth_getCode", [state["token1_address"], block_tag]),
            ],
        )
        self.assertEqual(manifest["transcript_count"], 10)
        self.assertEqual(
            {(row["status"], row["reason_code"])
             for row in manifest["transcripts"]},
            {("unavailable", "rpc_unavailable")},
        )
        rendered = route_cost_evidence.canonical_json_bytes(manifest)
        self.assertNotIn(b"TRACE-SECRET", rendered)
        self.assertNotIn(b"example.invalid", rendered)

    def test_configured_trace_invalid_batch_is_failed_without_partial_evidence(self):
        from tests.test_route_cost_evidence import (
            MARKET_ID,
            adapter_registry,
            retained_v2_pool_state,
            universe_for,
        )
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = universe_for()
        retained = retained_v2_pool_state(block_number=20_000_000)
        normalized = {
            "route_cohort_id": _COLLECTOR_COHORT_ID,
            "raw_evidence_run_id": "collector-raw-run",
            "candidate_source_generation": universe[
                "candidate_source_generation"
            ],
            "legs": [_collector_core_leg(universe["selected_legs"][0])],
            "routes": copy.deepcopy(universe["routes"]),
        }
        invalid_responses = {
            "duplicate_id": route_cost_evidence.canonical_json_bytes([
                {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
            ] * 11),
            "missing_id": route_cost_evidence.canonical_json_bytes([
                {"jsonrpc": "2.0", "id": value, "result": "0x1"}
                for value in range(1, 11)
            ]),
            "extra_id": route_cost_evidence.canonical_json_bytes([
                {"jsonrpc": "2.0", "id": value, "result": "0x1"}
                for value in range(1, 13)
            ]),
            "wrong_id": route_cost_evidence.canonical_json_bytes([
                {"jsonrpc": "2.0", "id": value, "result": "0x1"}
                for value in list(range(1, 11)) + [12]
            ]),
            "noncanonical": b'[ {"jsonrpc":"2.0","id":1,"result":"0x1"} ]',
            "malformed": b"[",
        }
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            profile_path = root / "trace.json"
            _canonical_private_json(profile_path, _trace_profile())
            for label, response in invalid_responses.items():
                capability = _RouteCostCapability(lambda _request, response=response: response)
                with self.subTest(case=label), patch.dict(os.environ, {
                    TRACE_PROFILE_ENV: str(profile_path),
                }, clear=True), patch(
                    "scripts.route_cost_collector._validate_route_cost_universe",
                    return_value=universe,
                ), patch(
                    "scripts.route_cost_collector._normalize_route_cost_cohort",
                    return_value=normalized,
                ), patch(
                    "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                    return_value={(MARKET_ID, "dex_pool_state"): retained},
                ), patch(
                    "scripts.route_cost_collector.load_route_cost_adapter_registry",
                    return_value=adapter_registry(supported=True),
                ):
                    manifest = _collect_route_cost_evidence_manifest_with_capability(
                        root, universe=universe, cohort={},
                        run_id="collector-run", phase="canary",
                        route_universe_sha256=hashlib.sha256(
                            route_cost_evidence.canonical_json_bytes(universe)
                        ).hexdigest(),
                        capability=capability,
                    )

                self.assertEqual(len(capability.rpc_requests), 1)
                self.assertEqual(manifest["chain_evidence"], [])
                self.assertEqual(manifest["market_evidence"], [])
                self.assertEqual(
                    {(row["status"], row["reason_code"])
                     for row in manifest["transcripts"]},
                    {("failed", "rpc_invalid")},
                )

    def test_canonical_phase_a_success_publishes_full_phase_b_evidence(self):
        from tests.test_route_cost_evidence import MARKET_ID
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )
        fixture = _phase_b_one_market_wire_fixture()
        capability = _RouteCostCapability(
            fixture["rpc"],
            monotonic_values=[
                100.0, 101.0, 102.0,
                200.0, 201.0, 202.0, 203.0, 204.0,
            ],
            utc_values=[
                "2026-08-01T12:00:02Z",
                "2026-08-01T12:00:09Z",
            ],
        )
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            trace_path = root / "trace.json"
            _canonical_private_json(trace_path, _trace_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(trace_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._validate_route_cost_universe",
                return_value=fixture["universe"],
            ), patch(
                "scripts.route_cost_collector._normalize_route_cost_cohort",
                return_value=fixture["normalized"],
            ), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value={
                    (MARKET_ID, "dex_pool_state"): fixture["retained"]
                },
            ), patch(
                "scripts.route_cost_collector.load_route_cost_adapter_registry",
                return_value=fixture["registry"],
            ), patch(
                "scripts.route_cost_collector.load_route_cost_connector_key_registry",
                return_value=fixture["keys"],
            ), patch(
                "scripts.route_cost_collector._capture_native_price_evidence",
                return_value=fixture["native_result"],
            ):
                manifest = _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=fixture["universe"], cohort={},
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=fixture["universe_sha"],
                    capability=capability,
                )
        decoded = [
            json.loads(body.decode("utf-8"))
            for body in capability.rpc_requests
        ]
        self.assertEqual([len(batch) for batch in decoded], [11, 10, 10])
        self.assertEqual(
            [{row["method"] for row in batch} for batch in decoded],
            [
                {
                    "eth_chainId", "eth_getBlockByNumber", "eth_feeHistory",
                    "eth_getCode", "eth_call",
                },
                {"eth_estimateGas"},
                {"debug_traceCall"},
            ],
        )
        self.assertEqual(
            [row["id"] for batch in decoded for row in batch],
            list(range(1, 32)),
        )
        self.assertEqual(manifest["transcript_count"], 10)
        self.assertEqual(manifest["chain_evidence_count"], 1)
        self.assertEqual(manifest["market_evidence_count"], 1)
        self.assertEqual(
            {(row["status"], row["completed_stage"], row["reason_code"])
             for row in manifest["transcripts"]},
            {("observed", "transfer_tax", None)},
        )
        self.assertEqual(manifest["counts"]["transcript_observed"], 10)
        self.assertEqual(manifest["evaluated_at"], "2026-08-01T12:00:09Z")
        self.assertEqual(
            (manifest["submission_policy_snapshot"]["status"],
             manifest["submission_policy_snapshot"]["reason_code"]),
            ("unavailable", "submission_connector_missing"),
        )
        self.assertEqual(
            {(row["status"], row["reason_code"])
             for row in manifest["bindings"]},
            {("unavailable", "submission_policy_unavailable")},
        )
        for batch in decoded[1:]:
            self.assertTrue(all("schema" not in row for row in batch))
        rendered = route_cost_evidence.canonical_json_bytes(manifest)
        self.assertNotIn(b"TRACE-SECRET", rendered)
        self.assertNotIn(b"example.invalid", rendered)

    def test_native_price_terminal_keeps_phase_a_and_releases_no_phase_b_rpc(self):
        from tests.test_route_cost_evidence import MARKET_ID
        from scripts.route_cost_collector import (
            _NativePriceCaptureResult,
            _collect_route_cost_evidence_manifest_with_capability,
        )

        for reason, native_status, transcript_status, chain_status in (
            (
                "native_price_unavailable", "unavailable",
                "unavailable", "incomplete",
            ),
            ("native_price_invalid", "failed", "failed", "failed"),
        ):
            fixture = _phase_b_one_market_wire_fixture()
            capability = _RouteCostCapability(
                fixture["rpc"],
                monotonic_values=[100.0, 101.0, 102.0],
                utc_values=[
                    "2026-08-01T12:00:02Z",
                    "2026-08-01T12:00:09Z",
                ],
            )
            native_result = _NativePriceCaptureResult(
                status=native_status, reason_code=reason, evidence=None
            )
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                trace_path = root / "trace.json"
                _canonical_private_json(trace_path, _trace_profile())
                with patch.dict(os.environ, {
                    TRACE_PROFILE_ENV: str(trace_path),
                }, clear=True), patch(
                    "scripts.route_cost_collector._validate_route_cost_universe",
                    return_value=fixture["universe"],
                ), patch(
                    "scripts.route_cost_collector._normalize_route_cost_cohort",
                    return_value=fixture["normalized"],
                ), patch(
                    "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                    return_value={
                        (MARKET_ID, "dex_pool_state"): fixture["retained"]
                    },
                ), patch(
                    "scripts.route_cost_collector.load_route_cost_adapter_registry",
                    return_value=fixture["registry"],
                ), patch(
                    "scripts.route_cost_collector.load_route_cost_connector_key_registry",
                    return_value=fixture["keys"],
                ), patch(
                    "scripts.route_cost_collector._capture_native_price_evidence",
                    return_value=native_result,
                ):
                    manifest = _collect_route_cost_evidence_manifest_with_capability(
                        root, universe=fixture["universe"], cohort={},
                        run_id="collector-run", phase="canary",
                        route_universe_sha256=fixture["universe_sha"],
                        capability=capability,
                    )

            decoded = [
                json.loads(body.decode("utf-8"))
                for body in capability.rpc_requests
            ]
            self.assertEqual([len(batch) for batch in decoded], [11])
            self.assertFalse(any(
                row["method"] in {"eth_estimateGas", "debug_traceCall"}
                for batch in decoded for row in batch
            ))
            self.assertEqual(manifest["native_price_evidence"], None)
            self.assertEqual(manifest["chain_evidence_count"], 1)
            self.assertEqual(manifest["market_evidence_count"], 1)
            self.assertEqual(manifest["transcript_count"], 10)
            self.assertEqual(
                (manifest["chain_evidence"][0]["status"],
                 manifest["chain_evidence"][0]["reason_code"]),
                (chain_status, reason),
            )
            self.assertEqual(
                {
                    (row["status"], row["completed_stage"], row["reason_code"])
                    for row in manifest["transcripts"]
                },
                {(transcript_status, "call", reason)},
            )
            self.assertTrue(all(
                row["raw_transcript"]["estimate_gas_request"] is None
                and row["raw_transcript"]["simulation_request"] is None
                for row in manifest["transcripts"]
            ))
            self.assertEqual(
                route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                    manifest,
                    universe=fixture["universe"],
                    expected_run_id="collector-run",
                    expected_route_cohort_id=_COLLECTOR_COHORT_ID,
                    expected_phase="canary",
                    expected_candidate_source_generation=fixture[
                        "universe"
                    ]["candidate_source_generation"],
                    expected_route_universe_sha256=fixture["universe_sha"],
                    retained_typed_pool_state_members={
                        MARKET_ID: fixture["retained"]
                    },
                ),
                manifest,
            )

    def test_native_terminal_with_missing_core_keeps_twenty_rows_and_one_rpc_batch(self):
        from tests.test_route_cost_evidence import MARKET_ID
        from scripts.route_cost_collector import (
            _NativePriceCaptureResult,
            _collect_route_cost_evidence_manifest_with_capability,
        )

        fixture = _phase_b_one_market_wire_fixture(
            include_missing_supported=True
        )
        missing_market = fixture["missing_supported_id"]
        capability = _RouteCostCapability(
            fixture["rpc"],
            monotonic_values=[100.0, 101.0, 102.0],
            utc_values=[
                "2026-08-01T12:00:02Z",
                "2026-08-01T12:00:09Z",
            ],
        )
        native_result = _NativePriceCaptureResult(
            status="unavailable",
            reason_code="native_price_unavailable",
            evidence=None,
        )
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            trace_path = root / "trace.json"
            _canonical_private_json(trace_path, _trace_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(trace_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._validate_route_cost_universe",
                return_value=fixture["universe"],
            ), patch(
                "scripts.route_cost_collector._normalize_route_cost_cohort",
                return_value=fixture["normalized"],
            ), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value={
                    (MARKET_ID, "dex_pool_state"): fixture["retained"]
                },
            ), patch(
                "scripts.route_cost_collector.load_route_cost_adapter_registry",
                return_value=fixture["registry"],
            ), patch(
                "scripts.route_cost_collector.load_route_cost_connector_key_registry",
                return_value=fixture["keys"],
            ), patch(
                "scripts.route_cost_collector._capture_native_price_evidence",
                return_value=native_result,
            ):
                manifest = _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=fixture["universe"], cohort={},
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=fixture["universe_sha"],
                    capability=capability,
                )

        decoded = [
            json.loads(body.decode("utf-8"))
            for body in capability.rpc_requests
        ]
        self.assertEqual([len(batch) for batch in decoded], [11])
        self.assertEqual(manifest["transcript_count"], 20)
        self.assertEqual(
            {
                (row["status"], row["completed_stage"], row["reason_code"])
                for row in manifest["transcripts"]
                if row["market_id"] == MARKET_ID
            },
            {("unavailable", "call", "native_price_unavailable")},
        )
        self.assertEqual(
            {
                (row["status"], row["completed_stage"], row["reason_code"])
                for row in manifest["transcripts"]
                if row["market_id"] == missing_market
            },
            {("unavailable", "none", "core_pool_state_unavailable")},
        )

    def test_phase_b_mixed_selected_denominator_keeps_unsupported_rows(self):
        from tests.test_route_cost_evidence import MARKET_ID
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        fixture = _phase_b_one_market_wire_fixture(include_unsupported=True)
        capability = _RouteCostCapability(
            fixture["rpc"],
            monotonic_values=[
                100.0, 101.0, 102.0,
                200.0, 201.0, 202.0, 203.0, 204.0,
            ],
            utc_values=[
                "2026-08-01T12:00:02Z", "2026-08-01T12:00:09Z",
            ],
        )
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            trace_path = root / "trace.json"
            _canonical_private_json(trace_path, _trace_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(trace_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._validate_route_cost_universe",
                return_value=fixture["universe"],
            ), patch(
                "scripts.route_cost_collector._normalize_route_cost_cohort",
                return_value=fixture["normalized"],
            ), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value={
                    (MARKET_ID, "dex_pool_state"): fixture["retained"]
                },
            ), patch(
                "scripts.route_cost_collector.load_route_cost_adapter_registry",
                return_value=fixture["registry"],
            ), patch(
                "scripts.route_cost_collector.load_route_cost_connector_key_registry",
                return_value=fixture["keys"],
            ), patch(
                "scripts.route_cost_collector._capture_native_price_evidence",
                return_value=fixture["native_result"],
            ):
                manifest = _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=fixture["universe"], cohort={},
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=fixture["universe_sha"],
                    capability=capability,
                )
        self.assertEqual(manifest["selected_market_count"], 2)
        self.assertEqual(manifest["transcript_count"], 20)
        by_market = {}
        for row in manifest["transcripts"]:
            by_market.setdefault(row["market_id"], set()).add(
                (row["status"], row["completed_stage"], row["reason_code"])
            )
        self.assertEqual(
            by_market[MARKET_ID], {("observed", "transfer_tax", None)}
        )
        unsupported_id = next(
            market_id for market_id in by_market if market_id != MARKET_ID
        )
        self.assertEqual(
            by_market[unsupported_id],
            {("unavailable", "none", "strict_cost_adapter_unsupported")},
        )

    def test_phase_b_retained_subset_keeps_missing_core_terminal_rows(self):
        from tests.test_route_cost_evidence import MARKET_ID
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        fixture = _phase_b_one_market_wire_fixture(
            include_missing_supported=True
        )
        capability = _RouteCostCapability(
            fixture["rpc"],
            monotonic_values=[
                100.0, 101.0, 102.0,
                200.0, 201.0, 202.0, 203.0, 204.0,
            ],
            utc_values=[
                "2026-08-01T12:00:02Z", "2026-08-01T12:00:09Z",
            ],
        )
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            trace_path = root / "trace.json"
            _canonical_private_json(trace_path, _trace_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(trace_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._validate_route_cost_universe",
                return_value=fixture["universe"],
            ), patch(
                "scripts.route_cost_collector._normalize_route_cost_cohort",
                return_value=fixture["normalized"],
            ), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value={
                    (MARKET_ID, "dex_pool_state"): fixture["retained"]
                },
            ), patch(
                "scripts.route_cost_collector.load_route_cost_adapter_registry",
                return_value=fixture["registry"],
            ), patch(
                "scripts.route_cost_collector.load_route_cost_connector_key_registry",
                return_value=fixture["keys"],
            ), patch(
                "scripts.route_cost_collector._capture_native_price_evidence",
                return_value=fixture["native_result"],
            ):
                manifest = _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=fixture["universe"], cohort={},
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=fixture["universe_sha"],
                    capability=capability,
                )
        decoded = [
            json.loads(body.decode("utf-8"))
            for body in capability.rpc_requests
        ]
        self.assertEqual([len(batch) for batch in decoded], [11, 10, 10])
        self.assertEqual(manifest["selected_market_count"], 2)
        self.assertEqual(manifest["transcript_count"], 20)
        by_market = {}
        for row in manifest["transcripts"]:
            by_market.setdefault(row["market_id"], set()).add(
                (row["status"], row["completed_stage"], row["reason_code"])
            )
        self.assertEqual(
            by_market[MARKET_ID], {("observed", "transfer_tax", None)}
        )
        self.assertEqual(
            by_market[fixture["missing_supported_id"]],
            {("unavailable", "none", "core_pool_state_unavailable")},
        )

    def test_incomplete_phase_b_estimate_batch_never_releases_trace(self):
        from tests.test_route_cost_evidence import MARKET_ID
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        fixture = _phase_b_one_market_wire_fixture(partial_estimates=True)
        capability = _RouteCostCapability(
            fixture["rpc"],
            monotonic_values=[
                100.0, 101.0, 102.0,
                200.0, 201.0, 202.0,
            ],
            utc_values=["2026-08-01T12:00:02Z"],
        )
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            trace_path = root / "trace.json"
            _canonical_private_json(trace_path, _trace_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(trace_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._validate_route_cost_universe",
                return_value=fixture["universe"],
            ), patch(
                "scripts.route_cost_collector._normalize_route_cost_cohort",
                return_value=fixture["normalized"],
            ), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value={
                    (MARKET_ID, "dex_pool_state"): fixture["retained"]
                },
            ), patch(
                "scripts.route_cost_collector.load_route_cost_adapter_registry",
                return_value=fixture["registry"],
            ), patch(
                "scripts.route_cost_collector.load_route_cost_connector_key_registry",
                return_value=fixture["keys"],
            ), patch(
                "scripts.route_cost_collector._capture_native_price_evidence",
                return_value=fixture["native_result"],
            ), self.assertRaises(RouteCostCollectorError):
                _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=fixture["universe"], cohort={},
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=fixture["universe_sha"],
                    capability=capability,
                )
        decoded = [
            json.loads(body.decode("utf-8"))
            for body in capability.rpc_requests
        ]
        self.assertEqual([len(batch) for batch in decoded], [11, 10])
        self.assertFalse(any(
            row["method"] == "debug_traceCall"
            for batch in decoded for row in batch
        ))

    def test_json_rpc_error_row_is_rpc_unavailable(self):
        requests = [{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}]
        roles = [{"id": 1, "role": "chain_id", "market_id": None}]
        response = route_cost_evidence.canonical_json_bytes([{
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32000, "message": "upstream unavailable"},
        }])
        from scripts.route_cost_collector import _decode_rpc_batch_bytes
        with self.assertRaisesRegex(RouteCostCollectorError, "RPC unavailable"):
            _decode_rpc_batch_bytes(response, requests, roles)

    def test_json_rpc_error_contract_rejects_malformed_rows(self):
        from scripts.route_cost_collector import _decode_rpc_batch_bytes

        invalid_errors = (
            None,
            {"message": "missing code"},
            {"code": True, "message": "boolean code"},
            {"code": "-32000", "message": "text code"},
            {"code": -32000, "message": ""},
            {"code": -32000, "message": 5},
            {"code": -32000, "message": "x", "extra": True},
            {"code": -32000, "message": "x", "data": "x" * (256 * 1024 + 1)},
        )
        for error in invalid_errors:
            with self.subTest(error=repr(error)[:80]), self.assertRaisesRegex(
                RouteCostCollectorError, "response is invalid"
            ):
                _decode_rpc_batch_bytes(
                    route_cost_evidence.canonical_json_bytes([{
                        "jsonrpc": "2.0", "id": 1, "error": error,
                    }]),
                    [{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}],
                    [{"id": 1, "role": "chain_id", "market_id": None}],
                )

        for error in (
            {"code": -32000, "message": "upstream unavailable"},
            {
                "code": 429,
                "message": "rate limited",
                "data": {"retryable": True, "nested": [None, "bounded"]},
            },
        ):
            with self.subTest(valid=error), self.assertRaisesRegex(
                RouteCostCollectorError, "RPC unavailable"
            ):
                _decode_rpc_batch_bytes(
                    route_cost_evidence.canonical_json_bytes([{
                        "jsonrpc": "2.0", "id": 1, "error": error,
                    }]),
                    [{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}],
                    [{"id": 1, "role": "chain_id", "market_id": None}],
                )

    def test_rpc_response_accepts_permuted_unique_exact_id_set(self):
        from scripts.route_cost_collector import _decode_rpc_batch_bytes

        requests = [
            {"jsonrpc": "2.0", "id": identifier, "method": "eth_chainId", "params": []}
            for identifier in (1, 2, 3)
        ]
        roles = [
            {"id": identifier, "role": "chain_id", "market_id": None}
            for identifier in (1, 2, 3)
        ]
        response = route_cost_evidence.canonical_json_bytes([
            {"jsonrpc": "2.0", "id": 3, "result": "0x3"},
            {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
            {"jsonrpc": "2.0", "id": 2, "result": "0x2"},
        ])
        self.assertEqual(
            _decode_rpc_batch_bytes(response, requests, roles),
            [
                {"jsonrpc": "2.0", "id": 3, "result": "0x3"},
                {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
                {"jsonrpc": "2.0", "id": 2, "result": "0x2"},
            ],
        )

    def test_rpc_decoder_sanitizes_real_block_and_fee_history_extra_fields(self):
        from scripts.route_cost_collector import _decode_rpc_batch_bytes

        requests = [
            {
                "jsonrpc": "2.0", "id": 2,
                "method": "eth_getBlockByNumber", "params": ["0x64", False],
            },
            {
                "jsonrpc": "2.0", "id": 3,
                "method": "eth_feeHistory", "params": ["0x1", "0x64", [50]],
            },
        ]
        roles = [
            {"id": 2, "role": "block_header", "market_id": None},
            {"id": 3, "role": "fee_history", "market_id": None},
        ]
        response = route_cost_evidence.canonical_json_bytes([
            {
                "jsonrpc": "2.0", "id": 3, "result": {
                    "oldestBlock": "0x64",
                    "baseFeePerGas": ["0x64", "0x64"],
                    "reward": [["0x3"]],
                    "gasUsedRatio": [0.5],
                    "blobGasUsedRatio": [0],
                },
            },
            {
                "jsonrpc": "2.0", "id": 2, "result": {
                    "number": "0x64", "hash": "0x" + "7" * 64,
                    "parentHash": "0x" + "a" * 64,
                    "timestamp": "0x1", "baseFeePerGas": "0x64",
                    "gasUsed": "0x1", "gasLimit": "0x2",
                    "transactions": [], "withdrawals": [], "difficulty": "0x0",
                },
            },
        ])
        rows = _decode_rpc_batch_bytes(response, requests, roles)
        by_id = {row["id"]: row["result"] for row in rows}
        self.assertEqual(set(by_id[2]), {
            "number", "hash", "parentHash", "timestamp", "baseFeePerGas",
            "gasUsed", "gasLimit",
        })
        self.assertEqual(set(by_id[3]), {
            "oldestBlock", "baseFeePerGas", "reward", "gasUsedRatio",
        })
        self.assertEqual(by_id[3]["gasUsedRatio"], [0.5])

    def test_rpc_decoder_preserves_exact_fee_ratio_until_pure_projection(self):
        from decimal import Decimal
        from scripts.route_cost_collector import _decode_rpc_batch_bytes

        requests = [{
            "jsonrpc": "2.0", "id": 3, "method": "eth_feeHistory",
            "params": ["0x1", "0x64", [50]],
        }]
        roles = [{"id": 3, "role": "fee_history", "market_id": None}]
        response = (
            b'[{"id":3,"jsonrpc":"2.0","result":'
            b'{"baseFeePerGas":["0x64","0x65"],'
            b'"gasUsedRatio":[0.12345678901234567],'
            b'"oldestBlock":"0x64","reward":[["0x3"]]}}]'
        )
        rows = _decode_rpc_batch_bytes(response, requests, roles)
        self.assertEqual(
            rows[0]["result"]["gasUsedRatio"],
            [Decimal("0.12345678901234567")],
        )

    def test_phase_a_batches_share_one_ten_second_monotonic_budget(self):
        from scripts.route_cost_collector import _phase_a_terminal_reasons
        from tests.test_route_cost_evidence import (
            MARKET_ID,
            adapter,
            adapter_registry,
            context,
            retained_v2_pool_state,
            universe_for,
        )

        # Force 41 requests through the private scheduler without involving
        # universe ranking: 3 chain calls + 5 retained markets * 8 identity.
        retained = {}
        pairs = []
        funding = []
        from tests.test_route_cost_evidence import (
            funding_descriptor, pair_descriptor,
        )
        for index in range(5):
            pair = "0x{:040x}".format(0x400 + index)
            token0 = "0x{:040x}".format(0x500 + index)
            token1 = "0x{:040x}".format(0x600 + index)
            market_id = "dex:eth:uniswap_v2:{}:T{}".format(pair, index)
            pairs.append(pair_descriptor(pair, token0=token0, token1=token1))
            funding.extend((funding_descriptor(token0), funding_descriptor(token1)))
            retained[market_id] = retained_v2_pool_state(
                market_id=market_id,
                block_number=20_000_000,
                pool_address=pair,
                token0=token0,
                token1=token1,
            )
        registry = adapter_registry(supported=True)
        registry["adapters"] = [adapter(pairs=sorted(
            pairs, key=lambda row: row["pair_address"]
        ))]
        registry["adapters"][0]["token_funding_descriptors"] = sorted(
            funding, key=lambda row: row["token_address"]
        )
        universe = _phase_a_multimarket_universe(retained)
        def batches(request_bytes):
            requests = json.loads(request_bytes.decode("utf-8"))
            if requests[0]["id"] == 1:
                plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
                    universe=universe, adapter_registry=registry,
                    retained_typed_pool_state_members=retained,
                )
                from tests.test_route_cost_evidence import phase_a_rpc_responses
                return route_cost_evidence.canonical_json_bytes([
                    row for row in phase_a_rpc_responses(plan)
                    if row["id"] <= 40
                ])
            raise TimeoutError("second batch")

        capability = _RouteCostCapability(
            batches, monotonic_values=[100.0, 103.0, 107.5]
        )
        profiles = load_route_cost_profile_capture()
        profiles = type(profiles)(
            trace_profile=_trace_profile(),
            connector_profile=None,
            trace_profile_identity=route_cost_evidence.trace_profile_identity(
                _trace_profile()
            )[0],
            trace_profile_generation=route_cost_evidence.trace_profile_identity(
                _trace_profile()
            )[1],
            submission_connector_profile_identity=(
                profiles.submission_connector_profile_identity
            ),
            submission_connector_profile_generation=(
                profiles.submission_connector_profile_generation
            ),
        )
        reasons = _phase_a_terminal_reasons(
            **_phase_a_lineage(universe),
            supported=tuple(sorted(retained)),
            retained_pool_members=retained,
            adapter_registry=registry,
            profiles=profiles,
            capability=capability,
        )
        self.assertEqual(capability.rpc_timeouts, [7.0, 2.5])
        self.assertEqual(set(reasons.values()), {"rpc_unavailable"})

    def test_phase_a_expired_or_reversing_monotonic_budget_fails_without_next_batch(self):
        from scripts.route_cost_collector import _phase_a_terminal_reasons
        from tests.test_route_cost_evidence import (
            adapter, adapter_registry, funding_descriptor, pair_descriptor,
            retained_v2_pool_state,
        )

        retained = {}
        pairs = []
        funding = []
        for index in range(5):
            pair = "0x{:040x}".format(0x700 + index)
            token0 = "0x{:040x}".format(0x800 + index)
            token1 = "0x{:040x}".format(0x900 + index)
            market_id = "dex:eth:uniswap_v2:{}:U{}".format(pair, index)
            pairs.append(pair_descriptor(pair, token0=token0, token1=token1))
            funding.extend((funding_descriptor(token0), funding_descriptor(token1)))
            retained[market_id] = retained_v2_pool_state(
                market_id=market_id, block_number=20_000_000,
                pool_address=pair, token0=token0, token1=token1,
            )
        registry = adapter_registry(supported=True)
        registry["adapters"] = [adapter(pairs=sorted(
            pairs, key=lambda row: row["pair_address"]
        ))]
        registry["adapters"][0]["token_funding_descriptors"] = sorted(
            funding, key=lambda row: row["token_address"]
        )
        universe = _phase_a_multimarket_universe(retained)
        base = load_route_cost_profile_capture()
        trace_identity, trace_generation = route_cost_evidence.trace_profile_identity(
            _trace_profile()
        )
        profiles = type(base)(
            trace_profile=_trace_profile(), connector_profile=None,
            trace_profile_identity=trace_identity,
            trace_profile_generation=trace_generation,
            submission_connector_profile_identity=base.submission_connector_profile_identity,
            submission_connector_profile_generation=base.submission_connector_profile_generation,
        )

        cases = (
            ("expired", [100.0, 101.0, 111.0], False),
            ("reversed", [100.0, 101.0, 99.0], True),
        )
        for label, samples, raises in cases:
            calls = 0

            def batches(request_bytes):
                nonlocal calls
                calls += 1
                requests = json.loads(request_bytes.decode("utf-8"))
                plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
                    universe=universe, adapter_registry=registry,
                    retained_typed_pool_state_members=retained,
                )
                from tests.test_route_cost_evidence import phase_a_rpc_responses
                wanted = {row["id"] for row in requests}
                return route_cost_evidence.canonical_json_bytes([
                    row for row in phase_a_rpc_responses(plan)
                    if row["id"] in wanted
                ])

            capability = _RouteCostCapability(batches, monotonic_values=samples)
            if raises:
                with self.subTest(case=label), self.assertRaises(
                    RouteCostCollectorError
                ):
                    _phase_a_terminal_reasons(
                        **_phase_a_lineage(universe),
                        supported=tuple(sorted(retained)),
                        retained_pool_members=retained,
                        adapter_registry=registry,
                        profiles=profiles,
                        capability=capability,
                    )
            else:
                reasons = _phase_a_terminal_reasons(
                    **_phase_a_lineage(universe),
                    supported=tuple(sorted(retained)),
                    retained_pool_members=retained,
                    adapter_registry=registry,
                    profiles=profiles,
                    capability=capability,
                )
                self.assertEqual(set(reasons.values()), {"rpc_unavailable"})
            self.assertEqual(calls, 1)

    def test_phase_a_single_batch_response_after_deadline_is_unavailable(self):
        from scripts.route_cost_collector import _phase_a_terminal_reasons
        from tests.test_route_cost_evidence import (
            MARKET_ID, adapter_registry, phase_a_rpc_responses,
            retained_v2_pool_state,
        )

        retained = {MARKET_ID: retained_v2_pool_state(block_number=20_000_000)}
        universe = _phase_a_multimarket_universe(retained)
        registry = adapter_registry(supported=True)
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=universe,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )
        base = load_route_cost_profile_capture()
        trace_identity, trace_generation = route_cost_evidence.trace_profile_identity(
            _trace_profile()
        )
        profiles = type(base)(
            trace_profile=_trace_profile(), connector_profile=None,
            trace_profile_identity=trace_identity,
            trace_profile_generation=trace_generation,
            submission_connector_profile_identity=(
                base.submission_connector_profile_identity
            ),
            submission_connector_profile_generation=(
                base.submission_connector_profile_generation
            ),
        )
        capability = _RouteCostCapability(
            lambda _request: route_cost_evidence.canonical_json_bytes(
                phase_a_rpc_responses(plan)
            ),
            monotonic_values=[100.0, 101.0, 111.0],
        )
        with patch(
            "scripts.route_cost_collector.project_fixed_block_phase_a_capture"
        ) as projector:
            reasons = _phase_a_terminal_reasons(
                **_phase_a_lineage(universe),
                supported=(MARKET_ID,),
                retained_pool_members=retained,
                adapter_registry=registry,
                profiles=profiles,
                capability=capability,
            )
        self.assertEqual(reasons, {MARKET_ID: "rpc_unavailable"})
        self.assertEqual(capability.rpc_timeouts, [9.0])
        projector.assert_not_called()

    def test_phase_a_monotonic_rejects_non_exact_or_nonfinite_samples(self):
        from scripts.route_cost_collector import _phase_a_terminal_reasons
        from tests.test_route_cost_evidence import (
            MARKET_ID, adapter_registry, retained_v2_pool_state,
        )

        retained = {MARKET_ID: retained_v2_pool_state(block_number=20_000_000)}
        universe = _phase_a_multimarket_universe(retained)
        base = load_route_cost_profile_capture()
        trace_identity, trace_generation = route_cost_evidence.trace_profile_identity(
            _trace_profile()
        )
        profiles = type(base)(
            trace_profile=_trace_profile(), connector_profile=None,
            trace_profile_identity=trace_identity,
            trace_profile_generation=trace_generation,
            submission_connector_profile_identity=base.submission_connector_profile_identity,
            submission_connector_profile_generation=base.submission_connector_profile_generation,
        )
        for sample in (True, "1", float("nan"), float("inf"), float("-inf")):
            capability = _RouteCostCapability(
                lambda _request: self.fail("invalid clock attempted RPC"),
                monotonic_values=[sample],
            )
            with self.subTest(sample=repr(sample)), self.assertRaisesRegex(
                RouteCostCollectorError, "monotonic capability is invalid"
            ):
                _phase_a_terminal_reasons(
                    **_phase_a_lineage(universe),
                    supported=(MARKET_ID,),
                    retained_pool_members=retained,
                    adapter_registry=adapter_registry(supported=True),
                    profiles=profiles,
                    capability=capability,
                )
            self.assertEqual(capability.rpc_requests, [])

    def test_production_rpc_opener_receives_remaining_timeout(self):
        from scripts.route_cost_collector import _production_rpc_batch

        body = route_cost_evidence.canonical_json_bytes([{
            "jsonrpc": "2.0", "id": 1, "result": "0x1",
        }])

        class ContextResponse(_Response):
            def __init__(self, current):
                super().__init__(current)
                self.socket = type(
                    "SocketSpy", (), {"settimeout": lambda _self, _value: None}
                )()
                self.fp = type("Buffered", (), {
                    "raw": type("Raw", (), {"_sock": self.socket})(),
                })()

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        response = ContextResponse(body)
        calls = []

        class Opener:
            addheaders = [("User-Agent", "must-be-cleared")]

            def open(self, request, *, timeout):
                calls.append((request, timeout, list(self.addheaders)))
                return response

        opener = Opener()
        with patch(
            "scripts.route_cost_collector.urllib.request.build_opener",
            return_value=opener,
        ):
            self.assertEqual(
                _production_rpc_batch(_trace_profile(), b"[]", 2.5), body
            )

        self.assertEqual(len(calls), 1)
        request, timeout, addheaders = calls[0]
        self.assertEqual(timeout, 2.5)
        self.assertEqual(addheaders, [])
        self.assertEqual(request.full_url, _trace_profile()["rpc_url"])
        self.assertEqual(request.data, b"[]")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(
            {key.lower(): value for key, value in request.header_items()},
            {
                "content-type": "application/json",
                "accept": "application/json",
                "authorization": "Bearer TRACE-SECRET",
            },
        )

    def test_production_rpc_accepts_canonical_fee_history_fraction(self):
        from scripts.route_cost_collector import _production_rpc_batch

        body = route_cost_evidence.canonical_json_bytes([{
            "jsonrpc": "2.0",
            "id": 3,
            "result": {
                "oldestBlock": "0x64",
                "baseFeePerGas": ["0x64", "0x65"],
                "reward": [["0x3"]],
                "gasUsedRatio": [0.5],
            },
        }])

        class ContextResponse(_Response):
            def __init__(self, current):
                super().__init__(current)
                self.socket = type(
                    "SocketSpy", (), {"settimeout": lambda _self, _value: None}
                )()
                self.fp = type("Buffered", (), {
                    "raw": type("Raw", (), {"_sock": self.socket})(),
                })()

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        class Opener:
            addheaders = []

            def open(self, _request, *, timeout):
                self.timeout = timeout
                return ContextResponse(body)

        opener = Opener()
        with patch(
            "scripts.route_cost_collector.urllib.request.build_opener",
            return_value=opener,
        ):
            self.assertEqual(
                _production_rpc_batch(_trace_profile(), b"[]", 2.5), body
            )
        self.assertEqual(opener.timeout, 2.5)

    def test_production_rpc_preserves_exact_legal_fee_history_decimals(self):
        from scripts.route_cost_collector import _production_rpc_batch

        for token in (b"0.12345678901234567", b"0.000001"):
            body = (
                b'[{"id":3,"jsonrpc":"2.0","result":'
                b'{"baseFeePerGas":["0x64","0x65"],'
                b'"gasUsedRatio":[' + token + b'],"oldestBlock":"0x64",'
                b'"reward":[["0x3"]]}}]'
            )

            class ContextResponse(_Response):
                def __init__(self, current):
                    super().__init__(current)
                    self.socket = type(
                        "SocketSpy", (), {"settimeout": lambda _self, _value: None}
                    )()
                    self.fp = type("Buffered", (), {
                        "raw": type("Raw", (), {"_sock": self.socket})(),
                    })()

                def read1(self, maximum):
                    return super().read(maximum)

            class Opener:
                addheaders = []

                def open(self, _request, *, timeout):
                    self.timeout = timeout
                    return ContextResponse(body)

            opener = Opener()
            with self.subTest(token=token), patch(
                "scripts.route_cost_collector.urllib.request.build_opener",
                return_value=opener,
            ):
                self.assertEqual(
                    _production_rpc_batch(
                        _trace_profile(), b"[]", 2.5
                    ),
                    body,
                )

    def test_any_retained_anchor_field_mismatch_is_fixed_block_mismatch_without_rpc(self):
        from scripts.route_cost_collector import _phase_a_terminal_reasons
        from tests.test_route_cost_evidence import (
            adapter, adapter_registry, pair_descriptor,
            retained_v2_pool_state,
        )

        market_a = "dex:eth:uniswap_v2:0x0000000000000000000000000000000000000a01:A"
        market_b = "dex:eth:uniswap_v2:0x0000000000000000000000000000000000000b01:B"
        token_a0 = "0x0000000000000000000000000000000000000a02"
        token_a1 = "0x0000000000000000000000000000000000000a03"
        token_b0 = "0x0000000000000000000000000000000000000b02"
        token_b1 = "0x0000000000000000000000000000000000000b03"
        registry = adapter_registry(supported=True)
        registry["adapters"] = [adapter(pairs=[
            pair_descriptor(
                market_a.split(":")[3], token0=token_a0, token1=token_a1
            ),
            pair_descriptor(
                market_b.split(":")[3], token0=token_b0, token1=token_b1
            ),
        ])]
        from tests.test_route_cost_evidence import funding_descriptor
        registry["adapters"][0]["token_funding_descriptors"] = sorted(
            [
                funding_descriptor(token_a0), funding_descriptor(token_a1),
                funding_descriptor(token_b0), funding_descriptor(token_b1),
            ],
            key=lambda row: row["token_address"],
        )
        retained = {
            market_a: retained_v2_pool_state(
                market_id=market_a, block_number=20_000_000,
                pool_address=market_a.split(":")[3], token0=token_a0,
                token1=token_a1,
            ),
            market_b: retained_v2_pool_state(
                market_id=market_b, block_number=20_000_000,
                pool_address=market_b.split(":")[3], token0=token_b0,
                token1=token_b1,
            ),
        }
        universe = _phase_a_multimarket_universe(retained)
        base = load_route_cost_profile_capture()
        trace_identity, trace_generation = route_cost_evidence.trace_profile_identity(
            _trace_profile()
        )
        profiles = type(base)(
            trace_profile=_trace_profile(), connector_profile=None,
            trace_profile_identity=trace_identity,
            trace_profile_generation=trace_generation,
            submission_connector_profile_identity=base.submission_connector_profile_identity,
            submission_connector_profile_generation=base.submission_connector_profile_generation,
        )
        mismatches = ("chain_id", "block_number", "block_timestamp")
        for field in mismatches:
            changed = copy.deepcopy(retained)
            if field == "chain_id":
                # A different chain is invalid typed source, not an anchor-only
                # mismatch, and must therefore hard-fail rather than classify.
                changed_payload = json.loads(changed[market_b]["payload"])
                changed_payload["chain_id"] = "2"
                changed[market_b]["payload"] = route_cost_evidence.canonical_json_bytes(
                    changed_payload
                )
                changed[market_b]["descriptor"]["sha256"] = hashlib.sha256(
                    changed[market_b]["payload"]
                ).hexdigest()
                changed[market_b]["descriptor"]["size"] = len(
                    changed[market_b]["payload"]
                )
            else:
                changed[market_b] = retained_v2_pool_state(
                    market_id=market_b,
                    pool_address=market_b.split(":")[3],
                    token0=token_b0, token1=token_b1,
                    **(
                        {"block_number": 20_000_001}
                        if field == "block_number" else
                        {"block_timestamp": 1785585601}
                    ),
                )
            capability = _RouteCostCapability(
                lambda _request: self.fail("anchor mismatch attempted RPC")
            )
            call = lambda: _phase_a_terminal_reasons(
                **_phase_a_lineage(universe),
                supported=(market_a, market_b),
                retained_pool_members=changed,
                adapter_registry=registry,
                profiles=profiles,
                capability=capability,
            )
            if field == "chain_id":
                with self.subTest(field=field), self.assertRaises(
                    route_cost_evidence.RouteCostEvidenceError
                ):
                    call()
            else:
                with self.subTest(field=field):
                    reasons = call()
                self.assertEqual(
                    reasons,
                    {market_a: "fixed_block_mismatch", market_b: "fixed_block_mismatch"},
                )
            self.assertEqual(capability.rpc_requests, [])

    def test_eight_retained_markets_use_canonical_40_27_batches_then_close_all_rows(self):
        from tests.test_route_cost_evidence import (
            adapter,
            context,
            funding_descriptor,
            pair_descriptor,
            retained_v2_pool_state,
            universe_for,
        )
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        markets = []
        routes = []
        pairs = []
        funding = []
        typed = {}
        for index in range(1, 9):
            pair = "0x{:040x}".format(0x300 + index)
            token0 = "0x{:040x}".format(0x100 + index)
            token1 = "0x{:040x}".format(0x200 + index)
            symbol = "T{:02d}".format(index)
            market_id = "dex:eth:uniswap_v2:{}:{}".format(pair, symbol)
            leg = copy.deepcopy(universe_for()["selected_legs"][0])
            leg.update({
                "market_id": market_id,
                "token_symbol": symbol,
                "selection_rank": index,
                "collector_context": context(token0, token1),
                "target_token_address": token0,
            })
            leg["selection_inputs"] = copy.deepcopy(leg["selection_inputs"])
            leg["selection_inputs"]["dex_24h_usd"] = str(1000 - index)
            markets.append(leg)
            routes.append({
                **copy.deepcopy(universe_for()["routes"][0]),
                "route_id": "route:{}:{}->cex:x:{}/USDT:prepositioned_inventory".format(
                    symbol, market_id, symbol
                ),
                "token_symbol": symbol,
                "buy_market_id": market_id,
                "sell_market_id": "cex:x:{}/USDT".format(symbol),
            })
            pairs.append(pair_descriptor(pair, token0=token0, token1=token1))
            funding.extend((funding_descriptor(token0), funding_descriptor(token1)))
            typed[(market_id, "dex_pool_state")] = retained_v2_pool_state(
                market_id=market_id,
                block_number=20_000_000,
                pool_address=pair,
                token0=token0,
                token1=token1,
            )
        universe = universe_for(markets=markets, routes=routes)
        registry = {
            "schema": "route_cost_adapter_registry/v1",
            "registry_version": "eight-market-test-v1",
            "adapters": [adapter(pairs=sorted(
                pairs, key=lambda row: row["pair_address"]
            ))],
        }
        registry["adapters"][0]["token_funding_descriptors"] = sorted(
            funding, key=lambda row: row["token_address"]
        )
        normalized = {
            "route_cohort_id": _COLLECTOR_COHORT_ID,
            "raw_evidence_run_id": "collector-raw-run",
            "candidate_source_generation": universe["candidate_source_generation"],
            "legs": [_collector_core_leg(row) for row in markets],
            "routes": copy.deepcopy(routes),
        }
        call_count = 0

        def batches(request_bytes):
            nonlocal call_count
            call_count += 1
            requests = json.loads(request_bytes.decode("utf-8"))
            if call_count == 1:
                plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
                    universe=universe,
                    adapter_registry=registry,
                    retained_typed_pool_state_members={
                        market_id: typed[(market_id, "dex_pool_state")]
                        for market_id in sorted(
                            item[0] for item in typed
                            if item[1] == "dex_pool_state"
                        )
                    },
                )
                from tests.test_route_cost_evidence import phase_a_rpc_responses
                return route_cost_evidence.canonical_json_bytes([
                    row for row in phase_a_rpc_responses(plan)
                    if row["id"] in {request["id"] for request in requests}
                ])
            raise TimeoutError("secret batch two")

        capability = _RouteCostCapability(batches)
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            trace_path = root / "trace.json"
            _canonical_private_json(trace_path, _trace_profile())
            with patch.dict(os.environ, {
                TRACE_PROFILE_ENV: str(trace_path),
            }, clear=True), patch(
                "scripts.route_cost_collector._validate_route_cost_universe",
                return_value=universe,
            ), patch(
                "scripts.route_cost_collector._normalize_route_cost_cohort",
                return_value=normalized,
            ), patch(
                "scripts.route_cost_collector._load_retained_route_cost_typed_members",
                return_value=typed,
            ), patch(
                "scripts.route_cost_collector.load_route_cost_adapter_registry",
                return_value=registry,
            ):
                manifest = _collect_route_cost_evidence_manifest_with_capability(
                    root, universe=universe, cohort={},
                    run_id="collector-run", phase="canary",
                    route_universe_sha256=hashlib.sha256(
                        route_cost_evidence.canonical_json_bytes(universe)
                    ).hexdigest(),
                    capability=capability,
                )

        decoded_batches = [
            json.loads(value.decode("utf-8"))
            for value in capability.rpc_requests
        ]
        self.assertEqual([len(value) for value in decoded_batches], [40, 27])
        self.assertEqual(
            [row["id"] for value in decoded_batches for row in value],
            list(range(1, 68)),
        )
        self.assertEqual(manifest["selected_market_count"], 8)
        self.assertEqual(manifest["transcript_count"], 80)
        self.assertEqual(
            {(row["status"], row["reason_code"])
             for row in manifest["transcripts"]},
            {("unavailable", "rpc_unavailable")},
        )

    def test_batch_two_failure_discards_first_batch_success_rows(self):
        from scripts.route_cost_collector import _phase_a_terminal_reasons
        from tests.test_route_cost_evidence import (
            adapter, adapter_registry, funding_descriptor, pair_descriptor,
            phase_a_rpc_responses, retained_v2_pool_state,
        )

        retained = {}
        pairs = []
        funding = []
        for index in range(5):
            pair = "0x{:040x}".format(0xA00 + index)
            token0 = "0x{:040x}".format(0xB00 + index)
            token1 = "0x{:040x}".format(0xC00 + index)
            market_id = "dex:eth:uniswap_v2:{}:P{}".format(pair, index)
            pairs.append(pair_descriptor(pair, token0=token0, token1=token1))
            funding.extend((funding_descriptor(token0), funding_descriptor(token1)))
            retained[market_id] = retained_v2_pool_state(
                market_id=market_id, block_number=20_000_000,
                pool_address=pair, token0=token0, token1=token1,
            )
        registry = adapter_registry(supported=True)
        registry["adapters"] = [adapter(pairs=sorted(
            pairs, key=lambda row: row["pair_address"]
        ))]
        registry["adapters"][0]["token_funding_descriptors"] = sorted(
            funding, key=lambda row: row["token_address"]
        )
        universe = _phase_a_multimarket_universe(retained)
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=universe, adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )
        attempts = 0

        def batches(request_bytes):
            nonlocal attempts
            attempts += 1
            requests = json.loads(request_bytes.decode("utf-8"))
            if attempts == 2:
                raise TimeoutError("batch two")
            wanted = {row["id"] for row in requests}
            return route_cost_evidence.canonical_json_bytes([
                row for row in phase_a_rpc_responses(plan) if row["id"] in wanted
            ])

        base = load_route_cost_profile_capture()
        trace_identity, trace_generation = route_cost_evidence.trace_profile_identity(
            _trace_profile()
        )
        profiles = type(base)(
            trace_profile=_trace_profile(), connector_profile=None,
            trace_profile_identity=trace_identity,
            trace_profile_generation=trace_generation,
            submission_connector_profile_identity=base.submission_connector_profile_identity,
            submission_connector_profile_generation=base.submission_connector_profile_generation,
        )
        with patch(
            "scripts.route_cost_collector.project_fixed_block_phase_a_capture"
        ) as projector:
            reasons = _phase_a_terminal_reasons(
                **_phase_a_lineage(universe),
                supported=tuple(sorted(retained)),
                retained_pool_members=retained,
                adapter_registry=registry,
                profiles=profiles,
                capability=_RouteCostCapability(
                    batches, monotonic_values=[100.0, 101.0, 102.0]
                ),
            )
        self.assertEqual(attempts, 2)
        self.assertEqual(set(reasons.values()), {"rpc_unavailable"})
        projector.assert_not_called()

    def test_lineage_is_derived_and_capability_is_sampled_once(self):
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = _collector_universe()
        cohort = _collector_cohort(universe)
        calls = 0

        def sample():
            nonlocal calls
            calls += 1
            if calls > 1:
                raise AssertionError("time capability sampled more than once")
            return "2026-08-01T12:00:04Z"

        with tempfile.TemporaryDirectory() as name, patch.dict(
            os.environ, {}, clear=True
        ), patch(
            "scripts.route_cost_collector._load_retained_route_cost_typed_members",
            return_value={},
        ):
            manifest = _collect_route_cost_evidence_manifest_with_capability(
                Path(name), universe=universe, cohort=cohort,
                run_id="collector-run", phase="canary",
                route_universe_sha256=hashlib.sha256(
                    route_cost_evidence.canonical_json_bytes(universe)
                ).hexdigest(),
                capability=sample,
            )
        self.assertEqual(calls, 1)
        self.assertEqual(
            manifest["candidate_source_generation"],
            universe["candidate_source_generation"],
        )
        forged = copy.deepcopy(cohort)
        forged["candidate_source_generation"] = "f" * 64
        forged["source_state"]["candidate_source_generation"] = "f" * 64
        with patch(
            "scripts.route_cost_collector._normalize_route_cost_cohort",
            return_value=forged,
        ), self.assertRaisesRegex(RouteCostCollectorError, "outer lineage"):
            _collect_route_cost_evidence_manifest_with_capability(
                Path("/tmp"), universe=universe, cohort=forged,
                run_id="collector-run", phase="canary",
                route_universe_sha256=hashlib.sha256(
                    route_cost_evidence.canonical_json_bytes(universe)
                ).hexdigest(),
                capability=sample,
            )

    def test_universe_schema_and_cross_cohort_route_inventory_are_not_caller_controlled(self):
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = _collector_universe()
        cohort = _collector_cohort(universe)
        invalid_universe = copy.deepcopy(universe)
        invalid_universe["caller_extra"] = "influence"
        with tempfile.TemporaryDirectory() as name, patch.dict(
            os.environ, {}, clear=True
        ), self.assertRaisesRegex(RouteCostCollectorError, "universe"):
            _collect_route_cost_evidence_manifest_with_capability(
                Path(name), universe=invalid_universe, cohort=cohort,
                run_id="collector-run", phase="canary",
                route_universe_sha256=hashlib.sha256(
                    route_cost_evidence.canonical_json_bytes(invalid_universe)
                ).hexdigest(),
                capability=lambda: "2026-08-01T12:00:04Z",
            )

        normalized = copy.deepcopy(cohort)
        normalized["routes"] = []
        with tempfile.TemporaryDirectory() as name, patch(
            "scripts.route_cost_collector._normalize_route_cost_cohort",
            return_value=normalized,
        ), patch(
            "scripts.route_cost_collector._load_retained_route_cost_typed_members",
            return_value={},
        ), self.assertRaisesRegex(RouteCostCollectorError, "inventory"):
            _collect_route_cost_evidence_manifest_with_capability(
                Path(name), universe=universe, cohort=cohort,
                run_id="collector-run", phase="canary",
                route_universe_sha256=hashlib.sha256(
                    route_cost_evidence.canonical_json_bytes(universe)
                ).hexdigest(),
                capability=lambda: "2026-08-01T12:00:04Z",
            )

    def test_same_identity_cohort_value_drift_rejects_before_private_work(self):
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = _collector_universe()
        drifted_universe = copy.deepcopy(universe)
        for leg in drifted_universe["selected_legs"]:
            leg["selection_inputs"]["cex_selected_window_usd"] = "3000"
        route = drifted_universe["routes"][0]
        route["buy_reference_volume_usd"] = "3000"
        route["sell_reference_volume_usd"] = "3000"
        route["route_volume_usd"] = "3000"
        drifted_cohort = _collector_cohort(drifted_universe)
        private_calls = []

        def private_work(*_args, **_kwargs):
            private_calls.append(True)
            raise AssertionError("lineage drift reached private work")

        with tempfile.TemporaryDirectory() as name, patch(
            "scripts.route_cost_collector.load_route_cost_profile_capture",
            side_effect=private_work,
        ), patch(
            "scripts.route_cost_collector._load_retained_route_cost_typed_members",
            side_effect=private_work,
        ), self.assertRaisesRegex(RouteCostCollectorError, "inventory"):
            _collect_route_cost_evidence_manifest_with_capability(
                Path(name), universe=universe, cohort=drifted_cohort,
                run_id="collector-run", phase="canary",
                route_universe_sha256=hashlib.sha256(
                    route_cost_evidence.canonical_json_bytes(universe)
                ).hexdigest(),
                capability=private_work,
            )
        self.assertEqual(private_calls, [])

    def test_same_identity_dex_context_drift_rejects_before_private_work(self):
        from tests.test_route_cost_evidence import (
            MARKET_ID,
            adapter_registry,
            universe_for,
        )
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = universe_for()
        cohort_leg = {
            "market_id": MARKET_ID,
            "market_type": "dex",
            "token_symbol": "AAA",
            "collector_context": copy.deepcopy(
                universe["selected_legs"][0]["collector_context"]
            ),
        }
        cohort_leg["collector_context"]["snapshot_id"] = "cross-wired"
        normalized = {
            "route_cohort_id": _COLLECTOR_COHORT_ID,
            "raw_evidence_run_id": "collector-raw-run",
            "candidate_source_generation": universe[
                "candidate_source_generation"
            ],
            "legs": [cohort_leg],
            "routes": copy.deepcopy(universe["routes"]),
        }
        private_calls = []

        def private_work(*_args, **_kwargs):
            private_calls.append(True)
            raise AssertionError("DEX context drift reached private work")

        with tempfile.TemporaryDirectory() as name, patch(
            "scripts.route_cost_collector._validate_route_cost_universe",
            return_value=universe,
        ), patch(
            "scripts.route_cost_collector._normalize_route_cost_cohort",
            return_value=normalized,
        ), patch(
            "scripts.route_cost_collector.load_route_cost_adapter_registry",
            return_value=adapter_registry(supported=True),
        ), patch(
            "scripts.route_cost_collector.load_route_cost_profile_capture",
            side_effect=private_work,
        ), patch(
            "scripts.route_cost_collector._load_retained_route_cost_typed_members",
            side_effect=private_work,
        ), self.assertRaisesRegex(RouteCostCollectorError, "inventory"):
            _collect_route_cost_evidence_manifest_with_capability(
                Path(name), universe=universe, cohort={},
                run_id="collector-run", phase="canary",
                route_universe_sha256=hashlib.sha256(
                    route_cost_evidence.canonical_json_bytes(universe)
                ).hexdigest(),
                capability=private_work,
            )
        self.assertEqual(private_calls, [])

    def test_supported_missing_trace_keeps_full_rows_and_retained_pool_state(self):
        from tests.test_route_cost_evidence import (
            MARKET_ID,
            adapter_registry,
            retained_v2_pool_state,
            universe_for,
        )
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )

        universe = universe_for()
        cohort = _collector_cohort(_collector_universe())
        cohort["candidate_source_generation"] = universe[
            "candidate_source_generation"
        ]
        cohort["source_state"]["candidate_source_generation"] = universe[
            "candidate_source_generation"
        ]
        # The canonical cohort validator is exercised separately.  This test
        # isolates the supported terminal branch with one already-normalized
        # cohort and descriptor-reread typed inventory.
        retained = retained_v2_pool_state()
        typed = {(MARKET_ID, "dex_pool_state"): retained}
        evaluated_at = "2026-08-01T12:00:04Z"
        with tempfile.TemporaryDirectory() as name, patch.dict(
            os.environ, {}, clear=True
        ), patch(
            "scripts.route_cost_collector._validate_route_cost_universe",
            return_value=universe,
        ), patch(
            "scripts.route_cost_collector._normalize_route_cost_cohort",
            return_value={
                **cohort,
                "route_cohort_id": _COLLECTOR_COHORT_ID,
                "raw_evidence_run_id": "collector-raw-run",
                "candidate_source_generation": universe[
                    "candidate_source_generation"
                ],
                "legs": [_collector_core_leg(universe["selected_legs"][0])],
                "routes": copy.deepcopy(universe["routes"]),
            },
        ), patch(
            "scripts.route_cost_collector._load_retained_route_cost_typed_members",
            return_value=typed,
        ) as retained_loader, patch(
            "scripts.route_cost_collector.load_route_cost_adapter_registry",
            return_value=adapter_registry(supported=True),
        ), patch(
            "scripts.route_cost_collector.urllib.request.urlopen",
            side_effect=AssertionError("missing trace path performed network I/O"),
        ):
            manifest = _collect_route_cost_evidence_manifest_with_capability(
                Path(name), universe=universe, cohort=cohort,
                run_id="collector-run", phase="canary",
                route_universe_sha256=hashlib.sha256(
                    route_cost_evidence.canonical_json_bytes(universe)
                ).hexdigest(),
                capability=lambda: evaluated_at,
            )

        self.assertEqual(manifest["transcript_count"], 10)
        retained_loader.assert_called_once_with(
            Path(name), unittest.mock.ANY,
            retain_keys=frozenset({(MARKET_ID, "dex_pool_state")}),
        )
        self.assertEqual(
            {(row["reason_code"], row["core_pool_state_sha256"])
             for row in manifest["transcripts"]},
            {("trace_profile_missing", retained["descriptor"]["sha256"])},
        )
        self.assertEqual(manifest["submission_policy_snapshot"]["member_count"], 5)
        self.assertEqual(
            manifest["submission_policy_snapshot"]["reason_code"],
            "submission_connector_missing",
        )
        self.assertEqual(len(manifest["bindings"]), 5)


class PhaseBPrestateTraceBatchDecoderTests(unittest.TestCase):
    ZERO = "0x" + "0" * 64

    def setUp(self):
        from tests.test_route_cost_evidence import (
            MARKET_ID, POOL, ROUTER, SENDER, TOKEN_A, TOKEN_B, adapter,
        )
        self.market_id, self.pair = MARKET_ID, POOL
        self.router, self.sender = ROUTER, SENDER
        self.token_in, self.token_out = TOKEN_A, TOKEN_B
        self.adapter = adapter()
        self.calldata = route_cost_evidence.build_v2_swap_calldata(
            direction="sell", quoted_amount_in_raw=100,
            quoted_amount_out_raw=50, submission_loss_bound_bps=0,
            path_token_in=self.token_in, path_token_out=self.token_out,
            recipient=self.sender, deadline=12345,
        )
        self.state_overrides = {
            self.sender: {"balance": self._word(10 ** 18)},
            self.token_in: {"stateDiff": {
                route_cost_evidence.solidity_balance_storage_key(
                    self.sender, 0
                ): self._word(100),
                route_cost_evidence.solidity_allowance_storage_key(
                    self.sender, self.router, 1
                ): self._word(100),
            }},
        }
        self.trace_requests = [self._trace_request(22)]
        self.scenario_specs = {22: {
            "schema": "route_cost_phase_b_scenario_spec/v1",
            "market_id": self.market_id,
            "direction": "sell",
            "requested_notional_usd": "1000",
            "simulation_target_token_address": self.token_in,
            "simulation_target_unit_decimals": "18",
            "simulation_target_raw_quantity": "100",
            "simulation_target_lattice_raw": "1",
            "simulation_target_sha256": "a" * 64,
            "core_pool_state_id": "state:test",
            "core_pool_state_sha256": "b" * 64,
            "chain_evidence_sha256": "c" * 64,
            "market_evidence_sha256": "d" * 64,
            "quoted_amount_in_raw": "100",
            "quoted_amount_out_raw": "50",
            "submission_loss_bound_bps": "0",
            "calldata_hex": self.calldata,
            "state_overrides": copy.deepcopy(self.state_overrides),
            "estimate_request_id": 12,
            "trace_request_id": 22,
        }}
        self.markets = {self.market_id: {"pair_address": self.pair}}
        self.keys = {
            "sender_balance": route_cost_evidence.solidity_balance_storage_key(
                self.sender, 0),
            "allowance": route_cost_evidence.solidity_allowance_storage_key(
                self.sender, self.router, 1),
            "pair_input": route_cost_evidence.solidity_balance_storage_key(
                self.pair, 0),
            "pair_output": route_cost_evidence.solidity_balance_storage_key(
                self.pair, 0),
            "recipient_output": route_cost_evidence.solidity_balance_storage_key(
                self.sender, 0),
        }

    def _trace_request(self, identifier):
        return {
            "schema": "route_cost_trace_request/v1", "jsonrpc": "2.0",
            "id": identifier, "method": "debug_traceCall",
            "params": [{
                "from": self.sender, "to": self.router, "gas": "0x5208",
                "data": self.calldata, "value": "0x0",
            }, "0x64", {
                "tracer": "prestateTracer",
                "tracerConfig": {"diffMode": True, "disableCode": True,
                                 "disableStorage": False},
                "stateOverrides": copy.deepcopy(self.state_overrides),
            }],
        }

    @staticmethod
    def _word(value):
        return "0x{:064x}".format(value)

    def _result(self):
        return {
            "pre": {
                self.token_in: {"balance": "0x0", "nonce": 1, "storage": {
                    self.keys["sender_balance"]: self._word(100),
                    self.keys["allowance"]: self._word(100),
                    self.keys["pair_input"]: self._word(1000),
                }},
                self.token_out: {"storage": {
                    self.keys["pair_output"]: self._word(1000),
                }},
                self.pair: {"storage": {"0x" + "f" * 64: self._word(7)}},
            },
            "post": {
                self.token_in: {"balance": "0x0", "nonce": 1, "storage": {
                    self.keys["sender_balance"]: self._word(0),
                    self.keys["allowance"]: self._word(0),
                    self.keys["pair_input"]: self._word(1100),
                }},
                self.token_out: {"storage": {
                    self.keys["pair_output"]: self._word(950),
                    self.keys["recipient_output"]: self._word(50),
                }},
                self.pair: {"storage": {"0x" + "f" * 64: self._word(8)}},
            },
        }

    def _decode(self, rows):
        return route_cost_collector._decode_phase_b_trace_batch(
            route_cost_evidence.canonical_json_bytes(rows),
            trace_requests=self.trace_requests,
            scenario_specs_by_trace_id=self.scenario_specs,
            adapter=self.adapter,
            market_evidence_by_id=self.markets,
            fixed_block_tag="0x64",
        )

    def test_valid_updates_new_and_cleared_slots_project_exact_response(self):
        raw = self._result()
        del raw["post"][self.token_in]["storage"][self.keys["allowance"]]
        got = self._decode([{"jsonrpc": "2.0", "id": 22, "result": raw}])
        self.assertEqual([row["id"] for row in got], [22])
        self.assertEqual(set(got[0]), {"schema", "jsonrpc", "id", "storage_diffs"})
        identities = [(row["token_address"], row["account_role"],
                       row["storage_key"]) for row in got[0]["storage_diffs"]]
        self.assertEqual(identities, sorted(identities))
        self.assertEqual(len(identities), 5)
        allowance = next(row for row in got[0]["storage_diffs"]
                         if row["storage_key"] == self.keys["allowance"])
        recipient = next(row for row in got[0]["storage_diffs"]
                         if row["account_role"] == "recipient")
        self.assertEqual((allowance["pre_present"], allowance["post_present"],
                          allowance["post_value"]), (True, False, self.ZERO))
        self.assertEqual((recipient["pre_present"], recipient["pre_value"],
                          recipient["post_present"]), (False, self.ZERO, True))

    def test_geth_code_hash_in_pre_and_post_is_validated_then_discarded(self):
        raw = self._result()
        raw["pre"][self.token_in]["codeHash"] = "0x" + "a" * 64
        raw["post"][self.token_out]["codeHash"] = "0x" + "b" * 64
        got = self._decode([{"jsonrpc": "2.0", "id": 22, "result": raw}])
        self.assertEqual(len(got[0]["storage_diffs"]), 5)
        for candidate in ("0x" + "a" * 63, "0x" + "A" * 64):
            invalid = self._result()
            invalid["pre"][self.token_in]["codeHash"] = candidate
            with self.subTest(code_hash=candidate), self.assertRaises(
                    RouteCostCollectorError):
                self._decode([{
                    "jsonrpc": "2.0", "id": 22, "result": invalid,
                }])

    def test_geth_empty_post_account_projects_cleared_planned_slots(self):
        raw = self._result()
        raw["pre"][self.token_out]["storage"][
            self.keys["recipient_output"]
        ] = self._word(50)
        raw["post"][self.token_out] = {}
        got = self._decode([{
            "jsonrpc": "2.0", "id": 22, "result": raw,
        }])
        token_out_rows = [
            row for row in got[0]["storage_diffs"]
            if row["token_address"] == self.token_out
        ]
        self.assertEqual(len(token_out_rows), 2)
        self.assertEqual(
            {(row["post_present"], row["post_value"])
             for row in token_out_rows},
            {(False, self.ZERO)},
        )

    def test_trace_request_authority_rejects_overrides_gas_and_block_forgery(self):
        raw = self._result()
        mutations = {}
        empty_overrides = copy.deepcopy(self.trace_requests)
        empty_overrides[0]["params"][2]["stateOverrides"] = {}
        mutations["empty-overrides"] = (empty_overrides, self.scenario_specs)
        arbitrary_overrides = copy.deepcopy(self.trace_requests)
        arbitrary_overrides[0]["params"][2]["stateOverrides"] = {
            self.sender: {"balance": self._word(1)},
        }
        mutations["arbitrary-overrides"] = (
            arbitrary_overrides, self.scenario_specs,
        )
        co_mutated_empty_request = copy.deepcopy(self.trace_requests)
        co_mutated_empty_request[0]["params"][2]["stateOverrides"] = {}
        co_mutated_empty_specs = copy.deepcopy(self.scenario_specs)
        co_mutated_empty_specs[22]["state_overrides"] = {}
        mutations["co-mutated-empty-overrides"] = (
            co_mutated_empty_request, co_mutated_empty_specs,
        )
        co_mutated_arbitrary_request = copy.deepcopy(self.trace_requests)
        co_mutated_arbitrary = {
            self.sender: {"balance": self._word(1)},
        }
        co_mutated_arbitrary_request[0]["params"][2][
            "stateOverrides"
        ] = copy.deepcopy(co_mutated_arbitrary)
        co_mutated_arbitrary_specs = copy.deepcopy(self.scenario_specs)
        co_mutated_arbitrary_specs[22]["state_overrides"] = copy.deepcopy(
            co_mutated_arbitrary
        )
        mutations["co-mutated-arbitrary-overrides"] = (
            co_mutated_arbitrary_request, co_mutated_arbitrary_specs,
        )
        gas_zero = copy.deepcopy(self.trace_requests)
        gas_zero[0]["params"][0]["gas"] = "0x0"
        mutations["gas-zero"] = (gas_zero, self.scenario_specs)
        nonminimal_gas = copy.deepcopy(self.trace_requests)
        nonminimal_gas[0]["params"][0]["gas"] = "0x05208"
        mutations["gas-nonminimal"] = (nonminimal_gas, self.scenario_specs)
        block_zero = copy.deepcopy(self.trace_requests)
        block_zero[0]["params"][1] = "0x0"
        mutations["block-zero"] = (block_zero, self.scenario_specs)
        forged_block = copy.deepcopy(self.trace_requests)
        forged_block[0]["params"][1] = "0x65"
        mutations["block-forged"] = (forged_block, self.scenario_specs)
        forged_specs = copy.deepcopy(self.scenario_specs)
        del forged_specs[22]["chain_evidence_sha256"]
        mutations["spec-authority-missing"] = (
            self.trace_requests, forged_specs,
        )
        forged_specs = copy.deepcopy(self.scenario_specs)
        forged_specs[22]["direction"] = "buy"
        mutations["spec-direction-forged"] = (
            self.trace_requests, forged_specs,
        )
        forged_specs = copy.deepcopy(self.scenario_specs)
        forged_specs[22]["quoted_amount_in_raw"] = "101"
        mutations["spec-amount-forged"] = (
            self.trace_requests, forged_specs,
        )
        forged_specs = copy.deepcopy(self.scenario_specs)
        forged_specs[22]["submission_loss_bound_bps"] = "1"
        mutations["spec-bound-forged"] = (
            self.trace_requests, forged_specs,
        )
        forged_specs = copy.deepcopy(self.scenario_specs)
        forged_specs[22]["simulation_target_token_address"] = self.token_out
        mutations["spec-target-forged"] = (
            self.trace_requests, forged_specs,
        )
        for label, (requests, specs) in mutations.items():
            with self.subTest(case=label), self.assertRaises(
                    RouteCostCollectorError):
                route_cost_collector._decode_phase_b_trace_batch(
                    route_cost_evidence.canonical_json_bytes([{
                        "jsonrpc": "2.0", "id": 22, "result": raw,
                    }]),
                    trace_requests=requests,
                    scenario_specs_by_trace_id=specs,
                    adapter=self.adapter,
                    market_evidence_by_id=self.markets,
                    fixed_block_tag="0x64",
                )

    def test_unplanned_changed_token_slot_is_rejected(self):
        raw = self._result()
        extra = "0x" + "e" * 64
        raw["pre"][self.token_in]["storage"][extra] = self._word(1)
        raw["post"][self.token_in]["storage"][extra] = self._word(2)
        with self.assertRaises(RouteCostCollectorError):
            self._decode([{"jsonrpc": "2.0", "id": 22, "result": raw}])

    def test_missing_planned_slot_is_rejected(self):
        raw = self._result()
        del raw["pre"][self.token_in]["storage"][self.keys["allowance"]]
        del raw["post"][self.token_in]["storage"][self.keys["allowance"]]
        with self.assertRaises(RouteCostCollectorError):
            self._decode([{"jsonrpc": "2.0", "id": 22, "result": raw}])

    def test_wrong_or_duplicate_response_ids_are_rejected(self):
        raw = self._result()
        cases = ([{"jsonrpc": "2.0", "id": 23, "result": raw}], [
            {"jsonrpc": "2.0", "id": 22, "result": raw},
            {"jsonrpc": "2.0", "id": 22, "result": raw},
        ])
        for rows in cases:
            with self.subTest(rows=len(rows)), self.assertRaises(
                    RouteCostCollectorError):
                self._decode(rows)

    def test_eip55_account_keys_normalize_but_case_collision_rejects(self):
        raw = self._result()
        mixed = "0x" + self.sender[2:].upper()
        raw["pre"][mixed] = {"balance": "0x0"}
        raw["post"][mixed] = {"balance": "0x1"}
        got = self._decode([{"jsonrpc": "2.0", "id": 22, "result": raw}])
        self.assertEqual({row["token_address"] for row in got[0]["storage_diffs"]},
                         {self.token_in, self.token_out})
        raw = self._result()
        raw["pre"][self.sender] = {"balance": "0x0"}
        raw["pre"][mixed] = {"balance": "0x1"}
        with self.assertRaises(RouteCostCollectorError):
            self._decode([{"jsonrpc": "2.0", "id": 22, "result": raw}])

    def test_pair_reserve_storage_is_validated_then_ignored(self):
        raw = self._result()
        got = self._decode([{"jsonrpc": "2.0", "id": 22, "result": raw}])
        self.assertNotIn(self.pair, {
            row["token_address"] for row in got[0]["storage_diffs"]
        })
        raw["pre"][self.pair]["storage"] = {"0x1": self._word(7)}
        with self.assertRaises(RouteCostCollectorError):
            self._decode([{"jsonrpc": "2.0", "id": 22, "result": raw}])

    def test_structlogs_null_short_uppercase_same_and_account_fields_reject(self):
        mutations = {}
        structlogs = self._result()
        structlogs = {"gas": 1, "failed": False, "structLogs": []}
        mutations["structLogs"] = structlogs
        null_storage = self._result()
        null_storage["pre"][self.token_in]["storage"] = None
        mutations["null-storage"] = null_storage
        short = self._result()
        short["pre"][self.token_in]["storage"][self.keys["allowance"]] = "0x0"
        mutations["short-word"] = short
        uppercase = self._result()
        uppercase["pre"][self.token_in]["storage"][self.keys["allowance"]] = (
            "0x" + "A" * 64
        )
        mutations["uppercase-word"] = uppercase
        same = self._result()
        same["post"][self.token_in]["storage"][self.keys["allowance"]] = (
            same["pre"][self.token_in]["storage"][self.keys["allowance"]]
        )
        mutations["same-word"] = same
        for field, value in (
            ("code", "0x6000"), ("callerExtra", 1),
        ):
            candidate = self._result()
            candidate["pre"][self.token_in][field] = value
            mutations[field] = candidate
        for label, result in mutations.items():
            with self.subTest(case=label), self.assertRaises(
                    RouteCostCollectorError):
                self._decode([{"jsonrpc": "2.0", "id": 22, "result": result}])

    def test_duplicate_json_keys_and_normalized_account_collision_reject(self):
        raw = route_cost_evidence.canonical_json_bytes([{
            "jsonrpc": "2.0", "id": 22, "result": self._result(),
        }])
        duplicated = raw.replace(b'"id":22', b'"id":22,"id":22', 1)
        with self.assertRaises(RouteCostCollectorError):
            route_cost_collector._decode_phase_b_trace_batch(
                duplicated, trace_requests=self.trace_requests,
                scenario_specs_by_trace_id=self.scenario_specs,
                adapter=self.adapter, market_evidence_by_id=self.markets,
                fixed_block_tag="0x64",
            )

    def test_calldata_descriptor_slot_and_pair_transplants_reject(self):
        raw = self._result()
        wrong_specs = copy.deepcopy(self.scenario_specs)
        wrong_specs[22]["calldata_hex"] = route_cost_evidence.build_v2_swap_calldata(
            direction="sell", quoted_amount_in_raw=101,
            quoted_amount_out_raw=50, submission_loss_bound_bps=0,
            path_token_in=self.token_in, path_token_out=self.token_out,
            recipient=self.sender, deadline=12345,
        )
        wrong_adapter = copy.deepcopy(self.adapter)
        wrong_adapter["token_funding_descriptors"][0]["allowance_mapping_slot"] = "2"
        wrong_markets = {self.market_id: {"pair_address": "0x" + "4" * 40}}
        for label, specs, adapter_value, markets in (
            ("calldata", wrong_specs, self.adapter, self.markets),
            ("slot", self.scenario_specs, wrong_adapter, self.markets),
            ("pair", self.scenario_specs, self.adapter, wrong_markets),
        ):
            with self.subTest(case=label), self.assertRaises(
                    RouteCostCollectorError):
                route_cost_collector._decode_phase_b_trace_batch(
                    route_cost_evidence.canonical_json_bytes([{
                        "jsonrpc": "2.0", "id": 22, "result": raw,
                    }]), trace_requests=self.trace_requests,
                    scenario_specs_by_trace_id=specs, adapter=adapter_value,
                    market_evidence_by_id=markets,
                    fixed_block_tag="0x64",
                )

    def test_response_is_returned_in_request_order(self):
        second = self._trace_request(23)
        requests = [second, self.trace_requests[0]]
        specs = {23: dict(self.scenario_specs[22], trace_request_id=23),
                 22: self.scenario_specs[22]}
        rows = [{"jsonrpc": "2.0", "id": 22, "result": self._result()},
                {"jsonrpc": "2.0", "id": 23, "result": self._result()}]
        got = route_cost_collector._decode_phase_b_trace_batch(
            route_cost_evidence.canonical_json_bytes(rows),
            trace_requests=requests, scenario_specs_by_trace_id=specs,
            adapter=self.adapter, market_evidence_by_id=self.markets,
            fixed_block_tag="0x64",
        )
        self.assertEqual([row["id"] for row in got], [23, 22])

    def test_fixed_resource_limit_and_json_rpc_error_classification(self):
        with self.assertRaisesRegex(RouteCostCollectorError, "resource limit"):
            route_cost_collector._decode_phase_b_trace_batch(
                b" " * (8 * 1024 * 1024 + 1),
                trace_requests=self.trace_requests,
                scenario_specs_by_trace_id=self.scenario_specs,
                adapter=self.adapter, market_evidence_by_id=self.markets,
                fixed_block_tag="0x64",
            )
        valid_error = [{"jsonrpc": "2.0", "id": 22,
                        "error": {"code": -32000, "message": "trace failed"}}]
        with self.assertRaisesRegex(RouteCostCollectorError, "RPC unavailable"):
            self._decode(valid_error)
        for error in (
            None, {"code": -32000, "message": ""},
            {"code": True, "message": "trace failed"},
            {"code": -32000, "message": "trace failed", "extra": 1},
        ):
            with self.subTest(error=error), self.assertRaisesRegex(
                    RouteCostCollectorError, "trace response is invalid"):
                self._decode([{"jsonrpc": "2.0", "id": 22, "error": error}])


class NativePriceCaptureTests(unittest.TestCase):
    @staticmethod
    def _raw_bodies():
        from tests.test_route_cost_evidence import native_price_captured_bytes

        return native_price_captured_bytes()

    @staticmethod
    def _response(body, *, headers=(), chunk_size=7):
        response = _Response(body, headers=headers, chunk_size=chunk_size)
        response.socket_timeouts = []
        socket_value = type("SocketSpy", (), {})()
        socket_value.settimeout = response.socket_timeouts.append
        response.fp = type("Buffered", (), {
            "raw": type("Raw", (), {"_sock": socket_value})(),
        })()
        return response

    @staticmethod
    def _capture(**overrides):
        from scripts.route_cost_collector import _capture_native_price_evidence

        arguments = {
            "run_id": "collector-run",
            "route_cohort_id": _COLLECTOR_COHORT_ID,
            "candidate_source_generation": _COLLECTOR_GENERATION,
            "capture_utc_anchor": "2026-08-01T12:00:00.123456Z",
        }
        arguments.update(overrides)
        return _capture_native_price_evidence(**arguments)

    def test_exact_two_gets_share_budget_and_seal_microsecond_receipts(self):
        book_raw, rules_raw = self._raw_bodies()
        opened = []
        make_response = self._response

        class Opener:
            addheaders = [("User-Agent", "must-be-cleared")]

            def open(self, request, *, timeout):
                opened.append((request, timeout, list(self.addheaders)))
                body = book_raw if len(opened) == 1 else rules_raw
                return make_response(body, headers=((
                    "Content-Length", str(len(body)),
                ),), chunk_size=len(body))

        opener = Opener()
        with patch(
            "scripts.route_cost_collector.urllib.request.build_opener",
            return_value=opener,
        ), patch(
            "scripts.route_cost_collector.time.monotonic",
            side_effect=[
                100.0, 100.0, 100.25, 100.25,
                100.5, 100.5, 100.75, 100.75, 101.0,
            ],
        ):
            result = self._capture()

        self.assertEqual(len(opened), 2)
        self.assertEqual((result.status, result.reason_code), ("observed", None))
        value = result.evidence
        self.assertEqual(
            [request.full_url for request, _timeout, _headers in opened],
            [
                "https://data-api.binance.vision/api/v3/depth?symbol=ETHUSDT&limit=100",
                "https://api.binance.com/api/v3/exchangeInfo?symbol=ETHUSDT",
            ],
        )
        self.assertEqual(
            [request.get_method() for request, _timeout, _headers in opened],
            ["GET", "GET"],
        )
        self.assertEqual(
            [
                {key.lower(): nested for key, nested in request.header_items()}
                for request, _timeout, _headers in opened
            ],
            [{"accept": "application/json"}, {"accept": "application/json"}],
        )
        self.assertEqual([timeout for _request, timeout, _headers in opened], [5.0, 4.5])
        self.assertEqual([headers for _request, _timeout, headers in opened], [[], []])
        self.assertEqual(
            value["book_request_receipt"]["captured_at"],
            "2026-08-01T12:00:00.623456Z",
        )
        self.assertEqual(
            value["market_rules_request_receipt"]["captured_at"],
            "2026-08-01T12:00:01.123456Z",
        )
        self.assertEqual(
            value["raw_response_sha256"], hashlib.sha256(book_raw).hexdigest()
        )
        self.assertEqual(
            value["market_rules_raw_response_sha256"],
            hashlib.sha256(rules_raw).hexdigest(),
        )

    def test_first_fast_transport_failure_still_attempts_rules_once(self):
        _book_raw, rules_raw = self._raw_bodies()
        attempts = []
        make_response = self._response

        class Opener:
            addheaders = []

            def open(self, request, *, timeout):
                attempts.append((request.full_url, timeout))
                if len(attempts) == 1:
                    raise urllib.error.URLError("SECRET fast failure")
                return make_response(rules_raw, chunk_size=len(rules_raw))

        with patch(
            "scripts.route_cost_collector.urllib.request.build_opener",
            return_value=Opener(),
        ), patch(
            "scripts.route_cost_collector.time.monotonic",
            side_effect=[100.0, 100.1, 100.2, 100.2, 100.3, 100.3, 100.4],
        ):
            result = self._capture()

        self.assertEqual(len(attempts), 2)
        self.assertEqual(
            (result.status, result.reason_code, result.evidence),
            ("unavailable", "native_price_unavailable", None),
        )
        self.assertNotIn("SECRET", repr(result))

    def test_expired_shared_budget_does_not_issue_rules_request(self):
        attempts = []

        class Opener:
            addheaders = []

            def open(self, request, *, timeout):
                attempts.append((request.full_url, timeout))
                raise urllib.error.URLError("first request timeout")

        with patch(
            "scripts.route_cost_collector.urllib.request.build_opener",
            return_value=Opener(),
        ), patch(
            "scripts.route_cost_collector.time.monotonic",
            side_effect=[100.0, 100.0, 105.0],
        ):
            result = self._capture()

        self.assertEqual(len(attempts), 1)
        self.assertEqual(
            (result.status, result.reason_code, result.evidence),
            ("unavailable", "native_price_unavailable", None),
        )

    def test_redirect_malformed_and_oversize_are_secret_free_typed_failures(self):
        _book_raw, rules_raw = self._raw_bodies()
        make_response = self._response
        cases = (
            (urllib.error.HTTPError(
                "https://secret.invalid", 302, "SECRET redirect", {}, None
            ), "native_price_invalid"),
            (make_response(b"[", chunk_size=1), "native_price_invalid"),
            (make_response(
                b" " * (2 * 1024 * 1024 + 1),
                headers=(("Content-Length", str(2 * 1024 * 1024 + 1)),),
                chunk_size=2 * 1024 * 1024 + 1,
            ), "resource_limit"),
        )
        for first_result, expected in cases:
            attempts = 0

            class Opener:
                addheaders = []

                def open(self, _request, *, timeout):
                    nonlocal attempts
                    attempts += 1
                    if attempts == 1:
                        if isinstance(first_result, BaseException):
                            raise first_result
                        return first_result
                    return make_response(rules_raw, chunk_size=len(rules_raw))

            context = (
                self.assertRaisesRegex(RouteCostCollectorError, "resource limit")
                if expected == "resource_limit" else __import__("contextlib").nullcontext()
            )
            with self.subTest(expected=expected), patch(
                "scripts.route_cost_collector.urllib.request.build_opener",
                return_value=Opener(),
            ), patch(
                "scripts.route_cost_collector.time.monotonic",
                side_effect=[100.0] * 16,
            ), context:
                result = self._capture()
            if expected != "resource_limit":
                self.assertEqual(
                    (result.status, result.reason_code, result.evidence),
                    ("failed", expected, None),
                )
                self.assertNotIn("SECRET", repr(result))
                self.assertNotIn("secret.invalid", repr(result))
