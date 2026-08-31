"""Pure historical replay bridge for the Phase-3 research universe.

The closed Mapping accepted here is deliberately **not root authority**.  It is
an immutable-input boundary used to freeze replay arithmetic and schemas.  Task 7
must supply it from its opaque, root-authenticated snapshot after descriptor
rereads; that integration must not be replaced by trusting a caller Mapping.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
import weakref
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
from types import MappingProxyType
from typing import Any, Dict, Mapping, Tuple

from scripts.fetch_dex_depth import decimal_text, depth_fields, v2_band_amounts
from scripts.execution_cost_components import (
    COST_COMPONENT_COLUMNS,
    COST_COMPONENT_CONTRACT_VERSION,
)
from scripts.historical_foundry_contracts import HistoricalFoundryConfigSet
from scripts.route_cohort import canonical_route_id
from scripts.route_opportunity import (
    OPPORTUNITY_FIELDS,
    ROUTE_OPPORTUNITY_CONTRACT_VERSION,
    route_opportunity_id,
)
from scripts.route_quantity import (
    V2PoolState,
    V2_FEE_FORMULA,
    v2_exact_input_amount_out_raw,
)
from scripts.route_cost_topology import HISTORICAL_ATOMIC_COMPONENT_MATRIX


_UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
_WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
_UNISWAP_ROUTER = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
_SUSHISWAP_ROUTER = "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f"
_VENUES = ("uniswap_v2", "sushiswap_v2")
_EXECUTION_CLAIM = "historical_counterfactual_state_override_next_block"
_RUN_FIELDS = frozenset((
    "schema", "run_id", "snapshot_run_id", "manifest_sha256",
    "policy_sha256", "authority_sha256",
    "toolchain_sha256", "scan_inventory_sha256", "selection", "selection_sha256",
    "scenarios", "typed_members", "task7_candidate_manifest_hex",
    "task7_selection_hex",
    "task7_typed_manifest_hex", "evidence_sha256",
))
_SELECTION_FIELDS = frozenset((
    "anchor_timestamp", "block_timestamp", "block_number", "block_hash", "block_header_sha256",
    "venues", "routes",
))
_PAYLOAD_FIELDS = frozenset((
    "schema", "chain", "chain_id", "dex", "pool_address", "token0_address",
    "token1_address", "token0_decimals", "token1_decimals", "reserve0_raw",
    "reserve1_raw", "reserve_timestamp_last_raw", "fee_bps", "fee_numerator",
    "fee_denominator", "fee_formula", "fee_proof_sha256", "block_number",
    "block_hash", "block_header_sha256", "observed_at",
    "raw_response_sha256", "state_id",
))
_DESCRIPTOR_FIELDS = frozenset((
    "market_id", "role", "adapter_id", "content_schema", "path", "filename",
    "byte_count", "sha256", "logical_generation",
))
_USD_PAYLOAD_FIELDS = frozenset((
    "schema", "market_id", "venue_id", "chain_id", "block_number",
    "block_hash", "proxy_address", "round_id", "phase_id", "answer",
    "decimals", "started_at", "updated_at", "answered_in_round",
    "valid_until", "scan_inventory_sha256",
))
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_RUN_ID = re.compile(r"run:[0-9a-f]{64}\Z")
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")
_UNISWAP_V2_UNI_WETH_PAIR = "0xd3d2e2692501a5c9ca623199d38826e513033a17"
_SUSHISWAP_V2_UNI_WETH_PAIR = "0xdafd66636e2561b0284edde37e42d192f2844d40"
_PAIR_BY_VENUE = MappingProxyType({
    "uniswap_v2": _UNISWAP_V2_UNI_WETH_PAIR,
    "sushiswap_v2": _SUSHISWAP_V2_UNI_WETH_PAIR,
})
_TASK7_RUN_ID_DOMAIN = b"historical_foundry_run_id/v1\0"
_TASK7_SELECTION_FIELDS = frozenset((
    "schema", "status", "staging_inventory_sha256", "prefilter_grid_digest",
    "candidate_block_count", "scenario_denominator",
    "initial_replay_required_count", "selected_block",
    "selected_scenario_count", "selected_scenarios", "candidate_states",
    "unresolved_candidate_count",
))
_TASK7_CANDIDATE_MANIFEST_FIELDS = frozenset((
    "schema", "staging_inventory_sha256", "prefilter_grid_digest",
    "candidate_block_count", "scenario_denominator",
    "initial_replay_required_count", "attempted_scenario_count",
    "candidate_states", "scenarios",
))
_TASK7_CANDIDATE_SCENARIO_FIELDS = frozenset((
    "scenario_key", "block_number", "status", "classification", "gas_used",
    "effective_gas_price", "weth_delta_raw", "proof_inputs_hash",
    "overlay_sha256", "receipt_sha256", "trace_sha256", "result_sha256",
    "economics",
))
_TASK7_BLOCK_FIELDS = frozenset((
    "number", "hash", "parent_hash", "state_root", "timestamp",
    "gas_limit", "gas_used", "base_fee_per_gas",
))
_TASK7_SCENARIO_FIELDS = frozenset((
    "scenario_key", "block_number", "status", "classification", "gas_used",
    "effective_gas_price", "weth_delta_raw", "proof_inputs_hash",
    "overlay_sha256", "receipt_sha256", "trace_sha256", "result_sha256",
    "economics", "direction", "requested_notional_usd",
))
_TASK7_CANDIDATE_STATE_FIELDS = frozenset((
    "block_number", "state", "transitions", "scenario_count",
))
_TASK7_TYPED_MANIFEST_FIELDS = frozenset((
    "schema", "selection_status", "selected_block", "market_count",
    "markets", "member_count", "members",
))
_TASK7_MARKET_FIELDS = frozenset((
    "market_id", "market_key", "venue_id", "pair_address",
    "factory_pair_forward", "factory_pair_reverse", "members",
))
_TASK7_MARKET_MEMBER_FIELDS = frozenset((
    "role", "path", "byte_count", "sha256",
))
_TASK7_GLOBAL_MEMBER_FIELDS = frozenset((
    "path", "byte_count", "sha256",
))
_TASK7_ECONOMICS_FIELDS = frozenset((
    "gross_edge_usd", "gas_cost_usd", "mev_buffer_usd",
    "policy_net_edge_usd",
))
_TASK7_FRACTION_FIELDS = frozenset((
    "numerator", "denominator", "display",
))


def _initialize_validated_historical_run_capability():
    issuer = object()
    registry = {}

    class ValidatedHistoricalRun(MappingABC):
        __slots__ = ("__weakref__",)

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise ValueError("validated historical run construction is private")

        def __getitem__(self, key: str) -> Any:
            return require_projection(self)[key]

        def __iter__(self):
            return iter(require_projection(self))

        def __len__(self) -> int:
            return len(require_projection(self))

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("validated historical run is immutable")

        def __repr__(self) -> str:
            return "ValidatedHistoricalRun(<redacted>)"

        def __reduce_ex__(self, _protocol: int) -> Any:
            raise TypeError("validated historical run is not serializable")

    def require_projection(value: Any) -> Mapping[str, Any]:
        entry = registry.get(id(value))
        if (
            type(value) is not ValidatedHistoricalRun
            or entry is None
            or entry[0]() is not value
            or entry[1].get("issuer") is not issuer
        ):
            raise ValueError("validated historical run capability is invalid")
        return entry[1]["projection"]

    def issue(projection: Mapping[str, Any]) -> Any:
        value = object.__new__(ValidatedHistoricalRun)
        value_id = id(value)
        record = {"issuer": issuer, "projection": projection}

        def retire(reference: weakref.ReferenceType) -> None:
            current = registry.get(value_id)
            if current is not None and current[0] is reference:
                registry.pop(value_id, None)

        registry[value_id] = (weakref.ref(value, retire), record)
        return value

    def validate_selected_historical_run(
        *, config: HistoricalFoundryConfigSet,
        run_evidence: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Validate one already-reread closed input; Task 7 owns root authenticity."""
        if not isinstance(config, HistoricalFoundryConfigSet):
            raise ValueError("historical config capability is invalid")
        value = _exact_fields(run_evidence, _RUN_FIELDS, "historical run")
        if value["schema"] != "historical_foundry_selected_run_closed/v1":
            raise ValueError("historical run schema is invalid")
        if (
            not isinstance(value["run_id"], str)
            or _RUN_ID.fullmatch(value["run_id"]) is None
            or value["snapshot_run_id"] != value["run_id"]
            or _hash(value["manifest_sha256"], "manifest hash")
            != value["manifest_sha256"]
        ):
            raise ValueError("historical run id is invalid")
        for field, expected in (
            ("policy_sha256", config.policy.physical_sha256),
            ("authority_sha256", config.authority.physical_sha256),
            ("toolchain_sha256", config.toolchain.physical_sha256),
        ):
            if value[field] != expected:
                raise ValueError("historical config binding differs")
        unsigned = {
            key: item for key, item in value.items() if key != "evidence_sha256"
        }
        if _hash(value["evidence_sha256"], "evidence hash") != _digest(unsigned):
            raise ValueError("historical evidence hash differs")
        task7_candidate_manifest_bytes, task7_candidate_manifest = (
            _decode_task7_canonical_document(
                value["task7_candidate_manifest_hex"],
                "Task-7 candidate manifest",
            )
        )
        task7_selection_bytes, task7_selection = (
            _decode_task7_canonical_document(
                value["task7_selection_hex"], "Task-7 selection"
            )
        )
        task7_typed_manifest_bytes, task7_typed_manifest = (
            _decode_task7_canonical_document(
                value["task7_typed_manifest_hex"], "Task-7 typed manifest"
            )
        )
        expected_run_id = "run:" + hashlib.sha256(
            _TASK7_RUN_ID_DOMAIN
            + task7_candidate_manifest_bytes
            + task7_typed_manifest_bytes
            + task7_selection_bytes
        ).hexdigest()
        if (
            value["run_id"] != expected_run_id
            or value["snapshot_run_id"] != expected_run_id
        ):
            raise ValueError("historical run preimage differs")
        selected = _exact_fields(
            value["selection"], _SELECTION_FIELDS, "selection"
        )
        if _hash(value["selection_sha256"], "selection hash") != _digest(selected):
            raise ValueError("selection hash differs")
        scan_inventory_sha256 = _hash(
            value["scan_inventory_sha256"], "scan inventory hash"
        )
        anchor_time = _timestamp(selected["anchor_timestamp"])
        block_time = _timestamp(selected["block_timestamp"])
        lookback = config.policy.value["lookback_seconds"]
        if not anchor_time - timedelta(seconds=lookback) <= block_time <= anchor_time:
            raise ValueError("selected block is outside the policy window")
        if type(selected["block_number"]) is not int or selected["block_number"] <= 0:
            raise ValueError("selected block differs")
        if (
            not isinstance(selected["block_hash"], str)
            or re.fullmatch(r"0x[0-9a-f]{64}", selected["block_hash"]) is None
            or _hash(selected["block_header_sha256"], "block header hash")
            != selected["block_header_sha256"]
        ):
            raise ValueError("selected block identity differs")
        _exact_fields(selected["venues"], frozenset(_VENUES), "selected venues")
        for dex in _VENUES:
            venue = _exact_fields(
                selected["venues"][dex],
                frozenset((
                    "pair_address", "factory_pair_forward",
                    "factory_pair_reverse", "reserve_uni_raw",
                    "reserve_weth_raw", "reserve_timestamp_last_raw",
                    "raw_response_sha256",
                )),
                "venue",
            )
            expected_pair = _PAIR_BY_VENUE[dex]
            if (
                not isinstance(venue["pair_address"], str)
                or _ADDRESS.fullmatch(venue["pair_address"]) is None
                or venue["pair_address"] != expected_pair
                or venue["factory_pair_forward"] != expected_pair
                or venue["factory_pair_reverse"] != expected_pair
                or type(venue["reserve_uni_raw"]) is not int
                or type(venue["reserve_weth_raw"]) is not int
                or type(venue["reserve_timestamp_last_raw"]) is not int
                or not 0 <= venue["reserve_timestamp_last_raw"] < 2 ** 32
                or _hash(venue["raw_response_sha256"], "reserve response hash")
                != venue["raw_response_sha256"]
                or min(venue["reserve_uni_raw"], venue["reserve_weth_raw"]) <= 0
            ):
                raise ValueError("selected venue differs")
        if not isinstance(selected["routes"], list) or len(selected["routes"]) != 2:
            raise ValueError("selected routes are invalid")
        expected_routes = []
        for buy, sell in (
            ("uniswap_v2", "sushiswap_v2"),
            ("sushiswap_v2", "uniswap_v2"),
        ):
            identity = {
                "token_symbol": "UNI",
                "buy_market_id": "dex:eth:{}:{}:UNI".format(
                    buy, selected["venues"][buy]["pair_address"]
                ),
                "sell_market_id": "dex:eth:{}:{}:UNI".format(
                    sell, selected["venues"][sell]["pair_address"]
                ),
                "route_mode": "atomic_onchain",
            }
            expected_routes.append({
                **identity, "route_id": canonical_route_id(identity)
            })
        if selected["routes"] != expected_routes:
            raise ValueError("selected routes differ")
        if (
            not isinstance(value["typed_members"], list)
            or len(value["typed_members"]) != 4
        ):
            raise ValueError("typed members are invalid")
        payloads = []
        price_payloads = []
        eth_usd_values = []
        for index, dex in enumerate(_VENUES):
            payloads.append(_validate_typed_member(
                value["typed_members"][index * 2], config=config,
                dex=dex, selected=selected,
            ))
            price_payload, eth_usd = _validate_usd_member(
                value["typed_members"][index * 2 + 1], config=config,
                dex=dex, selected=selected,
                scan_inventory_sha256=scan_inventory_sha256,
            )
            payloads.append(price_payload)
            price_payloads.append({
                key: item for key, item in price_payload.items()
                if key not in {"market_id", "venue_id"}
            })
            eth_usd_values.append(eth_usd)
        if (
            price_payloads[0] != price_payloads[1]
            or eth_usd_values[0] != eth_usd_values[1]
        ):
            raise ValueError("USD market contexts differ")
        scenarios = value["scenarios"]
        expected_pairs = {
            (route["route_id"], notional)
            for route in expected_routes
            for notional in (1000, 5000, 10000, 50000, 100000)
        }
        actual = set()
        if not isinstance(scenarios, list) or len(scenarios) != 10:
            raise ValueError("historical scenarios are invalid")
        for scenario in scenarios:
            _exact_fields(
                scenario,
                frozenset((
                    "route_id", "requested_notional_usd", "receipt_status",
                )),
                "scenario",
            )
            if (
                type(scenario["receipt_status"]) is not int
                or scenario["receipt_status"] != 1
                or type(scenario["requested_notional_usd"]) is not int
            ):
                raise ValueError("historical scenario is not proved")
            actual.add((scenario["route_id"], scenario["requested_notional_usd"]))
        if actual != expected_pairs:
            raise ValueError("historical scenarios differ")
        _validate_task7_selection(
            task7_selection,
            selected=selected,
            scan_inventory_sha256=scan_inventory_sha256,
            expected_routes=expected_routes,
            scenarios=scenarios,
        )
        _validate_task7_candidate_manifest(
            task7_candidate_manifest,
            task7_selection=task7_selection,
        )
        _validate_task7_typed_manifest(
            task7_typed_manifest,
            task7_selection=task7_selection,
            selected=selected,
            typed_members=value["typed_members"],
        )
        normalized = dict(_plain(value))
        normalized["typed_payloads"] = payloads
        normalized["eth_usd"] = decimal_text(eth_usd_values[0])
        return issue(_freeze(normalized))

    return validate_selected_historical_run, require_projection


(
    validate_selected_historical_run,
    _validated_run_projection,
) = _initialize_validated_historical_run_capability()
del _initialize_validated_historical_run_capability


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(_plain(value), allow_nan=False, ensure_ascii=False,
                      sort_keys=True, separators=(",", ":")).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _typed_digest(domain: str, value: Any) -> str:
    return hashlib.sha256(
        domain.encode("ascii") + b"\0" + _canonical_bytes(value)
    ).hexdigest()


def _exact_fields(value: Any, expected: frozenset, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or frozenset(value) != expected:
        raise ValueError("{} fields are invalid".format(label))
    return value


def _hash(value: Any, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise ValueError("{} is invalid".format(label))
    return value


def _decode_task7_canonical_document(value: Any, label: str):
    if (
        type(value) is not str
        or not value
        or re.fullmatch(r"(?:[0-9a-f]{2})+", value) is None
    ):
        raise ValueError("{} bytes are invalid".format(label))
    try:
        raw = bytes.fromhex(value)
        document = json.loads(raw.decode("utf-8"))
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("{} bytes are invalid".format(label))
    if (
        type(document) is not dict
        or raw.hex() != value
        or _canonical_bytes(document) != raw
    ):
        raise ValueError("{} is not canonical".format(label))
    return raw, document


def _task7_int(value: Any, label: str, *, minimum: Any = None) -> int:
    if type(value) is not int or (minimum is not None and value < minimum):
        raise ValueError("{} is invalid".format(label))
    return value


def _validate_task7_fraction(value: Any, label: str) -> Mapping[str, Any]:
    fraction = _exact_fields(value, _TASK7_FRACTION_FIELDS, label)
    numerator = _task7_int(fraction["numerator"], label + " numerator")
    denominator = _task7_int(
        fraction["denominator"], label + " denominator", minimum=1
    )
    display = fraction["display"]
    if type(display) is not str or not display:
        raise ValueError("{} display is invalid".format(label))
    try:
        decimal = Decimal(display)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("{} display is invalid".format(label))
    if not decimal.is_finite() or decimal * denominator != numerator:
        raise ValueError("{} display differs".format(label))
    return fraction


def _validate_task7_selection(
    value: Any, *, selected: Mapping[str, Any],
    scan_inventory_sha256: str, expected_routes: Any, scenarios: Any,
) -> None:
    selection = _exact_fields(
        value, _TASK7_SELECTION_FIELDS, "Task-7 selection"
    )
    if (
        selection["schema"] != "historical_foundry_selection/v1"
        or selection["status"]
        != "found_publishable_profitable_block"
        or selection["staging_inventory_sha256"]
        != scan_inventory_sha256
        or _hash(
            selection["prefilter_grid_digest"], "Task-7 prefilter grid hash"
        ) != selection["prefilter_grid_digest"]
    ):
        raise ValueError("Task-7 selection identity differs")
    candidate_count = _task7_int(
        selection["candidate_block_count"],
        "Task-7 candidate block count", minimum=1,
    )
    denominator = _task7_int(
        selection["scenario_denominator"],
        "Task-7 scenario denominator", minimum=10,
    )
    initial_count = _task7_int(
        selection["initial_replay_required_count"],
        "Task-7 initial replay count", minimum=1,
    )
    if denominator != candidate_count * 10 or initial_count > denominator:
        raise ValueError("Task-7 selection counts differ")
    block = _exact_fields(
        selection["selected_block"], _TASK7_BLOCK_FIELDS,
        "Task-7 selected block",
    )
    for field in (
        "number", "timestamp", "gas_limit", "gas_used", "base_fee_per_gas",
    ):
        _task7_int(block[field], "Task-7 block {}".format(field), minimum=0)
    if (
        block["number"] <= 0
        or block["gas_limit"] <= 0
        or block["gas_used"] > block["gas_limit"]
        or not isinstance(block["hash"], str)
        or re.fullmatch(r"0x[0-9a-f]{64}", block["hash"]) is None
        or not isinstance(block["parent_hash"], str)
        or re.fullmatch(r"0x[0-9a-f]{64}", block["parent_hash"]) is None
        or not isinstance(block["state_root"], str)
        or re.fullmatch(r"0x[0-9a-f]{64}", block["state_root"]) is None
        or block["number"] != selected["block_number"]
        or block["hash"] != selected["block_hash"]
        or block["timestamp"]
        != int(_timestamp(selected["block_timestamp"]).timestamp())
        or _digest(block) != selected["block_header_sha256"]
    ):
        raise ValueError("Task-7 selected block differs")
    selected_scenarios = selection["selected_scenarios"]
    if (
        _task7_int(
            selection["selected_scenario_count"],
            "Task-7 selected scenario count", minimum=0,
        ) != 10
        or type(selected_scenarios) is not list
        or len(selected_scenarios) != 10
        or _task7_int(
            selection["unresolved_candidate_count"],
            "Task-7 unresolved candidate count", minimum=0,
        ) != 0
    ):
        raise ValueError("Task-7 selected scenarios differ")
    notionals = (1000, 5000, 10000, 50000, 100000)
    directions = ("uniswap_to_sushiswap", "sushiswap_to_uniswap")
    expected_reduced = [
        (route["route_id"], notional, 1)
        for route in expected_routes for notional in notionals
    ]
    if [
        (row["route_id"], row["requested_notional_usd"], row["receipt_status"])
        for row in scenarios
    ] != expected_reduced:
        raise ValueError("historical scenario ordering differs")
    expected_task7 = [
        (direction, notional)
        for direction in directions for notional in notionals
    ]
    positive_count = 0
    for scenario, (direction, notional) in zip(
        selected_scenarios, expected_task7
    ):
        row = _exact_fields(
            scenario, _TASK7_SCENARIO_FIELDS, "Task-7 scenario"
        )
        if (
            row["scenario_key"]
            != "{}:{}:{}".format(block["number"], direction, notional)
            or _task7_int(
                row["block_number"], "Task-7 scenario block", minimum=1
            ) != block["number"]
            or _task7_int(
                row["status"], "Task-7 scenario status", minimum=0
            ) != 1
            or row["classification"] != "replay_success"
            or row["direction"] != direction
            or _task7_int(
                row["requested_notional_usd"],
                "Task-7 scenario notional", minimum=1,
            ) != notional
            or _task7_int(
                row["gas_used"], "Task-7 scenario gas", minimum=1
            ) <= 0
            or _task7_int(
                row["effective_gas_price"],
                "Task-7 scenario gas price", minimum=1,
            ) <= 0
            or type(row["weth_delta_raw"]) is not int
        ):
            raise ValueError("Task-7 scenario identity differs")
        for field in (
            "proof_inputs_hash", "overlay_sha256", "receipt_sha256",
            "trace_sha256", "result_sha256",
        ):
            _hash(row[field], "Task-7 scenario {}".format(field))
        economics = _exact_fields(
            row["economics"], _TASK7_ECONOMICS_FIELDS,
            "Task-7 scenario economics",
        )
        for field in _TASK7_ECONOMICS_FIELDS:
            _validate_task7_fraction(
                economics[field], "Task-7 scenario {}".format(field)
            )
        positive_count += (
            economics["policy_net_edge_usd"]["numerator"] > 0
        )
    if positive_count == 0:
        raise ValueError("Task-7 selection is not profitable")
    candidate_states = selection["candidate_states"]
    if type(candidate_states) is not list or not candidate_states:
        raise ValueError("Task-7 candidate states are invalid")
    selected_states = []
    for state in candidate_states:
        row = _exact_fields(
            state, _TASK7_CANDIDATE_STATE_FIELDS,
            "Task-7 candidate state",
        )
        _task7_int(row["block_number"], "Task-7 state block", minimum=1)
        _task7_int(row["scenario_count"], "Task-7 state count", minimum=0)
        if (
            type(row["state"]) is not str
            or row["state"] not in {
                "resolved_nonpositive", "nonpublishable_positive", "selected",
                "not_needed_older_than_selected",
            }
            or type(row["transitions"]) is not list
            or not row["transitions"]
            or any(type(item) is not str or not item for item in row["transitions"])
        ):
            raise ValueError("Task-7 candidate state differs")
        if row["state"] == "selected":
            selected_states.append(row)
    if (
        len(selected_states) != 1
        or selected_states[0]["block_number"] != block["number"]
        or selected_states[0]["scenario_count"] != 10
    ):
        raise ValueError("Task-7 candidate closure differs")


def _validate_task7_candidate_manifest(
    value: Any, *, task7_selection: Mapping[str, Any],
) -> None:
    candidate = _exact_fields(
        value, _TASK7_CANDIDATE_MANIFEST_FIELDS,
        "Task-7 candidate manifest",
    )
    for field in (
        "staging_inventory_sha256", "prefilter_grid_digest",
        "candidate_block_count", "scenario_denominator",
        "initial_replay_required_count", "candidate_states",
    ):
        if candidate[field] != task7_selection[field]:
            raise ValueError("Task-7 candidate selection binding differs")
    scenarios = candidate["scenarios"]
    if (
        candidate["schema"]
        != "historical_foundry_candidate_manifest/v1"
        or type(scenarios) is not list
        or _task7_int(
            candidate["attempted_scenario_count"],
            "Task-7 attempted scenario count", minimum=0,
        ) != len(scenarios)
    ):
        raise ValueError("Task-7 candidate manifest identity differs")
    observed_counts = {}
    observed_keys = set()
    normalized = []
    for scenario in scenarios:
        row = _exact_fields(
            scenario, _TASK7_CANDIDATE_SCENARIO_FIELDS,
            "Task-7 candidate scenario",
        )
        block_number = _task7_int(
            row["block_number"], "Task-7 candidate block", minimum=1
        )
        status = _task7_int(
            row["status"], "Task-7 candidate status", minimum=0
        )
        scenario_key = row["scenario_key"]
        if (
            status not in {0, 1}
            or type(scenario_key) is not str
            or not scenario_key.startswith("{}:".format(block_number))
            or scenario_key in observed_keys
            or row["classification"]
            != ("replay_success" if status == 1 else "closed_revert")
            or _task7_int(
                row["gas_used"], "Task-7 candidate gas", minimum=0
            ) < 0
            or _task7_int(
                row["effective_gas_price"],
                "Task-7 candidate gas price", minimum=0,
            ) < 0
            or type(row["weth_delta_raw"]) is not int
            or (status == 0 and row["weth_delta_raw"] != 0)
        ):
            raise ValueError("Task-7 candidate scenario differs")
        observed_keys.add(scenario_key)
        observed_counts[block_number] = observed_counts.get(block_number, 0) + 1
        for field in (
            "overlay_sha256", "receipt_sha256", "trace_sha256",
            "result_sha256",
        ):
            _hash(row[field], "Task-7 candidate {}".format(field))
        if status == 0:
            if (
                row["proof_inputs_hash"] is not None
                or row["economics"] is not None
            ):
                raise ValueError("Task-7 reverted economics differ")
        else:
            _hash(
                row["proof_inputs_hash"],
                "Task-7 candidate proof_inputs_hash",
            )
            economics = _exact_fields(
                row["economics"], _TASK7_ECONOMICS_FIELDS,
                "Task-7 candidate economics",
            )
            for field in _TASK7_ECONOMICS_FIELDS:
                _validate_task7_fraction(
                    economics[field], "Task-7 candidate {}".format(field)
                )
        normalized.append(dict(row))
    states = task7_selection["candidate_states"]
    state_blocks = set()
    for state in states:
        block_number = state["block_number"]
        if (
            block_number in state_blocks
            or observed_counts.get(block_number, 0) != state["scenario_count"]
        ):
            raise ValueError("Task-7 candidate state inventory differs")
        state_blocks.add(block_number)
    if set(observed_counts) != {
        state["block_number"] for state in states
        if state["scenario_count"] != 0
    }:
        raise ValueError("Task-7 candidate block inventory differs")
    selected_block = task7_selection["selected_block"]["number"]
    selected_facts = [
        row for row in normalized if row["block_number"] == selected_block
    ]
    expected_selected_facts = [{
        key: item for key, item in row.items()
        if key not in {"direction", "requested_notional_usd"}
    } for row in task7_selection["selected_scenarios"]]
    if selected_facts != expected_selected_facts:
        raise ValueError("Task-7 selected candidate facts differ")


def _validate_task7_typed_manifest(
    value: Any, *, task7_selection: Mapping[str, Any],
    selected: Mapping[str, Any], typed_members: Any,
) -> None:
    manifest = _exact_fields(
        value, _TASK7_TYPED_MANIFEST_FIELDS, "Task-7 typed manifest"
    )
    if (
        manifest["schema"] != "historical_foundry_typed_manifest/v1"
        or manifest["selection_status"]
        != "found_publishable_profitable_block"
        or manifest["selected_block"] != task7_selection["selected_block"]
        or _task7_int(
            manifest["market_count"], "Task-7 market count", minimum=0
        ) != 2
        or _task7_int(
            manifest["member_count"], "Task-7 member count", minimum=0
        ) != 4
        or type(manifest["markets"]) is not list
        or len(manifest["markets"]) != 2
        or type(manifest["members"]) is not list
        or len(manifest["members"]) != 4
    ):
        raise ValueError("Task-7 typed manifest identity differs")
    expected_global_members = []
    for index, dex in enumerate(_VENUES):
        market = _exact_fields(
            manifest["markets"][index], _TASK7_MARKET_FIELDS,
            "Task-7 typed market",
        )
        expected_market = _market_id(selected, dex)
        expected_pair = _PAIR_BY_VENUE[dex]
        if (
            market["market_id"] != expected_market
            or market["market_key"] != _market_key(expected_market)
            or market["venue_id"] != dex
            or market["pair_address"] != expected_pair
            or market["factory_pair_forward"] != expected_pair
            or market["factory_pair_reverse"] != expected_pair
            or type(market["members"]) is not list
            or len(market["members"]) != 2
        ):
            raise ValueError("Task-7 typed market differs")
        for offset, role in enumerate((
            "dex_pool_state", "dex_usd_price_context",
        )):
            row = _exact_fields(
                market["members"][offset], _TASK7_MARKET_MEMBER_FIELDS,
                "Task-7 typed market member",
            )
            descriptor = _exact_fields(
                typed_members[index * 2 + offset]["descriptor"],
                _DESCRIPTOR_FIELDS, "typed member descriptor",
            )
            if (
                row["role"] != role
                or type(row["path"]) is not str
                or row["path"] != descriptor["path"]
                or _task7_int(
                    row["byte_count"], "Task-7 member byte count", minimum=1
                ) != descriptor["byte_count"]
                or _hash(row["sha256"], "Task-7 member hash")
                != descriptor["sha256"]
                or descriptor["market_id"] != expected_market
                or descriptor["role"] != role
            ):
                raise ValueError("Task-7 typed member differs")
            expected_global_members.append({
                "path": row["path"],
                "byte_count": row["byte_count"],
                "sha256": row["sha256"],
            })
    observed_global_members = []
    for item in manifest["members"]:
        row = _exact_fields(
            item, _TASK7_GLOBAL_MEMBER_FIELDS, "Task-7 global member"
        )
        if type(row["path"]) is not str or not row["path"]:
            raise ValueError("Task-7 global member path is invalid")
        _task7_int(
            row["byte_count"], "Task-7 global member byte count", minimum=1
        )
        _hash(row["sha256"], "Task-7 global member hash")
        observed_global_members.append(dict(row))
    if observed_global_members != sorted(
        expected_global_members, key=lambda item: item["path"]
    ):
        raise ValueError("Task-7 global member closure differs")


def _positive_decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, (bool, float)) or not isinstance(value, (Decimal, int, str)):
        raise ValueError("{} is invalid".format(label))
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("{} is invalid".format(label))
    if not result.is_finite() or result <= 0:
        raise ValueError("{} is invalid".format(label))
    return result


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("anchor timestamp is invalid")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("anchor timestamp is invalid")
    if (result.tzinfo is None or result.utcoffset() != timedelta(0)
            or result.microsecond != 0
            or value != result.astimezone(timezone.utc).isoformat(
                timespec="seconds"
            ).replace("+00:00", "Z")):
        raise ValueError("anchor timestamp is invalid")
    return result.astimezone(timezone.utc)


def _uint_text(value: Any, label: str, *, minimum: int = 0,
               maximum: Any = None) -> int:
    if (not isinstance(value, str)
            or re.fullmatch(r"0|[1-9][0-9]*", value) is None):
        raise ValueError("{} is invalid".format(label))
    result = int(value)
    if result < minimum or (maximum is not None and result > maximum):
        raise ValueError("{} is invalid".format(label))
    return result


def _market_id(selected: Mapping[str, Any], dex: str) -> str:
    return "dex:eth:{}:{}:UNI".format(
        dex, selected["venues"][dex]["pair_address"]
    )


def _market_key(market_id: str) -> str:
    return _typed_digest(
        "historical_foundry_market_key/v1", {"market_id": market_id}
    )


def _decode_typed_member(member: Any, *, label: str):
    _exact_fields(member, frozenset(("descriptor", "payload_hex")), label)
    descriptor = _exact_fields(
        member["descriptor"], _DESCRIPTOR_FIELDS, "{} descriptor".format(label)
    )
    payload_hex = member["payload_hex"]
    if (not isinstance(payload_hex, str) or not payload_hex
            or re.fullmatch(r"(?:[0-9a-f]{2})+", payload_hex) is None):
        raise ValueError("{} bytes are invalid".format(label))
    try:
        payload_bytes = bytes.fromhex(payload_hex)
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("{} bytes are invalid".format(label))
    if payload_hex != payload_bytes.hex() or _canonical_bytes(payload) != payload_bytes:
        raise ValueError("{} payload is not canonical".format(label))
    return descriptor, payload_bytes, payload


def _validate_typed_member(member: Any, *, config: HistoricalFoundryConfigSet,
                           dex: str, selected: Mapping[str, Any]) -> Mapping[str, Any]:
    descriptor, payload_bytes, payload = _decode_typed_member(
        member, label="typed member"
    )
    pair = selected["venues"][dex]["pair_address"]
    expected_market = _market_id(selected, dex)
    _exact_fields(payload, _PAYLOAD_FIELDS, "typed payload")
    venue = selected["venues"][dex]
    fee_identity = {
        "schema": "historical_foundry_v2_fee_identity/v1",
        "authority_sha256": config.authority.physical_sha256,
        "venue_id": dex,
        "fee_numerator": 997,
        "fee_denominator": 1000,
        "fee_bps": 30,
    }
    fee_proof = _typed_digest(
        "historical_foundry_v2_fee_identity/v1", fee_identity
    )
    expected = {
        "schema": "route_v2_pool_state/v1", "chain": "eth", "chain_id": "1",
        "dex": dex, "pool_address": pair, "token0_address": _UNI,
        "token1_address": _WETH, "token0_decimals": "18", "token1_decimals": "18",
        "reserve0_raw": str(venue["reserve_uni_raw"]),
        "reserve1_raw": str(venue["reserve_weth_raw"]), "fee_bps": "30",
        "fee_numerator": "997", "fee_denominator": "1000",
        "fee_formula": V2_FEE_FORMULA, "fee_proof_sha256": fee_proof,
        "reserve_timestamp_last_raw": str(venue["reserve_timestamp_last_raw"]),
        "raw_response_sha256": venue["raw_response_sha256"],
        "block_number": str(selected["block_number"]),
        "block_hash": selected["block_hash"],
        "block_header_sha256": selected["block_header_sha256"],
        "observed_at": selected["block_timestamp"],
    }
    for field, expected_value in expected.items():
        if payload.get(field) != expected_value:
            raise ValueError("typed payload {} differs".format(field))
    integer_fields = {
        "chain_id", "token0_decimals", "token1_decimals", "reserve0_raw",
        "reserve1_raw", "reserve_timestamp_last_raw", "fee_bps",
        "fee_numerator", "fee_denominator", "block_number",
    }
    constructor = {
        key: int(value) if key in integer_fields else value
        for key, value in payload.items() if key not in {"schema", "state_id"}
    }
    try:
        state = V2PoolState(**constructor)
    except (TypeError, ValueError) as error:
        raise ValueError("typed V2 state is invalid") from error
    physical = hashlib.sha256(payload_bytes).hexdigest()
    expected_descriptor = {
        "market_id": expected_market,
        "role": "dex_pool_state",
        "adapter_id": "route_quantity_quote_for_v2_pool/v1",
        "content_schema": "route_v2_pool_state/v1",
        "path": "typed/{}/dex_pool_state.json".format(
            _market_key(expected_market)
        ),
        "filename": "dex_pool_state.json",
        "byte_count": len(payload_bytes),
        "sha256": physical,
        "logical_generation": state.state_id.split(":", 1)[1],
    }
    if payload["state_id"] != state.state_id or descriptor != expected_descriptor:
        raise ValueError("typed V2 state binding differs")
    return payload


def _validate_usd_member(member: Any, *, config: HistoricalFoundryConfigSet,
                         dex: str, selected: Mapping[str, Any],
                         scan_inventory_sha256: str):
    descriptor, raw, payload = _decode_typed_member(member, label="USD member")
    _exact_fields(payload, _USD_PAYLOAD_FIELDS, "USD payload")
    market = _market_id(selected, dex)
    authority = config.authority.value["price_feed"]
    expected_identity = {
        "schema": "route_dex_usd_price_context/v1",
        "market_id": market,
        "venue_id": dex,
        "chain_id": "1",
        "block_number": str(selected["block_number"]),
        "block_hash": selected["block_hash"],
        "proxy_address": authority["proxy_address"],
        "decimals": str(authority["decimals"]),
        "scan_inventory_sha256": scan_inventory_sha256,
    }
    for field, expected in expected_identity.items():
        if payload[field] != expected:
            raise ValueError("USD context differs")
    round_id = _uint_text(payload["round_id"], "USD round", minimum=1,
                          maximum=(1 << 80) - 1)
    phase_id = _uint_text(payload["phase_id"], "USD phase", minimum=1)
    answer = _uint_text(payload["answer"], "USD answer", minimum=1)
    decimals = _uint_text(payload["decimals"], "USD decimals", maximum=255)
    started_at = _uint_text(payload["started_at"], "USD started_at", minimum=1)
    updated_at = _uint_text(payload["updated_at"], "USD updated_at", minimum=1)
    answered = _uint_text(payload["answered_in_round"], "USD answered round",
                          minimum=1, maximum=(1 << 80) - 1)
    valid_until = _uint_text(payload["valid_until"], "USD valid_until", minimum=1)
    block_time = _timestamp(selected["block_timestamp"])
    block_epoch = int(block_time.timestamp())
    max_age = config.policy.value["max_eth_usd_age_seconds"]
    if (phase_id != round_id >> 64 or round_id & ((1 << 64) - 1) == 0
            or answered < round_id or answered >> 64 != phase_id
            or answered & ((1 << 64) - 1) == 0
            or started_at > updated_at or updated_at > block_epoch
            or block_epoch - updated_at > max_age
            or valid_until != updated_at + max_age + 1):
        raise ValueError("USD round binding differs")
    eth_usd = Decimal((0, tuple(int(character) for character in str(answer)),
                       -decimals))
    physical = hashlib.sha256(raw).hexdigest()
    expected_descriptor = {
        "market_id": market,
        "role": "dex_usd_price_context",
        "adapter_id": "route_dex_usd_price_context/v1",
        "content_schema": "route_dex_usd_price_context/v1",
        "path": "typed/{}/dex_usd_price_context.json".format(
            _market_key(market)
        ),
        "filename": "dex_usd_price_context.json",
        "byte_count": len(raw),
        "sha256": physical,
        "logical_generation": physical,
    }
    if descriptor != expected_descriptor:
        raise ValueError("USD descriptor identity differs")
    return payload, eth_usd


def _market_projection(validated: Mapping[str, Any], dex: str) -> Dict[str, Any]:
    selected = validated["selection"]
    venue = selected["venues"][dex]
    eth_usd = _positive_decimal(validated["eth_usd"], "ETH/USD")
    reserve_uni = Decimal(venue["reserve_uni_raw"])
    reserve_weth = Decimal(venue["reserve_weth_raw"])
    with localcontext() as context:
        context.prec = 28
        context.rounding = ROUND_HALF_EVEN
        uni_usd = reserve_weth / reserve_uni * eth_usd
        amounts = v2_band_amounts(reserve_uni, reserve_weth, Decimal(30), 100)
        fields = depth_fields(target_position_index=0, token0_decimals=18,
                              token1_decimals=18, token0_price=uni_usd,
                              token1_price=eth_usd, band_amounts={100: {
                                  "zero_input": amounts["zero_for_one_gross_input"],
                                  "zero_output": amounts["zero_for_one_output"],
                                  "one_input": amounts["one_for_zero_gross_input"],
                                  "one_output": amounts["one_for_zero_output"],
                                  "zero_complete": True, "one_complete": True}})
        tvl = reserve_uni / Decimal(10 ** 18) * uni_usd + reserve_weth / Decimal(10 ** 18) * eth_usd
    route_ids = {route["route_id"] for route in selected["routes"]}
    capacity = max(s["requested_notional_usd"] for s in validated["scenarios"]
                   if s["receipt_status"] == 1 and s["route_id"] in route_ids)
    return {
        "market_id": "dex:eth:{}:{}:UNI".format(dex, venue["pair_address"]),
        "market_type": "dex", "token_symbol": "UNI", "chain": "eth",
        "dex": dex, "pool_address": venue["pair_address"], "token0_address": _UNI,
        "token1_address": _WETH, "reserve0_raw": str(venue["reserve_uni_raw"]),
        "reserve1_raw": str(venue["reserve_weth_raw"]),
        "sell_depth_100bps_usd": fields["sell_depth_100bps_usd"],
        "buy_depth_100bps_usd": fields["buy_depth_100bps_usd"],
        "observed_100bps_depth_usd": fields["total_depth_100bps_usd"],
        "depth_100bps_complete": fields["depth_100bps_complete"],
        "depth_method": "fixed_block_pool_state_marginal_price_band",
        "dex_tvl_usd": decimal_text(tvl),
        "dex_24h_usd": None, "route_volume_usd": None,
        "proved_execution_capacity_usd": str(capacity),
        "execution_capability": "research_only",
    }


def build_historical_research_universe(*, config: HistoricalFoundryConfigSet,
                                       validated_run: Mapping[str, Any]) -> Mapping[str, Any]:
    if type(config) is not HistoricalFoundryConfigSet:
        raise ValueError("validated run/config binding differs")
    validated = _validated_run_projection(validated_run)
    if any(
        validated["{}_sha256".format(role)]
        != getattr(config, role).physical_sha256
        for role in ("policy", "authority", "toolchain")
    ):
        raise ValueError("validated run/config binding differs")
    anchor = _timestamp(validated["selection"]["anchor_timestamp"]).date()
    result = {
        "schema": "historical_research_universe/v1",
        "temporal_scope": "historical_replay", "execution_claim": _EXECUTION_CLAIM,
        "run_id": validated["run_id"],
        "selection_sha256": validated["selection_sha256"],
        "provenance_window": {"start_date": str(anchor - timedelta(days=29)),
                              "end_date": str(anchor), "calendar_days": 30,
                              "measured_volume_coverage": False},
        "markets": [_market_projection(validated, dex) for dex in _VENUES],
        "routes": _plain(validated["selection"]["routes"]),
    }
    return _freeze(result)


def build_historical_core_projection(*, config: HistoricalFoundryConfigSet,
                                     validated_run: Mapping[str, Any],
                                     universe: Mapping[str, Any]) -> Mapping[str, Any]:
    validated = _validated_run_projection(validated_run)
    rebuilt = build_historical_research_universe(config=config, validated_run=validated_run)
    if _canonical_bytes(rebuilt) != _canonical_bytes(universe):
        raise ValueError("historical universe binding differs")
    return _freeze({
        "schema": "historical_core_projection/v1",
        "temporal_scope": "historical_replay", "execution_claim": _EXECUTION_CLAIM,
        "run_id": validated["run_id"], "universe_sha256": _digest(universe),
        "markets": _plain(universe["markets"]), "routes": _plain(universe["routes"]),
        "typed_members": _plain(validated["typed_members"]),
    })


def _initialize_validated_historical_scenario_capability():
    issuer = object()
    registry = {}

    @dataclass(frozen=True, init=False, repr=False)
    class ValidatedHistoricalScenarioInputs:
        scenario_key: str
        context_projection_sha256: str
        source_descriptor_set_sha256: str
        proof_inputs_hash: str
        canonical_projection_bytes: bytes

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise ValueError(
                "validated historical scenario construction is private"
            )

        def __repr__(self) -> str:
            return "ValidatedHistoricalScenarioInputs(<redacted>)"

        def __reduce_ex__(self, _protocol: int) -> Any:
            raise TypeError(
                "validated historical scenario is not serializable"
            )

    def require(value: Any) -> Mapping[str, Any]:
        entry = registry.get(id(value))
        if (
            type(value) is not ValidatedHistoricalScenarioInputs
            or entry is None
            or entry[0]() is not value
            or entry[1].get("issuer") is not issuer
        ):
            raise ValueError(
                "validated historical scenario capability is invalid"
            )
        record = entry[1]
        for field in (
            "scenario_key", "context_projection_sha256",
            "source_descriptor_set_sha256", "proof_inputs_hash",
            "canonical_projection_bytes",
        ):
            try:
                current = object.__getattribute__(value, field)
            except AttributeError as error:
                raise ValueError(
                    "validated historical scenario capability is invalid"
                ) from error
            if current != record[field]:
                raise ValueError(
                    "validated historical scenario capability is invalid"
                )
        projection_bytes = record["canonical_projection_bytes"]
        if (
            type(projection_bytes) is not bytes
            or hashlib.sha256(projection_bytes).hexdigest()
            != record["projection_sha256"]
        ):
            raise ValueError(
                "validated historical scenario projection differs"
            )
        try:
            projection = json.loads(projection_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                "validated historical scenario projection is invalid"
            ) from error
        if (
            type(projection) is not dict
            or _canonical_bytes(projection) != projection_bytes
            or projection.get("schema")
            != "historical_foundry_scenario_inputs/v1"
            or projection.get("scenario_key") != record["scenario_key"]
            or projection.get("context_projection_sha256")
            != record["context_projection_sha256"]
            or projection.get("source_descriptor_set_sha256")
            != record["source_descriptor_set_sha256"]
            or projection.get("proof_inputs_hash")
            != record["proof_inputs_hash"]
        ):
            raise ValueError(
                "validated historical scenario projection binding differs"
            )
        return projection

    def issue(
        *, scenario_key: str, context_projection_sha256: str,
        source_descriptor_set_sha256: str, proof_inputs_hash: str,
        canonical_projection_bytes: bytes,
    ) -> Any:
        values = (
            scenario_key, context_projection_sha256,
            source_descriptor_set_sha256, proof_inputs_hash,
        )
        if (
            type(scenario_key) is not str or not scenario_key
            or any(type(value) is not str for value in values)
            or any(
                _SHA.fullmatch(value) is None
                for value in values[1:]
            )
            or type(canonical_projection_bytes) is not bytes
        ):
            raise ValueError("historical scenario issue input is invalid")
        try:
            projection = json.loads(canonical_projection_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("historical scenario projection is invalid") from error
        if (
            type(projection) is not dict
            or _canonical_bytes(projection) != canonical_projection_bytes
            or projection.get("schema")
            != "historical_foundry_scenario_inputs/v1"
            or projection.get("scenario_key") != scenario_key
            or projection.get("context_projection_sha256")
            != context_projection_sha256
            or projection.get("source_descriptor_set_sha256")
            != source_descriptor_set_sha256
            or projection.get("proof_inputs_hash") != proof_inputs_hash
        ):
            raise ValueError("historical scenario projection differs")
        value = object.__new__(ValidatedHistoricalScenarioInputs)
        fields = {
            "scenario_key": scenario_key,
            "context_projection_sha256": context_projection_sha256,
            "source_descriptor_set_sha256": source_descriptor_set_sha256,
            "proof_inputs_hash": proof_inputs_hash,
            "canonical_projection_bytes": canonical_projection_bytes,
        }
        for field, field_value in fields.items():
            object.__setattr__(value, field, field_value)
        value_id = id(value)
        record = {
            "issuer": issuer,
            **fields,
            "projection_sha256": hashlib.sha256(
                canonical_projection_bytes
            ).hexdigest(),
        }

        def retire(reference: weakref.ReferenceType) -> None:
            current = registry.get(value_id)
            if current is not None and current[0] is reference:
                registry.pop(value_id, None)

        registry[value_id] = (weakref.ref(value, retire), record)
        return value

    installed = [False]

    def bind_publication(installer: Any) -> None:
        publication = sys.modules.get(
            "scripts.historical_route_publication"
        )
        if (
            installed[0]
            or publication is None
            or installer is not getattr(
                publication,
                "_install_historical_scenario_capability",
                None,
            )
        ):
            raise ValueError(
                "historical scenario publication installer is invalid"
            )
        installer(
            capability_type=ValidatedHistoricalScenarioInputs,
            raw_mint=issue,
            require_projection=require,
        )
        installed[0] = True
        globals().pop(
            "_bind_historical_scenario_capability_to_publication", None
        )

    return ValidatedHistoricalScenarioInputs, bind_publication, require


(
    ValidatedHistoricalScenarioInputs,
    _bind_historical_scenario_capability_to_publication,
    _validated_historical_scenario_projection,
) = _initialize_validated_historical_scenario_capability()
del _initialize_validated_historical_scenario_capability


def _exact_fraction_decimal(value: Fraction) -> str:
    if not isinstance(value, Fraction):
        raise ValueError("historical exact fraction is invalid")
    sign = "-" if value < 0 else ""
    numerator = abs(value.numerator)
    denominator = value.denominator
    integer, remainder = divmod(numerator, denominator)
    if remainder == 0:
        return sign + str(integer)
    digits = []
    while remainder and len(digits) <= 4_096:
        remainder *= 10
        digit, remainder = divmod(remainder, denominator)
        digits.append(str(digit))
    if remainder:
        raise ValueError("historical exact fraction is nonterminating")
    return "{}{}.{}".format(
        sign, integer, "".join(digits).rstrip("0")
    )


def _rounded_fraction_text(value: Fraction, places: int = 8) -> str:
    sign = -1 if value < 0 else 1
    absolute = abs(value)
    scale = 10 ** places
    quotient, remainder = divmod(
        absolute.numerator * scale, absolute.denominator
    )
    doubled = remainder * 2
    if doubled > absolute.denominator or (
        doubled == absolute.denominator and quotient % 2
    ):
        quotient += 1
    quotient *= sign
    if quotient == 0:
        return "0"
    negative = quotient < 0
    digits = str(abs(quotient)).rjust(places + 1, "0")
    text = digits[:-places] + "." + digits[-places:]
    text = text.rstrip("0").rstrip(".")
    return ("-" if negative else "") + text


def _historical_ratio_fields(
    edge: Fraction, buy_cost: Fraction,
) -> Tuple[str, str, str]:
    if buy_cost <= 0:
        raise ValueError("historical buy cost is invalid")
    ratio = edge * 10_000 / buy_cost
    return (
        _rounded_fraction_text(ratio),
        str(ratio.numerator),
        str(ratio.denominator),
    )


def _run_historical_v2_exact_input_kat() -> None:
    first = v2_exact_input_amount_out_raw(
        reserve_in_raw=10, reserve_out_raw=10, amount_in_raw=4,
        fee_numerator=1, fee_denominator=1,
    )
    reverse = v2_exact_input_amount_out_raw(
        reserve_in_raw=10, reserve_out_raw=10, amount_in_raw=4,
        fee_numerator=1, fee_denominator=1,
    )
    second = v2_exact_input_amount_out_raw(
        reserve_in_raw=10, reserve_out_raw=20, amount_in_raw=2,
        fee_numerator=1, fee_denominator=1,
    )
    legacy_inverse = (10 * 2 + (10 - 2) - 1) // (10 - 2)
    if (first, reverse, second, legacy_inverse) != (2, 2, 3, 3):
        raise ValueError("historical V2 exact-input KAT differs")


def _historical_swap_calldata_sha256(
    *, amount_in_raw: int, path: Tuple[str, str],
    recipient: str, deadline: int,
) -> str:
    def word(value: int) -> bytes:
        if type(value) is not int or not 0 <= value < 2 ** 256:
            raise ValueError("historical router call is invalid")
        return value.to_bytes(32, "big")

    try:
        raw = bytes.fromhex("38ed1739") + b"".join((
            word(amount_in_raw), word(0), word(160),
            b"\0" * 12 + bytes.fromhex(recipient[2:]),
            word(deadline), word(2),
            b"\0" * 12 + bytes.fromhex(path[0][2:]),
            b"\0" * 12 + bytes.fromhex(path[1][2:]),
        ))
    except (TypeError, ValueError):
        raise ValueError("historical router call is invalid") from None
    return hashlib.sha256(raw).hexdigest()


def _historical_expected_successful_calls(
    *, direction: str, first_weth_in: int, first_uni_out: int,
    executor: str, deadline: int,
) -> list:
    routers = (
        (_UNISWAP_ROUTER, _SUSHISWAP_ROUTER)
        if direction == "uniswap_to_sushiswap"
        else (_SUSHISWAP_ROUTER, _UNISWAP_ROUTER)
    )
    specs = (
        ([2], "first_leg", routers[0], first_weth_in, (_WETH, _UNI)),
        ([5], "second_leg", routers[1], first_uni_out, (_UNI, _WETH)),
    )
    return [{
        "call_path": call_path,
        "leg": leg,
        "router": router,
        "calldata_sha256": _historical_swap_calldata_sha256(
            amount_in_raw=amount, path=path,
            recipient=executor, deadline=deadline,
        ),
        "amount_in_raw": amount,
        "amount_out_min_raw": 0,
        "path": list(path),
        "recipient": executor,
        "deadline": deadline,
        "value": 0,
    } for call_path, leg, router, amount, path in specs]


def build_historical_atomic_v2_cashflow(
    inputs: ValidatedHistoricalScenarioInputs,
) -> Mapping[str, Any]:
    projection = _validated_historical_scenario_projection(inputs)
    _run_historical_v2_exact_input_kat()
    selection = projection.get("selection_scenario")
    result = projection.get("result")
    receipt = projection.get("receipt")
    trace = projection.get("trace")
    overlay = projection.get("overlay")
    prefilter = projection.get("prefilter_scenario")
    fee = projection.get("fee")
    policy_fees = projection.get("policy_fees")
    pools = projection.get("pools")
    route = projection.get("route")
    price = projection.get("price")
    formula = projection.get("v2_formula")
    if any(type(value) is not dict for value in (
        selection, result, receipt, trace, overlay, pools, route, price,
        formula, prefilter, fee, policy_fees,
    )):
        raise ValueError("historical scenario arithmetic projection is invalid")
    direction = selection.get("direction")
    if direction == "uniswap_to_sushiswap":
        buy_venue, sell_venue = "uniswap_v2", "sushiswap_v2"
    elif direction == "sushiswap_to_uniswap":
        buy_venue, sell_venue = "sushiswap_v2", "uniswap_v2"
    else:
        raise ValueError("historical scenario direction is invalid")
    buy_pool = pools.get(buy_venue)
    sell_pool = pools.get(sell_venue)
    if type(buy_pool) is not dict or type(sell_pool) is not dict:
        raise ValueError("historical scenario pool projection is invalid")
    proof_authority = result.get("proof_authority")
    balances = result.get("balances")
    deltas = result.get("actual_deltas")
    trace_balances = trace.get("balances")
    trace_deltas = trace.get("actual_deltas")
    transaction = overlay.get("transaction")
    synthetic = overlay.get("synthetic_block")
    if any(type(value) is not dict for value in (
        proof_authority, balances, deltas, trace_balances, trace_deltas,
        transaction, synthetic,
    )):
        raise ValueError("historical execution closure is invalid")
    try:
        weth_account = overlay["accounts"][_WETH]
        first_weth_in = weth_account["balance_delta"]
        fee_numerator = formula["fee_numerator"]
        fee_denominator = formula["fee_denominator"]
        answer = int(price["answer"])
        feed_decimals = int(price["decimals"])
        requested_notional = selection["requested_notional_usd"]
        first_uni_out = v2_exact_input_amount_out_raw(
            reserve_in_raw=int(buy_pool["reserve1_raw"]),
            reserve_out_raw=int(buy_pool["reserve0_raw"]),
            amount_in_raw=first_weth_in,
            fee_numerator=fee_numerator,
            fee_denominator=fee_denominator,
        )
        second_uni_in = first_uni_out
        final_weth_out = v2_exact_input_amount_out_raw(
            reserve_in_raw=int(sell_pool["reserve0_raw"]),
            reserve_out_raw=int(sell_pool["reserve1_raw"]),
            amount_in_raw=second_uni_in,
            fee_numerator=fee_numerator,
            fee_denominator=fee_denominator,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("historical exact-input arithmetic is invalid") from error
    expected_weth_in = (
        requested_notional * 10 ** (18 + feed_decimals) // answer
    )
    if (
        type(first_weth_in) is not int or first_weth_in <= 0
        or type(requested_notional) is not int or requested_notional <= 0
    ):
        raise ValueError("historical exact-input arithmetic is invalid")
    direction_index = {
        "uniswap_to_sushiswap": 0,
        "sushiswap_to_uniswap": 1,
    }[direction]
    try:
        calldata_text = transaction["input"]
        if (
            type(calldata_text) is not str
            or not calldata_text.startswith("0x")
        ):
            raise ValueError
        calldata = bytes.fromhex(calldata_text[2:])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            "historical transaction calldata is invalid"
        ) from error
    expected_calldata = b"\x64\xcc\x5e\xae" + (
        direction_index.to_bytes(32, "big")
        + first_weth_in.to_bytes(32, "big")
    )
    try:
        expected_effective_gas_price = (
            fee["next_base_fee_per_gas"]
            + fee["p50_priority_fee_per_gas"]
        )
        expected_max_fee_per_gas = (
            policy_fees["max_fee_multiplier"]
            * prefilter["child_base_fee_wei"]
            + fee["p50_priority_fee_per_gas"]
        )
    except (KeyError, TypeError) as error:
        raise ValueError(
            "historical p50 gas envelope is invalid"
        ) from error
    if (
        calldata != expected_calldata
        or transaction.get("calldata_sha256")
        != hashlib.sha256(calldata).hexdigest()
        or transaction.get("value") != 0
        or receipt.get("effectiveGasPrice")
        != expected_effective_gas_price
        or receipt.get("maxPriorityFeePerGas")
        != fee.get("p50_priority_fee_per_gas")
        or transaction.get("maxPriorityFeePerGas")
        != fee.get("p50_priority_fee_per_gas")
        or receipt.get("maxFeePerGas") != expected_max_fee_per_gas
        or transaction.get("maxFeePerGas") != expected_max_fee_per_gas
        or synthetic.get("base_fee_per_gas")
        != prefilter.get("child_base_fee_wei")
        or fee.get("next_base_fee_per_gas")
        != prefilter.get("child_base_fee_wei")
    ):
        raise ValueError("historical p50 gas envelope differs")
    economics = selection.get("economics")
    if type(economics) is not dict or set(economics) != {
        "gross_edge_usd", "gas_cost_usd", "mev_buffer_usd",
        "policy_net_edge_usd",
    }:
        raise ValueError("historical scenario economics are invalid")

    def proved_fraction(name: str) -> Fraction:
        value = economics.get(name)
        if (
            type(value) is not dict
            or set(value) != {"numerator", "denominator", "display"}
            or type(value.get("numerator")) is not int
            or type(value.get("denominator")) is not int
            or value["denominator"] <= 0
            or type(value.get("display")) is not str
        ):
            raise ValueError("historical scenario economics are invalid")
        number = Fraction(value["numerator"], value["denominator"])
        if value["display"] != _exact_fraction_decimal(number):
            raise ValueError("historical scenario economics differ")
        return number

    gross_usd = Fraction(
        (final_weth_out - first_weth_in) * answer,
        10 ** (18 + feed_decimals),
    )
    gas_usd = Fraction(
        receipt["gasUsed"] * receipt["effectiveGasPrice"] * answer,
        10 ** (18 + feed_decimals),
    )
    mev_usd = Fraction(requested_notional * 10, 10_000)
    if (
        proved_fraction("gross_edge_usd") != gross_usd
        or proved_fraction("gas_cost_usd") != gas_usd
        or proved_fraction("mev_buffer_usd") != mev_usd
        or proved_fraction("policy_net_edge_usd")
        != gross_usd - gas_usd - mev_usd
    ):
        raise ValueError("historical scenario economics differ")
    scenario_key = projection["scenario_key"]
    expected_route_id = route.get("route_id")
    expected_balances = {
        "initial_weth_raw": first_weth_in,
        "initial_uni_raw": 0,
        "final_weth_raw": final_weth_out,
        "final_uni_raw": 0,
    }
    expected_deltas = {
        "first_leg_uni_raw": first_uni_out,
        "weth_raw": final_weth_out - first_weth_in,
        "residual_uni_raw": 0,
    }
    try:
        expected_calls = _historical_expected_successful_calls(
            direction=direction,
            first_weth_in=first_weth_in,
            first_uni_out=first_uni_out,
            executor=transaction["to"],
            deadline=synthetic["timestamp"] + 60,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            "historical successful router calls are invalid"
        ) from error
    baseline_pairs = result.get("pair_closure")
    trace_pairs = trace.get("pair_closure")
    if (
        type(baseline_pairs) is not dict
        or trace_pairs != baseline_pairs
        or set(baseline_pairs) != {"uniswap_v2", "sushiswap_v2"}
    ):
        raise ValueError("historical pair baseline differs")
    expected_post_pairs = {}
    for venue in ("uniswap_v2", "sushiswap_v2"):
        baseline = baseline_pairs.get(venue)
        pool = pools.get(venue)
        if (
            type(baseline) is not dict
            or type(pool) is not dict
            or set(baseline) != {
                "pair_address", "reserve_uni_raw", "reserve_weth_raw",
                "pair_uni_balance_raw", "pair_weth_balance_raw",
            }
            or baseline.get("pair_address") != pool.get("pool_address")
            or baseline.get("reserve_uni_raw") != int(pool["reserve0_raw"])
            or baseline.get("reserve_weth_raw") != int(pool["reserve1_raw"])
            or any(
                type(baseline.get(field)) is not int
                or baseline[field] < 0
                for field in (
                    "reserve_uni_raw", "reserve_weth_raw",
                    "pair_uni_balance_raw", "pair_weth_balance_raw",
                )
            )
        ):
            raise ValueError("historical pair baseline differs")
        expected_post_pairs[venue] = dict(baseline)
    expected_post_pairs[buy_venue]["reserve_weth_raw"] += first_weth_in
    expected_post_pairs[buy_venue]["pair_weth_balance_raw"] += first_weth_in
    expected_post_pairs[buy_venue]["reserve_uni_raw"] -= first_uni_out
    expected_post_pairs[buy_venue]["pair_uni_balance_raw"] -= first_uni_out
    expected_post_pairs[sell_venue]["reserve_uni_raw"] += second_uni_in
    expected_post_pairs[sell_venue]["pair_uni_balance_raw"] += second_uni_in
    expected_post_pairs[sell_venue]["reserve_weth_raw"] -= final_weth_out
    expected_post_pairs[sell_venue]["pair_weth_balance_raw"] -= final_weth_out
    if any(
        value < 0
        for row in expected_post_pairs.values()
        for field, value in row.items()
        if field != "pair_address"
    ):
        raise ValueError("historical pair transition is invalid")
    if (
        first_weth_in != expected_weth_in
        or projection.get("proof_inputs_hash")
        != result.get("cost_proof_inputs", {}).get("proof_inputs_hash")
        or proof_authority.get("amount_weth_in_wei") != first_weth_in
        or proof_authority.get("actual_first_leg_uni_raw") != first_uni_out
        or proof_authority.get("direction") != direction
        or proof_authority.get("requested_notional_usd")
        != requested_notional
        or balances != expected_balances
        or trace_balances != expected_balances
        or deltas != expected_deltas
        or trace_deltas != expected_deltas
        or prefilter.get("scenario_key") != scenario_key
        or prefilter.get("direction") != direction
        or prefilter.get("requested_notional_usd") != requested_notional
        or prefilter.get("amount_weth_in_wei") != first_weth_in
        or prefilter.get("first_amount_out_raw") != first_uni_out
        or prefilter.get("second_amount_out_raw") != final_weth_out
        or prefilter.get("gross_profit_weth_wei")
        != final_weth_out - first_weth_in
        or trace.get("successful_calls") != expected_calls
        or result.get("trace_closure", {}).get("successful_calls")
        != expected_calls
        or trace.get("post_pair_state") != expected_post_pairs
        or result.get("post_pair_state") != expected_post_pairs
        or selection.get("weth_delta_raw")
        != final_weth_out - first_weth_in
        or selection.get("scenario_key") != scenario_key
        or receipt.get("scenario_key") != scenario_key
        or receipt.get("status") != 1
        or result.get("scenario_key") != scenario_key
        or result.get("status") != 1
        or trace.get("scenario_key") != scenario_key
        or trace.get("failed") is not False
        or trace.get("gasprice_opcode_addresses") != []
        or projection.get("route_id") != expected_route_id
        or route.get("route_mode") != "atomic_onchain"
    ):
        raise ValueError("historical exact-input evidence differs")
    return _freeze({
        "schema": "historical_atomic_v2_cashflow/v1",
        "scenario_key": scenario_key,
        "direction": direction,
        "route_id": expected_route_id,
        "requested_notional_usd": requested_notional,
        "buy_venue": buy_venue,
        "sell_venue": sell_venue,
        "first_weth_in_raw": first_weth_in,
        "first_uni_out_raw": first_uni_out,
        "second_uni_in_raw": second_uni_in,
        "final_weth_out_raw": final_weth_out,
        "gross_edge_weth_raw": final_weth_out - first_weth_in,
        "buy_post_reserve_weth_raw": int(buy_pool["reserve1_raw"])
        + first_weth_in,
        "buy_post_reserve_uni_raw": int(buy_pool["reserve0_raw"])
        - first_uni_out,
        "sell_post_reserve_uni_raw": int(sell_pool["reserve0_raw"])
        + second_uni_in,
        "sell_post_reserve_weth_raw": int(sell_pool["reserve1_raw"])
        - final_weth_out,
    })


def _historical_cost_rows(
    projection: Mapping[str, Any], cashflow: Mapping[str, Any],
) -> Tuple[Mapping[str, Any], ...]:
    route = projection["route"]
    proof = projection["proof_inputs"]
    requested = projection["selection_scenario"]["requested_notional_usd"]
    opportunity_id = route_opportunity_id(route["route_id"], requested)
    target = _exact_fraction_decimal(Fraction(
        cashflow["first_uni_out_raw"], 10 ** 18
    ))
    rows = []
    for proof_row in proof["rows"]:
        leg = proof_row["grain"]
        rows.append({
            "contract_version": COST_COMPONENT_CONTRACT_VERSION,
            "cohort_id": projection["cohort_id"],
            "opportunity_id": opportunity_id,
            "leg": leg,
            "market_id": (
                route["buy_market_id"] if leg == "buy"
                else route["sell_market_id"] if leg == "sell" else ""
            ),
            "direction": (
                "buy_token" if leg == "buy"
                else "sell_token" if leg == "sell" else "route"
            ),
            "requested_notional_usd": str(requested),
            "target_token_quantity": target,
            "component_type": proof_row["component"],
            "value_status": proof_row["value_status"],
            "amount_usd": proof_row["amount_usd_exact"],
            "rate_bps": proof_row["rate_bps_exact"],
            "basis": "historical_foundry_exact_proof_input",
            "strict_eligible": False,
            "embedded_in_leg_quote": proof_row["embedded"],
            "observed_at": None,
            "valid_until": None,
            "source": proof_row["proof_role"],
            "source_record_sha256": proof_row["proof_sha256"],
            "reason_code": None,
        })
    return tuple(_freeze(row) for row in rows)


def build_historical_route_opportunity(
    inputs: ValidatedHistoricalScenarioInputs,
) -> Mapping[str, Any]:
    projection = _validated_historical_scenario_projection(inputs)
    cashflow = build_historical_atomic_v2_cashflow(inputs)
    route = projection["route"]
    pools = projection["pools"]
    price = projection["price"]
    selection = projection["selection_scenario"]
    answer = int(price["answer"])
    decimals = int(price["decimals"])
    usd_denominator = 10 ** (18 + decimals)
    buy_cost = Fraction(
        cashflow["first_weth_in_raw"] * answer, usd_denominator
    )
    sell_proceeds = Fraction(
        cashflow["final_weth_out_raw"] * answer, usd_denominator
    )
    gross_edge = sell_proceeds - buy_cost
    rows = _historical_cost_rows(projection, cashflow)
    bounded_cost = sum(
        (
            Fraction(Decimal(row["amount_usd"]))
            for row in rows
            if row["value_status"] == "bounded_estimate"
            and row["embedded_in_leg_quote"] is False
        ),
        Fraction(0),
    )
    assumed_cost = sum(
        (
            Fraction(Decimal(row["amount_usd"]))
            for row in rows if row["value_status"] == "assumed"
        ),
        Fraction(0),
    )
    strict_cost = Fraction(0)
    strict_net = gross_edge - strict_cost
    research_net = gross_edge - strict_cost - bounded_cost - assumed_cost
    gross_bps = _historical_ratio_fields(gross_edge, buy_cost)
    strict_bps = _historical_ratio_fields(strict_net, buy_cost)
    research_bps = _historical_ratio_fields(research_net, buy_cost)
    opportunity_id = route_opportunity_id(
        route["route_id"], selection["requested_notional_usd"]
    )
    target = _exact_fraction_decimal(Fraction(
        cashflow["first_uni_out_raw"], 10 ** 18
    ))
    buy_pool = pools[cashflow["buy_venue"]]
    sell_pool = pools[cashflow["sell_venue"]]
    row = {
        "contract_version": ROUTE_OPPORTUNITY_CONTRACT_VERSION,
        "cohort_id": projection["cohort_id"],
        "route_id": route["route_id"],
        "opportunity_id": opportunity_id,
        "token_symbol": route["token_symbol"],
        "buy_market_id": route["buy_market_id"],
        "sell_market_id": route["sell_market_id"],
        "route_mode": route["route_mode"],
        "requested_notional_usd": str(selection["requested_notional_usd"]),
        "target_token_quantity": target,
        "target_base_raw": str(cashflow["first_uni_out_raw"]),
        "target_base_unit_decimals": 18,
        "target_lattice_raw": "1",
        "buy_state_id": buy_pool["state_id"],
        "sell_state_id": sell_pool["state_id"],
        "buy_state_observed_at": buy_pool["observed_at"],
        "sell_state_observed_at": sell_pool["observed_at"],
        "skew_seconds": "0",
        "route_age_seconds": "0",
        "gross_buy_cost_usd": _exact_fraction_decimal(buy_cost),
        "gross_sell_proceeds_usd": _exact_fraction_decimal(sell_proceeds),
        "gross_edge_usd": _exact_fraction_decimal(gross_edge),
        "gross_edge_bps": gross_bps[0],
        "gross_edge_bps_numerator": gross_bps[1],
        "gross_edge_bps_denominator": gross_bps[2],
        "strict_nonembedded_cost_usd": _exact_fraction_decimal(strict_cost),
        "research_bounded_cost_usd": _exact_fraction_decimal(bounded_cost),
        "research_assumed_cost_usd": _exact_fraction_decimal(assumed_cost),
        "strict_net_edge_usd": _exact_fraction_decimal(strict_net),
        "strict_net_edge_bps": strict_bps[0],
        "strict_net_edge_bps_numerator": strict_bps[1],
        "strict_net_edge_bps_denominator": strict_bps[2],
        "research_net_edge_usd": _exact_fraction_decimal(research_net),
        "research_net_edge_bps": research_bps[0],
        "research_net_edge_bps_numerator": research_bps[1],
        "research_net_edge_bps_denominator": research_bps[2],
        "edge_bps_denominator_basis": "gross_buy_cost_usd",
        "cost_completeness": "incomplete",
        "scenario_cost_completeness": "complete",
        "reflected_or_embedded_component_keys": [
            "buy:pool_swap_fee", "sell:pool_swap_fee",
        ],
        "component_reasons": ["cost_component_estimated"],
        "mode_evidence_eligible": True,
        "inventory_profile_hash": projection["proof_inputs_hash"],
        "maximum_proved_capacity_quantity": target,
        "opportunity_class": "research_estimate",
        "primary_reason": "cost_component_estimated",
        "reason_codes": ["cost_component_estimated"],
        "strict_eligible": False,
        "strict_ready_for_publication": False,
        "publication_attestation_sha256": None,
        "buy_usd_projection_sha256": projection["feed_sha256_by_venue"][
            cashflow["buy_venue"]
        ],
        "sell_usd_projection_sha256": projection["feed_sha256_by_venue"][
            cashflow["sell_venue"]
        ],
        "cost_component_set_sha256": _digest(rows),
        "mode_evidence_sha256": _typed_digest(
            "historical_atomic_mode_evidence/v1",
            {
                "scenario_key": projection["scenario_key"],
                "proof_inputs_hash": projection["proof_inputs_hash"],
            },
        ),
        "buy_core_manifest_sha256": projection["context_projection_sha256"],
        "sell_core_manifest_sha256": projection["context_projection_sha256"],
    }
    if set(row) != set(OPPORTUNITY_FIELDS) - {"evidence_binding_sha256"}:
        raise ValueError("historical opportunity schema differs")
    row["evidence_binding_sha256"] = _digest(row)
    fee = projection["fee"]
    policy_fees = projection["policy_fees"]
    receipt = projection["receipt"]
    if (
        type(fee) is not dict
        or any(type(fee.get(field)) is not int for field in (
            "next_base_fee_per_gas", "p50_priority_fee_per_gas",
            "p90_priority_fee_per_gas",
        ))
        or fee["p50_priority_fee_per_gas"]
        > fee["p90_priority_fee_per_gas"]
        or type(policy_fees) is not dict
        or policy_fees.get("acceptance_tip_percentile") != 50
        or policy_fees.get("stress_tip_percentile") != 90
        or policy_fees.get("acceptance_mev_bps") != "10"
        or policy_fees.get("stress_mev_bps") != ["25", "50"]
        or projection["trace"].get("gasprice_opcode_addresses") != []
    ):
        raise ValueError("historical stress policy projection differs")
    gas_used = receipt["gasUsed"]
    baseline_gas = Fraction(
        gas_used * receipt["effectiveGasPrice"] * answer,
        usd_denominator,
    )
    p90_effective_gas_price = (
        fee["next_base_fee_per_gas"]
        + fee["p90_priority_fee_per_gas"]
    )
    stress_gas = Fraction(
        gas_used * p90_effective_gas_price * answer,
        usd_denominator,
    )
    scenarios = []
    for name, percentile, mev_bps, gas_amount in (
        ("baseline_p50_mev_10bps", 50, 10, baseline_gas),
        ("stress_p90_mev_25bps", 90, 25, stress_gas),
        ("stress_p90_mev_50bps", 90, 50, stress_gas),
    ):
        mev_amount = Fraction(
            selection["requested_notional_usd"] * mev_bps, 10_000
        )
        scenario_net = gross_edge - gas_amount - mev_amount
        scenarios.append({
            "name": name,
            "priority_fee_percentile": percentile,
            "mev_bps": str(mev_bps),
            "gas_used": gas_used,
            "effective_gas_price": (
                receipt["effectiveGasPrice"]
                if percentile == 50 else p90_effective_gas_price
            ),
            "gas_cost_usd": _exact_fraction_decimal(gas_amount),
            "mev_buffer_usd": _exact_fraction_decimal(mev_amount),
            "research_net_edge_usd": _exact_fraction_decimal(scenario_net),
            "positive_research_net": scenario_net > 0,
        })
    return _freeze({
        "schema": "historical_route_opportunity_build/v1",
        "opportunity": row,
        "cost_components": list(rows),
        "economics_scenarios": scenarios,
    })


def build_historical_scenario_projection(
    inputs: ValidatedHistoricalScenarioInputs,
) -> bytes:
    projection = _validated_historical_scenario_projection(inputs)
    cashflow = build_historical_atomic_v2_cashflow(inputs)
    built = build_historical_route_opportunity(inputs)
    sell_pool = projection["pools"][cashflow["sell_venue"]]
    value = {
        "schema": "historical_foundry_replay_scenario/v1",
        "scenario_key": projection["scenario_key"],
        "route_id": projection["route_id"],
        "requested_notional_usd": projection["selection_scenario"][
            "requested_notional_usd"
        ],
        "receipt_status": projection["receipt"]["status"],
        "proof_inputs_hash": projection["proof_inputs_hash"],
        "overlay_sha256": projection["overlay_sha256"],
        "receipt_sha256": projection["receipt_sha256"],
        "trace_sha256": projection["trace_sha256"],
        "result_sha256": projection["result_sha256"],
        "gross_edge_weth_raw": cashflow["gross_edge_weth_raw"],
        "proof_inputs": _plain(projection["proof_inputs"]),
        "economics_authority": {
            "first_weth_in_raw": cashflow["first_weth_in_raw"],
            "first_uni_out_raw": cashflow["first_uni_out_raw"],
            "final_weth_out_raw": cashflow["final_weth_out_raw"],
            "eth_usd_answer": int(projection["price"]["answer"]),
            "feed_decimals": int(projection["price"]["decimals"]),
            "fee_numerator": projection["v2_formula"]["fee_numerator"],
            "fee_denominator": projection["v2_formula"]["fee_denominator"],
            "second_leg_reserve_uni_raw": int(sell_pool["reserve0_raw"]),
            "second_leg_reserve_weth_raw": int(sell_pool["reserve1_raw"]),
            "gas_used": projection["receipt"]["gasUsed"],
            "receipt_effective_gas_price": projection["receipt"][
                "effectiveGasPrice"
            ],
            "next_base_fee_per_gas": projection["fee"][
                "next_base_fee_per_gas"
            ],
            "p50_priority_fee_per_gas": projection["fee"][
                "p50_priority_fee_per_gas"
            ],
            "p90_priority_fee_per_gas": projection["fee"][
                "p90_priority_fee_per_gas"
            ],
        },
        "opportunity": _plain(built["opportunity"]),
        "cost_components": _plain(built["cost_components"]),
        "economics_scenarios": _plain(built["economics_scenarios"]),
    }
    value["scenario_projection_sha256"] = _digest(value)
    return _canonical_bytes(value)


def _historical_canonical_decimal_fraction(
    value: Any, label: str, *, nonnegative: bool = False,
    positive: bool = False,
) -> Fraction:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(label + " is invalid")
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(label + " is invalid") from error
    if (
        not number.is_finite()
        or nonnegative and number < 0
        or positive and number <= 0
    ):
        raise ValueError(label + " is invalid")
    fraction = Fraction(number)
    if value != _exact_fraction_decimal(fraction):
        raise ValueError(label + " is not canonical")
    return fraction


def _validate_historical_compact_scenario(
    *, value: Mapping[str, Any], expected_scenario_key: str,
    expected_direction: str, expected_notional: int,
) -> Mapping[str, Any]:
    expected_fields = {
        "schema", "scenario_key", "route_id", "requested_notional_usd",
        "receipt_status", "proof_inputs_hash", "overlay_sha256",
        "receipt_sha256", "trace_sha256", "result_sha256",
        "gross_edge_weth_raw", "proof_inputs", "economics_authority",
        "opportunity", "cost_components", "economics_scenarios",
        "scenario_projection_sha256",
    }
    if (
        type(value) is not dict
        or set(value) != expected_fields
        or value.get("schema")
        != "historical_foundry_replay_scenario/v1"
        or value.get("scenario_key") != expected_scenario_key
        or value.get("requested_notional_usd") != expected_notional
        or value.get("receipt_status") != 1
        or type(value.get("route_id")) is not str
        or not value["route_id"]
        or type(value.get("gross_edge_weth_raw")) is not int
    ):
        raise ValueError("historical replay scenario contract differs")
    unsigned = dict(value)
    binding = unsigned.pop("scenario_projection_sha256")
    if (
        _SHA.fullmatch(binding or "") is None
        or binding != _digest(unsigned)
        or any(
            _SHA.fullmatch(value.get(field) or "") is None
            for field in (
                "proof_inputs_hash", "overlay_sha256", "receipt_sha256",
                "trace_sha256", "result_sha256",
            )
        )
    ):
        raise ValueError("historical replay scenario binding differs")

    opportunity = value.get("opportunity")
    rows = value.get("cost_components")
    economics = value.get("economics_scenarios")
    proof_inputs = value.get("proof_inputs")
    economics_authority = value.get("economics_authority")
    if (
        type(opportunity) is not dict
        or set(opportunity) != set(OPPORTUNITY_FIELDS)
        or type(rows) is not list or len(rows) != 9
        or type(economics) is not list or len(economics) != 3
        or type(proof_inputs) is not dict
        or type(economics_authority) is not dict
    ):
        raise ValueError("historical replay scenario inventory differs")
    proof_fields = {
        "schema", "scenario_key", "policy_sha256", "receipt_sha256",
        "trace_sha256", "adapter_proof_sha256", "rows",
        "proof_inputs_hash",
    }
    proof_row_fields = {
        "grain", "component", "value_status", "embedded",
        "amount_usd_exact", "rate_bps_exact", "proof_role",
        "proof_sha256",
    }
    proof_rows = proof_inputs.get("rows")
    proof_unsigned = {
        key: item for key, item in proof_inputs.items()
        if key != "proof_inputs_hash"
    }
    if (
        set(proof_inputs) != proof_fields
        or proof_inputs.get("schema")
        != "historical_foundry_cost_proof_inputs/v1"
        or proof_inputs.get("scenario_key") != expected_scenario_key
        or proof_inputs.get("receipt_sha256") != value["receipt_sha256"]
        or proof_inputs.get("trace_sha256") != value["trace_sha256"]
        or _SHA.fullmatch(proof_inputs.get("policy_sha256") or "") is None
        or _SHA.fullmatch(proof_inputs.get("adapter_proof_sha256") or "")
        is None
        or type(proof_rows) is not list or len(proof_rows) != 9
        or any(
            type(row) is not dict or set(row) != proof_row_fields
            for row in proof_rows
        )
        or proof_inputs.get("proof_inputs_hash") != _typed_digest(
            "historical_foundry_cost_proof_inputs/v1", proof_unsigned
        )
        or value["proof_inputs_hash"]
        != proof_inputs.get("proof_inputs_hash")
    ):
        raise ValueError("historical compact cost proof differs")
    authority_fields = {
        "first_weth_in_raw", "first_uni_out_raw", "final_weth_out_raw",
        "eth_usd_answer", "feed_decimals", "fee_numerator",
        "fee_denominator", "second_leg_reserve_uni_raw",
        "second_leg_reserve_weth_raw", "gas_used",
        "receipt_effective_gas_price", "next_base_fee_per_gas",
        "p50_priority_fee_per_gas", "p90_priority_fee_per_gas",
    }
    if (
        set(economics_authority) != authority_fields
        or any(
            type(economics_authority.get(field)) is not int
            for field in authority_fields
        )
        or any(
            economics_authority[field] <= 0
            for field in (
                "first_weth_in_raw", "first_uni_out_raw",
                "final_weth_out_raw", "eth_usd_answer",
                "fee_numerator", "fee_denominator",
                "second_leg_reserve_uni_raw",
                "second_leg_reserve_weth_raw", "gas_used",
                "receipt_effective_gas_price",
            )
        )
        or not 0 <= economics_authority["feed_decimals"] <= 255
        or not 0 < economics_authority["fee_numerator"]
        < economics_authority["fee_denominator"]
        or (
            economics_authority["fee_denominator"]
            - economics_authority["fee_numerator"]
        ) * 10_000 != 30 * economics_authority["fee_denominator"]
        or economics_authority["next_base_fee_per_gas"] < 0
        or economics_authority["p50_priority_fee_per_gas"] < 0
        or economics_authority["p90_priority_fee_per_gas"]
        < economics_authority["p50_priority_fee_per_gas"]
        or economics_authority["receipt_effective_gas_price"] != (
            economics_authority["next_base_fee_per_gas"]
            + economics_authority["p50_priority_fee_per_gas"]
        )
        or value["gross_edge_weth_raw"] != (
            economics_authority["final_weth_out_raw"]
            - economics_authority["first_weth_in_raw"]
        )
    ):
        raise ValueError("historical economics authority differs")
    usd_denominator = 10 ** (18 + economics_authority["feed_decimals"])
    answer = economics_authority["eth_usd_answer"]
    fee_units = (
        economics_authority["fee_denominator"]
        - economics_authority["fee_numerator"]
    )
    authority_gross_buy = Fraction(
        economics_authority["first_weth_in_raw"] * answer,
        usd_denominator,
    )
    authority_gross_sell = Fraction(
        economics_authority["final_weth_out_raw"] * answer,
        usd_denominator,
    )
    authority_gross_edge = authority_gross_sell - authority_gross_buy
    authority_buy_pool_fee = Fraction(
        economics_authority["first_weth_in_raw"] * answer * fee_units,
        usd_denominator * economics_authority["fee_denominator"],
    )
    authority_sell_pool_fee = Fraction(
        economics_authority["first_uni_out_raw"]
        * economics_authority["second_leg_reserve_weth_raw"]
        * answer * fee_units,
        economics_authority["second_leg_reserve_uni_raw"]
        * usd_denominator * economics_authority["fee_denominator"],
    )
    p50_effective = economics_authority["receipt_effective_gas_price"]
    p90_effective = (
        economics_authority["next_base_fee_per_gas"]
        + economics_authority["p90_priority_fee_per_gas"]
    )
    authority_p50_gas = Fraction(
        economics_authority["gas_used"] * p50_effective * answer,
        usd_denominator,
    )
    authority_p90_gas = Fraction(
        economics_authority["gas_used"] * p90_effective * answer,
        usd_denominator,
    )
    opportunity_unsigned = dict(opportunity)
    opportunity_binding = opportunity_unsigned.pop(
        "evidence_binding_sha256", None
    )
    route_identity = {
        "token_symbol": opportunity.get("token_symbol"),
        "buy_market_id": opportunity.get("buy_market_id"),
        "sell_market_id": opportunity.get("sell_market_id"),
        "route_mode": opportunity.get("route_mode"),
    }
    try:
        expected_route_id = canonical_route_id(route_identity)
        expected_opportunity_id = route_opportunity_id(
            expected_route_id, expected_notional
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("historical opportunity identity is invalid") from error
    venue_order = (
        ("uniswap_v2", "sushiswap_v2")
        if expected_direction == "uniswap_to_sushiswap"
        else ("sushiswap_v2", "uniswap_v2")
    )
    if (
        opportunity_binding != _digest(opportunity_unsigned)
        or opportunity.get("contract_version")
        != ROUTE_OPPORTUNITY_CONTRACT_VERSION
        or opportunity.get("route_id") != expected_route_id
        or value["route_id"] != expected_route_id
        or opportunity.get("opportunity_id") != expected_opportunity_id
        or opportunity.get("requested_notional_usd")
        != str(expected_notional)
        or opportunity.get("token_symbol") != "UNI"
        or opportunity.get("route_mode") != "atomic_onchain"
        or ":{}:".format(venue_order[0])
        not in opportunity.get("buy_market_id", "")
        or ":{}:".format(venue_order[1])
        not in opportunity.get("sell_market_id", "")
        or opportunity.get("opportunity_class") != "research_estimate"
        or opportunity.get("strict_eligible") is not False
        or opportunity.get("strict_ready_for_publication") is not False
        or opportunity.get("publication_attestation_sha256") is not None
        or opportunity.get("mode_evidence_eligible") is not True
        or opportunity.get("cost_completeness") != "incomplete"
        or opportunity.get("scenario_cost_completeness") != "complete"
        or opportunity.get("strict_nonembedded_cost_usd") != "0"
        or opportunity.get("component_reasons")
        != ["cost_component_estimated"]
        or opportunity.get("primary_reason") != "cost_component_estimated"
        or opportunity.get("reason_codes") != ["cost_component_estimated"]
        or opportunity.get("reflected_or_embedded_component_keys")
        != ["buy:pool_swap_fee", "sell:pool_swap_fee"]
        or opportunity.get("edge_bps_denominator_basis")
        != "gross_buy_cost_usd"
        or opportunity.get("target_base_unit_decimals") != 18
        or opportunity.get("target_lattice_raw") != "1"
        or opportunity.get("skew_seconds") != "0"
        or opportunity.get("route_age_seconds") != "0"
        or opportunity.get("buy_state_observed_at")
        != opportunity.get("sell_state_observed_at")
        or opportunity.get("buy_core_manifest_sha256")
        != opportunity.get("sell_core_manifest_sha256")
        or opportunity.get("inventory_profile_hash")
        != value["proof_inputs_hash"]
        or opportunity.get("cost_component_set_sha256") != _digest(rows)
        or opportunity.get("mode_evidence_sha256") != _typed_digest(
            "historical_atomic_mode_evidence/v1",
            {
                "scenario_key": expected_scenario_key,
                "proof_inputs_hash": value["proof_inputs_hash"],
            },
        )
        or any(
            _SHA.fullmatch(opportunity.get(field) or "") is None
            for field in (
                "buy_usd_projection_sha256", "sell_usd_projection_sha256",
                "buy_core_manifest_sha256", "sell_core_manifest_sha256",
                "cost_component_set_sha256", "mode_evidence_sha256",
                "inventory_profile_hash", "evidence_binding_sha256",
            )
        )
    ):
        raise ValueError("historical opportunity contract differs")

    target = _historical_canonical_decimal_fraction(
        opportunity.get("target_token_quantity"),
        "historical opportunity target", positive=True,
    )
    try:
        target_raw = int(opportunity.get("target_base_raw"))
    except (TypeError, ValueError) as error:
        raise ValueError("historical opportunity target is invalid") from error
    if (
        opportunity.get("target_base_raw") != str(target_raw)
        or target * 10 ** 18 != target_raw
        or target_raw != economics_authority["first_uni_out_raw"]
        or opportunity.get("maximum_proved_capacity_quantity")
        != opportunity.get("target_token_quantity")
    ):
        raise ValueError("historical opportunity target differs")

    proof_roles = (
        "receipt", "receipt", "receipt", "receipt", "receipt", "receipt",
        "receipt", "trace", "policy",
    )
    common = None
    amounts = []
    policy_sha256 = None
    for row, proof_row, shape, proof_role in zip(
        rows, proof_rows, HISTORICAL_ATOMIC_COMPONENT_MATRIX, proof_roles
    ):
        if type(row) is not dict or set(row) != set(COST_COMPONENT_COLUMNS):
            raise ValueError("historical compact cost row schema differs")
        leg, component, status, embedded = shape
        row_common = (
            row.get("cohort_id"), row.get("opportunity_id"),
            row.get("requested_notional_usd"),
            row.get("target_token_quantity"),
        )
        if common is None:
            common = row_common
        if (
            row_common != common
            or row.get("contract_version") != COST_COMPONENT_CONTRACT_VERSION
            or row.get("opportunity_id") != expected_opportunity_id
            or row.get("cohort_id") != opportunity.get("cohort_id")
            or row.get("requested_notional_usd") != str(expected_notional)
            or row.get("target_token_quantity")
            != opportunity.get("target_token_quantity")
            or row.get("leg") != leg
            or row.get("component_type") != component
            or row.get("value_status") != status
            or row.get("embedded_in_leg_quote") is not embedded
            or row.get("direction") != (
                "buy_token" if leg == "buy"
                else "sell_token" if leg == "sell" else "route"
            )
            or row.get("market_id") != (
                opportunity["buy_market_id"] if leg == "buy"
                else opportunity["sell_market_id"] if leg == "sell" else ""
            )
            or row.get("basis")
            != "historical_foundry_exact_proof_input"
            or row.get("strict_eligible") is not False
            or row.get("observed_at") is not None
            or row.get("valid_until") is not None
            or row.get("reason_code") is not None
            or row.get("source") != proof_role
            or _SHA.fullmatch(row.get("source_record_sha256") or "") is None
            or proof_row.get("grain") != leg
            or proof_row.get("component") != component
            or proof_row.get("value_status") != status
            or proof_row.get("embedded") is not embedded
            or proof_row.get("proof_role") != proof_role
            or proof_row.get("amount_usd_exact") != row.get("amount_usd")
            or proof_row.get("rate_bps_exact") != row.get("rate_bps")
            or proof_row.get("proof_sha256")
            != row.get("source_record_sha256")
            or proof_role == "receipt"
            and row.get("source_record_sha256") != value["receipt_sha256"]
            or proof_role == "trace"
            and row.get("source_record_sha256") != value["trace_sha256"]
        ):
            raise ValueError("historical compact cost row contract differs")
        key = (leg, component)
        amount = row.get("amount_usd")
        rate = row.get("rate_bps")
        if key in (("buy", "pool_swap_fee"), ("sell", "pool_swap_fee")):
            amount_value = _historical_canonical_decimal_fraction(
                amount, "historical pool fee", positive=True
            )
            if rate != "30":
                raise ValueError("historical pool fee differs")
        elif key in (
            ("buy", "router_or_integrator_fee"),
            ("buy", "token_transfer_tax"),
            ("sell", "router_or_integrator_fee"),
            ("sell", "token_transfer_tax"),
        ):
            amount_value = Fraction(0)
            if amount != "0" or rate != "0":
                raise ValueError("historical proved zero fee differs")
        elif key == ("route", "network_gas"):
            amount_value = _historical_canonical_decimal_fraction(
                amount, "historical network gas", positive=True
            )
            if rate is not None:
                raise ValueError("historical network gas differs")
        elif key == ("route", "rebalancing_or_transfer"):
            amount_value = None
            if amount is not None or rate is not None:
                raise ValueError("historical transfer topology differs")
        else:
            amount_value = _historical_canonical_decimal_fraction(
                amount, "historical MEV buffer", nonnegative=True
            )
            if (
                rate != "10"
                or amount_value != Fraction(expected_notional * 10, 10_000)
            ):
                raise ValueError("historical MEV buffer differs")
            policy_sha256 = row["source_record_sha256"]
        amounts.append(amount_value)

    if (
        amounts[0] != authority_buy_pool_fee
        or amounts[3] != authority_sell_pool_fee
        or amounts[6] != authority_p50_gas
        or proof_inputs["policy_sha256"] != policy_sha256
    ):
        raise ValueError("historical compact cost proof arithmetic differs")

    gross_buy = _historical_canonical_decimal_fraction(
        opportunity.get("gross_buy_cost_usd"),
        "historical gross buy cost", positive=True,
    )
    gross_sell = _historical_canonical_decimal_fraction(
        opportunity.get("gross_sell_proceeds_usd"),
        "historical gross sell proceeds", nonnegative=True,
    )
    gross_edge = _historical_canonical_decimal_fraction(
        opportunity.get("gross_edge_usd"), "historical gross edge"
    )
    gas_cost = amounts[6]
    mev_cost = amounts[8]
    research_net = _historical_canonical_decimal_fraction(
        opportunity.get("research_net_edge_usd"),
        "historical research net",
    )
    if (
        gross_sell - gross_buy != gross_edge
        or gross_buy != authority_gross_buy
        or gross_sell != authority_gross_sell
        or gross_edge != authority_gross_edge
        or amounts[0] != gross_buy * Fraction(30, 10_000)
        or opportunity.get("strict_net_edge_usd")
        != _exact_fraction_decimal(gross_edge)
        or opportunity.get("research_bounded_cost_usd") != "0"
        or opportunity.get("research_assumed_cost_usd")
        != _exact_fraction_decimal(gas_cost + mev_cost)
        or research_net != gross_edge - gas_cost - mev_cost
    ):
        raise ValueError("historical opportunity economics differ")
    for prefix, edge in (
        ("gross", gross_edge), ("strict_net", gross_edge),
        ("research_net", research_net),
    ):
        bps, numerator, denominator = _historical_ratio_fields(edge, gross_buy)
        if (
            opportunity.get(prefix + "_edge_bps") != bps
            or opportunity.get(prefix + "_edge_bps_numerator") != numerator
            or opportunity.get(prefix + "_edge_bps_denominator") != denominator
        ):
            raise ValueError("historical opportunity bps differ")

    economics_fields = {
        "name", "priority_fee_percentile", "mev_bps", "gas_used",
        "effective_gas_price", "gas_cost_usd", "mev_buffer_usd",
        "research_net_edge_usd", "positive_research_net",
    }
    economics_matrix = (
        ("baseline_p50_mev_10bps", 50, 10),
        ("stress_p90_mev_25bps", 90, 25),
        ("stress_p90_mev_50bps", 90, 50),
    )
    baseline_positive = None
    for index, (row, expected) in enumerate(zip(economics, economics_matrix)):
        name, percentile, mev_bps = expected
        if (
            type(row) is not dict or set(row) != economics_fields
            or row.get("name") != name
            or row.get("priority_fee_percentile") != percentile
            or row.get("mev_bps") != str(mev_bps)
            or type(row.get("gas_used")) is not int
            or row["gas_used"] <= 0
            or type(row.get("effective_gas_price")) is not int
            or row["effective_gas_price"] <= 0
            or type(row.get("positive_research_net")) is not bool
            or row["gas_used"] != economics[0].get("gas_used")
            or row["gas_used"] != economics_authority["gas_used"]
        ):
            raise ValueError("historical economics matrix differs")
        scenario_gas = _historical_canonical_decimal_fraction(
            row["gas_cost_usd"], "historical scenario gas", positive=True
        )
        scenario_mev = _historical_canonical_decimal_fraction(
            row["mev_buffer_usd"], "historical scenario MEV",
            nonnegative=True,
        )
        scenario_net = _historical_canonical_decimal_fraction(
            row["research_net_edge_usd"], "historical scenario net"
        )
        expected_effective = p50_effective if index == 0 else p90_effective
        expected_gas = authority_p50_gas if index == 0 else authority_p90_gas
        if index == 0:
            baseline_positive = row["positive_research_net"]
        if (
            scenario_mev != Fraction(expected_notional * mev_bps, 10_000)
            or scenario_net != gross_edge - scenario_gas - scenario_mev
            or row["positive_research_net"] is not (scenario_net > 0)
            or row["effective_gas_price"] != expected_effective
            or scenario_gas != expected_gas
        ):
            raise ValueError("historical economics values differ")
        if index == 0 and (
            scenario_gas != gas_cost
            or scenario_mev != mev_cost
            or scenario_net != research_net
        ):
            raise ValueError("historical baseline economics differ")
    return {
        "scenario_key": expected_scenario_key,
        "route_id": value["route_id"],
        "requested_notional_usd": expected_notional,
        "proof_inputs_hash": value["proof_inputs_hash"],
        "policy_sha256": policy_sha256,
        "overlay_sha256": value["overlay_sha256"],
        "scenario_projection_sha256": binding,
        "opportunity_evidence_binding_sha256": opportunity_binding,
        "research_net_edge_usd": opportunity["research_net_edge_usd"],
        "economics_scenarios_sha256": _digest(economics),
        "positive_research_net": baseline_positive,
    }


def build_historical_replay_evidence(
    canonical_scenario_projection_bytes: Tuple[bytes, ...],
) -> bytes:
    if (
        type(canonical_scenario_projection_bytes) is not tuple
        or len(canonical_scenario_projection_bytes) != 10
        or any(type(value) is not bytes for value in canonical_scenario_projection_bytes)
    ):
        raise ValueError("historical replay scenario inventory is not exact")
    first_scenario_key = None
    try:
        first_value = json.loads(canonical_scenario_projection_bytes[0])
        first_scenario_key = first_value.get("scenario_key")
        block_text, _direction, _notional = first_scenario_key.split(":")
        block_number = int(block_text)
    except (
        AttributeError, TypeError, ValueError, UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise ValueError("historical replay scenario ID is invalid") from error
    if str(block_number) != block_text or block_number < 0:
        raise ValueError("historical replay scenario ID is invalid")
    directions = (
        "uniswap_to_sushiswap", "sushiswap_to_uniswap",
    )
    notionals = (1000, 5000, 10000, 50000, 100000)
    expected_inventory = tuple(
        (
            "{}:{}:{}".format(block_number, direction, notional),
            direction,
            notional,
        )
        for direction in directions for notional in notionals
    )
    scenarios = []
    for raw, (scenario_key, direction, notional) in zip(
        canonical_scenario_projection_bytes, expected_inventory
    ):
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("historical replay scenario is invalid") from error
        if (
            type(value) is not dict
            or _canonical_bytes(value) != raw
        ):
            raise ValueError("historical replay scenario is not canonical")
        scenarios.append(dict(_validate_historical_compact_scenario(
            value=value,
            expected_scenario_key=scenario_key,
            expected_direction=direction,
            expected_notional=notional,
        )))
    route_ids = tuple(
        scenarios[index * len(notionals)]["route_id"]
        for index in range(len(directions))
    )
    proof_hashes = {row["proof_inputs_hash"] for row in scenarios}
    policy_hashes = {row.pop("policy_sha256") for row in scenarios}
    if (
        len(set(route_ids)) != 2
        or any(
            row["route_id"] != route_ids[index // len(notionals)]
            for index, row in enumerate(scenarios)
        )
        or len(proof_hashes) != 10
        or len(policy_hashes) != 1
        or not any(row["positive_research_net"] for row in scenarios)
    ):
        raise ValueError("historical replay scenario set differs")
    result = {
        "schema": "historical_foundry_replay_evidence/v1",
        "opportunity_counts": {
            "research_estimate": 10,
            "strict_eligible": 0,
            "executable_candidate": 0,
            "attested": 0,
            "unavailable": 0,
        },
        "overlay_set_sha256": _typed_digest(
            "historical_foundry_overlay_set/v1",
            sorted(row["overlay_sha256"] for row in scenarios),
        ),
        "scenario_set_sha256": _typed_digest(
            "historical_foundry_scenario_set/v1", scenarios
        ),
        "scenarios": scenarios,
    }
    result["complete_manifest_sha256"] = _typed_digest(
        "historical_foundry_replay_evidence/v1", result
    )
    return _canonical_bytes(result)
