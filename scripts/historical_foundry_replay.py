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
import weakref
from collections.abc import Mapping as MappingABC
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
from types import MappingProxyType
from typing import Any, Dict, Mapping

from scripts.fetch_dex_depth import decimal_text, depth_fields, v2_band_amounts
from scripts.historical_foundry_contracts import HistoricalFoundryConfigSet
from scripts.route_cohort import canonical_route_id
from scripts.route_quantity import V2PoolState, V2_FEE_FORMULA


_UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
_WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
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
