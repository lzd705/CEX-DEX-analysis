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
from datetime import date, datetime, timedelta, timezone
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
    "toolchain_sha256", "selection", "selection_sha256", "eth_usd",
    "scenarios", "typed_members", "evidence_sha256",
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
    "market_id", "role", "adapter_id", "content_schema", "filename", "size",
    "sha256", "logical_generation",
))
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_RUN_ID = re.compile(r"run:[0-9a-f]{64}\Z")
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")


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


def _exact_fields(value: Any, expected: frozenset, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or frozenset(value) != expected:
        raise ValueError("{} fields are invalid".format(label))
    return value


def _hash(value: Any, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise ValueError("{} is invalid".format(label))
    return value


def _positive_decimal(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
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
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("anchor timestamp is invalid")
    return result.astimezone(timezone.utc)


def _validate_typed_member(member: Any, *, config: HistoricalFoundryConfigSet,
                           dex: str, selected: Mapping[str, Any]) -> Mapping[str, Any]:
    _exact_fields(member, frozenset(("descriptor", "payload_hex")), "typed member")
    descriptor = _exact_fields(member["descriptor"], _DESCRIPTOR_FIELDS, "typed descriptor")
    if not isinstance(member["payload_hex"], str):
        raise ValueError("typed member bytes are invalid")
    try:
        payload_bytes = bytes.fromhex(member["payload_hex"])
    except ValueError:
        raise ValueError("typed member bytes are invalid")
    pair = selected["venues"][dex]["pair_address"]
    expected_market = "dex:eth:{}:{}:UNI".format(dex, pair)
    if (descriptor["market_id"] != expected_market
            or descriptor["role"] != "dex_pool_state"
            or descriptor["adapter_id"] != "route_quantity_quote_for_v2_pool/v1"
            or descriptor["content_schema"] != "route_v2_pool_state/v1"
            or type(descriptor["size"]) is not int
            or descriptor["size"] != len(payload_bytes)
            or descriptor["sha256"] != hashlib.sha256(payload_bytes).hexdigest()):
        raise ValueError("typed descriptor identity differs")
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("typed payload is invalid")
    _exact_fields(payload, _PAYLOAD_FIELDS, "typed payload")
    if _canonical_bytes(payload) != payload_bytes:
        raise ValueError("typed payload is not canonical")
    venue = selected["venues"][dex]
    fee_proof = _digest({"schema": "historical_v2_fee_identity/v1",
                         "authority_sha256": config.authority.physical_sha256,
                         "dex": dex, "fee_numerator": 997, "fee_denominator": 1000})
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
    if (payload["state_id"] != state.state_id
            or descriptor["logical_generation"] != state.state_id.split(":", 1)[1]):
        raise ValueError("typed V2 state binding differs")
    return payload


def _validate_usd_member(member: Any, *, dex: str, selected: Mapping[str, Any],
                         eth_usd: Decimal) -> Mapping[str, Any]:
    _exact_fields(member, frozenset(("descriptor", "payload_hex")), "USD member")
    descriptor = _exact_fields(member["descriptor"], _DESCRIPTOR_FIELDS, "USD descriptor")
    try:
        raw = bytes.fromhex(member["payload_hex"])
        payload = json.loads(raw.decode("utf-8"))
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("USD member bytes are invalid")
    pair = selected["venues"][dex]["pair_address"]
    market = "dex:eth:{}:{}:UNI".format(dex, pair)
    expected = {"schema": "route_dex_usd_price_context/v1", "market_id": market,
                "block_number": str(selected["block_number"]),
                "block_hash": selected["block_hash"],
                "observed_at": selected["block_timestamp"],
                "eth_usd": str(eth_usd),
                "uni_usd_method": "reserve_implied_uni_weth"}
    if payload != expected or _canonical_bytes(payload) != raw:
        raise ValueError("USD context differs")
    physical = hashlib.sha256(raw).hexdigest()
    if (descriptor != {"market_id": market, "role": "dex_usd_price_context",
                       "adapter_id": "historical_reserve_implied_usd_context/v1",
                       "content_schema": "route_dex_usd_price_context/v1",
                       "filename": "{}-dex_usd_price_context.json".format(dex),
                       "size": len(raw), "sha256": physical,
                       "logical_generation": physical}):
        raise ValueError("USD descriptor identity differs")
    return payload


def validate_selected_historical_run(*, config: HistoricalFoundryConfigSet,
                                     run_evidence: Mapping[str, Any]) -> Mapping[str, Any]:
    """Validate one already-reread closed input; Task 7 owns root authenticity."""
    if not isinstance(config, HistoricalFoundryConfigSet):
        raise ValueError("historical config capability is invalid")
    value = _exact_fields(run_evidence, _RUN_FIELDS, "historical run")
    if value["schema"] != "historical_foundry_selected_run_closed/v1":
        raise ValueError("historical run schema is invalid")
    if (not isinstance(value["run_id"], str) or _RUN_ID.fullmatch(value["run_id"]) is None
            or value["snapshot_run_id"] != value["run_id"]
            or _SHA.fullmatch(str(value["manifest_sha256"])) is None):
        raise ValueError("historical run id is invalid")
    for field, expected in (("policy_sha256", config.policy.physical_sha256),
                            ("authority_sha256", config.authority.physical_sha256),
                            ("toolchain_sha256", config.toolchain.physical_sha256)):
        if value[field] != expected:
            raise ValueError("historical config binding differs")
    unsigned = {key: item for key, item in value.items() if key != "evidence_sha256"}
    if _hash(value["evidence_sha256"], "evidence hash") != _digest(unsigned):
        raise ValueError("historical evidence hash differs")
    selected = _exact_fields(value["selection"], _SELECTION_FIELDS, "selection")
    if value["selection_sha256"] != _digest(selected):
        raise ValueError("selection hash differs")
    anchor_time = _timestamp(selected["anchor_timestamp"])
    block_time = _timestamp(selected["block_timestamp"])
    lookback = config.policy.value["lookback_seconds"]
    if not anchor_time - timedelta(seconds=lookback) <= block_time <= anchor_time:
        raise ValueError("selected block is outside the policy window")
    if type(selected["block_number"]) is not int or selected["block_number"] <= 0:
        raise ValueError("selected block differs")
    if (not isinstance(selected["block_hash"], str)
            or re.fullmatch(r"0x[0-9a-f]{64}", selected["block_hash"]) is None
            or _SHA.fullmatch(str(selected["block_header_sha256"])) is None):
        raise ValueError("selected block identity differs")
    _exact_fields(selected["venues"], frozenset(_VENUES), "selected venues")
    for dex in _VENUES:
        venue = _exact_fields(selected["venues"][dex], frozenset(("pair_address", "factory_pair_forward", "factory_pair_reverse", "reserve_uni_raw", "reserve_weth_raw", "reserve_timestamp_last_raw", "raw_response_sha256")), "venue")
        if (not isinstance(venue["pair_address"], str) or _ADDRESS.fullmatch(venue["pair_address"]) is None
                or venue["factory_pair_forward"] != venue["pair_address"]
                or venue["factory_pair_reverse"] != venue["pair_address"]
                or type(venue["reserve_uni_raw"]) is not int or type(venue["reserve_weth_raw"]) is not int
                or type(venue["reserve_timestamp_last_raw"]) is not int
                or not 0 <= venue["reserve_timestamp_last_raw"] < 2 ** 32
                or _SHA.fullmatch(str(venue["raw_response_sha256"])) is None
                or min(venue["reserve_uni_raw"], venue["reserve_weth_raw"]) <= 0):
            raise ValueError("selected venue differs")
    if not isinstance(selected["routes"], list) or len(selected["routes"]) != 2:
        raise ValueError("selected routes are invalid")
    expected_routes = []
    for buy, sell in (("uniswap_v2", "sushiswap_v2"), ("sushiswap_v2", "uniswap_v2")):
        identity = {"token_symbol": "UNI", "buy_market_id": "dex:eth:{}:{}:UNI".format(buy, selected["venues"][buy]["pair_address"]), "sell_market_id": "dex:eth:{}:{}:UNI".format(sell, selected["venues"][sell]["pair_address"]), "route_mode": "atomic_onchain"}
        expected_routes.append({**identity, "route_id": canonical_route_id(identity)})
    if selected["routes"] != expected_routes:
        raise ValueError("selected routes differ")
    if not isinstance(value["typed_members"], list) or len(value["typed_members"]) != 4:
        raise ValueError("typed members are invalid")
    eth_usd = _positive_decimal(value["eth_usd"], "ETH/USD")
    payloads = []
    for index, dex in enumerate(_VENUES):
        payloads.append(_validate_typed_member(value["typed_members"][index * 2], config=config, dex=dex, selected=selected))
        payloads.append(_validate_usd_member(value["typed_members"][index * 2 + 1], dex=dex, selected=selected, eth_usd=eth_usd))
    scenarios = value["scenarios"]
    expected_pairs = {(route["route_id"], n) for route in expected_routes
                      for n in (1000, 5000, 10000, 50000, 100000)}
    actual = set()
    if not isinstance(scenarios, list) or len(scenarios) != 10:
        raise ValueError("historical scenarios are invalid")
    for scenario in scenarios:
        _exact_fields(scenario, frozenset(("route_id", "requested_notional_usd", "receipt_status")), "scenario")
        if scenario["receipt_status"] != 1:
            raise ValueError("historical scenario is not proved")
        actual.add((scenario["route_id"], scenario["requested_notional_usd"]))
    if actual != expected_pairs:
        raise ValueError("historical scenarios differ")
    normalized = dict(_plain(value)); normalized["typed_payloads"] = payloads
    return _freeze(normalized)


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
    if not isinstance(config, HistoricalFoundryConfigSet) or validated_run.get("policy_sha256") != config.policy.physical_sha256:
        raise ValueError("validated run/config binding differs")
    anchor = _timestamp(validated_run["selection"]["anchor_timestamp"]).date()
    result = {
        "schema": "historical_research_universe/v1",
        "temporal_scope": "historical_replay", "execution_claim": _EXECUTION_CLAIM,
        "run_id": validated_run["run_id"],
        "selection_sha256": validated_run["selection_sha256"],
        "provenance_window": {"start_date": str(anchor - timedelta(days=29)),
                              "end_date": str(anchor), "calendar_days": 30,
                              "measured_volume_coverage": False},
        "markets": [_market_projection(validated_run, dex) for dex in _VENUES],
        "routes": _plain(validated_run["selection"]["routes"]),
    }
    return _freeze(result)


def build_historical_core_projection(*, config: HistoricalFoundryConfigSet,
                                     validated_run: Mapping[str, Any],
                                     universe: Mapping[str, Any]) -> Mapping[str, Any]:
    rebuilt = build_historical_research_universe(config=config, validated_run=validated_run)
    if _canonical_bytes(rebuilt) != _canonical_bytes(universe):
        raise ValueError("historical universe binding differs")
    return _freeze({
        "schema": "historical_core_projection/v1",
        "temporal_scope": "historical_replay", "execution_claim": _EXECUTION_CLAIM,
        "run_id": validated_run["run_id"], "universe_sha256": _digest(universe),
        "markets": _plain(universe["markets"]), "routes": _plain(universe["routes"]),
        "typed_members": _plain(validated_run["typed_members"]),
    })
