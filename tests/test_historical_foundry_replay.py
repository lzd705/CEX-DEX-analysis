from __future__ import annotations

import copy
import hashlib
import json
import unittest
from decimal import Decimal, localcontext

from scripts.fetch_dex_depth import depth_fields, v2_band_amounts
from scripts.historical_foundry_contracts import load_historical_foundry_config_set
from scripts.historical_foundry_replay import (
    build_historical_core_projection,
    build_historical_research_universe,
    validate_selected_historical_run,
)
from scripts.route_cohort import canonical_route_id
from scripts.route_quantity import V2PoolState, V2_FEE_FORMULA


UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
UNI_POOL = "0xd3d2e2692501a5c9ca623199d38826e513033a17"
SUSHI_POOL = "0xdafd66636e2561b0284edde37e42d192f2844d40"
BLOCK_HASH = "0x" + "a" * 64
HEADER_SHA = "b" * 64
FEE_SHA = "c" * 64
RAW_SHA = "d" * 64


def canonical_bytes(value):
    def plain(item):
        if isinstance(item, dict) or hasattr(item, "items"):
            return {key: plain(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [plain(child) for child in item]
        return item
    return json.dumps(plain(value), sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def typed_member(config, dex, pool, reserve_uni, reserve_weth):
    fee_sha = digest({"schema": "historical_v2_fee_identity/v1",
                      "authority_sha256": config.authority.physical_sha256,
                      "dex": dex, "fee_numerator": 997, "fee_denominator": 1000})
    state = V2PoolState(
        chain="eth", chain_id=1, dex=dex, pool_address=pool,
        token0_address=UNI, token1_address=WETH,
        token0_decimals=18, token1_decimals=18,
        reserve0_raw=reserve_uni, reserve1_raw=reserve_weth,
        reserve_timestamp_last_raw=1704067200, fee_bps=30,
        fee_numerator=997, fee_denominator=1000,
        fee_formula=V2_FEE_FORMULA, fee_proof_sha256=fee_sha,
        block_number=20_000_000, block_hash=BLOCK_HASH,
        block_header_sha256=HEADER_SHA,
        observed_at="2024-01-01T00:00:00+00:00",
        raw_response_sha256=RAW_SHA,
    )
    payload = {
        "schema": "route_v2_pool_state/v1",
        **{name: str(getattr(state, name)) for name in (
            "chain_id", "token0_decimals", "token1_decimals", "reserve0_raw",
            "reserve1_raw", "reserve_timestamp_last_raw", "fee_bps",
            "fee_numerator", "fee_denominator", "block_number")},
        **{name: getattr(state, name) for name in (
            "chain", "dex", "pool_address", "token0_address", "token1_address",
            "fee_formula", "fee_proof_sha256", "block_hash",
            "block_header_sha256", "observed_at", "raw_response_sha256", "state_id")},
    }
    raw = canonical_bytes(payload)
    market = "dex:eth:{}:{}:UNI".format(dex, pool)
    return {
        "descriptor": {
            "market_id": market, "role": "dex_pool_state",
            "adapter_id": "route_quantity_quote_for_v2_pool/v1",
            "content_schema": "route_v2_pool_state/v1",
            "filename": "{}-dex_pool_state.json".format(dex),
            "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
            "logical_generation": state.state_id.split(":", 1)[1],
        },
        "payload_hex": raw.hex(),
    }


def usd_member(dex, pool):
    market = "dex:eth:{}:{}:UNI".format(dex, pool)
    payload = {"schema": "route_dex_usd_price_context/v1", "market_id": market,
               "block_number": "20000000", "block_hash": BLOCK_HASH,
               "observed_at": "2024-01-01T00:00:00+00:00",
               "eth_usd": "2000", "uni_usd_method": "reserve_implied_uni_weth"}
    raw = canonical_bytes(payload)
    return {"descriptor": {"market_id": market, "role": "dex_usd_price_context",
            "adapter_id": "historical_reserve_implied_usd_context/v1",
            "content_schema": "route_dex_usd_price_context/v1",
            "filename": "{}-dex_usd_price_context.json".format(dex),
            "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
            "logical_generation": hashlib.sha256(raw).hexdigest()},
            "payload_hex": raw.hex()}


def fixture(config):
    venues = {
        "uniswap_v2": {"pair_address": UNI_POOL, "factory_pair_forward": UNI_POOL,
                       "factory_pair_reverse": UNI_POOL, "reserve_uni_raw": 100 * 10**18,
                       "reserve_weth_raw": 10_000 * 10**18,
                       "reserve_timestamp_last_raw": 1704067200,
                       "raw_response_sha256": RAW_SHA},
        "sushiswap_v2": {"pair_address": SUSHI_POOL, "factory_pair_forward": SUSHI_POOL,
                         "factory_pair_reverse": SUSHI_POOL, "reserve_uni_raw": 120 * 10**18,
                         "reserve_weth_raw": 11_400 * 10**18,
                         "reserve_timestamp_last_raw": 1704067200,
                         "raw_response_sha256": RAW_SHA},
    }
    routes = []
    for buy, sell in (("uniswap_v2", "sushiswap_v2"),
                      ("sushiswap_v2", "uniswap_v2")):
        identity = {
            "token_symbol": "UNI",
            "buy_market_id": "dex:eth:{}:{}:UNI".format(buy, venues[buy]["pair_address"]),
            "sell_market_id": "dex:eth:{}:{}:UNI".format(sell, venues[sell]["pair_address"]),
            "route_mode": "atomic_onchain",
        }
        routes.append({**identity, "route_id": canonical_route_id(identity)})
    selected = {"anchor_timestamp": "2024-01-01T00:01:00+00:00",
                "block_timestamp": "2024-01-01T00:00:00+00:00",
                "block_number": 20_000_000, "block_hash": BLOCK_HASH,
                "block_header_sha256": HEADER_SHA, "venues": venues, "routes": routes}
    evidence = {
        "schema": "historical_foundry_selected_run_closed/v1",
        "run_id": "run:" + "1" * 64,
        "snapshot_run_id": "run:" + "1" * 64,
        "manifest_sha256": "9" * 64,
        "policy_sha256": config.policy.physical_sha256,
        "authority_sha256": config.authority.physical_sha256,
        "toolchain_sha256": config.toolchain.physical_sha256,
        "selection": selected,
        "selection_sha256": digest(selected),
        "eth_usd": "2000",
        "scenarios": [
            {"route_id": route["route_id"], "requested_notional_usd": n,
             "receipt_status": 1}
            for route in routes for n in (1000, 5000, 10000, 50000, 100000)
        ],
        "typed_members": [
            typed_member(config, "uniswap_v2", UNI_POOL, 100 * 10**18, 10_000 * 10**18),
            usd_member("uniswap_v2", UNI_POOL),
            typed_member(config, "sushiswap_v2", SUSHI_POOL, 120 * 10**18, 11_400 * 10**18),
            usd_member("sushiswap_v2", SUSHI_POOL),
        ],
    }
    evidence["evidence_sha256"] = digest(evidence)
    return evidence


class HistoricalResearchUniverseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_historical_foundry_config_set()

    def validated(self):
        return validate_selected_historical_run(
            config=self.config, run_evidence=fixture(self.config))

    def test_builds_exact_two_market_two_route_historical_universe(self):
        validated = self.validated()
        universe = build_historical_research_universe(
            config=self.config, validated_run=validated)
        self.assertEqual(universe["schema"], "historical_research_universe/v1")
        self.assertEqual(universe["temporal_scope"], "historical_replay")
        self.assertEqual(universe["execution_claim"], "historical_counterfactual_state_override_next_block")
        self.assertEqual(len(universe["markets"]), 2)
        self.assertEqual(len(universe["routes"]), 2)
        self.assertEqual({m["dex"] for m in universe["markets"]}, {"uniswap_v2", "sushiswap_v2"})
        self.assertEqual(universe["provenance_window"], {"start_date": "2023-12-03", "end_date": "2024-01-01", "calendar_days": 30, "measured_volume_coverage": False})
        for market in universe["markets"]:
            self.assertIsNone(market["dex_24h_usd"])
            self.assertIsNone(market["route_volume_usd"])
            self.assertEqual(market["proved_execution_capacity_usd"], "100000")

    def test_100bps_depth_literal_and_live_helper_parity(self):
        universe = build_historical_research_universe(
            config=self.config, validated_run=self.validated())
        uni = next(m for m in universe["markets"] if m["dex"] == "uniswap_v2")
        self.assertEqual(uni["sell_depth_100bps_usd"], "100251.25786760090531040358")
        self.assertEqual(uni["buy_depth_100bps_usd"], "100051.3964310737617237009283")
        self.assertEqual(uni["observed_100bps_depth_usd"], "200302.6542986746670341045083")
        self.assertEqual(uni["dex_tvl_usd"], "40000000")
        with localcontext() as context:
            context.prec = 28
            amount = v2_band_amounts(Decimal(100 * 10**18), Decimal(10_000 * 10**18), Decimal(30), 100)
            live = depth_fields(target_position_index=0, token0_decimals=18,
                                token1_decimals=18, token0_price=Decimal(200000),
                                token1_price=Decimal(2000), band_amounts={100: {
                                    "zero_input": amount["zero_for_one_gross_input"],
                                    "zero_output": amount["zero_for_one_output"],
                                    "one_input": amount["one_for_zero_gross_input"],
                                    "one_output": amount["one_for_zero_output"],
                                    "zero_complete": True, "one_complete": True}})
        self.assertEqual(uni["observed_100bps_depth_usd"], live["total_depth_100bps_usd"])

    def test_core_projection_cross_binds_universe(self):
        validated = self.validated()
        universe = build_historical_research_universe(config=self.config, validated_run=validated)
        core = build_historical_core_projection(config=self.config, validated_run=validated, universe=universe)
        self.assertEqual(core["schema"], "historical_core_projection/v1")
        self.assertEqual(core["universe_sha256"], digest(universe))
        self.assertEqual(len(core["typed_members"]), 4)

    def test_rejects_closed_input_transplants_and_rehashes(self):
        mutations = [
            ("run", lambda x: x.__setitem__("run_id", "run:" + "2" * 64)),
            ("policy", lambda x: x.__setitem__("policy_sha256", "0" * 64)),
            ("authority", lambda x: x.__setitem__("authority_sha256", "0" * 64)),
            ("toolchain", lambda x: x.__setitem__("toolchain_sha256", "0" * 64)),
            ("selection", lambda x: x["selection"].__setitem__("block_hash", "0x" + "e" * 64)),
            ("block", lambda x: x["selection"].__setitem__("block_number", 20_000_001)),
            ("pair", lambda x: x["selection"]["venues"]["uniswap_v2"].__setitem__("pair_address", SUSHI_POOL)),
            ("token", lambda x: x["typed_members"][0].__setitem__("payload_hex", "00" + x["typed_members"][0]["payload_hex"][2:])),
            ("typed_transplant", lambda x: x["typed_members"].__setitem__(0, copy.deepcopy(x["typed_members"][2]))),
            ("descriptor_hash", lambda x: x["typed_members"][0]["descriptor"].__setitem__("sha256", "0" * 64)),
            ("route", lambda x: x["selection"]["routes"][0].__setitem__("route_id", "route:" + "0" * 64)),
            ("direction", lambda x: x["selection"]["routes"][0].__setitem__("buy_market_id", x["selection"]["routes"][0]["sell_market_id"])),
        ]
        for label, mutate in mutations:
            value = fixture(self.config); mutate(value)
            value["selection_sha256"] = digest(value["selection"])
            value["evidence_sha256"] = digest({k: v for k, v in value.items() if k != "evidence_sha256"})
            with self.subTest(label=label), self.assertRaises(ValueError):
                validate_selected_historical_run(config=self.config, run_evidence=value)

    def test_caller_mapping_is_not_documented_as_root_authority(self):
        import scripts.historical_foundry_replay as module
        self.assertIn("Task 7", module.__doc__)
        self.assertIn("not root authority", module.__doc__)


if __name__ == "__main__":
    unittest.main()
