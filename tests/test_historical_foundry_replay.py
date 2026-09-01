from __future__ import annotations

import copy
import dataclasses
import gzip
import hashlib
import inspect
import json
import pickle
import unittest
import weakref
from contextlib import ExitStack
from decimal import (
    Decimal, ROUND_DOWN, ROUND_HALF_EVEN, ROUND_UP, localcontext,
)
from fractions import Fraction
from unittest import mock

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
PARENT_HASH = "0x" + "b" * 64
STATE_ROOT = "0x" + "c" * 64
RAW_SHA = "d" * 64
SCAN_SHA = "e" * 64
BLOCK_TIME = "2024-01-01T00:00:00Z"
ANCHOR_TIME = "2024-01-01T00:01:00Z"
BLOCK_TIMESTAMP = 1_704_067_200
PRICE_UPDATED_AT = BLOCK_TIMESTAMP - 10
PRICE_ROUND_ID = (7 << 64) + 123
PRICE_ANSWER = 2_000 * 10 ** 8
PRICE_DECIMALS = 8
EXPECTED_FEE_PROOF_BY_DEX = {
    "uniswap_v2": "8e86eea01b5359b641bf51a3b93d8c247bb25be6c4400f9724af2f1216684bad",
    "sushiswap_v2": "6c30ff91892f35420c399848f78b0022afaecf32fb5c7237ca0c3edbdcf678ae",
}


def canonical_bytes(value):
    def plain(item):
        if isinstance(item, dict) or hasattr(item, "items"):
            return {key: plain(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [plain(child) for child in item]
        return item
    return json.dumps(
        plain(value), allow_nan=False, ensure_ascii=False,
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def typed_digest(domain, value):
    return hashlib.sha256(domain.encode("ascii") + b"\0" + canonical_bytes(value)).hexdigest()


TASK7_SELECTED_BLOCK = {
    "number": 20_000_000,
    "hash": BLOCK_HASH,
    "parent_hash": PARENT_HASH,
    "state_root": STATE_ROOT,
    "timestamp": BLOCK_TIMESTAMP,
    "gas_limit": 30_000_000,
    "gas_used": 15_000_000,
    "base_fee_per_gas": 20_000_000_000,
}
HEADER_SHA = digest(TASK7_SELECTED_BLOCK)


def market_key(market_id):
    return typed_digest("historical_foundry_market_key/v1", {"market_id": market_id})


def descriptor(*, market, role, filename, payload, logical_generation):
    role_contract = {
        "dex_pool_state": (
            "route_quantity_quote_for_v2_pool/v1", "route_v2_pool_state/v1"
        ),
        "dex_usd_price_context": (
            "route_dex_usd_price_context/v1", "route_dex_usd_price_context/v1"
        ),
    }[role]
    return {
        "market_id": market,
        "role": role,
        "adapter_id": role_contract[0],
        "content_schema": role_contract[1],
        "path": "typed/{}/{}".format(market_key(market), filename),
        "filename": filename,
        "byte_count": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "logical_generation": logical_generation,
    }


def typed_member(config, dex, pool, reserve_uni, reserve_weth):
    fee_identity = {
        "schema": "historical_foundry_v2_fee_identity/v1",
        "authority_sha256": config.authority.physical_sha256,
        "venue_id": dex,
        "fee_numerator": 997,
        "fee_denominator": 1000,
        "fee_bps": 30,
    }
    fee_sha = typed_digest("historical_foundry_v2_fee_identity/v1", fee_identity)
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
        observed_at=BLOCK_TIME,
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
        "descriptor": descriptor(
            market=market, role="dex_pool_state", filename="dex_pool_state.json",
            payload=raw, logical_generation=state.state_id.split(":", 1)[1],
        ),
        "payload_hex": raw.hex(),
    }


def usd_member(dex, pool):
    market = "dex:eth:{}:{}:UNI".format(dex, pool)
    payload = {
        "schema": "route_dex_usd_price_context/v1",
        "market_id": market,
        "venue_id": dex,
        "chain_id": "1",
        "block_number": "20000000",
        "block_hash": BLOCK_HASH,
        "proxy_address": "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419",
        "round_id": str(PRICE_ROUND_ID),
        "phase_id": "7",
        "answer": str(PRICE_ANSWER),
        "decimals": str(PRICE_DECIMALS),
        "started_at": str(PRICE_UPDATED_AT - 10),
        "updated_at": str(PRICE_UPDATED_AT),
        "answered_in_round": str(PRICE_ROUND_ID),
        "valid_until": str(PRICE_UPDATED_AT + 3601),
        "scan_inventory_sha256": SCAN_SHA,
    }
    raw = canonical_bytes(payload)
    physical = hashlib.sha256(raw).hexdigest()
    return {
        "descriptor": descriptor(
            market=market, role="dex_usd_price_context",
            filename="dex_usd_price_context.json", payload=raw,
            logical_generation=physical,
        ),
        "payload_hex": raw.hex(),
    }


def task7_selection_value():
    selected_scenarios = []
    for direction in ("uniswap_to_sushiswap", "sushiswap_to_uniswap"):
        for notional in (1000, 5000, 10000, 50000, 100000):
            prefix = "{}:{}".format(direction, notional)
            outcome = (
                1 if not selected_scenarios
                else 0 if len(selected_scenarios) == 1
                else -1
            )
            selected_scenarios.append({
                "scenario_key": "20000000:" + prefix,
                "block_number": 20_000_000,
                "status": 1,
                "classification": "replay_success",
                "gas_used": 100_000,
                "effective_gas_price": 20_000_000_000,
                "weth_delta_raw": outcome * 10 ** 16,
                "proof_inputs_hash": hashlib.sha256(
                    ("proof:" + prefix).encode("ascii")
                ).hexdigest(),
                "overlay_sha256": hashlib.sha256(
                    ("overlay:" + prefix).encode("ascii")
                ).hexdigest(),
                "receipt_sha256": hashlib.sha256(
                    ("receipt:" + prefix).encode("ascii")
                ).hexdigest(),
                "trace_sha256": hashlib.sha256(
                    ("trace:" + prefix).encode("ascii")
                ).hexdigest(),
                "result_sha256": hashlib.sha256(
                    ("result:" + prefix).encode("ascii")
                ).hexdigest(),
                "economics": {
                    "gross_edge_usd": {
                        "numerator": outcome, "denominator": 1,
                        "display": str(outcome),
                    },
                    "gas_cost_usd": {
                        "numerator": 0, "denominator": 1, "display": "0",
                    },
                    "mev_buffer_usd": {
                        "numerator": 0, "denominator": 1, "display": "0",
                    },
                    "policy_net_edge_usd": {
                        "numerator": outcome, "denominator": 1,
                        "display": str(outcome),
                    },
                },
                "direction": direction,
                "requested_notional_usd": notional,
            })
    return {
        "schema": "historical_foundry_selection/v1",
        "status": "found_publishable_profitable_block",
        "staging_inventory_sha256": SCAN_SHA,
        "prefilter_grid_digest": "7" * 64,
        "candidate_block_count": 1,
        "scenario_denominator": 10,
        "initial_replay_required_count": 5,
        "selected_block": dict(TASK7_SELECTED_BLOCK),
        "selected_scenario_count": 10,
        "selected_scenarios": selected_scenarios,
        "candidate_states": [{
            "block_number": 20_000_000,
            "state": "selected",
            "transitions": [
                "candidate", "replaying_required", "tentative_positive",
                "completing_full_ten", "selected",
            ],
            "scenario_count": 10,
        }],
        "unresolved_candidate_count": 0,
    }


def task7_candidate_manifest_value(selection):
    scenarios = [{
        key: value for key, value in row.items()
        if key not in {"direction", "requested_notional_usd"}
    } for row in selection["selected_scenarios"]]
    return {
        "schema": "historical_foundry_candidate_manifest/v1",
        "staging_inventory_sha256": selection[
            "staging_inventory_sha256"
        ],
        "prefilter_grid_digest": selection["prefilter_grid_digest"],
        "candidate_block_count": selection["candidate_block_count"],
        "scenario_denominator": selection["scenario_denominator"],
        "initial_replay_required_count": selection[
            "initial_replay_required_count"
        ],
        "attempted_scenario_count": len(scenarios),
        "candidate_states": json.loads(canonical_bytes(
            selection["candidate_states"]
        ).decode("utf-8")),
        "scenarios": scenarios,
    }


def task7_typed_manifest_value(evidence):
    markets = []
    global_members = []
    for index, dex in enumerate(("uniswap_v2", "sushiswap_v2")):
        venue = evidence["selection"]["venues"][dex]
        member_rows = []
        for member_index, role in (
            (index * 2, "dex_pool_state"),
            (index * 2 + 1, "dex_usd_price_context"),
        ):
            descriptor_value = evidence["typed_members"][member_index]["descriptor"]
            row = {
                "role": role,
                "path": descriptor_value["path"],
                "byte_count": descriptor_value["byte_count"],
                "sha256": descriptor_value["sha256"],
            }
            member_rows.append(row)
            global_members.append({
                key: row[key] for key in ("path", "byte_count", "sha256")
            })
        market = descriptor_value["market_id"]
        markets.append({
            "market_id": market,
            "market_key": market_key(market),
            "venue_id": dex,
            "pair_address": venue["pair_address"],
            "factory_pair_forward": venue["factory_pair_forward"],
            "factory_pair_reverse": venue["factory_pair_reverse"],
            "members": member_rows,
        })
    return {
        "schema": "historical_foundry_typed_manifest/v1",
        "selection_status": "found_publishable_profitable_block",
        "selected_block": dict(TASK7_SELECTED_BLOCK),
        "market_count": 2,
        "markets": markets,
        "member_count": 4,
        "members": sorted(global_members, key=lambda row: row["path"]),
    }


def replace_task7_run_preimage(
    evidence, *, candidate_manifest_value=None, selection_value=None,
    typed_manifest_value=None, candidate_manifest_bytes=None,
    selection_bytes=None, typed_manifest_bytes=None,
):
    if candidate_manifest_bytes is None:
        if candidate_manifest_value is None:
            candidate_manifest_value = json.loads(bytes.fromhex(
                evidence["task7_candidate_manifest_hex"]
            ).decode("utf-8"))
        candidate_manifest_bytes = canonical_bytes(candidate_manifest_value)
    if selection_bytes is None:
        if selection_value is None:
            selection_value = json.loads(bytes.fromhex(
                evidence["task7_selection_hex"]
            ).decode("utf-8"))
        selection_bytes = canonical_bytes(selection_value)
    if typed_manifest_bytes is None:
        if typed_manifest_value is None:
            typed_manifest_value = json.loads(bytes.fromhex(
                evidence["task7_typed_manifest_hex"]
            ).decode("utf-8"))
        typed_manifest_bytes = canonical_bytes(typed_manifest_value)
    evidence["task7_candidate_manifest_hex"] = (
        candidate_manifest_bytes.hex()
    )
    evidence["task7_selection_hex"] = selection_bytes.hex()
    evidence["task7_typed_manifest_hex"] = typed_manifest_bytes.hex()
    run_id = "run:" + hashlib.sha256(
        b"historical_foundry_run_id/v1\0"
        + candidate_manifest_bytes + typed_manifest_bytes + selection_bytes
    ).hexdigest()
    evidence["run_id"] = run_id
    evidence["snapshot_run_id"] = run_id


def install_task7_run_preimage(evidence):
    selection = task7_selection_value()
    replace_task7_run_preimage(
        evidence,
        candidate_manifest_value=task7_candidate_manifest_value(selection),
        selection_value=selection,
        typed_manifest_value=task7_typed_manifest_value(evidence),
    )


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
    selected = {"anchor_timestamp": ANCHOR_TIME,
                "block_timestamp": BLOCK_TIME,
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
        "scan_inventory_sha256": SCAN_SHA,
        "selection": selected,
        "selection_sha256": digest(selected),
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
    install_task7_run_preimage(evidence)
    evidence["evidence_sha256"] = digest(evidence)
    return evidence


def reseal(evidence, *, selection_changed=False):
    if selection_changed:
        evidence["selection_sha256"] = digest(evidence["selection"])
    evidence["evidence_sha256"] = digest({
        key: value for key, value in evidence.items()
        if key != "evidence_sha256"
    })


class HistoricalResearchUniverseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_historical_foundry_config_set()

    def validated(self):
        return validate_selected_historical_run(
            config=self.config, run_evidence=fixture(self.config))

    def test_accepts_task7_typed_bytes_and_derives_eth_usd(self):
        validated = self.validated()
        self.assertEqual(validated["eth_usd"], "2000")
        self.assertEqual(len(validated["typed_payloads"]), 4)
        pool_payloads = validated["typed_payloads"][::2]
        self.assertEqual(
            [row["fee_proof_sha256"] for row in pool_payloads],
            [
                EXPECTED_FEE_PROOF_BY_DEX["uniswap_v2"],
                EXPECTED_FEE_PROOF_BY_DEX["sushiswap_v2"],
            ],
        )
        for price in validated["typed_payloads"][1::2]:
            self.assertEqual(price["answer"], str(PRICE_ANSWER))
            self.assertEqual(price["decimals"], str(PRICE_DECIMALS))
            self.assertEqual(price["scan_inventory_sha256"], SCAN_SHA)

    def test_task7_run_id_preimage_rejects_resealed_run_alias(self):
        value = fixture(self.config)
        value["run_id"] = "run:" + "2" * 64
        value["snapshot_run_id"] = value["run_id"]
        reseal(value)
        with self.assertRaises(ValueError):
            validate_selected_historical_run(
                config=self.config, run_evidence=value)

    def test_task7_run_id_requires_candidate_typed_selection_order(self):
        for label, order in (
            ("candidate_omitted", ("typed", "selection")),
            ("candidate_last", ("selection", "typed", "candidate")),
            ("typed_last", ("candidate", "selection", "typed")),
        ):
            value = fixture(self.config)
            parts = {
                "candidate": bytes.fromhex(
                    value["task7_candidate_manifest_hex"]
                ),
                "typed": bytes.fromhex(value["task7_typed_manifest_hex"]),
                "selection": bytes.fromhex(value["task7_selection_hex"]),
            }
            bad_run_id = "run:" + hashlib.sha256(
                b"historical_foundry_run_id/v1\0"
                + b"".join(parts[name] for name in order)
            ).hexdigest()
            value["run_id"] = bad_run_id
            value["snapshot_run_id"] = bad_run_id
            reseal(value)
            with self.subTest(label=label), self.assertRaises(ValueError):
                validate_selected_historical_run(
                    config=self.config, run_evidence=value)

    def test_task7_candidate_manifest_is_closed_against_selection(self):
        def mutate_staging(candidate):
            candidate["staging_inventory_sha256"] = "8" * 64

        def mutate_attempted_count(candidate):
            candidate["attempted_scenario_count"] -= 1

        def mutate_state(candidate):
            candidate["candidate_states"][0]["scenario_count"] -= 1

        def mutate_selected_fact(candidate):
            candidate["scenarios"][0]["result_sha256"] = "0" * 64

        def mutate_scenario_order(candidate):
            candidate["scenarios"].reverse()

        def mutate_extra_field(candidate):
            candidate["caller_note"] = "accepted"

        for label, mutate in (
            ("staging", mutate_staging),
            ("attempted_count", mutate_attempted_count),
            ("candidate_state", mutate_state),
            ("selected_fact", mutate_selected_fact),
            ("scenario_order", mutate_scenario_order),
            ("extra_field", mutate_extra_field),
        ):
            value = fixture(self.config)
            candidate = json.loads(bytes.fromhex(
                value["task7_candidate_manifest_hex"]
            ).decode("utf-8"))
            mutate(candidate)
            replace_task7_run_preimage(
                value, candidate_manifest_value=candidate
            )
            reseal(value)
            with self.subTest(label=label), self.assertRaises(ValueError):
                validate_selected_historical_run(
                    config=self.config, run_evidence=value)

        value = fixture(self.config)
        candidate = json.loads(bytes.fromhex(
            value["task7_candidate_manifest_hex"]
        ).decode("utf-8"))
        noncanonical = json.dumps(candidate, sort_keys=True).encode("utf-8")
        replace_task7_run_preimage(
            value, candidate_manifest_bytes=noncanonical
        )
        reseal(value)
        with self.assertRaises(ValueError):
            validate_selected_historical_run(
                config=self.config, run_evidence=value)

    def test_task7_candidate_manifest_accepts_prior_closed_revert(self):
        value = fixture(self.config)
        selected = json.loads(bytes.fromhex(
            value["task7_selection_hex"]
        ).decode("utf-8"))
        selected["candidate_block_count"] = 2
        selected["scenario_denominator"] = 20
        selected["initial_replay_required_count"] = 10
        selected["candidate_states"].insert(0, {
            "block_number": 20_000_001,
            "state": "nonpublishable_positive",
            "transitions": [
                "candidate", "replaying_required", "tentative_positive",
                "completing_full_ten", "nonpublishable_positive",
            ],
            "scenario_count": 10,
        })
        candidate = task7_candidate_manifest_value(selected)
        prior = copy.deepcopy(candidate["scenarios"])
        for index, row in enumerate(prior):
            row["block_number"] = 20_000_001
            row["scenario_key"] = row["scenario_key"].replace(
                "20000000:", "20000001:", 1
            )
            for field in (
                "proof_inputs_hash", "overlay_sha256", "receipt_sha256",
                "trace_sha256", "result_sha256",
            ):
                row[field] = hashlib.sha256(
                    "prior:{}:{}".format(field, index).encode("ascii")
                ).hexdigest()
        prior[0].update({
            "status": 0,
            "classification": "closed_revert",
            "weth_delta_raw": 0,
            "proof_inputs_hash": None,
            "economics": None,
        })
        candidate["scenarios"] = prior + candidate["scenarios"]
        candidate["attempted_scenario_count"] = 20
        replace_task7_run_preimage(
            value, candidate_manifest_value=candidate,
            selection_value=selected,
        )
        reseal(value)
        validate_selected_historical_run(
            config=self.config, run_evidence=value)

    def test_task7_selection_must_be_canonical_and_cross_bound(self):
        mutations = (
            ("selected_block", lambda selected: selected["selected_block"].__setitem__(
                "number", selected["selected_block"]["number"] + 1)),
            ("scan_inventory", lambda selected: selected.__setitem__(
                "staging_inventory_sha256", "8" * 64)),
            ("scenario_status", lambda selected: selected[
                "selected_scenarios"
            ][0].__setitem__("status", 0)),
        )
        for label, mutate in mutations:
            value = fixture(self.config)
            selected = json.loads(bytes.fromhex(
                value["task7_selection_hex"]
            ).decode("utf-8"))
            mutate(selected)
            replace_task7_run_preimage(value, selection_value=selected)
            reseal(value)
            with self.subTest(label=label), self.assertRaises(ValueError):
                validate_selected_historical_run(
                    config=self.config, run_evidence=value)

        value = fixture(self.config)
        selected = json.loads(bytes.fromhex(
            value["task7_selection_hex"]
        ).decode("utf-8"))
        noncanonical = json.dumps(selected, sort_keys=True).encode("utf-8")
        replace_task7_run_preimage(value, selection_bytes=noncanonical)
        reseal(value)
        with self.assertRaises(ValueError):
            validate_selected_historical_run(
                config=self.config, run_evidence=value)

    def test_task7_selection_accepts_mixed_verified_economics_but_requires_positive(self):
        value = fixture(self.config)
        selected = json.loads(bytes.fromhex(
            value["task7_selection_hex"]
        ).decode("utf-8"))
        nets = [
            row["economics"]["policy_net_edge_usd"]["numerator"]
            for row in selected["selected_scenarios"]
        ]
        self.assertEqual(set(nets), {-1, 0, 1})
        validate_selected_historical_run(
            config=self.config, run_evidence=value)

        for row in selected["selected_scenarios"]:
            row["economics"]["policy_net_edge_usd"] = {
                "numerator": 0, "denominator": 1, "display": "0",
            }
        replace_task7_run_preimage(
            value,
            candidate_manifest_value=task7_candidate_manifest_value(
                selected
            ),
            selection_value=selected,
        )
        reseal(value)
        with self.assertRaises(ValueError):
            validate_selected_historical_run(
                config=self.config, run_evidence=value)

    def test_task7_typed_manifest_is_closed_after_full_preimage_rehash(self):
        def mutate_selected_block(manifest):
            manifest["selected_block"]["number"] += 1

        def mutate_factory(manifest):
            manifest["markets"][0]["factory_pair_forward"] = "0x" + "1" * 40

        def mutate_market_order(manifest):
            manifest["markets"].reverse()

        def mutate_member_hash(manifest):
            manifest["markets"][0]["members"][0]["sha256"] = "0" * 64

        def mutate_extra_field(manifest):
            manifest["caller_note"] = "accepted"

        for label, mutate in (
            ("selected_block", mutate_selected_block),
            ("factory", mutate_factory),
            ("market_order", mutate_market_order),
            ("member_hash", mutate_member_hash),
            ("extra_field", mutate_extra_field),
        ):
            value = fixture(self.config)
            manifest = json.loads(bytes.fromhex(
                value["task7_typed_manifest_hex"]
            ).decode("utf-8"))
            mutate(manifest)
            replace_task7_run_preimage(value, typed_manifest_value=manifest)
            reseal(value)
            with self.subTest(label=label), self.assertRaises(ValueError):
                validate_selected_historical_run(
                    config=self.config, run_evidence=value)

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

    def test_full_projections_ignore_hostile_global_decimal_rounding(self):
        validated = self.validated()
        projections = {}
        for rounding in (ROUND_HALF_EVEN, ROUND_DOWN, ROUND_UP):
            with localcontext() as ambient:
                ambient.rounding = rounding
                universe = build_historical_research_universe(
                    config=self.config, validated_run=validated,
                )
                core = build_historical_core_projection(
                    config=self.config, validated_run=validated,
                    universe=universe,
                )
                projections[rounding] = (
                    canonical_bytes(universe), canonical_bytes(core),
                )
        self.assertEqual(
            projections[ROUND_DOWN], projections[ROUND_HALF_EVEN]
        )
        self.assertEqual(
            projections[ROUND_UP], projections[ROUND_HALF_EVEN]
        )

    def test_core_projection_cross_binds_universe(self):
        validated = self.validated()
        universe = build_historical_research_universe(config=self.config, validated_run=validated)
        core = build_historical_core_projection(config=self.config, validated_run=validated, universe=universe)
        self.assertEqual(core["schema"], "historical_core_projection/v1")
        self.assertEqual(core["universe_sha256"], digest(universe))
        self.assertEqual(len(core["typed_members"]), 4)

    def test_builders_reject_plain_mapping_validation_bypass(self):
        validated = self.validated()
        universe = build_historical_research_universe(
            config=self.config, validated_run=validated)
        forged = json.loads(canonical_bytes(validated))
        forged["authority_sha256"] = "0" * 64
        forged["toolchain_sha256"] = "0" * 64
        forged["evidence_sha256"] = "0" * 64
        forged["typed_members"] = []
        forged["selection"]["venues"]["uniswap_v2"]["pair_address"] = (
            "0x" + "1" * 40
        )
        with self.assertRaises(ValueError):
            build_historical_research_universe(
                config=self.config, validated_run=forged)
        with self.assertRaises(ValueError):
            build_historical_core_projection(
                config=self.config, validated_run=forged, universe=universe)
        forged_capability = object.__new__(type(validated))
        with self.assertRaises(ValueError):
            build_historical_research_universe(
                config=self.config, validated_run=forged_capability)

    def test_module_registry_injection_cannot_mint_builder_capability(self):
        """A module-visible issuer/registry must not authorize a forged object."""
        import scripts.historical_foundry_replay as module

        validated = self.validated()
        universe = build_historical_research_universe(
            config=self.config, validated_run=validated)
        forged = object.__new__(type(validated))
        missing = object()
        prior_issuer = getattr(module, "_VALIDATED_RUN_ISSUER", missing)
        prior_registry = getattr(module, "_VALIDATED_RUN_REGISTRY", missing)
        injected_issuer = (
            prior_issuer if prior_issuer is not missing else object()
        )
        injected_registry = (
            prior_registry if isinstance(prior_registry, dict) else {}
        )
        if prior_issuer is missing:
            module._VALIDATED_RUN_ISSUER = injected_issuer
        if prior_registry is missing:
            module._VALIDATED_RUN_REGISTRY = injected_registry
        injected_registry[id(forged)] = (
            weakref.ref(forged),
            {"issuer": injected_issuer, "projection": validated},
        )
        try:
            with self.assertRaises(ValueError):
                build_historical_research_universe(
                    config=self.config, validated_run=forged)
            with self.assertRaises(ValueError):
                build_historical_core_projection(
                    config=self.config, validated_run=forged,
                    universe=universe,
                )
        finally:
            injected_registry.pop(id(forged), None)
            if prior_issuer is missing:
                del module._VALIDATED_RUN_ISSUER
            if prior_registry is missing:
                del module._VALIDATED_RUN_REGISTRY
        for name in (
            "_ValidatedHistoricalRun", "_issue_validated_run",
            "_VALIDATED_RUN_ISSUER", "_VALIDATED_RUN_REGISTRY",
            "_initialize_validated_historical_run_capability",
        ):
            self.assertFalse(hasattr(module, name), name)

    def test_descriptor_paths_and_exact_scalar_types_are_closed(self):
        mutations = [
            ("pool_path", lambda x: x["typed_members"][0]["descriptor"].__setitem__(
                "path", "../../transplanted.json")),
            ("pool_filename", lambda x: x["typed_members"][0]["descriptor"].__setitem__(
                "filename", "transplanted.json")),
            ("float_notional", lambda x: x["scenarios"][0].__setitem__(
                "requested_notional_usd", 1000.0)),
            ("boolean_status", lambda x: x["scenarios"][0].__setitem__(
                "receipt_status", True)),
            ("noncanonical_hex", lambda x: x["typed_members"][0].__setitem__(
                "payload_hex", x["typed_members"][0]["payload_hex"].upper())),
            ("integer_hash", lambda x: x.__setitem__(
                "manifest_sha256", int("1" * 64))),
        ]
        for label, mutate in mutations:
            value = fixture(self.config)
            mutate(value)
            reseal(value)
            with self.subTest(label=label), self.assertRaises(ValueError):
                validate_selected_historical_run(
                    config=self.config, run_evidence=value)

    def test_task7_price_contexts_must_be_identical_after_rehash(self):
        value = fixture(self.config)
        member = value["typed_members"][3]
        payload = json.loads(bytes.fromhex(member["payload_hex"]).decode("utf-8"))
        payload["answer"] = str(PRICE_ANSWER + 1)
        raw = canonical_bytes(payload)
        member["payload_hex"] = raw.hex()
        member["descriptor"]["byte_count"] = len(raw)
        member["descriptor"]["sha256"] = hashlib.sha256(raw).hexdigest()
        member["descriptor"]["logical_generation"] = hashlib.sha256(raw).hexdigest()
        reseal(value)
        with self.assertRaises(ValueError):
            validate_selected_historical_run(config=self.config, run_evidence=value)

    def test_self_consistent_alternative_pair_is_rejected_after_full_rehash(self):
        value = fixture(self.config)
        new_pool = "0x" + "1" * 40
        venue = value["selection"]["venues"]["uniswap_v2"]
        venue["pair_address"] = new_pool
        venue["factory_pair_forward"] = new_pool
        venue["factory_pair_reverse"] = new_pool
        routes = []
        for buy, sell in (("uniswap_v2", "sushiswap_v2"),
                          ("sushiswap_v2", "uniswap_v2")):
            identity = {
                "token_symbol": "UNI",
                "buy_market_id": "dex:eth:{}:{}:UNI".format(
                    buy, value["selection"]["venues"][buy]["pair_address"]),
                "sell_market_id": "dex:eth:{}:{}:UNI".format(
                    sell, value["selection"]["venues"][sell]["pair_address"]),
                "route_mode": "atomic_onchain",
            }
            routes.append({**identity, "route_id": canonical_route_id(identity)})
        value["selection"]["routes"] = routes
        for index, scenario in enumerate(value["scenarios"]):
            scenario["route_id"] = routes[0 if index < 5 else 1]["route_id"]
        value["typed_members"][0] = typed_member(
            self.config, "uniswap_v2", new_pool,
            100 * 10 ** 18, 10_000 * 10 ** 18,
        )
        value["typed_members"][1] = usd_member("uniswap_v2", new_pool)
        install_task7_run_preimage(value)
        reseal(value, selection_changed=True)
        with self.assertRaises(ValueError):
            validate_selected_historical_run(
                config=self.config, run_evidence=value)

    def test_pure_bridge_performs_no_io(self):
        value = fixture(self.config)
        import os
        import socket
        import subprocess

        del os, socket, subprocess
        denied = AssertionError("pure historical bridge attempted I/O")
        with ExitStack() as stack:
            for target in (
                "builtins.open", "os.open", "os.stat", "socket.socket",
                "subprocess.run", "subprocess.Popen",
            ):
                stack.enter_context(mock.patch(target, side_effect=denied))
            validated = validate_selected_historical_run(
                config=self.config, run_evidence=value)
            universe = build_historical_research_universe(
                config=self.config, validated_run=validated)
            build_historical_core_projection(
                config=self.config, validated_run=validated, universe=universe)

    def test_pure_bridge_never_calls_live_shadow_wrappers(self):
        import scripts.route_publication as publication
        import scripts.route_quantity as quantity
        import scripts.route_universe as universe_module

        denied = AssertionError("historical bridge called a live wrapper")
        with ExitStack() as stack:
            for module, name in (
                (publication, "publish_route_cohort_bundle"),
                (publication, "load_latest_route_cohort"),
                (quantity, "quote_v2_pool_quantity"),
                (quantity, "validate_v2_quantity_quote_against_state"),
                (universe_module, "build_route_universe"),
            ):
                stack.enter_context(mock.patch.object(module, name, side_effect=denied))
            validated = self.validated()
            historical_universe = build_historical_research_universe(
                config=self.config, validated_run=validated)
            build_historical_core_projection(
                config=self.config, validated_run=validated,
                universe=historical_universe,
            )

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


class HistoricalOpportunityBridgeTests(unittest.TestCase):
    @staticmethod
    def _open_stage():
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, lease, _identity = (
            HistoricalCorePublicationTests._open_real_task7_lease()
        )
        stage = context = None
        try:
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir,
                config=run["config"],
                publication_lease=lease,
            )
            lease = None
            context = publication.load_validated_historical_replay_core_at(
                staged_core=stage
            )
            return run, finalized, stage, context
        except BaseException:
            if context is not None:
                context.close()
            if stage is not None:
                stage.close()
            if lease is not None:
                lease.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )
            raise

    @staticmethod
    def _close_stage(run, finalized, stage, context):
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        try:
            if context is not None:
                context.close()
        finally:
            try:
                if stage is not None:
                    stage.close()
            finally:
                HistoricalCorePublicationTests._close_real_task7_run(
                    run, finalized
                )

    @staticmethod
    def _swap_calldata(
        *, amount_in_raw, path, recipient, deadline,
    ):
        def word(value):
            return int(value).to_bytes(32, "big")

        encoded = b"\x38\xed\x17\x39" + b"".join((
            word(amount_in_raw), word(0), word(160),
            b"\0" * 12 + bytes.fromhex(recipient[2:]),
            word(deadline), word(2),
            b"\0" * 12 + bytes.fromhex(path[0][2:]),
            b"\0" * 12 + bytes.fromhex(path[1][2:]),
        ))
        return encoded

    @classmethod
    def _expected_successful_calls(cls, projection):
        direction = projection["selection_scenario"]["direction"]
        first = projection["prefilter_scenario"]["first_amount_out_raw"]
        amount = projection["prefilter_scenario"]["amount_weth_in_wei"]
        executor = projection["overlay"]["transaction"]["to"]
        deadline = projection["overlay"]["synthetic_block"]["timestamp"] + 60
        if direction == "uniswap_to_sushiswap":
            routers = (
                "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
                "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",
            )
        else:
            routers = (
                "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",
                "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
            )
        rows = []
        for call_path, leg, router, amount_in, path in (
            ([2], "first_leg", routers[0], amount, [WETH, UNI]),
            ([5], "second_leg", routers[1], first, [UNI, WETH]),
        ):
            calldata = cls._swap_calldata(
                amount_in_raw=amount_in, path=path,
                recipient=executor, deadline=deadline,
            )
            rows.append({
                "call_path": call_path,
                "leg": leg,
                "router": router,
                "calldata_sha256": hashlib.sha256(calldata).hexdigest(),
                "amount_in_raw": amount_in,
                "amount_out_min_raw": 0,
                "path": path,
                "recipient": executor,
                "deadline": deadline,
                "value": 0,
            })
        return rows

    @staticmethod
    def _expected_post_pair_state(projection):
        direction = projection["selection_scenario"]["direction"]
        amount = projection["prefilter_scenario"]["amount_weth_in_wei"]
        first = projection["prefilter_scenario"]["first_amount_out_raw"]
        second = projection["prefilter_scenario"]["second_amount_out_raw"]
        buy, sell = (
            ("uniswap_v2", "sushiswap_v2")
            if direction == "uniswap_to_sushiswap"
            else ("sushiswap_v2", "uniswap_v2")
        )
        result = copy.deepcopy(projection["result"]["pair_closure"])
        result[buy]["reserve_uni_raw"] -= first
        result[buy]["pair_uni_balance_raw"] -= first
        result[buy]["reserve_weth_raw"] += amount
        result[buy]["pair_weth_balance_raw"] += amount
        result[sell]["reserve_uni_raw"] += first
        result[sell]["pair_uni_balance_raw"] += first
        result[sell]["reserve_weth_raw"] -= second
        result[sell]["pair_weth_balance_raw"] -= second
        return result

    @staticmethod
    def _scenario_bytes(value):
        result = copy.deepcopy(value)
        result.pop("scenario_projection_sha256", None)
        result["scenario_projection_sha256"] = digest(result)
        return canonical_bytes(result)

    def test_apparent_issuer_symbols_cannot_mint_arbitrary_capabilities(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            legitimate = publication._issue_validated_historical_scenario_inputs(
                context=context,
                scenario_key="2:uniswap_to_sushiswap:1000",
            )
            projection = json.loads(legitimate.canonical_projection_bytes)
            raw_scenario_issuer = getattr(
                replay,
                "_issue_validated_historical_scenario_inputs_from_publication",
                None,
            )
            if raw_scenario_issuer is not None:
                with self.assertRaises((TypeError, ValueError)):
                    forged = raw_scenario_issuer(
                        scenario_key=legitimate.scenario_key,
                        context_projection_sha256=(
                            legitimate.context_projection_sha256
                        ),
                        source_descriptor_set_sha256=(
                            legitimate.source_descriptor_set_sha256
                        ),
                        proof_inputs_hash=legitimate.proof_inputs_hash,
                        canonical_projection_bytes=canonical_bytes(projection),
                    )
                    replay.build_historical_atomic_v2_cashflow(forged)
            raw_proof_issuer = getattr(
                publication,
                "_issue_validated_historical_cost_proof_inputs",
                None,
            )
            if raw_proof_issuer is not None:
                with self.assertRaises((TypeError, ValueError)):
                    raw_proof_issuer(projection["proof_inputs"])
            self.assertIsNone(raw_scenario_issuer)
            self.assertIsNone(raw_proof_issuer)
            self.assertFalse(hasattr(publication, "_SCENARIO_CONTEXT_ISSUER"))
            apparent_replay = tuple(
                name for name in dir(replay)
                if "issue" in name.lower()
                and "historical_scenario" in name.lower()
            )
            apparent_publication = tuple(
                name for name in dir(publication)
                if "issue" in name.lower()
                and "historical_cost_proof" in name.lower()
            )
            self.assertEqual(apparent_replay, ())
            self.assertEqual(apparent_publication, ())
            with self.assertRaises((TypeError, ValueError)):
                publication._issue_validated_historical_scenario_inputs(
                    context=object(),
                    scenario_key="2:uniswap_to_sushiswap:1000",
                )
            with self.assertRaises(TypeError):
                publication.load_historical_cost_proof_inputs_for_build_context(
                    proof=projection["proof_inputs"]
                )
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_preloaded_publication_module_cannot_capture_raw_mint(self):
        import subprocess
        import sys

        attack = r'''\
import importlib
import importlib.machinery
import sys
import types
from pathlib import Path

publication_name = "scripts.historical_route_publication"
publication_path = (
    Path.cwd() / "scripts" / "historical_route_publication.py"
).resolve()
captured = {}
alternate = types.ModuleType(publication_name)
alternate.__file__ = str(publication_path)
alternate.__loader__ = importlib.machinery.SourceFileLoader(
    publication_name, str(publication_path),
)
alternate.__spec__ = importlib.machinery.ModuleSpec(
    publication_name,
    loader=alternate.__loader__,
    origin=str(publication_path),
)
alternate.__dict__["captured"] = captured
exec(compile(
    "def _install_historical_scenario_capability(**authority):\n"
    "    captured.update(authority)\n",
    str(publication_path),
    "exec",
), alternate.__dict__)
sys.modules[publication_name] = alternate

replay = importlib.import_module("scripts.historical_foundry_replay")
if captured:
    raise SystemExit("preloaded publication captured scenario authority")
binder = getattr(
    replay, "_bind_historical_scenario_capability_to_publication", None
)
if binder is None:
    raise SystemExit("replay binder disappeared before authentic publication")
try:
    binder(alternate._install_historical_scenario_capability)
except ValueError:
    pass
else:
    raise SystemExit("preloaded publication installer was accepted")
if captured:
    raise SystemExit("rejected publication captured scenario authority")

del sys.modules[publication_name]
publication = importlib.import_module(publication_name)
if publication.ValidatedHistoricalScenarioInputs is not (
    replay.ValidatedHistoricalScenarioInputs
):
    raise SystemExit("publication does not use the replay-owned capability")
if getattr(
    replay, "_bind_historical_scenario_capability_to_publication", None
) is not None:
    raise SystemExit("scenario capability binder remains after authentic import")
'''
        completed = subprocess.run(
            [sys.executable, "-c", attack],
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            completed.returncode,
            0,
            completed.stdout + completed.stderr,
        )

    def test_replay_rejects_canonical_projection_bytes_before_arithmetic(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            inputs = publication._issue_validated_historical_scenario_inputs(
                context=context,
                scenario_key="2:uniswap_to_sushiswap:1000",
            )
            with mock.patch.object(
                replay,
                "_run_historical_v2_exact_input_kat",
                side_effect=AssertionError("arithmetic reached"),
            ):
                with self.assertRaises(ValueError):
                    replay.build_historical_atomic_v2_cashflow(
                        inputs.canonical_projection_bytes
                    )
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_installed_scenario_issuer_captures_exact_material_validator(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            diverted = AssertionError("module-global material loader reached")
            with mock.patch.object(
                publication, "_historical_scenario_material",
                side_effect=diverted,
            ):
                inputs = publication._issue_validated_historical_scenario_inputs(
                    context=context,
                    scenario_key="2:uniswap_to_sushiswap:1000",
                )
            self.assertEqual(
                replay.build_historical_atomic_v2_cashflow(inputs)[
                    "first_weth_in_raw"
                ],
                333333333333333333,
            )
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_cashflow_rejects_each_retained_execution_projection_attack(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            inputs = publication._issue_validated_historical_scenario_inputs(
                context=context,
                scenario_key="2:uniswap_to_sushiswap:1000",
            )
            original = json.loads(inputs.canonical_projection_bytes)
            successful_calls = self._expected_successful_calls(original)
            post_pair_state = self._expected_post_pair_state(original)

            def with_retained_execution(value):
                value["trace"]["successful_calls"] = copy.deepcopy(
                    successful_calls
                )
                value["result"]["trace_closure"]["successful_calls"] = (
                    copy.deepcopy(successful_calls)
                )
                value["trace"]["post_pair_state"] = copy.deepcopy(
                    post_pair_state
                )
                value["result"]["post_pair_state"] = copy.deepcopy(
                    post_pair_state
                )

            def calldata_direction(value):
                raw = bytearray(bytes.fromhex(
                    value["overlay"]["transaction"]["input"][2:]
                ))
                raw[35] = 1
                value["overlay"]["transaction"]["input"] = "0x" + raw.hex()
                value["overlay"]["transaction"]["calldata_sha256"] = (
                    hashlib.sha256(raw).hexdigest()
                )

            def calldata_input(value):
                raw = bytearray(bytes.fromhex(
                    value["overlay"]["transaction"]["input"][2:]
                ))
                raw[-1] ^= 1
                value["overlay"]["transaction"]["input"] = "0x" + raw.hex()
                value["overlay"]["transaction"]["calldata_sha256"] = (
                    hashlib.sha256(raw).hexdigest()
                )

            mutations = [
                ("calldata_direction", calldata_direction),
                ("calldata_input", calldata_input),
                ("calldata_hash", lambda value: value["overlay"][
                    "transaction"
                ].__setitem__("calldata_sha256", "0" * 64)),
                ("prefilter_first_output", lambda value: value[
                    "prefilter_scenario"
                ].__setitem__(
                    "first_amount_out_raw",
                    value["prefilter_scenario"]["first_amount_out_raw"] + 1,
                )),
                ("prefilter_second_output", lambda value: value[
                    "prefilter_scenario"
                ].__setitem__(
                    "second_amount_out_raw",
                    value["prefilter_scenario"]["second_amount_out_raw"] + 1,
                )),
                ("trace_first_output", lambda value: value["trace"][
                    "actual_deltas"
                ].__setitem__(
                    "first_leg_uni_raw",
                    value["trace"]["actual_deltas"]["first_leg_uni_raw"] + 1,
                )),
                ("trace_second_output", lambda value: value["trace"][
                    "balances"
                ].__setitem__(
                    "final_weth_raw",
                    value["trace"]["balances"]["final_weth_raw"] + 1,
                )),
                ("first_router_input", lambda value: value["trace"][
                    "successful_calls"
                ][0].__setitem__(
                    "amount_in_raw",
                    value["trace"]["successful_calls"][0]["amount_in_raw"] + 1,
                )),
                ("second_router_input", lambda value: value["trace"][
                    "successful_calls"
                ][1].__setitem__(
                    "amount_in_raw",
                    value["trace"]["successful_calls"][1]["amount_in_raw"] + 1,
                )),
                ("p50_priority", lambda value: value["fee"].__setitem__(
                    "p50_priority_fee_per_gas",
                    value["receipt"]["effectiveGasPrice"]
                    - value["fee"]["next_base_fee_per_gas"] + 1,
                )),
            ]
            for venue in ("uniswap_v2", "sushiswap_v2"):
                for field in (
                    "reserve_uni_raw", "reserve_weth_raw",
                    "pair_uni_balance_raw", "pair_weth_balance_raw",
                ):
                    def mutate_post(value, venue=venue, field=field):
                        value["trace"]["post_pair_state"][venue][field] += 1

                    mutations.append(("post_{}_{}".format(venue, field), mutate_post))

            for label, mutate in mutations:
                value = copy.deepcopy(original)
                with_retained_execution(value)
                mutate(value)
                with self.subTest(label=label), self.assertRaises(ValueError):
                    with mock.patch.object(
                        replay, "_validated_historical_scenario_projection",
                        return_value=value,
                    ):
                        replay.build_historical_atomic_v2_cashflow(inputs)
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_scenario_capability_retains_p50_calls_and_post_pair_state(self):
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            inputs = publication._issue_validated_historical_scenario_inputs(
                context=context,
                scenario_key="2:uniswap_to_sushiswap:1000",
            )
            projection = json.loads(inputs.canonical_projection_bytes)
            self.assertEqual(
                projection["receipt"]["effectiveGasPrice"],
                projection["fee"]["next_base_fee_per_gas"]
                + projection["fee"]["p50_priority_fee_per_gas"],
            )
            self.assertEqual(
                projection["receipt"]["maxPriorityFeePerGas"],
                projection["fee"]["p50_priority_fee_per_gas"],
            )
            self.assertEqual(
                projection["overlay"]["transaction"]["maxPriorityFeePerGas"],
                projection["fee"]["p50_priority_fee_per_gas"],
            )
            expected_calls = self._expected_successful_calls(projection)
            self.assertEqual(
                projection["trace"]["successful_calls"], expected_calls
            )
            self.assertEqual(
                projection["result"]["trace_closure"]["successful_calls"],
                expected_calls,
            )
            expected_post = self._expected_post_pair_state(projection)
            self.assertEqual(
                projection["trace"]["post_pair_state"], expected_post
            )
            self.assertEqual(
                projection["result"]["post_pair_state"], expected_post
            )
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_compact_evidence_rejects_self_rehashed_malformed_inventory(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            projections = tuple(
                publication._build_historical_scenario_for_publication(
                    context=context,
                    scenario_key="2:{}:{}".format(direction, notional),
                )["canonical_projection_bytes"]
                for direction in (
                    "uniswap_to_sushiswap", "sushiswap_to_uniswap"
                )
                for notional in (1000, 5000, 10000, 50000, 100000)
            )

            def mutate_one(index, mutate):
                values = list(projections)
                value = json.loads(values[index])
                mutate(value)
                values[index] = self._scenario_bytes(value)
                return tuple(values)

            attacks = [
                ("empty_costs", lambda: mutate_one(
                    0, lambda value: value.__setitem__("cost_components", [])
                )),
                ("reordered_costs", lambda: mutate_one(
                    0, lambda value: value["cost_components"].reverse()
                )),
                ("altered_cost", lambda: mutate_one(
                    0, lambda value: value["cost_components"][0].__setitem__(
                        "amount_usd", "123"
                    )
                )),
                ("duplicate_proof_hash", lambda: mutate_one(
                    1, lambda value: value.__setitem__(
                        "proof_inputs_hash",
                        json.loads(projections[0])["proof_inputs_hash"],
                    )
                )),
                ("wrong_scenario_id", lambda: mutate_one(
                    0, lambda value: value.__setitem__(
                        "scenario_key", "2:uniswap_to_sushiswap:1001"
                    )
                )),
                ("wrong_opportunity_id", lambda: mutate_one(
                    0, lambda value: value["opportunity"].__setitem__(
                        "opportunity_id", "opportunity:" + "0" * 64
                    )
                )),
                ("arbitrary_economics", lambda: mutate_one(
                    0, lambda value: value["economics_scenarios"][0].__setitem__(
                        "name", "attacker"
                    )
                )),
                ("inconsistent_positive", lambda: mutate_one(
                    0, lambda value: value["economics_scenarios"][0].__setitem__(
                        "positive_research_net",
                        not value["economics_scenarios"][0][
                            "positive_research_net"
                        ],
                    )
                )),
                ("wrong_order", lambda: (
                    projections[1], projections[0], *projections[2:]
                )),
            ]
            for label, attack in attacks:
                with self.subTest(label=label), self.assertRaises(ValueError):
                    replay.build_historical_replay_evidence(attack())
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_scenario_projection_retains_independent_economics_authority(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            inputs = publication._issue_validated_historical_scenario_inputs(
                context=context,
                scenario_key="2:uniswap_to_sushiswap:1000",
            )
            sealed = json.loads(inputs.canonical_projection_bytes)
            compact = json.loads(
                replay.build_historical_scenario_projection(inputs)
            )
            self.assertEqual(compact["proof_inputs"], sealed["proof_inputs"])
            authority = compact["economics_authority"]
            self.assertEqual(authority, {
                "first_weth_in_raw": 333333333333333333,
                "first_uni_out_raw": 1328891698325589794,
                "final_weth_out_raw": 1323151972535702977,
                "eth_usd_answer": 300000000000,
                "feed_decimals": 8,
                "fee_numerator": 997,
                "fee_denominator": 1000,
                "second_leg_reserve_uni_raw": 1000000000000000000000,
                "second_leg_reserve_weth_raw": 1000000000000000000000,
                "gas_used": 123456,
                "receipt_effective_gas_price": 1,
                "next_base_fee_per_gas": 0,
                "p50_priority_fee_per_gas": 1,
                "p90_priority_fee_per_gas": 2,
            })
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_compact_evidence_rejects_fully_rebound_proof_row_and_gas_attacks(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            projections = tuple(
                publication._build_historical_scenario_for_publication(
                    context=context,
                    scenario_key="2:{}:{}".format(direction, notional),
                )["canonical_projection_bytes"]
                for direction in (
                    "uniswap_to_sushiswap", "sushiswap_to_uniswap"
                )
                for notional in (1000, 5000, 10000, 50000, 100000)
            )

            def rebind(value):
                proof = value["proof_inputs"]
                proof_unsigned = {
                    key: item for key, item in proof.items()
                    if key != "proof_inputs_hash"
                }
                proof_hash = typed_digest(
                    "historical_foundry_cost_proof_inputs/v1",
                    proof_unsigned,
                )
                proof["proof_inputs_hash"] = proof_hash
                value["proof_inputs_hash"] = proof_hash
                opportunity = value["opportunity"]
                opportunity["inventory_profile_hash"] = proof_hash
                opportunity["mode_evidence_sha256"] = typed_digest(
                    "historical_atomic_mode_evidence/v1",
                    {
                        "scenario_key": value["scenario_key"],
                        "proof_inputs_hash": proof_hash,
                    },
                )
                opportunity["cost_component_set_sha256"] = digest(
                    value["cost_components"]
                )
                opportunity.pop("evidence_binding_sha256", None)
                opportunity["evidence_binding_sha256"] = digest(opportunity)
                value.pop("scenario_projection_sha256", None)
                value["scenario_projection_sha256"] = digest(value)
                return canonical_bytes(value)

            def attacked(mutate):
                values = list(projections)
                value = json.loads(values[0])
                mutate(value)
                values[0] = rebind(value)
                return tuple(values)

            def alter_sell_pool_fee(value):
                value["proof_inputs"]["rows"][3][
                    "amount_usd_exact"
                ] = "123"
                value["cost_components"][3]["amount_usd"] = "123"

            def alter_proof_row(value):
                value["proof_inputs"]["rows"][0]["proof_sha256"] = "0" * 64

            def alter_displayed_gas_used(value):
                for row in value["economics_scenarios"]:
                    row["gas_used"] += 1

            for label, mutate in (
                ("sell_pool_fee", alter_sell_pool_fee),
                ("proof_row", alter_proof_row),
                ("displayed_gas_used", alter_displayed_gas_used),
            ):
                with self.subTest(label=label), self.assertRaises(ValueError):
                    replay.build_historical_replay_evidence(attacked(mutate))
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_compact_evidence_rejects_fully_rebound_final_output_attack(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            projections = list(
                publication._build_historical_scenario_for_publication(
                    context=context,
                    scenario_key="2:{}:{}".format(direction, notional),
                )["canonical_projection_bytes"]
                for direction in (
                    "uniswap_to_sushiswap", "sushiswap_to_uniswap"
                )
                for notional in (1000, 5000, 10000, 50000, 100000)
            )
            value = json.loads(projections[0])
            authority = value["economics_authority"]
            authority["final_weth_out_raw"] += 1
            value["gross_edge_weth_raw"] = (
                authority["final_weth_out_raw"]
                - authority["first_weth_in_raw"]
            )
            usd_denominator = 10 ** (18 + authority["feed_decimals"])
            gross_buy = Fraction(
                authority["first_weth_in_raw"]
                * authority["eth_usd_answer"],
                usd_denominator,
            )
            gross_sell = Fraction(
                authority["final_weth_out_raw"]
                * authority["eth_usd_answer"],
                usd_denominator,
            )
            gross_edge = gross_sell - gross_buy
            opportunity = value["opportunity"]
            bounded_cost = Fraction(Decimal(
                opportunity["research_bounded_cost_usd"]
            ))
            assumed_cost = Fraction(Decimal(
                opportunity["research_assumed_cost_usd"]
            ))
            research_net = gross_edge - bounded_cost - assumed_cost
            opportunity["gross_sell_proceeds_usd"] = (
                replay._exact_fraction_decimal(gross_sell)
            )
            opportunity["gross_edge_usd"] = replay._exact_fraction_decimal(
                gross_edge
            )
            opportunity["strict_net_edge_usd"] = (
                replay._exact_fraction_decimal(gross_edge)
            )
            opportunity["research_net_edge_usd"] = (
                replay._exact_fraction_decimal(research_net)
            )
            for prefix, edge in (
                ("gross", gross_edge),
                ("strict_net", gross_edge),
                ("research_net", research_net),
            ):
                bps, numerator, denominator = (
                    replay._historical_ratio_fields(edge, gross_buy)
                )
                opportunity[prefix + "_edge_bps"] = bps
                opportunity[prefix + "_edge_bps_numerator"] = numerator
                opportunity[prefix + "_edge_bps_denominator"] = denominator
            opportunity.pop("evidence_binding_sha256", None)
            opportunity["evidence_binding_sha256"] = digest(opportunity)
            for row in value["economics_scenarios"]:
                gas = Fraction(Decimal(row["gas_cost_usd"]))
                mev = Fraction(Decimal(row["mev_buffer_usd"]))
                net = gross_edge - gas - mev
                row["research_net_edge_usd"] = (
                    replay._exact_fraction_decimal(net)
                )
                row["positive_research_net"] = net > 0
            projections[0] = self._scenario_bytes(value)
            with self.assertRaises(ValueError):
                replay.build_historical_replay_evidence(tuple(projections))
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_publication_rejects_rehashed_raw_fee_adapter_and_proof_attacks(self):
        import scripts.historical_route_publication as publication

        scenario_key = "2:uniswap_to_sushiswap:1000"

        def opened_run_root(run):
            manifests = tuple(
                run["fixture"].data_dir.rglob("run_manifest.json")
            )
            self.assertEqual(len(manifests), 1)
            return manifests[0].parent

        def commit_replacements(root, replacements, manifest_updates=None):
            manifest_path = root / "run_manifest.json"
            manifest = json.loads(manifest_path.read_bytes())
            by_path = {row["path"]: row for row in manifest["members"]}
            for relative, raw in replacements.items():
                (root / relative).write_bytes(raw)
                by_path[relative]["byte_count"] = len(raw)
                by_path[relative]["sha256"] = hashlib.sha256(raw).hexdigest()
            if manifest_updates is not None:
                manifest_updates(manifest, replacements)
            manifest_path.write_bytes(canonical_bytes(manifest))

        def mutate_fee(run):
            root = opened_run_root(run)
            inventory_path = root / "scan/capture_inventory.json"
            inventory = json.loads(inventory_path.read_bytes())
            descriptor = next(
                row for row in inventory["typed_chunks"]
                if row["role"] == "fees"
            )
            fee_path = root / descriptor["path"]
            rows = json.loads(gzip.decompress(fee_path.read_bytes()))
            selected = next(
                row for row in rows if row["block_number"] == 2
            )
            selected["p90_priority_fee_per_gas"] += 1
            decoded = canonical_bytes(rows)
            compressed = gzip.compress(decoded, mtime=0)
            descriptor["decoded_byte_count"] = len(decoded)
            descriptor["decoded_sha256"] = hashlib.sha256(decoded).hexdigest()
            descriptor["gzip_byte_count"] = len(compressed)
            descriptor["gzip_sha256"] = hashlib.sha256(compressed).hexdigest()
            inventory_raw = canonical_bytes(inventory)
            commit_replacements(root, {
                descriptor["path"]: compressed,
                "scan/capture_inventory.json": inventory_raw,
            })

        def mutate_proof(run, *, replace_adapter):
            root = opened_run_root(run)
            result_path = next(
                path for path in root.rglob("result.json")
                if scenario_key in str(path)
                and "10000" not in str(path)
                and "100000" not in str(path)
            )
            result_relative = result_path.relative_to(root).as_posix()
            result = json.loads(result_path.read_bytes())
            proof = result["cost_proof_inputs"]
            replacements = {}
            toolchain_sha = None
            if replace_adapter:
                toolchain_path = root / "toolchain.json"
                toolchain = json.loads(toolchain_path.read_bytes())
                replacement = "f" * 64
                if (
                    toolchain["executor_build"][
                        "creation_bytecode_sha256"
                    ] == replacement
                ):
                    replacement = "e" * 64
                toolchain["executor_build"][
                    "creation_bytecode_sha256"
                ] = replacement
                toolchain_raw = canonical_bytes(toolchain)
                toolchain_sha = hashlib.sha256(toolchain_raw).hexdigest()
                replacements["toolchain.json"] = toolchain_raw
                proof["adapter_proof_sha256"] = replacement
                result["proof_authority"][
                    "adapter_proof_sha256"
                ] = replacement
                result["proof_authority"]["toolchain_sha256"] = toolchain_sha
            else:
                proof["rows"][3]["amount_usd_exact"] = "123"
            unsigned = {
                key: value for key, value in proof.items()
                if key != "proof_inputs_hash"
            }
            proof["proof_inputs_hash"] = typed_digest(
                "historical_foundry_cost_proof_inputs/v1", unsigned
            )
            result_raw = canonical_bytes(result)
            replacements[result_relative] = result_raw
            selection = json.loads((root / "selection.json").read_bytes())
            selected = next(
                row for row in selection["selected_scenarios"]
                if row["scenario_key"] == scenario_key
            )
            selected["proof_inputs_hash"] = proof["proof_inputs_hash"]
            selected["result_sha256"] = hashlib.sha256(result_raw).hexdigest()
            replacements["selection.json"] = canonical_bytes(selection)

            def update_manifest(manifest, _replacements):
                if toolchain_sha is not None:
                    manifest["toolchain_sha256"] = toolchain_sha

            commit_replacements(root, replacements, update_manifest)

        for label, mutate in (
            ("p90_fee", mutate_fee),
            ("adapter", lambda run: mutate_proof(
                run, replace_adapter=True
            )),
            ("proof", lambda run: mutate_proof(
                run, replace_adapter=False
            )),
        ):
            with self.subTest(label=label):
                run, finalized, stage, context = self._open_stage()
                try:
                    mutate(run)
                    serializer = mock.Mock(
                        side_effect=AssertionError("serialization reached")
                    )
                    matrix = mock.Mock(
                        side_effect=AssertionError("matrix reached")
                    )
                    with mock.patch.object(
                        publication,
                        "build_historical_scenario_projection",
                        serializer,
                    ), mock.patch.object(
                        publication,
                        "_validate_historical_atomic_cost_component_matrix",
                        matrix,
                    ):
                        with self.assertRaises(
                            publication.HistoricalRoutePublicationError
                        ):
                            publication._build_historical_scenario_for_publication(
                                context=context, scenario_key=scenario_key
                            )
                    serializer.assert_not_called()
                    matrix.assert_not_called()
                finally:
                    self._close_stage(run, finalized, stage, context)

    def test_pure_bridge_rejects_forged_scenario_capability_before_arithmetic(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        self.assertEqual(
            [field.name for field in dataclasses.fields(
                replay.ValidatedHistoricalScenarioInputs
            )],
            [
                "scenario_key", "context_projection_sha256",
                "source_descriptor_set_sha256", "proof_inputs_hash",
                "canonical_projection_bytes",
            ],
        )
        self.assertEqual(
            [field.name for field in dataclasses.fields(
                publication.ValidatedHistoricalCostProofInputs
            )],
            ["scenario_key", "proof_inputs_hash", "object_value"],
        )
        self.assertEqual(
            tuple(inspect.signature(
                replay.build_historical_atomic_v2_cashflow
            ).parameters),
            ("inputs",),
        )
        with self.assertRaises((TypeError, ValueError)):
            replay.ValidatedHistoricalScenarioInputs()
        with self.assertRaises((TypeError, ValueError)):
            publication.ValidatedHistoricalCostProofInputs(
                "scenario", "0" * 64, {}
            )
        forged = object.__new__(replay.ValidatedHistoricalScenarioInputs)
        self.assertEqual(
            repr(forged), "ValidatedHistoricalScenarioInputs(<redacted>)"
        )
        with self.assertRaises((TypeError, ValueError)):
            pickle.dumps(forged)
        with mock.patch.object(
            replay, "v2_exact_input_amount_out_raw",
            side_effect=AssertionError("arithmetic reached"),
        ):
            with self.assertRaises(ValueError):
                replay.build_historical_atomic_v2_cashflow(forged)

    def test_same_capability_produces_byte_identical_projection(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication
        from scripts.route_opportunity import OPPORTUNITY_FIELDS

        run, finalized, stage, context = self._open_stage()
        try:
            scenario_key = "2:uniswap_to_sushiswap:1000"
            proof = publication.load_historical_cost_proof_inputs_for_build_context(
                context=context, scenario_key=scenario_key
            )
            self.assertEqual(proof.scenario_key, scenario_key)
            self.assertEqual(
                proof.object_value["schema"],
                "historical_foundry_cost_proof_inputs/v1",
            )
            self.assertEqual(len(proof.object_value["rows"]), 9)
            self.assertEqual(
                repr(proof),
                "ValidatedHistoricalCostProofInputs(<redacted>)",
            )
            with self.assertRaises((TypeError, ValueError)):
                dataclasses.replace(proof, scenario_key="transplanted")
            with self.assertRaises((TypeError, ValueError)):
                pickle.dumps(proof)
            inputs = publication._issue_validated_historical_scenario_inputs(
                context=context, scenario_key=scenario_key
            )
            with self.assertRaises((TypeError, ValueError)):
                dataclasses.replace(inputs, scenario_key="transplanted")
            cashflow = replay.build_historical_atomic_v2_cashflow(inputs)
            self.assertEqual(cashflow["first_weth_in_raw"], 333333333333333333)
            self.assertEqual(cashflow["first_uni_out_raw"], 1328891698325589794)
            self.assertEqual(cashflow["second_uni_in_raw"], 1328891698325589794)
            self.assertEqual(cashflow["final_weth_out_raw"], 1323151972535702977)
            built = replay.build_historical_route_opportunity(inputs)
            self.assertEqual(set(built["opportunity"]), OPPORTUNITY_FIELDS)
            self.assertEqual(
                built["opportunity"]["opportunity_class"],
                "research_estimate",
            )
            self.assertEqual(len(built["cost_components"]), 9)
            self.assertGreater(
                Decimal(built["opportunity"]["research_net_edge_usd"]), 0
            )
            first = replay.build_historical_scenario_projection(inputs)
            second = replay.build_historical_scenario_projection(inputs)
            self.assertEqual(first, second)
            publication._require_historical_scenario_inputs_current(
                context=context, inputs=inputs
            )
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_ten_scenarios_produce_ninety_costs_and_compact_evidence(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication
        from scripts.route_cost_topology import (
            HISTORICAL_ATOMIC_COMPONENT_MATRIX,
        )

        run, finalized, stage, context = self._open_stage()
        try:
            scenario_keys = [
                "2:{}:{}".format(direction, notional)
                for direction in (
                    "uniswap_to_sushiswap", "sushiswap_to_uniswap"
                )
                for notional in (1000, 5000, 10000, 50000, 100000)
            ]
            projections = []
            costs = []
            classes = []
            proof_hashes = set()
            economics_names = set()
            baseline_positive = []
            for scenario_key in scenario_keys:
                built = publication._build_historical_scenario_for_publication(
                    context=context, scenario_key=scenario_key
                )
                projections.append(built["canonical_projection_bytes"])
                scenario_projection = json.loads(
                    built["canonical_projection_bytes"]
                )
                economics = scenario_projection["economics_scenarios"]
                economics_names.update(row["name"] for row in economics)
                baseline_positive.append(economics[0]["positive_research_net"])
                self.assertEqual(
                    [row["priority_fee_percentile"] for row in economics],
                    [50, 90, 90],
                )
                self.assertEqual(
                    [row["mev_bps"] for row in economics],
                    ["10", "25", "50"],
                )
                costs.extend(built["cost_components"])
                classes.append(built["opportunity"]["opportunity_class"])
                proof_hashes.add(built["proof_inputs_hash"])
            self.assertEqual(len(scenario_keys), 10)
            self.assertEqual(len(costs), 90)
            self.assertEqual(len(proof_hashes), 10)
            self.assertEqual(set(classes), {"research_estimate"})
            self.assertEqual(economics_names, {
                "baseline_p50_mev_10bps",
                "stress_p90_mev_25bps",
                "stress_p90_mev_50bps",
            })
            self.assertTrue(any(baseline_positive))
            for offset in range(0, 90, 9):
                self.assertEqual(
                    tuple(
                        (
                            row["leg"], row["component_type"],
                            row["value_status"],
                            row["embedded_in_leg_quote"],
                        )
                        for row in costs[offset:offset + 9]
                    ),
                    HISTORICAL_ATOMIC_COMPONENT_MATRIX,
                )
            evidence = json.loads(
                replay.build_historical_replay_evidence(tuple(projections))
            )
            self.assertEqual(
                evidence["schema"], "historical_foundry_replay_evidence/v1"
            )
            self.assertEqual(len(evidence["scenarios"]), 10)
            self.assertEqual(evidence["opportunity_counts"], {
                "research_estimate": 10,
                "strict_eligible": 0,
                "executable_candidate": 0,
                "attested": 0,
                "unavailable": 0,
            })
            self.assertEqual(
                {row["proof_inputs_hash"] for row in evidence["scenarios"]},
                proof_hashes,
            )
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_publication_rejects_transplanted_scenario_capability_before_pure_bridge(self):
        import scripts.historical_route_publication as publication

        first = self._open_stage()
        second_context = publication.load_validated_historical_replay_core_at(
            staged_core=first[2]
        )
        try:
            inputs = publication._issue_validated_historical_scenario_inputs(
                context=first[3],
                scenario_key="2:uniswap_to_sushiswap:1000",
            )
            pure_spy = mock.Mock(
                side_effect=AssertionError("pure bridge reached")
            )
            with mock.patch.object(
                publication,
                "_issue_validated_historical_scenario_inputs",
                return_value=inputs,
            ), mock.patch.object(
                publication, "build_historical_scenario_projection", pure_spy
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication._build_historical_scenario_for_publication(
                        context=second_context,
                        scenario_key="2:uniswap_to_sushiswap:1000",
                    )
            pure_spy.assert_not_called()
        finally:
            second_context.close()
            self._close_stage(*first)

    def test_publication_rejects_stale_capability_before_matrix_and_serialization(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            scenario_key = "2:uniswap_to_sushiswap:1000"
            inputs = publication._issue_validated_historical_scenario_inputs(
                context=context, scenario_key=scenario_key
            )
            first_projection = replay.build_historical_scenario_projection(
                inputs
            )
            result_path = next(
                path for path in run["fixture"].data_dir.rglob("result.json")
                if scenario_key in str(path)
                and "10000" not in str(path)
                and "100000" not in str(path)
            )
            original = result_path.read_bytes()
            real_pure = publication.build_historical_scenario_projection
            matrix_spy = mock.Mock(
                side_effect=AssertionError("matrix validation reached")
            )

            def pure_then_mutate(capability):
                value = real_pure(capability)
                result_path.write_bytes(
                    original.replace(b'"status":1', b'"status":0', 1)
                )
                return value

            with mock.patch.object(
                publication, "build_historical_scenario_projection",
                side_effect=pure_then_mutate,
            ), mock.patch.object(
                publication,
                "_validate_historical_atomic_cost_component_matrix",
                matrix_spy,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication._build_historical_scenario_for_publication(
                        context=context, scenario_key=scenario_key
                    )
            self.assertEqual(
                replay.build_historical_scenario_projection(inputs),
                first_projection,
            )
            matrix_spy.assert_not_called()
        finally:
            self._close_stage(run, finalized, stage, context)

    def test_equivalent_staged_and_committed_capabilities_produce_identical_projection(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, stage, staged_context = self._open_stage()
        published = None
        try:
            scenario_key = "2:uniswap_to_sushiswap:1000"
            staged_inputs = publication._issue_validated_historical_scenario_inputs(
                context=staged_context, scenario_key=scenario_key
            )
            staged_projection = replay.build_historical_scenario_projection(
                staged_inputs
            )
            staged_context.close()
            staged_context = None
            published = publication.publish_historical_replay_core(
                data_dir=run["fixture"].data_dir, staged_core=stage
            )
            stage = None
            committed_inputs = publication._issue_validated_historical_scenario_inputs(
                context=published, scenario_key=scenario_key
            )
            self.assertEqual(
                staged_projection,
                replay.build_historical_scenario_projection(committed_inputs),
            )
        finally:
            if published is not None:
                published.close()
            if staged_context is not None:
                staged_context.close()
            if stage is not None:
                stage.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )

    def test_pure_bridge_performs_no_io_clock_rpc_or_subprocess(self):
        import scripts.historical_foundry_replay as replay
        import scripts.historical_route_publication as publication

        run, finalized, stage, context = self._open_stage()
        try:
            all_inputs = tuple(
                publication._issue_validated_historical_scenario_inputs(
                    context=context,
                    scenario_key="2:{}:{}".format(direction, notional),
                )
                for direction in (
                    "uniswap_to_sushiswap", "sushiswap_to_uniswap"
                )
                for notional in (1000, 5000, 10000, 50000, 100000)
            )
            inputs = all_inputs[0]
            projections = tuple(
                replay.build_historical_scenario_projection(item)
                for item in all_inputs
            )
            projection = projections[0]
            denied = AssertionError("pure historical bridge attempted I/O")
            with ExitStack() as stack:
                for target in (
                    "builtins.open", "pathlib.Path.open", "pathlib.Path.stat",
                    "pathlib.Path.read_bytes", "os.open", "os.stat",
                    "socket.socket", "subprocess.run", "subprocess.Popen",
                    "time.time", "time.time_ns",
                ):
                    stack.enter_context(mock.patch(target, side_effect=denied))
                for _repeat in range(2):
                    replay.build_historical_atomic_v2_cashflow(inputs)
                    replay.build_historical_route_opportunity(inputs)
                    self.assertEqual(
                        replay.build_historical_scenario_projection(inputs),
                        projection,
                    )
                    replay.build_historical_replay_evidence(projections)
        finally:
            self._close_stage(run, finalized, stage, context)


if __name__ == "__main__":
    unittest.main()
