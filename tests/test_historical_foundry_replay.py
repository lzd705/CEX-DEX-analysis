from __future__ import annotations

import copy
import hashlib
import json
import unittest
import weakref
from contextlib import ExitStack
from decimal import Decimal, localcontext
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


if __name__ == "__main__":
    unittest.main()
