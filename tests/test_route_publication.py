"""Tests for immutable publication of normalized route-cohort bundles."""

from __future__ import annotations

import copy
import csv
from dataclasses import replace
from decimal import Decimal, localcontext
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.route_publication import (
    build_route_cohort_sqlite,
    load_active_phase_state,
    load_historical_phase_state,
    load_latest_route_cohort,
    load_latest_shadow_result,
    load_shadow_result,
    publish_shadow_result,
    publish_route_cohort_bundle,
    validate_route_cohort_bundle,
)
from scripts.cex_fee_facts import PRIVATE_FEE_PROFILE_COLUMNS, collect_cex_fee_snapshot
from scripts.execution_cost_components import cost_component_row
from scripts.fetch_cex_depth import (
    cex_quantity_state_id,
    parse_book,
    route_quantity_quote_for_book,
    source_request,
)
from scripts.route_opportunity import (
    build_route_opportunity,
    route_opportunity_id,
    usd_projection_evidence,
)
from scripts.route_quantity import CommonTarget
from scripts.route_shadow_inputs import SourceFileIdentity, _candidate_source_generation
from scripts.token_registry import normalize_contract_address
from scripts.route_universe import route_universe_sha256
from scripts.route_inventory import (
    INVENTORY_PROFILE_COLUMNS,
    classify_route_mode_evidence,
    inventory_capacity_for_route,
    load_validated_inventory_profile,
)
from scripts.route_cost_evidence import (
    build_unavailable_route_cost_evidence_manifest,
)
from tests.test_route_opportunity import (
    atomic_v2_fixture,
    cex_leg,
    collapsed_atomic_gas_costs,
    route_and_mode,
)
import scripts.route_publication as route_publication


def _canonical_sha256(value):
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _rehash_opportunity(row):
    normalized = dict(row)
    normalized.pop("evidence_binding_sha256", None)
    normalized["evidence_binding_sha256"] = hashlib.sha256(json.dumps(
        normalized,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    return normalized


def _atomic_cost_rows(bundle, opportunity, *, collapse_route_gas=False):
    fixture = atomic_v2_fixture()
    templates = (
        collapsed_atomic_gas_costs(fixture)
        if collapse_route_gas
        else fixture["cost_components"]
    )
    notional = Decimal(opportunity["requested_notional_usd"])
    rows = []
    for template in templates:
        leg = template["leg"]
        rate = template["rate_bps"]
        rows.append(cost_component_row(
            cohort_id=bundle["route_cohort_id"],
            opportunity_id=opportunity["opportunity_id"],
            leg=leg,
            market_id=(
                opportunity[leg + "_market_id"]
                if leg in {"buy", "sell"}
                else ""
            ),
            direction=template["direction"],
            requested_notional_usd=notional,
            target_token_quantity=Decimal(
                opportunity["target_token_quantity"]
            ),
            component_type=template["component_type"],
            value_status=template["value_status"],
            amount_usd=(
                notional * Decimal(rate) / Decimal("10000")
                if rate is not None
                else None
            ),
            rate_bps=rate,
            basis=template["basis"],
            strict_eligible=template["strict_eligible"],
            embedded_in_leg_quote=template["embedded_in_leg_quote"],
            observed_at=template["observed_at"],
            valid_until=template["valid_until"],
            source=template["source"],
            source_record_sha256=template["source_record_sha256"],
            reason_code=template["reason_code"],
        ))
    return sorted(rows, key=lambda row: (
        row["opportunity_id"], row["leg"], row["component_type"]
    ))


def _atomic_complete_bundle(complete):
    bundle = copy.deepcopy(complete)
    route = dict(bundle["routes"][0])
    route.update({
        "token_symbol": "AAVE",
        "buy_market_id": (
            "dex:eth:uniswap_v2:"
            "0x3333333333333333333333333333333333333333:AAVE"
        ),
        "sell_market_id": (
            "dex:eth:sushiswap_v2:"
            "0x4444444444444444444444444444444444444444:AAVE"
        ),
        "route_mode": "atomic_onchain",
        "route_class": "candidate",
        "settlement_reason": None,
    })
    route["route_id"] = route_publication.canonical_route_id(route)
    bundle["routes"] = [route]
    bundle["legs"] = sorted([
        {
            "available": True,
            "fixed_block_number": "123",
            "fixed_block_timestamp": "2026-08-01T11:59:59Z",
            "leg_id": market_id,
            "market_id": market_id,
            "market_type": "dex",
            "raw_response_sha256": raw_hash,
            "reason_code": "observed",
            "snapshot_id": bundle["core_context"]["raw_evidence_run_id"],
            "source_endpoint": "https://rpc.example/eth",
            "state_observed_at": observed_at,
            "status": "observed",
            "token_symbol": "AAVE",
        }
        for market_id, raw_hash, observed_at in (
            (route["buy_market_id"], "1" * 64, "2026-08-01T12:00:00Z"),
            (route["sell_market_id"], "2" * 64, "2026-08-01T12:01:00Z"),
        )
    ], key=lambda row: row["market_id"])

    opportunities = []
    costs = []
    for original in bundle["opportunities"]:
        opportunity = dict(original)
        opportunity.update({
            "route_id": route["route_id"],
            "opportunity_id": route_opportunity_id(
                route["route_id"], opportunity["requested_notional_usd"]
            ),
            "token_symbol": route["token_symbol"],
            "buy_market_id": route["buy_market_id"],
            "sell_market_id": route["sell_market_id"],
            "route_mode": route["route_mode"],
            "opportunity_class": "research_estimate",
            "primary_reason": "cost_component_estimated",
            "reason_codes": ["cost_component_estimated"],
            "strict_eligible": False,
            "strict_ready_for_publication": False,
            "publication_attestation_sha256": None,
            "reflected_or_embedded_component_keys": [
                "buy:pool_swap_fee", "sell:pool_swap_fee"
            ],
        })
        scenario_costs = _atomic_cost_rows(bundle, opportunity)
        opportunity["cost_component_set_sha256"] = (
            route_publication._canonical_cost_set_sha256(scenario_costs)
        )
        opportunity = _rehash_opportunity(opportunity)
        opportunities.append(opportunity)
        costs.extend(scenario_costs)
    bundle["opportunities"] = sorted(
        opportunities,
        key=lambda row: (
            row["route_id"], Decimal(row["requested_notional_usd"])
        ),
    )
    bundle["cost_components"] = sorted(costs, key=lambda row: (
        row["opportunity_id"], row["leg"], row["component_type"]
    ))
    bundle["input_generations"]["cost_component_generation"] = (
        route_publication._canonical_input_sha256(bundle["cost_components"])
    )
    bundle["input_generations"]["classified_opportunity_generation"] = (
        route_publication._canonical_input_sha256(bundle["opportunities"])
    )
    return bundle


def _route(token_symbol, buy_market_id, sell_market_id):
    route_mode = "prepositioned_inventory"
    route_id = "route:{}:{}->{}:{}".format(
        token_symbol, buy_market_id, sell_market_id, route_mode
    )
    return {
        "token_symbol": token_symbol,
        "buy_market_id": buy_market_id,
        "sell_market_id": sell_market_id,
        "route_mode": route_mode,
        "route_id": route_id,
        "route_class": "candidate",
        "settlement_reason": None,
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "candidate_source_generation": "candidate-generation-a",
        "buy_reference_volume_usd": "9000" if ":alpha:" in buy_market_id else "7000",
        "sell_reference_volume_usd": "9000" if ":alpha:" in sell_market_id else "7000",
        "route_volume_usd": "7000",
        "route_volume_basis": "minimum_leg_source_horizon_usd",
    }


def _cohort():
    alpha = "cex:alpha:UNI/USDT"
    beta = "cex:beta:UNI/USDT"
    routes = [
        _route("UNI", alpha, beta),
        _route("UNI", beta, alpha),
    ]
    legs = [
        {
            "leg_id": alpha,
            "market_id": alpha,
            "market_type": "cex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:01.000000000Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://api.alpha.example/orderbook",
            "raw_response_sha256": "a" * 64,
        },
        {
            "leg_id": beta,
            "market_id": beta,
            "market_type": "cex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:02.000000000Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://api.beta.example/orderbook",
            "raw_response_sha256": "b" * 64,
        },
    ]
    route_rows = [
        {
            **route,
            "validated_at": "2026-08-01T12:00:03Z",
            "skew_seconds": "1.000000000",
            "timing_status": "within_sla",
            "reason_code": None,
        }
        for route in routes
    ]
    cohort = {
        "schema": "route_cohort_collection/v1",
        "candidate_source_generation": "candidate-generation-a",
        "collection_input_generation": "collection-generation-a",
        "source_state": {
            "candidate_source_generation": "candidate-generation-a",
            "collection_input_generation": "collection-generation-a",
        },
        "raw_evidence_run_id": "snapshot-a",
        "target_observed_at": "2026-08-01T12:00:00Z",
        "collection_started_at": "2026-08-01T12:00:00Z",
        "collection_completed_at": "2026-08-01T12:00:03Z",
        "collection_deadline_at": "2026-08-01T12:01:00Z",
        "skew_sla_seconds": "60",
        "route_age_sla_seconds": "120",
        "selection_window": {
            "start": "2026-07-25",
            "end": "2026-08-01",
        },
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "legs": sorted(legs, key=lambda row: row["market_id"]),
        "routes": sorted(routes, key=lambda row: row["route_id"]),
        "route_rows": sorted(route_rows, key=lambda row: row["route_id"]),
    }
    cohort["route_cohort_id"] = "cohort:" + _canonical_sha256(cohort)
    cohort["fingerprint"] = _canonical_sha256(cohort)
    return cohort


def _cex_typed_source_lineage(prefix):
    return {
        "schema": "route_leg_typed_source_lineage/v1",
        "members": [
            {
                "role": "cex_market_rules",
                "status": "observed",
                "reason_code": None,
                "filename": prefix + "-market-rules.json",
                "sha256": "c" * 64,
                "size": 1024,
                "logical_generation": "d" * 64,
                "adapter_id": "route_quantity_quote_for_book/v1",
                "content_schema": "route_market_rules_source/v1",
            },
            {
                "role": "cex_raw_book_response",
                "status": "observed",
                "reason_code": None,
                "filename": prefix + "-raw-book.json",
                "sha256": "a" * 64,
                "size": 2048,
                "logical_generation": "b" * 64,
                "adapter_id": "fetch_cex_depth/parse_book/v1",
                "content_schema": "route_bytes/v1",
            },
            {
                "role": "quote_usd_conversion",
                "status": "observed",
                "reason_code": None,
                "filename": prefix + "-usd-conversion.json",
                "sha256": "e" * 64,
                "size": 512,
                "logical_generation": "f" * 64,
                "adapter_id": "route_usd_conversion_source/v1",
                "content_schema": "route_usd_conversion_source/v1",
            },
        ],
    }


def _dex_typed_source_lineage(prefix):
    return {
        "schema": "route_leg_typed_source_lineage/v1",
        "members": [
            {
                "role": "dex_pool_state",
                "status": "unavailable",
                "reason_code": "typed_source_adapter_unsupported",
                "filename": None,
                "sha256": None,
                "size": None,
                "logical_generation": None,
                "adapter_id": "route_quantity_quote_for_v2_pool/v1",
                "content_schema": "route_v2_pool_state/v1",
            },
            {
                "role": "dex_usd_price_context",
                "status": "unavailable",
                "reason_code": "typed_source_adapter_unsupported",
                "filename": None,
                "sha256": None,
                "size": None,
                "logical_generation": None,
                "adapter_id": "route_dex_usd_price_context/v1",
                "content_schema": "route_dex_usd_price_context/v1",
            },
        ],
    }


def _rehash(cohort):
    value = copy.deepcopy(cohort)
    for field, key in (
        ("routes", "route_id"),
        ("legs", "market_id"),
        ("route_rows", "route_id"),
    ):
        value[field] = sorted(value[field], key=lambda row: row[key])
    value.pop("route_cohort_id", None)
    value.pop("fingerprint", None)
    value["route_cohort_id"] = "cohort:" + _canonical_sha256(value)
    value["fingerprint"] = _canonical_sha256(value)
    return value


def _second_cohort():
    cohort = _cohort()
    cohort["raw_evidence_run_id"] = "snapshot-b"
    for leg in cohort["legs"]:
        leg["snapshot_id"] = "snapshot-b"
    return _rehash(cohort)


def _third_cohort():
    cohort = _cohort()
    cohort["raw_evidence_run_id"] = "snapshot-c"
    for leg in cohort["legs"]:
        leg["snapshot_id"] = "snapshot-c"
    return _rehash(cohort)


def _publish_core_with_raw_members(core_root, raw_root):
    cohort = _cohort()
    raw_by_market = {
        cohort["legs"][0]["market_id"]: b"alpha typed raw member",
        cohort["legs"][1]["market_id"]: b"beta typed raw member",
    }
    for leg in cohort["legs"]:
        leg["raw_response_sha256"] = hashlib.sha256(
            raw_by_market[leg["market_id"]]
        ).hexdigest()
    cohort = _rehash(cohort)
    pointer = publish_route_cohort_bundle(cohort, core_root=core_root)
    accepted = raw_root / cohort["raw_evidence_run_id"] / "accepted"
    for market_id, payload in raw_by_market.items():
        member = accepted / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
        member.mkdir(parents=True)
        (member / "response.json").write_bytes(payload)
    return cohort, pointer


def _json_member(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _task7_cex_inputs(core_root, raw_root, source_root, private_root):
    source_root.mkdir(parents=True)
    private_root.mkdir(parents=True)
    private_root = private_root.resolve()
    run_id = "task7-source-run"
    now = "2026-08-01T12:02:00Z"
    valid_until = "2026-08-01T13:00:00Z"
    profile_id = "9" * 64

    base = {}
    raw_members = {}
    for venue, direction, price, observed_at in (
        ("binance", "buy", "100", "2026-08-01T12:00:00Z"),
        ("bybit", "sell", "102", "2026-08-01T12:01:00Z"),
    ):
        _quote, evidence, _leg, _projection, fee = cex_leg(
            venue=venue,
            direction=direction,
            price=price,
            state_observed_at=observed_at,
            target=CommonTarget(
                asset="AAVE",
                unit_decimals=2,
                raw_quantity=1000,
                lattice_raw=1,
            ),
            cohort_now="2026-08-01T12:01:00Z",
            fee_asset="AAVE" if direction == "buy" else "USDT",
            charge_basis="received_base" if direction == "buy" else "received_quote",
        )
        market = evidence["market"]
        ask_price = str(Decimal(price) + Decimal("1"))
        member_value = (
            {
                "bids": [[price, "10000"]],
                "asks": [[ask_price, "10000"]],
                "lastUpdateId": 1,
            }
            if venue == "binance"
            else {
                "retCode": 0,
                "result": {
                    "s": "AAVEUSDT",
                    "b": [[price, "10000"]],
                    "a": [[ask_price, "10000"]],
                },
            }
        )
        raw_payload = _json_member(member_value)
        raw_members[evidence["market_rules"].market_id] = raw_payload
        endpoint, source_instrument, quote_asset, full_book = source_request(
            venue,
            "AAVE/USDT",
        )
        parsed = parse_book(
            venue,
            member_value,
            requested_instrument=source_instrument,
        )
        book = {
            **parsed,
            "source_observed_at": parsed["source_observed_at"] or observed_at,
            "source_endpoint": endpoint,
            "source_quote_asset": quote_asset,
            "full_book_reported": full_book,
            "raw": raw_payload,
        }

        rules_value = {
            "schema": "route_market_rules_source/v1",
            "market_id": evidence["market_rules"].market_id,
            "base_asset": "AAVE",
            "quote_asset": "USDT",
            "base_unit_decimals": 2,
            "quote_unit_decimals": 2,
            "base_increment": "0.01",
            "quote_increment": "0.01",
            "min_base_quantity": "0.01",
            "min_quote_notional": "1",
            "observed_at": "2026-08-01T11:55:00Z",
            "valid_until": valid_until,
        }
        rules_name = venue + "-rules.json"
        rules_payload = _json_member(rules_value)
        (source_root / rules_name).write_bytes(rules_payload)
        rules = replace(
            evidence["market_rules"],
            source_record_sha256=hashlib.sha256(rules_payload).hexdigest(),
        )
        fee = replace(
            fee,
            source_record_sha256=("3" if venue == "binance" else "4") * 64,
        )
        usd_value = {
            "schema": "route_usd_conversion_source/v1",
            "quote_asset": "USDT",
            "usd_per_quote": "1",
            "observed_at": observed_at,
            "valid_until": valid_until,
            "source": "synchronized USDT/USD conversion",
        }
        usd_name = venue + "-usd.json"
        usd_payload = _json_member(usd_value)
        (source_root / usd_name).write_bytes(usd_payload)
        base[direction] = {
            "market": market,
            "book": book,
            "rules": rules,
            "fee": fee,
            "observed_at": observed_at,
            "rules_member": rules_name,
            "usd_member": usd_name,
            "usd_source_hash": hashlib.sha256(usd_payload).hexdigest(),
        }

    route, _unused_mode = route_and_mode(
        base["buy"]["rules"].market_id,
        base["sell"]["rules"].market_id,
    )
    candidate = {
        **route,
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "candidate_source_generation": "candidate-generation-a",
        "buy_reference_volume_usd": "9000",
        "sell_reference_volume_usd": "7000",
        "route_volume_usd": "7000",
        "route_volume_basis": "minimum_leg_source_horizon_usd",
    }
    cohort = _cohort()
    cohort.update({
        "raw_evidence_run_id": run_id,
        "target_observed_at": "2026-08-01T12:00:00Z",
        "collection_started_at": "2026-08-01T12:00:00Z",
        "collection_completed_at": "2026-08-01T12:01:00Z",
        "collection_deadline_at": "2026-08-01T12:01:00Z",
        "routes": [candidate],
        "legs": [
            {
                "leg_id": item["rules"].market_id,
                "market_id": item["rules"].market_id,
                "market_type": "cex",
                "token_symbol": "AAVE",
                "status": "observed",
                "available": True,
                "reason_code": "observed",
                "state_observed_at": item["observed_at"],
                "snapshot_id": run_id,
                "source_endpoint": item["book"]["source_endpoint"].split("?", 1)[0],
                "raw_response_sha256": hashlib.sha256(
                    raw_members[item["rules"].market_id]
                ).hexdigest(),
            }
            for item in (base["buy"], base["sell"])
        ],
        "route_rows": [{
            **candidate,
            "validated_at": "2026-08-01T12:01:00Z",
            "skew_seconds": "60",
            "timing_status": "within_sla",
            "reason_code": None,
        }],
    })
    cohort["source_state"] = {
        "candidate_source_generation": cohort["candidate_source_generation"],
        "collection_input_generation": cohort["collection_input_generation"],
    }
    cohort = _rehash(cohort)
    pointer = publish_route_cohort_bundle(cohort, core_root=core_root)
    for market_id, payload in raw_members.items():
        member = (
            raw_root / run_id / "accepted"
            / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
        )
        member.mkdir(parents=True)
        (member / "response.json").write_bytes(payload)

    fee_path = private_root / "fees.csv"
    with fee_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PRIVATE_FEE_PROFILE_COLUMNS)
        writer.writeheader()
        for venue, direction, fee_asset, source_hash in (
            ("binance", "buy", "AAVE", "3" * 64),
            ("bybit", "sell", "USDT", "4" * 64),
        ):
            writer.writerow({
                "profile_id": profile_id,
                "venue": venue,
                "instrument": "AAVE/USDT",
                "side": direction,
                "taker_fee_bps": "10",
                "fee_asset": fee_asset,
                "basis": "authenticated_taker_fee",
                "observed_at": "2026-08-01T11:55:00Z",
                "valid_until": valid_until,
                "source_record_sha256": source_hash,
            })
    fee_path.chmod(0o600)

    inventory_path = private_root / "inventory.csv"
    with inventory_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=INVENTORY_PROFILE_COLUMNS)
        writer.writeheader()
        writer.writerows([
            {
                "profile_id": profile_id,
                "market_id": route["buy_market_id"],
                "asset": "USDT",
                "available_quantity": "1000000",
                "observed_at": "2026-08-01T11:55:00Z",
                "valid_until": valid_until,
                "source_record_sha256": "7" * 64,
            },
            {
                "profile_id": profile_id,
                "market_id": route["sell_market_id"],
                "asset": "AAVE",
                "available_quantity": "10000",
                "observed_at": "2026-08-01T11:55:00Z",
                "valid_until": valid_until,
                "source_record_sha256": "8" * 64,
            },
        ])
    inventory_path.chmod(0o600)
    inventory_rows = load_validated_inventory_profile(inventory_path, now=now)

    opportunities = []
    for notional in cohort["requested_notionals_usd"]:
        target = CommonTarget(
            asset="AAVE",
            unit_decimals=2,
            raw_quantity=notional,
            lattice_raw=1,
        )
        quotes = {}
        evidences = {}
        legs = {}
        projections = {}
        for direction in ("buy", "sell"):
            item = base[direction]
            state_id = cex_quantity_state_id(
                item["market"],
                item["book"],
                snapshot_id=run_id,
                observed_at=item["observed_at"],
                cohort_now="2026-08-01T12:01:00Z",
                market_rules=item["rules"],
                fee_semantics=item["fee"],
            )
            quote = route_quantity_quote_for_book(
                item["market"],
                item["book"],
                direction=direction,
                target_token_quantity=target,
                market_rules=item["rules"],
                fee_semantics=item["fee"],
                snapshot_id=run_id,
                observed_at=item["observed_at"],
                cohort_now="2026-08-01T12:01:00Z",
                expected_state_id=state_id,
            )
            quotes[direction] = quote
            evidences[direction] = {
                "kind": "cex_book",
                "market": item["market"],
                "book": item["book"],
                "market_rules": item["rules"],
                "fee_semantics": item["fee"],
                "snapshot_id": run_id,
                "observed_at": item["observed_at"],
                "cohort_now": "2026-08-01T12:01:00Z",
                "expected_state_id": state_id,
                "assurance_status": "route_bundle_validated",
                "core_manifest_sha256": pointer["manifest_sha256"],
            }
            legs[direction] = {
                **next(
                    row for row in cohort["legs"]
                    if row["market_id"] == quote.market_id
                ),
                "state_id": state_id,
            }
            cash = (
                quote.quote_debit_quantity
                if direction == "buy"
                else quote.quote_received_quantity
            )
            projections[direction] = usd_projection_evidence(
                market_id=quote.market_id,
                state_id=state_id,
                direction=direction,
                quote_asset="USDT",
                quote_cash_quantity=cash,
                usd_per_quote=Decimal("1"),
                value_status="authenticated",
                observed_at=item["observed_at"],
                valid_until=valid_until,
                source="synchronized USDT/USD conversion",
                source_record_sha256=item["usd_source_hash"],
                core_manifest_sha256=pointer["manifest_sha256"],
            )
        opportunity_id = route_opportunity_id(route["route_id"], notional)
        costs = [
            collect_cex_fee_snapshot(
                cohort_id=cohort["route_cohort_id"],
                opportunity_id=opportunity_id,
                leg=direction,
                market_id=quotes[direction].market_id,
                venue=quotes[direction].market_id.split(":", 2)[1],
                instrument="AAVE/USDT",
                side=direction,
                requested_notional_usd=notional,
                target_token_quantity=target.quantity,
                now=now,
                private_profile_path=fee_path,
                profile_id=profile_id,
            )
            for direction in ("buy", "sell")
        ]
        costs.append(cost_component_row(
            cohort_id=cohort["route_cohort_id"],
            opportunity_id=opportunity_id,
            leg="route",
            market_id="",
            direction="route",
            requested_notional_usd=notional,
            target_token_quantity=target.quantity,
            component_type="rebalancing_or_transfer",
            value_status="not_applicable",
            amount_usd=None,
            rate_bps=None,
            basis="prepositioned inventory proves no immediate transfer",
            strict_eligible=True,
            observed_at=None,
            valid_until=None,
            source="validated route topology",
            source_record_sha256=None,
        ))
        inventory = inventory_capacity_for_route(
            route,
            inventory_rows,
            buy_quote_asset=quotes["buy"].quote_debit_asset,
            buy_quote_quantity=quotes["buy"].quote_debit_quantity,
            sell_token_asset="AAVE",
            sell_net_token_quantity=quotes["sell"].base_debit_quantity,
            now=now,
        )
        expected_request = {
            key: inventory[key]
            for key in (
                "route_id", "buy_market_id", "sell_market_id",
                "buy_quote_asset", "buy_quote_quantity", "sell_token_asset",
                "sell_net_token_quantity", "target_asset", "target_quantity",
            )
        }
        mode = classify_route_mode_evidence(
            route,
            expected_request=expected_request,
            inventory_evidence=inventory,
            now=now,
        )
        build_inputs = {
            "cohort_id": cohort["route_cohort_id"],
            "route": route,
            "requested_notional_usd": Decimal(notional),
            "common_target": target,
            "buy_leg": legs["buy"],
            "sell_leg": legs["sell"],
            "buy_quote": quotes["buy"],
            "sell_quote": quotes["sell"],
            "buy_quote_evidence": evidences["buy"],
            "sell_quote_evidence": evidences["sell"],
            "buy_usd_projection": projections["buy"],
            "sell_usd_projection": projections["sell"],
            "cost_components": costs,
            "mode_evidence": mode,
            "now": now,
        }
        classified = build_route_opportunity(**build_inputs)
        opportunities.append({
            "classified_opportunity": classified,
            "build_inputs": build_inputs,
            "source_members": {
                "buy_market_rules": base["buy"]["rules_member"],
                "sell_market_rules": base["sell"]["rules_member"],
                "buy_usd_conversion": base["buy"]["usd_member"],
                "sell_usd_conversion": base["sell"]["usd_member"],
            },
        })
    return {
        "cohort": cohort,
        "pointer": pointer,
        "opportunity_inputs": opportunities,
        "fee_profile_path": fee_path,
        "fee_profile_id": profile_id,
        "inventory_profile_path": inventory_path,
        "source_root": source_root,
    }


_LIVE_COMPLETE_BUNDLE_GOLDEN_BY_RUNTIME = {
    (3, 8, 10): {
        "core_manifest_sha256": (
            "ae09cedcea54e8509e71abeb561d51ca707155d6dc70b3758094190ada3222cc"
        ),
        "artifacts": {
            "cost_components.csv": (
                19269,
                "af3c06ccfc6f614ae5945a68a7f599f97bafc6b97925ab5449feead0783146fd",
            ),
            "manifest.json": (
                3239,
                "482f7050c7f965750fffc07b23f9df75231ee75bb9467b3cdf520e2e1513617d",
            ),
            "route_cohort.sqlite3": (
                135168,
                "b94743b68f6d87dcf4b231257ab08cf48cb5ffebe257e3ab3be451f613fa9537",
            ),
            "route_legs.csv": (
                1683,
                "477324f07f1106dc0932ab35d5fe1f456a0e741b49c54d86d262d4182b8995c8",
            ),
            "route_opportunities.csv": (
                23563,
                "1c8f97875ee1215f2d1a517ec3b07372ec1ea536a6919a6bc5d18e458c8ace6d",
            ),
        },
    },
    (3, 13, 5): {
        "core_manifest_sha256": (
            "5a3344f9f68bfded5cc12b178970ac78d190be99787f5a55f15df64ad5ff2f93"
        ),
        "artifacts": {
            "cost_components.csv": (
                19269,
                "af3c06ccfc6f614ae5945a68a7f599f97bafc6b97925ab5449feead0783146fd",
            ),
            "manifest.json": (
                3239,
                "a5bec61af07c36e4ccb0c40642a958e8fb31b046f8e223347b7fe12863957334",
            ),
            "route_cohort.sqlite3": (
                135168,
                "f3ff7e6914c2159277ccb9aa1aef5e8d1302fb3e125c3f74ecf04bcd464efc8a",
            ),
            "route_legs.csv": (
                1683,
                "477324f07f1106dc0932ab35d5fe1f456a0e741b49c54d86d262d4182b8995c8",
            ),
            "route_opportunities.csv": (
                23563,
                "1f2cd8c28e28781ed0eb223ad0d4b1cac91e01149da68fda251f00af6f714702",
            ),
        },
    },
}


def _refresh_database_hash_in_manifest(bundle):
    database = bundle / "route_cohort.sqlite3"
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["route_cohort.sqlite3"]["sha256"] = hashlib.sha256(
        database.read_bytes()
    ).hexdigest()
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _rewrite_route_legs_schema(
    bundle,
    *,
    market_definition="market_id TEXT PRIMARY KEY NOT NULL",
    status_definition="status TEXT NOT NULL",
    table_primary_key=None,
    without_rowid=True,
):
    database = bundle / "route_cohort.sqlite3"
    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DROP INDEX route_legs_token_idx")
        connection.execute("ALTER TABLE route_legs RENAME TO route_legs_old")
        definitions = [
            "route_cohort_id TEXT NOT NULL",
            "leg_id TEXT NOT NULL",
            market_definition,
            "market_type TEXT NOT NULL",
            "token_symbol TEXT NOT NULL",
            status_definition,
            "available INTEGER",
            "reason_code TEXT NOT NULL",
            "state_observed_at TEXT NOT NULL",
            "snapshot_id TEXT NOT NULL",
            "source_endpoint TEXT NOT NULL",
            "raw_response_sha256 TEXT NOT NULL",
            "fixed_block_number TEXT NOT NULL",
            "fixed_block_timestamp TEXT NOT NULL",
            "row_json TEXT NOT NULL",
        ]
        if table_primary_key is not None:
            definitions.append(table_primary_key)
        connection.execute(
            "CREATE TABLE route_legs ({}){}".format(
                ",".join(definitions),
                " WITHOUT ROWID" if without_rowid else "",
            )
        )
        columns = ",".join(route_publication.LEG_COLUMNS)
        connection.execute(
            "INSERT INTO route_legs ({0}) SELECT {0} FROM route_legs_old".format(
                columns
            )
        )
        connection.execute("DROP TABLE route_legs_old")
        connection.execute(
            "CREATE INDEX route_legs_token_idx "
            "ON route_legs(token_symbol, market_id)"
        )
        connection.commit()
    finally:
        connection.close()


def _rewrite_route_timing_without_foreign_key(bundle):
    database = bundle / "route_cohort.sqlite3"
    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DROP INDEX route_timing_status_idx")
        connection.execute("ALTER TABLE route_timing RENAME TO route_timing_old")
        connection.execute(
            """
            CREATE TABLE route_timing (
                route_cohort_id TEXT NOT NULL,
                route_id TEXT PRIMARY KEY NOT NULL,
                skew_seconds TEXT NOT NULL,
                timing_status TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                row_json TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )
        columns = ",".join(route_publication.TIMING_COLUMNS)
        connection.execute(
            "INSERT INTO route_timing ({0}) "
            "SELECT {0} FROM route_timing_old".format(columns)
        )
        connection.execute("DROP TABLE route_timing_old")
        connection.execute(
            "CREATE INDEX route_timing_status_idx "
            "ON route_timing(timing_status, route_id)"
        )
        connection.commit()
    finally:
        connection.close()


def _dex_cohort(block_numbers=("100", "100")):
    cohort = _cohort()
    first = "dex:eth:uniswap_v2:0x{}:UNI".format("a" * 40)
    second = "dex:eth:uniswap_v2:0x{}:UNI".format("b" * 40)

    def route(buy, sell):
        identity = {
            "token_symbol": "UNI",
            "buy_market_id": buy,
            "sell_market_id": sell,
            "route_mode": "atomic_onchain",
        }
        return {
            **identity,
            "route_id": "route:UNI:{}->{}:atomic_onchain".format(buy, sell),
            "route_class": "candidate",
            "settlement_reason": None,
            "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
            "candidate_source_generation": "candidate-generation-a",
            "buy_reference_volume_usd": "9000" if buy == first else "7000",
            "sell_reference_volume_usd": "9000" if sell == first else "7000",
            "route_volume_usd": "7000",
            "route_volume_basis": "minimum_leg_source_horizon_usd",
        }

    routes = [route(first, second), route(second, first)]
    cohort["routes"] = routes
    cohort["legs"] = [
        {
            "leg_id": market_id,
            "market_id": market_id,
            "market_type": "dex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": observed_at,
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://rpc.example/eth",
            "raw_response_sha256": raw_hash * 64,
            "fixed_block_number": block_number,
            "fixed_block_timestamp": "2026-08-01T11:59:59Z",
        }
        for market_id, observed_at, raw_hash, block_number in (
            (first, "2026-08-01T12:00:01Z", "a", block_numbers[0]),
            (second, "2026-08-01T12:00:02Z", "b", block_numbers[1]),
        )
    ]
    cohort["route_rows"] = [
        {
            **candidate,
            "validated_at": "2026-08-01T12:00:03Z",
            "skew_seconds": "1",
            "timing_status": "within_sla",
            "reason_code": None,
        }
        for candidate in routes
    ]
    return _rehash(cohort)


def _dex_cohort_with_lineage_conflict():
    return _dex_cohort(("100", "101"))


class TemporaryRouteRootTestCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "data/local/routes/core"


class RoutePublicationInterfaceTests(unittest.TestCase):
    def test_task_five_publication_interfaces_exist(self):
        from scripts.route_publication import (
            build_route_cohort_sqlite,
            load_latest_route_cohort,
            publish_route_cohort_bundle,
            validate_route_cohort_bundle,
        )

        self.assertTrue(callable(build_route_cohort_sqlite))
        self.assertTrue(callable(validate_route_cohort_bundle))
        self.assertTrue(callable(publish_route_cohort_bundle))
        self.assertTrue(callable(load_latest_route_cohort))

    def test_task_two_joint_shadow_interfaces_exist(self):
        self.assertTrue(callable(publish_shadow_result))
        self.assertTrue(callable(load_shadow_result))
        self.assertTrue(callable(load_latest_shadow_result))
        self.assertTrue(callable(load_active_phase_state))
        self.assertTrue(callable(load_historical_phase_state))

    def test_private_tmp_alias_normalization_is_darwin_only(self):
        with patch("scripts.route_publication.sys.platform", "linux"):
            self.assertEqual(
                route_publication._absolute_without_symlink_resolution(
                    Path("/tmp/route-core")
                ),
                Path("/tmp/route-core"),
            )


class CompleteRouteBundleTests(TemporaryRouteRootTestCase):
    def test_live_complete_bundle_bytes_match_prefactor_golden(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        bundle = route_publication.build_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        artifacts, _manifest = route_publication._complete_artifact_bytes(bundle)
        golden = _LIVE_COMPLETE_BUNDLE_GOLDEN_BY_RUNTIME[sys.version_info[:3]]

        self.assertEqual(
            bundle["core_manifest_sha256"], golden["core_manifest_sha256"]
        )
        self.assertEqual(
            {
                name: (len(payload), hashlib.sha256(payload).hexdigest())
                for name, payload in sorted(artifacts.items())
            },
            golden["artifacts"],
        )

    def test_complete_sqlite_reader_supports_legacy_sqlite_catalog_name(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        bundle = route_publication.build_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        artifacts, _manifest = route_publication._complete_artifact_bytes(bundle)
        database_bytes = artifacts[
            route_publication.ROUTE_OPPORTUNITY_SQLITE_FILENAME
        ]
        real_connect = sqlite3.connect

        class LegacyCatalogConnection:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, statement, *args, **kwargs):
                if "sqlite_schema" in statement:
                    raise sqlite3.OperationalError(
                        "no such table: sqlite_schema"
                    )
                return self.connection.execute(statement, *args, **kwargs)

            def close(self):
                return self.connection.close()

        def legacy_connect(*args, **kwargs):
            return LegacyCatalogConnection(real_connect(*args, **kwargs))

        with patch.object(
            route_publication.sqlite3,
            "connect",
            side_effect=legacy_connect,
        ):
            loaded, legs, costs, opportunities = (
                route_publication._read_complete_sqlite(database_bytes)
            )

        self.assertEqual(loaded, bundle)
        self.assertEqual(legs, bundle["legs"])
        self.assertEqual(costs, bundle["cost_components"])
        self.assertEqual(opportunities, bundle["opportunities"])

    def test_complete_sqlite_reader_rejects_semantically_forged_ddl(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        bundle = route_publication.build_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        artifacts, _manifest = route_publication._complete_artifact_bytes(bundle)
        database_bytes = artifacts[
            route_publication.ROUTE_OPPORTUNITY_SQLITE_FILENAME
        ]
        mutations = {
            "rowid table disguised by CHECK text": """
                ALTER TABLE bundle_metadata RENAME TO old_bundle_metadata;
                CREATE TABLE bundle_metadata (
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    PRIMARY KEY (key),
                    CHECK ('WITHOUT ROWID' IS NOT NULL)
                );
                INSERT INTO bundle_metadata SELECT * FROM old_bundle_metadata;
                DROP TABLE old_bundle_metadata;
            """,
            "partial descending unique index": """
                DROP INDEX route_opportunities_route_idx;
                CREATE UNIQUE INDEX route_opportunities_route_idx
                    ON route_opportunities(
                        route_id COLLATE NOCASE DESC,
                        requested_notional_usd
                    )
                    WHERE strict_eligible = 'true';
            """,
            "unexpected internal statistics table": """
                ANALYZE;
            """,
        }
        for label, mutation in mutations.items():
            with self.subTest(label=label):
                database = Path(self.temporary.name) / (
                    "tampered-{}.sqlite3".format(len(label))
                )
                database.write_bytes(database_bytes)
                connection = sqlite3.connect(str(database))
                try:
                    connection.executescript(mutation)
                    connection.commit()
                    connection.execute("VACUUM")
                finally:
                    connection.close()
                with self.assertRaisesRegex(
                    route_publication.RoutePublicationError,
                    "schema",
                ):
                    route_publication._read_complete_sqlite(
                        database.read_bytes()
                    )

    def test_builder_preserves_route_volume_lineage_from_pinned_core(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )

        routes_root = Path(self.temporary.name) / "data/local/routes"
        route_publication.publish_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            routes_root=routes_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        complete = route_publication.load_latest_complete_route_bundle(
            routes_root
        )["bundle"]

        self.assertEqual(len(complete["routes"]), 1)
        self.assertEqual(complete["routes"][0]["buy_reference_volume_usd"], "9000")
        self.assertEqual(complete["routes"][0]["sell_reference_volume_usd"], "7000")
        self.assertEqual(complete["routes"][0]["route_volume_usd"], "7000")
        self.assertEqual(
            complete["routes"][0]["route_volume_basis"],
            "minimum_leg_source_horizon_usd",
        )

    def test_builder_rejects_a_core_without_exactly_five_scenarios_per_route(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        _cohort_value, pointer = _publish_core_with_raw_members(
            self.root,
            raw_root,
        )

        with self.assertRaisesRegex(ValueError, "five notional scenarios"):
            route_publication.build_complete_route_bundle(
                core_root=self.root,
                raw_root=raw_root,
                opportunity_inputs=[],
            )

        self.assertEqual(
            load_latest_route_cohort(self.root)["manifest_sha256"],
            pointer["manifest_sha256"],
        )

    def test_builder_replays_actual_cex_sources_before_issuing_strict_rows(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        self.assertTrue(all(
            item["classified_opportunity"]["strict_ready_for_publication"]
            and not item["classified_opportunity"]["strict_eligible"]
            for item in fixture["opportunity_inputs"]
        ))

        complete = route_publication.build_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )

        self.assertEqual(complete["schema"], "route_opportunity/v1")
        self.assertEqual(
            complete["core_manifest_sha256"],
            fixture["pointer"]["manifest_sha256"],
        )
        self.assertEqual(len(complete["opportunities"]), 5)
        self.assertTrue(all(
            row["strict_eligible"]
            and row["opportunity_class"] == "executable_candidate"
            for row in complete["opportunities"]
        ))

    def test_publisher_writes_exact_five_file_bundle_with_full_reread_parity(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        routes_root = Path(self.temporary.name) / "data/local/routes"
        core_bundle = (
            self.root / "bundles" / fixture["cohort"]["route_cohort_id"]
        )
        core_before = {
            path.name: path.read_bytes() for path in core_bundle.iterdir()
        }

        pointer = route_publication.publish_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            routes_root=routes_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        loaded = route_publication.load_latest_complete_route_bundle(routes_root)
        final = routes_root / "bundles" / fixture["cohort"]["route_cohort_id"]

        self.assertEqual(set(path.name for path in final.iterdir()), {
            "route_legs.csv",
            "cost_components.csv",
            "route_opportunities.csv",
            "route_cohort.sqlite3",
            "manifest.json",
        })
        self.assertEqual(pointer, loaded["pointer"])
        self.assertEqual(len(loaded["legs"]), 2)
        self.assertEqual(len(loaded["cost_components"]), 15)
        self.assertEqual(len(loaded["opportunities"]), 5)
        self.assertEqual(
            loaded["manifest"]["input_generations"],
            loaded["bundle"]["input_generations"],
        )
        self.assertEqual(
            core_before,
            {path.name: path.read_bytes() for path in core_bundle.iterdir()},
        )

    def test_shuffled_complete_inputs_publish_byte_identical_bundles(self):
        first = Path(self.temporary.name) / "first"
        second = Path(self.temporary.name) / "second"
        fixture_one = _task7_cex_inputs(
            first / "routes/core",
            first / "raw/route-cohort",
            first / "sources",
            first / "private",
        )
        fixture_two = _task7_cex_inputs(
            second / "routes/core",
            second / "raw/route-cohort",
            second / "sources",
            second / "private",
        )

        pointer_one = route_publication.publish_complete_route_bundle(
            core_root=first / "routes/core",
            routes_root=first / "routes",
            raw_root=first / "raw/route-cohort",
            source_root=fixture_one["source_root"],
            fee_profile_path=fixture_one["fee_profile_path"],
            fee_profile_id=fixture_one["fee_profile_id"],
            inventory_profile_path=fixture_one["inventory_profile_path"],
            opportunity_inputs=fixture_one["opportunity_inputs"],
        )
        pointer_two = route_publication.publish_complete_route_bundle(
            core_root=second / "routes/core",
            routes_root=second / "routes",
            raw_root=second / "raw/route-cohort",
            source_root=fixture_two["source_root"],
            fee_profile_path=fixture_two["fee_profile_path"],
            fee_profile_id=fixture_two["fee_profile_id"],
            inventory_profile_path=fixture_two["inventory_profile_path"],
            opportunity_inputs=reversed(fixture_two["opportunity_inputs"]),
        )

        self.assertEqual(pointer_one, pointer_two)
        cohort_id = fixture_one["cohort"]["route_cohort_id"]
        first_bundle = first / "routes/bundles" / cohort_id
        second_bundle = second / "routes/bundles" / cohort_id
        self.assertEqual(
            {path.name: path.read_bytes() for path in first_bundle.iterdir()},
            {path.name: path.read_bytes() for path in second_bundle.iterdir()},
        )

    def test_logical_validator_recomputes_attestation_and_strict_readiness(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        complete = route_publication.build_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        cases = []
        transplanted = copy.deepcopy(complete)
        transplanted["opportunities"][0]["publication_attestation_sha256"] = (
            transplanted["opportunities"][1]["publication_attestation_sha256"]
        )
        transplanted["opportunities"][0] = _rehash_opportunity(
            transplanted["opportunities"][0]
        )
        cases.append(("attestation", transplanted))

        wrong_core = copy.deepcopy(complete)
        wrong_core["core_manifest_sha256"] = "f" * 64
        for row in wrong_core["opportunities"]:
            row["buy_core_manifest_sha256"] = "f" * 64
            row["sell_core_manifest_sha256"] = "f" * 64
            replacement = _rehash_opportunity(row)
            row.clear()
            row.update(replacement)
        cases.append(("attestation", wrong_core))

        not_ready = copy.deepcopy(complete)
        not_ready["opportunities"][0]["strict_ready_for_publication"] = False
        not_ready["opportunities"][0] = _rehash_opportunity(
            not_ready["opportunities"][0]
        )
        cases.append(("strict", not_ready))

        for expected, candidate in cases:
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(ValueError, expected):
                    route_publication._validate_complete_logical_bundle(candidate)

    def test_strict_route_identity_rejects_same_venue_and_upbit(self):
        self.assertFalse(route_publication._strict_cex_route_identity({
            "route_mode": "prepositioned_inventory",
            "buy_market_id": "cex:binance:AAVE/USDT",
            "sell_market_id": "cex:binance:AAVE/USDC",
        }))
        self.assertFalse(route_publication._strict_cex_route_identity({
            "route_mode": "prepositioned_inventory",
            "buy_market_id": "cex:upbit:AAVE/KRW",
            "sell_market_id": "cex:bybit:AAVE/USDT",
        }))

    def test_logical_validator_rejects_private_path_or_credential_sentinel(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        complete = route_publication.build_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        complete["input_generations"]["adapter_versions"]["credential_path"] = (
            "/private/owner/fees.csv"
        )
        with self.assertRaisesRegex(ValueError, "unsafe evidence"):
            route_publication._validate_complete_logical_bundle(complete)

    def test_public_pointer_failure_preserves_old_bytes_and_retry_selects_bundle(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        routes_root = Path(self.temporary.name) / "routes"
        routes_root.mkdir()
        old_pointer = b'{"sentinel":"old"}\n'
        (routes_root / "latest.json").write_bytes(old_pointer)
        kwargs = {
            "core_root": self.root,
            "routes_root": routes_root,
            "raw_root": raw_root,
            "source_root": fixture["source_root"],
            "fee_profile_path": fixture["fee_profile_path"],
            "fee_profile_id": fixture["fee_profile_id"],
            "inventory_profile_path": fixture["inventory_profile_path"],
            "opportunity_inputs": fixture["opportunity_inputs"],
        }
        with patch(
            "scripts.route_publication._replace_pointer_bytes_at",
            side_effect=OSError("injected pointer replacement failure"),
        ):
            with self.assertRaisesRegex(OSError, "injected pointer"):
                route_publication.publish_complete_route_bundle(**kwargs)
        self.assertEqual((routes_root / "latest.json").read_bytes(), old_pointer)
        final = routes_root / "bundles" / fixture["cohort"]["route_cohort_id"]
        orphan_identity = os.lstat(final).st_ino

        pointer = route_publication.publish_complete_route_bundle(**kwargs)
        self.assertEqual(os.lstat(final).st_ino, orphan_identity)
        self.assertEqual(
            route_publication.load_latest_complete_route_bundle(routes_root)["pointer"],
            pointer,
        )

    def test_post_replace_pointer_fsync_failure_rolls_back_and_retry_reuses_orphan(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        routes_root = Path(self.temporary.name) / "routes"
        routes_root.mkdir()
        old_pointer = b'{"schema":"old-complete-pointer"}\n'
        (routes_root / "latest.json").write_bytes(old_pointer)
        kwargs = {
            "core_root": self.root,
            "routes_root": routes_root,
            "raw_root": raw_root,
            "source_root": fixture["source_root"],
            "fee_profile_path": fixture["fee_profile_path"],
            "fee_profile_id": fixture["fee_profile_id"],
            "inventory_profile_path": fixture["inventory_profile_path"],
            "opportunity_inputs": fixture["opportunity_inputs"],
        }
        original_replace = route_publication._replace_pointer_bytes_at
        original_fsync = route_publication._fsync_directory
        pointer_replaced = {"value": False}
        injected = {"value": False}

        def track_pointer_replace(directory_fd, value):
            original_replace(directory_fd, value)
            pointer_replaced["value"] = True

        def fail_first_post_replace_fsync(path, *, directory_fd=None):
            if pointer_replaced["value"] and not injected["value"]:
                injected["value"] = True
                raise OSError("injected post-replace pointer fsync failure")
            return original_fsync(path, directory_fd=directory_fd)

        with patch(
            "scripts.route_publication._replace_pointer_bytes_at",
            side_effect=track_pointer_replace,
        ), patch(
            "scripts.route_publication._fsync_directory",
            side_effect=fail_first_post_replace_fsync,
        ):
            with self.assertRaisesRegex(OSError, "post-replace pointer fsync"):
                route_publication.publish_complete_route_bundle(**kwargs)

        self.assertTrue(injected["value"])
        self.assertEqual((routes_root / "latest.json").read_bytes(), old_pointer)
        final = routes_root / "bundles" / fixture["cohort"]["route_cohort_id"]
        orphan_inode = os.lstat(final).st_ino

        pointer = route_publication.publish_complete_route_bundle(**kwargs)

        self.assertEqual(os.lstat(final).st_ino, orphan_inode)
        self.assertEqual(
            route_publication.load_latest_complete_route_bundle(routes_root)["pointer"],
            pointer,
        )

    def test_real_core_pointer_advance_preserves_old_complete_pointer(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        routes_root = Path(self.temporary.name) / "routes"
        kwargs = {
            "core_root": self.root,
            "routes_root": routes_root,
            "raw_root": raw_root,
            "source_root": fixture["source_root"],
            "fee_profile_path": fixture["fee_profile_path"],
            "fee_profile_id": fixture["fee_profile_id"],
            "inventory_profile_path": fixture["inventory_profile_path"],
            "opportunity_inputs": fixture["opportunity_inputs"],
        }
        route_publication.publish_complete_route_bundle(**kwargs)
        old_pointer_bytes = (routes_root / "latest.json").read_bytes()
        old_complete = route_publication.load_latest_complete_route_bundle(
            routes_root
        )
        next_core = _second_cohort()
        original_verify = route_publication._verify_complete_core_lineage
        advanced = {"pointer": None}

        def advance_core_then_verify(*args, **kwargs):
            advanced["pointer"] = publish_route_cohort_bundle(
                next_core,
                core_root=self.root,
            )
            return original_verify(*args, **kwargs)

        with patch(
            "scripts.route_publication._verify_complete_core_lineage",
            side_effect=advance_core_then_verify,
        ):
            with self.assertRaisesRegex(ValueError, "core changed"):
                route_publication.publish_complete_route_bundle(**kwargs)

        self.assertIsNotNone(advanced["pointer"])
        self.assertEqual((routes_root / "latest.json").read_bytes(), old_pointer_bytes)
        self.assertEqual(
            route_publication.load_latest_complete_route_bundle(routes_root)[
                "manifest_sha256"
            ],
            old_complete["manifest_sha256"],
        )
        current_core = load_latest_route_cohort(self.root)
        self.assertEqual(current_core["pointer"], advanced["pointer"])
        self.assertEqual(
            current_core["cohort"]["route_cohort_id"],
            next_core["route_cohort_id"],
        )

    def test_caller_pre_attested_input_is_rejected_before_public_write(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        supplied = copy.deepcopy(fixture["opportunity_inputs"])
        classified = supplied[0]["classified_opportunity"]
        attestation = route_publication._issue_publication_attestation(
            cohort_id=classified["cohort_id"],
            opportunity_id=classified["opportunity_id"],
            route_id=classified["route_id"],
            target_token_quantity=classified["target_token_quantity"],
            buy_state_id=classified["buy_state_id"],
            sell_state_id=classified["sell_state_id"],
            buy_usd_projection_sha256=classified["buy_usd_projection_sha256"],
            sell_usd_projection_sha256=classified["sell_usd_projection_sha256"],
            cost_component_set_sha256=classified["cost_component_set_sha256"],
            mode_evidence_sha256=classified["mode_evidence_sha256"],
            core_manifest_sha256=fixture["pointer"]["manifest_sha256"],
        )
        supplied[0]["classified_opportunity"] = build_route_opportunity(
            **supplied[0]["build_inputs"],
            publication_attestation=attestation,
        )
        self.assertTrue(
            supplied[0]["classified_opportunity"]["strict_eligible"]
        )
        routes_root = Path(self.temporary.name) / "routes-public"

        with self.assertRaisesRegex(ValueError, "must not be attested"):
            route_publication.publish_complete_route_bundle(
                core_root=self.root,
                routes_root=routes_root,
                raw_root=raw_root,
                source_root=fixture["source_root"],
                fee_profile_path=fixture["fee_profile_path"],
                fee_profile_id=fixture["fee_profile_id"],
                inventory_profile_path=fixture["inventory_profile_path"],
                opportunity_inputs=supplied,
            )

        self.assertFalse(routes_root.exists())

    def test_stage_write_validation_rename_reread_and_core_failures_preserve_pointer(self):
        phases = (
            "partial-write", "validation", "rename", "reread", "core-swap",
            "lock",
        )
        for phase in phases:
            with self.subTest(phase=phase):
                case = Path(self.temporary.name) / phase
                core_root = case / "routes/core"
                raw_root = case / "raw/route-cohort"
                fixture = _task7_cex_inputs(
                    core_root,
                    raw_root,
                    case / "sources",
                    case / "private",
                )
                routes_root = case / "routes-public"
                routes_root.mkdir(parents=True)
                old_pointer = b'{"sentinel":"old-' + phase.encode("ascii") + b'"}\n'
                (routes_root / "latest.json").write_bytes(old_pointer)
                core_bundle = core_root / "bundles" / fixture["cohort"]["route_cohort_id"]
                core_before = {
                    path.name: path.read_bytes() for path in core_bundle.iterdir()
                }
                kwargs = {
                    "core_root": core_root,
                    "routes_root": routes_root,
                    "raw_root": raw_root,
                    "source_root": fixture["source_root"],
                    "fee_profile_path": fixture["fee_profile_path"],
                    "fee_profile_id": fixture["fee_profile_id"],
                    "inventory_profile_path": fixture["inventory_profile_path"],
                    "opportunity_inputs": fixture["opportunity_inputs"],
                }

                if phase == "partial-write":
                    original = route_publication._write_new_bytes_at

                    def fail_cost(directory_fd, filename, value):
                        if filename == "cost_components.csv":
                            raise OSError("injected partial write failure")
                        return original(directory_fd, filename, value)

                    failure = patch(
                        "scripts.route_publication._write_new_bytes_at",
                        side_effect=fail_cost,
                    )
                elif phase == "validation":
                    failure = patch(
                        "scripts.route_publication._validate_complete_route_bundle",
                        side_effect=ValueError("injected validation failure"),
                    )
                elif phase == "rename":
                    failure = patch(
                        "scripts.route_publication._rename_directory_noreplace",
                        side_effect=OSError("injected rename failure"),
                    )
                elif phase == "core-swap":
                    failure = patch(
                        "scripts.route_publication._verify_complete_core_lineage",
                        side_effect=ValueError("injected core swap"),
                    )
                elif phase == "lock":
                    original_flock = route_publication.fcntl.flock

                    def fail_exclusive(fd, operation):
                        if operation == route_publication.fcntl.LOCK_EX:
                            raise OSError("injected lock failure")
                        return original_flock(fd, operation)

                    failure = patch(
                        "scripts.route_publication.fcntl.flock",
                        side_effect=fail_exclusive,
                    )
                else:
                    original_validate = route_publication._validate_complete_route_bundle
                    calls = {"count": 0}

                    def fail_final_reread(*args, **kwargs):
                        calls["count"] += 1
                        if calls["count"] == 2:
                            raise ValueError("injected final reread failure")
                        return original_validate(*args, **kwargs)

                    failure = patch(
                        "scripts.route_publication._validate_complete_route_bundle",
                        side_effect=fail_final_reread,
                    )
                with failure:
                    with self.assertRaises(Exception):
                        route_publication.publish_complete_route_bundle(**kwargs)

                self.assertEqual(
                    (routes_root / "latest.json").read_bytes(), old_pointer
                )
                self.assertEqual(
                    {path.name: path.read_bytes() for path in core_bundle.iterdir()},
                    core_before,
                )
                bundles = routes_root / "bundles"
                if bundles.exists():
                    self.assertFalse(any(
                        path.name.startswith(".route-opportunity-")
                        for path in bundles.iterdir()
                    ))

                pointer = route_publication.publish_complete_route_bundle(**kwargs)
                self.assertEqual(
                    route_publication.load_latest_complete_route_bundle(routes_root)["pointer"],
                    pointer,
                )

    def test_raw_and_typed_source_tampering_fails_before_public_pointer(self):
        cases = ("raw-bytes", "raw-symlink", "typed-cross-bind", "typed-symlink")
        for case_name in cases:
            with self.subTest(case=case_name):
                case = Path(self.temporary.name) / ("tamper-" + case_name)
                core_root = case / "routes/core"
                raw_root = case / "raw/route-cohort"
                fixture = _task7_cex_inputs(
                    core_root, raw_root, case / "sources", case / "private"
                )
                routes_root = case / "routes-public"
                routes_root.mkdir(parents=True)
                old = b'{"sentinel":"old"}\n'
                (routes_root / "latest.json").write_bytes(old)
                kwargs = {
                    "core_root": core_root,
                    "routes_root": routes_root,
                    "raw_root": raw_root,
                    "source_root": fixture["source_root"],
                    "fee_profile_path": fixture["fee_profile_path"],
                    "fee_profile_id": fixture["fee_profile_id"],
                    "inventory_profile_path": fixture["inventory_profile_path"],
                    "opportunity_inputs": fixture["opportunity_inputs"],
                }
                if case_name.startswith("raw-"):
                    market_id = fixture["cohort"]["legs"][0]["market_id"]
                    response = (
                        raw_root / fixture["cohort"]["raw_evidence_run_id"]
                        / "accepted" / hashlib.sha256(market_id.encode()).hexdigest()
                        / "response.json"
                    )
                    if case_name == "raw-bytes":
                        response.write_bytes(response.read_bytes() + b" ")
                    else:
                        external = case / "external-response.json"
                        external.write_text("{}", encoding="utf-8")
                        response.unlink()
                        response.symlink_to(external)
                elif case_name == "typed-cross-bind":
                    tampered = copy.deepcopy(fixture["opportunity_inputs"])
                    for item in tampered:
                        item["source_members"]["buy_market_rules"] = (
                            item["source_members"]["sell_market_rules"]
                        )
                    kwargs["opportunity_inputs"] = tampered
                else:
                    source = fixture["source_root"] / "binance-rules.json"
                    external = case / "external-rules.json"
                    external.write_bytes(source.read_bytes())
                    source.unlink()
                    source.symlink_to(external)

                with self.assertRaises(ValueError):
                    route_publication.publish_complete_route_bundle(**kwargs)
                self.assertEqual((routes_root / "latest.json").read_bytes(), old)

    def test_replayed_cex_endpoint_must_match_the_safe_core_endpoint(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root, raw_root,
            Path(self.temporary.name) / "sources",
            Path(self.temporary.name) / "private",
        )
        original = source_request

        def changed_endpoint(*args, **kwargs):
            endpoint, instrument, quote_asset, full_book = original(*args, **kwargs)
            endpoint = endpoint.replace("/api/", "/unexpected/")
            return endpoint, instrument, quote_asset, full_book

        with patch(
            "scripts.route_publication.source_request",
            side_effect=changed_endpoint,
        ):
            with self.assertRaisesRegex(ValueError, "endpoint"):
                route_publication.build_complete_route_bundle(
                    core_root=self.root,
                    raw_root=raw_root,
                    source_root=fixture["source_root"],
                    fee_profile_path=fixture["fee_profile_path"],
                    fee_profile_id=fixture["fee_profile_id"],
                    inventory_profile_path=fixture["inventory_profile_path"],
                    opportunity_inputs=fixture["opportunity_inputs"],
                )

    def test_stale_fee_or_inventory_profile_fails_before_public_pointer(self):
        for profile in ("fee", "inventory"):
            with self.subTest(profile=profile):
                case = Path(self.temporary.name) / ("stale-" + profile)
                core_root = case / "routes/core"
                raw_root = case / "raw/route-cohort"
                fixture = _task7_cex_inputs(
                    core_root, raw_root, case / "sources", case / "private"
                )
                target = (
                    fixture["fee_profile_path"]
                    if profile == "fee" else fixture["inventory_profile_path"]
                )
                target.write_text(
                    target.read_text(encoding="utf-8").replace(
                        "2026-08-01T13:00:00Z", "2026-08-01T12:01:00Z"
                    ),
                    encoding="utf-8",
                )
                target.chmod(0o600)
                routes_root = case / "routes-public"
                routes_root.mkdir(parents=True)
                old = b'{"sentinel":"old"}\n'
                (routes_root / "latest.json").write_bytes(old)
                with self.assertRaisesRegex(ValueError, "stale|invalid"):
                    route_publication.publish_complete_route_bundle(
                        core_root=core_root,
                        routes_root=routes_root,
                        raw_root=raw_root,
                        source_root=fixture["source_root"],
                        fee_profile_path=fixture["fee_profile_path"],
                        fee_profile_id=fixture["fee_profile_id"],
                        inventory_profile_path=fixture["inventory_profile_path"],
                        opportunity_inputs=fixture["opportunity_inputs"],
                    )
                self.assertEqual((routes_root / "latest.json").read_bytes(), old)

    def test_component_omission_duplication_and_orphan_are_rejected(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root, raw_root,
            Path(self.temporary.name) / "sources",
            Path(self.temporary.name) / "private",
        )
        complete = route_publication.build_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        omission = copy.deepcopy(complete)
        omission["cost_components"].pop(0)
        duplicate = copy.deepcopy(complete)
        duplicate["cost_components"].append(copy.deepcopy(duplicate["cost_components"][0]))
        duplicate["cost_components"].sort(key=lambda row: (
            row["opportunity_id"], row["leg"], row["component_type"]
        ))
        orphan = copy.deepcopy(complete)
        extra = copy.deepcopy(orphan["cost_components"][0])
        extra["opportunity_id"] = "route:orphan:1000"
        orphan["cost_components"].append(extra)
        orphan["cost_components"].sort(key=lambda row: (
            row["opportunity_id"], row["leg"], row["component_type"]
        ))
        for candidate in (omission, duplicate, orphan):
            with self.subTest(rows=len(candidate["cost_components"])):
                with self.assertRaises(ValueError):
                    route_publication._validate_complete_logical_bundle(candidate)

    def test_live_publication_rejects_collapsed_atomic_nine_row_topology(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root, raw_root,
            Path(self.temporary.name) / "sources",
            Path(self.temporary.name) / "private",
        )
        complete = route_publication.build_complete_route_bundle(
            core_root=self.root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        route_publication._validate_complete_logical_bundle(complete)
        candidate = _atomic_complete_bundle(complete)
        route_publication._validate_complete_logical_bundle(candidate)

        opportunity = dict(candidate["opportunities"][0])
        scenario_id = opportunity["opportunity_id"]
        collapsed_costs = _atomic_cost_rows(
            candidate,
            opportunity,
            collapse_route_gas=True,
        )
        collapsed_keys = frozenset(
            (row["leg"], row["component_type"])
            for row in collapsed_costs
        )
        live_keys = route_publication.live_complete_cost_component_keys(
            candidate["routes"][0]
        )
        self.assertEqual(len(live_keys), 10)
        self.assertEqual(len(collapsed_costs), 9)
        self.assertEqual(len(collapsed_keys), 9)
        self.assertEqual(
            live_keys - collapsed_keys,
            frozenset({
                ("buy", "network_gas"),
                ("sell", "network_gas"),
            }),
        )
        self.assertEqual(
            collapsed_keys - live_keys,
            frozenset({("route", "network_gas")}),
        )

        candidate["cost_components"] = [
            row for row in candidate["cost_components"]
            if row["opportunity_id"] != scenario_id
        ] + collapsed_costs
        candidate["cost_components"].sort(key=lambda row: (
            row["opportunity_id"], row["leg"], row["component_type"]
        ))
        opportunity["cost_component_set_sha256"] = (
            route_publication._canonical_cost_set_sha256(collapsed_costs)
        )
        candidate["opportunities"][0] = _rehash_opportunity(opportunity)
        candidate["input_generations"]["cost_component_generation"] = (
            route_publication._canonical_input_sha256(
                candidate["cost_components"]
            )
        )
        candidate["input_generations"]["classified_opportunity_generation"] = (
            route_publication._canonical_input_sha256(
                candidate["opportunities"]
            )
        )

        live_helper = route_publication.live_complete_cost_component_keys
        with patch(
            "scripts.route_publication.live_complete_cost_component_keys",
            wraps=live_helper,
        ) as topology:
            with self.assertRaisesRegex(
                route_publication.RoutePublicationError,
                "route opportunity cost binding is invalid",
            ):
                route_publication._validate_complete_logical_bundle(candidate)

        topology.assert_called_once_with(candidate["routes"][0])

        helper_calls = 0

        def accept_one_collapsed_scenario(route):
            nonlocal helper_calls
            helper_calls += 1
            if helper_calls == 1:
                return collapsed_keys
            return live_helper(route)

        with patch(
            "scripts.route_publication.live_complete_cost_component_keys",
            side_effect=accept_one_collapsed_scenario,
        ) as mutant:
            validated = route_publication._validate_complete_logical_bundle(
                candidate
            )

        self.assertEqual(mutant.call_count, 5)
        self.assertEqual(validated["cost_components"], candidate["cost_components"])

    def test_sqlite_csv_divergence_and_incomplete_public_pointer_are_rejected(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root, raw_root,
            Path(self.temporary.name) / "sources",
            Path(self.temporary.name) / "private",
        )
        routes_root = Path(self.temporary.name) / "routes-public"
        route_publication.publish_complete_route_bundle(
            core_root=self.root,
            routes_root=routes_root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        bundle = routes_root / "bundles" / fixture["cohort"]["route_cohort_id"]
        database = bundle / "route_cohort.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            records = connection.execute(
                "SELECT opportunity_id, row_json FROM route_opportunities "
                "ORDER BY opportunity_id"
            ).fetchall()
            connection.execute(
                "UPDATE route_opportunities SET row_json = ? WHERE opportunity_id = ?",
                (records[1][1], records[0][0]),
            )
            connection.commit()
        finally:
            connection.close()
        _refresh_database_hash_in_manifest(bundle)
        with self.assertRaisesRegex(ValueError, "projection|match"):
            route_publication.validate_complete_route_bundle(bundle)

        incomplete = Path(self.temporary.name) / "incomplete-public"
        incomplete.mkdir()
        (incomplete / "latest.json").write_bytes(
            (self.root / "latest.json").read_bytes()
        )
        with self.assertRaisesRegex(ValueError, "pointer schema"):
            route_publication.load_latest_complete_route_bundle(incomplete)

    def test_pointer_rollback_never_overwrites_a_concurrent_writer(self):
        routes_root = Path(self.temporary.name) / "concurrent-routes"
        routes_root.mkdir()
        old = route_publication._pointer_payload_bytes({"writer": "old"})
        attempted = route_publication._pointer_payload_bytes({"writer": "attempted"})
        third_party = route_publication._pointer_payload_bytes({"writer": "third"})
        pointer_path = routes_root / "latest.json"
        pointer_path.write_bytes(old)
        routes_fd = os.open(str(routes_root), os.O_RDONLY)
        try:
            snapshot = route_publication._optional_pointer_snapshot_at(routes_fd)
            pointer_path.write_bytes(third_party)
            with self.assertRaisesRegex(ValueError, "concurrent writer"):
                route_publication._restore_pointer_after_failure(
                    routes_fd, routes_root, snapshot, attempted
                )
            self.assertEqual(pointer_path.read_bytes(), third_party)
        finally:
            os.close(routes_fd)

    def test_manifest_classification_count_tamper_is_rejected(self):
        raw_root = Path(self.temporary.name) / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            self.root,
            raw_root,
            Path(self.temporary.name) / "typed-sources",
            Path(self.temporary.name) / "private-profiles",
        )
        routes_root = Path(self.temporary.name) / "routes"
        route_publication.publish_complete_route_bundle(
            core_root=self.root,
            routes_root=routes_root,
            raw_root=raw_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        bundle = routes_root / "bundles" / fixture["cohort"]["route_cohort_id"]
        manifest_path = bundle / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["counts"]["classification"]["strict"] -= 1
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "manifest"):
            route_publication.validate_complete_route_bundle(bundle)

    def test_rebalance_required_core_mode_round_trips_without_remapping(self):
        cohort = _cohort()
        route = cohort["routes"][0]
        old_id = route["route_id"]
        route["route_mode"] = "rebalance_required"
        route["route_id"] = old_id.rsplit(":", 1)[0] + ":rebalance_required"
        for row in cohort["route_rows"]:
            if row["route_id"] == old_id:
                row["route_mode"] = "rebalance_required"
                row["route_id"] = route["route_id"]
        cohort = _rehash(cohort)

        publish_route_cohort_bundle(cohort, core_root=self.root)
        loaded = load_latest_route_cohort(self.root)
        self.assertEqual(loaded["cohort"]["routes"][0]["route_mode"], "rebalance_required")


class DeterministicRoutePublicationTests(TemporaryRouteRootTestCase):
    def test_typed_source_lineage_round_trips_csv_sqlite_and_loader(self):
        cohort = _cohort()
        expected_by_market = {}
        for index, leg in enumerate(cohort["legs"], start=1):
            lineage = _cex_typed_source_lineage("leg{}".format(index))
            leg["typed_source_lineage"] = lineage
            expected_by_market[leg["market_id"]] = lineage
        cohort = _rehash(cohort)

        publish_route_cohort_bundle(cohort, core_root=self.root)
        loaded = load_latest_route_cohort(self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]

        self.assertEqual(
            {
                row["market_id"]: row["typed_source_lineage"]
                for row in loaded["legs"]
            },
            expected_by_market,
        )
        with (bundle / "route_legs.csv").open(
            newline="", encoding="utf-8"
        ) as handle:
            csv_lineages = {
                row["market_id"]: json.loads(row["row_json"])[
                    "typed_source_lineage"
                ]
                for row in csv.DictReader(handle)
            }
        self.assertEqual(csv_lineages, expected_by_market)
        connection = sqlite3.connect(str(bundle / "route_cohort.sqlite3"))
        try:
            sqlite_lineages = {
                market_id: json.loads(row_json)["typed_source_lineage"]
                for market_id, row_json in connection.execute(
                    "SELECT market_id, row_json FROM route_legs ORDER BY market_id"
                )
            }
        finally:
            connection.close()
        self.assertEqual(sqlite_lineages, expected_by_market)

    def test_typed_source_lineage_exact_contract_fails_closed(self):
        cases = []

        extra_key = _cex_typed_source_lineage("extra")
        extra_key["members"][0]["extra"] = True
        cases.append(("extra", extra_key))

        wrong_market_role = _cex_typed_source_lineage("wrong-role")
        wrong_market_role["members"][0].update({
            "role": "dex_pool_state",
            "adapter_id": "route_quantity_quote_for_v2_pool/v1",
            "content_schema": "route_v2_pool_state/v1",
        })
        cases.append(("role", wrong_market_role))

        duplicate_role = _cex_typed_source_lineage("duplicate")
        duplicate_role["members"][1]["role"] = "cex_market_rules"
        cases.append(("role", duplicate_role))

        unavailable_with_bytes = _cex_typed_source_lineage("unavailable")
        unavailable_with_bytes["members"][0].update({
            "status": "unavailable",
            "reason_code": "typed_source_missing",
        })
        cases.append(("unavailable", unavailable_with_bytes))

        oversized = _cex_typed_source_lineage("oversized")
        oversized["members"][0]["size"] = 256 * 1024 + 1
        cases.append(("size", oversized))

        unsorted = _cex_typed_source_lineage("unsorted")
        unsorted["members"] = list(reversed(unsorted["members"]))
        cases.append(("order", unsorted))

        for label, lineage in cases:
            with self.subTest(label=label):
                cohort = _cohort()
                cohort["legs"][0]["typed_source_lineage"] = lineage
                cohort = _rehash(cohort)
                with self.assertRaisesRegex(ValueError, "typed-source"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_route_volume_lineage_round_trips_candidate_csv_and_sqlite(self):
        cohort = _cohort()

        publish_route_cohort_bundle(cohort, core_root=self.root)
        loaded = load_latest_route_cohort(self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]

        candidate = loaded["candidates"][0]
        self.assertEqual(candidate["buy_reference_volume_usd"], "9000")
        self.assertEqual(candidate["sell_reference_volume_usd"], "7000")
        self.assertEqual(candidate["route_volume_usd"], "7000")
        self.assertEqual(
            candidate["route_volume_basis"],
            "minimum_leg_source_horizon_usd",
        )
        with (bundle / "route_candidates.csv").open(
            newline="", encoding="utf-8"
        ) as handle:
            csv_row = next(csv.DictReader(handle))
        self.assertEqual(csv_row["buy_reference_volume_usd"], "9000")
        self.assertEqual(csv_row["sell_reference_volume_usd"], "7000")
        self.assertEqual(csv_row["route_volume_usd"], "7000")
        self.assertEqual(
            csv_row["route_volume_basis"],
            "minimum_leg_source_horizon_usd",
        )
        connection = sqlite3.connect(str(bundle / "route_cohort.sqlite3"))
        try:
            sqlite_row = connection.execute(
                "SELECT buy_reference_volume_usd, sell_reference_volume_usd, "
                "route_volume_usd, route_volume_basis "
                "FROM route_candidates ORDER BY route_id LIMIT 1"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(
            sqlite_row,
            ("9000", "7000", "7000", "minimum_leg_source_horizon_usd"),
        )

    def test_route_volume_must_equal_positive_minimum_or_na(self):
        cases = []
        wrong_minimum = _cohort()
        wrong_minimum["routes"][0]["route_volume_usd"] = "9000"
        wrong_minimum["route_rows"][0]["route_volume_usd"] = "9000"
        cases.append(wrong_minimum)

        invented_missing_leg_volume = _cohort()
        invented_missing_leg_volume["routes"][0]["sell_reference_volume_usd"] = None
        invented_missing_leg_volume["route_rows"][0]["sell_reference_volume_usd"] = None
        cases.append(invented_missing_leg_volume)

        noncanonical_decimal = _cohort()
        noncanonical_decimal["routes"][0]["route_volume_usd"] = "7000.0"
        noncanonical_decimal["route_rows"][0]["route_volume_usd"] = "7000.0"
        cases.append(noncanonical_decimal)

        for cohort in cases:
            with self.subTest(route=cohort["routes"][0]):
                with self.assertRaisesRegex(ValueError, "route volume lineage"):
                    publish_route_cohort_bundle(_rehash(cohort), core_root=self.root)

    def test_shuffled_rows_publish_identical_five_file_bundles(self):
        first_root = self.root / "first"
        second_root = self.root / "second"
        cohort = _cohort()
        shuffled = copy.deepcopy(cohort)
        for field in ("routes", "legs", "route_rows"):
            shuffled[field].reverse()

        first_pointer = publish_route_cohort_bundle(cohort, core_root=first_root)
        second_pointer = publish_route_cohort_bundle(
            shuffled, core_root=second_root
        )

        self.assertEqual(first_pointer, second_pointer)
        cohort_id = cohort["route_cohort_id"]
        first_bundle = first_root / "bundles" / cohort_id
        second_bundle = second_root / "bundles" / cohort_id
        expected_files = {
            "manifest.json",
            "route_candidates.csv",
            "route_cohort.sqlite3",
            "route_legs.csv",
            "route_timing.csv",
        }
        self.assertEqual(
            {path.name for path in first_bundle.iterdir()}, expected_files
        )
        self.assertEqual(
            {path.name for path in second_bundle.iterdir()}, expected_files
        )
        for filename in expected_files:
            self.assertEqual(
                (first_bundle / filename).read_bytes(),
                (second_bundle / filename).read_bytes(),
                filename,
            )

        first = validate_route_cohort_bundle(first_bundle)
        second = validate_route_cohort_bundle(second_bundle)
        self.assertEqual(first["manifest"], second["manifest"])
        self.assertEqual(first["candidates"], second["candidates"])
        self.assertEqual(first["legs"], second["legs"])
        self.assertEqual(first["timing"], second["timing"])
        self.assertEqual(
            first["manifest"]["files"]["route_cohort.sqlite3"][
                "logical_sha256"
            ],
            second["manifest"]["files"]["route_cohort.sqlite3"][
                "logical_sha256"
            ],
        )

    def test_latest_loader_cross_validates_csv_sqlite_manifest_and_pointer(self):
        cohort = _cohort()
        pointer = publish_route_cohort_bundle(cohort, core_root=self.root)

        loaded = load_latest_route_cohort(self.root)

        self.assertEqual(loaded["pointer"], pointer)
        self.assertEqual(loaded["manifest"]["bundle_stage"], "route_cohort_core/v1")
        self.assertEqual(
            {row["route_id"] for row in loaded["candidates"]},
            {row["route_id"] for row in cohort["routes"]},
        )
        self.assertEqual(
            {row["market_id"] for row in loaded["legs"]},
            {row["market_id"] for row in cohort["legs"]},
        )
        self.assertEqual(
            {row["route_id"] for row in loaded["timing"]},
            {row["route_id"] for row in cohort["route_rows"]},
        )

    def test_public_complete_pointer_is_never_created_or_replaced(self):
        public_pointer = self.root.parent / "latest.json"
        public_pointer.parent.mkdir(parents=True)
        sentinel = b'{"schema":"complete-sentinel"}\n'
        public_pointer.write_bytes(sentinel)
        public_bundles = self.root.parent / "bundles"
        public_bundles.mkdir()
        bundle_sentinel = public_bundles / "complete-sentinel"
        bundle_sentinel.write_bytes(b"complete bundle sentinel\n")

        publish_route_cohort_bundle(_cohort(), core_root=self.root)

        self.assertEqual(public_pointer.read_bytes(), sentinel)
        self.assertEqual(
            bundle_sentinel.read_bytes(), b"complete bundle sentinel\n"
        )
        self.assertEqual(list(public_bundles.iterdir()), [bundle_sentinel])

    def test_build_sqlite_returns_stable_logical_fingerprint(self):
        cohort = _cohort()
        shuffled = copy.deepcopy(cohort)
        for field in ("routes", "legs", "route_rows"):
            shuffled[field].reverse()
        first = Path(self.temporary.name) / "first.sqlite3"
        second = Path(self.temporary.name) / "second.sqlite3"

        first_logical = build_route_cohort_sqlite(first, cohort)
        second_logical = build_route_cohort_sqlite(second, shuffled)

        self.assertEqual(first_logical, second_logical)
        self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_canonical_utc_nanoseconds_are_preserved_exactly(self):
        cohort = _dex_cohort()
        cohort["target_observed_at"] = "2026-08-01T11:59:58.123456789Z"
        cohort["legs"][0][
            "state_observed_at"
        ] = "2026-08-01T12:00:01.123456789Z"
        cohort["legs"][1][
            "state_observed_at"
        ] = "2026-08-01T12:00:02.123456789Z"
        for leg in cohort["legs"]:
            leg["fixed_block_timestamp"] = "2026-08-01T11:59:59.123456789Z"
        for row in cohort["route_rows"]:
            row["skew_seconds"] = "1.000000000"
        cohort = _rehash(cohort)

        publish_route_cohort_bundle(cohort, core_root=self.root)
        loaded = load_latest_route_cohort(self.root)["cohort"]

        self.assertEqual(
            loaded["target_observed_at"],
            "2026-08-01T11:59:58.123456789Z",
        )
        self.assertEqual(
            loaded["legs"][0]["state_observed_at"],
            "2026-08-01T12:00:01.123456789Z",
        )
        self.assertEqual(
            loaded["legs"][0]["fixed_block_timestamp"],
            "2026-08-01T11:59:59.123456789Z",
        )

    def test_raw_evidence_path_unsafe_terminal_reason_round_trips(self):
        cohort = _cohort()
        failed_market = cohort["legs"][0]["market_id"]
        cohort["legs"][0]["status"] = "failed"
        cohort["legs"][0]["available"] = False
        cohort["legs"][0]["reason_code"] = "raw_evidence_path_unsafe"
        for row in cohort["route_rows"]:
            row["skew_seconds"] = None
            row["timing_status"] = "unavailable"
            row["reason_code"] = (
                "buy_leg_unavailable"
                if row["buy_market_id"] == failed_market
                else "sell_leg_unavailable"
            )
        cohort = _rehash(cohort)

        publish_route_cohort_bundle(cohort, core_root=self.root)
        loaded = load_latest_route_cohort(self.root)["cohort"]

        failed = next(
            leg for leg in loaded["legs"] if leg["market_id"] == failed_market
        )
        self.assertEqual(failed["reason_code"], "raw_evidence_path_unsafe")


class RoutePublicationFailureTests(TemporaryRouteRootTestCase):
    def test_cex_collector_and_orchestrator_reason_inventory_is_publishable(self):
        terminal_reasons = {
            "observed": "observed",
            "source_level_limit": "partial",
            "source_no_two_sided_book": "failed",
            "source_no_order_book": "failed",
            "source_invalid_order_book": "failed",
            "not_listed": "failed",
            "rate_limit": "failed",
            "source_unavailable": "failed",
            "source_rejected_request": "failed",
            "network": "failed",
            "parse": "failed",
            "unsupported_source": "failed",
            "collection_failed": "failed",
            "collector_identity_mismatch": "failed",
            "raw_evidence_missing": "failed",
            "raw_evidence_hash_mismatch": "failed",
            "raw_evidence_path_unsafe": "failed",
            "route_deadline_exceeded": "deadline_exceeded",
        }
        for reason_code, status in terminal_reasons.items():
            with self.subTest(reason_code=reason_code):
                market_id = "cex:binance:UNI/USDT"
                leg = {
                    "leg_id": market_id,
                    "market_id": market_id,
                    "market_type": "cex",
                    "token_symbol": "UNI",
                    "status": status,
                    "available": status in {"observed", "partial"},
                    "reason_code": reason_code,
                }
                if status in {"observed", "partial"}:
                    leg.update({
                        "state_observed_at": "2026-08-01T12:00:01Z",
                        "snapshot_id": "snapshot-a",
                        "raw_response_sha256": "a" * 64,
                    })
                rows = route_publication._validate_leg_rows(
                    [leg],
                    raw_evidence_run_id="snapshot-a",
                    collection_completed_at="2026-08-01T12:00:03Z",
                    collection_deadline_at="2026-08-01T12:01:00Z",
                )
                self.assertEqual(rows[market_id]["reason_code"], reason_code)

    def test_cex_leg_status_and_reason_must_match_collector_contract(self):
        forged_pairs = (
            ("observed", "network"),
            ("partial", "observed"),
            ("failed", "observed"),
            ("unsupported", "unsupported_source"),
            ("deadline_exceeded", "network"),
        )
        for status, reason_code in forged_pairs:
            with self.subTest(status=status, reason_code=reason_code):
                cohort = _cohort()
                leg = cohort["legs"][0]
                leg["status"] = status
                leg["available"] = status in {"observed", "partial"}
                leg["reason_code"] = reason_code
                cohort = _rehash(cohort)
                with self.assertRaisesRegex(
                    ValueError, "CEX leg status and reason conflict"
                ):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_dex_collector_terminal_reason_inventory_is_publishable(self):
        terminal_reasons = {
            "measurement_limit": "partial",
            "source_range_unavailable": "unsupported",
            "unsupported_chain": "unsupported",
            "unsupported_protocol": "unsupported",
            "unsupported_method": "unsupported",
            "unsupported_source": "unsupported",
            "unsupported_protocol_or_chain": "unsupported",
            "network": "failed",
            "rate_limit": "failed",
            "source_unavailable": "failed",
            "parse": "failed",
            "validation": "failed",
            "collection_failed": "failed",
            "depth_usd_price_time_mismatch": "failed",
            "fixed_block_unavailable": "failed",
            "fixed_block_lineage_mismatch": "failed",
            "collector_identity_mismatch": "failed",
            "raw_evidence_missing": "failed",
            "raw_evidence_hash_mismatch": "failed",
            "raw_evidence_path_unsafe": "failed",
            "usd_price_context_missing": "failed",
            "usd_price_context_not_found": "failed",
            "usd_price_context_failed": "failed",
        }
        for reason_code, status in terminal_reasons.items():
            with self.subTest(reason_code=reason_code):
                market_id = (
                    "dex:eth:uniswap_v2:0x{}:UNI".format("a" * 40)
                    if status == "partial"
                    else "dex:solana:orca:pool-{}:UNI".format(reason_code)
                )
                leg = {
                    "leg_id": market_id,
                    "market_id": market_id,
                    "market_type": "dex",
                    "token_symbol": "UNI",
                    "status": status,
                    "available": status == "partial",
                    "reason_code": reason_code,
                }
                if status == "partial":
                    leg.update({
                        "state_observed_at": "2026-08-01T12:00:01Z",
                        "snapshot_id": "snapshot-a",
                        "raw_response_sha256": "a" * 64,
                        "fixed_block_number": "100",
                        "fixed_block_timestamp": "2026-08-01T11:59:59Z",
                    })
                rows = route_publication._validate_leg_rows(
                    [leg],
                    raw_evidence_run_id="snapshot-a",
                    collection_completed_at="2026-08-01T12:00:03Z",
                    collection_deadline_at="2026-08-01T12:01:00Z",
                )
                self.assertEqual(rows[market_id]["reason_code"], reason_code)

    def test_dex_leg_status_and_reason_must_match_collector_contract(self):
        forged_pairs = (
            ("observed", "unsupported_chain"),
            ("observed", "validation"),
            ("partial", "depth_usd_price_time_mismatch"),
            ("unsupported", "network"),
            ("failed", "unsupported_protocol"),
        )
        for status, reason_code in forged_pairs:
            with self.subTest(status=status, reason_code=reason_code):
                cohort = _dex_cohort()
                leg = cohort["legs"][0]
                leg["status"] = status
                leg["available"] = status in {"observed", "partial"}
                leg["reason_code"] = reason_code
                cohort = _rehash(cohort)
                with self.assertRaisesRegex(
                    ValueError, "DEX leg status and reason conflict"
                ):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_duplicate_route_identity_fails_closed(self):
        cohort = _cohort()
        cohort["routes"].append(copy.deepcopy(cohort["routes"][0]))

        with self.assertRaisesRegex(ValueError, "duplicate route candidate"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertFalse(self.root.exists())

    def test_route_and_timing_enum_drift_fail_closed(self):
        route_drift = _cohort()
        route_drift["routes"][0]["route_class"] = "strict"
        with self.assertRaisesRegex(ValueError, "enum"):
            publish_route_cohort_bundle(route_drift, core_root=self.root)

        timing_drift = _cohort()
        timing_drift["route_rows"][0]["timing_status"] = "stale"
        with self.assertRaisesRegex(ValueError, "timing status enum"):
            publish_route_cohort_bundle(timing_drift, core_root=self.root)

        leg_drift = _cohort()
        leg_drift["legs"][0]["reason_code"] = "future_reason_code"
        with self.assertRaisesRegex(ValueError, "leg reason enum"):
            publish_route_cohort_bundle(leg_drift, core_root=self.root)

        status_drift = _cohort()
        status_drift["legs"][0]["status"] = "future_status"
        with self.assertRaisesRegex(ValueError, "leg status enum"):
            publish_route_cohort_bundle(status_drift, core_root=self.root)

    def test_forged_exact_skew_and_future_state_time_fail_closed(self):
        forged_skew = _cohort()
        forged_skew["legs"][0][
            "state_observed_at"
        ] = "2026-08-01T12:00:00.000000000Z"
        forged_skew["legs"][1][
            "state_observed_at"
        ] = "2026-08-01T12:01:00.000000001Z"
        forged_skew["collection_completed_at"] = "2026-08-01T12:01:01Z"
        forged_skew["collection_deadline_at"] = "2026-08-01T12:02:00Z"
        for row in forged_skew["route_rows"]:
            row["validated_at"] = "2026-08-01T12:01:01Z"
            row["skew_seconds"] = "60.000000001"
            row["timing_status"] = "within_sla"
            row["reason_code"] = None
        forged_skew = _rehash(forged_skew)
        with self.assertRaisesRegex(ValueError, "timing classification"):
            publish_route_cohort_bundle(forged_skew, core_root=self.root)

        future_state = _cohort()
        future_state["collection_completed_at"] = "2026-08-01T12:00:00.5Z"
        for row in future_state["route_rows"]:
            row["validated_at"] = "2026-08-01T12:00:00.5Z"
        future_state = _rehash(future_state)
        with self.assertRaisesRegex(ValueError, "state timestamp is in the future"):
            publish_route_cohort_bundle(future_state, core_root=self.root)

    def test_candidate_and_collection_source_generation_conflicts_fail_closed(self):
        candidate_conflict = _cohort()
        candidate_conflict["routes"][0][
            "candidate_source_generation"
        ] = "candidate-generation-b"
        with self.assertRaisesRegex(ValueError, "candidate source lineage"):
            publish_route_cohort_bundle(candidate_conflict, core_root=self.root)

        collection_conflict = _cohort()
        collection_conflict["source_state"][
            "collection_input_generation"
        ] = "collection-generation-b"
        with self.assertRaisesRegex(ValueError, "source lineage"):
            publish_route_cohort_bundle(collection_conflict, core_root=self.root)

    def test_incomplete_route_pair_fails_closed(self):
        cohort = _cohort()
        cohort["legs"].pop()

        with self.assertRaisesRegex(ValueError, "route pair is incomplete"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_malformed_market_identity_is_rejected_before_bundle_writes(self):
        cohort = _cohort()
        original = "cex:alpha:UNI/USDT"
        malformed = "cex::"
        for leg in cohort["legs"]:
            if leg["market_id"] == original:
                leg["market_id"] = malformed
                leg["leg_id"] = malformed
        for row in cohort["routes"] + cohort["route_rows"]:
            if row["buy_market_id"] == original:
                row["buy_market_id"] = malformed
            if row["sell_market_id"] == original:
                row["sell_market_id"] = malformed
            row["route_id"] = "route:{}:{}->{}:{}".format(
                row["token_symbol"],
                row["buy_market_id"],
                row["sell_market_id"],
                row["route_mode"],
            )
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(ValueError, "market identity"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertFalse(self.root.exists())

    def test_float_coercion_cannot_satisfy_the_exact_notional_grid(self):
        cohort = _cohort()
        cohort["requested_notionals_usd"][0] = 1000.0
        for row in cohort["routes"] + cohort["route_rows"]:
            row["requested_notionals_usd"][0] = 1000.0
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(ValueError, "notional grid"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_fixed_block_lineage_conflict_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "fixed block lineage conflict"):
            publish_route_cohort_bundle(
                _dex_cohort_with_lineage_conflict(), core_root=self.root
            )

    def test_observed_dex_leg_requires_complete_fixed_block_lineage(self):
        cohort = _dex_cohort()
        cohort["legs"][0].pop("fixed_block_number")
        cohort["legs"][0].pop("fixed_block_timestamp")
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(ValueError, "observed DEX.*fixed block"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_terminal_dex_leg_cannot_drop_chain_fixed_block_lineage(self):
        cohort = _dex_cohort()
        terminal = cohort["legs"][0]
        terminal["status"] = "failed"
        terminal["available"] = False
        terminal["reason_code"] = "collection_failed"
        terminal.pop("fixed_block_number")
        terminal.pop("fixed_block_timestamp")
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(
            ValueError, "DEX chain fixed block lineage is incomplete"
        ):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_fixed_block_timestamp_cannot_exceed_collection_bound(self):
        after_completion = _dex_cohort()
        for leg in after_completion["legs"]:
            leg["fixed_block_timestamp"] = "2026-08-01T12:00:04.000000001Z"

        after_earlier_deadline = _dex_cohort()
        after_earlier_deadline[
            "collection_deadline_at"
        ] = "2026-08-01T12:00:00Z"
        for leg in after_earlier_deadline["legs"]:
            leg["fixed_block_timestamp"] = "2026-08-01T12:00:00.000000001Z"

        for label, cohort in (
            ("completion-bound", _rehash(after_completion)),
            ("deadline-bound", _rehash(after_earlier_deadline)),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    ValueError, "fixed block timestamp exceeds collection bound"
                ):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_all_metadata_timestamps_require_canonical_utc_z(self):
        cases = []

        top_level = _cohort()
        top_level["target_observed_at"] = "2026-08-01T20:00:00+08:00"
        cases.append(("top-level", _rehash(top_level)))

        leg_level = _cohort()
        leg_level["legs"][0][
            "state_observed_at"
        ] = "2026-08-01T20:00:01.000000000+08:00"
        cases.append(("leg", _rehash(leg_level)))

        fixed_block = _dex_cohort()
        for leg in fixed_block["legs"]:
            leg["fixed_block_timestamp"] = "2026-08-01T19:59:59+08:00"
        cases.append(("fixed-block", _rehash(fixed_block)))

        for label, cohort in cases:
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, "canonical UTC timestamp"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_collection_timeline_bounds_fail_closed(self):
        deadline_before_start = _cohort()
        deadline_before_start[
            "target_observed_at"
        ] = "2026-08-01T11:59:58Z"
        deadline_before_start[
            "collection_deadline_at"
        ] = "2026-08-01T11:59:59Z"

        target_after_deadline = _cohort()
        target_after_deadline[
            "target_observed_at"
        ] = "2026-08-01T12:01:00.000000001Z"

        cases = (
            ("deadline-before-start", _rehash(deadline_before_start)),
            ("target-after-deadline", _rehash(target_after_deadline)),
        )
        for label, cohort in cases:
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    ValueError, "route cohort collection timeline is invalid"
                ):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_selection_window_requires_strict_ordered_iso_dates(self):
        malformed = _cohort()
        malformed["selection_window"]["start"] = "2026-7-25"
        reversed_window = _cohort()
        reversed_window["selection_window"] = {
            "start": "2026-08-02",
            "end": "2026-08-01",
        }

        for label, cohort in (
            ("malformed", _rehash(malformed)),
            ("reversed", _rehash(reversed_window)),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, "selection window"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_nested_endpoint_credentials_and_local_paths_fail_closed(self):
        unsafe_values = (
            {"endpoint": "https://user:pass@example.test/rpc?api_key=secret"},
            {"cache_path": "/private/tmp/raw-response.json"},
        )
        for provenance in unsafe_values:
            with self.subTest(provenance=provenance):
                cohort = _cohort()
                cohort["legs"][0]["provenance"] = provenance
                cohort = _rehash(cohort)
                with self.assertRaisesRegex(ValueError, "unsafe evidence"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_task4_unsafe_evidence_forms_fail_closed(self):
        unsafe_mutations = {
            "file-uri": lambda leg: leg.update(
                {"provenance": {"uri": "file:///private/tmp/raw.json"}}
            ),
            "relative-parent": lambda leg: leg.update(
                {"provenance": "../private/raw.json"}
            ),
            "private-key": lambda leg: leg.update({"private_key": "secret"}),
            "cookie": lambda leg: leg.update({"cookie": "session=secret"}),
            "session": lambda leg: leg.update({"session_id": "secret"}),
            "credential-uri": lambda leg: leg.update(
                {"provenance": "ssh://user:secret@example.test/private"}
            ),
            "invalid-port": lambda leg: leg.update(
                {"source_endpoint": "https://example.test:99999/orderbook"}
            ),
        }
        for label, mutate in unsafe_mutations.items():
            with self.subTest(label=label):
                cohort = _cohort()
                mutate(cohort["legs"][0])
                cohort = _rehash(cohort)
                with self.assertRaisesRegex(ValueError, "unsafe evidence"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_terminal_nonempty_raw_hash_must_be_lowercase_sha256(self):
        for label, invalid_hash in (
            ("uppercase", "A" * 64),
            ("short", "a" * 63),
            ("nonhex", "g" * 64),
        ):
            with self.subTest(label=label):
                cohort = _cohort()
                failed_market = cohort["legs"][0]["market_id"]
                cohort["legs"][0]["status"] = "failed"
                cohort["legs"][0]["available"] = False
                cohort["legs"][0]["reason_code"] = "collection_failed"
                cohort["legs"][0]["raw_response_sha256"] = invalid_hash
                for row in cohort["route_rows"]:
                    row["skew_seconds"] = None
                    row["timing_status"] = "unavailable"
                    row["reason_code"] = (
                        "buy_leg_unavailable"
                        if row["buy_market_id"] == failed_market
                        else "sell_leg_unavailable"
                    )
                cohort = _rehash(cohort)

                with self.assertRaisesRegex(ValueError, "raw evidence hash"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_dex_pool_identity_rejects_path_like_components(self):
        cohort = _dex_cohort()
        original = "dex:eth:uniswap_v2:0x{}:UNI".format("a" * 40)
        malformed = "dex:eth:uniswap_v2:../x:UNI"
        for leg in cohort["legs"]:
            if leg["market_id"] == original:
                leg["market_id"] = malformed
                leg["leg_id"] = malformed
        for row in cohort["routes"] + cohort["route_rows"]:
            if row["buy_market_id"] == original:
                row["buy_market_id"] = malformed
            if row["sell_market_id"] == original:
                row["sell_market_id"] = malformed
            row["route_id"] = "route:{}:{}->{}:{}".format(
                row["token_symbol"],
                row["buy_market_id"],
                row["sell_market_id"],
                row["route_mode"],
            )
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(ValueError, "market identity"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_market_id_component_lengths_exactly_match_task4(self):
        invalid_ids = (
            "cex:{}:UNI/USDT".format("x" * 65),
            "cex:alpha:{}/USDT".format("U" * 65),
            "cex:alpha:UNI/{}".format("T" * 65),
            "dex:{}:uniswap:pool:UNI".format("c" * 65),
            "dex:eth:{}:pool:UNI".format("d" * 129),
            "dex:eth:uniswap:{}:UNI".format("p" * 257),
            "dex:eth:uniswap:pool:{}".format("U" * 65),
        )
        for market_id in invalid_ids:
            with self.subTest(market_id=market_id[:80]):
                with self.assertRaisesRegex(ValueError, "market identity"):
                    route_publication._canonical_market_token(market_id)

        self.assertEqual(
            route_publication._canonical_market_token(
                "cex:{}:{}/{}".format(
                    "x" * 64,
                    "U" * 64,
                    "T" * 64,
                )
            ),
            "U" * 64,
        )
        self.assertEqual(
            route_publication._canonical_market_token(
                "dex:{}:{}:{}:{}".format(
                    "c" * 64,
                    "d" * 128,
                    "p" * 256,
                    "U" * 64,
                )
            ),
            "U" * 64,
        )

    def test_tampered_file_hash_is_rejected(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        with (bundle / "route_legs.csv").open("ab") as handle:
            handle.write(b"\n")

        with self.assertRaisesRegex(ValueError, "checksum"):
            validate_route_cohort_bundle(bundle)

    def test_csv_sqlite_inventory_mismatch_is_rejected_even_with_new_file_hash(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        database = bundle / "route_cohort.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            connection.execute(
                "DELETE FROM route_timing WHERE route_id = ?",
                (cohort["route_rows"][0]["route_id"],),
            )
            connection.commit()
        finally:
            connection.close()
        _refresh_database_hash_in_manifest(bundle)

        with self.assertRaisesRegex(ValueError, "inventories do not match"):
            validate_route_cohort_bundle(bundle)

    def test_existing_immutable_id_is_never_overwritten(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer_before = pointer_path.read_bytes()
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        manifest_before = (bundle / "manifest.json").read_bytes()

        with self.assertRaisesRegex(ValueError, "already exists"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), pointer_before)
        self.assertEqual((bundle / "manifest.json").read_bytes(), manifest_before)

    def test_no_replace_rename_rejects_file_empty_nonempty_and_symlink_targets(self):
        for target_kind in ("file", "empty", "nonempty", "symlink"):
            with self.subTest(target_kind=target_kind):
                case_root = Path(self.temporary.name) / ("rename-" + target_kind)
                case_root.mkdir()
                stage = case_root / "stage"
                stage.mkdir()
                (stage / "payload").write_text("new", encoding="utf-8")
                target = case_root / "target"
                if target_kind == "file":
                    target.write_text("old", encoding="utf-8")
                elif target_kind == "empty":
                    target.mkdir()
                elif target_kind == "nonempty":
                    target.mkdir()
                    (target / "sentinel").write_text("old", encoding="utf-8")
                else:
                    external = case_root / "external"
                    external.mkdir()
                    target.symlink_to(external, target_is_directory=True)

                with self.assertRaisesRegex(ValueError, "already exists"):
                    route_publication._rename_directory_noreplace(stage, target)

                self.assertTrue(stage.is_dir())
                self.assertEqual((stage / "payload").read_text(encoding="utf-8"), "new")
                self.assertTrue(os.path.lexists(str(target)))

    def test_darwin_and_linux_no_replace_rename_use_verified_dirfds(self):
        class FakeOperation:
            def __init__(self):
                self.calls = []
                self.argtypes = None
                self.restype = None

            def __call__(self, *arguments):
                self.calls.append(arguments)
                return 0

        case_root = Path(self.temporary.name) / "rename-dirfd"
        case_root.mkdir()
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(str(case_root), flags)
        self.addCleanup(os.close, directory_fd)

        for platform, operation_name, expected_flag in (
            ("darwin", "renameatx_np", 0x00000004),
            ("linux", "renameat2", 1),
        ):
            with self.subTest(platform=platform):
                operation = FakeOperation()
                library = type("FakeLibrary", (), {})()
                setattr(library, operation_name, operation)
                with patch("scripts.route_publication.sys.platform", platform), patch(
                    "scripts.route_publication.ctypes.CDLL",
                    return_value=library,
                ):
                    route_publication._rename_directory_noreplace_at(
                        directory_fd,
                        "stage",
                        directory_fd,
                        "cohort",
                        destination_display=case_root / "cohort",
                    )

                self.assertEqual(
                    operation.calls,
                    [
                        (
                            directory_fd,
                            b"stage",
                            directory_fd,
                            b"cohort",
                            expected_flag,
                        )
                    ],
                )

    def test_destination_created_in_the_final_rename_race_is_not_replaced(self):
        cohort = _cohort()
        original = route_publication._rename_directory_noreplace

        def race(source, destination, **kwargs):
            destination.mkdir()
            return original(source, destination, **kwargs)

        with patch(
            "scripts.route_publication._rename_directory_noreplace",
            side_effect=race,
        ):
            with self.assertRaisesRegex(ValueError, "already exists"):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        final = self.root / "bundles" / cohort["route_cohort_id"]
        self.assertTrue(final.is_dir())
        self.assertEqual(list(final.iterdir()), [])
        self.assertFalse((self.root / "latest.json").exists())

    def test_stage_entry_swapped_before_rename_cannot_be_published(self):
        cohort = _cohort()
        original = route_publication._rename_directory_noreplace
        detached = self.root / "bundles" / ".detached-stage"

        def swap_stage(source, destination, **kwargs):
            os.rename(str(source), str(detached))
            shutil.copytree(str(detached), str(source))
            return original(source, destination, **kwargs)

        with patch(
            "scripts.route_publication._rename_directory_noreplace",
            side_effect=swap_stage,
        ):
            with self.assertRaisesRegex(ValueError, "changed during validation"):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertFalse((self.root / "latest.json").exists())
        final = self.root / "bundles" / cohort["route_cohort_id"]
        self.assertEqual(
            validate_route_cohort_bundle(final)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )

    def test_pointer_replace_failure_preserves_exact_old_pointer(self):
        first = _cohort()
        second = _second_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer_before = pointer_path.read_bytes()

        with patch(
            "scripts.route_publication.os.replace",
            side_effect=OSError("injected pointer failure"),
        ):
            with self.assertRaisesRegex(OSError, "injected pointer failure"):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), pointer_before)
        second_bundle = self.root / "bundles" / second["route_cohort_id"]
        self.assertEqual(
            validate_route_cohort_bundle(second_bundle)["cohort"]["route_cohort_id"],
            second["route_cohort_id"],
        )

    def test_post_replace_fsync_failure_keeps_new_pointer_over_old_pointer(self):
        first = _cohort()
        second = _second_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer_before = pointer_path.read_bytes()
        original_fsync_directory = route_publication._fsync_directory
        injected = {"failed": False}

        def fail_first_core_fsync(path, **kwargs):
            if Path(path).resolve() == self.root.resolve() and not injected["failed"]:
                injected["failed"] = True
                raise OSError("injected post-replace fsync failure")
            return original_fsync_directory(path, **kwargs)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=fail_first_core_fsync,
        ):
            with self.assertRaisesRegex(
                ValueError, "pointer state uncertain"
            ):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertNotEqual(pointer_path.read_bytes(), pointer_before)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            second["route_cohort_id"],
        )

    def test_post_replace_fsync_failure_keeps_new_pointer_when_none_existed(self):
        cohort = _cohort()
        original_fsync_directory = route_publication._fsync_directory
        injected = {"failed": False}

        def fail_first_core_fsync(path, **kwargs):
            if Path(path).resolve() == self.root.resolve() and not injected["failed"]:
                injected["failed"] = True
                raise OSError("injected post-replace fsync failure")
            return original_fsync_directory(path, **kwargs)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=fail_first_core_fsync,
        ):
            with self.assertRaisesRegex(
                ValueError, "pointer state uncertain"
            ):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertTrue((self.root / "latest.json").is_file())
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        self.assertEqual(
            validate_route_cohort_bundle(bundle)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )

    def test_concurrent_pointer_during_failed_fsync_is_preserved(self):
        first = _cohort()
        second = _second_cohort()
        third = _third_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        first_pointer = pointer_path.read_bytes()
        publish_route_cohort_bundle(third, core_root=self.root)
        third_pointer = pointer_path.read_bytes()

        restore_first = self.root / ".restore-first-pointer"
        restore_first.write_bytes(first_pointer)
        os.replace(str(restore_first), str(pointer_path))
        original_fsync_directory = route_publication._fsync_directory
        injected = {"done": False}

        def install_concurrent_pointer_then_fail(path, **kwargs):
            if Path(path).resolve() == self.root.resolve() and not injected["done"]:
                injected["done"] = True
                concurrent = self.root / ".concurrent-pointer"
                concurrent.write_bytes(third_pointer)
                with concurrent.open("rb") as handle:
                    os.fsync(handle.fileno())
                os.replace(str(concurrent), str(pointer_path))
                os.fsync(kwargs["directory_fd"])
                raise OSError("injected pointer fsync failure after concurrent C")
            return original_fsync_directory(path, **kwargs)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=install_concurrent_pointer_then_fail,
        ):
            with self.assertRaisesRegex(
                Exception,
                "pointer state uncertain",
            ):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), third_pointer)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            third["route_cohort_id"],
        )

    def test_concurrent_pointer_after_post_fsync_diagnostic_is_preserved(self):
        first = _cohort()
        second = _second_cohort()
        third = _third_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        first_pointer = pointer_path.read_bytes()
        publish_route_cohort_bundle(third, core_root=self.root)
        third_pointer = pointer_path.read_bytes()

        restore_first = self.root / ".restore-first-pointer"
        restore_first.write_bytes(first_pointer)
        os.replace(str(restore_first), str(pointer_path))
        original_snapshot = route_publication._optional_pointer_snapshot_at
        calls = {"count": 0}

        def install_concurrent_pointer_after_read(core_fd):
            calls["count"] += 1
            snapshot = original_snapshot(core_fd)
            if calls["count"] == 2:
                concurrent = self.root / ".concurrent-after-diagnostic"
                concurrent.write_bytes(third_pointer)
                os.replace(str(concurrent), str(pointer_path))
                raise OSError("injected diagnostic failure after concurrent C")
            return snapshot

        with patch(
            "scripts.route_publication._optional_pointer_snapshot_at",
            side_effect=install_concurrent_pointer_after_read,
        ):
            with self.assertRaisesRegex(ValueError, "pointer state uncertain"):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(calls["count"], 2)
        self.assertEqual(pointer_path.read_bytes(), third_pointer)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            third["route_cohort_id"],
        )

    def test_concurrent_pointer_during_successful_fsync_is_detected_and_preserved(self):
        first = _cohort()
        second = _second_cohort()
        third = _third_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        first_pointer = pointer_path.read_bytes()
        publish_route_cohort_bundle(third, core_root=self.root)
        third_pointer = pointer_path.read_bytes()

        restore_first = self.root / ".restore-first-pointer"
        restore_first.write_bytes(first_pointer)
        os.replace(str(restore_first), str(pointer_path))
        original_fsync_directory = route_publication._fsync_directory
        injected = {"done": False}

        def install_concurrent_pointer_and_succeed(path, **kwargs):
            if Path(path).resolve() == self.root.resolve() and not injected["done"]:
                injected["done"] = True
                concurrent = self.root / ".concurrent-during-fsync"
                concurrent.write_bytes(third_pointer)
                os.replace(str(concurrent), str(pointer_path))
                os.fsync(kwargs["directory_fd"])
                return None
            return original_fsync_directory(path, **kwargs)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=install_concurrent_pointer_and_succeed,
        ):
            with self.assertRaisesRegex(ValueError, "pointer state uncertain"):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), third_pointer)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            third["route_cohort_id"],
        )

    def test_pointer_lock_acquisition_failure_is_clear_and_preserves_old_pointer(self):
        first = _cohort()
        second = _second_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer_before = pointer_path.read_bytes()

        with patch(
            "scripts.route_publication.fcntl.flock",
            side_effect=OSError("injected lock acquisition failure"),
        ):
            with self.assertRaisesRegex(ValueError, "lock acquisition failed"):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), pointer_before)

    def test_pointer_lock_release_failure_after_commit_is_clear_and_fd_close_releases(self):
        first = _cohort()
        second = _second_cohort()
        original_flock = route_publication.fcntl.flock

        def fail_unlock(fd, operation):
            if operation == route_publication.fcntl.LOCK_UN:
                raise OSError("injected lock release failure")
            return original_flock(fd, operation)

        with patch(
            "scripts.route_publication.fcntl.flock",
            side_effect=fail_unlock,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "lock release failed after pointer commit",
            ):
                publish_route_cohort_bundle(first, core_root=self.root)

        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            first["route_cohort_id"],
        )
        publish_route_cohort_bundle(second, core_root=self.root)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            second["route_cohort_id"],
        )

    def test_pointer_lock_release_failure_does_not_mask_post_replace_failure(self):
        cohort = _cohort()
        original_fsync_directory = route_publication._fsync_directory
        original_flock = route_publication.fcntl.flock

        def fail_core_fsync(path, **kwargs):
            if Path(path).resolve() == self.root.resolve():
                raise OSError("injected post-replace fsync failure")
            return original_fsync_directory(path, **kwargs)

        def fail_unlock(fd, operation):
            if operation == route_publication.fcntl.LOCK_UN:
                raise OSError("injected lock release failure")
            return original_flock(fd, operation)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=fail_core_fsync,
        ), patch(
            "scripts.route_publication.fcntl.flock",
            side_effect=fail_unlock,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "pointer state uncertain",
            ) as raised:
                publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertIn(
            "injected post-replace fsync failure",
            str(raised.exception.__cause__),
        )
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )

    def test_bundle_directory_swap_during_validation_is_rejected(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundles = self.root / "bundles"
        bundle = bundles / cohort["route_cohort_id"]
        replacement = bundles / ".replacement"
        detached = bundles / ".detached"
        shutil.copytree(str(bundle), str(replacement))
        original_listdir = route_publication.os.listdir
        swapped = {"done": False}

        def swap_after_list(directory):
            entries = original_listdir(directory)
            if not swapped["done"]:
                swapped["done"] = True
                os.rename(str(bundle), str(detached))
                os.rename(str(replacement), str(bundle))
            return entries

        with patch(
            "scripts.route_publication.os.listdir",
            side_effect=swap_after_list,
        ):
            with self.assertRaisesRegex(ValueError, "changed during validation"):
                validate_route_cohort_bundle(bundle)

    def test_csv_replaced_after_read_during_final_validation_is_rejected(self):
        cohort = _cohort()
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        original_sqlite_validation = route_publication._read_and_validate_sqlite_at
        calls = {"count": 0}

        def replace_csv_after_its_read(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 2:
                replacement = bundle.parent / ".replacement-candidates.csv"
                replacement.write_bytes(b"attacker replacement\n")
                os.replace(
                    str(replacement),
                    str(bundle / "route_candidates.csv"),
                )
            return original_sqlite_validation(*args, **kwargs)

        with patch(
            "scripts.route_publication._read_and_validate_sqlite_at",
            side_effect=replace_csv_after_its_read,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "route candidate CSV changed during validation",
            ):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertFalse((self.root / "latest.json").exists())

    def test_pre_rename_failures_leave_no_final_bundle_or_pointer(self):
        original_fsync_directory = route_publication._fsync_directory

        def fail_stage_fsync(path, **kwargs):
            if path.name.startswith(".route-cohort-"):
                raise OSError("injected stage fsync failure")
            return original_fsync_directory(path, **kwargs)

        cases = {
            "write": patch(
                "scripts.route_publication._write_bundle_artifacts",
                side_effect=OSError("injected stage write failure"),
            ),
            "validate": patch(
                "scripts.route_publication._validate_route_cohort_bundle",
                side_effect=ValueError("injected stage validation failure"),
            ),
            "fsync": patch(
                "scripts.route_publication._fsync_directory",
                side_effect=fail_stage_fsync,
            ),
            "rename": patch(
                "scripts.route_publication._rename_directory_noreplace",
                side_effect=OSError("injected final rename failure"),
            ),
        }
        for phase, failure in cases.items():
            with self.subTest(phase=phase):
                core_root = Path(self.temporary.name) / ("failure-" + phase)
                cohort = _cohort()
                with failure:
                    with self.assertRaisesRegex(Exception, "injected"):
                        publish_route_cohort_bundle(cohort, core_root=core_root)
                final = core_root / "bundles" / cohort["route_cohort_id"]
                self.assertFalse(os.path.lexists(str(final)))
                self.assertFalse((core_root / "latest.json").exists())
                self.assertEqual(list((core_root / "bundles").iterdir()), [])

    def test_final_reread_failure_leaves_only_a_valid_unpointed_orphan(self):
        cohort = _cohort()
        original_validate = route_publication._validate_route_cohort_bundle

        def fail_final_reread(bundle_path, **kwargs):
            if kwargs.get("require_directory_identity"):
                raise ValueError("injected final reread failure")
            return original_validate(bundle_path, **kwargs)

        with patch(
            "scripts.route_publication._validate_route_cohort_bundle",
            side_effect=fail_final_reread,
        ):
            with self.assertRaisesRegex(ValueError, "injected final reread failure"):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        final = self.root / "bundles" / cohort["route_cohort_id"]
        self.assertFalse((self.root / "latest.json").exists())
        self.assertEqual(
            validate_route_cohort_bundle(final)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )

    def test_pointer_manifest_hash_tamper_is_rejected(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer["manifest_sha256"] = "f" * 64
        pointer_path.write_text(
            json.dumps(pointer, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "manifest hash"):
            load_latest_route_cohort(self.root)

    def test_symlink_roots_files_and_existing_broken_bundle_are_rejected(self):
        real = Path(self.temporary.name) / "real"
        real.mkdir()
        symlink_root = Path(self.temporary.name) / "core-link"
        symlink_root.symlink_to(real, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "real directory"):
            publish_route_cohort_bundle(_cohort(), core_root=symlink_root)

        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        manifest = bundle / "manifest.json"
        manifest_bytes = manifest.read_bytes()
        manifest.unlink()
        external = Path(self.temporary.name) / "external-manifest.json"
        external.write_bytes(manifest_bytes)
        manifest.symlink_to(external)
        with self.assertRaisesRegex(ValueError, "non-symlink"):
            validate_route_cohort_bundle(bundle)

        broken_root = Path(self.temporary.name) / "broken-core"
        bundles = broken_root / "bundles"
        bundles.mkdir(parents=True)
        broken = bundles / cohort["route_cohort_id"]
        broken.symlink_to(Path(self.temporary.name) / "missing")
        with self.assertRaisesRegex(ValueError, "already exists"):
            publish_route_cohort_bundle(cohort, core_root=broken_root)

    def test_symlinked_bundle_ancestor_is_rejected(self):
        real_parent = Path(self.temporary.name) / "real-parent"
        real_core = real_parent / "core"
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=real_core)
        linked_parent = Path(self.temporary.name) / "linked-parent"
        linked_parent.symlink_to(real_parent, target_is_directory=True)

        linked_bundle = (
            linked_parent / "core/bundles" / cohort["route_cohort_id"]
        )
        with self.assertRaisesRegex(ValueError, "symlink"):
            validate_route_cohort_bundle(linked_bundle)
        with self.assertRaisesRegex(ValueError, "symlink"):
            load_latest_route_cohort(linked_parent / "core")

    def test_extra_sqlite_index_is_rejected_even_when_manifest_hash_is_updated(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        database = bundle / "route_cohort.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            connection.execute(
                "CREATE INDEX attacker_extra_idx ON route_legs(status)"
            )
            connection.commit()
        finally:
            connection.close()
        _refresh_database_hash_in_manifest(bundle)

        with self.assertRaisesRegex(ValueError, "SQLite schema"):
            validate_route_cohort_bundle(bundle)

    def test_sqlite_exact_column_and_table_semantics_are_enforced(self):
        cases = {
            "type": lambda bundle: _rewrite_route_legs_schema(
                bundle,
                status_definition="status BLOB NOT NULL",
            ),
            "not-null": lambda bundle: _rewrite_route_legs_schema(
                bundle,
                status_definition="status TEXT",
            ),
            "primary-key": lambda bundle: _rewrite_route_legs_schema(
                bundle,
                market_definition="market_id TEXT NOT NULL",
                table_primary_key="PRIMARY KEY (market_id, leg_id)",
            ),
            "without-rowid": lambda bundle: _rewrite_route_legs_schema(
                bundle,
                without_rowid=False,
            ),
            "foreign-key": _rewrite_route_timing_without_foreign_key,
        }
        for label, mutate in cases.items():
            with self.subTest(label=label):
                core_root = Path(self.temporary.name) / ("schema-" + label)
                cohort = _cohort()
                publish_route_cohort_bundle(cohort, core_root=core_root)
                bundle = core_root / "bundles" / cohort["route_cohort_id"]
                mutate(bundle)
                _refresh_database_hash_in_manifest(bundle)

                with self.assertRaisesRegex(ValueError, "SQLite schema"):
                    validate_route_cohort_bundle(bundle)

    def test_sqlite_foreign_key_or_file_corruption_is_rejected(self):
        for corruption in ("foreign-key", "file"):
            with self.subTest(corruption=corruption):
                core_root = Path(self.temporary.name) / ("sqlite-" + corruption)
                cohort = _cohort()
                publish_route_cohort_bundle(cohort, core_root=core_root)
                bundle = core_root / "bundles" / cohort["route_cohort_id"]
                database = bundle / "route_cohort.sqlite3"
                if corruption == "foreign-key":
                    connection = sqlite3.connect(str(database))
                    try:
                        connection.execute(
                            "DELETE FROM route_candidates WHERE route_id = ?",
                            (cohort["routes"][0]["route_id"],),
                        )
                        connection.commit()
                    finally:
                        connection.close()
                    expected = "foreign keys"
                else:
                    value = bytearray(database.read_bytes())
                    value[:16] = b"not-a-sqlite-db!"
                    database.write_bytes(bytes(value))
                    expected = "SQLite"
                _refresh_database_hash_in_manifest(bundle)

                with self.assertRaisesRegex(ValueError, expected):
                    validate_route_cohort_bundle(bundle)

    def test_direct_sqlite_builder_cleans_up_logical_and_fsync_failures(self):
        failures = {
            "logical": patch(
                "scripts.route_publication._sqlite_candidate_values",
                side_effect=ValueError("injected SQLite logical failure"),
            ),
            "fsync": patch(
                "scripts.route_publication._fsync_file",
                side_effect=OSError("injected SQLite fsync failure"),
            ),
        }
        for phase, failure in failures.items():
            with self.subTest(phase=phase):
                database = Path(self.temporary.name) / (phase + ".sqlite3")
                with failure:
                    with self.assertRaisesRegex(Exception, "injected SQLite"):
                        build_route_cohort_sqlite(database, _cohort())
                self.assertFalse(os.path.lexists(str(database)))
                for suffix in ("-journal", "-wal", "-shm"):
                    self.assertFalse(os.path.lexists(str(database) + suffix))

    def test_pointer_path_traversal_is_rejected_without_reading_outside_bundle_root(self):
        bundles = self.root / "bundles"
        bundles.mkdir(parents=True)
        outside = self.root / "outside-marker"
        outside.write_text("do not read", encoding="utf-8")
        (self.root / "latest.json").write_text(json.dumps({
            "schema": "route_cohort_core_pointer/v1",
            "bundle_stage": "route_cohort_core/v1",
            "route_cohort_id": "../outside-marker",
            "manifest_sha256": "a" * 64,
        }), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "path-unsafe"):
            load_latest_route_cohort(self.root)

        self.assertEqual(outside.read_text(encoding="utf-8"), "do not read")

    def test_extra_or_missing_bundle_files_are_rejected_by_exact_allowlist(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        (bundle / "cost_components.csv").write_text("\n", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "file inventory"):
            validate_route_cohort_bundle(bundle)


def _shadow_json_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _shadow_pointer_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8") + b"\n"


class JointShadowPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.routes_root = Path(self.temporary.name) / "data/local/routes"
        self.core_root = self.routes_root / "core"
        self.shadow_root = self.routes_root / "shadow"
        self.run_id = "shadow-run-001"
        logical_paths = (
            "market_facts.sqlite3",
            "cex_instrument_lifecycle.json",
            "admin/token_registry.json",
            "cex_exchange_volume_daily.csv",
            "cex_depth_latest.csv",
            "dex_depth_latest.csv",
            "cex_execution_cost_latest.csv",
            "dex_execution_cost_latest.csv",
            "dex_pool_tvl_latest.csv",
            "config/tokens.csv",
            "config/token_chains.csv",
        )
        self.source_identities = [
            SourceFileIdentity(
                path,
                index + 1,
                hashlib.sha256(path.encode("utf-8")).hexdigest(),
            )
            for index, path in enumerate(logical_paths)
        ]
        self.generation = _candidate_source_generation(self.source_identities)
        self.cohort, self.core_pointer = self._publish_core(_cohort())
        self.audit = self._write_shadow_inputs(self.run_id, self.cohort)

    def _publish_core(self, cohort, *, typed_lineage=True):
        value = copy.deepcopy(cohort)
        value["candidate_source_generation"] = self.generation
        value["source_state"]["candidate_source_generation"] = self.generation
        value["selection_window"] = {
            "start": "2026-07-03",
            "end": "2026-08-01",
        }
        for collection in (value["routes"], value["route_rows"]):
            for row in collection:
                row["candidate_source_generation"] = self.generation
        if typed_lineage:
            manifest_members = []
            typed_payloads = {}
            for index, leg in enumerate(value["legs"], start=1):
                prefix = "shadow-leg{}".format(index)
                leg["typed_source_lineage"] = (
                    _cex_typed_source_lineage(prefix)
                    if leg["market_type"] == "cex"
                    else _dex_typed_source_lineage(prefix)
                )
                for member in leg["typed_source_lineage"]["members"]:
                    if member["status"] != "observed":
                        continue
                    payload = _shadow_json_bytes({
                        "fixture": "joint-shadow-typed-source/v1",
                        "market_id": leg["market_id"],
                        "role": member["role"],
                    })
                    member["sha256"] = hashlib.sha256(payload).hexdigest()
                    member["size"] = len(payload)
                    typed_payloads[member["filename"]] = payload
                    manifest_members.append({
                        "market_id": leg["market_id"],
                        **{
                            field: member[field]
                            for field in (
                                "role", "filename", "sha256", "size",
                                "logical_generation", "adapter_id",
                                "content_schema",
                            )
                        },
                    })
            manifest_members.sort(
                key=lambda row: (row["market_id"], row["role"])
            )
        value = _rehash(value)
        pointer = publish_route_cohort_bundle(value, core_root=self.core_root)
        if typed_lineage:
            raw_run = (
                self.routes_root.parent
                / "raw/route-cohort"
                / value["raw_evidence_run_id"]
            )
            typed_root = raw_run / "typed"
            typed_root.mkdir(parents=True)
            for filename, payload in typed_payloads.items():
                (typed_root / filename).write_bytes(payload)
            manifest = {
                "schema": "route_typed_source_manifest/v1",
                "raw_evidence_run_id": value["raw_evidence_run_id"],
                "member_count": len(manifest_members),
                "members": manifest_members,
            }
            (raw_run / "typed-manifest.json").write_bytes(
                _shadow_json_bytes(manifest)
            )
        return value, pointer

    def _universe(self, cohort):
        selected_legs = []
        for rank, leg in enumerate(cohort["legs"], start=1):
            selected_leg = {
                "market_id": leg["market_id"],
                "market_type": leg["market_type"],
                "token_symbol": leg["token_symbol"],
                "candidate_source_generation": self.generation,
                "selection_window": copy.deepcopy(cohort["selection_window"]),
                "selection_inputs": {
                    "execution_capability": "proved",
                    "proved_execution_capacity_usd": "100000",
                    "observed_100bps_depth_usd": "100000",
                    "cex_selected_window_usd": (
                        ("9000" if rank == 1 else "7000")
                        if leg["market_type"] == "cex" else None
                    ),
                    "dex_24h_usd": (
                        ("9000" if rank == 1 else "7000")
                        if leg["market_type"] == "dex" else None
                    ),
                    "dex_tvl_usd": (
                        "10000" if leg["market_type"] == "dex" else None
                    ),
                },
                "selection_rank": rank,
            }
            if "collector_context" in leg:
                context = copy.deepcopy(leg["collector_context"])
                selected_leg.update({
                    "collector_context": context,
                    "target_token_address": (
                        context["base_token_id"].split("_", 1)[1]
                        if context["status"] == "observed"
                        else "0x" + "1" * 40
                    ),
                    "target_token_side": (
                        "base" if context["status"] == "observed" else None
                    ),
                })
            selected_legs.append(selected_leg)
        return {
            "schema": "route_universe/v1",
            "candidate_source_generation": self.generation,
            "selection_window": copy.deepcopy(cohort["selection_window"]),
            "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
            "selected_legs": selected_legs,
            "routes": copy.deepcopy(cohort["routes"]),
        }

    def test_route_universe_accepts_chain_native_dex_target_identities(self):
        cases = (
            (
                "starknet",
                "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
                "0x" + "2" * 64,
            ),
            (
                "solana",
                "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
                "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
            ),
        )
        for chain, target, other in cases:
            target = normalize_contract_address(chain, target)
            other = normalize_contract_address(chain, other)
            context = {
                "schema": "route_collector_context/v1",
                "snapshot_id": "tvl-snapshot-1",
                "request_started_at": "2026-08-01T11:59:58+00:00",
                "observed_at": "2026-08-01T11:59:59+00:00",
                "response_received_at": "2026-08-01T12:00:00+00:00",
                "status": "observed",
                "reason_code": "observed",
                "pool_name": "TOKEN / USD",
                "base_token_id": chain + "_" + target,
                "quote_token_id": chain + "_" + other,
                "base_token_price_usd": "7",
                "quote_token_price_usd": "1",
                "tvl_method": "reserve_value",
                "source": "geckoterminal",
                "source_endpoint": "https://api.example.test/pools",
                "raw_response_sha256": "d" * 64,
            }
            candidate = {
                "schema": "route_universe/v1",
                "candidate_source_generation": self.generation,
                "selection_window": {
                    "start": "2026-07-03", "end": "2026-08-01",
                },
                "requested_notionals_usd": [
                    1000, 5000, 10000, 50000, 100000,
                ],
                "selected_legs": [{
                    "market_id": "dex:{}:unsupported:pool:UNI".format(chain),
                    "market_type": "dex",
                    "token_symbol": "UNI",
                    "candidate_source_generation": self.generation,
                    "selection_window": {
                        "start": "2026-07-03", "end": "2026-08-01",
                    },
                    "selection_inputs": {
                        "execution_capability": "proved",
                        "proved_execution_capacity_usd": "100000",
                        "observed_100bps_depth_usd": "100000",
                        "cex_selected_window_usd": None,
                        "dex_24h_usd": "9000",
                        "dex_tvl_usd": "10000",
                    },
                    "selection_rank": 1,
                    "collector_context": context,
                    "target_token_address": target,
                    "target_token_side": "base",
                }],
                "routes": [],
            }
            with self.subTest(chain=chain):
                validated = route_publication._validate_route_universe_payload(
                    candidate
                )
                self.assertEqual(
                    next(
                        item for item in validated["selected_legs"]
                        if item["market_type"] == "dex"
                    )["target_token_address"],
                    normalize_contract_address(chain, target),
                )

    def test_route_universe_rejects_wrong_chain_address_or_context_matrix(self):
        context = {
            "schema": "route_collector_context/v1",
            "snapshot_id": "tvl-snapshot-1",
            "request_started_at": "2026-08-01T11:59:58+00:00",
            "observed_at": "2026-08-01T11:59:59+00:00",
            "response_received_at": "2026-08-01T12:00:00+00:00",
            "status": "observed",
            "reason_code": "observed",
            "pool_name": "UNI / USDC",
            "base_token_id": "eth_0x" + "1" * 40,
            "quote_token_id": "eth_0x" + "2" * 40,
            "base_token_price_usd": "7",
            "quote_token_price_usd": "1",
            "tvl_method": "reserve_value",
            "source": "geckoterminal",
            "source_endpoint": "https://api.example.test/pools",
            "raw_response_sha256": "d" * 64,
        }
        universe = {
            "schema": "route_universe/v1",
            "candidate_source_generation": self.generation,
            "selection_window": {
                "start": "2026-07-03", "end": "2026-08-01",
            },
            "requested_notionals_usd": [
                1000, 5000, 10000, 50000, 100000,
            ],
            "selected_legs": [{
                "market_id": "dex:eth:uniswap_v2:pool:UNI",
                "market_type": "dex",
                "token_symbol": "UNI",
                "candidate_source_generation": self.generation,
                "selection_window": {
                    "start": "2026-07-03", "end": "2026-08-01",
                },
                "selection_inputs": {
                    "execution_capability": "proved",
                    "proved_execution_capacity_usd": "100000",
                    "observed_100bps_depth_usd": "100000",
                    "cex_selected_window_usd": None,
                    "dex_24h_usd": "9000",
                    "dex_tvl_usd": "10000",
                },
                "selection_rank": 1,
                "collector_context": context,
                "target_token_address": "0x" + "1" * 40,
                "target_token_side": "base",
            }],
            "routes": [],
        }
        cases = []
        wrong_chain = copy.deepcopy(universe)
        wrong = next(
            row for row in wrong_chain["selected_legs"]
            if row["market_type"] == "dex"
        )
        wrong_id = wrong["market_id"]
        wrong["market_id"] = wrong_id.replace("dex:eth:", "dex:solana:", 1)
        wrong["collector_context"]["base_token_id"] = (
            "solana_" + wrong["target_token_address"]
        )
        wrong["collector_context"]["quote_token_id"] = "solana_" + "1" * 32
        cases.append(("wrong-chain-address", wrong_chain))
        stale_nonobserved = copy.deepcopy(universe)
        stale = next(
            row for row in stale_nonobserved["selected_legs"]
            if row["market_type"] == "dex"
        )
        stale["collector_context"].update({
            "status": "missing",
            "reason_code": "source_no_tvl_observation",
            "quote_token_id": None,
            "base_token_price_usd": None,
            "quote_token_price_usd": None,
        })
        stale["target_token_side"] = None
        cases.append(("stale-nonobserved-id", stale_nonobserved))
        missing_price = copy.deepcopy(universe)
        observed = next(
            row for row in missing_price["selected_legs"]
            if row["market_type"] == "dex"
        )
        observed["collector_context"]["base_token_price_usd"] = None
        cases.append(("observed-price-null", missing_price))
        for label, candidate in cases:
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    route_publication.RoutePublicationError,
                    "DEX target identity|collector|context|Token|price",
                ):
                    route_publication._validate_route_universe_payload(candidate)

    def _write_shadow_inputs(
        self, run_id, cohort, *, phase="canary", core_pointer=None
    ):
        run_directory = self.shadow_root / "runs" / run_id
        run_directory.mkdir(parents=True)
        universe = self._universe(cohort)
        universe_bytes = _shadow_json_bytes(universe)
        universe_sha256 = route_universe_sha256(universe)
        baseline = {
            "schema": "route_shadow_baseline_manifest/v1",
            "calculation_version": "route_shadow_inputs/v1",
            "candidate_source_generation": self.generation,
            "selection_window": copy.deepcopy(cohort["selection_window"]),
            "filters": {
                "window_days": 30,
                "calendar": "complete_utc_days",
                "cex_volume_aggregation": "sum_quote_volume_usd",
                "maximum_legs_per_token_market_type": 3,
            },
            "observation_bounds": {
                "start_inclusive": "2026-07-03T00:00:00Z",
                "end_exclusive": "2026-08-02T00:00:00Z",
            },
            "inputs": [
                {"path": row.path, "size": row.size, "sha256": row.sha256}
                for row in self.source_identities
            ],
            "route_universe_sha256": universe_sha256,
        }
        baseline_bytes = _shadow_json_bytes(baseline)

        def typed_sha(domain, value):
            return hashlib.sha256(
                domain + _shadow_json_bytes(value) + b"\n"
            ).hexdigest()

        adapter_registry = {
            "schema": "route_cost_adapter_registry/v1",
            "registry_version": "test-v1",
            "adapters": [],
        }
        adapter_registry_sha256 = hashlib.sha256(
            _shadow_json_bytes(adapter_registry)
        ).hexdigest()
        connector_key_registry = {
            "schema": "route_cost_connector_key_registry/v1",
            "registry_version": "test-v1",
            "keys": [],
        }
        connector_key_registry_sha256 = hashlib.sha256(
            _shadow_json_bytes(connector_key_registry)
        ).hexdigest()
        trace_identity = {
            "schema": "route_cost_trace_profile_identity/v1",
            "status": "missing",
            "profile_id": None,
            "endpoint_id": None,
        }
        trace_profile_generation = typed_sha(
            b"route-cost-trace-profile-identity/v1\n", trace_identity
        )
        connector_identity = {
            "schema": "route_cost_submission_connector_identity/v1",
            "status": "missing",
            "profile_id": None,
            "connector_id": None,
        }
        submission_connector_profile_generation = typed_sha(
            b"route-cost-submission-connector-identity/v1\n",
            connector_identity,
        )
        selected_markets = []
        for leg in sorted(
            (
                row for row in universe["selected_legs"]
                if row["market_type"] == "dex"
            ),
            key=lambda row: row["market_id"],
        ):
            route_volumes = [
                Decimal(route["route_volume_usd"])
                for route in universe["routes"]
                if leg["market_id"] in {
                    route["buy_market_id"], route["sell_market_id"]
                }
                and route.get("route_volume_usd") is not None
            ]
            selected_markets.append({
                "market_id": leg["market_id"],
                "token_rank": 1,
                "selection_rank": leg["selection_rank"],
                "best_route_volume_usd": (
                    format(max(route_volumes), "f") if route_volumes else None
                ),
                "dex_24h_usd": leg["selection_inputs"]["dex_24h_usd"],
                "dex_tvl_usd": leg["selection_inputs"]["dex_tvl_usd"],
                "adapter_id": "uniswap-v2-router02-ethereum",
                "structural_support_status": "unsupported",
                "structural_reason": "strict_cost_adapter_unsupported",
            })
        transcripts = [
            {
                "market_id": market["market_id"],
                "direction": direction,
                "requested_notional_usd": str(notional),
                "status": "unavailable",
                "reason_code": "strict_cost_adapter_unsupported",
            }
            for market in selected_markets
            for direction in ("buy", "sell")
            for notional in universe["requested_notionals_usd"]
        ]
        selected_market_set_sha256 = hashlib.sha256(_shadow_json_bytes({
            "schema": "route_cost_selected_markets/v1",
            "members": selected_markets,
        })).hexdigest()
        empty_member_set_sha256 = typed_sha(
            b"route-cost-submission-policy-member-set/v1\n", []
        )
        submission_policy_snapshot = {
            "schema": "route_cost_submission_policy_snapshot/v1",
            "run_id": run_id,
            "route_cohort_id": cohort["route_cohort_id"],
            "candidate_source_generation": self.generation,
            "route_universe_sha256": universe_sha256,
            "adapter_registry_sha256": adapter_registry_sha256,
            "selected_market_set_sha256": selected_market_set_sha256,
            "connector_key_registry_sha256": connector_key_registry_sha256,
            "trace_profile_generation": trace_profile_generation,
            "submission_connector_profile_generation": (
                submission_connector_profile_generation
            ),
            "connector_id": None,
            "member_count": 0,
            "members": [],
            "member_set_sha256": empty_member_set_sha256,
            "status": "not_applicable",
            "reason_code": "scope_empty",
            "observed_at": None,
            "valid_until": None,
            "issuer_key_id": None,
            "signature_algorithm": None,
            "attested_payload_sha256": None,
            "signature": None,
        }
        cost_evidence = {
            "schema": "route_cost_evidence_manifest/v1",
            "run_id": run_id,
            "route_cohort_id": cohort["route_cohort_id"],
            "phase": phase,
            "candidate_source_generation": self.generation,
            "route_universe_sha256": universe_sha256,
            "adapter_registry": adapter_registry,
            "adapter_registry_sha256": adapter_registry_sha256,
            "connector_key_registry": connector_key_registry,
            "connector_key_registry_sha256": connector_key_registry_sha256,
            "transcript_count": len(transcripts),
            "trace_profile_generation": trace_profile_generation,
            "submission_connector_profile_generation": (
                submission_connector_profile_generation
            ),
            "evaluated_at": "2026-08-01T12:00:03Z",
            "selected_market_count": len(selected_markets),
            "selected_markets": selected_markets,
            "selected_market_set_sha256": selected_market_set_sha256,
            "native_price_evidence": None,
            "native_price_evidence_sha256": None,
            "chain_evidence_count": 0,
            "chain_evidence": [],
            "chain_evidence_set_sha256": typed_sha(
                b"route-cost-chain-evidence-set/v1\n", []
            ),
            "market_evidence_count": 0,
            "market_evidence": [],
            "market_evidence_set_sha256": typed_sha(
                b"route-cost-market-evidence-set/v1\n", []
            ),
            "transcripts": transcripts,
            "transcript_set_sha256": typed_sha(
                b"route-cost-evidence-transcript-set/v1\n", transcripts
            ),
            "binding_count": 0,
            "bindings": [],
            "binding_set_sha256": typed_sha(
                b"route-cost-evidence-binding-set/v1\n", []
            ),
            "submission_policy_snapshot": submission_policy_snapshot,
            "submission_policy_snapshot_sha256": typed_sha(
                b"route-cost-submission-policy-snapshot/v1\n",
                submission_policy_snapshot,
            ),
            "counts": {
                "transcript_observed": 0,
                "transcript_unavailable": len(transcripts),
                "transcript_failed": 0,
                "binding_observed": 0,
                "binding_unavailable": 0,
                "binding_failed": 0,
            },
        }
        cost_evidence = build_unavailable_route_cost_evidence_manifest(
            universe=universe,
            run_id=run_id,
            route_cohort_id=cohort["route_cohort_id"],
            phase=phase,
            candidate_source_generation=self.generation,
            route_universe_sha256=universe_sha256,
            evaluated_at="2026-08-01T12:00:03Z",
        )
        cost_bytes = _shadow_json_bytes(cost_evidence)
        (run_directory / "route_universe.json").write_bytes(universe_bytes)
        (run_directory / "baseline_manifest.json").write_bytes(baseline_bytes)
        (run_directory / "route-cost-evidence.json").write_bytes(cost_bytes)
        bound_core_pointer = self.core_pointer if core_pointer is None else core_pointer
        core_pointer_sha256 = hashlib.sha256(
            _shadow_pointer_bytes(bound_core_pointer)
        ).hexdigest()
        phase_state_sha256 = hashlib.sha256(
            b"route-shadow-phase/implicit-canary/v1\n"
        ).hexdigest()
        return {
            "schema": "route_shadow_audit/v1",
            "run_id": run_id,
            "phase": phase,
            "route_cohort_id": cohort["route_cohort_id"],
            "phase_state_sha256": phase_state_sha256,
            "phase_transition_id": None,
            "core_pointer_sha256": core_pointer_sha256,
            "core_manifest_sha256": bound_core_pointer["manifest_sha256"],
            "route_cost_evidence_sha256": hashlib.sha256(cost_bytes).hexdigest(),
            "route_universe_sha256": universe_sha256,
            "baseline_manifest_sha256": hashlib.sha256(baseline_bytes).hexdigest(),
            "candidate_source_generation": self.generation,
            "audit_finished_at": "2026-08-01T12:00:04Z",
            "metrics": {
                "leg_availability": {
                    "status": "evaluated", "numerator": 2,
                    "denominator": 2, "value": "1",
                },
                "timing_availability": {
                    "status": "evaluated", "numerator": 2,
                    "denominator": 2, "value": "1",
                },
                "conditional_skew_sla": {
                    "status": "evaluated", "numerator": 2,
                    "denominator": 2, "value": "1",
                },
                "passing_skew_seconds_p95": {
                    "status": "evaluated", "sample_count": 2, "value": "1",
                },
                "passing_skew_seconds_max": {
                    "status": "evaluated", "sample_count": 2, "value": "1",
                },
                "route_age_seconds_p95": {
                    "status": "evaluated", "sample_count": 2, "value": "3",
                },
                "route_age_seconds_max": {
                    "status": "evaluated", "sample_count": 2, "value": "3",
                },
            },
        }

    def _publish(self):
        return publish_shadow_result(
            self.shadow_root,
            core_pointer=self.core_pointer,
            audit=self.audit,
        )

    def test_legacy_core_is_readable_but_cannot_become_shadow_candidate_ready(self):
        legacy, legacy_pointer = self._publish_core(
            _second_cohort(), typed_lineage=False
        )
        loaded = load_latest_route_cohort(self.core_root)
        self.assertEqual(
            loaded["cohort"]["route_cohort_id"], legacy["route_cohort_id"]
        )
        self.assertTrue(all(
            "typed_source_lineage" not in leg for leg in loaded["legs"]
        ))
        legacy_audit = self._write_shadow_inputs(
            "legacy-shadow", legacy, core_pointer=legacy_pointer
        )
        with self.assertRaisesRegex(ValueError, "typed-source lineage is missing"):
            publish_shadow_result(
                self.shadow_root,
                core_pointer=legacy_pointer,
                audit=legacy_audit,
            )
        self.assertFalse((self.shadow_root / "latest.json").exists())

    def test_typed_source_byte_cap_is_per_leg_not_global_cohort(self):
        raw_run_id = "snapshot-per-leg-cap"
        raw_run = (
            self.routes_root.parent / "raw/route-cohort" / raw_run_id
        )
        typed_root = raw_run / "typed"
        typed_root.mkdir(parents=True)
        payload = b"x" * (8 * 1024 * 1024)
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        manifest_members = []
        legs = []
        for index in range(4):
            market_id = "cex:venue{}:UNI/USDT".format(index)
            filename = "leg{}-raw-book.json".format(index)
            (typed_root / filename).write_bytes(payload)
            lineage = _cex_typed_source_lineage("leg{}".format(index))
            for member in lineage["members"]:
                if member["role"] == "cex_raw_book_response":
                    member.update({
                        "filename": filename,
                        "sha256": payload_sha256,
                        "size": len(payload),
                    })
                else:
                    member.update({
                        "status": "unavailable",
                        "reason_code": "typed_source_missing",
                        "filename": None,
                        "sha256": None,
                        "size": None,
                        "logical_generation": None,
                    })
                if member["status"] == "observed":
                    manifest_members.append({
                        "market_id": market_id,
                        **{
                            field: member[field]
                            for field in (
                                "role", "filename", "sha256", "size",
                                "logical_generation", "adapter_id",
                                "content_schema",
                            )
                        },
                    })
            legs.append({
                "market_id": market_id,
                "market_type": "cex",
                "typed_source_lineage": lineage,
            })
        manifest_members.sort(key=lambda row: (row["market_id"], row["role"]))
        (raw_run / "typed-manifest.json").write_bytes(_shadow_json_bytes({
            "schema": "route_typed_source_manifest/v1",
            "raw_evidence_run_id": raw_run_id,
            "member_count": len(manifest_members),
            "members": manifest_members,
        }))

        retained = route_publication._load_retained_typed_source_members(
            self.shadow_root,
            {"cohort": {"raw_evidence_run_id": raw_run_id}, "legs": legs},
        )

        self.assertEqual(retained, {})

    def _install_full_phase(self):
        gates = self.shadow_root / "gates"
        transitions = self.shadow_root / "transitions"
        gates.mkdir()
        transitions.mkdir()
        gate_bytes = _shadow_json_bytes({
            "schema": "route_shadow_gate/v1",
            "phase": "canary",
            "blocking_reasons": [],
        })
        gate_sha256 = hashlib.sha256(gate_bytes).hexdigest()
        (gates / (gate_sha256 + ".json")).write_bytes(gate_bytes)
        fields = {
            "schema": "route_shadow_phase/v1",
            "prior_phase": "canary",
            "phase": "full",
            "evaluated_at": "2026-08-02T00:00:00Z",
            "gate_evidence_sha256": gate_sha256,
            "storage_admission_sha256": "1" * 64,
            "anchored_joint_pointer_sha256": "2" * 64,
            "primary_schedule_guard_sha256": "3" * 64,
            "schedule_envelope_sha256": None,
            "phase_identity_id": "4" * 64,
        }
        transition_id = hashlib.sha256(_shadow_json_bytes(fields)).hexdigest()
        state = {**fields, "transition_id": transition_id}
        state_bytes = _shadow_json_bytes(state)
        (transitions / (transition_id + ".json")).write_bytes(state_bytes)
        (self.shadow_root / "phase.json").write_bytes(state_bytes)
        return state, state_bytes

    def _dex_context_fixture(self, status="observed"):
        first = "0x" + "11" * 20
        second = "0x" + "22" * 20
        observed = status == "observed"
        reason_by_status = {
            "observed": "observed",
            "missing": "source_no_tvl_observation",
            "not_found": "source_pool_not_found",
            "failed": "source_unavailable",
        }
        context = {
            "schema": "route_collector_context/v1",
            "snapshot_id": "tvl-snapshot-1",
            "request_started_at": "2026-08-01T11:59:58+00:00",
            "observed_at": "2026-08-01T11:59:59+00:00",
            "response_received_at": "2026-08-01T12:00:00+00:00",
            "status": status,
            "reason_code": reason_by_status[status],
            "pool_name": "UNI / USDC",
            "base_token_id": "eth_" + first if observed else None,
            "quote_token_id": "eth_" + second if observed else None,
            "base_token_price_usd": "7" if observed else None,
            "quote_token_price_usd": "1" if observed else None,
            "tvl_method": "reserve_value",
            "source": "geckoterminal",
            "source_endpoint": "https://api.example.test/pools",
            "raw_response_sha256": "d" * 64,
        }
        universe = {
            "selected_legs": [{
                "market_id": "dex:eth:uniswap_v2:0x{}:UNI".format("a" * 40),
                "collector_context": context,
            }],
        }
        core_leg = {
            "market_id": "dex:eth:uniswap_v2:0x{}:UNI".format("a" * 40),
            "market_type": "dex",
            "collector_context": copy.deepcopy(context),
            "available": True if observed else False,
            "reason_code": None if observed else "usd_price_context_" + status,
            "usd_price_source_snapshot_id": context["snapshot_id"],
            "usd_price_observed_at": context["observed_at"],
            "usd_price_source": context["source"],
            "usd_price_source_endpoint": context["source_endpoint"],
            "usd_price_raw_response_sha256": context["raw_response_sha256"],
            "token0_address": second,
            "token1_address": first,
            "token0_price_usd": "1" if observed else None,
            "token1_price_usd": "7" if observed else None,
        }
        return universe, core_leg

    def test_publish_and_all_loaders_return_only_the_exact_joint_view(self):
        published = self._publish()

        self.assertEqual(set(published), {
            "pointer", "pointer_sha256", "audit", "audit_sha256",
            "cohort", "manifest",
        })
        self.assertEqual(set(published["pointer"]), {
            "schema", "run_id", "phase", "route_cohort_id",
            "phase_state_sha256", "phase_transition_id",
            "core_pointer_sha256", "core_manifest_sha256",
            "route_universe_sha256", "route_cost_evidence_sha256",
            "baseline_manifest_sha256", "candidate_source_generation",
            "audit_sha256",
        })
        pointer_bytes = (self.shadow_root / "latest.json").read_bytes()
        self.assertEqual(pointer_bytes, _shadow_pointer_bytes(published["pointer"]))
        self.assertEqual(
            published["pointer_sha256"], hashlib.sha256(pointer_bytes).hexdigest()
        )
        self.assertEqual(
            load_shadow_result(
                self.shadow_root,
                run_id=self.run_id,
                expected_pointer_sha256=published["pointer_sha256"],
            ),
            published,
        )
        self.assertEqual(load_latest_shadow_result(self.shadow_root), published)

    def _assert_raw_typed_failure_preserves_public_latest(self):
        public_pointer = self.routes_root / "latest.json"
        public_pointer.write_bytes(b"public-route-sentinel\n")

        with self.assertRaisesRegex(ValueError, "typed-source|typed source"):
            self._publish()

        self.assertEqual(
            public_pointer.read_bytes(), b"public-route-sentinel\n"
        )
        self.assertFalse((self.shadow_root / "latest.json").exists())

    def test_publish_requires_raw_typed_run_root(self):
        raw_run = (
            self.routes_root.parent
            / "raw/route-cohort"
            / self.cohort["raw_evidence_run_id"]
        )
        shutil.rmtree(raw_run)
        self._assert_raw_typed_failure_preserves_public_latest()

    def test_publish_requires_raw_typed_manifest(self):
        manifest = (
            self.routes_root.parent
            / "raw/route-cohort"
            / self.cohort["raw_evidence_run_id"]
            / "typed-manifest.json"
        )
        manifest.unlink()
        self._assert_raw_typed_failure_preserves_public_latest()

    def test_publish_requires_every_raw_typed_payload(self):
        typed_root = (
            self.routes_root.parent
            / "raw/route-cohort"
            / self.cohort["raw_evidence_run_id"]
            / "typed"
        )
        next(typed_root.iterdir()).unlink()
        self._assert_raw_typed_failure_preserves_public_latest()

    def test_historical_load_replays_raw_typed_manifest_and_payloads(self):
        published = self._publish()
        raw_run = (
            self.routes_root.parent
            / "raw/route-cohort"
            / self.cohort["raw_evidence_run_id"]
        )
        (raw_run / "typed-manifest.json").unlink()
        with self.assertRaisesRegex(ValueError, "typed-source|typed source"):
            load_shadow_result(
                self.shadow_root,
                run_id=self.run_id,
                expected_pointer_sha256=published["pointer_sha256"],
            )
        with self.assertRaisesRegex(ValueError, "typed-source|typed source"):
            load_latest_shadow_result(self.shadow_root)

        manifest = {
            "schema": "route_typed_source_manifest/v1",
            "raw_evidence_run_id": self.cohort["raw_evidence_run_id"],
            "member_count": 0,
            "members": [],
        }
        (raw_run / "typed-manifest.json").write_bytes(
            _shadow_json_bytes(manifest)
        )
        with self.assertRaisesRegex(ValueError, "typed-source|typed source"):
            load_latest_shadow_result(self.shadow_root)

    def test_shadow_hash_and_cohort_fields_reject_string_coercion(self):
        pointer = copy.deepcopy(self._publish()["pointer"])
        for field in (
            "phase_state_sha256",
            "core_pointer_sha256",
            "core_manifest_sha256",
            "route_universe_sha256",
            "route_cost_evidence_sha256",
            "baseline_manifest_sha256",
            "candidate_source_generation",
            "audit_sha256",
        ):
            with self.subTest(field=field):
                invalid = copy.deepcopy(pointer)
                invalid[field] = int("1" * 64)
                with self.assertRaisesRegex(ValueError, field):
                    route_publication._validate_shadow_pointer(invalid)

        core_pointer = copy.deepcopy(self.core_pointer)
        core_pointer["manifest_sha256"] = int("1" * 64)
        with self.assertRaisesRegex(ValueError, "core pointer"):
            route_publication._validate_core_pointer_mapping(core_pointer)

    def test_historical_canary_remains_readable_after_full_transition(self):
        published = self._publish()
        state, state_bytes = self._install_full_phase()

        active = load_active_phase_state(self.shadow_root)
        self.assertEqual(active, {
            "phase": "full",
            "phase_state_sha256": hashlib.sha256(state_bytes).hexdigest(),
            "phase_transition_id": state["transition_id"],
            "state": state,
        })
        historical = load_historical_phase_state(
            self.shadow_root,
            phase="canary",
            phase_state_sha256=published["pointer"]["phase_state_sha256"],
            phase_transition_id=None,
        )
        self.assertEqual(historical["phase"], "canary")
        self.assertIsNone(historical["state"])
        self.assertEqual(load_latest_shadow_result(self.shadow_root), published)

    def test_full_phase_rejects_noncanonical_state_and_mutated_gate(self):
        state, _state_bytes = self._install_full_phase()
        (self.shadow_root / "phase.json").write_bytes(
            json.dumps(state, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        )
        with self.assertRaisesRegex(ValueError, "canonical|phase-state"):
            load_active_phase_state(self.shadow_root)

    def test_full_phase_gate_hash_is_replayed_from_immutable_bytes(self):
        state, state_bytes = self._install_full_phase()
        gate_path = self.shadow_root / "gates" / (
            state["gate_evidence_sha256"] + ".json"
        )
        gate_path.write_bytes(_shadow_json_bytes({
            "schema": "route_shadow_gate/v1",
            "phase": "canary",
            "blocking_reasons": ["changed"],
        }))

        with self.assertRaisesRegex(ValueError, "gate.*hash"):
            load_historical_phase_state(
                self.shadow_root,
                phase="full",
                phase_state_sha256=hashlib.sha256(state_bytes).hexdigest(),
                phase_transition_id=state["transition_id"],
            )

    def test_full_phase_transition_replacement_during_gate_read_fails(self):
        state, state_bytes = self._install_full_phase()
        transition_path = self.shadow_root / "transitions" / (
            state["transition_id"] + ".json"
        )
        real_read = route_publication._read_canonical_object_at
        replaced = {"done": False}

        def replace_transition_after_gate(directory_fd, filename, **kwargs):
            result = real_read(directory_fd, filename, **kwargs)
            if kwargs.get("label") == "route shadow gate evidence" and not replaced[
                "done"
            ]:
                temporary = transition_path.with_name(".transition-replacement")
                temporary.write_bytes(state_bytes)
                os.replace(temporary, transition_path)
                replaced["done"] = True
            return result

        with patch.object(
            route_publication,
            "_read_canonical_object_at",
            side_effect=replace_transition_after_gate,
        ):
            with self.assertRaisesRegex(ValueError, "transition changed"):
                load_historical_phase_state(
                    self.shadow_root,
                    phase="full",
                    phase_state_sha256=hashlib.sha256(state_bytes).hexdigest(),
                    phase_transition_id=state["transition_id"],
                )

    def test_active_phase_absence_is_rechecked_before_return(self):
        original = route_publication._optional_regular_snapshot_at
        calls = {"phase": 0}

        def appear_after_absent(directory_fd, filename, **kwargs):
            result = original(directory_fd, filename, **kwargs)
            if filename == "phase.json":
                calls["phase"] += 1
                if calls["phase"] == 1 and result is None:
                    (self.shadow_root / "phase.json").write_bytes(b"{}")
            return result

        with patch.object(
            route_publication,
            "_optional_regular_snapshot_at",
            side_effect=appear_after_absent,
        ):
            with self.assertRaisesRegex(ValueError, "phase changed"):
                load_active_phase_state(self.shadow_root)

    def test_phase_absence_aba_before_commit_fails_closed(self):
        real_install = route_publication._install_immutable_audit_at

        def install_then_appear_and_delete(run_fd, run_path, audit_bytes):
            real_install(run_fd, run_path, audit_bytes)
            self._install_full_phase()
            (self.shadow_root / "phase.json").unlink()

        with patch.object(
            route_publication,
            "_install_immutable_audit_at",
            side_effect=install_then_appear_and_delete,
        ):
            with self.assertRaisesRegex(ValueError, "phase|shadow root.*changed"):
                self._publish()

        self.assertFalse((self.shadow_root / "latest.json").exists())

    def test_dex_context_replays_unordered_address_price_map(self):
        universe, core_leg = self._dex_context_fixture()

        route_publication._validate_dex_collector_contexts(
            universe, [core_leg]
        )

        core_leg["token0_price_usd"] = "2"
        with self.assertRaisesRegex(ValueError, "address-price map"):
            route_publication._validate_dex_collector_contexts(
                universe, [core_leg]
            )

    def test_dex_context_preserves_the_fact_collectors_utc_representation(self):
        universe, core_leg = self._dex_context_fixture()
        universe["selected_legs"][0]["collector_context"][
            "request_started_at"
        ] = "2026-08-01T11:59:58Z"
        core_leg["collector_context"][
            "request_started_at"
        ] = "2026-08-01T11:59:58Z"

        with self.assertRaisesRegex(ValueError, "collector.*canonical UTC"):
            route_publication._validate_dex_collector_contexts(
                universe, [core_leg]
            )

    def test_observed_dex_context_is_bound_through_joint_publication(self):
        dex_cohort = _dex_cohort()
        dex_cohort["raw_evidence_run_id"] = "snapshot-dex"
        for leg in dex_cohort["legs"]:
            leg["snapshot_id"] = "snapshot-dex"
        dex_cohort = _rehash(dex_cohort)
        _universe, context_leg = self._dex_context_fixture()
        for leg in dex_cohort["legs"]:
            context = copy.deepcopy(context_leg["collector_context"])
            leg.update({
                "collector_context": context,
                "usd_price_source_snapshot_id": context["snapshot_id"],
                "usd_price_observed_at": context["observed_at"],
                "usd_price_source": context["source"],
                "usd_price_source_endpoint": context["source_endpoint"],
                "usd_price_raw_response_sha256": context[
                    "raw_response_sha256"
                ],
                "token0_address": context_leg["token0_address"],
                "token1_address": context_leg["token1_address"],
                "token0_price_usd": context_leg["token0_price_usd"],
                "token1_price_usd": context_leg["token1_price_usd"],
            })
        dex_cohort = _rehash(dex_cohort)
        self.cohort, self.core_pointer = self._publish_core(dex_cohort)
        self.run_id = "shadow-run-dex"
        self.audit = self._write_shadow_inputs(self.run_id, self.cohort)

        published = self._publish()

        self.assertEqual(
            published["cohort"]["route_cohort_id"],
            self.cohort["route_cohort_id"],
        )

    def test_dex_context_exact_schema_token_ids_and_unavailable_matrix(self):
        for status in ("missing", "not_found", "failed"):
            with self.subTest(status=status):
                universe, core_leg = self._dex_context_fixture(status)
                route_publication._validate_dex_collector_contexts(
                    universe, [core_leg]
                )
                universe["selected_legs"][0]["collector_context"][
                    "base_token_price_usd"
                ] = "7"
                core_leg["collector_context"]["base_token_price_usd"] = "7"
                with self.assertRaisesRegex(ValueError, "unavailable"):
                    route_publication._validate_dex_collector_contexts(
                        universe, [core_leg]
                    )

        universe, core_leg = self._dex_context_fixture()
        context = universe["selected_legs"][0]["collector_context"]
        context["quote_token_id"] = context["base_token_id"]
        core_leg["collector_context"]["quote_token_id"] = context[
            "base_token_id"
        ]
        with self.assertRaisesRegex(ValueError, "distinct"):
            route_publication._validate_dex_collector_contexts(
                universe, [core_leg]
            )

        universe, core_leg = self._dex_context_fixture()
        universe["selected_legs"][0]["collector_context"]["extra"] = True
        core_leg["collector_context"]["extra"] = True
        with self.assertRaisesRegex(ValueError, "context (?:lineage|schema)"):
            route_publication._validate_dex_collector_contexts(
                universe, [core_leg]
            )

        universe, core_leg = self._dex_context_fixture("failed")
        universe["selected_legs"][0]["collector_context"][
            "reason_code"
        ] = "made_up_failure"
        core_leg["collector_context"]["reason_code"] = "made_up_failure"
        with self.assertRaisesRegex(ValueError, "unavailable"):
            route_publication._validate_dex_collector_contexts(
                universe, [core_leg]
            )

    def test_observed_non_evm_price_context_allows_terminal_research_leg(self):
        market_id = (
            "dex:solana:orca:"
            "9WwG7yYCr7HiGJLnoD2joxJdFWFzrY1h7i5AdbWwtuCR:UNI"
        )
        target = normalize_contract_address(
            "solana", "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"
        )
        other = normalize_contract_address(
            "solana", "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"
        )
        context = {
            "schema": "route_collector_context/v1",
            "snapshot_id": "tvl-snapshot-1",
            "request_started_at": "2026-08-01T11:59:58+00:00",
            "observed_at": "2026-08-01T11:59:59+00:00",
            "response_received_at": "2026-08-01T12:00:00+00:00",
            "status": "observed",
            "reason_code": "observed",
            "pool_name": "UNI / USDC",
            "base_token_id": "solana_" + target,
            "quote_token_id": "solana_" + other,
            "base_token_price_usd": "7",
            "quote_token_price_usd": "1",
            "tvl_method": "reserve_value",
            "source": "geckoterminal",
            "source_endpoint": "https://api.example.test/pools",
            "raw_response_sha256": "d" * 64,
        }
        universe = {"selected_legs": [{
            "market_id": market_id,
            "collector_context": context,
        }]}
        core_leg = {
            "market_id": market_id,
            "market_type": "dex",
            "status": "unsupported",
            "available": False,
            "reason_code": "unsupported_chain",
            "collector_context": copy.deepcopy(context),
            "usd_price_source_snapshot_id": context["snapshot_id"],
            "usd_price_observed_at": context["observed_at"],
            "usd_price_source": context["source"],
            "usd_price_source_endpoint": context["source_endpoint"],
            "usd_price_raw_response_sha256": context["raw_response_sha256"],
            "token0_address": None,
            "token1_address": None,
            "token0_price_usd": None,
            "token1_price_usd": None,
        }

        route_publication._validate_dex_collector_contexts(
            universe, [core_leg]
        )

        forged = copy.deepcopy(core_leg)
        forged["token0_price_usd"] = "7"
        with self.assertRaisesRegex(ValueError, "terminal|price"):
            route_publication._validate_dex_collector_contexts(
                universe, [forged]
            )

    def test_loader_pins_core_a_when_a_new_private_core_is_orphaned(self):
        published = self._publish()
        second = _second_cohort()
        self._publish_core(second)

        loaded = load_latest_shadow_result(self.shadow_root)

        self.assertEqual(loaded["pointer_sha256"], published["pointer_sha256"])
        self.assertEqual(loaded["cohort"]["route_cohort_id"], self.cohort["route_cohort_id"])

    def test_publish_rejects_a_supplied_core_pointer_that_is_no_longer_current(self):
        old_pointer = copy.deepcopy(self.core_pointer)
        self._publish_core(_second_cohort())

        with self.assertRaisesRegex(ValueError, "core.*current|core.*changed"):
            publish_shadow_result(
                self.shadow_root,
                core_pointer=old_pointer,
                audit=self.audit,
            )
        self.assertFalse((self.shadow_root / "latest.json").exists())

    def test_structurally_valid_but_forged_metrics_are_rebuilt_from_core(self):
        self.audit["metrics"]["route_age_seconds_p95"]["value"] = "2"
        self.audit["metrics"]["route_age_seconds_max"]["value"] = "2"

        with self.assertRaisesRegex(ValueError, "audit.*metrics|rebuilt"):
            self._publish()

        self.assertFalse((self.shadow_root / "latest.json").exists())

    def test_universe_selection_inputs_rank_and_window_are_exact(self):
        mutations = (
            ("extra input", lambda value: value["selected_legs"][0][
                "selection_inputs"
            ].__setitem__("extra", "1")),
            ("rank gap", lambda value: value["selected_legs"][1].__setitem__(
                "selection_rank", 3
            )),
            ("wrong market inputs", lambda value: value["selected_legs"][0][
                "selection_inputs"
            ].__setitem__("dex_tvl_usd", "1")),
            ("negative zero", lambda value: value["selected_legs"][0][
                "selection_inputs"
            ].__setitem__("cex_selected_window_usd", "-0")),
            ("extreme exponent", lambda value: value["selected_legs"][0][
                "selection_inputs"
            ].__setitem__("cex_selected_window_usd", "1e999999999")),
            ("rank comparator", lambda value: value["selected_legs"][0][
                "selection_inputs"
            ].__setitem__("cex_selected_window_usd", "1")),
            ("short window", lambda value: value["selection_window"].__setitem__(
                "start", "2026-07-04"
            )),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                universe = self._universe(self.cohort)
                mutate(universe)
                with self.assertRaisesRegex(
                    ValueError,
                    "universe.*(input|rank|window|contract|leg|volume|invalid)",
                ):
                    route_publication._validate_route_universe_payload(universe)

        universe = self._universe(self.cohort)
        universe["selected_legs"][0]["collector_context"] = {
            "password": "secret",
            "path": "/tmp/outside",
        }
        with self.assertRaisesRegex(ValueError, "leg schema"):
            route_publication._validate_route_universe_payload(universe)

        universe = self._universe(self.cohort)
        universe["selected_legs"][0]["selection_inputs"][
            "cex_selected_window_usd"
        ] = "123456780"
        universe["selected_legs"][1]["selection_inputs"][
            "cex_selected_window_usd"
        ] = "123456789"
        with localcontext() as context:
            context.prec = 2
            with self.assertRaisesRegex(ValueError, "selection ranks"):
                route_publication._validate_route_universe_payload(universe)

    def test_cost_evidence_rejects_unknown_fields_on_publish_and_load(self):
        run_directory = self.shadow_root / "runs" / self.run_id
        cost_path = run_directory / "route-cost-evidence.json"
        cost = json.loads(cost_path.read_text(encoding="utf-8"))
        cost["unrelated_routes"] = ["route-from-another-run"]
        cost_bytes = _shadow_json_bytes(cost)
        cost_path.write_bytes(cost_bytes)
        self.audit["route_cost_evidence_sha256"] = hashlib.sha256(
            cost_bytes
        ).hexdigest()
        with self.assertRaisesRegex(ValueError, "route-cost evidence schema"):
            self._publish()

        self.run_id = "shadow-run-002"
        self.audit = self._write_shadow_inputs(self.run_id, self.cohort)
        run_directory = self.shadow_root / "runs" / self.run_id
        cost_path = run_directory / "route-cost-evidence.json"
        cost = json.loads(cost_path.read_text(encoding="utf-8"))
        cost_bytes = _shadow_json_bytes(cost)
        cost_path.write_bytes(cost_bytes)
        self.audit["route_cost_evidence_sha256"] = hashlib.sha256(
            cost_bytes
        ).hexdigest()
        published = self._publish()

        cost["unrelated_notionals"] = [999]
        corrupt_cost_bytes = _shadow_json_bytes(cost)
        cost_path.write_bytes(corrupt_cost_bytes)
        audit_path = run_directory / "audit.json"
        corrupt_audit = copy.deepcopy(published["audit"])
        corrupt_audit["route_cost_evidence_sha256"] = hashlib.sha256(
            corrupt_cost_bytes
        ).hexdigest()
        corrupt_audit_bytes = _shadow_json_bytes(corrupt_audit)
        audit_path.write_bytes(corrupt_audit_bytes)
        corrupt_pointer = copy.deepcopy(published["pointer"])
        corrupt_pointer["route_cost_evidence_sha256"] = corrupt_audit[
            "route_cost_evidence_sha256"
        ]
        corrupt_pointer["audit_sha256"] = hashlib.sha256(
            corrupt_audit_bytes
        ).hexdigest()
        corrupt_pointer_bytes = _shadow_pointer_bytes(corrupt_pointer)
        (self.shadow_root / "latest.json").write_bytes(corrupt_pointer_bytes)

        with self.assertRaisesRegex(ValueError, "route-cost evidence schema"):
            load_shadow_result(
                self.shadow_root,
                run_id=self.run_id,
                expected_pointer_sha256=hashlib.sha256(
                    corrupt_pointer_bytes
                ).hexdigest(),
            )

    def test_cost_evidence_requires_full_v1_shape_counts_and_hashes(self):
        run_directory = self.shadow_root / "runs" / self.run_id
        cost = json.loads(
            (run_directory / "route-cost-evidence.json").read_text(
                encoding="utf-8"
            )
        )
        universe = self._universe(self.cohort)
        universe_sha256 = route_universe_sha256(universe)

        def validate(value):
            route_publication._validate_cost_evidence_outer_lineage(
                value,
                run_id=self.run_id,
                route_cohort_id=self.cohort["route_cohort_id"],
                phase="canary",
                candidate_source_generation=self.generation,
                route_universe_sha256_value=universe_sha256,
                universe=universe,
            )

        validate(cost)
        common_fields = {
            "schema", "run_id", "route_cohort_id", "phase",
            "candidate_source_generation", "route_universe_sha256",
        }
        mutations = (
            (
                "bootstrap six",
                {key: value for key, value in cost.items() if key in common_fields},
                "schema",
            ),
            (
                "partial full",
                {key: value for key, value in cost.items() if key != "counts"},
                "schema",
            ),
            (
                "set hash",
                {**cost, "transcript_set_sha256": "f" * 64},
                "hash (?:mismatch|differs)",
            ),
            (
                "count",
                {**cost, "transcript_count": 1},
                "count differs|scope",
            ),
            (
                "profile generation",
                {**cost, "trace_profile_generation": "not-a-sha"},
                "trace profile generation differs",
            ),
        )
        for label, value, message in mutations:
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, message):
                    validate(value)

    def test_cost_evidence_cannot_shrink_dex_scope_or_forge_empty_bindings(self):
        dex_cohort = _dex_cohort()
        run_id = "shadow-run-dex-cost"
        self._write_shadow_inputs(run_id, dex_cohort)
        run_directory = self.shadow_root / "runs" / run_id
        cost = json.loads(
            (run_directory / "route-cost-evidence.json").read_text(
                encoding="utf-8"
            )
        )
        universe = self._universe(dex_cohort)
        universe_sha256 = route_universe_sha256(universe)

        def validate(value):
            route_publication._validate_cost_evidence_outer_lineage(
                value,
                run_id=run_id,
                route_cohort_id=dex_cohort["route_cohort_id"],
                phase="canary",
                candidate_source_generation=self.generation,
                route_universe_sha256_value=universe_sha256,
                universe=universe,
            )

        validate(cost)
        empty = copy.deepcopy(cost)
        empty["selected_markets"] = []
        empty["selected_market_count"] = 0
        with self.assertRaisesRegex(ValueError, "selected.*(?:scope|replay)"):
            validate(empty)

        subset = copy.deepcopy(cost)
        subset["selected_markets"] = subset["selected_markets"][:1]
        subset["selected_market_count"] = 1
        with self.assertRaisesRegex(ValueError, "selected.*(?:scope|replay)"):
            validate(subset)

        def typed_sha(domain, value):
            return hashlib.sha256(
                domain + _shadow_json_bytes(value) + b"\n"
            ).hexdigest()

        fake_binding = copy.deepcopy(cost)
        fake_binding["binding_count"] = 1
        fake_binding["bindings"] = [{"status": "observed"}]
        fake_binding["binding_set_sha256"] = typed_sha(
            b"route-cost-evidence-binding-set/v1\n",
            fake_binding["bindings"],
        )
        fake_binding["counts"]["binding_observed"] = 1
        with self.assertRaisesRegex(
            ValueError, "binding schema|empty selected scope|empty policy"
        ):
            validate(fake_binding)

        arbitrary_transcripts = copy.deepcopy(cost)
        arbitrary_transcripts["transcripts"] = [
            {"status": "unavailable"}
            for _row in arbitrary_transcripts["transcripts"]
        ]
        arbitrary_transcripts["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n",
            arbitrary_transcripts["transcripts"],
        )
        with self.assertRaisesRegex(ValueError, "transcript (?:schema|scope)"):
            validate(arbitrary_transcripts)

        observed_unsupported = copy.deepcopy(cost)
        observed_unsupported["transcripts"][0]["status"] = "observed"
        observed_unsupported["transcripts"][0]["reason_code"] = None
        observed_unsupported["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n",
            observed_unsupported["transcripts"],
        )
        observed_unsupported["counts"]["transcript_unavailable"] -= 1
        observed_unsupported["counts"]["transcript_observed"] += 1
        with self.assertRaisesRegex(
            ValueError, "observed transcript presence|unsupported-market transcript"
        ):
            validate(observed_unsupported)

        mixed = copy.deepcopy(cost)
        mixed["selected_markets"][0]["structural_support_status"] = "supported"
        mixed["selected_markets"][0]["structural_reason"] = None
        mixed["selected_market_set_sha256"] = hashlib.sha256(
            _shadow_json_bytes({
                "schema": "route_cost_selected_markets/v1",
                "members": mixed["selected_markets"],
            })
        ).hexdigest()
        mixed["submission_policy_snapshot"][
            "selected_market_set_sha256"
        ] = mixed["selected_market_set_sha256"]
        mixed["submission_policy_snapshot_sha256"] = typed_sha(
            b"route-cost-submission-policy-snapshot/v1\n",
            mixed["submission_policy_snapshot"],
        )
        unsupported_id = mixed["selected_markets"][1]["market_id"]
        mixed_transcript = next(
            row for row in mixed["transcripts"]
            if row["market_id"] == unsupported_id
        )
        mixed_transcript["status"] = "observed"
        mixed_transcript["reason_code"] = None
        mixed["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n",
            mixed["transcripts"],
        )
        mixed["counts"]["transcript_unavailable"] -= 1
        mixed["counts"]["transcript_observed"] += 1
        with self.assertRaisesRegex(
            ValueError, "selected market replay|unsupported-market transcript"
        ):
            validate(mixed)

    def test_post_replace_failure_rolls_back_only_an_owned_pointer(self):
        real_fsync = route_publication._fsync_directory

        def fail_after_replace(path, *, directory_fd=None):
            actual_root = Path(path)
            if actual_root.name == "shadow" and (
                actual_root / "latest.json"
            ).exists():
                raise OSError("injected shadow fsync failure")
            return real_fsync(path, directory_fd=directory_fd)

        with patch.object(
            route_publication, "_fsync_directory", side_effect=fail_after_replace
        ):
            with self.assertRaisesRegex(Exception, "fsync|uncertain"):
                self._publish()

        self.assertFalse((self.shadow_root / "latest.json").exists())
        self.assertFalse(any(
            path.name.startswith(".latest.")
            for path in self.shadow_root.iterdir()
        ))

    def test_post_replace_failure_restores_the_exact_prior_pointer(self):
        first = self._publish()
        prior_bytes = (self.shadow_root / "latest.json").read_bytes()
        self.run_id = "shadow-run-002"
        self.audit = self._write_shadow_inputs(self.run_id, self.cohort)
        real_fsync = route_publication._fsync_directory

        def fail_second_pointer(path, *, directory_fd=None):
            actual_root = Path(path)
            if actual_root.name == "shadow" and (
                actual_root / "latest.json"
            ).read_bytes() != prior_bytes:
                raise OSError("injected second shadow fsync failure")
            return real_fsync(path, directory_fd=directory_fd)

        with patch.object(
            route_publication,
            "_fsync_directory",
            side_effect=fail_second_pointer,
        ):
            with self.assertRaisesRegex(Exception, "fsync|uncertain"):
                self._publish()

        self.assertEqual((self.shadow_root / "latest.json").read_bytes(), prior_bytes)
        self.assertEqual(load_latest_shadow_result(self.shadow_root), first)

    def test_rollback_never_overwrites_same_bytes_from_a_new_inode(self):
        observed = {"third_party": None}
        real_fsync = route_publication._fsync_directory

        def replace_same_bytes_then_fail(path, *, directory_fd=None):
            actual_root = Path(path)
            pointer_path = actual_root / "latest.json"
            if actual_root.name == "shadow" and pointer_path.exists():
                attempted = pointer_path.read_bytes()
                replacement = actual_root / ".third-party-latest"
                replacement.write_bytes(attempted)
                os.replace(replacement, pointer_path)
                observed["third_party"] = os.stat(pointer_path).st_ino
                raise OSError("injected same-byte concurrent shadow writer")
            return real_fsync(path, directory_fd=directory_fd)

        with patch.object(
            route_publication,
            "_fsync_directory",
            side_effect=replace_same_bytes_then_fail,
        ):
            with self.assertRaisesRegex(Exception, "concurrent|uncertain"):
                self._publish()

        pointer_path = self.shadow_root / "latest.json"
        self.assertTrue(pointer_path.exists())
        self.assertEqual(os.stat(pointer_path).st_ino, observed["third_party"])
        self.assertEqual(
            json.loads(pointer_path.read_text(encoding="utf-8"))["run_id"],
            self.run_id,
        )

    def test_rollback_never_overwrites_a_concurrent_third_party_pointer(self):
        attacker = b'{"attacker":true}\n'
        real_fsync = route_publication._fsync_directory

        def replace_then_fail(path, *, directory_fd=None):
            actual_root = Path(path)
            if actual_root.name == "shadow" and (
                actual_root / "latest.json"
            ).exists():
                (actual_root / "latest.json").write_bytes(attacker)
                raise OSError("injected concurrent shadow writer")
            return real_fsync(path, directory_fd=directory_fd)

        with patch.object(
            route_publication, "_fsync_directory", side_effect=replace_then_fail
        ):
            with self.assertRaisesRegex(Exception, "concurrent|uncertain"):
                self._publish()

        self.assertEqual((self.shadow_root / "latest.json").read_bytes(), attacker)

    def test_shadow_root_rename_away_and_back_is_detected(self):
        real_read = route_publication._read_shadow_run_evidence
        detached = self.shadow_root.with_name("shadow-detached")

        def read_then_swap_back(shadow_root, run_id):
            result = real_read(shadow_root, run_id)
            os.rename(self.shadow_root, detached)
            os.rename(detached, self.shadow_root)
            return result

        with patch.object(
            route_publication,
            "_read_shadow_run_evidence",
            side_effect=read_then_swap_back,
        ):
            with self.assertRaisesRegex(ValueError, "shadow root|phase.*changed"):
                self._publish()

        self.assertFalse((self.shadow_root / "latest.json").exists())

    def test_run_evidence_replacement_and_in_place_aba_fail_closed(self):
        published = self._publish()
        run_directory = self.shadow_root / "runs" / self.run_id
        real_read = route_publication._read_canonical_object_at

        for mode in ("replace", "in_place"):
            with self.subTest(mode=mode):
                changed = {"done": False}

                def mutate_after_later_member(directory_fd, filename, **kwargs):
                    result = real_read(directory_fd, filename, **kwargs)
                    if filename == "audit.json" and not changed["done"]:
                        target = run_directory / "route_universe.json"
                        original = target.read_bytes()
                        if mode == "replace":
                            replacement = run_directory / ".universe-replacement"
                            replacement.write_bytes(original)
                            os.replace(replacement, target)
                        else:
                            with target.open("r+b") as handle:
                                handle.seek(0)
                                handle.write(original)
                                handle.flush()
                                os.fsync(handle.fileno())
                        changed["done"] = True
                    return result

                with patch.object(
                    route_publication,
                    "_read_canonical_object_at",
                    side_effect=mutate_after_later_member,
                ):
                    with self.assertRaisesRegex(ValueError, "changed"):
                        load_shadow_result(
                            self.shadow_root,
                            run_id=self.run_id,
                            expected_pointer_sha256=published["pointer_sha256"],
                        )

    def test_cleanup_preserves_primary_failure_and_attempts_both_unlocks(self):
        real_fsync = route_publication._fsync_directory
        real_flock = route_publication.fcntl.flock
        unlocks = []

        def fail_after_replace(path, *, directory_fd=None):
            actual_root = Path(path)
            if actual_root.name == "shadow" and (
                actual_root / "latest.json"
            ).exists():
                raise OSError("primary shadow fsync failure")
            return real_fsync(path, directory_fd=directory_fd)

        def fail_first_unlock(fd, operation):
            if operation == route_publication.fcntl.LOCK_UN:
                unlocks.append(fd)
                if len(unlocks) == 1:
                    raise OSError("secondary unlock failure")
            return real_flock(fd, operation)

        with patch.object(
            route_publication, "_fsync_directory", side_effect=fail_after_replace
        ), patch.object(
            route_publication.fcntl, "flock", side_effect=fail_first_unlock
        ):
            with self.assertRaisesRegex(OSError, "primary shadow fsync failure"):
                self._publish()

        self.assertGreaterEqual(len(unlocks), 2)

    def test_shadow_close_failure_does_not_skip_core_cleanup(self):
        real_flock = route_publication.fcntl.flock
        real_close = route_publication.os.close
        descriptors = {"core": None, "shadow": None}
        close_attempts = []
        failed = {"shadow": False}

        def remember_locks(fd, operation):
            if operation == route_publication.fcntl.LOCK_SH:
                descriptors["core"] = fd
            elif operation == route_publication.fcntl.LOCK_EX:
                descriptors["shadow"] = fd
            return real_flock(fd, operation)

        def fail_shadow_close_once(fd):
            close_attempts.append(fd)
            if fd == descriptors["shadow"] and not failed["shadow"]:
                failed["shadow"] = True
                raise OSError("injected shadow close failure")
            return real_close(fd)

        try:
            with patch.object(
                route_publication.fcntl, "flock", side_effect=remember_locks
            ), patch.object(
                route_publication.os, "close", side_effect=fail_shadow_close_once
            ):
                with self.assertRaisesRegex(ValueError, "shadow descriptor close"):
                    self._publish()
        finally:
            if descriptors["shadow"] is not None and failed["shadow"]:
                try:
                    real_close(descriptors["shadow"])
                except OSError:
                    pass

        self.assertIn(descriptors["core"], close_attempts)

    def test_historical_loader_preserves_lineage_error_over_unlock_error(self):
        published = self._publish()
        real_flock = route_publication.fcntl.flock

        def fail_unlock(fd, operation):
            if operation == route_publication.fcntl.LOCK_UN:
                raise OSError("secondary historical unlock failure")
            return real_flock(fd, operation)

        with patch.object(
            route_publication,
            "_validate_joint_lineage",
            side_effect=ValueError("primary historical lineage failure"),
        ), patch.object(
            route_publication.fcntl,
            "flock",
            side_effect=fail_unlock,
        ):
            with self.assertRaisesRegex(
                ValueError, "primary historical lineage failure"
            ):
                load_shadow_result(
                    self.shadow_root,
                    run_id=self.run_id,
                    expected_pointer_sha256=published["pointer_sha256"],
                )

    def test_core_descriptor_is_closed_when_second_root_open_fails(self):
        real_open = route_publication._open_verified_directory
        real_release = route_publication._release_route_lock_and_close
        shadow_opens = {"count": 0}
        cleanup_labels = []

        def fail_final_shadow_open(path, label):
            if label == "route shadow root":
                shadow_opens["count"] += 1
                if shadow_opens["count"] == 3:
                    raise OSError("injected final shadow open failure")
            return real_open(path, label)

        def record_cleanup(descriptor, *, locked, label):
            cleanup_labels.append(label)
            return real_release(descriptor, locked=locked, label=label)

        with patch.object(
            route_publication,
            "_open_verified_directory",
            side_effect=fail_final_shadow_open,
        ), patch.object(
            route_publication,
            "_release_route_lock_and_close",
            side_effect=record_cleanup,
        ):
            with self.assertRaisesRegex(OSError, "final shadow open failure"):
                self._publish()

        self.assertIn("route core", cleanup_labels)

    def test_phase_change_at_pointer_boundary_rolls_back_shadow_latest(self):
        real_commit = route_publication._commit_shadow_pointer_at_locked

        def transition_then_commit(
            shadow_fd, shadow_path, pointer_bytes, *, commit_state=None
        ):
            self._install_full_phase()
            return real_commit(
                shadow_fd,
                shadow_path,
                pointer_bytes,
                commit_state=commit_state,
            )

        with patch.object(
            route_publication,
            "_commit_shadow_pointer_at_locked",
            side_effect=transition_then_commit,
        ):
            with self.assertRaisesRegex(ValueError, "lineage changed|phase"):
                self._publish()

        self.assertFalse((self.shadow_root / "latest.json").exists())

    def test_hardlinked_run_evidence_and_unsafe_run_ids_fail_closed(self):
        run_directory = self.shadow_root / "runs" / self.run_id
        universe = run_directory / "route_universe.json"
        replacement = run_directory / "universe-hardlink"
        os.link(universe, replacement)
        with self.assertRaisesRegex(ValueError, "hard-linked|single-link|unsafe"):
            self._publish()

        with self.assertRaisesRegex(ValueError, "run ID"):
            load_shadow_result(
                self.shadow_root,
                run_id="../outside",
                expected_pointer_sha256="a" * 64,
            )


if __name__ == "__main__":
    unittest.main()
