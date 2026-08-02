"""Tests for immutable publication of normalized route-cohort bundles."""

from __future__ import annotations

import copy
import csv
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from scripts.route_publication import (
    build_route_cohort_sqlite,
    load_latest_route_cohort,
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
from scripts.route_inventory import (
    INVENTORY_PROFILE_COLUMNS,
    classify_route_mode_evidence,
    inventory_capacity_for_route,
    load_validated_inventory_profile,
)
from tests.test_route_opportunity import cex_leg, route_and_mode
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
    first = "dex:eth:uniswap:0xaaa:UNI"
    second = "dex:eth:uniswap:0xbbb:UNI"

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

    def test_private_tmp_alias_normalization_is_darwin_only(self):
        with patch("scripts.route_publication.sys.platform", "linux"):
            self.assertEqual(
                route_publication._absolute_without_symlink_resolution(
                    Path("/tmp/route-core")
                ),
                Path("/tmp/route-core"),
            )


class CompleteRouteBundleTests(TemporaryRouteRootTestCase):
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
        original = "dex:eth:uniswap:0xaaa:UNI"
        malformed = "dex:eth:uniswap:../x:UNI"
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


if __name__ == "__main__":
    unittest.main()
