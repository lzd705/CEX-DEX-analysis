"""Explicit CEX-only finalization of one pinned route Shadow run."""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Mapping, Optional

if __package__ in {None, ""}:  # Support ``python scripts/<name>.py``.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.cex_fee_facts import (
    collect_cex_fee_snapshot,
    load_validated_fee_profile,
)
from scripts.execution_cost_components import cost_component_row
from scripts.fetch_cex_depth import (
    cex_quantity_state_id,
    route_quantity_quote_for_book,
)
from scripts.route_cost_evidence import (
    RouteCostEvidenceError,
    replay_route_cost_coverage_outcomes,
)
from scripts.route_inventory import (
    classify_route_mode_evidence,
    inventory_capacity_for_route,
    load_validated_inventory_profile,
)
from scripts.route_opportunity import (
    build_route_opportunity,
    common_target_quantity,
    route_opportunity_id,
    usd_projection_evidence,
)
from scripts.route_publication import (
    _USD_SOURCE_FIELDS,
    _open_verified_directory,
    _parse_cex_book_source,
    _parse_market_rules_source,
    _pointer_payload_bytes,
    _read_core_raw_members,
    _read_member_from_root,
    _read_shadow_run_evidence,
    _verify_open_path_identity,
    load_latest_route_cohort,
    load_shadow_result,
    publish_complete_route_bundle,
)
from scripts.route_quantity import FeeSemantics
from scripts.route_shadow_inputs import typed_source_lineage_observed_members
from scripts.timestamp_contract import exact_rfc3339_epoch_seconds


class RouteOpportunityPipelineError(ValueError):
    """Raised before publication when pinned opportunity inputs are invalid."""


_FEE_PROFILE_ENV = "MARKET_CEX_PRIVATE_FEE_PROFILE"
_INVENTORY_PROFILE_ENV = "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE"


def _profile_path(name: str) -> Path:
    value = os.environ.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise RouteOpportunityPipelineError(
            "required private profile environment is missing"
        )
    path = Path(value)
    if not path.is_absolute():
        raise RouteOpportunityPipelineError(
            "private profile environment must name an absolute path"
        )
    return path


def _source_members_by_market(
    cohort: Mapping[str, Any],
) -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = {}
    expected = {
        "cex_raw_book_response", "cex_market_rules", "quote_usd_conversion"
    }
    for leg in cohort["legs"]:
        try:
            observed = typed_source_lineage_observed_members(
                leg["typed_source_lineage"], market_type="cex"
            )
        except (KeyError, TypeError, ValueError) as error:
            raise RouteOpportunityPipelineError(
                "CEX typed-source lineage is incomplete"
            ) from error
        members = {row["role"]: row["filename"] for row in observed}
        if set(members) != expected:
            raise RouteOpportunityPipelineError(
                "CEX typed-source lineage is incomplete"
            )
        result[leg["market_id"]] = members
    return result


def _validated_usd_source(
    payload: Mapping[str, Any],
    *,
    quote_asset: str,
    now: str,
) -> Decimal:
    if (
        set(payload) != _USD_SOURCE_FIELDS
        or payload.get("schema") != "route_usd_conversion_source/v1"
        or payload.get("quote_asset") != quote_asset
    ):
        raise RouteOpportunityPipelineError("CEX USD source is invalid")
    try:
        raw_rate = payload["usd_per_quote"]
        if not isinstance(raw_rate, str) or not raw_rate:
            raise ValueError("invalid rate")
        rate = Decimal(raw_rate)
        observed = exact_rfc3339_epoch_seconds(payload["observed_at"])
        valid = exact_rfc3339_epoch_seconds(payload["valid_until"])
        evaluated = exact_rfc3339_epoch_seconds(now)
    except (KeyError, InvalidOperation, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError("CEX USD source is invalid") from error
    canonical_rate = format(rate, "f")
    if "." in canonical_rate:
        canonical_rate = canonical_rate.rstrip("0").rstrip(".")
    if (
        not rate.is_finite()
        or rate <= 0
        or canonical_rate != raw_rate
        or not observed <= evaluated < valid
        or not isinstance(payload.get("source"), str)
        or not payload["source"]
    ):
        raise RouteOpportunityPipelineError("CEX USD source is invalid")
    return rate


def _fee_semantics(
    rows: List[Mapping[str, str]],
    *,
    profile_id: str,
    market_id: str,
    direction: str,
    rules: Any,
) -> FeeSemantics:
    _prefix, venue, instrument = market_id.split(":", 2)
    matches = [
        row for row in rows
        if row["profile_id"] == profile_id
        and row["venue"] == venue
        and row["instrument"] == instrument
        and row["side"] == direction
    ]
    if len(matches) != 1:
        raise RouteOpportunityPipelineError(
            "private fee profile is incomplete for the CEX route"
        )
    row = matches[0]
    try:
        return FeeSemantics(
            rate_bps=Decimal(row["taker_fee_bps"]),
            fee_asset=row["fee_asset"],
            charge_basis=(
                "received_base" if direction == "buy" else "received_quote"
            ),
            fee_increment=(
                rules.base_increment
                if direction == "buy" else rules.quote_increment
            ),
            rounding_mode="ceiling",
            third_asset_quote_price=None,
            observed_at=row["observed_at"],
            valid_until=row["valid_until"],
            source_record_sha256=row["source_record_sha256"],
            conversion_source_record_sha256=None,
        )
    except (InvalidOperation, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "private fee profile cannot build CEX fee semantics"
        ) from error


def _build_inputs(
    *,
    root: Path,
    cohort: Mapping[str, Any],
    core_manifest_sha256: str,
    source_root: Path,
    fee_profile_path: Path,
    inventory_profile_path: Path,
    now: str,
) -> tuple[List[Dict[str, Any]], str]:
    try:
        fee_rows = load_validated_fee_profile(fee_profile_path, now=now)
        inventory_rows = load_validated_inventory_profile(
            inventory_profile_path, now=now
        )
    except (TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "private fee or inventory profile is invalid"
        ) from error
    profile_ids = {row["profile_id"] for row in fee_rows}
    if len(profile_ids) != 1:
        raise RouteOpportunityPipelineError(
            "private fee profile identity is not exact"
        )
    profile_id = next(iter(profile_ids))
    members_by_market = _source_members_by_market(cohort)
    legs_by_market = {row["market_id"]: row for row in cohort["legs"]}
    if len(legs_by_market) != len(cohort["legs"]):
        raise RouteOpportunityPipelineError("CEX core leg inventory is invalid")

    try:
        raw_members = _read_core_raw_members(
            root / "raw/route-cohort", cohort
        )
        source_path, source_fd, source_details = _open_verified_directory(
            source_root, "route typed-source root"
        )
    except (TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "retained CEX source evidence is invalid"
        ) from error
    sources: Dict[str, Dict[str, Any]] = {}
    try:
        for market_id, leg in legs_by_market.items():
            payload, raw_bytes, _raw_sha = raw_members[market_id]
            market, book = _parse_cex_book_source(
                payload,
                raw_bytes,
                market_id=market_id,
                state_observed_at=leg["state_observed_at"],
            )
            filenames = members_by_market[market_id]
            rules_payload, _rules_bytes, rules_sha = _read_member_from_root(
                source_fd,
                filenames["cex_market_rules"],
                label="CEX market-rules source",
            )
            rules = _parse_market_rules_source(
                rules_payload, rules_sha, market_id=market_id
            )
            usd_payload, _usd_bytes, usd_sha = _read_member_from_root(
                source_fd,
                filenames["quote_usd_conversion"],
                label="CEX USD conversion source",
            )
            usd_rate = _validated_usd_source(
                usd_payload, quote_asset=rules.quote_asset, now=now
            )
            if not (
                exact_rfc3339_epoch_seconds(rules.observed_at)
                <= exact_rfc3339_epoch_seconds(now)
                < exact_rfc3339_epoch_seconds(rules.valid_until)
            ):
                raise RouteOpportunityPipelineError(
                    "CEX market rules do not cover evaluation time"
                )
            sources[market_id] = {
                "market": market,
                "book": book,
                "rules": rules,
                "usd": usd_payload,
                "usd_rate": usd_rate,
                "usd_sha": usd_sha,
                "filenames": filenames,
            }
        _verify_open_path_identity(
            source_path,
            source_details,
            "route typed-source root",
        )
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, RouteOpportunityPipelineError):
            raise
        raise RouteOpportunityPipelineError(
            "retained CEX source evidence is invalid"
        ) from error
    finally:
        os.close(source_fd)

    opportunities: List[Dict[str, Any]] = []
    cohort_now = cohort["collection_completed_at"]
    for route in cohort["routes"]:
        buy_source = sources[route["buy_market_id"]]
        sell_source = sources[route["sell_market_id"]]
        fees = {
            direction: _fee_semantics(
                fee_rows,
                profile_id=profile_id,
                market_id=route[direction + "_market_id"],
                direction=direction,
                rules=sources[route[direction + "_market_id"]]["rules"],
            )
            for direction in ("buy", "sell")
        }
        buy_reference = (
            buy_source["book"]["asks"][0][0] * buy_source["usd_rate"]
        )
        sell_reference = (
            sell_source["book"]["bids"][0][0] * sell_source["usd_rate"]
        )
        for raw_notional in cohort["requested_notionals_usd"]:
            notional = Decimal(str(raw_notional))
            try:
                target = common_target_quantity(
                    requested_notional_usd=notional,
                    buy_reference_price_usd=buy_reference,
                    sell_reference_price_usd=sell_reference,
                    buy_market_rules=buy_source["rules"],
                    sell_market_rules=sell_source["rules"],
                )
                quotes: Dict[str, Any] = {}
                quote_evidence: Dict[str, Any] = {}
                build_legs: Dict[str, Dict[str, Any]] = {}
                projections: Dict[str, Optional[Dict[str, Any]]] = {}
                for direction in ("buy", "sell"):
                    market_id = route[direction + "_market_id"]
                    source = sources[market_id]
                    leg = legs_by_market[market_id]
                    state_id = cex_quantity_state_id(
                        source["market"],
                        source["book"],
                        snapshot_id=leg["snapshot_id"],
                        observed_at=leg["state_observed_at"],
                        cohort_now=cohort_now,
                        market_rules=source["rules"],
                        fee_semantics=fees[direction],
                    )
                    quote = route_quantity_quote_for_book(
                        source["market"],
                        source["book"],
                        direction=direction,
                        target_token_quantity=target,
                        market_rules=source["rules"],
                        fee_semantics=fees[direction],
                        snapshot_id=leg["snapshot_id"],
                        observed_at=leg["state_observed_at"],
                        cohort_now=cohort_now,
                        expected_state_id=state_id,
                    )
                    quotes[direction] = quote
                    build_legs[direction] = {**dict(leg), "state_id": state_id}
                    quote_evidence[direction] = {
                        "kind": "cex_book",
                        "market": source["market"],
                        "book": source["book"],
                        "market_rules": source["rules"],
                        "fee_semantics": fees[direction],
                        "snapshot_id": leg["snapshot_id"],
                        "observed_at": leg["state_observed_at"],
                        "cohort_now": cohort_now,
                        "expected_state_id": state_id,
                        "assurance_status": "route_bundle_validated",
                        "core_manifest_sha256": core_manifest_sha256,
                    }
                    cash = (
                        quote.quote_debit_quantity
                        if direction == "buy"
                        else quote.quote_received_quantity
                    )
                    projections[direction] = (
                        None
                        if cash is None
                        else usd_projection_evidence(
                            market_id=market_id,
                            state_id=state_id,
                            direction=direction,
                            quote_asset=source["rules"].quote_asset,
                            quote_cash_quantity=cash,
                            usd_per_quote=source["usd_rate"],
                            value_status="authenticated",
                            observed_at=source["usd"]["observed_at"],
                            valid_until=source["usd"]["valid_until"],
                            source=source["usd"]["source"],
                            source_record_sha256=source["usd_sha"],
                            core_manifest_sha256=core_manifest_sha256,
                        )
                    )

                opportunity_id = route_opportunity_id(
                    route["route_id"], notional
                )
                costs = [
                    collect_cex_fee_snapshot(
                        cohort_id=cohort["route_cohort_id"],
                        opportunity_id=opportunity_id,
                        leg=direction,
                        market_id=route[direction + "_market_id"],
                        venue=route[direction + "_market_id"].split(":", 2)[1],
                        instrument=route[direction + "_market_id"].split(":", 2)[2],
                        side=direction,
                        requested_notional_usd=notional,
                        target_token_quantity=target.quantity,
                        now=now,
                        private_profile_path=fee_profile_path,
                        profile_id=profile_id,
                    )
                    for direction in ("buy", "sell")
                ]
                if any(row.get("value_status") != "authenticated" for row in costs):
                    raise RouteOpportunityPipelineError(
                        "private fee profile did not reproduce every CEX cost"
                    )
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
                if all(
                    quotes[direction].calculation_complete
                    for direction in ("buy", "sell")
                ):
                    inventory = inventory_capacity_for_route(
                        route,
                        inventory_rows,
                        buy_quote_asset=quotes["buy"].quote_debit_asset,
                        buy_quote_quantity=quotes["buy"].quote_debit_quantity,
                        sell_token_asset=quotes["sell"].target_base_asset,
                        sell_net_token_quantity=quotes["sell"].base_debit_quantity,
                        now=now,
                    )
                    if inventory.get("status") == "inventory_unavailable":
                        raise RouteOpportunityPipelineError(
                            "private inventory profile is incomplete for the CEX route"
                        )
                    expected_request = {
                        key: inventory[key]
                        for key in (
                            "route_id", "buy_market_id", "sell_market_id",
                            "buy_quote_asset", "buy_quote_quantity",
                            "sell_token_asset", "sell_net_token_quantity",
                            "target_asset", "target_quantity",
                        )
                    }
                    mode = classify_route_mode_evidence(
                        route,
                        expected_request=expected_request,
                        inventory_evidence=inventory,
                        now=now,
                    )
                else:
                    mode = classify_route_mode_evidence(route, now=now)
                build_inputs = {
                    "cohort_id": cohort["route_cohort_id"],
                    "route": route,
                    "requested_notional_usd": notional,
                    "common_target": target,
                    "buy_leg": build_legs["buy"],
                    "sell_leg": build_legs["sell"],
                    "buy_quote": quotes["buy"],
                    "sell_quote": quotes["sell"],
                    "buy_quote_evidence": quote_evidence["buy"],
                    "sell_quote_evidence": quote_evidence["sell"],
                    "buy_usd_projection": projections["buy"],
                    "sell_usd_projection": projections["sell"],
                    "cost_components": costs,
                    "mode_evidence": mode,
                    "now": now,
                }
                opportunities.append({
                    "classified_opportunity": build_route_opportunity(
                        **build_inputs
                    ),
                    "build_inputs": build_inputs,
                    "source_members": {
                        "buy_market_rules": buy_source["filenames"][
                            "cex_market_rules"
                        ],
                        "sell_market_rules": sell_source["filenames"][
                            "cex_market_rules"
                        ],
                        "buy_usd_conversion": buy_source["filenames"][
                            "quote_usd_conversion"
                        ],
                        "sell_usd_conversion": sell_source["filenames"][
                            "quote_usd_conversion"
                        ],
                    },
                })
            except (KeyError, InvalidOperation, TypeError, ValueError) as error:
                if isinstance(error, RouteOpportunityPipelineError):
                    raise
                raise RouteOpportunityPipelineError(
                    "CEX opportunity inputs cannot be reconstructed"
                ) from error
    return opportunities, profile_id


def finalize_cex_route_opportunities(
    *,
    data_dir: Path,
    shadow_run_id: str,
    expected_joint_pointer_sha256: str,
) -> Dict[str, Any]:
    """Finalize one explicitly pinned CEX-only Shadow run."""
    root = Path(data_dir)
    try:
        shadow = load_shadow_result(
            root / "routes/shadow",
            run_id=shadow_run_id,
            expected_pointer_sha256=expected_joint_pointer_sha256,
        )
        # Shadow selection is historical and pinned above.  Current publication
        # separately requires the pinned core to remain the live core, matching
        # the publisher's freshness/CAS gate.
        latest_core = load_latest_route_cohort(root / "routes/core")
    except (TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "pinned route Shadow evidence is invalid"
        ) from error

    pointer = shadow.get("pointer")
    cohort = shadow.get("cohort")
    latest_cohort = latest_core.get("cohort")
    latest_pointer = latest_core.get("pointer")
    if (
        not isinstance(pointer, dict)
        or not isinstance(cohort, dict)
        or shadow.get("pointer_sha256") != expected_joint_pointer_sha256
        or pointer.get("run_id") != shadow_run_id
        or not isinstance(latest_pointer, dict)
        or latest_core.get("manifest_sha256")
        != pointer.get("core_manifest_sha256")
        or pointer.get("core_pointer_sha256")
        != hashlib.sha256(
            _pointer_payload_bytes(latest_pointer)
        ).hexdigest()
        or latest_cohort != cohort
        or cohort.get("raw_evidence_run_id") != shadow_run_id
        or cohort.get("candidate_source_generation")
        != pointer.get("candidate_source_generation")
    ):
        raise RouteOpportunityPipelineError(
            "pinned Shadow run and latest core lineage differ"
        )

    legs = cohort.get("legs")
    routes = cohort.get("routes")
    if not isinstance(legs, list) or not isinstance(routes, list):
        raise RouteOpportunityPipelineError("CEX-only core inventory is invalid")
    leg_types = {
        leg.get("market_id"): leg.get("market_type")
        for leg in legs if isinstance(leg, dict)
    }
    if (
        len(leg_types) != len(legs)
        or any(market_type != "cex" for market_type in leg_types.values())
        or any(
            not isinstance(route, dict)
            or leg_types.get(route.get("buy_market_id")) != "cex"
            or leg_types.get(route.get("sell_market_id")) != "cex"
            for route in routes
        )
    ):
        raise RouteOpportunityPipelineError(
            "route opportunity finalization is CEX-only"
        )

    try:
        evidence = _read_shadow_run_evidence(
            root / "routes/shadow", shadow_run_id
        )
    except (TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "pinned route-cost evidence is invalid"
        ) from error
    cost_bytes = evidence.get("cost_evidence_bytes")
    cost_evidence = evidence.get("cost_evidence")
    if (
        not isinstance(cost_bytes, bytes)
        or not isinstance(cost_evidence, dict)
        or hashlib.sha256(cost_bytes).hexdigest()
        != pointer.get("route_cost_evidence_sha256")
    ):
        raise RouteOpportunityPipelineError(
            "pinned route-cost sidecar hash differs"
        )
    try:
        outcomes = replay_route_cost_coverage_outcomes(
            cost_evidence,
            universe=evidence["universe"],
            expected_run_id=shadow_run_id,
            expected_route_cohort_id=cohort["route_cohort_id"],
            expected_phase=pointer["phase"],
            expected_candidate_source_generation=cohort[
                "candidate_source_generation"
            ],
            expected_route_universe_sha256=pointer[
                "route_universe_sha256"
            ],
            retained_typed_pool_state_members={},
        )
    except (KeyError, RouteCostEvidenceError, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "pinned route-cost evidence replay failed"
        ) from error
    expected_count = len(cohort["routes"]) * len(
        cohort["requested_notionals_usd"]
    )
    if len(outcomes) != expected_count or any(
        row.get("status") != "not_applicable"
        or row.get("reason_code") is not None
        or row.get("coverage_kind") != "not_applicable"
        for row in outcomes
    ):
        raise RouteOpportunityPipelineError(
            "route-cost evidence is not CEX-only"
        )
    now = cost_evidence.get("evaluated_at")
    if not isinstance(now, str):
        raise RouteOpportunityPipelineError(
            "route-cost evidence evaluation time is missing"
        )
    fee_profile_path = _profile_path(_FEE_PROFILE_ENV)
    inventory_profile_path = _profile_path(_INVENTORY_PROFILE_ENV)
    source_root = (
        root / "raw/route-cohort" / shadow_run_id / "typed"
    )
    inputs, profile_id = _build_inputs(
        root=root,
        cohort=cohort,
        core_manifest_sha256=pointer["core_manifest_sha256"],
        source_root=source_root,
        fee_profile_path=fee_profile_path,
        inventory_profile_path=inventory_profile_path,
        now=now,
    )
    def validate_pinned_shadow() -> None:
        try:
            confirmed_shadow = load_shadow_result(
                root / "routes/shadow",
                run_id=shadow_run_id,
                expected_pointer_sha256=expected_joint_pointer_sha256,
            )
        except (TypeError, ValueError) as error:
            raise RouteOpportunityPipelineError(
                "pinned route Shadow evidence changed during input reconstruction"
            ) from error
        if confirmed_shadow != shadow:
            raise RouteOpportunityPipelineError(
                "pinned route Shadow evidence changed during input reconstruction"
            )

    validate_pinned_shadow()
    try:
        return publish_complete_route_bundle(
            core_root=root / "routes/core",
            routes_root=root / "routes",
            raw_root=root / "raw/route-cohort",
            opportunity_inputs=inputs,
            source_root=source_root,
            fee_profile_path=fee_profile_path,
            fee_profile_id=profile_id,
            inventory_profile_path=inventory_profile_path,
            precommit_validator=validate_pinned_shadow,
        )
    except (TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "CEX opportunity publication failed"
        ) from error


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Finalize one explicitly pinned CEX-only route Shadow run."
    )
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--shadow-run-id", required=True)
    parser.add_argument("--expected-joint-pointer-sha256", required=True)
    arguments = parser.parse_args(argv)
    pointer = finalize_cex_route_opportunities(
        data_dir=arguments.data_dir,
        shadow_run_id=arguments.shadow_run_id,
        expected_joint_pointer_sha256=(
            arguments.expected_joint_pointer_sha256
        ),
    )
    print(json.dumps(pointer, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
