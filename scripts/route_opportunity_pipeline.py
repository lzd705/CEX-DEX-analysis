"""Finalize one pinned route Shadow run into Current Opportunities."""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

if __package__ in {None, ""}:  # Support ``python scripts/<name>.py``.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.cex_fee_facts import (
    _PublicFeeScheduleSnapshot,
    _collect_cex_fee_snapshot_from_schedule_snapshot,
    _load_public_fee_schedule_snapshot,
    collect_cex_fee_snapshot,
    load_validated_fee_profile,
)
from scripts.collect_route_cohort import _validated_typed_payload_inventory
from scripts.execution_cost_components import cost_component_row
from scripts.fetch_cex_depth import (
    cex_quantity_state_id,
    route_quantity_quote_for_book,
)
from scripts.fetch_dex_depth import freeze_v2_pool_state
from scripts.live_cex_research import (
    build_live_cex_research_universe,
    live_cex_research_generation,
    public_fee_semantics,
)
from scripts.route_cost_evidence import (
    RouteCostEvidenceError,
    network_gas_usd,
    physical_sha256,
    replay_route_cost_coverage_outcomes,
    typed_sha256,
    validate_retained_v2_pool_state_member,
)
from scripts.route_cost_topology import live_complete_cost_component_keys
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
    _validate_route_collector_context,
    _verify_open_path_identity,
    load_latest_route_cohort,
    load_latest_complete_route_bundle,
    load_shadow_result,
    publish_complete_route_bundle,
)
from scripts.route_quantity import (
    CommonTarget,
    FeeSemantics,
    MarketRules,
    quote_v2_pool_quantity,
)
from scripts.route_shadow_inputs import (
    TYPED_SOURCE_LINEAGE_SCHEMA_V2,
    typed_source_lineage_observed_members,
)
from scripts.timestamp_contract import exact_rfc3339_epoch_seconds


class RouteOpportunityPipelineError(ValueError):
    """Raised before publication when pinned opportunity inputs are invalid."""


_FEE_PROFILE_ENV = "MARKET_CEX_PRIVATE_FEE_PROFILE"
_INVENTORY_PROFILE_ENV = "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE"
_DEX_TYPED_ROLES = frozenset({
    "dex_market_rules",
    "dex_pool_state",
    "dex_usd_conversion",
    "dex_usd_price_context",
})
_DEX_POOL_INTEGER_FIELDS = frozenset({
    "chain_id", "token0_decimals", "token1_decimals", "reserve0_raw",
    "reserve1_raw", "reserve_timestamp_last_raw", "fee_bps",
    "fee_numerator", "fee_denominator", "block_number",
})
_RESEARCH_MEV_BPS = re.compile(
    r"(?:0|[1-9][0-9]{0,4})(?:\.[0-9]{1,6})?\Z",
    flags=re.ASCII,
)


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _dex_source_members_by_market(
    cohort: Mapping[str, Any],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for leg in cohort["legs"]:
        lineage = (
            leg.get("typed_source_lineage")
            if isinstance(leg, Mapping) else None
        )
        if (
            not isinstance(leg, Mapping)
            or not isinstance(lineage, Mapping)
            or lineage.get("schema")
            != TYPED_SOURCE_LINEAGE_SCHEMA_V2
        ):
            raise RouteOpportunityPipelineError(
                "DEX typed-source lineage is not complete v2 evidence"
            )
        try:
            observed = typed_source_lineage_observed_members(
                lineage, market_type="dex"
            )
        except (KeyError, TypeError, ValueError) as error:
            raise RouteOpportunityPipelineError(
                "DEX typed-source lineage is not complete v2 evidence"
            ) from error
        members = {row["role"]: row for row in observed}
        market_id = leg.get("market_id")
        if (
            not isinstance(market_id, str)
            or set(members) != _DEX_TYPED_ROLES
            or market_id in result
        ):
            raise RouteOpportunityPipelineError(
                "DEX typed-source lineage is not complete v2 evidence"
            )
        result[market_id] = members
    return result


def _cost_supported_pool_members(
    cost_evidence: Mapping[str, Any],
    retained: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Mapping[str, Any]]:
    selected = cost_evidence.get("selected_markets")
    if not isinstance(selected, list):
        raise RouteOpportunityPipelineError(
            "pinned route-cost selected-market inventory is invalid"
        )
    supported = {
        row.get("market_id")
        for row in selected
        if isinstance(row, Mapping)
        and row.get("structural_support_status") == "supported"
    }
    if (
        any(not isinstance(market_id, str) for market_id in supported)
        or not supported.issubset(retained)
    ):
        raise RouteOpportunityPipelineError(
            "supported DEX cost pool-state evidence is missing"
        )
    return {
        market_id: retained[market_id]
        for market_id in sorted(supported)
    }


def _frozen_v2_state(payload: Mapping[str, Any]) -> Any:
    source = {
        key: int(value) if key in _DEX_POOL_INTEGER_FIELDS else value
        for key, value in payload.items()
        if key not in {"schema", "state_id"}
    }
    return freeze_v2_pool_state(source)


def _loopback_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer") from error
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(
            "port must be between 1 and 65535"
        )
    return port


def _exec_read_only_dashboard(*, data_dir: Path, port: int) -> None:
    root = Path(data_dir).resolve()
    project_root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    for name in (
        _FEE_PROFILE_ENV,
        _INVENTORY_PROFILE_ENV,
        "MARKET_DATABASE",
        "MARKET_CEX_DATA",
        "MARKET_DEX_DATA",
        "MARKET_TVL_DATA",
        "MARKET_CEX_DEPTH_DATA",
        "MARKET_DEX_DEPTH_DATA",
        "MARKET_CEX_EXECUTION_COST_DATA",
        "MARKET_DEX_EXECUTION_COST_DATA",
        "MARKET_EVENT_DATA_DIR",
        "MARKET_CEX_INSTRUMENT_LIFECYCLE",
        "ADMIN_JOB_DIR",
        "TOKEN_REGISTRY_PATH",
        "ADMIN_USERNAME",
        "ADMIN_PASSWORD_HASH",
    ):
        environment.pop(name, None)
    environment.update({
        "DASHBOARD_SKIP_LOCAL_ENV": "true",
        "MARKET_DATA_DIR": str(root),
        "MARKET_ROUTE_DATA_DIR": str((root / "routes").resolve()),
        "ADMIN_ENABLED": "false",
        "ADMIN_LOGIN_REQUIRED": "true",
        "ADMIN_ALLOW_OPEN_LOCAL": "false",
        "PUBLIC_ADD_TOKEN_ENABLED": "false",
        "PUBLIC_QUALITY_RETRY_ENABLED": "false",
        "PUBLIC_FACT_REFRESH_ENABLED": "false",
        "TRUST_LOOPBACK_PROXY_CLIENT_IP": "false",
    })
    os.execve(
        sys.executable,
        [
            sys.executable,
            str(
                project_root
                / "scripts/run_current_opportunity_dashboard.py"
            ),
            "--data-dir", str(root),
            "--port", str(port),
        ],
        environment,
    )


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


def _load_dex_sources(
    *,
    root: Path,
    cohort: Mapping[str, Any],
    source_root: Path,
    now: str,
) -> tuple[
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
]:
    members_by_market = _dex_source_members_by_market(cohort)
    try:
        raw_members = _read_core_raw_members(
            root / "raw/route-cohort", cohort
        )
        source_path, source_fd, source_details = _open_verified_directory(
            source_root, "DEX typed-source root"
        )
    except (KeyError, OSError, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "retained DEX source evidence is invalid"
        ) from error

    sources: Dict[str, Dict[str, Any]] = {}
    retained_pool_members: Dict[str, Dict[str, Any]] = {}
    try:
        for leg in cohort["legs"]:
            market_id = leg["market_id"]
            descriptors = members_by_market[market_id]
            payloads: Dict[str, Dict[str, Any]] = {}
            payload_bytes: Dict[str, bytes] = {}
            full_descriptors: Dict[str, Dict[str, Any]] = {}
            for role in sorted(_DEX_TYPED_ROLES):
                descriptor = descriptors[role]
                payload, raw_bytes, digest = _read_member_from_root(
                    source_fd,
                    descriptor["filename"],
                    label="DEX " + role + " source",
                )
                if (
                    len(raw_bytes) != descriptor["size"]
                    or digest != descriptor["sha256"]
                ):
                    raise RouteOpportunityPipelineError(
                        "DEX typed-source bytes differ from lineage"
                    )
                payloads[role] = payload
                payload_bytes[role] = raw_bytes
                full_descriptors[role] = {
                    "market_id": market_id,
                    **descriptor,
                }

            accepted_raw_sha256 = raw_members[market_id][2]
            replayed = _validated_typed_payload_inventory(
                trusted_leg=leg,
                collector_row=leg,
                accepted_raw_sha256=accepted_raw_sha256,
                values=[
                    {"role": role, "payload": payload_bytes[role]}
                    for role in sorted(_DEX_TYPED_ROLES)
                ],
            )
            replayed_by_role = {row["role"]: row for row in replayed}
            if set(replayed_by_role) != _DEX_TYPED_ROLES:
                raise RouteOpportunityPipelineError(
                    "DEX typed-source replay inventory differs"
                )
            for role in sorted(_DEX_TYPED_ROLES):
                actual = replayed_by_role[role]
                descriptor = descriptors[role]
                if (
                    actual["market_id"] != market_id
                    or actual["payload"] != payload_bytes[role]
                    or actual["logical_generation"]
                    != descriptor["logical_generation"]
                    or actual["adapter_id"] != descriptor["adapter_id"]
                    or actual["content_schema"]
                    != descriptor["content_schema"]
                ):
                    raise RouteOpportunityPipelineError(
                        "DEX typed-source replay differs from lineage"
                    )

            context_payload = payloads["dex_usd_price_context"]
            context_view = _validate_route_collector_context(
                context_payload, market_id=market_id
            )
            if (
                context_payload != leg.get("collector_context")
                or payload_bytes["dex_usd_price_context"]
                != _canonical_json_bytes(context_payload)
            ):
                raise RouteOpportunityPipelineError(
                    "DEX price context differs from the core leg"
                )

            pool_payload = validate_retained_v2_pool_state_member(
                payload_bytes["dex_pool_state"],
                descriptor=full_descriptors["dex_pool_state"],
            )
            state = _frozen_v2_state(pool_payload)
            if (
                state.state_id != pool_payload["state_id"]
                or str(state.block_number) != leg.get("fixed_block_number")
                or state.observed_at != leg.get("state_observed_at")
                or exact_rfc3339_epoch_seconds(state.observed_at)
                != exact_rfc3339_epoch_seconds(
                    leg.get("fixed_block_timestamp")
                )
                or leg.get("snapshot_id")
                != cohort.get("raw_evidence_run_id")
            ):
                raise RouteOpportunityPipelineError(
                    "DEX pool state differs from the pinned core leg"
                )

            rules_payload = payloads["dex_market_rules"]
            rules_bytes = payload_bytes["dex_market_rules"]
            rules = MarketRules(
                market_id=rules_payload["market_id"],
                base_asset=rules_payload["base_asset"],
                quote_asset=rules_payload["quote_asset"],
                base_unit_decimals=rules_payload["base_unit_decimals"],
                quote_unit_decimals=rules_payload["quote_unit_decimals"],
                base_increment=Decimal(rules_payload["base_increment"]),
                quote_increment=Decimal(rules_payload["quote_increment"]),
                min_base_quantity=Decimal(
                    rules_payload["min_base_quantity"]
                ),
                min_quote_notional=Decimal(
                    rules_payload["min_quote_notional"]
                ),
                observed_at=rules_payload["observed_at"],
                valid_until=rules_payload["valid_until"],
                source_record_sha256=hashlib.sha256(rules_bytes).hexdigest(),
            )
            conversion = payloads["dex_usd_conversion"]
            evaluated = exact_rfc3339_epoch_seconds(now)
            if not (
                exact_rfc3339_epoch_seconds(rules.observed_at)
                <= evaluated
                < exact_rfc3339_epoch_seconds(rules.valid_until)
                and exact_rfc3339_epoch_seconds(conversion["observed_at"])
                <= evaluated
                < exact_rfc3339_epoch_seconds(conversion["valid_until"])
            ):
                raise RouteOpportunityPipelineError(
                    "DEX rules or USD conversion do not cover evaluation time"
                )
            reference_price = context_view["address_prices"].get(
                leg.get("target_token_address")
            )
            if reference_price is None:
                raise RouteOpportunityPipelineError(
                    "DEX target USD reference is unavailable"
                )
            retained_pool_members[market_id] = {
                "descriptor": full_descriptors["dex_pool_state"],
                "payload": payload_bytes["dex_pool_state"],
            }
            sources[market_id] = {
                "state": state,
                "pool_sha256": full_descriptors["dex_pool_state"]["sha256"],
                "rules": rules,
                "conversion": conversion,
                "usd_rate": Decimal(conversion["usd_per_quote"]),
                "usd_sha": hashlib.sha256(
                    payload_bytes["dex_usd_conversion"]
                ).hexdigest(),
                "reference_price_usd": Decimal(reference_price),
                "filenames": {
                    role: descriptors[role]["filename"]
                    for role in sorted(_DEX_TYPED_ROLES)
                },
            }
        _verify_open_path_identity(
            source_path, source_details, "DEX typed-source root"
        )
    except (
        KeyError,
        InvalidOperation,
        OSError,
        RouteCostEvidenceError,
        TypeError,
        ValueError,
    ) as error:
        if isinstance(error, RouteOpportunityPipelineError):
            raise
        raise RouteOpportunityPipelineError(
            "retained DEX source evidence is invalid"
        ) from error
    finally:
        os.close(source_fd)
    return sources, retained_pool_members


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


def _load_cex_sources(
    *,
    root: Path,
    cohort: Mapping[str, Any],
    source_root: Path,
    now: str,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
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
    return sources, legs_by_market


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
    sources, legs_by_market = _load_cex_sources(
        root=root,
        cohort=cohort,
        source_root=source_root,
        now=now,
    )

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


def _build_public_cex_research_inputs(
    *,
    data_dir: Path,
    cohort: Mapping[str, Any],
    core_manifest_sha256: str,
    public_fee_schedule_snapshot: _PublicFeeScheduleSnapshot,
) -> List[Dict[str, Any]]:
    """Replay one current CEX core into non-strict public research inputs."""
    root = Path(data_dir)
    try:
        cohort_now = cohort["collection_completed_at"]
        source_root = (
            root / "raw/route-cohort"
            / cohort["raw_evidence_run_id"] / "typed"
        )
        sources, legs_by_market = _load_cex_sources(
            root=root,
            cohort=cohort,
            source_root=source_root,
            now=cohort_now,
        )
    except (KeyError, OSError, TypeError, ValueError) as error:
        if isinstance(error, RouteOpportunityPipelineError):
            raise
        raise RouteOpportunityPipelineError(
            "public CEX source evidence is invalid"
        ) from error

    opportunities: List[Dict[str, Any]] = []
    for route in cohort["routes"]:
        try:
            buy_source = sources[route["buy_market_id"]]
            sell_source = sources[route["sell_market_id"]]
            buy_reference = (
                buy_source["book"]["asks"][0][0]
                * buy_source["usd_rate"]
            )
            sell_reference = (
                sell_source["book"]["bids"][0][0]
                * sell_source["usd_rate"]
            )
            for raw_notional in cohort["requested_notionals_usd"]:
                notional = Decimal(str(raw_notional))
                target = common_target_quantity(
                    requested_notional_usd=notional,
                    buy_reference_price_usd=buy_reference,
                    sell_reference_price_usd=sell_reference,
                    buy_market_rules=buy_source["rules"],
                    sell_market_rules=sell_source["rules"],
                )
                opportunity_id = route_opportunity_id(
                    route["route_id"], notional
                )
                costs = [
                    _collect_cex_fee_snapshot_from_schedule_snapshot(
                        cohort_id=cohort["route_cohort_id"],
                        opportunity_id=opportunity_id,
                        leg=direction,
                        market_id=route[direction + "_market_id"],
                        venue=route[direction + "_market_id"].split(":", 2)[1],
                        instrument=route[direction + "_market_id"].split(":", 2)[2],
                        side=direction,
                        requested_notional_usd=notional,
                        target_token_quantity=target.quantity,
                        now=cohort_now,
                        public_schedule_snapshot=public_fee_schedule_snapshot,
                    )
                    for direction in ("buy", "sell")
                ]
                fee_components = {
                    row["leg"]: row for row in costs
                }
                fees = {
                    direction: public_fee_semantics(
                        fee_components[direction],
                        direction=direction,
                        rules=sources[
                            route[direction + "_market_id"]
                        ]["rules"],
                        now=cohort_now,
                    )
                    for direction in ("buy", "sell")
                }

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

                costs.append(cost_component_row(
                    cohort_id=cohort["route_cohort_id"],
                    opportunity_id=opportunity_id,
                    leg="route",
                    market_id="",
                    direction="route",
                    requested_notional_usd=notional,
                    target_token_quantity=target.quantity,
                    component_type="rebalancing_or_transfer",
                    value_status="assumed",
                    amount_usd=Decimal(0),
                    rate_bps=Decimal(0),
                    basis=(
                        "zero immediate transfer cost is a public research "
                        "scenario assumption; no account inventory was observed"
                    ),
                    strict_eligible=False,
                    observed_at=None,
                    valid_until=None,
                    source=(
                        "public research scenario without account inventory"
                    ),
                    source_record_sha256=None,
                    reason_code="inventory_not_observed_for_public_research",
                ))
                mode = classify_route_mode_evidence(route, now=cohort_now)
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
                    "now": cohort_now,
                }
                classified = build_route_opportunity(**build_inputs)
                if (
                    classified.get("opportunity_class")
                    not in {"research_estimate", "unavailable"}
                    or classified.get("strict_eligible") is not False
                    or classified.get("strict_ready_for_publication") is not False
                    or classified.get("publication_attestation_sha256") is not None
                ):
                    raise RouteOpportunityPipelineError(
                        "public CEX result exceeded its research claim boundary"
                    )
                opportunities.append({
                    "classified_opportunity": classified,
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
                "public CEX opportunity inputs cannot be reconstructed"
            ) from error
    return opportunities


def _terminal_dex_cost_components(
    *,
    cohort_id: str,
    route: Mapping[str, Any],
    requested_notional_usd: Decimal,
    target_token_quantity: Decimal,
    coverage: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    status = coverage.get("status")
    reason = coverage.get("reason_code")
    if (
        status not in {"unavailable", "failed"}
        or not isinstance(reason, str)
        or not reason
        or coverage.get("coverage_kind")
        not in {"terminal_scope_replay", "binding"}
    ):
        raise RouteOpportunityPipelineError(
            "DEX route-cost coverage is not a terminal replay"
        )
    value_status = "failed" if status == "failed" else "unavailable"
    opportunity_id = route_opportunity_id(
        route["route_id"], requested_notional_usd
    )
    rows: List[Dict[str, Any]] = []
    for leg, component_type in sorted(
        live_complete_cost_component_keys(route)
    ):
        market_id = "" if leg == "route" else route[leg + "_market_id"]
        rows.append(cost_component_row(
            cohort_id=cohort_id,
            opportunity_id=opportunity_id,
            leg=leg,
            market_id=market_id,
            direction=("route" if leg == "route" else leg + "_token"),
            requested_notional_usd=requested_notional_usd,
            target_token_quantity=target_token_quantity,
            component_type=component_type,
            value_status=value_status,
            amount_usd=None,
            rate_bps=None,
            basis="pinned route-cost coverage did not establish this component",
            strict_eligible=False,
            observed_at=None,
            valid_until=None,
            source="pinned route-cost evidence",
            source_record_sha256=coverage[
                "route_cost_evidence_sha256"
            ],
            reason_code=reason,
        ))
    return rows


def _canonical_research_mev_bps(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 32
        or _RESEARCH_MEV_BPS.fullmatch(value) is None
    ):
        raise RouteOpportunityPipelineError(
            "research MEV bps must be canonical decimal text"
        )
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "research MEV bps must be canonical decimal text"
        ) from error
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if number == 0:
        canonical = "0"
    if (
        not number.is_finite()
        or number < 0
        or number > 10000
        or canonical != value
        or number.as_tuple().exponent < -6
    ):
        raise RouteOpportunityPipelineError(
            "research MEV bps must be canonical decimal text from 0 to 10000 "
            "with at most 6 decimal places"
        )
    return canonical


def _research_mev_bps_argument(value: str) -> str:
    try:
        return _canonical_research_mev_bps(value)
    except RouteOpportunityPipelineError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _dex_cost_projection_index(
    cost_evidence: Mapping[str, Any],
    outcomes: List[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Freeze hash-addressed views of one replay-validated cost sidecar."""
    try:
        sidecar_sha256 = physical_sha256(cost_evidence)
        if {
            row.get("route_cost_evidence_sha256") for row in outcomes
            if isinstance(row, Mapping)
        } != {sidecar_sha256}:
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost sidecar hash differs after replay"
            )
        bindings = cost_evidence.get("bindings")
        transcripts = cost_evidence.get("transcripts")
        if not isinstance(bindings, list) or not isinstance(transcripts, list):
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost projection inventory is invalid"
            )
        binding_by_sha: Dict[str, Mapping[str, Any]] = {}
        for binding in bindings:
            if not isinstance(binding, Mapping):
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost binding is invalid"
                )
            digest = typed_sha256(
                b"route-cost-evidence-binding/v1\n", binding
            )
            if digest in binding_by_sha:
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost binding hash is duplicated"
                )
            binding_by_sha[digest] = binding
        transcript_by_sha: Dict[str, Mapping[str, Any]] = {}
        for transcript in transcripts:
            if not isinstance(transcript, Mapping):
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost transcript is invalid"
                )
            digest = typed_sha256(
                b"route-cost-evidence-transcript/v1\n", transcript
            )
            if digest in transcript_by_sha:
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost transcript hash is duplicated"
                )
            transcript_by_sha[digest] = transcript
        return {
            "sidecar_sha256": sidecar_sha256,
            "binding_by_sha": binding_by_sha,
            "transcript_by_sha": transcript_by_sha,
        }
    except (
        RouteCostEvidenceError,
        TypeError,
        ValueError,
    ) as error:
        if isinstance(error, RouteOpportunityPipelineError):
            raise
        raise RouteOpportunityPipelineError(
            "DEX route-cost projection index is invalid"
        ) from error


def _resolved_observed_dex_cost_evidence(
    *,
    route: Mapping[str, Any],
    requested_notional_usd: Decimal,
    coverage: Mapping[str, Any],
    projection_index: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Resolve one exact observed binding and its shared transaction target."""
    try:
        if projection_index.get("sidecar_sha256") != coverage.get(
            "route_cost_evidence_sha256"
        ):
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost sidecar hash differs after replay"
            )
        if coverage.get("coverage_kind") != "binding":
            return None
        expected_markets = sorted((
            route.get("buy_market_id"), route.get("sell_market_id")
        ))
        if (
            coverage.get("status")
            not in {"observed", "unavailable", "failed"}
            or coverage.get("covered_dex_market_ids") != expected_markets
            or coverage.get("uncovered_dex_market_ids") != []
        ):
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost binding coverage differs"
            )
        binding_sha = coverage.get("scoped_binding_sha256")
        if not isinstance(binding_sha, str):
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost binding hash is missing"
            )
        binding_by_sha = projection_index.get("binding_by_sha")
        transcript_by_sha = projection_index.get("transcript_by_sha")
        if not isinstance(binding_by_sha, Mapping) or not isinstance(
            transcript_by_sha, Mapping
        ):
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost projection inventory is invalid"
            )
        binding = binding_by_sha.get(binding_sha)
        if not isinstance(binding, Mapping):
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost binding does not resolve exactly"
            )
        notional_text = format(requested_notional_usd, "f")
        if (
            binding.get("route_id") != route.get("route_id")
            or binding.get("requested_notional_usd") != notional_text
            or binding.get("status") != coverage.get("status")
            or binding.get("reason_code") != coverage.get("reason_code")
        ):
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost binding scenario differs"
            )
        resolved: Dict[str, Tuple[Mapping[str, Any], str]] = {}
        shared_target: Optional[Dict[str, str]] = None
        for leg in ("buy", "sell"):
            market_id = route.get(leg + "_market_id")
            source = sources.get(market_id)
            if not isinstance(source, Mapping):
                raise RouteOpportunityPipelineError(
                    "DEX cost projection source is missing"
                )
            state = source.get("state")
            conversion = source.get("conversion")
            pool_sha256 = source.get("pool_sha256")
            if (
                state is None
                or not isinstance(conversion, Mapping)
                or not isinstance(pool_sha256, str)
            ):
                raise RouteOpportunityPipelineError(
                    "DEX cost projection source is incomplete"
                )
            transcript_sha = binding.get(leg + "_transcript_sha256")
            transcript = transcript_by_sha.get(transcript_sha)
            if transcript is None:
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost transcript does not resolve"
                )
            if (
                transcript.get("market_id") != market_id
                or transcript.get("direction") != leg
                or transcript.get("requested_notional_usd") != notional_text
            ):
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost transcript scenario differs"
                )
            if transcript.get("status") != "observed":
                return None
            target_address = conversion.get("target_token_address")
            target_value = {
                "schema": "route_cost_simulation_target/v1",
                "token_address": target_address,
                "unit_decimals": transcript.get(
                    "simulation_target_unit_decimals"
                ),
                "raw_quantity": transcript.get(
                    "simulation_target_raw_quantity"
                ),
                "lattice_raw": transcript.get(
                    "simulation_target_lattice_raw"
                ),
            }
            if any((
                transcript.get("simulation_target_token_address")
                != target_address,
                transcript.get("simulation_target_sha256")
                != typed_sha256(
                    b"route-cost-simulation-target/v1\n", target_value
                ),
                transcript.get("core_pool_state_id") != state.state_id,
                transcript.get("core_pool_state_sha256") != pool_sha256,
            )):
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost transcript target or state differs"
                )
            if not all(
                isinstance(target_value[field], str)
                and target_value[field]
                and target_value[field].isdigit()
                and str(int(target_value[field])) == target_value[field]
                for field in (
                    "unit_decimals", "raw_quantity", "lattice_raw"
                )
            ):
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost transcript target is invalid"
                )
            comparable_target = dict(target_value)
            if shared_target is None:
                shared_target = comparable_target
            elif comparable_target != shared_target:
                raise RouteOpportunityPipelineError(
                    "pinned DEX route-cost leg targets differ"
                )
            if (
                transcript.get("completed_stage") != "transfer_tax"
                or transcript.get("reason_code") is not None
            ):
                raise RouteOpportunityPipelineError(
                    "observed DEX route-cost transcript is not terminal"
                )
            gas = transcript.get("gas_evidence")
            router = transcript.get("router_fee_evidence")
            transfer = transcript.get("transfer_tax_evidence")
            if (
                not isinstance(gas, Mapping)
                or not isinstance(router, Mapping)
                or not isinstance(transfer, Mapping)
                or router.get("status") != "not_applicable"
                or transfer.get("status") != "not_applicable"
            ):
                raise RouteOpportunityPipelineError(
                    "observed DEX cost-component evidence is incomplete"
                )
            resolved[leg] = (transcript, str(transcript_sha))
        if shared_target is None:
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost target is absent"
            )
        target = CommonTarget(
            asset=route["token_symbol"],
            unit_decimals=int(shared_target["unit_decimals"]),
            raw_quantity=int(shared_target["raw_quantity"]),
            lattice_raw=int(shared_target["lattice_raw"]),
        )
        return {"target": target, "legs": resolved}
    except (
        KeyError,
        InvalidOperation,
        RouteCostEvidenceError,
        TypeError,
        ValueError,
    ) as error:
        if isinstance(error, RouteOpportunityPipelineError):
            raise
        raise RouteOpportunityPipelineError(
            "observed DEX route-cost evidence cannot be resolved"
        ) from error


def _observed_dex_cost_components(
    *,
    cohort_id: str,
    route: Mapping[str, Any],
    requested_notional_usd: Decimal,
    common_target: CommonTarget,
    coverage: Mapping[str, Any],
    projection_index: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
    research_mev_bps: Optional[str],
) -> Optional[List[Dict[str, Any]]]:
    """Project one resolved V2 binding into the exact ten cost rows."""
    try:
        context = _resolved_observed_dex_cost_evidence(
            route=route,
            requested_notional_usd=requested_notional_usd,
            coverage=coverage,
            projection_index=projection_index,
            sources=sources,
        )
        if context is None:
            return None
        if context["target"] != common_target:
            raise RouteOpportunityPipelineError(
                "pinned DEX route-cost transcript target or state differs"
            )
        resolved = context["legs"]

        opportunity_id = route_opportunity_id(
            route["route_id"], requested_notional_usd
        )
        rows: List[Dict[str, Any]] = []
        for leg in ("buy", "sell"):
            market_id = route[leg + "_market_id"]
            state = sources[market_id]["state"]
            transcript, transcript_sha = resolved[leg]
            gas = transcript["gas_evidence"]
            router = transcript["router_fee_evidence"]
            transfer = transcript["transfer_tax_evidence"]
            gas_amount = network_gas_usd(
                gas_units=int(gas["gas_units"]),
                max_fee_per_gas_wei_value=int(
                    gas["max_fee_per_gas_wei"]
                ),
                native_price_usd=gas["native_price_usd"],
            )
            gas_rate = (
                Decimal(gas_amount) * Decimal(10000)
                / requested_notional_usd
            )
            fee_rate = Decimal(state.fee_bps)
            fee_amount = (
                requested_notional_usd * fee_rate / Decimal(10000)
            )
            common = {
                "cohort_id": cohort_id,
                "opportunity_id": opportunity_id,
                "leg": leg,
                "market_id": market_id,
                "direction": leg + "_token",
                "requested_notional_usd": requested_notional_usd,
                "target_token_quantity": common_target.quantity,
            }
            rows.extend((
                cost_component_row(
                    **common,
                    component_type="pool_swap_fee",
                    value_status="measured",
                    amount_usd=fee_amount,
                    rate_bps=fee_rate,
                    basis=(
                        "retained Uniswap V2 fee rate; informational "
                        "notional equivalent already embedded in the quote"
                    ),
                    strict_eligible=True,
                    embedded_in_leg_quote=True,
                    observed_at=state.observed_at,
                    valid_until=None,
                    source="retained Uniswap V2 pool state",
                    source_record_sha256=state.fee_proof_sha256,
                ),
                cost_component_row(
                    **common,
                    component_type="network_gas",
                    value_status="quoted",
                    amount_usd=gas_amount,
                    rate_bps=gas_rate,
                    basis=(
                        "fixed-block gas units times max fee per gas times "
                        "pinned native-token USD price"
                    ),
                    strict_eligible=True,
                    observed_at=gas["observed_at"],
                    valid_until=gas["valid_until"],
                    source="pinned route-cost gas transcript",
                    source_record_sha256=transcript_sha,
                ),
                cost_component_row(
                    **common,
                    component_type="router_or_integrator_fee",
                    value_status="not_applicable",
                    amount_usd=None,
                    rate_bps=None,
                    basis="validated Uniswap V2 Router02 adapter has no integrator fee",
                    strict_eligible=True,
                    observed_at=None,
                    valid_until=None,
                    source="validated route adapter contract",
                    source_record_sha256=router["source_record_sha256"],
                ),
                cost_component_row(
                    **common,
                    component_type="token_transfer_tax",
                    value_status="not_applicable",
                    amount_usd=None,
                    rate_bps=None,
                    basis="validated trace balance deltas prove no transfer tax",
                    strict_eligible=True,
                    observed_at=None,
                    valid_until=None,
                    source="validated route adapter contract",
                    source_record_sha256=transfer["trace_sha256"],
                ),
            ))

        route_common = {
            "cohort_id": cohort_id,
            "opportunity_id": opportunity_id,
            "leg": "route",
            "market_id": "",
            "direction": "route",
            "requested_notional_usd": requested_notional_usd,
            "target_token_quantity": common_target.quantity,
        }
        rows.append(cost_component_row(
            **route_common,
            component_type="rebalancing_or_transfer",
            value_status="not_applicable",
            amount_usd=None,
            rate_bps=None,
            basis="same-chain atomic route proves no external rebalance leg",
            strict_eligible=True,
            observed_at=None,
            valid_until=None,
            source="validated route topology",
            source_record_sha256=None,
        ))
        if research_mev_bps is None:
            rows.append(cost_component_row(
                **route_common,
                component_type="mev_buffer",
                value_status="unavailable",
                amount_usd=None,
                rate_bps=None,
                basis="validated sidecar contains no independent MEV estimate",
                strict_eligible=False,
                observed_at=None,
                valid_until=None,
                source="pinned route-cost evidence",
                source_record_sha256=coverage[
                    "route_cost_evidence_sha256"
                ],
                reason_code="mev_protection_unavailable",
            ))
        else:
            mev_text = _canonical_research_mev_bps(research_mev_bps)
            mev_rate = Decimal(mev_text)
            rows.append(cost_component_row(
                **route_common,
                component_type="mev_buffer",
                value_status="assumed",
                amount_usd=(
                    requested_notional_usd * mev_rate / Decimal(10000)
                ),
                rate_bps=mev_rate,
                basis=(
                    "explicit operator research scenario of {} bps; not "
                    "derived from submission-loss bounds"
                ).format(mev_text),
                strict_eligible=False,
                observed_at=None,
                valid_until=None,
                source="explicit operator research scenario",
                source_record_sha256=None,
            ))
        if {
            (row["leg"], row["component_type"]) for row in rows
        } != set(live_complete_cost_component_keys(route)):
            raise RouteOpportunityPipelineError(
                "DEX cost projection topology is incomplete"
            )
        return rows
    except (
        KeyError,
        InvalidOperation,
        RouteCostEvidenceError,
        TypeError,
        ValueError,
    ) as error:
        if isinstance(error, RouteOpportunityPipelineError):
            raise
        raise RouteOpportunityPipelineError(
            "observed DEX route-cost evidence cannot be projected"
        ) from error


def _build_dex_inputs(
    *,
    cohort: Mapping[str, Any],
    core_manifest_sha256: str,
    sources: Mapping[str, Mapping[str, Any]],
    outcomes: List[Mapping[str, Any]],
    cost_evidence: Mapping[str, Any],
    research_mev_bps: Optional[str],
    now: str,
) -> List[Dict[str, Any]]:
    legs_by_market = {
        row["market_id"]: row for row in cohort["legs"]
    }
    outcome_by_key = {
        (row.get("route_id"), str(row.get("requested_notional_usd"))): row
        for row in outcomes
        if isinstance(row, Mapping)
    }
    expected_keys = {
        (route["route_id"], str(notional))
        for route in cohort["routes"]
        for notional in cohort["requested_notionals_usd"]
    }
    if (
        len(outcome_by_key) != len(outcomes)
        or set(outcome_by_key) != expected_keys
    ):
        raise RouteOpportunityPipelineError(
            "DEX route-cost coverage scenario inventory differs"
        )
    if research_mev_bps is not None:
        research_mev_bps = _canonical_research_mev_bps(research_mev_bps)
    projection_index = _dex_cost_projection_index(cost_evidence, outcomes)

    opportunities: List[Dict[str, Any]] = []
    cohort_now = cohort["collection_completed_at"]
    for route in cohort["routes"]:
        buy_source = sources[route["buy_market_id"]]
        sell_source = sources[route["sell_market_id"]]
        for raw_notional in cohort["requested_notionals_usd"]:
            notional = Decimal(str(raw_notional))
            try:
                coverage = outcome_by_key[(
                    route["route_id"], str(raw_notional)
                )]
                observed_cost = _resolved_observed_dex_cost_evidence(
                    route=route,
                    requested_notional_usd=notional,
                    coverage=coverage,
                    projection_index=projection_index,
                    sources=sources,
                )
                target = (
                    observed_cost["target"]
                    if observed_cost is not None
                    else common_target_quantity(
                        requested_notional_usd=notional,
                        buy_reference_price_usd=buy_source[
                            "reference_price_usd"
                        ],
                        sell_reference_price_usd=sell_source[
                            "reference_price_usd"
                        ],
                        buy_market_rules=buy_source["rules"],
                        sell_market_rules=sell_source["rules"],
                    )
                )
                quotes: Dict[str, Any] = {}
                quote_evidence: Dict[str, Dict[str, Any]] = {}
                projections: Dict[str, Optional[Dict[str, Any]]] = {}
                build_legs: Dict[str, Dict[str, Any]] = {}
                source_members: Dict[str, str] = {}
                for direction in ("buy", "sell"):
                    market_id = route[direction + "_market_id"]
                    source = sources[market_id]
                    leg = legs_by_market[market_id]
                    rules = source["rules"]
                    state = source["state"]
                    quote = quote_v2_pool_quantity(
                        state,
                        target,
                        rules,
                        direction=direction,
                        target_token_address=source["conversion"][
                            "target_token_address"
                        ],
                        quote_token_address=source["conversion"][
                            "quote_token_address"
                        ],
                        cohort_now=cohort_now,
                        snapshot_id=leg["snapshot_id"],
                    )
                    quotes[direction] = quote
                    build_legs[direction] = {
                        **dict(leg),
                        "state_id": quote.state_id,
                    }
                    quote_evidence[direction] = {
                        "kind": "dex_v2",
                        "pool_state": state,
                        "market_rules": rules,
                        "target_token_address": source["conversion"][
                            "target_token_address"
                        ],
                        "quote_token_address": source["conversion"][
                            "quote_token_address"
                        ],
                        "cohort_now": cohort_now,
                        "snapshot_id": leg["snapshot_id"],
                        "assurance_status": "route_bundle_validated",
                        "core_manifest_sha256": core_manifest_sha256,
                    }
                    cash = (
                        quote.quote_debit_quantity
                        if direction == "buy"
                        else quote.quote_received_quantity
                    )
                    conversion = source["conversion"]
                    projections[direction] = (
                        None
                        if cash is None
                        else usd_projection_evidence(
                            market_id=market_id,
                            state_id=quote.state_id,
                            direction=direction,
                            quote_asset=conversion["quote_asset"],
                            quote_cash_quantity=cash,
                            usd_per_quote=source["usd_rate"],
                            value_status="measured",
                            observed_at=conversion["observed_at"],
                            valid_until=conversion["valid_until"],
                            source=conversion["source"],
                            source_record_sha256=source["usd_sha"],
                            core_manifest_sha256=core_manifest_sha256,
                        )
                    )
                    source_members.update({
                        "{}_{}".format(direction, role): filename
                        for role, filename in source["filenames"].items()
                    })

                costs = _observed_dex_cost_components(
                    cohort_id=cohort["route_cohort_id"],
                    route=route,
                    requested_notional_usd=notional,
                    common_target=target,
                    coverage=coverage,
                    projection_index=projection_index,
                    sources=sources,
                    research_mev_bps=research_mev_bps,
                )
                if costs is None:
                    costs = _terminal_dex_cost_components(
                        cohort_id=cohort["route_cohort_id"],
                        route=route,
                        requested_notional_usd=notional,
                        target_token_quantity=target.quantity,
                        coverage=coverage,
                    )
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
                classified = build_route_opportunity(**build_inputs)
                if (
                    classified.get("opportunity_class")
                    not in {"research_estimate", "unavailable"}
                    or classified.get("strict_eligible") is not False
                    or classified.get("strict_ready_for_publication") is not False
                    or classified.get("publication_attestation_sha256")
                    is not None
                ):
                    raise RouteOpportunityPipelineError(
                        "DEX research finalizer attempted a strict upgrade"
                    )
                opportunities.append({
                    "classified_opportunity": classified,
                    "build_inputs": build_inputs,
                    "source_members": source_members,
                })
            except (
                KeyError,
                InvalidOperation,
                TypeError,
                ValueError,
            ) as error:
                if isinstance(error, RouteOpportunityPipelineError):
                    raise
                raise RouteOpportunityPipelineError(
                    "DEX opportunity inputs cannot be reconstructed"
                ) from error
    return opportunities


def finalize_public_cex_research_opportunities(
    *,
    data_dir: Path,
    public_fee_schedule_path: Path,
    expected_route_cohort_id: str,
    expected_core_manifest_sha256: str,
    _postcommit_validator: Optional[
        Callable[[Mapping[str, Any]], None]
    ] = None,
) -> Dict[str, Any]:
    """Finalize the current CEX core as public, non-strict research."""
    if (
        _postcommit_validator is not None
        and not callable(_postcommit_validator)
    ):
        raise RouteOpportunityPipelineError(
            "public CEX postcommit validator is invalid"
        )
    root = Path(data_dir)
    schedule_path = Path(public_fee_schedule_path)
    try:
        current_core = load_latest_route_cohort(root / "routes/core")
    except (OSError, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "current public CEX inputs are invalid"
        ) from error
    cohort = current_core.get("cohort")
    core_pointer = current_core.get("pointer")
    core_manifest_sha256 = current_core.get("manifest_sha256")
    if (
        not isinstance(cohort, dict)
        or not isinstance(core_pointer, dict)
        or not isinstance(core_manifest_sha256, str)
        or core_pointer.get("manifest_sha256") != core_manifest_sha256
    ):
        raise RouteOpportunityPipelineError(
            "current public CEX core identity is invalid"
        )
    if (
        cohort.get("route_cohort_id") != expected_route_cohort_id
        or core_manifest_sha256 != expected_core_manifest_sha256
    ):
        raise RouteOpportunityPipelineError(
            "current public CEX core differs from expected published identity"
        )
    legs = cohort.get("legs")
    routes = cohort.get("routes")
    if not isinstance(legs, list) or not isinstance(routes, list):
        raise RouteOpportunityPipelineError(
            "current public CEX core inventory is invalid"
        )
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
            "public research finalization is CEX-only"
        )
    fixed_universe = build_live_cex_research_universe()
    expected_leg_ids = {
        leg["market_id"] for leg in fixed_universe["selected_legs"]
    }
    actual_leg_ids = set(leg_types)
    actual_leg_tokens = {
        leg.get("market_id"): leg.get("token_symbol")
        for leg in legs if isinstance(leg, dict)
    }
    if (
        cohort.get("candidate_source_generation")
        != live_cex_research_generation()
        or cohort.get("selection_window")
        != fixed_universe["selection_window"]
        or cohort.get("requested_notionals_usd")
        != fixed_universe["requested_notionals_usd"]
        or routes != fixed_universe["routes"]
        or actual_leg_ids != expected_leg_ids
        or set(actual_leg_tokens.values()) != {"UNI"}
    ):
        raise RouteOpportunityPipelineError(
            "current public CEX core is outside the fixed UNI/USDT "
            "Binance/Bybit research universe"
        )

    try:
        schedule_snapshot = _load_public_fee_schedule_snapshot(
            schedule_path,
            now=cohort["collection_completed_at"],
        )
    except (KeyError, OSError, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "current public CEX inputs are invalid"
        ) from error

    inputs = _build_public_cex_research_inputs(
        data_dir=root,
        cohort=cohort,
        core_manifest_sha256=core_manifest_sha256,
        public_fee_schedule_snapshot=schedule_snapshot,
    )
    source_root = (
        root / "raw/route-cohort"
        / cohort["raw_evidence_run_id"] / "typed"
    )

    def validate_public_inputs() -> None:
        try:
            confirmed_schedule = _load_public_fee_schedule_snapshot(
                schedule_path,
                now=cohort["collection_completed_at"],
            )
            confirmed_core = load_latest_route_cohort(root / "routes/core")
        except (OSError, TypeError, ValueError) as error:
            raise RouteOpportunityPipelineError(
                "public CEX inputs changed before pointer commit"
            ) from error
        if (
            confirmed_schedule != schedule_snapshot
            or confirmed_core.get("pointer") != core_pointer
            or confirmed_core.get("manifest_sha256")
            != core_manifest_sha256
            or confirmed_core.get("cohort") != cohort
        ):
            raise RouteOpportunityPipelineError(
                "public CEX inputs changed before pointer commit"
            )

    validate_public_inputs()
    cold_loaded: Dict[str, Any] = {}

    def validate_committed_public_pointer(
        committed_pointer: Mapping[str, Any],
    ) -> None:
        loaded = load_latest_complete_route_bundle(
            root / "routes",
            core_root=root / "routes/core",
        )
        if loaded.get("pointer") != committed_pointer:
            raise RouteOpportunityPipelineError(
                "published public CEX pointer failed cold reload"
            )
        cold_loaded["pointer"] = loaded["pointer"]
        if _postcommit_validator is not None:
            _postcommit_validator(committed_pointer)

    try:
        pointer = publish_complete_route_bundle(
            core_root=root / "routes/core",
            routes_root=root / "routes",
            raw_root=root / "raw/route-cohort",
            opportunity_inputs=inputs,
            source_root=source_root,
            precommit_validator=validate_public_inputs,
            postcommit_validator=validate_committed_public_pointer,
        )
    except (OSError, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "public CEX opportunity publication failed"
        ) from error
    if cold_loaded.get("pointer") != pointer:
        raise RouteOpportunityPipelineError(
            "published public CEX pointer failed cold reload"
        )
    return pointer


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


def finalize_eth_uniswap_v2_research_opportunities(
    *,
    data_dir: Path,
    shadow_run_id: str,
    expected_joint_pointer_sha256: str,
    research_mev_bps: Optional[str] = None,
) -> Dict[str, Any]:
    """Replay one pinned DEX-only Shadow run without strict upgrading."""
    if research_mev_bps is not None:
        research_mev_bps = _canonical_research_mev_bps(research_mev_bps)
    root = Path(data_dir)
    try:
        shadow = load_shadow_result(
            root / "routes/shadow",
            run_id=shadow_run_id,
            expected_pointer_sha256=expected_joint_pointer_sha256,
        )
        latest_core = load_latest_route_cohort(root / "routes/core")
    except (TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "pinned DEX route Shadow evidence is invalid"
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
            "pinned DEX Shadow run and latest core lineage differ"
        )

    legs = cohort.get("legs")
    routes = cohort.get("routes")
    if (
        not isinstance(legs, list)
        or not legs
        or not isinstance(routes, list)
        or not routes
    ):
        raise RouteOpportunityPipelineError(
            "Ethereum Uniswap V2 core inventory is invalid"
        )
    legs_by_market = {
        leg.get("market_id"): leg
        for leg in legs
        if isinstance(leg, Mapping)
    }
    if len(legs_by_market) != len(legs):
        raise RouteOpportunityPipelineError(
            "Ethereum Uniswap V2 core inventory is invalid"
        )
    for market_id, leg in legs_by_market.items():
        parts = market_id.split(":") if isinstance(market_id, str) else []
        if (
            len(parts) != 5
            or parts[:3] != ["dex", "eth", "uniswap_v2"]
            or leg.get("market_type") != "dex"
            or leg.get("status") != "observed"
            or leg.get("available") is not True
        ):
            raise RouteOpportunityPipelineError(
                "finalizer requires observed Ethereum Uniswap V2 legs"
            )
    if any(
        not isinstance(route, Mapping)
        or route.get("route_mode") != "atomic_onchain"
        or route.get("route_class") != "candidate"
        or route.get("settlement_reason") is not None
        or route.get("buy_market_id") not in legs_by_market
        or route.get("sell_market_id") not in legs_by_market
        for route in routes
    ):
        raise RouteOpportunityPipelineError(
            "finalizer requires same-chain atomic Uniswap V2 routes"
        )

    try:
        evidence = _read_shadow_run_evidence(
            root / "routes/shadow", shadow_run_id
        )
    except (TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "pinned DEX route-cost evidence is invalid"
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
            "pinned DEX route-cost sidecar hash differs"
        )
    now = cost_evidence.get("evaluated_at")
    if not isinstance(now, str):
        raise RouteOpportunityPipelineError(
            "DEX route-cost evaluation time is missing"
        )
    source_root = root / "raw/route-cohort" / shadow_run_id / "typed"
    sources, retained_pool_members = _load_dex_sources(
        root=root,
        cohort=cohort,
        source_root=source_root,
        now=now,
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
            retained_typed_pool_state_members=(
                _cost_supported_pool_members(
                    cost_evidence, retained_pool_members
                )
            ),
        )
    except (
        KeyError,
        RouteCostEvidenceError,
        TypeError,
        ValueError,
    ) as error:
        if isinstance(error, RouteOpportunityPipelineError):
            raise
        raise RouteOpportunityPipelineError(
            "pinned DEX route-cost evidence replay failed"
        ) from error
    inputs = _build_dex_inputs(
        cohort=cohort,
        core_manifest_sha256=pointer["core_manifest_sha256"],
        sources=sources,
        outcomes=outcomes,
        cost_evidence=cost_evidence,
        research_mev_bps=research_mev_bps,
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
                "pinned DEX Shadow evidence changed during reconstruction"
            ) from error
        if confirmed_shadow != shadow:
            raise RouteOpportunityPipelineError(
                "pinned DEX Shadow evidence changed during reconstruction"
            )

    validate_pinned_shadow()
    try:
        return publish_complete_route_bundle(
            core_root=root / "routes/core",
            routes_root=root / "routes",
            raw_root=root / "raw/route-cohort",
            opportunity_inputs=inputs,
            source_root=source_root,
            precommit_validator=validate_pinned_shadow,
        )
    except (TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "DEX research opportunity publication failed"
        ) from error


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Finalize one explicitly pinned route Shadow run."
    )
    parser.add_argument(
        "--finalizer",
        choices=("cex", "eth-uniswap-v2-research"),
        default="cex",
        help=(
            "pinned evidence workflow to publish (default: cex)"
        ),
    )
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--shadow-run-id", required=True)
    parser.add_argument("--expected-joint-pointer-sha256", required=True)
    parser.add_argument(
        "--serve",
        action="store_true",
        help=(
            "after publication verification, run the read-only Current "
            "Opportunity dashboard on 127.0.0.1"
        ),
    )
    parser.add_argument(
        "--port",
        type=_loopback_port,
        default=8765,
        help="loopback dashboard port used with --serve (default: 8765)",
    )
    parser.add_argument(
        "--research-mev-bps",
        type=_research_mev_bps_argument,
        help=(
            "explicit nonnegative MEV cost assumption for the DEX research "
            "scenario; no value is inferred when omitted"
        ),
    )
    arguments = parser.parse_args(argv)
    if (
        arguments.finalizer != "eth-uniswap-v2-research"
        and arguments.research_mev_bps is not None
    ):
        parser.error(
            "--research-mev-bps is only valid with "
            "--finalizer eth-uniswap-v2-research"
        )
    finalizer = (
        finalize_cex_route_opportunities
        if arguments.finalizer == "cex"
        else finalize_eth_uniswap_v2_research_opportunities
    )
    finalizer_kwargs = {
        "data_dir": arguments.data_dir,
        "shadow_run_id": arguments.shadow_run_id,
        "expected_joint_pointer_sha256": (
            arguments.expected_joint_pointer_sha256
        ),
    }
    if arguments.finalizer == "eth-uniswap-v2-research":
        finalizer_kwargs["research_mev_bps"] = arguments.research_mev_bps
    pointer = finalizer(
        **finalizer_kwargs
    )
    try:
        loaded = load_latest_complete_route_bundle(
            Path(arguments.data_dir) / "routes",
            core_root=Path(arguments.data_dir) / "routes/core",
        )
    except (OSError, TypeError, ValueError) as error:
        raise RouteOpportunityPipelineError(
            "published complete route bundle cannot be reloaded"
        ) from error
    if loaded.get("pointer") != pointer:
        raise RouteOpportunityPipelineError(
            "published complete route bundle cannot be reloaded"
        )
    print(
        json.dumps(pointer, ensure_ascii=False, sort_keys=True),
        flush=arguments.serve,
    )
    if arguments.serve:
        _exec_read_only_dashboard(
            data_dir=arguments.data_dir,
            port=arguments.port,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
