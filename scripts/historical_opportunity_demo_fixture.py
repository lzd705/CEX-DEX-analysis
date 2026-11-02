"""Honest, disposable data source for the local Opportunity workflow demo."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import tempfile
import threading
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


DEMO_CONTRACT = "opportunity_historical_demo_summary/v1"
DEMO_EVIDENCE_MODE = "offline_test_fixture"
DEMO_VERIFICATION_STATUS = "structurally_validated"
DEMO_VALIDATION_CONTRACT = "opportunity_historical_demo_validation/v1"
DEMO_TEMPORAL_SCOPE = "historical_demo_fixture"
DEMO_EXECUTION_CLAIM = "synthetic_fixture_no_execution"
DEMO_SIMULATION_BASIS = "deterministic_repository_fixture"
DEMO_EXECUTOR_MODEL = "not_run"
DEMO_REFERENCE_KIND = "synthetic_block_coordinate"
DEMO_NOTIONALS = ("1000", "5000", "10000", "50000", "100000")
_WORKER_TIMEOUT_SECONDS = 20
_REFERENCE_BLOCK_NUMBER = 18_000_000
_REFERENCE_BLOCK_TIMESTAMP = 1_692_230_400
_STATE_AGE_INPUT_SECONDS = 12
_DEMO_COMPONENT_MATRIX: Sequence[Tuple[str, str, str, bool]] = (
    ("buy", "pool_swap_fee", "bounded_estimate", True),
    ("buy", "router_or_integrator_fee", "bounded_estimate", False),
    ("buy", "token_transfer_tax", "bounded_estimate", False),
    ("sell", "pool_swap_fee", "bounded_estimate", True),
    ("sell", "router_or_integrator_fee", "bounded_estimate", False),
    ("sell", "token_transfer_tax", "bounded_estimate", False),
    ("route", "network_gas", "assumed", False),
    ("route", "rebalancing_or_transfer", "not_applicable", False),
    ("route", "mev_buffer", "assumed", False),
)
_DEMO_ROUTES: Sequence[Mapping[str, str]] = (
    {
        "direction": "uniswap_to_sushiswap",
        "buy_market_id": "dex:ethereum:uniswap_v2:pool:UNI-WETH",
        "sell_market_id": "dex:ethereum:sushiswap_v2:pool:UNI-WETH",
        "gross_edge_bps": "6",
        "net_edge_bps": "-2",
    },
    {
        "direction": "sushiswap_to_uniswap",
        "buy_market_id": "dex:ethereum:sushiswap_v2:pool:UNI-WETH",
        "sell_market_id": "dex:ethereum:uniswap_v2:pool:UNI-WETH",
        "gross_edge_bps": "4",
        "net_edge_bps": "-4",
    },
)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _content_sha256(value: Any) -> str:
    return _sha256_bytes(_canonical_bytes(value))


def _fixture_digest(label: str, value: Any) -> str:
    return _sha256_bytes(label.encode("ascii") + b":" + _canonical_bytes(value))


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if value == 0 else text


def _canonical_decimal(value: Any) -> Decimal:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("fixture decimal is not canonical text")
    try:
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("fixture decimal is invalid") from None
    if not number.is_finite() or _decimal_text(number) != value:
        raise ValueError("fixture decimal is not canonical text")
    return number


def _amount_at_bps(notional: str, basis_points: str) -> str:
    value = Decimal(notional) * Decimal(basis_points) / Decimal("10000")
    return _decimal_text(value)


def _reference_timestamp(value: int) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _venue_from_market_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("fixture market id is invalid")
    parts = value.split(":")
    if len(parts) != 5 or parts[0] != "dex" or not parts[2]:
        raise ValueError("fixture market id is invalid")
    return parts[2]


def _build_repository_fixture_bundle() -> Dict[str, Any]:
    """Create deterministic synthetic inputs and stored fixture artifacts."""

    reference = {
        "kind": DEMO_REFERENCE_KIND,
        "block_number": _REFERENCE_BLOCK_NUMBER,
        "block_hash": "0x" + _fixture_digest(
            "synthetic-block-coordinate", _REFERENCE_BLOCK_NUMBER
        ),
        "block_timestamp": _REFERENCE_BLOCK_TIMESTAMP,
        "state_age_input_seconds": _STATE_AGE_INPUT_SECONDS,
    }
    policy = {
        "contract_version": "opportunity_historical_demo_policy/v1",
        "execution_status": DEMO_EXECUTOR_MODEL,
        "source": DEMO_SIMULATION_BASIS,
        "notionals_usd": list(DEMO_NOTIONALS),
        "stress_bps": [25, 50],
    }
    policy_sha256 = _content_sha256(policy)
    selection = {
        "contract_version": "opportunity_historical_demo_selection/v1",
        "reference": reference,
        "directions": [route["direction"] for route in _DEMO_ROUTES],
        "notionals_usd": list(DEMO_NOTIONALS),
    }
    selection_sha256 = _content_sha256(selection)
    run_id = "demo-run:" + _fixture_digest("demo-run-id", {
        "policy_sha256": policy_sha256,
        "selection_sha256": selection_sha256,
    })
    run_manifest = {
        "contract_version": "opportunity_historical_demo_run/v1",
        "run_id": run_id,
        "execution_status": DEMO_EXECUTOR_MODEL,
        "policy_sha256": policy_sha256,
        "selection_sha256": selection_sha256,
    }
    run_manifest_sha256 = _content_sha256(run_manifest)
    replay_id = "replay:" + _fixture_digest(
        "demo-scenario-set", run_manifest
    )
    route_cohort_id = "demo-cohort:" + _fixture_digest(
        "route-cohort", {
            "directions": selection["directions"],
            "notionals_usd": selection["notionals_usd"],
        }
    )

    routes: List[Dict[str, Any]] = []
    opportunities: List[Dict[str, Any]] = []
    cost_components: List[Dict[str, Any]] = []
    scenarios: List[Dict[str, Any]] = []
    artifacts: Dict[str, Dict[str, Any]] = {}
    for route_index, route_spec in enumerate(_DEMO_ROUTES):
        direction = route_spec["direction"]
        route_id = "historical-demo:UNI:" + direction
        routes.append({
            "route_id": route_id,
            "direction": direction,
            "buy_market_id": route_spec["buy_market_id"],
            "sell_market_id": route_spec["sell_market_id"],
            "route_volume_usd": str(25_000_000 - route_index * 5_000_000),
        })
        for notional_index, notional in enumerate(DEMO_NOTIONALS):
            coordinate = {
                "direction": direction,
                "notional_usd": notional,
                "replay_id": replay_id,
            }
            opportunity_id = "historical-demo:" + _fixture_digest(
                "opportunity", coordinate
            )
            gross_edge = _amount_at_bps(
                notional, route_spec["gross_edge_bps"]
            )
            net_edge = _amount_at_bps(
                notional, route_spec["net_edge_bps"]
            )
            stress_25 = _amount_at_bps(
                notional,
                str(Decimal(route_spec["net_edge_bps"]) - Decimal("25")),
            )
            stress_50 = _amount_at_bps(
                notional,
                str(Decimal(route_spec["net_edge_bps"]) - Decimal("50")),
            )
            opportunities.append({
                "opportunity_id": opportunity_id,
                "route_id": route_id,
                "token_symbol": "UNI",
                "buy_market_id": route_spec["buy_market_id"],
                "sell_market_id": route_spec["sell_market_id"],
                "requested_notional_usd": notional,
                "opportunity_class": "research_estimate",
                "strict_eligible": False,
                "strict_ready_for_publication": False,
                "publication_attestation_sha256": None,
                "gross_edge_usd": gross_edge,
                "research_net_edge_usd": net_edge,
                "research_net_edge_bps": route_spec["net_edge_bps"],
                "maximum_proved_capacity_quantity": _decimal_text(
                    Decimal(notional) / Decimal("5")
                ),
                "skew_seconds": "0",
            })
            for leg, component_type, value_status, embedded in (
                _DEMO_COMPONENT_MATRIX
            ):
                cost_components.append({
                    "opportunity_id": opportunity_id,
                    "leg": leg,
                    "component_type": component_type,
                    "value_status": value_status,
                    "embedded_in_leg_quote": embedded,
                })
            receipt_record = {
                "contract_version": "opportunity_historical_demo_receipt/v1",
                "execution_status": DEMO_EXECUTOR_MODEL,
                "gas_assumption": (
                    180_000 + route_index * 1_000 + notional_index
                ),
            }
            workflow_trace = {
                "contract_version": "opportunity_historical_demo_trace/v1",
                "execution_status": DEMO_EXECUTOR_MODEL,
                "steps": [
                    "load_deterministic_fixture_inputs",
                    "calculate_research_estimate",
                    "publish_read_only_demo_contract",
                ],
            }
            result_record = {
                "contract_version": "opportunity_historical_demo_result/v1",
                "execution_status": DEMO_EXECUTOR_MODEL,
                "gross_edge_usd": gross_edge,
                "research_net_edge_usd": net_edge,
                "baseline_net_edge_usd": net_edge,
                "stress_25_net_edge_usd": stress_25,
                "stress_50_net_edge_usd": stress_50,
                "stress_robust": False,
            }
            proof_inputs = {
                "contract_version": "opportunity_historical_demo_inputs/v1",
                "source": DEMO_SIMULATION_BASIS,
                "reference_sha256": selection_sha256,
                "direction": direction,
                "notional_usd": notional,
                "buy_market_id": route_spec["buy_market_id"],
                "sell_market_id": route_spec["sell_market_id"],
            }
            artifacts[opportunity_id] = {
                "receipt_record": receipt_record,
                "workflow_trace": workflow_trace,
                "result_record": result_record,
                "proof_inputs": proof_inputs,
            }
            scenarios.append({
                "scenario_key": "{}:{}".format(direction, notional),
                "opportunity_id": opportunity_id,
                "route_id": route_id,
                "direction": direction,
                "requested_notional_usd": notional,
                "execution_status": DEMO_EXECUTOR_MODEL,
                "receipt_sha256": _content_sha256(receipt_record),
                "trace_sha256": _content_sha256(workflow_trace),
                "result_sha256": _content_sha256(result_record),
                "proof_inputs_hash": _content_sha256(proof_inputs),
            })

    evidence = {
        "contract_version": "opportunity_historical_demo_evidence/v1",
        "evidence_mode": DEMO_EVIDENCE_MODE,
        "execution_status": DEMO_EXECUTOR_MODEL,
        "scenario_count": len(scenarios),
        "scenarios": scenarios,
    }
    evidence["scenario_set_sha256"] = _content_sha256(scenarios)
    manifest = {
        "contract_version": "opportunity_historical_demo_manifest/v1",
        "temporal_scope": DEMO_TEMPORAL_SCOPE,
        "execution_claim": DEMO_EXECUTION_CLAIM,
        "simulation_basis": DEMO_SIMULATION_BASIS,
        "reference_kind": DEMO_REFERENCE_KIND,
        "replay_id": replay_id,
        "route_cohort_id": route_cohort_id,
        "policy_sha256": policy_sha256,
        "run_id": run_id,
        "run_manifest_sha256": run_manifest_sha256,
        "selection_sha256": selection_sha256,
        "scenario_set_sha256": evidence["scenario_set_sha256"],
    }
    return {
        "artifacts": artifacts,
        "cost_components": cost_components,
        "evidence": evidence,
        "manifest": manifest,
        "opportunities": opportunities,
        "policy": policy,
        "routes": routes,
        "run_manifest": run_manifest,
        "selection": selection,
    }


def _validate_and_project_demo_bundle(
    bundle: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """Validate the synthetic contract without accepting production evidence."""

    expected_keys = {
        "artifacts", "cost_components", "evidence", "manifest",
        "opportunities", "policy", "routes", "run_manifest", "selection",
    }
    if not isinstance(bundle, Mapping) or set(bundle) != expected_keys:
        raise ValueError("fixture bundle shape differs")
    manifest = bundle["manifest"]
    policy = bundle["policy"]
    run_manifest = bundle["run_manifest"]
    selection = bundle["selection"]
    evidence = bundle["evidence"]
    artifacts = bundle["artifacts"]
    if not all(isinstance(item, Mapping) for item in (
        manifest, policy, run_manifest, selection, evidence, artifacts,
    )):
        raise ValueError("fixture metadata shape differs")
    reference = selection.get("reference")
    if not isinstance(reference, Mapping):
        raise ValueError("fixture reference is absent")
    if (
        manifest.get("contract_version")
        != "opportunity_historical_demo_manifest/v1"
        or manifest.get("temporal_scope") != DEMO_TEMPORAL_SCOPE
        or manifest.get("execution_claim") != DEMO_EXECUTION_CLAIM
        or manifest.get("simulation_basis") != DEMO_SIMULATION_BASIS
        or manifest.get("reference_kind") != DEMO_REFERENCE_KIND
        or reference.get("kind") != DEMO_REFERENCE_KIND
        or type(reference.get("block_number")) is not int
        or reference["block_number"] < 0
        or type(reference.get("block_timestamp")) is not int
        or reference["block_timestamp"] < 0
        or type(reference.get("state_age_input_seconds")) is not int
        or reference["state_age_input_seconds"] < 0
        or not isinstance(reference.get("block_hash"), str)
        or len(reference["block_hash"]) != 66
        or not reference["block_hash"].startswith("0x")
        or any(
            character not in "0123456789abcdef"
            for character in reference["block_hash"][2:]
        )
        or policy.get("execution_status") != DEMO_EXECUTOR_MODEL
        or run_manifest.get("execution_status") != DEMO_EXECUTOR_MODEL
        or evidence.get("execution_status") != DEMO_EXECUTOR_MODEL
        or evidence.get("evidence_mode") != DEMO_EVIDENCE_MODE
    ):
        raise ValueError("fixture declaration is invalid")
    if (
        manifest.get("policy_sha256") != _content_sha256(policy)
        or manifest.get("selection_sha256") != _content_sha256(selection)
        or manifest.get("run_manifest_sha256") != _content_sha256(run_manifest)
        or run_manifest.get("policy_sha256") != manifest["policy_sha256"]
        or run_manifest.get("selection_sha256") != manifest["selection_sha256"]
        or run_manifest.get("run_id") != manifest.get("run_id")
        or manifest.get("replay_id")
        != "replay:" + _fixture_digest("demo-scenario-set", run_manifest)
    ):
        raise ValueError("fixture manifest hash binding failed")

    route_rows = bundle["routes"]
    opportunity_rows = bundle["opportunities"]
    cost_rows = bundle["cost_components"]
    scenario_rows = evidence.get("scenarios")
    if not all(isinstance(rows, list) for rows in (
        route_rows, opportunity_rows, cost_rows, scenario_rows,
    )):
        raise ValueError("fixture inventory shape differs")
    if (
        len(route_rows) != 2
        or len(opportunity_rows) != 10
        or len(cost_rows) != 90
        or len(scenario_rows) != 10
        or evidence.get("scenario_count") != 10
        or evidence.get("scenario_set_sha256")
        != _content_sha256(scenario_rows)
        or manifest.get("scenario_set_sha256")
        != evidence.get("scenario_set_sha256")
    ):
        raise ValueError("fixture inventory count or hash differs")

    routes_by_id = {}
    for route in route_rows:
        if not isinstance(route, Mapping):
            raise ValueError("fixture route is invalid")
        route_id = route.get("route_id")
        direction = route.get("direction")
        matching_spec = next(
            (item for item in _DEMO_ROUTES if item["direction"] == direction),
            None,
        )
        if (
            not isinstance(route_id, str)
            or route_id in routes_by_id
            or matching_spec is None
            or route.get("buy_market_id") != matching_spec["buy_market_id"]
            or route.get("sell_market_id") != matching_spec["sell_market_id"]
            or _canonical_decimal(route.get("route_volume_usd")) <= 0
        ):
            raise ValueError("fixture route differs")
        routes_by_id[route_id] = route

    scenarios_by_id = {}
    for scenario in scenario_rows:
        if not isinstance(scenario, Mapping):
            raise ValueError("fixture scenario is invalid")
        opportunity_id = scenario.get("opportunity_id")
        if not isinstance(opportunity_id, str) or opportunity_id in scenarios_by_id:
            raise ValueError("fixture scenario identity differs")
        scenarios_by_id[opportunity_id] = scenario
    costs_by_id: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for component in cost_rows:
        if not isinstance(component, Mapping):
            raise ValueError("fixture cost component is invalid")
        opportunity_id = component.get("opportunity_id")
        if not isinstance(opportunity_id, str):
            raise ValueError("fixture cost identity differs")
        costs_by_id[opportunity_id].append(component)
    opportunity_ids = {
        item.get("opportunity_id")
        for item in opportunity_rows
        if isinstance(item, Mapping)
    }
    if (
        len(opportunity_ids) != 10
        or None in opportunity_ids
        or set(scenarios_by_id) != opportunity_ids
        or set(costs_by_id) != opportunity_ids
        or set(artifacts) != opportunity_ids
    ):
        raise ValueError("fixture scenario inventory differs")

    expected_coordinates = {
        (route["direction"], notional)
        for route in _DEMO_ROUTES
        for notional in DEMO_NOTIONALS
    }
    coordinates = set()
    projected: List[Dict[str, Any]] = []
    for opportunity in opportunity_rows:
        if not isinstance(opportunity, Mapping):
            raise ValueError("fixture opportunity is invalid")
        opportunity_id = opportunity["opportunity_id"]
        route_id = opportunity.get("route_id")
        route = routes_by_id.get(route_id)
        scenario = scenarios_by_id[opportunity_id]
        artifact_set = artifacts[opportunity_id]
        components = costs_by_id[opportunity_id]
        if not isinstance(artifact_set, Mapping) or route is None:
            raise ValueError("fixture opportunity references differ")
        direction = scenario.get("direction")
        notional = scenario.get("requested_notional_usd")
        coordinate = (direction, notional)
        observed_matrix = [
            (
                row.get("leg"), row.get("component_type"),
                row.get("value_status"), row.get("embedded_in_leg_quote"),
            )
            for row in components
        ]
        if (
            coordinate not in expected_coordinates
            or coordinate in coordinates
            or scenario.get("route_id") != route_id
            or route.get("direction") != direction
            or opportunity.get("requested_notional_usd") != notional
            or opportunity.get("token_symbol") != "UNI"
            or opportunity.get("buy_market_id") != route.get("buy_market_id")
            or opportunity.get("sell_market_id") != route.get("sell_market_id")
            or opportunity.get("opportunity_class") != "research_estimate"
            or opportunity.get("strict_eligible") is not False
            or opportunity.get("strict_ready_for_publication") is not False
            or opportunity.get("publication_attestation_sha256") is not None
            or scenario.get("execution_status") != DEMO_EXECUTOR_MODEL
            or observed_matrix != list(_DEMO_COMPONENT_MATRIX)
        ):
            raise ValueError("fixture opportunity contract differs")
        coordinates.add(coordinate)

        receipt_record = artifact_set.get("receipt_record")
        workflow_trace = artifact_set.get("workflow_trace")
        result_record = artifact_set.get("result_record")
        proof_inputs = artifact_set.get("proof_inputs")
        if not all(isinstance(item, Mapping) for item in (
            receipt_record, workflow_trace, result_record, proof_inputs,
        )):
            raise ValueError("fixture artifact shape differs")
        if (
            receipt_record.get("execution_status") != DEMO_EXECUTOR_MODEL
            or workflow_trace.get("execution_status") != DEMO_EXECUTOR_MODEL
            or result_record.get("execution_status") != DEMO_EXECUTOR_MODEL
            or proof_inputs.get("source") != DEMO_SIMULATION_BASIS
            or proof_inputs.get("reference_sha256")
            != manifest["selection_sha256"]
            or proof_inputs.get("direction") != direction
            or proof_inputs.get("notional_usd") != notional
            or proof_inputs.get("buy_market_id") != route["buy_market_id"]
            or proof_inputs.get("sell_market_id") != route["sell_market_id"]
            or scenario.get("receipt_sha256")
            != _content_sha256(receipt_record)
            or scenario.get("trace_sha256") != _content_sha256(workflow_trace)
            or scenario.get("result_sha256") != _content_sha256(result_record)
            or scenario.get("proof_inputs_hash") != _content_sha256(proof_inputs)
            or type(receipt_record.get("gas_assumption")) is not int
            or receipt_record["gas_assumption"] < 0
        ):
            raise ValueError("fixture artifact hash binding failed")
        gross_edge = _canonical_decimal(opportunity.get("gross_edge_usd"))
        research_net = _canonical_decimal(
            opportunity.get("research_net_edge_usd")
        )
        net_bps = _canonical_decimal(opportunity.get("research_net_edge_bps"))
        capacity = _canonical_decimal(
            opportunity.get("maximum_proved_capacity_quantity")
        )
        skew = _canonical_decimal(opportunity.get("skew_seconds"))
        result_values = {
            key: _canonical_decimal(result_record.get(key))
            for key in (
                "gross_edge_usd", "research_net_edge_usd",
                "baseline_net_edge_usd", "stress_25_net_edge_usd",
                "stress_50_net_edge_usd",
            )
        }
        if (
            gross_edge != result_values["gross_edge_usd"]
            or research_net != result_values["research_net_edge_usd"]
            or research_net != result_values["baseline_net_edge_usd"]
            or result_record.get("stress_robust") is not False
            or capacity <= 0
            or skew < 0
        ):
            raise ValueError("fixture result binding failed")
        projected.append({
            "route_id": route_id,
            "opportunity_id": opportunity_id,
            "scenario_key": scenario.get("scenario_key"),
            "token_symbol": "UNI",
            "buy_market_id": route["buy_market_id"],
            "sell_market_id": route["sell_market_id"],
            "leg_venues": {
                "buy": _venue_from_market_id(route["buy_market_id"]),
                "sell": _venue_from_market_id(route["sell_market_id"]),
            },
            "route_type": "dex_dex",
            "route_mode": DEMO_EXECUTION_CLAIM,
            "direction": direction,
            "requested_notional_usd": notional,
            "opportunity_class": "research_estimate",
            "availability": {"status": "available", "reason": None},
            "gross_edge_usd": _decimal_text(gross_edge),
            "net_edge_usd": _decimal_text(research_net),
            "net_edge_bps": _decimal_text(net_bps),
            "capacity_quantity": _decimal_text(capacity),
            "skew_seconds": _decimal_text(skew),
            "route_age_seconds": reference["state_age_input_seconds"],
            "route_volume_usd": route["route_volume_usd"],
            "selected_block_number": reference["block_number"],
            "selected_block_hash": reference["block_hash"],
            "selected_block_timestamp": _reference_timestamp(
                reference["block_timestamp"]
            ),
            "state_age_seconds": reference["state_age_input_seconds"],
            "foundry_verified": False,
            "gas_assumption": receipt_record["gas_assumption"],
            "receipt_sha256": scenario["receipt_sha256"],
            "trace_sha256": scenario["trace_sha256"],
            "result_sha256": scenario["result_sha256"],
            "proof_inputs_hash": scenario["proof_inputs_hash"],
            "executor_model": DEMO_EXECUTOR_MODEL,
            "policy_net_edge_usd": _decimal_text(research_net),
            "research_net_edge_usd": _decimal_text(research_net),
            "baseline_net_edge_usd": _decimal_text(
                result_values["baseline_net_edge_usd"]
            ),
            "stress_25_net_edge_usd": _decimal_text(
                result_values["stress_25_net_edge_usd"]
            ),
            "stress_50_net_edge_usd": _decimal_text(
                result_values["stress_50_net_edge_usd"]
            ),
            "stress_robust": False,
        })
    if coordinates != expected_coordinates:
        raise ValueError("fixture coordinate matrix differs")
    return projected


def _offline_validation_worker(bundle_path: str, connection: Any) -> None:
    """Validate canonical fixture bytes in a fresh local Python process."""

    try:
        raw = Path(bundle_path).read_bytes()
        bundle = json.loads(raw.decode("ascii"))
        if raw != _canonical_bytes(bundle):
            raise ValueError("fixture bundle is not canonical")
        rows = _validate_and_project_demo_bundle(bundle)
        report = {
            "contract_version": DEMO_VALIDATION_CONTRACT,
            "status": DEMO_VERIFICATION_STATUS,
            "evidence_mode": DEMO_EVIDENCE_MODE,
            "execution_status": DEMO_EXECUTOR_MODEL,
            "validation_boundary": "spawned_local_process",
            "bundle_sha256": _sha256_bytes(raw),
            "projected_rows_sha256": _content_sha256(rows),
            "scenario_count": len(rows),
        }
        connection.send({"kind": "ok", "report": report, "rows": rows})
    except BaseException as error:
        try:
            connection.send({
                "kind": "error",
                "error_type": type(error).__name__,
            })
        except Exception:
            pass
    finally:
        connection.close()


def _fresh_offline_validation(bundle_path: Path) -> Mapping[str, Any]:
    """Return structural fixture evidence without invoking RPC or Foundry."""

    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_offline_validation_worker,
        args=(str(bundle_path), child),
    )
    process.start()
    child.close()
    result = None
    try:
        if not parent.poll(_WORKER_TIMEOUT_SECONDS):
            raise RuntimeError("offline fixture validation timed out")
        try:
            result = parent.recv()
        except EOFError:
            result = None
    finally:
        parent.close()
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
    if process.exitcode != 0:
        raise RuntimeError("offline fixture validation process failed")
    if not isinstance(result, Mapping) or result.get("kind") != "ok":
        error_type = (
            result.get("error_type") if isinstance(result, Mapping) else "unknown"
        )
        raise RuntimeError(
            "offline fixture structural validation failed ({})".format(
                error_type
            )
        )
    return result


class HistoricalOpportunityDemoFixture:
    """Build and retain one toolchain-free, structurally validated fixture."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._temporary = None
        self._bundle_path = None
        self._report_path = None
        self._rows = None
        self.data_dir = self.raw_root = self.historical_root = None
        self.pointer = None
        try:
            self._temporary = tempfile.TemporaryDirectory(
                prefix="historical-opportunity-demo-"
            )
            self.data_dir = Path(self._temporary.name).resolve()
            self.raw_root = self.data_dir / "raw" / "historical-demo"
            self.historical_root = self.data_dir / "routes" / "historical"
            self.historical_root.mkdir(parents=True)
            bundle = _build_repository_fixture_bundle()
            bundle_bytes = _canonical_bytes(bundle)
            self._bundle_sha256 = _sha256_bytes(bundle_bytes)
            self._bundle_path = self.historical_root / "demo-fixture.json"
            pending_path = self._bundle_path.with_suffix(".json.pending")
            pending_path.write_bytes(bundle_bytes)
            os.replace(str(pending_path), str(self._bundle_path))

            validation = _fresh_offline_validation(self._bundle_path)
            report = validation.get("report")
            rows = validation.get("rows")
            if (
                not isinstance(report, Mapping)
                or not isinstance(rows, list)
                or len(rows) != 10
                or report.get("contract_version") != DEMO_VALIDATION_CONTRACT
                or report.get("status") != DEMO_VERIFICATION_STATUS
                or report.get("evidence_mode") != DEMO_EVIDENCE_MODE
                or report.get("execution_status") != DEMO_EXECUTOR_MODEL
                or report.get("validation_boundary")
                != "spawned_local_process"
                or report.get("bundle_sha256") != self._bundle_sha256
                or report.get("scenario_count") != len(rows)
                or report.get("projected_rows_sha256")
                != _content_sha256(rows)
            ):
                raise RuntimeError("offline fixture validation contract differs")
            self._rows = rows
            self._manifest = bundle["manifest"]
            self._evidence = bundle["evidence"]
            self._reference = bundle["selection"]["reference"]
            self._manifest_sha256 = _content_sha256(self._manifest)
            self._validation_report = dict(report)
            report_bytes = _canonical_bytes(self._validation_report)
            self._verification_report_sha256 = _sha256_bytes(report_bytes)
            self._report_path = self.historical_root / "validation-report.json"
            self._report_path.write_bytes(report_bytes)
            self.pointer = {
                "replay_id": self._manifest["replay_id"],
                "bundle_sha256": self._bundle_sha256,
            }
            generation_projection = {
                "contract_version": DEMO_CONTRACT,
                "demo_fixture": True,
                "evidence_mode": DEMO_EVIDENCE_MODE,
                "verification_status": DEMO_VERIFICATION_STATUS,
                "execution_status": DEMO_EXECUTOR_MODEL,
                "replay_id": self._manifest["replay_id"],
                "manifest_sha256": self._manifest_sha256,
                "bundle_sha256": self._bundle_sha256,
                "verification_report_sha256": (
                    self._verification_report_sha256
                ),
                "scenario_set_sha256": self._evidence[
                    "scenario_set_sha256"
                ],
            }
            self.data_generation = _fixture_digest(
                "data-generation", generation_projection
            )
        except BaseException:
            self.close()
            raise

    def _reread_unchanged(self) -> None:
        if self._bundle_path is None or self._report_path is None:
            raise RuntimeError("offline fixture is closed")
        try:
            bundle_bytes = self._bundle_path.read_bytes()
            report_bytes = self._report_path.read_bytes()
        except OSError:
            raise RuntimeError("offline fixture files are unavailable") from None
        if (
            _sha256_bytes(bundle_bytes) != self._bundle_sha256
            or _sha256_bytes(report_bytes)
            != self._verification_report_sha256
        ):
            raise RuntimeError("offline fixture changed after validation")

    def build_payload(
        self,
        *,
        token: Optional[str] = None,
        venue: Optional[str] = None,
        notional_usd: Optional[str] = None,
        opportunity_class: Optional[str] = None,
        route_type: Optional[str] = None,
        availability: Optional[str] = None,
        sort: Optional[str] = None,
        direction: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Project the fixture into a wire contract that cannot imply execution."""

        from dashboard import opportunity_facts as facts

        with self._lock:
            if self._rows is None:
                raise RuntimeError("offline fixture is closed")
            self._reread_unchanged()
            filters = facts.normalize_opportunity_filters(
                token=token,
                venue=venue,
                notional_usd=notional_usd,
                opportunity_class=opportunity_class,
                route_type=route_type,
                availability=availability,
                sort=sort,
                direction=direction,
            )
            rows = json.loads(_canonical_bytes(self._rows).decode("ascii"))
            filtered = [
                row for row in rows if facts._matches_filters(row, filters)
            ]
            filtered = facts._sort_routes(
                filtered,
                str(filters["sort"]),
                str(filters["direction"]),
            )
            positive_count = sum(
                Decimal(row["research_net_edge_usd"]) > 0
                for row in rows
            )
            payload = {
                "availability": {"status": "available", "reason": None},
                "metadata": {
                    "contract_version": DEMO_CONTRACT,
                    "demo_fixture": True,
                    "evidence_mode": DEMO_EVIDENCE_MODE,
                    "verification_status": DEMO_VERIFICATION_STATUS,
                    "validation_boundary": "spawned_local_process",
                    "temporal_scope": DEMO_TEMPORAL_SCOPE,
                    "execution_claim": DEMO_EXECUTION_CLAIM,
                    "execution_status": DEMO_EXECUTOR_MODEL,
                    "simulation_basis": DEMO_SIMULATION_BASIS,
                    "reference_kind": DEMO_REFERENCE_KIND,
                    "data_generation": self.data_generation,
                    "replay_id": self._manifest["replay_id"],
                    "route_cohort_id": self._manifest["route_cohort_id"],
                    "manifest_sha256": self._manifest_sha256,
                    "verification_report_sha256": (
                        self._verification_report_sha256
                    ),
                    "policy_sha256": self._manifest["policy_sha256"],
                    "run_id": self._manifest["run_id"],
                    "run_manifest_sha256": self._manifest[
                        "run_manifest_sha256"
                    ],
                    "selection_sha256": self._manifest["selection_sha256"],
                    "scenario_set_sha256": self._evidence[
                        "scenario_set_sha256"
                    ],
                    "selected_block_number": self._reference["block_number"],
                    "selected_block_hash": self._reference["block_hash"],
                    "selected_block_timestamp": _reference_timestamp(
                        self._reference["block_timestamp"]
                    ),
                    "publication_status": "staged_demo_fixture",
                    "coverage": {
                        "route_count": len({
                            row["route_id"] for row in rows
                        }),
                        "scenario_count": len(rows),
                        "returned_count": len(filtered),
                        "foundry_verified_count": 0,
                        "research_estimate_count": len(rows),
                        "positive_count": positive_count,
                        "strict_count": 0,
                        "executable_count": 0,
                        "attested_count": 0,
                        "unavailable_count": 0,
                    },
                },
                "freshness": {
                    "applicable": False,
                    "reason_code": "historical_demo_fixture",
                    "next_deadline": None,
                },
                "filters": filters,
                "routes": filtered,
            }
            self._reread_unchanged()
            return payload

    def close(self) -> None:
        self._rows = None
        self.pointer = None
        self._bundle_path = None
        self._report_path = None
        temporary = self._temporary
        self._temporary = None
        if temporary is not None:
            temporary.cleanup()

    def __enter__(self) -> "HistoricalOpportunityDemoFixture":
        return self

    def __exit__(self, _error_type: Any, _error: Any, _traceback: Any) -> None:
        self.close()
