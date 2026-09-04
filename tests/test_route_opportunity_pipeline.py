"""Tests for pinned CEX and DEX route-opportunity finalizers."""

from __future__ import annotations

import copy
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import ExitStack, redirect_stdout
from unittest.mock import patch

from scripts.route_cost_evidence import (
    build_unavailable_route_cost_evidence_manifest,
    physical_sha256,
    typed_sha256,
)
from scripts.cex_fee_facts import PUBLIC_FEE_SCHEDULE_COLUMNS
from scripts.live_cex_research import build_live_cex_research_universe
from scripts.route_cohort import canonical_route_id
from scripts.fetch_dex_depth import ROUTE_V2_FEE_PROOF_SHA256
from scripts.route_publication import (
    load_latest_complete_route_bundle,
    publish_complete_route_bundle,
    publish_route_cohort_bundle,
    publish_shadow_result,
)
from scripts.route_shadow_audit import build_shadow_audit
from scripts.route_shadow_inputs import (
    SourceFileIdentity,
    TYPED_SOURCE_ROLE_CONTRACTS,
    TYPED_SOURCE_LINEAGE_SCHEMA_V2,
    _candidate_source_generation,
    write_run_universe,
)
from scripts.route_quantity import (
    CommonTarget,
    MarketRules,
    V2PoolState,
    V2_FEE_FORMULA,
)
from scripts.route_universe import route_universe_sha256
import scripts.route_opportunity_pipeline as opportunity_pipeline
from scripts.route_opportunity_pipeline import (
    RouteOpportunityPipelineError,
    finalize_cex_route_opportunities,
)
import tests.test_route_publication as route_publication_test
from tests.test_route_publication import _rehash, _shadow_json_bytes, _task7_cex_inputs


def _physical_json(value):
    return json.dumps(
        value, ensure_ascii=False, indent=2, sort_keys=True
    ).encode("utf-8") + b"\n"


def _observed_dex_cost_sidecar(cohort, sources):
    """Small already-replayed sidecar projection fixture for pipeline tests."""
    targets = {}
    for route in cohort["routes"]:
        buy_source = sources[route["buy_market_id"]]
        sell_source = sources[route["sell_market_id"]]
        for raw_notional in cohort["requested_notionals_usd"]:
            target = opportunity_pipeline.common_target_quantity(
                requested_notional_usd=raw_notional,
                buy_reference_price_usd=buy_source["reference_price_usd"],
                sell_reference_price_usd=sell_source["reference_price_usd"],
                buy_market_rules=buy_source["rules"],
                sell_market_rules=sell_source["rules"],
            )
            for market_id in (
                route["buy_market_id"], route["sell_market_id"]
            ):
                key = (market_id, str(raw_notional))
                prior = targets.setdefault(key, target)
                if prior != target:
                    raise AssertionError("fixture target differs by route")
    transcripts = []
    transcript_by_scope = {}
    for leg in sorted(cohort["legs"], key=lambda row: row["market_id"]):
        market_id = leg["market_id"]
        for direction in ("buy", "sell"):
            for raw_notional in cohort["requested_notionals_usd"]:
                notional = str(raw_notional)
                source = sources[market_id]
                target = targets[(market_id, notional)]
                target_address = source["conversion"]["target_token_address"]
                target_value = {
                    "schema": "route_cost_simulation_target/v1",
                    "token_address": target_address,
                    "unit_decimals": str(target.unit_decimals),
                    "raw_quantity": str(target.raw_quantity),
                    "lattice_raw": str(target.lattice_raw),
                }
                transcript = {
                    "market_id": market_id,
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "simulation_target_token_address": target_address,
                    "simulation_target_unit_decimals": target_value[
                        "unit_decimals"
                    ],
                    "simulation_target_raw_quantity": target_value[
                        "raw_quantity"
                    ],
                    "simulation_target_lattice_raw": target_value[
                        "lattice_raw"
                    ],
                    "simulation_target_sha256": typed_sha256(
                        b"route-cost-simulation-target/v1\n", target_value
                    ),
                    "core_pool_state_id": source["state"].state_id,
                    "core_pool_state_sha256": source["pool_sha256"],
                    "status": "observed",
                    "completed_stage": "transfer_tax",
                    "reason_code": None,
                    "gas_evidence": {
                        "gas_units": "21000",
                        "max_fee_per_gas_wei": "20000000000",
                        "native_price_usd": "3000",
                        "observed_at": "2026-08-01T12:00:00Z",
                        "valid_until": "2026-08-01T12:05:00Z",
                    },
                    "router_fee_evidence": {
                        "status": "not_applicable",
                        "source_record_sha256": "7" * 64,
                    },
                    "transfer_tax_evidence": {
                        "status": "not_applicable",
                        "trace_sha256": "8" * 64,
                    },
                    # This is a calldata loss ceiling, not an MEV cost.
                    "call_evidence": {
                        "submission_loss_bound_bps": "9999",
                    },
                }
                transcripts.append(transcript)
                transcript_by_scope[(market_id, direction, notional)] = (
                    transcript
                )
    bindings = []
    for route in sorted(cohort["routes"], key=lambda row: row["route_id"]):
        for raw_notional in cohort["requested_notionals_usd"]:
            notional = str(raw_notional)
            binding = {
                "route_id": route["route_id"],
                "requested_notional_usd": notional,
                "buy_transcript_sha256": typed_sha256(
                    b"route-cost-evidence-transcript/v1\n",
                    transcript_by_scope[(
                        route["buy_market_id"], "buy", notional
                    )],
                ),
                "sell_transcript_sha256": typed_sha256(
                    b"route-cost-evidence-transcript/v1\n",
                    transcript_by_scope[(
                        route["sell_market_id"], "sell", notional
                    )],
                ),
                "status": "unavailable",
                "reason_code": "submission_policy_unavailable",
            }
            bindings.append(binding)
    sidecar = {"transcripts": transcripts, "bindings": bindings}
    sidecar_sha = physical_sha256(sidecar)
    route_by_id = {route["route_id"]: route for route in cohort["routes"]}
    outcomes = [
        {
            "route_id": binding["route_id"],
            "requested_notional_usd": binding["requested_notional_usd"],
            "status": binding["status"],
            "reason_code": binding["reason_code"],
            "coverage_kind": "binding",
            "covered_dex_market_ids": sorted((
                route_by_id[binding["route_id"]]["buy_market_id"],
                route_by_id[binding["route_id"]]["sell_market_id"],
            )),
            "uncovered_dex_market_ids": [],
            "scoped_binding_sha256": typed_sha256(
                b"route-cost-evidence-binding/v1\n", binding
            ),
            "route_cost_evidence_sha256": sidecar_sha,
        }
        for binding in bindings
    ]
    return sidecar, outcomes


def _v2_pool_payload(
    *,
    pool_address,
    raw_response_sha256,
    reserve_quote_raw,
):
    state = V2PoolState(
        chain="eth",
        chain_id=1,
        dex="uniswap_v2",
        pool_address=pool_address,
        token0_address="0x" + "1" * 40,
        token1_address="0x" + "2" * 40,
        token0_decimals=18,
        token1_decimals=6,
        reserve0_raw=1_000_000 * 10**18,
        reserve1_raw=reserve_quote_raw,
        reserve_timestamp_last_raw=1_754_046_400,
        fee_bps=30,
        fee_numerator=9_970,
        fee_denominator=10_000,
        fee_formula=V2_FEE_FORMULA,
        fee_proof_sha256=ROUTE_V2_FEE_PROOF_SHA256,
        block_number=123,
        block_hash="0x" + "a" * 64,
        block_header_sha256="b" * 64,
        observed_at="2026-08-01T12:00:00Z",
        raw_response_sha256=raw_response_sha256,
    )
    integer_fields = {
        "chain_id", "token0_decimals", "token1_decimals", "reserve0_raw",
        "reserve1_raw", "reserve_timestamp_last_raw", "fee_bps",
        "fee_numerator", "fee_denominator", "block_number",
    }
    return {
        "schema": "route_v2_pool_state/v1",
        **{
            field: str(getattr(state, field)) if field in integer_fields
            else getattr(state, field)
            for field in (
                "chain", "chain_id", "dex", "pool_address",
                "token0_address", "token1_address", "token0_decimals",
                "token1_decimals", "reserve0_raw", "reserve1_raw",
                "reserve_timestamp_last_raw", "fee_bps", "fee_numerator",
                "fee_denominator", "fee_formula", "fee_proof_sha256",
                "block_number", "block_hash", "block_header_sha256",
                "observed_at", "raw_response_sha256", "state_id",
            )
        },
    }


class RouteOpportunityPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name) / "data"
        self.routes_root = self.data_dir / "routes"
        self.routes_root.mkdir(parents=True)
        self.public_pointer = self.routes_root / "latest.json"
        self.old_pointer_bytes = b'{"old":"route-pointer"}\n'
        self.public_pointer.write_bytes(self.old_pointer_bytes)
        self.run_id = "run-001"
        self.expected_joint_sha256 = "a" * 64

    def test_direct_script_cli_help_is_runnable(self):
        project_root = Path(__file__).resolve().parents[1]
        completed = subprocess.run(
            [
                sys.executable,
                str(project_root / "scripts/route_opportunity_pipeline.py"),
                "--help",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--shadow-run-id", completed.stdout)
        self.assertIn("--expected-joint-pointer-sha256", completed.stdout)
        self.assertIn("--research-mev-bps", completed.stdout)

    def test_research_mev_bps_cli_is_explicit_dex_only_and_bounded(self):
        for value in ("0", "0.000001", "25", "10000"):
            with self.subTest(valid=value):
                self.assertEqual(
                    opportunity_pipeline._canonical_research_mev_bps(value),
                    value,
                )
        for value in (
            "", " 1", "1.0", "1e1", "1e100000000", "-1",
            "0.0000001", "10000.1",
        ):
            with self.subTest(invalid=value):
                with self.assertRaises(RouteOpportunityPipelineError):
                    opportunity_pipeline._canonical_research_mev_bps(value)

        pointer = {"schema": "route_opportunity_pointer/v1"}
        with ExitStack() as stack:
            finalizer = stack.enter_context(patch.object(
                opportunity_pipeline,
                "finalize_eth_uniswap_v2_research_opportunities",
                return_value=pointer,
            ))
            stack.enter_context(patch.object(
                opportunity_pipeline,
                "load_latest_complete_route_bundle",
                return_value={"pointer": pointer},
            ))
            stack.enter_context(redirect_stdout(io.StringIO()))
            self.assertEqual(opportunity_pipeline.main([
                "--finalizer", "eth-uniswap-v2-research",
                "--data-dir", str(self.data_dir),
                "--shadow-run-id", self.run_id,
                "--expected-joint-pointer-sha256", self.expected_joint_sha256,
                "--research-mev-bps", "25",
            ]), 0)
        self.assertEqual(finalizer.call_args.kwargs["research_mev_bps"], "25")

        with self.assertRaises(SystemExit):
            opportunity_pipeline.main([
                "--data-dir", str(self.data_dir),
                "--shadow-run-id", self.run_id,
                "--expected-joint-pointer-sha256", self.expected_joint_sha256,
                "--research-mev-bps", "25",
            ])

    def test_cli_serve_publishes_then_execs_read_only_loopback_dashboard(self):
        data_dir, fixture, joint = self._install_real_cex_run()
        output = io.StringIO()

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                    fixture["fee_profile_path"]
                ),
                "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                    fixture["inventory_profile_path"]
                ),
                "ADMIN_JOB_DIR": "/ambient/admin/jobs",
                "TOKEN_REGISTRY_PATH": "/ambient/admin/registry.json",
            }, clear=False))
            dashboard_exec = stack.enter_context(patch.object(
                opportunity_pipeline.os,
                "execve",
                side_effect=RuntimeError("dashboard exec sentinel"),
            ))
            stack.enter_context(redirect_stdout(output))
            stack.enter_context(self.assertRaisesRegex(
                RuntimeError, "dashboard exec sentinel"
            ))
            opportunity_pipeline.main([
                "--data-dir", str(data_dir),
                "--shadow-run-id", joint["pointer"]["run_id"],
                "--expected-joint-pointer-sha256", joint["pointer_sha256"],
                "--serve",
                "--port", "43210",
            ])

        loaded = load_latest_complete_route_bundle(
            data_dir / "routes", core_root=data_dir / "routes/core"
        )
        self.assertEqual(len(loaded["bundle"]["opportunities"]), 5)
        self.assertEqual(
            json.loads(output.getvalue().splitlines()[0]), loaded["pointer"]
        )
        executable, arguments, environment = dashboard_exec.call_args.args
        project_root = Path(opportunity_pipeline.__file__).resolve().parents[1]
        self.assertEqual(executable, sys.executable)
        self.assertEqual(arguments, [
            sys.executable,
            str(project_root / "scripts/run_current_opportunity_dashboard.py"),
            "--data-dir", str(data_dir.resolve()),
            "--port", "43210",
        ])
        self.assertEqual(environment["DASHBOARD_SKIP_LOCAL_ENV"], "true")
        self.assertEqual(
            environment["MARKET_ROUTE_DATA_DIR"],
            str((data_dir / "routes").resolve()),
        )
        for name in (
            "ADMIN_ENABLED",
            "PUBLIC_ADD_TOKEN_ENABLED",
            "PUBLIC_QUALITY_RETRY_ENABLED",
            "PUBLIC_FACT_REFRESH_ENABLED",
        ):
            self.assertEqual(environment[name], "false")
        for name in (
            "MARKET_CEX_PRIVATE_FEE_PROFILE",
            "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE",
            "ADMIN_JOB_DIR",
            "TOKEN_REGISTRY_PATH",
        ):
            self.assertNotIn(name, environment)

    def test_cli_never_serves_when_published_bundle_cannot_be_reloaded(self):
        data_dir, fixture, joint = self._install_real_cex_run()
        actual_finalizer = opportunity_pipeline.finalize_cex_route_opportunities

        def finalize_then_corrupt(**kwargs):
            pointer = actual_finalizer(**kwargs)
            (data_dir / "routes/latest.json").write_bytes(b"corrupt\n")
            return pointer

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                    fixture["fee_profile_path"]
                ),
                "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                    fixture["inventory_profile_path"]
                ),
            }, clear=False))
            stack.enter_context(patch.object(
                opportunity_pipeline,
                "finalize_cex_route_opportunities",
                side_effect=finalize_then_corrupt,
            ))
            dashboard_exec = stack.enter_context(
                patch.object(opportunity_pipeline.os, "execve")
            )
            stack.enter_context(self.assertRaisesRegex(
                RouteOpportunityPipelineError,
                "cannot be reloaded",
            ))
            opportunity_pipeline.main([
                "--data-dir", str(data_dir),
                "--shadow-run-id", joint["pointer"]["run_id"],
                "--expected-joint-pointer-sha256", joint["pointer_sha256"],
                "--serve",
                "--port", "43210",
            ])

        dashboard_exec.assert_not_called()

    def test_cli_without_serve_keeps_pointer_output_and_does_not_exec(self):
        data_dir, fixture, joint = self._install_real_cex_run()
        output = io.StringIO()

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                    fixture["fee_profile_path"]
                ),
                "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                    fixture["inventory_profile_path"]
                ),
            }, clear=False))
            dashboard_exec = stack.enter_context(
                patch.object(opportunity_pipeline.os, "execve")
            )
            stack.enter_context(redirect_stdout(output))
            result = opportunity_pipeline.main([
                "--data-dir", str(data_dir),
                "--shadow-run-id", joint["pointer"]["run_id"],
                "--expected-joint-pointer-sha256", joint["pointer_sha256"],
            ])

        loaded = load_latest_complete_route_bundle(
            data_dir / "routes", core_root=data_dir / "routes/core"
        )
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output.getvalue()), loaded["pointer"])
        dashboard_exec.assert_not_called()

    def test_cli_dex_v2_publishes_then_execs_read_only_dashboard(self):
        data_dir, _cohort, joint = self._install_real_dex_run()
        output = io.StringIO()

        with ExitStack() as stack:
            dashboard_exec = stack.enter_context(patch.object(
                opportunity_pipeline.os,
                "execve",
                side_effect=RuntimeError("dashboard exec sentinel"),
            ))
            stack.enter_context(redirect_stdout(output))
            stack.enter_context(self.assertRaisesRegex(
                RuntimeError, "dashboard exec sentinel"
            ))
            opportunity_pipeline.main([
                "--finalizer", "eth-uniswap-v2-research",
                "--data-dir", str(data_dir),
                "--shadow-run-id", joint["pointer"]["run_id"],
                "--expected-joint-pointer-sha256", joint["pointer_sha256"],
                "--serve",
                "--port", "43211",
            ])

        loaded = load_latest_complete_route_bundle(
            data_dir / "routes", core_root=data_dir / "routes/core"
        )
        self.assertEqual(
            json.loads(output.getvalue().splitlines()[0]), loaded["pointer"]
        )
        self.assertEqual(
            dashboard_exec.call_args.args[1][-2:], ["--port", "43211"]
        )

    def _install_real_cex_run(
        self, *, sell_quantity="10000", both_directions=False
    ):
        data_dir = Path(self.temporary.name) / "real-data"
        core_root = data_dir / "routes/core"
        shadow_root = data_dir / "routes/shadow"
        raw_root = data_dir / "raw/route-cohort"
        staged_sources = Path(self.temporary.name) / "staged-sources"
        private_root = Path(self.temporary.name) / "private-profiles"
        fixture = _task7_cex_inputs(
            core_root, raw_root, staged_sources, private_root
        )
        cohort = copy.deepcopy(fixture["cohort"])
        run_id = cohort["raw_evidence_run_id"]

        if both_directions:
            forward = cohort["routes"][0]
            reverse = copy.deepcopy(forward)
            reverse["buy_market_id"] = forward["sell_market_id"]
            reverse["sell_market_id"] = forward["buy_market_id"]
            reverse["buy_reference_volume_usd"] = forward[
                "sell_reference_volume_usd"
            ]
            reverse["sell_reference_volume_usd"] = forward[
                "buy_reference_volume_usd"
            ]
            reverse["route_id"] = canonical_route_id(reverse)
            reverse_row = copy.deepcopy(cohort["route_rows"][0])
            reverse_row.update(reverse)
            cohort["routes"].append(reverse)
            cohort["route_rows"].append(reverse_row)

        sell_market = cohort["routes"][0]["sell_market_id"]
        sell_raw = _shadow_json_bytes({
            "retCode": 0,
            "result": {
                "s": "AAVEUSDT",
                "b": [["90", sell_quantity]],
                "a": [["91", "10000"]],
            },
        })
        sell_member = (
            raw_root / run_id / "accepted"
            / hashlib.sha256(sell_market.encode("utf-8")).hexdigest()
            / "response.json"
        )
        sell_member.write_bytes(sell_raw)
        next(
            leg for leg in cohort["legs"] if leg["market_id"] == sell_market
        )["raw_response_sha256"] = hashlib.sha256(sell_raw).hexdigest()

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
        identities = [
            SourceFileIdentity(
                path,
                index + 1,
                hashlib.sha256(path.encode("utf-8")).hexdigest(),
            )
            for index, path in enumerate(logical_paths)
        ]
        generation = _candidate_source_generation(identities)
        cohort["candidate_source_generation"] = generation
        cohort["source_state"]["candidate_source_generation"] = generation
        cohort["selection_window"] = {
            "start": "2026-07-03",
            "end": "2026-08-01",
        }
        for rows in (cohort["routes"], cohort["route_rows"]):
            for row in rows:
                row["candidate_source_generation"] = generation

        first_inputs = fixture["opportunity_inputs"][0]
        direction_by_market = {
            cohort["routes"][0][direction + "_market_id"]: direction
            for direction in ("buy", "sell")
        }
        typed_root = raw_root / run_id / "typed"
        typed_root.mkdir()
        manifest_members = []
        for leg in cohort["legs"]:
            market_id = leg["market_id"]
            direction = direction_by_market[market_id]
            venue = market_id.split(":", 2)[1]
            accepted = (
                raw_root / run_id / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
                / "response.json"
            ).read_bytes()
            payloads = {
                "cex_raw_book_response": accepted,
                "cex_market_rules": (
                    staged_sources
                    / first_inputs["source_members"][
                        direction + "_market_rules"
                    ]
                ).read_bytes(),
                "quote_usd_conversion": (
                    staged_sources
                    / first_inputs["source_members"][
                        direction + "_usd_conversion"
                    ]
                ).read_bytes(),
            }
            members = []
            for role, payload in sorted(payloads.items()):
                contract = TYPED_SOURCE_ROLE_CONTRACTS[role]
                filename = "{}-{}.json".format(venue, role)
                (typed_root / filename).write_bytes(payload)
                digest = hashlib.sha256(payload).hexdigest()
                member = {
                    "role": role,
                    "status": "observed",
                    "reason_code": None,
                    "filename": filename,
                    "sha256": digest,
                    "size": len(payload),
                    "logical_generation": digest,
                    "adapter_id": contract["adapter_id"],
                    "content_schema": contract["content_schema"],
                }
                members.append(member)
                manifest_members.append({"market_id": market_id, **{
                    key: member[key]
                    for key in (
                        "role", "filename", "sha256", "size",
                        "logical_generation", "adapter_id", "content_schema",
                    )
                }})
            leg["typed_source_lineage"] = {
                "schema": "route_leg_typed_source_lineage/v1",
                "members": members,
            }
        manifest_members.sort(key=lambda row: (row["market_id"], row["role"]))
        (raw_root / run_id / "typed-manifest.json").write_bytes(
            _shadow_json_bytes({
                "schema": "route_typed_source_manifest/v1",
                "raw_evidence_run_id": run_id,
                "member_count": len(manifest_members),
                "members": manifest_members,
            })
        )
        cohort = _rehash(cohort)
        core_pointer = publish_route_cohort_bundle(
            cohort, core_root=core_root
        )

        harness = object.__new__(
            route_publication_test.JointShadowPublicationTests
        )
        harness.generation = generation
        universe = route_publication_test.JointShadowPublicationTests._universe(
            harness, cohort
        )
        universe_sha = route_universe_sha256(universe)
        baseline = {
            "schema": "route_shadow_baseline_manifest/v1",
            "calculation_version": "route_shadow_inputs/v1",
            "candidate_source_generation": generation,
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
                for row in identities
            ],
            "route_universe_sha256": universe_sha,
        }
        write_run_universe(shadow_root, run_id, universe, baseline)
        evaluated_at = "2026-08-01T12:02:00Z"
        cost = build_unavailable_route_cost_evidence_manifest(
            universe=universe,
            run_id=run_id,
            route_cohort_id=cohort["route_cohort_id"],
            phase="canary",
            candidate_source_generation=generation,
            route_universe_sha256=universe_sha,
            evaluated_at=evaluated_at,
        )
        cost_bytes = _shadow_json_bytes(cost)
        (shadow_root / "runs" / run_id / "route-cost-evidence.json").write_bytes(
            cost_bytes
        )
        audit = build_shadow_audit(
            cohort,
            core_pointer=core_pointer,
            run={
                "run_id": run_id,
                "phase_state_sha256": hashlib.sha256(
                    b"route-shadow-phase/implicit-canary/v1\n"
                ).hexdigest(),
                "phase_transition_id": None,
                "route_universe_sha256": universe_sha,
                "baseline_manifest_sha256": hashlib.sha256(
                    _shadow_json_bytes(baseline)
                ).hexdigest(),
                "candidate_source_generation": generation,
                "route_cost_evidence_sha256": hashlib.sha256(
                    cost_bytes
                ).hexdigest(),
            },
            phase="canary",
            audit_finished_at="2026-08-01T12:02:01Z",
        )
        joint = publish_shadow_result(
            shadow_root, core_pointer=core_pointer, audit=audit
        )
        return data_dir, fixture, joint

    def _install_real_dex_run(self, *, typed_context_price=None):
        data_dir = Path(self.temporary.name) / "real-dex-data"
        core_root = data_dir / "routes/core"
        shadow_root = data_dir / "routes/shadow"
        raw_root = data_dir / "raw/route-cohort"
        cohort = copy.deepcopy(route_publication_test._dex_cohort())
        run_id = cohort["raw_evidence_run_id"]

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
        identities = [
            SourceFileIdentity(
                path,
                index + 1,
                hashlib.sha256(path.encode("utf-8")).hexdigest(),
            )
            for index, path in enumerate(logical_paths)
        ]
        generation = _candidate_source_generation(identities)
        cohort["candidate_source_generation"] = generation
        cohort["source_state"]["candidate_source_generation"] = generation
        cohort["selection_window"] = {
            "start": "2026-07-03",
            "end": "2026-08-01",
        }
        for rows in (cohort["routes"], cohort["route_rows"]):
            for row in rows:
                row["candidate_source_generation"] = generation

        typed_root = raw_root / run_id / "typed"
        typed_root.mkdir(parents=True)
        manifest_members = []
        target_address = "0x" + "1" * 40
        quote_address = "0x" + "2" * 40
        price_source_hash = "e" * 64
        for index, leg in enumerate(cohort["legs"], start=1):
            market_id = leg["market_id"]
            pool_address = market_id.split(":", 4)[3]
            raw_bytes = _shadow_json_bytes({
                "fixture": "pinned-dex-pool-rpc/v1",
                "market_id": market_id,
            })
            raw_sha256 = hashlib.sha256(raw_bytes).hexdigest()
            accepted = (
                raw_root / run_id / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            )
            accepted.mkdir(parents=True)
            (accepted / "response.json").write_bytes(raw_bytes)

            context = {
                "schema": "route_collector_context/v1",
                "snapshot_id": "tvl-snapshot-001",
                "request_started_at": "2026-08-01T11:59:58+00:00",
                "observed_at": "2026-08-01T12:00:01+00:00",
                "response_received_at": "2026-08-01T12:00:02+00:00",
                "status": "observed",
                "reason_code": "observed",
                "pool_name": "UNI / USDC",
                "base_token_id": "eth_" + target_address,
                "quote_token_id": "eth_" + quote_address,
                "base_token_price_usd": "95",
                "quote_token_price_usd": "1",
                "tvl_method": "geckoterminal_reserve_in_usd",
                "source": "retained local fixture",
                "source_endpoint": "https://api.example.test/pools",
                "raw_response_sha256": price_source_hash,
            }
            leg.update({
                "status": "observed",
                "available": True,
                "reason_code": None,
                "snapshot_id": run_id,
                "source_endpoint": "https://rpc.example.test/eth",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "fixed_block_number": "123",
                "fixed_block_timestamp": "2026-08-01T12:00:00Z",
                "raw_response_sha256": raw_sha256,
                "chain": "eth",
                "dex": "uniswap_v2",
                "pool_address": pool_address,
                "block_timestamp": "2026-08-01T12:00:00Z",
                "target_token_position": "token0",
                "target_token_address": target_address,
                "target_token_side": "base",
                "token0_address": target_address,
                "token0_symbol": "UNI",
                "token0_decimals": "18",
                "token0_price_usd": "95",
                "token1_address": quote_address,
                "token1_symbol": "USDC",
                "token1_decimals": "6",
                "token1_price_usd": "1",
                "usd_price_source_snapshot_id": context["snapshot_id"],
                "usd_price_observed_at": context["observed_at"],
                "usd_price_source": context["source"],
                "usd_price_source_endpoint": context["source_endpoint"],
                "usd_price_raw_response_sha256": price_source_hash,
                "collector_context": context,
            })
            pool = _v2_pool_payload(
                pool_address=pool_address,
                raw_response_sha256=raw_sha256,
                reserve_quote_raw=(
                    90_000_000 * 10**6
                    if index == 1 else 100_000_000 * 10**6
                ),
            )
            rules = {
                "schema": "route_dex_market_rules_source/v1",
                "market_id": market_id,
                "base_asset": "UNI",
                "quote_asset": "USDC",
                "base_token_address": target_address,
                "quote_token_address": quote_address,
                "base_unit_decimals": 18,
                "quote_unit_decimals": 6,
                "base_increment": "0.000000000000000001",
                "quote_increment": "0.000001",
                "min_base_quantity": "0",
                "min_quote_notional": "0",
                "increment_source": "fixed_block_token_decimals",
                "minimum_source": "dex_protocol_no_additional_order_minimum",
                "observed_at": "2026-08-01T12:00:00Z",
                "valid_until": "2026-08-01T12:02:00Z",
                "raw_response_sha256": raw_sha256,
            }
            conversion = {
                "schema": "route_dex_usd_conversion_source/v1",
                "market_id": market_id,
                "target_asset": "UNI",
                "target_token_address": target_address,
                "quote_asset": "USDC",
                "quote_token_address": quote_address,
                "usd_per_quote": "1",
                "value_status": "measured",
                "observed_at": "2026-08-01T12:00:01Z",
                "valid_until": "2026-08-01T14:00:01Z",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "source": context["source"],
                "source_snapshot_id": context["snapshot_id"],
                "source_raw_response_sha256": price_source_hash,
                "state_raw_response_sha256": raw_sha256,
            }
            typed_context = copy.deepcopy(context)
            if typed_context_price is not None:
                typed_context["base_token_price_usd"] = typed_context_price
            payloads = {
                "dex_market_rules": _shadow_json_bytes(rules),
                "dex_pool_state": _shadow_json_bytes(pool),
                "dex_usd_conversion": _shadow_json_bytes(conversion),
                "dex_usd_price_context": _shadow_json_bytes(typed_context),
            }
            members = []
            for role, payload in sorted(payloads.items()):
                contract = TYPED_SOURCE_ROLE_CONTRACTS[role]
                filename = "dex-leg{}-{}.json".format(index, role)
                (typed_root / filename).write_bytes(payload)
                digest = hashlib.sha256(payload).hexdigest()
                logical_generation = (
                    pool["state_id"].split(":", 1)[1]
                    if role == "dex_pool_state" else digest
                )
                member = {
                    "role": role,
                    "status": "observed",
                    "reason_code": None,
                    "filename": filename,
                    "sha256": digest,
                    "size": len(payload),
                    "logical_generation": logical_generation,
                    "adapter_id": contract["adapter_id"],
                    "content_schema": contract["content_schema"],
                }
                members.append(member)
                manifest_members.append({
                    "market_id": market_id,
                    **{
                        key: member[key]
                        for key in (
                            "role", "filename", "sha256", "size",
                            "logical_generation", "adapter_id",
                            "content_schema",
                        )
                    },
                })
            leg["typed_source_lineage"] = {
                "schema": TYPED_SOURCE_LINEAGE_SCHEMA_V2,
                "members": members,
            }

        for row in cohort["route_rows"]:
            row["skew_seconds"] = "0"

        manifest_members.sort(key=lambda row: (row["market_id"], row["role"]))
        (raw_root / run_id / "typed-manifest.json").write_bytes(
            _shadow_json_bytes({
                "schema": "route_typed_source_manifest/v1",
                "raw_evidence_run_id": run_id,
                "member_count": len(manifest_members),
                "members": manifest_members,
            })
        )
        cohort = _rehash(cohort)
        core_pointer = publish_route_cohort_bundle(
            cohort, core_root=core_root
        )

        harness = object.__new__(
            route_publication_test.JointShadowPublicationTests
        )
        harness.generation = generation
        universe = route_publication_test.JointShadowPublicationTests._universe(
            harness, cohort
        )
        universe_sha = route_universe_sha256(universe)
        baseline = {
            "schema": "route_shadow_baseline_manifest/v1",
            "calculation_version": "route_shadow_inputs/v1",
            "candidate_source_generation": generation,
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
                for row in identities
            ],
            "route_universe_sha256": universe_sha,
        }
        write_run_universe(shadow_root, run_id, universe, baseline)
        evaluated_at = "2026-08-01T12:00:03Z"
        cost = build_unavailable_route_cost_evidence_manifest(
            universe=universe,
            run_id=run_id,
            route_cohort_id=cohort["route_cohort_id"],
            phase="canary",
            candidate_source_generation=generation,
            route_universe_sha256=universe_sha,
            evaluated_at=evaluated_at,
        )
        cost_bytes = _shadow_json_bytes(cost)
        (shadow_root / "runs" / run_id / "route-cost-evidence.json").write_bytes(
            cost_bytes
        )
        audit = build_shadow_audit(
            cohort,
            core_pointer=core_pointer,
            run={
                "run_id": run_id,
                "phase_state_sha256": hashlib.sha256(
                    b"route-shadow-phase/implicit-canary/v1\n"
                ).hexdigest(),
                "phase_transition_id": None,
                "route_universe_sha256": universe_sha,
                "baseline_manifest_sha256": hashlib.sha256(
                    _shadow_json_bytes(baseline)
                ).hexdigest(),
                "candidate_source_generation": generation,
                "route_cost_evidence_sha256": hashlib.sha256(
                    cost_bytes
                ).hexdigest(),
            },
            phase="canary",
            audit_finished_at="2026-08-01T12:00:04Z",
        )
        joint = publish_shadow_result(
            shadow_root, core_pointer=core_pointer, audit=audit
        )
        return data_dir, cohort, joint

    def _pinned_views(self, *, dex=False):
        cohort_id = "cohort:" + "b" * 64
        generation = "c" * 64
        manifest_sha256 = "d" * 64
        buy_market = "cex:binance:AAVE/USDT"
        sell_market = (
            "dex:eth:uniswap_v2:pool:AAVE"
            if dex else "cex:bybit:AAVE/USDT"
        )
        cohort = {
            "route_cohort_id": cohort_id,
            "candidate_source_generation": generation,
            "raw_evidence_run_id": self.run_id,
            "routes": [{
                "route_id": "route-1",
                "buy_market_id": buy_market,
                "sell_market_id": sell_market,
            }],
            "legs": [
                {"market_id": buy_market, "market_type": "cex"},
                {
                    "market_id": sell_market,
                    "market_type": "dex" if dex else "cex",
                },
            ],
        }
        core_pointer = {
            "schema": "route_cohort_core_pointer/v1",
            "bundle_stage": "route_cohort_core/v1",
            "route_cohort_id": cohort_id,
            "manifest_sha256": manifest_sha256,
        }
        cost_bytes = json.dumps(
            {"evaluated_at": "2026-08-01T12:02:00Z"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        cost_path = (
            self.routes_root / "shadow/runs" / self.run_id
            / "route-cost-evidence.json"
        )
        cost_path.parent.mkdir(parents=True, exist_ok=True)
        cost_path.write_bytes(cost_bytes)
        shadow = {
            "pointer_sha256": self.expected_joint_sha256,
            "pointer": {
                "run_id": self.run_id,
                "route_cohort_id": cohort_id,
                "candidate_source_generation": generation,
                "core_manifest_sha256": manifest_sha256,
                "core_pointer_sha256": hashlib.sha256(
                    _physical_json(core_pointer)
                ).hexdigest(),
                "route_cost_evidence_sha256": hashlib.sha256(
                    cost_bytes
                ).hexdigest(),
            },
            "cohort": copy.deepcopy(cohort),
        }
        latest_core = {
            "cohort": copy.deepcopy(cohort),
            "manifest_sha256": manifest_sha256,
            "pointer": core_pointer,
        }
        return shadow, latest_core

    def _assert_post_confirmation_mutation_fails(self, mutate):
        data_dir, fixture, joint = self._install_real_cex_run()
        latest_path = data_dir / "routes/latest.json"
        latest_path.write_bytes(self.old_pointer_bytes)
        actual_publisher = opportunity_pipeline.publish_complete_route_bundle

        def mutate_then_publish(**kwargs):
            mutate(data_dir, fixture, joint)
            return actual_publisher(**kwargs)

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                    fixture["fee_profile_path"]
                ),
                "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                    fixture["inventory_profile_path"]
                ),
            }, clear=False))
            stack.enter_context(patch(
                "scripts.route_opportunity_pipeline."
                "publish_complete_route_bundle",
                side_effect=mutate_then_publish,
            ))
            with self.assertRaises(RouteOpportunityPipelineError):
                finalize_cex_route_opportunities(
                    data_dir=data_dir,
                    shadow_run_id=joint["pointer"]["run_id"],
                    expected_joint_pointer_sha256=joint["pointer_sha256"],
                )

        self.assertEqual(latest_path.read_bytes(), self.old_pointer_bytes)

    def test_dex_core_is_rejected_before_finalizer_and_preserves_pointer(self):
        shadow, latest_core = self._pinned_views(dex=True)
        with ExitStack() as stack:
            stack.enter_context(patch(
                "scripts.route_opportunity_pipeline.load_shadow_result",
                return_value=shadow,
            ))
            stack.enter_context(patch(
                "scripts.route_opportunity_pipeline.load_latest_route_cohort",
                return_value=latest_core,
            ))
            publisher = stack.enter_context(patch(
                "scripts.route_opportunity_pipeline.publish_complete_route_bundle"
            ))
            with self.assertRaisesRegex(
                RouteOpportunityPipelineError, "CEX-only"
            ):
                finalize_cex_route_opportunities(
                    data_dir=self.data_dir,
                    shadow_run_id=self.run_id,
                    expected_joint_pointer_sha256=self.expected_joint_sha256,
                )

        self.assertEqual(publisher.call_count, 0)
        self.assertEqual(self.public_pointer.read_bytes(), self.old_pointer_bytes)

    def test_lineage_and_sidecar_drift_stop_before_finalizer(self):
        for case in ("cost-sidecar", "cohort", "generation", "hash"):
            with self.subTest(case=case):
                shadow, latest_core = self._pinned_views()
                if case == "cost-sidecar":
                    sidecar = (
                        self.routes_root / "shadow/runs" / self.run_id
                        / "route-cost-evidence.json"
                    )
                    sidecar.write_bytes(sidecar.read_bytes() + b"\n")
                elif case == "cohort":
                    latest_core["cohort"]["raw_evidence_run_id"] = "other-run"
                elif case == "generation":
                    shadow["pointer"]["candidate_source_generation"] = "e" * 64
                else:
                    shadow["pointer_sha256"] = "f" * 64

                with ExitStack() as stack:
                    stack.enter_context(patch(
                        "scripts.route_opportunity_pipeline.load_shadow_result",
                        return_value=shadow,
                    ))
                    stack.enter_context(patch(
                        "scripts.route_opportunity_pipeline.load_latest_route_cohort",
                        return_value=latest_core,
                    ))
                    stack.enter_context(patch(
                        "scripts.route_opportunity_pipeline."
                        "_read_shadow_run_evidence",
                        side_effect=lambda _root, _run_id: {
                            "cost_evidence": {
                                "evaluated_at": "2026-08-01T12:02:00Z"
                            },
                            "cost_evidence_bytes": (
                                self.routes_root
                                / "shadow/runs"
                                / self.run_id
                                / "route-cost-evidence.json"
                            ).read_bytes(),
                        },
                    ))
                    publisher = stack.enter_context(patch(
                        "scripts.route_opportunity_pipeline."
                        "publish_complete_route_bundle"
                    ))
                    with self.assertRaisesRegex(
                        RouteOpportunityPipelineError,
                        "evidence|lineage|hash|sidecar",
                    ):
                        finalize_cex_route_opportunities(
                            data_dir=self.data_dir,
                            shadow_run_id=self.run_id,
                            expected_joint_pointer_sha256=(
                                self.expected_joint_sha256
                            ),
                        )

                self.assertEqual(publisher.call_count, 0)
                self.assertEqual(
                    self.public_pointer.read_bytes(), self.old_pointer_bytes
                )

    def test_all_negative_cex_grid_is_fully_published(self):
        from dashboard import server

        data_dir, fixture, joint = self._install_real_cex_run()
        (data_dir / "routes/shadow/latest.json").write_bytes(
            b'{"unrelated":"moving-shadow-pointer"}\n'
        )
        with patch.dict(os.environ, {
            "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                fixture["fee_profile_path"]
            ),
            "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                fixture["inventory_profile_path"]
            ),
        }, clear=False):
            result = finalize_cex_route_opportunities(
                data_dir=data_dir,
                shadow_run_id=joint["pointer"]["run_id"],
                expected_joint_pointer_sha256=joint["pointer_sha256"],
            )

        loaded = load_latest_complete_route_bundle(
            data_dir / "routes", core_root=data_dir / "routes/core"
        )["bundle"]
        self.assertEqual(result["schema"], "route_opportunity_pointer/v1")
        self.assertEqual(len(loaded["opportunities"]), 5)
        self.assertEqual(
            {row["requested_notional_usd"] for row in loaded["opportunities"]},
            {"1000", "5000", "10000", "50000", "100000"},
        )
        self.assertEqual(
            {row["opportunity_class"] for row in loaded["opportunities"]},
            {"research_estimate"},
        )
        self.assertTrue(all(
            float(row["strict_net_edge_bps"]) < 0
            for row in loaded["opportunities"]
        ))
        server.clear_runtime_caches()
        try:
            with patch.dict(
                server.os.environ,
                {"MARKET_ROUTE_DATA_DIR": str(data_dir / "routes")},
                clear=True,
            ):
                payload = server.build_route_opportunities(notional="1000")
        finally:
            server.clear_runtime_caches()
        self.assertEqual(
            payload["availability"], {"status": "available", "reason": None}
        )
        self.assertEqual(len(payload["routes"]), 1)
        self.assertEqual(
            payload["routes"][0]["opportunity_class"], "research_estimate"
        )

    def test_pinned_dex_v2_terminal_cost_grid_is_published_and_visible(self):
        from dashboard import server

        data_dir, cohort, joint = self._install_real_dex_run()
        result = (
            opportunity_pipeline.finalize_eth_uniswap_v2_research_opportunities(
                data_dir=data_dir,
                shadow_run_id=joint["pointer"]["run_id"],
                expected_joint_pointer_sha256=joint["pointer_sha256"],
            )
        )

        loaded = load_latest_complete_route_bundle(
            data_dir / "routes", core_root=data_dir / "routes/core"
        )["bundle"]
        self.assertEqual(result["schema"], "route_opportunity_pointer/v1")
        self.assertEqual(
            len(loaded["opportunities"]),
            len(cohort["routes"]) * 5,
        )
        self.assertEqual(
            {
                row["requested_notional_usd"]
                for row in loaded["opportunities"]
            },
            {"1000", "5000", "10000", "50000", "100000"},
        )
        self.assertEqual(
            {row["opportunity_class"] for row in loaded["opportunities"]},
            {"unavailable"},
        )
        self.assertEqual(
            loaded["input_generations"]["adapter_versions"],
            {
                "dex_market_rules": "route_dex_market_rules_source/v1",
                "dex_pool_state": "route_quantity_quote_for_v2_pool/v1",
                "dex_usd_conversion": (
                    "route_dex_usd_conversion_source/v1"
                ),
                "dex_usd_price_context": (
                    "route_dex_usd_price_context/v1"
                ),
            },
        )
        self.assertTrue(all(
            row["strict_eligible"] is False
            and row["strict_ready_for_publication"] is False
            and row["publication_attestation_sha256"] is None
            and row["primary_reason"] == "cost_components_incomplete"
            and row["reason_codes"] == ["cost_components_incomplete"]
            and row["component_reasons"] == [
                "mev_scenario_unavailable:route:mev_buffer:"
                "strict_cost_adapter_unsupported"
            ]
            for row in loaded["opportunities"]
        ))
        for opportunity in loaded["opportunities"]:
            self.assertRegex(
                opportunity["buy_state_id"],
                r"^dex-v2-quantity:[0-9a-f]{64}$",
            )
            self.assertRegex(
                opportunity["sell_state_id"],
                r"^dex-v2-quantity:[0-9a-f]{64}$",
            )
            self.assertRegex(
                opportunity["buy_usd_projection_sha256"],
                r"^[0-9a-f]{64}$",
            )
            self.assertRegex(
                opportunity["sell_usd_projection_sha256"],
                r"^[0-9a-f]{64}$",
            )
            costs = [
                row for row in loaded["cost_components"]
                if row["opportunity_id"] == opportunity["opportunity_id"]
            ]
            self.assertEqual(len(costs), 10)
            self.assertEqual(
                {(row["leg"], row["component_type"]) for row in costs},
                {
                    ("buy", "network_gas"),
                    ("buy", "pool_swap_fee"),
                    ("buy", "router_or_integrator_fee"),
                    ("buy", "token_transfer_tax"),
                    ("route", "mev_buffer"),
                    ("route", "rebalancing_or_transfer"),
                    ("sell", "network_gas"),
                    ("sell", "pool_swap_fee"),
                    ("sell", "router_or_integrator_fee"),
                    ("sell", "token_transfer_tax"),
                },
            )
            self.assertTrue(all(
                row["opportunity_id"] == opportunity["opportunity_id"]
                and row["requested_notional_usd"]
                == opportunity["requested_notional_usd"]
                and row["target_token_quantity"]
                == opportunity["target_token_quantity"]
                and row["source_record_sha256"]
                == joint["pointer"]["route_cost_evidence_sha256"]
                for row in costs
            ))
        self.assertTrue(all(
            component["value_status"] == "unavailable"
            and component["amount_usd"] is None
            and component["rate_bps"] is None
            and component["reason_code"]
            == "strict_cost_adapter_unsupported"
            for component in loaded["cost_components"]
        ))

        server.clear_runtime_caches()
        try:
            with patch.dict(
                server.os.environ,
                {"MARKET_ROUTE_DATA_DIR": str(data_dir / "routes")},
                clear=True,
            ):
                payload = server.build_route_opportunities(notional="1000")
        finally:
            server.clear_runtime_caches()
        self.assertEqual(
            payload["availability"], {"status": "available", "reason": None}
        )
        self.assertEqual(len(payload["routes"]), len(cohort["routes"]))
        self.assertEqual(
            {row["route_type"] for row in payload["routes"]}, {"dex_dex"}
        )
        self.assertEqual(
            {row["opportunity_class"] for row in payload["routes"]},
            {"unavailable"},
        )

    def test_observed_dex_transcripts_and_explicit_mev_close_research_grid(self):
        data_dir, cohort, joint = self._install_real_dex_run()
        now = "2026-08-01T12:00:03Z"
        sources, _retained = opportunity_pipeline._load_dex_sources(
            root=data_dir,
            cohort=cohort,
            source_root=(
                data_dir / "raw/route-cohort"
                / cohort["raw_evidence_run_id"] / "typed"
            ),
            now=now,
        )
        cost_evidence, outcomes = _observed_dex_cost_sidecar(cohort, sources)

        built = opportunity_pipeline._build_dex_inputs(
            cohort=cohort,
            core_manifest_sha256=joint["pointer"]["core_manifest_sha256"],
            sources=sources,
            outcomes=outcomes,
            cost_evidence=cost_evidence,
            research_mev_bps="25",
            now=now,
        )

        self.assertEqual(len(built), len(cohort["routes"]) * 5)
        for item in built:
            opportunity = item["classified_opportunity"]
            costs = item["build_inputs"]["cost_components"]
            notional = Decimal(opportunity["requested_notional_usd"])
            self.assertEqual(opportunity["opportunity_class"], "research_estimate")
            self.assertEqual(opportunity["scenario_cost_completeness"], "complete")
            self.assertEqual(opportunity["cost_completeness"], "incomplete")
            self.assertIsNotNone(opportunity["gross_edge_usd"])
            self.assertEqual(opportunity["strict_nonembedded_cost_usd"], "2.52")
            self.assertEqual(
                Decimal(opportunity["research_assumed_cost_usd"]),
                notional * Decimal("25") / Decimal("10000"),
            )
            self.assertEqual(
                Decimal(opportunity["research_net_edge_usd"]),
                Decimal(opportunity["gross_edge_usd"])
                - Decimal("2.52")
                - notional * Decimal("25") / Decimal("10000"),
            )
            self.assertFalse(opportunity["strict_eligible"])
            self.assertFalse(opportunity["strict_ready_for_publication"])
            self.assertIsNone(opportunity["publication_attestation_sha256"])
            self.assertEqual(
                opportunity["primary_reason"],
                "quantity_quote_evidence_not_strict",
            )
            self.assertIn(
                "mode_expected_request_unavailable",
                opportunity["reason_codes"],
            )
            self.assertEqual(len(costs), 10)
            by_key = {
                (row["leg"], row["component_type"]): row for row in costs
            }
            for leg in ("buy", "sell"):
                self.assertEqual(
                    by_key[(leg, "pool_swap_fee")]["value_status"],
                    "measured",
                )
                self.assertTrue(
                    by_key[(leg, "pool_swap_fee")][
                        "embedded_in_leg_quote"
                    ]
                )
                self.assertEqual(
                    by_key[(leg, "network_gas")]["amount_usd"], "1.26"
                )
                self.assertEqual(
                    by_key[(leg, "network_gas")]["value_status"], "quoted"
                )
                self.assertEqual(
                    by_key[(leg, "router_or_integrator_fee")][
                        "value_status"
                    ],
                    "not_applicable",
                )
                self.assertEqual(
                    by_key[(leg, "token_transfer_tax")]["value_status"],
                    "not_applicable",
                )
            mev = by_key[("route", "mev_buffer")]
            self.assertEqual(mev["value_status"], "assumed")
            self.assertEqual(mev["rate_bps"], "25")
            self.assertEqual(
                Decimal(mev["amount_usd"]),
                notional * Decimal("25") / Decimal("10000"),
            )
            self.assertEqual(mev["source"], "explicit operator research scenario")
            self.assertNotEqual(mev["rate_bps"], "9999")

        source_root = (
            data_dir / "raw/route-cohort"
            / cohort["raw_evidence_run_id"] / "typed"
        )
        publish_complete_route_bundle(
            core_root=data_dir / "routes/core",
            routes_root=data_dir / "routes",
            raw_root=data_dir / "raw/route-cohort",
            opportunity_inputs=built,
            source_root=source_root,
        )
        loaded = load_latest_complete_route_bundle(
            data_dir / "routes", core_root=data_dir / "routes/core"
        )
        from dashboard.opportunity_facts import build_opportunity_payload

        payload = build_opportunity_payload(
            loaded["opportunities"],
            manifest=loaded["manifest"],
            legs=loaded["legs"],
            cost_components=loaded["cost_components"],
            route_candidates=loaded["bundle"]["routes"],
            manifest_sha256=loaded["manifest_sha256"],
            notional_usd="1000",
            now=datetime(2026, 8, 1, 12, 0, 3, tzinfo=timezone.utc),
        )
        self.assertEqual(len(payload["routes"]), len(cohort["routes"]))
        for route in payload["routes"]:
            self.assertEqual(
                route["availability"], {"status": "available", "reason": None}
            )
            self.assertEqual(route["opportunity_class"], "research_estimate")
            self.assertIsNotNone(route["gross_edge_usd"])
            self.assertIsNotNone(route["net_edge_usd"])
            self.assertEqual(
                route["cost_breakdown"]["strict_nonembedded_usd"], "2.52"
            )
            self.assertEqual(
                route["cost_breakdown"]["research_assumed_usd"], "2.5"
            )
            self.assertEqual(len(route["cost_components"]), 10)

    def test_observed_dex_cost_projection_fails_closed_and_never_invents_mev(self):
        data_dir, cohort, joint = self._install_real_dex_run()
        now = "2026-08-01T12:00:03Z"
        sources, _retained = opportunity_pipeline._load_dex_sources(
            root=data_dir,
            cohort=cohort,
            source_root=(
                data_dir / "raw/route-cohort"
                / cohort["raw_evidence_run_id"] / "typed"
            ),
            now=now,
        )
        cost_evidence, outcomes = _observed_dex_cost_sidecar(cohort, sources)
        kwargs = {
            "cohort": cohort,
            "core_manifest_sha256": joint["pointer"]["core_manifest_sha256"],
            "sources": sources,
            "outcomes": outcomes,
            "cost_evidence": cost_evidence,
            "research_mev_bps": None,
            "now": now,
        }

        built = opportunity_pipeline._build_dex_inputs(**kwargs)
        for item in built:
            opportunity = item["classified_opportunity"]
            mev = next(
                row
                for row in item["build_inputs"]["cost_components"]
                if row["component_type"] == "mev_buffer"
            )
            self.assertEqual(opportunity["opportunity_class"], "research_estimate")
            self.assertEqual(opportunity["scenario_cost_completeness"], "incomplete")
            self.assertIsNotNone(opportunity["strict_net_edge_usd"])
            self.assertIsNone(opportunity["research_net_edge_usd"])
            self.assertEqual(mev["value_status"], "unavailable")
            self.assertIsNone(mev["amount_usd"])
            self.assertIsNone(mev["rate_bps"])
            self.assertEqual(mev["reason_code"], "mev_protection_unavailable")

        mutated = copy.deepcopy(cost_evidence)
        mutated["transcripts"][0]["gas_evidence"]["gas_units"] = "1"
        with self.assertRaisesRegex(
            RouteOpportunityPipelineError, "sidecar hash differs"
        ):
            opportunity_pipeline._build_dex_inputs(
                **{**kwargs, "cost_evidence": mutated}
            )

    def test_authenticated_route_cost_kat_projects_exact_dex_cost_rows(self):
        from tests.test_route_cost_evidence import (
            supported_observed_manifest,
        )

        cost_evidence, universe, retained = supported_observed_manifest()
        outcomes = opportunity_pipeline.replay_route_cost_coverage_outcomes(
            cost_evidence,
            universe=universe,
            expected_run_id=cost_evidence["run_id"],
            expected_route_cohort_id=cost_evidence["route_cohort_id"],
            expected_phase=cost_evidence["phase"],
            expected_candidate_source_generation=cost_evidence[
                "candidate_source_generation"
            ],
            expected_route_universe_sha256=cost_evidence[
                "route_universe_sha256"
            ],
            retained_typed_pool_state_members=retained,
        )
        route = universe["routes"][0]
        coverage = next(
            row for row in outcomes
            if row["route_id"] == route["route_id"]
            and row["requested_notional_usd"] == "1000"
        )
        states = {
            market_id: opportunity_pipeline._frozen_v2_state(
                json.loads(member["payload"])
            )
            for market_id, member in retained.items()
        }
        target_transcript = next(
            row for row in cost_evidence["transcripts"]
            if row["market_id"] == route["buy_market_id"]
            and row["direction"] == "buy"
            and row["requested_notional_usd"] == "1000"
        )
        target = CommonTarget(
            asset="AAA",
            unit_decimals=int(
                target_transcript["simulation_target_unit_decimals"]
            ),
            raw_quantity=int(
                target_transcript["simulation_target_raw_quantity"]
            ),
            lattice_raw=int(
                target_transcript["simulation_target_lattice_raw"]
            ),
        )
        sources = {
            market_id: None for market_id in states
        }
        for market_id, state in states.items():
            target_address = target_transcript[
                "simulation_target_token_address"
            ]
            target_is_token0 = target_address == state.token0_address
            quote_address = (
                state.token1_address if target_is_token0 else state.token0_address
            )
            target_decimals = (
                state.token0_decimals
                if target_is_token0 else state.token1_decimals
            )
            quote_decimals = (
                state.token1_decimals
                if target_is_token0 else state.token0_decimals
            )
            sources[market_id] = {
                "state": state,
                "pool_sha256": retained[market_id]["descriptor"]["sha256"],
                "rules": MarketRules(
                    market_id=market_id,
                    base_asset="AAA",
                    quote_asset="WETH",
                    base_unit_decimals=target_decimals,
                    quote_unit_decimals=quote_decimals,
                    base_increment=Decimal(1) / (Decimal(10) ** target_decimals),
                    quote_increment=Decimal(1) / (Decimal(10) ** quote_decimals),
                    min_base_quantity=Decimal(0),
                    min_quote_notional=Decimal(0),
                    observed_at=state.observed_at,
                    valid_until="2026-08-01T12:05:00Z",
                    source_record_sha256="9" * 64,
                ),
                "conversion": {
                    "target_token_address": target_address,
                    "quote_token_address": quote_address,
                    "quote_asset": "WETH",
                    "observed_at": "2026-08-01T12:00:00Z",
                    "valid_until": "2026-08-01T12:05:00Z",
                    "source": "authenticated local KAT",
                },
                "usd_rate": Decimal("3000"),
                "usd_sha": "a" * 64,
                # Deliberately differs from the KAT's pool-derived target.
                "reference_price_usd": Decimal("95"),
                "filenames": {
                    role: "{}-{}.json".format(index, role)
                    for index, role in enumerate(sorted({
                        "dex_market_rules", "dex_pool_state",
                        "dex_usd_conversion", "dex_usd_price_context",
                    }))
                },
            }
        cohort = {
            "route_cohort_id": cost_evidence["route_cohort_id"],
            "collection_completed_at": cost_evidence["evaluated_at"],
            "requested_notionals_usd": list(universe[
                "requested_notionals_usd"
            ]),
            "routes": [copy.deepcopy(route)],
            "legs": [
                {
                    "market_id": market_id,
                    "market_type": "dex",
                    "status": "observed",
                    "available": True,
                    "reason_code": None,
                    "state_observed_at": state.observed_at,
                    "raw_response_sha256": state.raw_response_sha256,
                    "snapshot_id": "kat-snapshot",
                }
                for market_id, state in sorted(states.items())
            ],
        }
        built = opportunity_pipeline._build_dex_inputs(
            cohort=cohort,
            core_manifest_sha256="b" * 64,
            sources=sources,
            outcomes=outcomes,
            cost_evidence=cost_evidence,
            research_mev_bps="25",
            now=cost_evidence["evaluated_at"],
        )
        self.assertEqual(len(built), 5)
        self.assertTrue(all(
            item["classified_opportunity"]["opportunity_class"]
            == "research_estimate"
            and item["classified_opportunity"][
                "scenario_cost_completeness"
            ] == "complete"
            and item["classified_opportunity"]["research_net_edge_usd"]
            is not None
            for item in built
        ))
        self.assertTrue(any(
            Decimal(item["classified_opportunity"]["research_net_edge_usd"])
            < 0
            for item in built
        ))
        rows = built[0]["build_inputs"]["cost_components"]
        self.assertEqual(len(rows), 10)
        by_key = {
            (row["leg"], row["component_type"]): row for row in rows
        }
        self.assertEqual(
            by_key[("buy", "network_gas")]["amount_usd"],
            "0.000000012789",
        )
        self.assertEqual(
            by_key[("sell", "network_gas")]["amount_usd"],
            "0.000000012789",
        )
        self.assertEqual(
            by_key[("buy", "pool_swap_fee")]["amount_usd"], "3"
        )
        self.assertEqual(
            by_key[("sell", "pool_swap_fee")]["amount_usd"], "3"
        )
        self.assertEqual(
            by_key[("route", "mev_buffer")]["amount_usd"], "2.5"
        )
        with self.assertRaisesRegex(
            RouteOpportunityPipelineError, "target or state differs"
        ):
            opportunity_pipeline._observed_dex_cost_components(
                cohort_id=cost_evidence["route_cohort_id"],
                route=route,
                requested_notional_usd=Decimal("1000"),
                common_target=CommonTarget(
                    asset="AAA",
                    unit_decimals=target.unit_decimals,
                    raw_quantity=target.raw_quantity + target.lattice_raw,
                    lattice_raw=target.lattice_raw,
                ),
                coverage=coverage,
                projection_index=(
                    opportunity_pipeline._dex_cost_projection_index(
                        cost_evidence, outcomes
                    )
                ),
                sources=sources,
                research_mev_bps="25",
            )

    def test_dex_supported_cost_pool_filter_is_exact_and_fail_closed(self):
        retained = {
            "dex:eth:uniswap_v2:pool-a:UNI": {"payload": b"a"},
            "dex:eth:uniswap_v2:pool-b:UNI": {"payload": b"b"},
        }
        cost_evidence = {
            "selected_markets": [
                {
                    "market_id": "dex:eth:uniswap_v2:pool-a:UNI",
                    "structural_support_status": "supported",
                },
                {
                    "market_id": "dex:eth:uniswap_v2:pool-b:UNI",
                    "structural_support_status": "unsupported",
                },
            ],
        }

        self.assertEqual(
            opportunity_pipeline._cost_supported_pool_members(
                cost_evidence, retained
            ),
            {"dex:eth:uniswap_v2:pool-a:UNI": retained[
                "dex:eth:uniswap_v2:pool-a:UNI"
            ]},
        )
        with self.assertRaisesRegex(
            RouteOpportunityPipelineError,
            "pool-state evidence is missing",
        ):
            opportunity_pipeline._cost_supported_pool_members(
                cost_evidence,
                {"dex:eth:uniswap_v2:pool-b:UNI": retained[
                    "dex:eth:uniswap_v2:pool-b:UNI"
                ]},
            )

    def test_published_dex_bundle_serves_page_and_api_over_loopback(self):
        from dashboard import server

        data_dir, cohort, joint = self._install_real_dex_run()
        opportunity_pipeline.finalize_eth_uniswap_v2_research_opportunities(
            data_dir=data_dir,
            shadow_run_id=joint["pointer"]["run_id"],
            expected_joint_pointer_sha256=joint["pointer_sha256"],
        )

        server.clear_runtime_caches()
        http_server = None
        worker = None
        try:
            with patch.dict(
                server.os.environ,
                {"MARKET_ROUTE_DATA_DIR": str(data_dir / "routes")},
                clear=True,
            ), patch.object(
                server.MarketMonitorHandler,
                "log_message",
                return_value=None,
            ):
                http_server = server.ThreadingHTTPServer(
                    ("127.0.0.1", 0),
                    server.MarketMonitorHandler,
                )
                http_server.daemon_threads = True
                worker = threading.Thread(
                    target=http_server.serve_forever,
                    daemon=True,
                )
                worker.start()
                connection = http.client.HTTPConnection(
                    "127.0.0.1",
                    http_server.server_address[1],
                    timeout=5,
                )
                try:
                    connection.request("GET", "/opportunities")
                    page = connection.getresponse()
                    page_body = page.read().decode("utf-8")
                    self.assertEqual(page.status, 200)
                    self.assertIn("text/html", page.getheader("Content-Type"))
                    self.assertIn('id="opportunities-view"', page_body)

                    connection.request(
                        "GET",
                        "/api/markets/opportunities?notional=1000&"
                        "route_type=dex_dex&availability=unavailable",
                    )
                    response = connection.getresponse()
                    payload = json.loads(response.read().decode("utf-8"))
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.getheader("Cache-Control"), "no-store")
                finally:
                    connection.close()
        finally:
            if http_server is not None:
                if worker is not None and worker.is_alive():
                    http_server.shutdown()
                http_server.server_close()
            if worker is not None:
                worker.join(timeout=5)
            server.clear_runtime_caches()

        self.assertEqual(
            payload["availability"], {"status": "available", "reason": None}
        )
        self.assertEqual(len(payload["routes"]), len(cohort["routes"]))
        self.assertTrue(all(
            row["route_type"] == "dex_dex"
            and row["opportunity_class"] == "unavailable"
            and row["primary_reason"] == "cost_components_incomplete"
            and len(row["cost_components"]) == 10
            for row in payload["routes"]
        ))

    def test_dex_price_context_must_equal_core_collector_context(self):
        data_dir, _cohort, joint = self._install_real_dex_run(
            typed_context_price="96"
        )
        latest_path = data_dir / "routes/latest.json"
        latest_path.write_bytes(self.old_pointer_bytes)

        with self.assertRaisesRegex(
            RouteOpportunityPipelineError,
            "price context differs",
        ):
            opportunity_pipeline.finalize_eth_uniswap_v2_research_opportunities(
                data_dir=data_dir,
                shadow_run_id=joint["pointer"]["run_id"],
                expected_joint_pointer_sha256=joint["pointer_sha256"],
            )

        self.assertEqual(latest_path.read_bytes(), self.old_pointer_bytes)

    def test_dex_leg_snapshot_must_equal_raw_evidence_run(self):
        data_dir, cohort, joint = self._install_real_dex_run()
        changed = copy.deepcopy(cohort)
        changed["legs"][0]["snapshot_id"] = "different-run"

        with self.assertRaisesRegex(
            RouteOpportunityPipelineError,
            "pool state differs",
        ):
            opportunity_pipeline._load_dex_sources(
                root=data_dir,
                cohort=changed,
                source_root=(
                    data_dir / "raw/route-cohort"
                    / joint["pointer"]["run_id"] / "typed"
                ),
                now="2026-08-01T12:00:03Z",
            )

    def test_dex_scope_rejects_non_eth_non_v2_and_non_atomic_routes(self):
        cases = (
            ("wrong-chain", "arb", "uniswap_v2", "atomic_onchain"),
            ("wrong-dex", "eth", "uniswap_v3", "atomic_onchain"),
            ("wrong-mode", "eth", "uniswap_v2", "research_only"),
        )
        for label, chain, dex, route_mode in cases:
            with self.subTest(label=label):
                shadow, latest_core = self._pinned_views(dex=True)
                first = "dex:{}:{}:0x{}:AAVE".format(
                    chain, dex, "1" * 40
                )
                second = "dex:{}:{}:0x{}:AAVE".format(
                    chain, dex, "2" * 40
                )
                cohort = shadow["cohort"]
                cohort["legs"] = [
                    {
                        "market_id": market_id,
                        "market_type": "dex",
                        "status": "observed",
                        "available": True,
                    }
                    for market_id in (first, second)
                ]
                cohort["routes"] = [{
                    "route_id": "route-1",
                    "buy_market_id": first,
                    "sell_market_id": second,
                    "route_mode": route_mode,
                    "route_class": "candidate",
                    "settlement_reason": None,
                }]
                latest_core["cohort"] = copy.deepcopy(cohort)
                with ExitStack() as stack:
                    stack.enter_context(patch(
                        "scripts.route_opportunity_pipeline.load_shadow_result",
                        return_value=shadow,
                    ))
                    stack.enter_context(patch(
                        "scripts.route_opportunity_pipeline."
                        "load_latest_route_cohort",
                        return_value=latest_core,
                    ))
                    publisher = stack.enter_context(patch(
                        "scripts.route_opportunity_pipeline."
                        "publish_complete_route_bundle"
                    ))
                    with self.assertRaisesRegex(
                        RouteOpportunityPipelineError,
                        "requires observed|requires same-chain",
                    ):
                        opportunity_pipeline.finalize_eth_uniswap_v2_research_opportunities(
                            data_dir=self.data_dir,
                            shadow_run_id=self.run_id,
                            expected_joint_pointer_sha256=(
                                self.expected_joint_sha256
                            ),
                        )

                publisher.assert_not_called()
                self.assertEqual(
                    self.public_pointer.read_bytes(), self.old_pointer_bytes
                )

    def test_dex_sidecar_drift_before_commit_preserves_pointer(self):
        data_dir, _cohort, joint = self._install_real_dex_run()
        latest_path = data_dir / "routes/latest.json"
        latest_path.write_bytes(self.old_pointer_bytes)
        actual_publisher = opportunity_pipeline.publish_complete_route_bundle

        def mutate_then_publish(**kwargs):
            sidecar = (
                data_dir / "routes/shadow/runs"
                / joint["pointer"]["run_id"]
                / "route-cost-evidence.json"
            )
            sidecar.write_bytes(sidecar.read_bytes() + b"\n")
            return actual_publisher(**kwargs)

        with patch(
            "scripts.route_opportunity_pipeline."
            "publish_complete_route_bundle",
            side_effect=mutate_then_publish,
        ):
            with self.assertRaises(RouteOpportunityPipelineError):
                opportunity_pipeline.finalize_eth_uniswap_v2_research_opportunities(
                    data_dir=data_dir,
                    shadow_run_id=joint["pointer"]["run_id"],
                    expected_joint_pointer_sha256=joint["pointer_sha256"],
                )

        self.assertEqual(latest_path.read_bytes(), self.old_pointer_bytes)

    def test_dex_transient_typed_mutation_cannot_publish(self):
        data_dir, cohort, joint = self._install_real_dex_run()
        latest_path = data_dir / "routes/latest.json"
        latest_path.write_bytes(self.old_pointer_bytes)
        actual_publisher = opportunity_pipeline.publish_complete_route_bundle
        context_member = next(
            member
            for member in cohort["legs"][0]["typed_source_lineage"]["members"]
            if member["role"] == "dex_usd_price_context"
        )
        context_path = (
            data_dir / "raw/route-cohort" / joint["pointer"]["run_id"]
            / "typed" / context_member["filename"]
        )
        original_bytes = context_path.read_bytes()
        mutated = json.loads(original_bytes)
        mutated["base_token_price_usd"] = "96"
        mutated_bytes = _shadow_json_bytes(mutated)
        self.assertNotEqual(mutated_bytes, original_bytes)

        def mutate_then_restore_at_precommit(**kwargs):
            original_validator = kwargs["precommit_validator"]
            context_path.write_bytes(mutated_bytes)

            def restore_then_validate():
                context_path.write_bytes(original_bytes)
                original_validator()

            kwargs["precommit_validator"] = restore_then_validate
            try:
                return actual_publisher(**kwargs)
            finally:
                context_path.write_bytes(original_bytes)

        with patch(
            "scripts.route_opportunity_pipeline."
            "publish_complete_route_bundle",
            side_effect=mutate_then_restore_at_precommit,
        ):
            with self.assertRaises(RouteOpportunityPipelineError):
                opportunity_pipeline.finalize_eth_uniswap_v2_research_opportunities(
                    data_dir=data_dir,
                    shadow_run_id=joint["pointer"]["run_id"],
                    expected_joint_pointer_sha256=joint["pointer_sha256"],
                )

        self.assertEqual(context_path.read_bytes(), original_bytes)
        self.assertEqual(latest_path.read_bytes(), self.old_pointer_bytes)

    def test_dex_typed_member_inventory_must_be_exact(self):
        data_dir, _cohort, joint = self._install_real_dex_run()
        latest_path = data_dir / "routes/latest.json"
        actual_publisher = opportunity_pipeline.publish_complete_route_bundle

        def without_members(**kwargs):
            changed = copy.deepcopy(kwargs["opportunity_inputs"])
            for item in changed:
                item["source_members"] = {}
            kwargs["opportunity_inputs"] = changed
            return actual_publisher(**kwargs)

        latest_path.write_bytes(self.old_pointer_bytes)
        with patch(
            "scripts.route_opportunity_pipeline."
            "publish_complete_route_bundle",
            side_effect=without_members,
        ):
            with self.assertRaises(RouteOpportunityPipelineError):
                opportunity_pipeline.finalize_eth_uniswap_v2_research_opportunities(
                    data_dir=data_dir,
                    shadow_run_id=joint["pointer"]["run_id"],
                    expected_joint_pointer_sha256=joint["pointer_sha256"],
                )

        self.assertEqual(latest_path.read_bytes(), self.old_pointer_bytes)

    def test_incomplete_high_notional_is_published_as_unavailable(self):
        data_dir, fixture, joint = self._install_real_cex_run(
            sell_quantity="100"
        )
        with patch.dict(os.environ, {
            "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                fixture["fee_profile_path"]
            ),
            "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                fixture["inventory_profile_path"]
            ),
        }, clear=False):
            finalize_cex_route_opportunities(
                data_dir=data_dir,
                shadow_run_id=joint["pointer"]["run_id"],
                expected_joint_pointer_sha256=joint["pointer_sha256"],
            )

        opportunities = load_latest_complete_route_bundle(
            data_dir / "routes", core_root=data_dir / "routes/core"
        )["bundle"]["opportunities"]
        by_notional = {
            row["requested_notional_usd"]: row for row in opportunities
        }
        self.assertEqual(len(by_notional), 5)
        self.assertEqual(
            by_notional["100000"]["opportunity_class"], "unavailable"
        )
        self.assertIn(
            "leg_not_completely_filled",
            by_notional["100000"]["reason_codes"],
        )

    def test_sidecar_drift_during_input_build_stops_before_finalizer(self):
        data_dir, fixture, joint = self._install_real_cex_run()
        latest_path = data_dir / "routes/latest.json"
        latest_path.write_bytes(self.old_pointer_bytes)
        original_builder = opportunity_pipeline._build_inputs
        run_id = joint["pointer"]["run_id"]

        def build_then_mutate(**kwargs):
            result = original_builder(**kwargs)
            sidecar = (
                data_dir / "routes/shadow/runs" / run_id
                / "route-cost-evidence.json"
            )
            sidecar.write_bytes(sidecar.read_bytes() + b"\n")
            return result

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                    fixture["fee_profile_path"]
                ),
                "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                    fixture["inventory_profile_path"]
                ),
            }, clear=False))
            stack.enter_context(patch(
                "scripts.route_opportunity_pipeline._build_inputs",
                side_effect=build_then_mutate,
            ))
            publisher = stack.enter_context(patch(
                "scripts.route_opportunity_pipeline."
                "publish_complete_route_bundle"
            ))
            with self.assertRaises(RouteOpportunityPipelineError):
                finalize_cex_route_opportunities(
                    data_dir=data_dir,
                    shadow_run_id=run_id,
                    expected_joint_pointer_sha256=joint["pointer_sha256"],
                )

        self.assertEqual(publisher.call_count, 0)
        self.assertEqual(latest_path.read_bytes(), self.old_pointer_bytes)

    def test_fee_profile_replacement_after_input_build_preserves_pointer(self):
        def replace_fee(_data_dir, fixture, _joint):
            path = fixture["fee_profile_path"]
            original = path.read_text(encoding="utf-8")
            changed = original.replace(",10,", ",11,")
            self.assertNotEqual(changed, original)
            path.write_text(changed, encoding="utf-8")

        self._assert_post_confirmation_mutation_fails(replace_fee)

    def test_typed_usd_replacement_after_input_build_preserves_pointer(self):
        def replace_usd(data_dir, fixture, joint):
            buy_market_id = fixture["cohort"]["routes"][0]["buy_market_id"]
            venue = buy_market_id.split(":", 2)[1]
            filename = venue + "-quote_usd_conversion.json"
            path = (
                data_dir / "raw/route-cohort"
                / joint["pointer"]["run_id"] / "typed" / filename
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["usd_per_quote"] = "1.01"
            path.write_bytes(_shadow_json_bytes(payload))

        self._assert_post_confirmation_mutation_fails(replace_usd)

    def test_post_confirmation_sidecar_drift_preserves_pointer(self):
        def replace_sidecar(data_dir, _fixture, joint):
            path = (
                data_dir / "routes/shadow/runs"
                / joint["pointer"]["run_id"] / "route-cost-evidence.json"
            )
            path.write_bytes(path.read_bytes() + b"\n")

        self._assert_post_confirmation_mutation_fails(replace_sidecar)


class PublicCexResearchFinalizerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.old_pointer_bytes = b'{"old":"public-research-pointer"}\n'

    def _install_aave_core(self):
        data_dir, fixture, _joint = (
            RouteOpportunityPipelineTests._install_real_cex_run(
                self,
                both_directions=True,
            )
        )
        latest = data_dir / "routes/latest.json"
        latest.write_bytes(self.old_pointer_bytes)
        return data_dir, fixture, latest

    def _install_public_core(self):
        data_dir, fixture, latest = self._install_aave_core()
        core_root = data_dir / "routes/core"
        raw_root = data_dir / "raw/route-cohort"
        loaded = opportunity_pipeline.load_latest_route_cohort(core_root)
        cohort = copy.deepcopy(loaded["cohort"])
        universe = build_live_cex_research_universe()
        run_id = cohort["raw_evidence_run_id"]
        typed_root = raw_root / run_id / "typed"

        old_legs = {
            leg["market_id"].split(":", 2)[1]: leg
            for leg in cohort["legs"]
        }
        fixed_legs = []
        manifest_members = []
        for selected in universe["selected_legs"]:
            venue = selected["exchange"]
            market_id = selected["market_id"]
            old_leg = old_legs[venue]
            old_market_id = old_leg["market_id"]
            old_response = (
                raw_root / run_id / "accepted"
                / hashlib.sha256(old_market_id.encode("utf-8")).hexdigest()
                / "response.json"
            ).read_bytes()
            response = old_response.replace(b"AAVEUSDT", b"UNIUSDT")
            accepted = (
                raw_root / run_id / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            )
            accepted.mkdir(parents=True)
            (accepted / "response.json").write_bytes(response)

            leg = copy.deepcopy(old_leg)
            leg.update({
                "leg_id": market_id,
                "market_id": market_id,
                "token_symbol": "UNI",
                "raw_response_sha256": hashlib.sha256(response).hexdigest(),
            })
            members = []
            for old_member in old_leg["typed_source_lineage"]["members"]:
                member = copy.deepcopy(old_member)
                filename = member["filename"]
                payload = json.loads(
                    (typed_root / filename).read_text(encoding="utf-8")
                )
                if member["role"] == "cex_market_rules":
                    payload["market_id"] = market_id
                    payload["base_asset"] = "UNI"
                elif member["role"] == "cex_raw_book_response":
                    result = payload.get("result")
                    if isinstance(result, dict) and result.get("s") == "AAVEUSDT":
                        result["s"] = "UNIUSDT"
                payload_bytes = _shadow_json_bytes(payload)
                (typed_root / filename).write_bytes(payload_bytes)
                digest = hashlib.sha256(payload_bytes).hexdigest()
                member.update({
                    "sha256": digest,
                    "size": len(payload_bytes),
                    "logical_generation": digest,
                })
                members.append(member)
                manifest_members.append({
                    "market_id": market_id,
                    **{
                        key: member[key]
                        for key in (
                            "role", "filename", "sha256", "size",
                            "logical_generation", "adapter_id",
                            "content_schema",
                        )
                    },
                })
            leg["typed_source_lineage"] = {
                "schema": old_leg["typed_source_lineage"]["schema"],
                "members": members,
            }
            fixed_legs.append(leg)

        manifest_members.sort(key=lambda row: (row["market_id"], row["role"]))
        (raw_root / run_id / "typed-manifest.json").write_bytes(
            _shadow_json_bytes({
                "schema": "route_typed_source_manifest/v1",
                "raw_evidence_run_id": run_id,
                "member_count": len(manifest_members),
                "members": manifest_members,
            })
        )

        old_rows = {
            row["buy_market_id"].split(":", 2)[1]: row
            for row in cohort["route_rows"]
        }
        cohort["candidate_source_generation"] = universe[
            "candidate_source_generation"
        ]
        cohort["source_state"]["candidate_source_generation"] = universe[
            "candidate_source_generation"
        ]
        cohort["selection_window"] = copy.deepcopy(universe["selection_window"])
        cohort["requested_notionals_usd"] = copy.deepcopy(
            universe["requested_notionals_usd"]
        )
        cohort["routes"] = copy.deepcopy(universe["routes"])
        cohort["route_rows"] = []
        for route in cohort["routes"]:
            timing = old_rows[route["buy_market_id"].split(":", 2)[1]]
            cohort["route_rows"].append({
                **copy.deepcopy(route),
                "validated_at": timing["validated_at"],
                "skew_seconds": timing["skew_seconds"],
                "timing_status": timing["timing_status"],
                "reason_code": timing["reason_code"],
            })
        cohort["legs"] = fixed_legs
        cohort = _rehash(cohort)
        publish_route_cohort_bundle(cohort, core_root=core_root)
        latest.write_bytes(self.old_pointer_bytes)
        return data_dir, fixture, latest

    def _write_schedule(self, rows=None):
        schedule = Path(self.temporary.name) / "public-fees.csv"
        if rows is None:
            rows = [
                self._schedule_row("binance", "10"),
                self._schedule_row("bybit", "8"),
            ]
        with schedule.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=PUBLIC_FEE_SCHEDULE_COLUMNS,
            )
            writer.writeheader()
            writer.writerows(rows)
        return schedule

    @staticmethod
    def _schedule_row(venue, maximum, **overrides):
        row = {
            "venue": venue,
            "instrument_pattern": "UNI/USDT",
            "side": "both",
            "min_taker_fee_bps": "4",
            "max_taker_fee_bps": maximum,
            "fee_asset": "received_asset",
            "basis": "official_spot_taker_fee_range",
            "checked_at": "2026-08-01T11:55:00Z",
            "valid_until": "2026-08-01T13:00:00Z",
            "source_url": "https://{}.example.test/spot-fees".format(venue),
        }
        row.update(overrides)
        return row

    def _finalize_public(self, data_dir, schedule):
        current = opportunity_pipeline.load_latest_route_cohort(
            data_dir / "routes/core"
        )
        return opportunity_pipeline.finalize_public_cex_research_opportunities(
            data_dir=data_dir,
            public_fee_schedule_path=schedule,
            expected_route_cohort_id=current["cohort"]["route_cohort_id"],
            expected_core_manifest_sha256=current["manifest_sha256"],
        )

    def _assert_failure_preserves_pointer(self, *, schedule, mutate=None):
        data_dir, _fixture, latest = self._install_public_core()
        if mutate is not None:
            mutate(data_dir)
        with self.assertRaises(RouteOpportunityPipelineError):
            self._finalize_public(data_dir, schedule)
        self.assertEqual(latest.read_bytes(), self.old_pointer_bytes)

    def test_stale_expected_core_identity_preserves_prior_pointer(self):
        data_dir, _fixture, latest = self._install_public_core()
        schedule = self._write_schedule()
        current = opportunity_pipeline.load_latest_route_cohort(
            data_dir / "routes/core"
        )

        with self.assertRaises(RouteOpportunityPipelineError):
            opportunity_pipeline.finalize_public_cex_research_opportunities(
                data_dir=data_dir,
                public_fee_schedule_path=schedule,
                expected_route_cohort_id="cohort:" + "0" * 64,
                expected_core_manifest_sha256=current["manifest_sha256"],
            )

        self.assertEqual(latest.read_bytes(), self.old_pointer_bytes)

    def test_stale_expected_core_manifest_preserves_prior_pointer(self):
        data_dir, _fixture, latest = self._install_public_core()
        schedule = self._write_schedule()
        current = opportunity_pipeline.load_latest_route_cohort(
            data_dir / "routes/core"
        )

        with self.assertRaises(RouteOpportunityPipelineError):
            opportunity_pipeline.finalize_public_cex_research_opportunities(
                data_dir=data_dir,
                public_fee_schedule_path=schedule,
                expected_route_cohort_id=current["cohort"]["route_cohort_id"],
                expected_core_manifest_sha256="0" * 64,
            )

        self.assertEqual(latest.read_bytes(), self.old_pointer_bytes)

    def test_matching_identity_aave_core_preserves_prior_pointer(self):
        data_dir, _fixture, latest = self._install_aave_core()
        schedule = self._write_schedule()
        current = opportunity_pipeline.load_latest_route_cohort(
            data_dir / "routes/core"
        )

        with self.assertRaises(RouteOpportunityPipelineError):
            opportunity_pipeline.finalize_public_cex_research_opportunities(
                data_dir=data_dir,
                public_fee_schedule_path=schedule,
                expected_route_cohort_id=current["cohort"]["route_cohort_id"],
                expected_core_manifest_sha256=current["manifest_sha256"],
            )

        self.assertEqual(latest.read_bytes(), self.old_pointer_bytes)

    def test_two_direction_public_grid_is_cold_loadable_and_research_only(self):
        data_dir, _fixture, latest = self._install_public_core()
        schedule = self._write_schedule()
        captured = {}
        actual_publish = opportunity_pipeline.publish_complete_route_bundle

        def capture_and_publish(**kwargs):
            captured["inputs"] = kwargs["opportunity_inputs"]
            return actual_publish(**kwargs)

        with patch(
            "scripts.route_opportunity_pipeline.publish_complete_route_bundle",
            side_effect=capture_and_publish,
        ):
            pointer = self._finalize_public(data_dir, schedule)

        self.assertNotEqual(latest.read_bytes(), self.old_pointer_bytes)
        loaded = load_latest_complete_route_bundle(
            data_dir / "routes",
            core_root=data_dir / "routes/core",
        )
        self.assertEqual(pointer, loaded["pointer"])
        opportunities = loaded["bundle"]["opportunities"]
        self.assertEqual(len(opportunities), 10)
        self.assertEqual(
            {row["requested_notional_usd"] for row in opportunities},
            {"1000", "5000", "10000", "50000", "100000"},
        )
        for row in opportunities:
            self.assertEqual(row["opportunity_class"], "research_estimate")
            self.assertIs(row["strict_eligible"], False)
            self.assertIs(row["strict_ready_for_publication"], False)
            self.assertIsNone(row["publication_attestation_sha256"])
            self.assertIs(row["mode_evidence_eligible"], False)
            self.assertIsNone(row["inventory_profile_hash"])

        self.assertEqual(len(captured["inputs"]), 10)
        for item in captured["inputs"]:
            build = item["build_inputs"]
            target = build["common_target"].quantity
            self.assertEqual(build["buy_quote"].target_base_quantity, target)
            self.assertEqual(build["sell_quote"].target_base_quantity, target)
            self.assertIs(build["mode_evidence"]["mode_evidence_eligible"], False)
            fees = {
                row["leg"]: row
                for row in build["cost_components"]
                if row["component_type"] == "venue_taker_fee"
            }
            self.assertEqual(
                fees["buy"]["rate_bps"],
                "10" if fees["buy"]["market_id"].startswith(
                    "cex:binance:"
                ) else "8",
            )
            self.assertEqual(
                fees["sell"]["rate_bps"],
                "10" if fees["sell"]["market_id"].startswith(
                    "cex:binance:"
                ) else "8",
            )
            for direction in ("buy", "sell"):
                self.assertEqual(fees[direction]["value_status"], "bounded_estimate")
                semantics = build[
                    direction + "_quote_evidence"
                ]["fee_semantics"]
                self.assertEqual(
                    str(semantics.rate_bps), fees[direction]["rate_bps"]
                )
                self.assertEqual(
                    semantics.source_record_sha256,
                    fees[direction]["source_record_sha256"],
                )
            rebalancing = next(
                row for row in build["cost_components"]
                if row["component_type"] == "rebalancing_or_transfer"
            )
            self.assertEqual(rebalancing["value_status"], "assumed")
            self.assertEqual(rebalancing["amount_usd"], "0")
            self.assertEqual(rebalancing["rate_bps"], "0")
            self.assertIs(rebalancing["strict_eligible"], False)
            self.assertEqual(
                rebalancing["reason_code"],
                "inventory_not_observed_for_public_research",
            )

    def test_missing_public_fee_row_stays_unavailable_and_never_becomes_zero(self):
        data_dir, _fixture, _latest = self._install_public_core()
        schedule = self._write_schedule([
            self._schedule_row("binance", "10"),
        ])
        self._finalize_public(data_dir, schedule)
        bundle = load_latest_complete_route_bundle(
            data_dir / "routes",
            core_root=data_dir / "routes/core",
        )["bundle"]
        missing = [
            row for row in bundle["cost_components"]
            if row["market_id"].startswith("cex:bybit:")
            and row["component_type"] == "venue_taker_fee"
        ]
        self.assertEqual(len(missing), 10)
        self.assertTrue(all(
            row["value_status"] == "unavailable"
            and row["rate_bps"] is None
            and row["amount_usd"] is None
            for row in missing
        ))
        self.assertTrue(all(
            row["opportunity_class"] == "unavailable"
            and row["strict_eligible"] is False
            and row["publication_attestation_sha256"] is None
            for row in bundle["opportunities"]
        ))

    def test_stale_public_schedule_preserves_prior_pointer(self):
        schedule = self._write_schedule([
            self._schedule_row(
                "binance", "10", valid_until="2026-08-01T12:01:00Z"
            ),
            self._schedule_row("bybit", "8"),
        ])
        self._assert_failure_preserves_pointer(schedule=schedule)

    def test_ambiguous_public_schedule_preserves_prior_pointer(self):
        schedule = self._write_schedule([
            self._schedule_row("binance", "10"),
            self._schedule_row(
                "binance", "12", instrument_pattern="UNI/*", side="buy"
            ),
            self._schedule_row("bybit", "8"),
        ])
        self._assert_failure_preserves_pointer(schedule=schedule)

    def test_typed_source_mutation_preserves_prior_pointer(self):
        schedule = self._write_schedule()

        def mutate(data_dir):
            member = next(
                (data_dir / "raw/route-cohort/task7-source-run/typed").glob(
                    "*-cex_market_rules.json"
                )
            )
            member.write_bytes(member.read_bytes() + b"\n")

        self._assert_failure_preserves_pointer(
            schedule=schedule,
            mutate=mutate,
        )

    def test_raw_book_mutation_preserves_prior_pointer(self):
        schedule = self._write_schedule()

        def mutate(data_dir):
            member = next(
                (data_dir / "raw/route-cohort/task7-source-run/accepted").glob(
                    "*/response.json"
                )
            )
            member.write_bytes(member.read_bytes() + b"\n")

        self._assert_failure_preserves_pointer(
            schedule=schedule,
            mutate=mutate,
        )

    def test_schedule_replacement_during_precommit_preserves_prior_pointer(self):
        data_dir, _fixture, latest = self._install_public_core()
        schedule = self._write_schedule()
        actual_publish = opportunity_pipeline.publish_complete_route_bundle

        def replace_then_publish(**kwargs):
            original_validator = kwargs["precommit_validator"]

            def replace_then_validate():
                schedule.write_bytes(schedule.read_bytes() + b"\n")
                original_validator()

            kwargs["precommit_validator"] = replace_then_validate
            return actual_publish(**kwargs)

        with patch(
            "scripts.route_opportunity_pipeline.publish_complete_route_bundle",
            side_effect=replace_then_publish,
        ):
            with self.assertRaises(RouteOpportunityPipelineError):
                self._finalize_public(data_dir, schedule)
        self.assertEqual(latest.read_bytes(), self.old_pointer_bytes)

    def test_transient_fee_schedule_swap_cannot_change_calculated_rates(self):
        data_dir, _fixture, _latest = self._install_public_core()
        schedule = self._write_schedule()
        original_bytes = schedule.read_bytes()
        transient_bytes = original_bytes.replace(b",10,", b",12,", 1)
        self.assertNotEqual(transient_bytes, original_bytes)
        actual_collect = (
            opportunity_pipeline.
            _collect_cex_fee_snapshot_from_schedule_snapshot
        )

        def collect_during_transient_swap(**kwargs):
            schedule.write_bytes(transient_bytes)
            try:
                return actual_collect(**kwargs)
            finally:
                schedule.write_bytes(original_bytes)

        with patch(
            "scripts.route_opportunity_pipeline."
            "_collect_cex_fee_snapshot_from_schedule_snapshot",
            side_effect=collect_during_transient_swap,
        ):
            self._finalize_public(data_dir, schedule)

        bundle = load_latest_complete_route_bundle(
            data_dir / "routes",
            core_root=data_dir / "routes/core",
        )["bundle"]
        binance_fees = [
            row for row in bundle["cost_components"]
            if row["market_id"].startswith("cex:binance:")
            and row["component_type"] == "venue_taker_fee"
        ]
        self.assertEqual(len(binance_fees), 10)
        self.assertEqual({row["rate_bps"] for row in binance_fees}, {"10"})

    def test_same_bytes_fee_schedule_inode_swap_preserves_prior_pointer(self):
        data_dir, _fixture, latest = self._install_public_core()
        schedule = self._write_schedule()
        actual_publish = opportunity_pipeline.publish_complete_route_bundle

        def replace_then_publish(**kwargs):
            original_validator = kwargs["precommit_validator"]

            def replace_inode_then_validate():
                replacement = schedule.with_name("replacement-fees.csv")
                replacement.write_bytes(schedule.read_bytes())
                os.replace(replacement, schedule)
                original_validator()

            kwargs["precommit_validator"] = replace_inode_then_validate
            return actual_publish(**kwargs)

        with patch(
            "scripts.route_opportunity_pipeline.publish_complete_route_bundle",
            side_effect=replace_then_publish,
        ):
            with self.assertRaises(RouteOpportunityPipelineError):
                self._finalize_public(data_dir, schedule)
        self.assertEqual(latest.read_bytes(), self.old_pointer_bytes)

    def test_cold_reload_failure_restores_prior_pointer(self):
        data_dir, _fixture, latest = self._install_public_core()
        schedule = self._write_schedule()

        with patch(
            "scripts.route_opportunity_pipeline."
            "load_latest_complete_route_bundle",
            side_effect=ValueError("forced cold reload failure"),
        ):
            with self.assertRaises(RouteOpportunityPipelineError):
                self._finalize_public(data_dir, schedule)

        self.assertEqual(latest.read_bytes(), self.old_pointer_bytes)

    def test_runner_postcommit_validator_failure_restores_prior_pointer(self):
        data_dir, _fixture, latest = self._install_public_core()
        schedule = self._write_schedule()
        current = opportunity_pipeline.load_latest_route_cohort(
            data_dir / "routes/core"
        )

        class RunnerReloadFailure(RuntimeError):
            pass

        def reject_loaded_bundle(_pointer):
            raise RunnerReloadFailure("runner rejected cold-loaded bundle")

        with self.assertRaises(RunnerReloadFailure):
            opportunity_pipeline.finalize_public_cex_research_opportunities(
                data_dir=data_dir,
                public_fee_schedule_path=schedule,
                expected_route_cohort_id=current["cohort"]["route_cohort_id"],
                expected_core_manifest_sha256=current["manifest_sha256"],
                _postcommit_validator=reject_loaded_bundle,
            )

        self.assertEqual(latest.read_bytes(), self.old_pointer_bytes)

    def test_cold_reload_failure_never_overwrites_concurrent_pointer(self):
        data_dir, _fixture, latest = self._install_public_core()
        schedule = self._write_schedule()
        concurrent_pointer = b'{"concurrent":"writer"}\n'

        def replace_pointer_then_fail(*_args, **_kwargs):
            latest.write_bytes(concurrent_pointer)
            raise ValueError("forced cold reload failure after concurrent write")

        with patch(
            "scripts.route_opportunity_pipeline."
            "load_latest_complete_route_bundle",
            side_effect=replace_pointer_then_fail,
        ):
            with self.assertRaises(RouteOpportunityPipelineError):
                self._finalize_public(data_dir, schedule)

        self.assertEqual(latest.read_bytes(), concurrent_pointer)


if __name__ == "__main__":
    unittest.main()
