"""Tests for the explicit CEX-only route-opportunity finalizer."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.route_cost_evidence import (
    build_unavailable_route_cost_evidence_manifest,
)
from scripts.route_publication import (
    load_latest_complete_route_bundle,
    publish_route_cohort_bundle,
    publish_shadow_result,
)
from scripts.route_shadow_audit import build_shadow_audit
from scripts.route_shadow_inputs import (
    SourceFileIdentity,
    TYPED_SOURCE_ROLE_CONTRACTS,
    _candidate_source_generation,
    write_run_universe,
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

    def _install_real_cex_run(self, *, sell_quantity="10000"):
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

        with (
            patch.dict(os.environ, {
                "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                    fixture["fee_profile_path"]
                ),
                "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                    fixture["inventory_profile_path"]
                ),
            }, clear=False),
            patch(
                "scripts.route_opportunity_pipeline."
                "publish_complete_route_bundle",
                side_effect=mutate_then_publish,
            ),
        ):
            with self.assertRaises(RouteOpportunityPipelineError):
                finalize_cex_route_opportunities(
                    data_dir=data_dir,
                    shadow_run_id=joint["pointer"]["run_id"],
                    expected_joint_pointer_sha256=joint["pointer_sha256"],
                )

        self.assertEqual(latest_path.read_bytes(), self.old_pointer_bytes)

    def test_dex_core_is_rejected_before_finalizer_and_preserves_pointer(self):
        shadow, latest_core = self._pinned_views(dex=True)
        with (
            patch(
                "scripts.route_opportunity_pipeline.load_shadow_result",
                return_value=shadow,
            ),
            patch(
                "scripts.route_opportunity_pipeline.load_latest_route_cohort",
                return_value=latest_core,
            ),
            patch(
                "scripts.route_opportunity_pipeline.publish_complete_route_bundle"
            ) as publisher,
        ):
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

                with (
                    patch(
                        "scripts.route_opportunity_pipeline.load_shadow_result",
                        return_value=shadow,
                    ),
                    patch(
                        "scripts.route_opportunity_pipeline.load_latest_route_cohort",
                        return_value=latest_core,
                    ),
                    patch(
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
                    ),
                    patch(
                        "scripts.route_opportunity_pipeline."
                        "publish_complete_route_bundle"
                    ) as publisher,
                ):
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

        with (
            patch.dict(os.environ, {
                "MARKET_CEX_PRIVATE_FEE_PROFILE": str(
                    fixture["fee_profile_path"]
                ),
                "MARKET_ROUTE_PRIVATE_INVENTORY_PROFILE": str(
                    fixture["inventory_profile_path"]
                ),
            }, clear=False),
            patch(
                "scripts.route_opportunity_pipeline._build_inputs",
                side_effect=build_then_mutate,
            ),
            patch(
                "scripts.route_opportunity_pipeline."
                "publish_complete_route_bundle"
            ) as publisher,
        ):
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


if __name__ == "__main__":
    unittest.main()
