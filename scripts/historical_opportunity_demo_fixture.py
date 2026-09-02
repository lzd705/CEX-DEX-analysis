"""Honest, disposable data source for the local Opportunity workflow demo."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import threading
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
from unittest import mock


DEMO_CONTRACT = "opportunity_historical_demo_summary/v1"
DEMO_EVIDENCE_MODE = "offline_test_fixture"
DEMO_VERIFICATION_STATUS = "structurally_validated"
_WORKER_TIMEOUT_SECONDS = 60


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _offline_observation_worker(request: Mapping[str, Any], connection: Any) -> None:
    """Build fixture-only evidence in a real, separate local process."""

    try:
        import scripts.historical_foundry_verifier as verifier

        connection.send(
            verifier._build_connected_observation_for_retained_fixture(
                request
            )
        )
    except BaseException as error:
        connection.send(("error", type(error).__name__, str(error)))
    finally:
        connection.close()


def _fresh_offline_observation(request: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return structural fixture evidence without invoking RPC or Foundry."""

    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_offline_observation_worker,
        args=(request, child),
    )
    process.start()
    child.close()
    observation = None
    try:
        if not parent.poll(_WORKER_TIMEOUT_SECONDS):
            process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
            raise RuntimeError("offline fixture validation timed out")
        observation = parent.recv()
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
    if isinstance(observation, tuple) and observation[:1] == ("error",):
        raise RuntimeError("offline fixture validation failed")
    if not isinstance(observation, Mapping):
        raise RuntimeError("offline fixture validation returned invalid evidence")
    return observation


class HistoricalOpportunityDemoFixture:
    """Build and retain one structurally validated repository-only fixture."""

    def __init__(self) -> None:
        import scripts.historical_foundry_verifier as verifier
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        self._helper = HistoricalCorePublicationTests
        self._lock = threading.Lock()
        self._loaded = None
        self._loaded_subject = None
        self.run = self.finalized = self.context = None
        self.data_dir = self.raw_root = self.historical_root = None
        self.pointer = None
        core_stage = lease = None
        staged_subject = pointer_publication = None
        try:
            self.run, self.finalized, lease, _identity = (
                self._helper._open_real_task7_lease(
                    include_newer_mixed_rows=True
                )
            )
            self.data_dir = self.run["fixture"].data_dir
            self.raw_root = (
                self.data_dir / "raw" / "historical-foundry-replay"
            )
            self.historical_root = self.data_dir / "routes" / "historical"
            core_stage = publication.stage_historical_replay_core(
                data_dir=self.data_dir,
                config=self.run["config"],
                publication_lease=lease,
            )
            lease = None
            self.context = publication.publish_historical_replay_core(
                data_dir=self.data_dir,
                staged_core=core_stage,
            )
            core_stage = None
            staged = publication.stage_historical_replay_bundle(
                data_dir=self.data_dir,
                raw_root=self.raw_root,
                context=self.context,
            )
            staged_subject = staged["verification_subject"]
            pointer_publication = staged["pointer_publication"]
            request = verifier._connected_request_for_subject(staged_subject)
            observation = _fresh_offline_observation(request)
            with mock.patch.object(
                verifier,
                "_invoke_connected_historical_verification_engine",
                return_value=(DEMO_EVIDENCE_MODE, observation),
            ):
                verification = verifier.run_connected_historical_verification(
                    staged_subject,
                    mode="staged",
                )
            if (
                verification["report"].get("status")
                != DEMO_VERIFICATION_STATUS
                or verification["report"].get("evidence_mode")
                != DEMO_EVIDENCE_MODE
                or verification.get("install_result") is not None
            ):
                raise RuntimeError("offline fixture evidence contract differs")
            self.pointer = dict(verification["final_pointer"])
            self._verification_report = verification["report"]
            self._verification_report_sha256 = verification["report_sha256"]

            staged_subject.close()
            staged_subject = None
            pointer_publication.close()
            pointer_publication = None

            validated = publication.validate_historical_replay_bundle(
                data_dir=self.data_dir,
                raw_root=self.raw_root,
                bundle_path=staged["path"],
                expected_pointer_core=verification["pointer_core"],
            )
            self._loaded_subject = validated["verification_subject"]
            self._loaded = {
                **dict(validated),
                "routes": validated["bundle"]["routes"],
                "verification_report": verification["report"],
                "verification_report_sha256": verification[
                    "report_sha256"
                ],
            }
            manifest = self._loaded["manifest"]
            evidence = self._loaded["replay_evidence"]
            generation_projection = {
                "contract_version": DEMO_CONTRACT,
                "demo_fixture": True,
                "evidence_mode": DEMO_EVIDENCE_MODE,
                "verification_status": DEMO_VERIFICATION_STATUS,
                "replay_id": manifest["replay_id"],
                "manifest_sha256": self._loaded["manifest_sha256"],
                "verification_report_sha256": (
                    self._verification_report_sha256
                ),
                "scenario_set_sha256": evidence["scenario_set_sha256"],
            }
            self.data_generation = hashlib.sha256(
                _canonical_bytes(generation_projection)
            ).hexdigest()
        except BaseException:
            if staged_subject is not None:
                staged_subject.close()
            if pointer_publication is not None:
                pointer_publication.close()
            if core_stage is not None:
                core_stage.close()
            if lease is not None:
                lease.close()
            self.close()
            raise

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
        """Project the fixture into a wire contract that cannot imply verification."""

        from dashboard import opportunity_facts as facts

        with self._lock:
            if self._loaded is None:
                raise RuntimeError("offline fixture is closed")
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
            self._loaded["validated_view"].reread_unchanged()
            rows = facts._historical_projected_rows(
                self._loaded,
                expected_verification_status=DEMO_VERIFICATION_STATUS,
                expected_evidence_mode=DEMO_EVIDENCE_MODE,
            )
            filtered = [
                row for row in rows if facts._matches_filters(row, filters)
            ]
            filtered = facts._sort_routes(
                filtered,
                str(filters["sort"]),
                str(filters["direction"]),
            )
            manifest = self._loaded["manifest"]
            evidence = self._loaded["replay_evidence"]
            selected = manifest["selected_block"]
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
                    "temporal_scope": "historical_replay",
                    "execution_claim": manifest["execution_claim"],
                    "simulation_basis": facts.HISTORICAL_SIMULATION_BASIS,
                    "data_generation": self.data_generation,
                    "replay_id": manifest["replay_id"],
                    "route_cohort_id": manifest["route_cohort_id"],
                    "manifest_sha256": self._loaded["manifest_sha256"],
                    "verification_report_sha256": (
                        self._verification_report_sha256
                    ),
                    "policy_sha256": manifest["policy_sha256"],
                    "run_id": manifest["run_id"],
                    "run_manifest_sha256": manifest[
                        "run_manifest_sha256"
                    ],
                    "selection_sha256": manifest["selection_sha256"],
                    "scenario_set_sha256": evidence[
                        "scenario_set_sha256"
                    ],
                    "selected_block_number": selected["number"],
                    "selected_block_hash": selected["hash"],
                    "selected_block_timestamp": (
                        facts._historical_block_timestamp(
                            selected["timestamp"]
                        )
                    ),
                    "simulation_block_number": selected[
                        "synthetic_child_number"
                    ],
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
                    "reason_code": "historical_replay",
                    "next_deadline": None,
                },
                "filters": filters,
                "routes": filtered,
            }
            self._loaded["validated_view"].reread_unchanged()
            return payload

    def close(self) -> None:
        if self._loaded_subject is not None:
            try:
                self._loaded_subject.close()
            except Exception:
                pass
            self._loaded_subject = None
            self._loaded = None
        if self.context is not None:
            try:
                self.context.close()
            except Exception:
                pass
            self.context = None
        if self.run is not None:
            self._helper._close_real_task7_run(
                self.run,
                self.finalized,
            )
            self.run = self.finalized = None

    def __enter__(self) -> "HistoricalOpportunityDemoFixture":
        return self

    def __exit__(self, _error_type: Any, _error: Any, _traceback: Any) -> None:
        self.close()
