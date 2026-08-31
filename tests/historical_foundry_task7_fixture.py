"""Deterministic Phase-3 preflight projection for finalized Task-7 test runs.

This module is test support only.  It does not mint a production capability,
open a path, or represent connected replay evidence.
"""

import hashlib
import json
import re
from types import MappingProxyType


PHASE3_FINAL_RUN_FIXTURE_SCHEMA = (
    "historical_foundry_phase3_final_run_fixture/v1"
)


def canonical_phase3_final_run_fixture(
    *, run_id, run_manifest_sha256, selection_sha256,
    selection_status, selected_block, market_ids, scenario_count,
):
    if (
        type(run_id) is not str
        or re.fullmatch(r"run:[0-9a-f]{64}", run_id) is None
        or type(run_manifest_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", run_manifest_sha256) is None
        or type(selection_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", selection_sha256) is None
        or selection_status not in (
            "found_publishable_profitable_block",
            "no_publishable_profitable_block",
        )
        or selected_block is not None and (
            type(selected_block) is not int or selected_block < 0
        )
        or type(market_ids) not in (tuple, list)
        or any(type(value) is not str or not value for value in market_ids)
        or len(set(market_ids)) != len(market_ids)
        or type(scenario_count) is not int
        or scenario_count < 0
        or (
            selection_status == "found_publishable_profitable_block"
            and (
                selected_block is None
                or len(market_ids) != 2
                or scenario_count != 10
            )
        )
        or (
            selection_status == "no_publishable_profitable_block"
            and (selected_block is not None or market_ids or scenario_count)
        )
    ):
        raise ValueError("Task-7 final-run test fixture is invalid")
    projection = {
        "schema": PHASE3_FINAL_RUN_FIXTURE_SCHEMA,
        "run_id": run_id,
        "run_manifest_sha256": run_manifest_sha256,
        "selection_sha256": selection_sha256,
        "selection_status": selection_status,
        "selected_block": selected_block,
        "market_ids": sorted(market_ids),
        "scenario_count": scenario_count,
        "evidence_mode": "offline_test_fixture",
    }
    canonical = json.dumps(
        projection, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return MappingProxyType({
        "schema": PHASE3_FINAL_RUN_FIXTURE_SCHEMA,
        "projection_sha256": hashlib.sha256(canonical).hexdigest(),
        "canonical_bytes": canonical,
    })
