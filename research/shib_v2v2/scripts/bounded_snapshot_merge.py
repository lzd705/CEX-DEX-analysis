"""Fail-closed merge for one freshly collected market into a full snapshot.

The per-row observation timestamps and raw hashes remain the source evidence.
``snapshot_id`` identifies the published latest-view generation, so every row
is rebound to the new generation after the exact target is replaced.  This
keeps consumers on one coherent publication without making another network
request for any non-target market.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Hashable, Iterable, List, Sequence


Row = Dict[str, str]


def _one_snapshot_id(rows: Iterable[Row], *, label: str) -> str:
    snapshot_ids = {
        str(row.get("snapshot_id") or "").strip()
        for row in rows
    }
    if len(snapshot_ids) != 1 or not next(iter(snapshot_ids)):
        raise ValueError(
            "{} must contain one non-empty snapshot identity".format(label)
        )
    return next(iter(snapshot_ids))


def _unique_rows(
    rows: Iterable[Row],
    *,
    row_identity: Callable[[Row], Hashable],
    label: str,
) -> Dict[Hashable, Row]:
    indexed: Dict[Hashable, Row] = {}
    for source_row in rows:
        row = dict(source_row)
        identity = row_identity(row)
        if identity in indexed:
            raise ValueError("{} contains duplicate row identity".format(label))
        indexed[identity] = row
    return indexed


def _uniform_schema(rows: List[Row], *, label: str) -> set:
    if not rows:
        raise ValueError("{} is empty".format(label))
    schema = set(rows[0])
    if any(set(row) != schema for row in rows):
        raise ValueError("{} contains inconsistent schema".format(label))
    return schema


def require_aligned_depth_execution_lineage(
    depth_rows: List[Row],
    execution_rows: List[Row],
) -> str:
    """Return the cohort ID only when depth and execution share one source."""
    depth_snapshot_id = _one_snapshot_id(
        depth_rows,
        label="depth publication",
    )
    execution_snapshot_id = _one_snapshot_id(
        execution_rows,
        label="execution publication",
    )
    source_snapshot_ids = {
        str(row.get("source_snapshot_id") or "").strip()
        for row in execution_rows
    }
    if (
        execution_snapshot_id != depth_snapshot_id
        or source_snapshot_ids != {depth_snapshot_id}
    ):
        raise ValueError(
            "depth and execution must identify the same source publication"
        )
    return depth_snapshot_id


def merge_exact_market_snapshot(
    baseline_rows: List[Row],
    candidate_rows: List[Row],
    *,
    target_market_id: str,
    market_id_for_row: Callable[[Row], str],
    row_identity: Callable[[Row], Hashable],
    rebind_source_snapshot_id: bool = False,
    allow_target_insert: bool = False,
) -> List[Row]:
    """Replace exactly one market while preserving every non-target fact.

    Candidate scenario coverage must exactly equal the target's baseline keys.
    This protects execution snapshots from accepting nine of ten notionals or
    one direction only. A caller may explicitly permit insertion of one absent
    target row; this is reserved for a cataloged scalar fact such as TVL and is
    never the default. Schema drift, missing publication baselines,
    cross-market rows, duplicate identities, and reused publication identities
    fail closed.
    """
    target = str(target_market_id or "").strip()
    if not target:
        raise ValueError("exact target market identity is empty")
    baseline = [dict(row) for row in baseline_rows]
    candidate = [dict(row) for row in candidate_rows]
    baseline_schema = _uniform_schema(baseline, label="publication baseline")
    candidate_schema = _uniform_schema(candidate, label="exact target candidate")
    if candidate_schema != baseline_schema:
        raise ValueError("exact target candidate schema does not match baseline")

    baseline_snapshot_id = _one_snapshot_id(
        baseline,
        label="publication baseline",
    )
    candidate_snapshot_id = _one_snapshot_id(
        candidate,
        label="exact target candidate",
    )
    if candidate_snapshot_id == baseline_snapshot_id:
        raise ValueError("exact refresh did not create a new publication identity")
    if rebind_source_snapshot_id:
        for rows, snapshot_id, label in (
            (baseline, baseline_snapshot_id, "publication baseline"),
            (candidate, candidate_snapshot_id, "exact target candidate"),
        ):
            source_snapshot_ids = {
                str(row.get("source_snapshot_id") or "").strip()
                for row in rows
            }
            if source_snapshot_ids != {snapshot_id}:
                raise ValueError(
                    "{} has incoherent source snapshot lineage".format(label)
                )

    baseline_by_identity = _unique_rows(
        baseline,
        row_identity=row_identity,
        label="publication baseline",
    )
    candidate_by_identity = _unique_rows(
        candidate,
        row_identity=row_identity,
        label="exact target candidate",
    )
    baseline_target_keys = {
        identity
        for identity, row in baseline_by_identity.items()
        if market_id_for_row(row) == target
    }
    if {
        market_id_for_row(row)
        for row in candidate_by_identity.values()
    } != {target}:
        raise ValueError("candidate rows do not belong to the exact target market")
    inserting_target = not baseline_target_keys
    if inserting_target and not allow_target_insert:
        raise ValueError("exact target is absent from the publication baseline")
    if inserting_target and len(candidate_by_identity) != 1:
        raise ValueError("exact target insertion requires one candidate row")
    if inserting_target and any(
        identity in baseline_by_identity for identity in candidate_by_identity
    ):
        raise ValueError("exact target insertion collides with the baseline")
    if not inserting_target and set(candidate_by_identity) != baseline_target_keys:
        raise ValueError(
            "exact target candidate scenario coverage does not match baseline"
        )

    merged = []
    for baseline_row in baseline:
        identity = row_identity(baseline_row)
        source = (
            candidate_by_identity[identity]
            if identity in baseline_target_keys
            else baseline_row
        )
        output = dict(source)
        output["snapshot_id"] = candidate_snapshot_id
        if rebind_source_snapshot_id:
            if "source_snapshot_id" not in output:
                raise ValueError(
                    "execution publication lacks source_snapshot_id schema"
                )
            output["source_snapshot_id"] = candidate_snapshot_id
        merged.append(output)

    if inserting_target:
        for candidate_row in candidate:
            output = dict(candidate_row)
            output["snapshot_id"] = candidate_snapshot_id
            if rebind_source_snapshot_id:
                if "source_snapshot_id" not in output:
                    raise ValueError(
                        "execution publication lacks source_snapshot_id schema"
                    )
                output["source_snapshot_id"] = candidate_snapshot_id
            merged.append(output)

    expected_row_count = len(baseline) + (len(candidate) if inserting_target else 0)
    if len(merged) != expected_row_count:
        raise AssertionError("bounded merge changed publication row count")
    if _one_snapshot_id(merged, label="merged publication") != candidate_snapshot_id:
        raise AssertionError("bounded merge produced an incoherent publication")

    rebound_fields = {"snapshot_id"}
    if rebind_source_snapshot_id:
        rebound_fields.add("source_snapshot_id")
    for before, after in zip(baseline, merged):
        if market_id_for_row(before) == target:
            continue
        if {
            key: value
            for key, value in before.items()
            if key not in rebound_fields
        } != {
            key: value
            for key, value in after.items()
            if key not in rebound_fields
        }:
            raise AssertionError("bounded merge changed a non-target fact")
    return merged


def validate_exact_publication_scope(
    baseline_rows: List[Row],
    candidate_rows: List[Row],
    *,
    target_market_id: str,
    market_id_for_row: Callable[[Row], str],
    row_identity: Callable[[Row], Hashable],
    rebound_fields: Sequence[str] = ("snapshot_id",),
) -> Dict[str, Any]:
    """Prove that a full candidate changes only one existing market.

    This is a publication-boundary recheck, independent of the merge helper.
    It prevents a caller from presenting an unrelated full snapshot as an
    exact recovery and binds every target scenario to one new generation.
    """
    target = str(target_market_id or "").strip()
    if not target:
        raise ValueError("exact target market identity is empty")
    baseline = [dict(row) for row in baseline_rows]
    candidate = [dict(row) for row in candidate_rows]
    baseline_schema = _uniform_schema(baseline, label="publication baseline")
    candidate_schema = _uniform_schema(candidate, label="exact publication")
    if candidate_schema != baseline_schema:
        raise ValueError("exact publication schema does not match baseline")

    baseline_snapshot_id = _one_snapshot_id(
        baseline,
        label="publication baseline",
    )
    candidate_snapshot_id = _one_snapshot_id(
        candidate,
        label="exact publication",
    )
    if candidate_snapshot_id == baseline_snapshot_id:
        raise ValueError("exact publication did not create a new identity")

    baseline_by_identity = _unique_rows(
        baseline,
        row_identity=row_identity,
        label="publication baseline",
    )
    candidate_by_identity = _unique_rows(
        candidate,
        row_identity=row_identity,
        label="exact publication",
    )
    if set(candidate_by_identity) != set(baseline_by_identity):
        raise ValueError("exact publication changed the baseline inventory")

    target_identities = {
        identity
        for identity, row in baseline_by_identity.items()
        if market_id_for_row(row) == target
    }
    if not target_identities:
        raise ValueError("exact target is absent from the publication baseline")
    if {
        market_id_for_row(candidate_by_identity[identity])
        for identity in target_identities
    } != {target}:
        raise ValueError("exact target scenarios changed market identity")

    rebound = {str(field) for field in rebound_fields}
    if "snapshot_id" not in rebound:
        raise ValueError("exact publication must rebind snapshot_id")
    for identity, before in baseline_by_identity.items():
        if identity in target_identities:
            continue
        after = candidate_by_identity[identity]
        if {
            key: value for key, value in before.items() if key not in rebound
        } != {
            key: value for key, value in after.items() if key not in rebound
        }:
            raise ValueError("exact publication changed a non-target fact")

    return {
        "market_id": target,
        "baseline_snapshot_id": baseline_snapshot_id,
        "candidate_snapshot_id": candidate_snapshot_id,
        "baseline_row_count": len(baseline),
        "candidate_row_count": len(candidate),
        "target_row_count": len(target_identities),
    }
