"""Fail-closed coverage checks for publishing fact snapshots.

The gate protects a healthy ``latest`` snapshot from being replaced by a
candidate whose collection coverage has materially regressed.  It deliberately
uses integer cross-products for basis-point comparisons, so boundary behaviour
does not depend on floating-point rounding.

``excluded_statuses`` removes expected non-measurements (for example a DEX
adapter's ``unsupported`` rows) from the candidate's absolute-coverage
denominator.  Exclusion does *not* erase history: if a common identity was
usable in the baseline and becomes excluded in the candidate, that identity is
still a lost prior success.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import hashlib
import hmac
import json
from pathlib import Path
import secrets
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Set, Tuple


COVERAGE_GATE_SCHEMA = "publication_coverage_gate/v1"
COVERAGE_GATE_BUNDLE_SCHEMA = "publication_coverage_gate_bundle/v1"
COVERAGE_GATE_LOG_MARKER = "PUBLICATION_COVERAGE_GATE="
_PREFLIGHT_SEAL_KEY = secrets.token_bytes(32)


class CoverageRegressionError(RuntimeError):
    """Raised when a candidate must not replace the published snapshot."""

    def __init__(self, report: Mapping[str, Any]):
        self.report = dict(report)
        self.report_json = json.dumps(
            self.report,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        super().__init__(COVERAGE_GATE_LOG_MARKER + self.report_json)

    def __str__(self) -> str:
        return COVERAGE_GATE_LOG_MARKER + self.report_json


def _passing_coverage_report_copy(
    report: Mapping[str, Any],
    *,
    fact_family: str,
) -> Dict[str, Any]:
    if not isinstance(report, Mapping):
        raise TypeError("preflight coverage report must be a mapping")
    copied = dict(report)
    if copied.get("schema") != COVERAGE_GATE_SCHEMA:
        raise ValueError("preflight coverage report has an invalid schema")
    if copied.get("gate") != "coverage_regression":
        raise ValueError("preflight coverage report has an invalid gate")
    if copied.get("fact_family") != fact_family:
        raise ValueError(
            "preflight coverage report does not match {}".format(fact_family)
        )
    if copied.get("status") != "passed" or copied.get("passed") is not True:
        raise ValueError("preflight coverage report is not passing")
    if copied.get("reasons") or copied.get("errors"):
        raise ValueError("preflight coverage report has rejection evidence")
    candidate = copied.get("candidate")
    comparison = copied.get("comparison")
    cohort = copied.get("cohort")
    if (
        not isinstance(candidate, Mapping)
        or candidate.get("absolute_check") not in {"passed", "skipped"}
        or not isinstance(comparison, Mapping)
        or comparison.get("retention_check") not in {"passed", "skipped"}
        or not isinstance(cohort, Mapping)
        or cohort.get("failed_checks")
    ):
        raise ValueError("preflight coverage report has rejected checks")
    return copied


def _coverage_report_seal(report: Mapping[str, Any]) -> str:
    payload = dict(report)
    payload.pop("publication_seal", None)
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hmac.new(_PREFLIGHT_SEAL_KEY, encoded, hashlib.sha256).hexdigest()


def validate_passing_coverage_report(
    report: Mapping[str, Any],
    *,
    fact_family: str,
    candidate_rows: Optional[Iterable[Mapping[str, Any]]] = None,
    identity: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    baseline_path: Optional[Path] = None,
    expected_policy: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Validate a sealed preflight report before publication reuses it."""

    copied = _passing_coverage_report_copy(
        report,
        fact_family=fact_family,
    )
    supplied_seal = copied.get("publication_seal")
    if (
        not isinstance(supplied_seal, str)
        or not hmac.compare_digest(
            supplied_seal,
            _coverage_report_seal(copied),
        )
    ):
        raise ValueError("preflight coverage report has an invalid seal")
    if (candidate_rows is None) != (identity is None):
        raise ValueError(
            "candidate_rows and identity must be provided together"
        )
    if candidate_rows is not None and identity is not None:
        materialised, materialise_error = _materialise_rows(candidate_rows)
        indexed, identity_errors = _index_rows(
            materialised,
            identity,
            label="candidate",
        )
        if materialise_error or identity_errors:
            raise ValueError(
                "cannot validate preflight report against candidate rows"
            )
        candidate = copied.get("candidate")
        expected_fingerprint = (
            candidate.get("identity_row_sha256")
            if isinstance(candidate, Mapping)
            else None
        )
        if (
            not expected_fingerprint
            or expected_fingerprint
            != _identity_row_sha256(indexed)
        ):
            raise ValueError(
                "preflight coverage report does not match candidate rows"
            )
    if baseline_path is not None:
        resolved_baseline = baseline_path.resolve()
        context = copied.get("publication_context")
        if not isinstance(context, Mapping):
            raise ValueError(
                "preflight coverage report has no publication context"
            )
        current_exists = resolved_baseline.exists()
        current_sha256 = (
            _file_sha256(resolved_baseline) if current_exists else None
        )
        if (
            context.get("baseline_path") != str(resolved_baseline)
            or context.get("baseline_exists") is not current_exists
            or context.get("baseline_sha256") != current_sha256
        ):
            raise ValueError(
                "preflight coverage report does not match current baseline"
            )
    if expected_policy is not None:
        actual_policy = {
            "thresholds": copied.get("thresholds"),
            "usable_statuses": copied.get("usable_statuses"),
            "excluded_statuses": copied.get("excluded_statuses"),
            "valid_statuses": copied.get("valid_statuses"),
        }
        if actual_policy != dict(expected_policy):
            raise ValueError(
                "preflight coverage report does not match publication policy"
            )
    return copied


def bind_passing_coverage_report(
    report: Mapping[str, Any],
    *,
    fact_family: str,
    baseline_path: Path,
) -> Dict[str, Any]:
    """Bind a passing report to the exact baseline file used by preflight."""

    copied = _passing_coverage_report_copy(
        report,
        fact_family=fact_family,
    )
    resolved_baseline = baseline_path.resolve()
    baseline_exists = resolved_baseline.exists()
    copied["publication_context"] = {
        "baseline_path": str(resolved_baseline),
        "baseline_exists": baseline_exists,
        "baseline_sha256": (
            _file_sha256(resolved_baseline) if baseline_exists else None
        ),
    }
    copied["publication_seal"] = _coverage_report_seal(copied)
    return copied


def enforce_publication_coverage_bundle(
    checks: Iterable[Tuple[str, Callable[[], Mapping[str, Any]]]],
    *,
    bundle: str,
) -> Dict[str, Dict[str, Any]]:
    """Evaluate every family, then reject the bundle with all gate evidence."""

    reports: Dict[str, Dict[str, Any]] = {}
    rejected = False
    for fact_family, check in checks:
        try:
            report = dict(check())
        except CoverageRegressionError as exc:
            report = dict(exc.report)
            rejected = True
        if report.get("fact_family") != fact_family:
            raise ValueError(
                "coverage report does not match {}".format(fact_family)
            )
        if report.get("status") == "passed" and report.get("passed") is True:
            validate_passing_coverage_report(
                report,
                fact_family=fact_family,
            )
        elif report.get("status") == "rejected" and report.get("passed") is False:
            rejected = True
        else:
            raise ValueError(
                "coverage report for {} has invalid status".format(fact_family)
            )
        reports[fact_family] = report

    if not reports:
        raise ValueError("publication coverage bundle has no checks")
    if rejected:
        raise CoverageRegressionError(
            {
                "schema": COVERAGE_GATE_BUNDLE_SCHEMA,
                "gate": "coverage_regression_bundle",
                "bundle": str(bundle),
                "status": "rejected",
                "passed": False,
                "reasons": ["one_or_more_publication_gates_rejected"],
                "publication_gates": reports,
            }
        )
    return reports


def _validate_bps(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("{} must be an integer".format(name))
    if value < 0 or value > 10_000:
        raise ValueError("{} must be between 0 and 10000".format(name))
    return value


def _validate_positive_integer(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("{} must be an integer".format(name))
    if value < 1:
        raise ValueError("{} must be at least 1".format(name))
    return value


def _normalise_statuses(statuses: Iterable[str]) -> Set[str]:
    return {str(status).strip() for status in statuses}


def _row_status(row: Mapping[str, Any]) -> str:
    value = row.get("status")
    if value is None:
        return ""
    return str(value).strip()


def _status_counts(rows: List[Any]) -> Dict[str, int]:
    counts: Counter = Counter()
    for row in rows:
        if not isinstance(row, Mapping):
            counts["<invalid-row>"] += 1
            continue
        try:
            status = _row_status(row)
        except Exception:
            counts["<status-error>"] += 1
            continue
        counts[status or "<missing>"] += 1
    return dict(sorted(counts.items()))


def _status_errors(
    rows: List[Any],
    *,
    label: str,
    valid_statuses: Optional[Set[str]],
) -> List[Dict[str, Any]]:
    if valid_statuses is None:
        return []
    errors = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            continue
        status = _row_status(row)
        if not status:
            errors.append(
                {
                    "code": "{}_status_missing".format(label),
                    "row_index": index,
                }
            )
        elif status not in valid_statuses:
            errors.append(
                {
                    "code": "{}_status_invalid".format(label),
                    "row_index": index,
                    "status": status,
                }
            )
    return errors


def _is_missing_key(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, bytes):
        return not value
    if isinstance(value, (tuple, list)):
        return not value or any(_is_missing_key(part) for part in value)
    return False


def _display_key(value: Any) -> Any:
    """Return a deterministic JSON-safe representation for diagnostics."""

    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return repr(value)
    return value


def _identity_status_sha256(
    indexed: Mapping[Any, Mapping[str, Any]],
) -> str:
    entries = [
        json.dumps(
            [_display_key(key), _row_status(row)],
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        for key, row in indexed.items()
    ]
    entries.sort()
    return hashlib.sha256("\n".join(entries).encode("utf-8")).hexdigest()


def _identity_row_sha256(
    indexed: Mapping[Any, Mapping[str, Any]],
) -> str:
    """Fingerprint every published field, not only identity and status."""
    entries = [
        json.dumps(
            [_display_key(key), dict(row)],
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        for key, row in indexed.items()
    ]
    entries.sort()
    return hashlib.sha256("\n".join(entries).encode("utf-8")).hexdigest()


def publication_rows_sha256(
    rows: Iterable[Mapping[str, Any]],
    *,
    identity: Callable[[Mapping[str, Any]], Any],
) -> str:
    """Return the canonical complete-row fingerprint used by sealed reports."""
    materialised, materialise_error = _materialise_rows(rows)
    indexed, identity_errors = _index_rows(
        materialised,
        identity,
        label="candidate",
    )
    if materialise_error or identity_errors:
        raise ValueError("cannot fingerprint publication rows")
    return _identity_row_sha256(indexed)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _materialise_rows(
    rows: Optional[Iterable[Mapping[str, Any]]],
) -> Tuple[List[Any], Optional[str]]:
    try:
        return list(rows or ()), None
    except Exception as exc:
        return [], "{}: {}".format(type(exc).__name__, exc)


def _index_rows(
    rows: List[Any],
    identity: Callable[[Mapping[str, Any]], Any],
    *,
    label: str,
) -> Tuple[Dict[Any, Mapping[str, Any]], List[Dict[str, Any]]]:
    indexed: Dict[Any, Mapping[str, Any]] = {}
    errors: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            errors.append(
                {
                    "code": "{}_row_not_mapping".format(label),
                    "row_index": index,
                }
            )
            continue
        try:
            key = identity(row)
        except Exception as exc:
            errors.append(
                {
                    "code": "{}_identity_callback_error".format(label),
                    "row_index": index,
                    "error": "{}: {}".format(type(exc).__name__, exc),
                }
            )
            continue
        if _is_missing_key(key):
            errors.append(
                {
                    "code": "{}_identity_missing".format(label),
                    "row_index": index,
                }
            )
            continue
        try:
            hash(key)
        except Exception as exc:
            errors.append(
                {
                    "code": "{}_identity_unhashable".format(label),
                    "row_index": index,
                    "error": "{}: {}".format(type(exc).__name__, exc),
                }
            )
            continue
        if key in indexed:
            errors.append(
                {
                    "code": "{}_identity_duplicate".format(label),
                    "row_index": index,
                    "identity": _display_key(key),
                }
            )
            continue
        indexed[key] = row
    return indexed, errors


def _floor_bps(numerator: int, denominator: int) -> Optional[int]:
    if denominator <= 0:
        return None
    return numerator * 10_000 // denominator


def _meets_bps(numerator: int, denominator: int, minimum_bps: int) -> bool:
    if denominator <= 0:
        return True
    return numerator * 10_000 >= denominator * minimum_bps


def _append_skipped(report: Dict[str, Any], reason: str) -> None:
    report["skipped_reasons"].append(reason)


def evaluate_publication_coverage(
    candidate_rows: Iterable[Mapping[str, Any]],
    baseline_rows: Optional[Iterable[Mapping[str, Any]]],
    *,
    fact_family: str,
    identity: Callable[[Mapping[str, Any]], Any],
    usable_statuses: Iterable[str],
    excluded_statuses: Iterable[str] = frozenset(),
    valid_statuses: Optional[Iterable[str]] = None,
    allow_no_eligible_candidate: bool = False,
    minimum_candidate_usable_bps: int = 8000,
    minimum_baseline_retention_bps: int = 9500,
    cohort: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    minimum_cohort_baseline_count: int = 5,
    minimum_cohort_lost_count: int = 2,
    minimum_cohort_retention_bps: int = 5000,
) -> Dict[str, Any]:
    """Evaluate whether ``candidate_rows`` are safe to publish.

    The returned report is always JSON serialisable.  Data/identity problems
    produce a rejected report rather than escaping as callback exceptions.
    Invalid threshold configuration remains a programmer error and raises
    ``TypeError`` or ``ValueError``.
    """

    minimum_candidate_usable_bps = _validate_bps(
        "minimum_candidate_usable_bps", minimum_candidate_usable_bps
    )
    minimum_baseline_retention_bps = _validate_bps(
        "minimum_baseline_retention_bps", minimum_baseline_retention_bps
    )
    minimum_cohort_retention_bps = _validate_bps(
        "minimum_cohort_retention_bps", minimum_cohort_retention_bps
    )
    minimum_cohort_baseline_count = _validate_positive_integer(
        "minimum_cohort_baseline_count", minimum_cohort_baseline_count
    )
    minimum_cohort_lost_count = _validate_positive_integer(
        "minimum_cohort_lost_count", minimum_cohort_lost_count
    )
    if not isinstance(allow_no_eligible_candidate, bool):
        raise TypeError("allow_no_eligible_candidate must be a boolean")

    usable = _normalise_statuses(usable_statuses)
    excluded = _normalise_statuses(excluded_statuses)
    valid = (
        _normalise_statuses(valid_statuses)
        if valid_statuses is not None
        else None
    )
    if usable.intersection(excluded):
        raise ValueError("usable_statuses and excluded_statuses must be disjoint")
    if valid is not None and not usable.union(excluded).issubset(valid):
        raise ValueError(
            "valid_statuses must include every usable and excluded status"
        )
    candidate, candidate_materialise_error = _materialise_rows(candidate_rows)
    baseline_present = baseline_rows is not None
    baseline, baseline_materialise_error = _materialise_rows(baseline_rows)

    candidate_status_counts = _status_counts(candidate)
    baseline_status_counts = _status_counts(baseline)
    candidate_eligible_count = sum(
        count
        for status, count in candidate_status_counts.items()
        if status not in excluded and not status.startswith("<")
    )
    candidate_usable_count = sum(
        count
        for status, count in candidate_status_counts.items()
        if status in usable and status not in excluded
    )
    baseline_usable_count = sum(
        count
        for status, count in baseline_status_counts.items()
        if status in usable and status not in excluded
    )

    report: Dict[str, Any] = {
        "schema": COVERAGE_GATE_SCHEMA,
        "gate": "coverage_regression",
        "fact_family": str(fact_family),
        "thresholds": {
            "allow_no_eligible_candidate": allow_no_eligible_candidate,
            "minimum_candidate_usable_bps": minimum_candidate_usable_bps,
            "minimum_baseline_retention_bps": minimum_baseline_retention_bps,
            "minimum_cohort_baseline_count": minimum_cohort_baseline_count,
            "minimum_cohort_lost_count": minimum_cohort_lost_count,
            "minimum_cohort_retention_bps": minimum_cohort_retention_bps,
        },
        "usable_statuses": sorted(usable),
        "excluded_statuses": sorted(excluded),
        "valid_statuses": sorted(valid) if valid is not None else None,
        "candidate": {
            "row_count": len(candidate),
            "status_counts": candidate_status_counts,
            "eligible_count": candidate_eligible_count,
            "usable_count": candidate_usable_count,
            "usable_bps": _floor_bps(
                candidate_usable_count, candidate_eligible_count
            ),
            "identity_status_sha256": None,
            "identity_row_sha256": None,
            "absolute_check": "pending",
        },
        "baseline": {
            "present": baseline_present,
            "row_count": len(baseline),
            "status_counts": baseline_status_counts,
            "usable_count": baseline_usable_count,
            "identity_status_sha256": None,
            "identity_row_sha256": None,
        },
        "comparison": {
            "common_identity_count": 0,
            "comparable_baseline_usable_count": 0,
            "retained_count": 0,
            "lost_count": 0,
            "retention_bps": None,
            "retention_check": "pending",
        },
        "cohort": {
            "enabled": cohort is not None,
            "checks": [],
            "failed_checks": [],
        },
        "status": "passed",
        "passed": True,
        "reasons": [],
        "errors": [],
        "skipped_reason": None,
        "skipped_reasons": [],
    }

    if candidate_materialise_error:
        report["errors"].append(
            {
                "code": "candidate_rows_not_iterable",
                "error": candidate_materialise_error,
            }
        )
    if baseline_materialise_error:
        report["errors"].append(
            {
                "code": "baseline_rows_not_iterable",
                "error": baseline_materialise_error,
            }
        )
    elif baseline_present and not baseline:
        report["errors"].append({"code": "baseline_has_no_rows"})

    candidate_index, candidate_identity_errors = _index_rows(
        candidate, identity, label="candidate"
    )
    if not candidate_materialise_error and not candidate_identity_errors:
        try:
            report["candidate"][
                "identity_status_sha256"
            ] = _identity_status_sha256(candidate_index)
            report["candidate"][
                "identity_row_sha256"
            ] = _identity_row_sha256(candidate_index)
        except Exception as exc:
            report["errors"].append(
                {
                    "code": "candidate_fingerprint_error",
                    "error": "{}: {}".format(type(exc).__name__, exc),
                }
            )
    baseline_index: Dict[Any, Mapping[str, Any]] = {}
    baseline_identity_errors: List[Dict[str, Any]] = []
    if baseline_present:
        baseline_index, baseline_identity_errors = _index_rows(
            baseline, identity, label="baseline"
        )
        if not baseline_materialise_error and not baseline_identity_errors:
            try:
                report["baseline"][
                    "identity_status_sha256"
                ] = _identity_status_sha256(baseline_index)
                report["baseline"][
                    "identity_row_sha256"
                ] = _identity_row_sha256(baseline_index)
            except Exception as exc:
                report["errors"].append(
                    {
                        "code": "baseline_fingerprint_error",
                        "error": "{}: {}".format(type(exc).__name__, exc),
                    }
                )
    report["errors"].extend(candidate_identity_errors)
    report["errors"].extend(baseline_identity_errors)
    report["errors"].extend(
        _status_errors(
            candidate,
            label="candidate",
            valid_statuses=valid,
        )
    )
    if baseline_present:
        report["errors"].extend(
            _status_errors(
                baseline,
                label="baseline",
                valid_statuses=valid,
            )
        )

    if not candidate:
        report["reasons"].append("candidate_has_no_rows")
        report["candidate"]["absolute_check"] = "rejected"
    elif candidate_eligible_count == 0:
        if allow_no_eligible_candidate:
            report["candidate"]["absolute_check"] = "skipped"
            _append_skipped(report, "no_candidate_eligible_rows_allowed")
        else:
            report["candidate"]["absolute_check"] = "rejected"
            report["reasons"].append("candidate_has_no_eligible_rows")
    elif _meets_bps(
        candidate_usable_count,
        candidate_eligible_count,
        minimum_candidate_usable_bps,
    ):
        report["candidate"]["absolute_check"] = "passed"
    else:
        report["candidate"]["absolute_check"] = "rejected"
        report["reasons"].append("candidate_usable_coverage_below_threshold")

    if not baseline_present:
        report["comparison"]["retention_check"] = "skipped"
        _append_skipped(report, "no_baseline")
    elif not report["errors"]:
        common_ids = set(candidate_index).intersection(baseline_index)
        comparable_ids = {
            key
            for key in common_ids
            if _row_status(baseline_index[key]) in usable
            and _row_status(baseline_index[key]) not in excluded
        }
        retained_ids = {
            key
            for key in comparable_ids
            if _row_status(candidate_index[key]) in usable
            and _row_status(candidate_index[key]) not in excluded
        }
        lost_ids = comparable_ids.difference(retained_ids)
        report["comparison"].update(
            {
                "common_identity_count": len(common_ids),
                "comparable_baseline_usable_count": len(comparable_ids),
                "retained_count": len(retained_ids),
                "lost_count": len(lost_ids),
                "retention_bps": _floor_bps(
                    len(retained_ids), len(comparable_ids)
                ),
            }
        )

        if not comparable_ids:
            report["comparison"]["retention_check"] = "skipped"
            _append_skipped(report, "no_common_previous_success")
        elif _meets_bps(
            len(retained_ids),
            len(comparable_ids),
            minimum_baseline_retention_bps,
        ):
            report["comparison"]["retention_check"] = "passed"
        else:
            report["comparison"]["retention_check"] = "rejected"
            report["reasons"].append("baseline_retention_below_threshold")

        if cohort is not None and comparable_ids:
            grouped: Dict[Any, List[Any]] = defaultdict(list)
            for key in comparable_ids:
                try:
                    cohort_key = cohort(baseline_index[key])
                except Exception as exc:
                    report["errors"].append(
                        {
                            "code": "cohort_callback_error",
                            "identity": _display_key(key),
                            "error": "{}: {}".format(type(exc).__name__, exc),
                        }
                    )
                    continue
                if _is_missing_key(cohort_key):
                    report["errors"].append(
                        {
                            "code": "cohort_identity_missing",
                            "identity": _display_key(key),
                        }
                    )
                    continue
                try:
                    hash(cohort_key)
                except Exception as exc:
                    report["errors"].append(
                        {
                            "code": "cohort_identity_unhashable",
                            "identity": _display_key(key),
                            "error": "{}: {}".format(type(exc).__name__, exc),
                        }
                    )
                    continue
                grouped[cohort_key].append(key)

            checks: List[Dict[str, Any]] = []
            for cohort_key, keys in grouped.items():
                cohort_baseline_count = len(keys)
                cohort_retained_count = sum(
                    1 for key in keys if key in retained_ids
                )
                cohort_lost_count = cohort_baseline_count - cohort_retained_count
                check: Dict[str, Any] = {
                    "cohort": _display_key(cohort_key),
                    "baseline_usable_count": cohort_baseline_count,
                    "retained_count": cohort_retained_count,
                    "lost_count": cohort_lost_count,
                    "retention_bps": _floor_bps(
                        cohort_retained_count, cohort_baseline_count
                    ),
                    "status": "skipped",
                    "skipped_reason": None,
                }
                if cohort_baseline_count < minimum_cohort_baseline_count:
                    check["skipped_reason"] = "baseline_count_below_minimum"
                elif cohort_lost_count < minimum_cohort_lost_count:
                    check["skipped_reason"] = "lost_count_below_minimum"
                elif _meets_bps(
                    cohort_retained_count,
                    cohort_baseline_count,
                    minimum_cohort_retention_bps,
                ):
                    check["status"] = "passed"
                else:
                    check["status"] = "rejected"
                checks.append(check)

            checks.sort(
                key=lambda item: json.dumps(
                    item["cohort"],
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            failed_checks = [
                check for check in checks if check["status"] == "rejected"
            ]
            report["cohort"]["checks"] = checks
            report["cohort"]["failed_checks"] = failed_checks
            if failed_checks:
                report["reasons"].append("cohort_retention_below_threshold")
    elif baseline_present:
        report["comparison"]["retention_check"] = "rejected"

    if report["errors"]:
        report["reasons"].append("invalid_coverage_identity_data")

    report["reasons"] = list(dict.fromkeys(report["reasons"]))
    report["skipped_reasons"] = list(dict.fromkeys(report["skipped_reasons"]))
    if report["skipped_reasons"]:
        report["skipped_reason"] = ";".join(report["skipped_reasons"])

    if report["reasons"] or report["errors"]:
        report["status"] = "rejected"
        report["passed"] = False
    return report


def enforce_publication_coverage(
    candidate_rows: Iterable[Mapping[str, Any]],
    baseline_rows: Optional[Iterable[Mapping[str, Any]]],
    *,
    fact_family: str,
    identity: Callable[[Mapping[str, Any]], Any],
    usable_statuses: Iterable[str],
    excluded_statuses: Iterable[str] = frozenset(),
    valid_statuses: Optional[Iterable[str]] = None,
    allow_no_eligible_candidate: bool = False,
    minimum_candidate_usable_bps: int = 8000,
    minimum_baseline_retention_bps: int = 9500,
    cohort: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    minimum_cohort_baseline_count: int = 5,
    minimum_cohort_lost_count: int = 2,
    minimum_cohort_retention_bps: int = 5000,
) -> Dict[str, Any]:
    """Return a passing report or raise ``CoverageRegressionError``."""

    report = evaluate_publication_coverage(
        candidate_rows,
        baseline_rows,
        fact_family=fact_family,
        identity=identity,
        usable_statuses=usable_statuses,
        excluded_statuses=excluded_statuses,
        valid_statuses=valid_statuses,
        allow_no_eligible_candidate=allow_no_eligible_candidate,
        minimum_candidate_usable_bps=minimum_candidate_usable_bps,
        minimum_baseline_retention_bps=minimum_baseline_retention_bps,
        cohort=cohort,
        minimum_cohort_baseline_count=minimum_cohort_baseline_count,
        minimum_cohort_lost_count=minimum_cohort_lost_count,
        minimum_cohort_retention_bps=minimum_cohort_retention_bps,
    )
    if report["status"] == "rejected":
        raise CoverageRegressionError(report)
    return report
