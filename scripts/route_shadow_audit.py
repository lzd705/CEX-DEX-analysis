"""Deterministic metrics for one immutable Shadow route cohort."""

from __future__ import annotations

from decimal import (
    Context,
    Decimal,
    Inexact,
    InvalidOperation,
    ROUND_HALF_EVEN,
    Rounded,
    localcontext,
)
import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional

try:
    from scripts.route_publication import _normalize_and_validate_cohort
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from route_publication import _normalize_and_validate_cohort  # type: ignore[no-redef]
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore[no-redef]


ROUTE_SHADOW_AUDIT_SCHEMA = "route_shadow_audit/v1"
IMPLICIT_CANARY_PHASE_SHA256 = hashlib.sha256(
    b"route-shadow-phase/implicit-canary/v1\n"
).hexdigest()

AUDIT_FIELDS = frozenset({
    "schema",
    "run_id",
    "phase",
    "route_cohort_id",
    "phase_state_sha256",
    "phase_transition_id",
    "core_pointer_sha256",
    "core_manifest_sha256",
    "route_cost_evidence_sha256",
    "route_universe_sha256",
    "baseline_manifest_sha256",
    "candidate_source_generation",
    "audit_finished_at",
    "metrics",
})

METRIC_FIELDS = frozenset({
    "leg_availability",
    "timing_availability",
    "conditional_skew_sla",
    "passing_skew_seconds_p95",
    "passing_skew_seconds_max",
    "route_age_seconds_p95",
    "route_age_seconds_max",
})

_RATIO_FIELDS = frozenset({"status", "numerator", "denominator", "value"})
_SAMPLE_FIELDS = frozenset({"status", "sample_count", "value"})
_CORE_POINTER_FIELDS = frozenset({
    "schema", "bundle_stage", "route_cohort_id", "manifest_sha256"
})
_RUN_FIELDS = frozenset({
    "run_id",
    "phase_state_sha256",
    "phase_transition_id",
    "route_universe_sha256",
    "baseline_manifest_sha256",
    "candidate_source_generation",
    "route_cost_evidence_sha256",
})

_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_COHORT_ID = re.compile(r"cohort:[0-9a-f]{64}\Z", flags=re.ASCII)
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", flags=re.ASCII)
_AUDIT_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z",
    flags=re.ASCII,
)
_PLAIN_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?\Z", flags=re.ASCII)
_SOURCE_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z", flags=re.ASCII)


class RouteShadowAuditError(ValueError):
    """Raised when audit inputs or persisted audit bytes are inconsistent."""


def _canonical_json_clone(value: Any) -> Any:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return json.loads(encoded)
    except (TypeError, ValueError) as error:
        raise RouteShadowAuditError("Shadow audit contains invalid JSON data") from error


def _physical_pointer_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8") + b"\n"
    except (TypeError, ValueError) as error:
        raise RouteShadowAuditError("core pointer contains invalid JSON data") from error


def _require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or _HEX_SHA256.fullmatch(value) is None:
        raise RouteShadowAuditError("{} is invalid".format(label))
    return value


def _require_run_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or _RUN_ID.fullmatch(value) is None
        or value in {".", ".."}
    ):
        raise RouteShadowAuditError("run ID is invalid")
    return value


def _require_generation(value: Any) -> str:
    if not isinstance(value, str) or _HEX_SHA256.fullmatch(value) is None:
        raise RouteShadowAuditError("candidate source generation is invalid")
    return value


def _require_audit_timestamp(value: Any) -> str:
    if not isinstance(value, str) or _AUDIT_TIMESTAMP.fullmatch(value) is None:
        raise RouteShadowAuditError("audit_finished_at is invalid")
    try:
        exact_rfc3339_epoch_seconds(value)
    except (TypeError, ValueError) as error:
        raise RouteShadowAuditError("audit_finished_at is invalid") from error
    return value


def _decimal_text(value: Decimal) -> str:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise RouteShadowAuditError("metric value is invalid")
    if value.is_zero():
        if value.is_signed():
            raise RouteShadowAuditError("metric value is negative zero")
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if _PLAIN_DECIMAL.fullmatch(text) is None:
        raise RouteShadowAuditError("metric value is not canonical")
    return text


def _parse_decimal_text(value: Any, label: str) -> Decimal:
    if not isinstance(value, str) or _PLAIN_DECIMAL.fullmatch(value) is None:
        raise RouteShadowAuditError("{} is invalid".format(label))
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise RouteShadowAuditError("{} is invalid".format(label)) from error
    if not parsed.is_finite() or parsed < 0 or (parsed.is_zero() and parsed.is_signed()):
        raise RouteShadowAuditError("{} is invalid".format(label))
    if _decimal_text(parsed) != value:
        raise RouteShadowAuditError("{} is not canonical".format(label))
    return parsed


def _parse_source_decimal(value: Any, label: str) -> Decimal:
    if not isinstance(value, str) or _SOURCE_DECIMAL.fullmatch(value) is None:
        raise RouteShadowAuditError("{} is invalid".format(label))
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise RouteShadowAuditError("{} is invalid".format(label)) from error
    if not parsed.is_finite() or parsed < 0 or (parsed.is_zero() and parsed.is_signed()):
        raise RouteShadowAuditError("{} is invalid".format(label))
    return parsed


def _nearest_rank_index(percentile: Decimal, count: int) -> int:
    sign, digits, exponent = percentile.as_tuple()
    if sign:
        raise RouteShadowAuditError("percentile is invalid")
    coefficient = int("".join(str(digit) for digit in digits) or "0")
    if exponent >= 0:
        # Validation permits no positive-exponent value except exact one.
        return count - 1
    scale = -exponent
    scaled_numerator = coefficient * count
    # Any positive integer with at most ``scale`` decimal digits is strictly
    # smaller than 10**scale.  This proves ceil(product / denominator) == 1
    # without materializing an attacker-sized power such as 10**999999999.
    if len(str(scaled_numerator)) <= scale:
        return 0
    denominator = 10 ** scale
    rank = (scaled_numerator + denominator - 1) // denominator
    return rank - 1


def nearest_rank(
    values: Iterable[Decimal], percentile: Decimal
) -> Optional[str]:
    """Return a canonical nearest-rank percentile, or ``None`` when empty."""
    if (
        not isinstance(percentile, Decimal)
        or not percentile.is_finite()
        or percentile <= 0
        or percentile > 1
        or (percentile.is_zero() and percentile.is_signed())
    ):
        raise RouteShadowAuditError("percentile is invalid")
    try:
        samples = list(values)
    except TypeError as error:
        raise RouteShadowAuditError("percentile samples are invalid") from error
    for value in samples:
        _decimal_text(value)
    if not samples:
        return None
    samples.sort()
    return _decimal_text(samples[_nearest_rank_index(percentile, len(samples))])


def _ratio_value(numerator: int, denominator: int) -> str:
    precision = max(len(str(numerator)), len(str(denominator))) + 32
    arithmetic = Context(prec=precision, rounding=ROUND_HALF_EVEN)
    arithmetic.traps[Inexact] = False
    arithmetic.traps[Rounded] = False
    with localcontext(arithmetic):
        value = (Decimal(numerator) / Decimal(denominator)).quantize(
            Decimal("0.000000000001"),
            rounding=ROUND_HALF_EVEN,
        )
    return _decimal_text(value)


def _exact_decimal_subtract(left: Decimal, right: Decimal) -> Decimal:
    lowest_exponent = min(left.as_tuple().exponent, right.as_tuple().exponent)
    highest_adjusted = max(left.adjusted(), right.adjusted())
    precision = max(1, highest_adjusted - lowest_exponent + 2)
    arithmetic = Context(prec=precision)
    arithmetic.traps[Inexact] = True
    arithmetic.traps[Rounded] = True
    with localcontext(arithmetic):
        return left - right


def _ratio_metric(numerator: int, denominator: int) -> Dict[str, Any]:
    if (
        type(numerator) is not int
        or type(denominator) is not int
        or numerator < 0
        or denominator < 0
        or numerator > denominator
    ):
        raise RouteShadowAuditError("ratio counts are invalid")
    if denominator == 0:
        if numerator != 0:
            raise RouteShadowAuditError("ratio counts are invalid")
        return {
            "status": "not_evaluated",
            "numerator": 0,
            "denominator": 0,
            "value": None,
        }
    return {
        "status": "evaluated",
        "numerator": numerator,
        "denominator": denominator,
        "value": _ratio_value(numerator, denominator),
    }


def _sample_metric(values: List[Decimal], *, maximum: bool) -> Dict[str, Any]:
    if not values:
        return {"status": "not_evaluated", "sample_count": 0, "value": None}
    value = _decimal_text(max(values)) if maximum else nearest_rank(
        values, Decimal("0.95")
    )
    return {"status": "evaluated", "sample_count": len(values), "value": value}


def _validate_ratio_metric(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _RATIO_FIELDS:
        raise RouteShadowAuditError("{} schema is invalid".format(label))
    numerator = value.get("numerator")
    denominator = value.get("denominator")
    expected = _ratio_metric(numerator, denominator)
    if dict(value) != expected:
        raise RouteShadowAuditError("{} is inconsistent".format(label))
    return expected


def _validate_sample_metric(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _SAMPLE_FIELDS:
        raise RouteShadowAuditError("{} schema is invalid".format(label))
    sample_count = value.get("sample_count")
    if type(sample_count) is not int or sample_count < 0:
        raise RouteShadowAuditError("{} sample count is invalid".format(label))
    status = value.get("status")
    metric_value = value.get("value")
    if sample_count == 0:
        if status != "not_evaluated" or metric_value is not None:
            raise RouteShadowAuditError("{} is inconsistent".format(label))
    else:
        if status != "evaluated":
            raise RouteShadowAuditError("{} is inconsistent".format(label))
        _parse_decimal_text(metric_value, "{} value".format(label))
    return dict(value)


def _validate_metrics(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != METRIC_FIELDS:
        raise RouteShadowAuditError("audit metrics schema is invalid")
    metrics = {
        name: _validate_ratio_metric(value[name], name)
        for name in (
            "leg_availability",
            "timing_availability",
            "conditional_skew_sla",
        )
    }
    for name in (
        "passing_skew_seconds_p95",
        "passing_skew_seconds_max",
        "route_age_seconds_p95",
        "route_age_seconds_max",
    ):
        metrics[name] = _validate_sample_metric(value[name], name)

    if (
        metrics["timing_availability"]["numerator"]
        != metrics["conditional_skew_sla"]["denominator"]
    ):
        raise RouteShadowAuditError("timing and conditional counts disagree")
    passing_count = metrics["conditional_skew_sla"]["numerator"]
    if any(
        metrics[name]["sample_count"] != passing_count
        for name in ("passing_skew_seconds_p95", "passing_skew_seconds_max")
    ):
        raise RouteShadowAuditError("passing-skew sample counts disagree")
    if (
        metrics["route_age_seconds_p95"]["sample_count"]
        != metrics["route_age_seconds_max"]["sample_count"]
    ):
        raise RouteShadowAuditError("route-age sample counts disagree")
    for prefix in ("passing_skew_seconds", "route_age_seconds"):
        p95 = metrics[prefix + "_p95"]
        maximum = metrics[prefix + "_max"]
        if p95["sample_count"] and _parse_decimal_text(
            p95["value"], prefix + " p95"
        ) > _parse_decimal_text(maximum["value"], prefix + " max"):
            raise RouteShadowAuditError("{} percentile exceeds maximum".format(prefix))
    return metrics


def validate_shadow_audit(audit: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate and return a canonical JSON copy of an exact v1 audit."""
    if not isinstance(audit, Mapping):
        raise RouteShadowAuditError("Shadow audit must be a mapping")
    cloned = _canonical_json_clone(audit)
    if not isinstance(cloned, dict) or set(cloned) != AUDIT_FIELDS:
        raise RouteShadowAuditError("Shadow audit schema is invalid")
    if cloned.get("schema") != ROUTE_SHADOW_AUDIT_SCHEMA:
        raise RouteShadowAuditError("Shadow audit schema is unsupported")
    _require_run_id(cloned.get("run_id"))
    phase = cloned.get("phase")
    if phase not in {"canary", "full"}:
        raise RouteShadowAuditError("Shadow audit phase is invalid")
    cohort_id = cloned.get("route_cohort_id")
    if not isinstance(cohort_id, str) or _COHORT_ID.fullmatch(cohort_id) is None:
        raise RouteShadowAuditError("Shadow audit cohort ID is invalid")
    for field in (
        "phase_state_sha256",
        "core_pointer_sha256",
        "core_manifest_sha256",
        "route_cost_evidence_sha256",
        "route_universe_sha256",
        "baseline_manifest_sha256",
    ):
        _require_sha256(cloned.get(field), field)
    transition_id = cloned.get("phase_transition_id")
    if phase == "canary":
        if (
            transition_id is not None
            or cloned["phase_state_sha256"] != IMPLICIT_CANARY_PHASE_SHA256
        ):
            raise RouteShadowAuditError("implicit canary phase lineage is invalid")
    elif transition_id is None:
        raise RouteShadowAuditError("full phase transition is missing")
    else:
        _require_sha256(transition_id, "phase_transition_id")
    _require_generation(cloned.get("candidate_source_generation"))
    _require_audit_timestamp(cloned.get("audit_finished_at"))
    cloned["metrics"] = _validate_metrics(cloned.get("metrics"))
    return cloned


def _validate_core_pointer(
    pointer: Mapping[str, Any], route_cohort_id: str
) -> Dict[str, Any]:
    if not isinstance(pointer, Mapping) or set(pointer) != _CORE_POINTER_FIELDS:
        raise RouteShadowAuditError("core pointer schema is invalid")
    cloned = _canonical_json_clone(pointer)
    if (
        cloned.get("schema") != "route_cohort_core_pointer/v1"
        or cloned.get("bundle_stage") != "route_cohort_core/v1"
        or cloned.get("route_cohort_id") != route_cohort_id
    ):
        raise RouteShadowAuditError("core pointer lineage is invalid")
    _require_sha256(cloned.get("manifest_sha256"), "core manifest hash")
    return cloned


def _validate_run(run: Mapping[str, Any], phase: str) -> Dict[str, Any]:
    if not isinstance(run, Mapping) or set(run) != _RUN_FIELDS:
        raise RouteShadowAuditError("Shadow run schema is invalid")
    cloned = _canonical_json_clone(run)
    _require_run_id(cloned.get("run_id"))
    for field in (
        "phase_state_sha256",
        "route_universe_sha256",
        "baseline_manifest_sha256",
        "route_cost_evidence_sha256",
    ):
        _require_sha256(cloned.get(field), field)
    _require_generation(cloned.get("candidate_source_generation"))
    transition_id = cloned.get("phase_transition_id")
    if phase == "canary":
        if (
            transition_id is not None
            or cloned["phase_state_sha256"] != IMPLICIT_CANARY_PHASE_SHA256
        ):
            raise RouteShadowAuditError("implicit canary run lineage is invalid")
    elif phase == "full":
        if transition_id is None:
            raise RouteShadowAuditError("full run transition is missing")
        _require_sha256(transition_id, "phase_transition_id")
    else:
        raise RouteShadowAuditError("Shadow phase is invalid")
    return cloned


def _leg_is_available(leg: Mapping[str, Any]) -> bool:
    return leg.get("status") in {"observed", "partial"} and leg.get("available") is not False


def build_shadow_audit(
    cohort: Mapping[str, Any],
    *,
    core_pointer: Mapping[str, Any],
    run: Mapping[str, Any],
    phase: str,
    audit_finished_at: str
) -> Dict[str, Any]:
    """Build one strict prepublication audit from immutable cohort facts."""
    try:
        normalized = _normalize_and_validate_cohort(cohort)
    except (TypeError, ValueError) as error:
        raise RouteShadowAuditError(str(error)) from error
    pointer = _validate_core_pointer(core_pointer, normalized["route_cohort_id"])
    run_view = _validate_run(run, phase)
    finished_at = _require_audit_timestamp(audit_finished_at)
    finished_epoch = exact_rfc3339_epoch_seconds(finished_at)
    completed_epoch = exact_rfc3339_epoch_seconds(
        normalized["collection_completed_at"]
    )
    if finished_epoch < completed_epoch:
        raise RouteShadowAuditError("audit precedes cohort completion")
    if (
        run_view["candidate_source_generation"]
        != normalized["candidate_source_generation"]
    ):
        raise RouteShadowAuditError("candidate source lineage conflict")

    legs = normalized["legs"]
    leg_by_market = {leg["market_id"]: leg for leg in legs}
    available_leg_count = sum(1 for leg in legs if _leg_is_available(leg))

    route_rows = normalized["route_rows"]
    timing_available_count = sum(
        1 for row in route_rows
        if row["timing_status"] in {"within_sla", "outside_sla"}
    )
    passing_rows = [
        row for row in route_rows if row["timing_status"] == "within_sla"
    ]
    passing_skews = [
        _parse_source_decimal(row.get("skew_seconds"), "route skew")
        for row in passing_rows
    ]

    route_ages: List[Decimal] = []
    for route in normalized["routes"]:
        try:
            buy_leg = leg_by_market[route["buy_market_id"]]
            sell_leg = leg_by_market[route["sell_market_id"]]
        except KeyError as error:
            raise RouteShadowAuditError("route leg lineage is incomplete") from error
        if not (_leg_is_available(buy_leg) and _leg_is_available(sell_leg)):
            continue
        try:
            buy_epoch = exact_rfc3339_epoch_seconds(buy_leg["state_observed_at"])
            sell_epoch = exact_rfc3339_epoch_seconds(sell_leg["state_observed_at"])
        except (KeyError, TypeError, ValueError) as error:
            raise RouteShadowAuditError("route state lineage is invalid") from error
        age = _exact_decimal_subtract(
            finished_epoch, min(buy_epoch, sell_epoch)
        )
        if age < 0:
            raise RouteShadowAuditError("route age is negative")
        route_ages.append(age)

    metrics = {
        "leg_availability": _ratio_metric(available_leg_count, len(legs)),
        "timing_availability": _ratio_metric(
            timing_available_count, len(route_rows)
        ),
        "conditional_skew_sla": _ratio_metric(
            len(passing_rows), timing_available_count
        ),
        "passing_skew_seconds_p95": _sample_metric(
            passing_skews, maximum=False
        ),
        "passing_skew_seconds_max": _sample_metric(
            passing_skews, maximum=True
        ),
        "route_age_seconds_p95": _sample_metric(route_ages, maximum=False),
        "route_age_seconds_max": _sample_metric(route_ages, maximum=True),
    }
    audit = {
        "schema": ROUTE_SHADOW_AUDIT_SCHEMA,
        "run_id": run_view["run_id"],
        "phase": phase,
        "route_cohort_id": normalized["route_cohort_id"],
        "phase_state_sha256": run_view["phase_state_sha256"],
        "phase_transition_id": run_view["phase_transition_id"],
        "core_pointer_sha256": hashlib.sha256(
            _physical_pointer_bytes(pointer)
        ).hexdigest(),
        "core_manifest_sha256": pointer["manifest_sha256"],
        "route_cost_evidence_sha256": run_view["route_cost_evidence_sha256"],
        "route_universe_sha256": run_view["route_universe_sha256"],
        "baseline_manifest_sha256": run_view["baseline_manifest_sha256"],
        "candidate_source_generation": run_view["candidate_source_generation"],
        "audit_finished_at": finished_at,
        "metrics": metrics,
    }
    return validate_shadow_audit(audit)
