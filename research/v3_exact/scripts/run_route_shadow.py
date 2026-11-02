"""Non-blocking route Shadow orchestrator and immutable run ledger."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

try:
    from scripts.collection_lock_evidence import (
        clear_shadow_lock_owner,
        open_verified_directory_chain,
        open_verified_regular_at,
        verify_directory_chain,
        write_shadow_lock_owner,
    )
    from scripts.route_shadow_authority import load_committed_route_shadow_authority
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from collection_lock_evidence import (  # type: ignore
        clear_shadow_lock_owner,
        open_verified_directory_chain,
        open_verified_regular_at,
        verify_directory_chain,
        write_shadow_lock_owner,
    )
    from route_shadow_authority import (  # type: ignore
        load_committed_route_shadow_authority,
    )


RUN_STARTED_SCHEMA = "route_shadow_run_started/v1"
RUN_VERIFICATION_SCHEMA = "route_shadow_run_verification/v1"
RUN_TERMINAL_SCHEMA = "route_shadow_run_terminal/v1"
SERVICE_SCHEMA = "route_shadow_service/v1"

STARTED_FIELDS = frozenset({
    "schema", "run_id", "dispatch_id", "phase", "phase_state_sha256",
    "phase_transition_id", "invocation_id", "started_at", "boot_id",
    "monotonic_ns",
})
VERIFICATION_FIELDS = frozenset({
    "schema", "run_id", "dispatch_id", "started_sha256", "verified_at",
    "primary_failure_class", "collector_process_started_count",
    "collector_process_reaped_count", "orphan_process_count",
    "primary_publication_interference_count", "core_orphan_count",
    "pointer_interference_count", "lineage_error_count",
    "unsafe_path_error_count", "source_generation_error_count",
    "resource_limit_error_count", "runtime_limit_error_count",
    "last_completed_stage", "result_status", "typed_source_manifest_sha256",
    "route_cost_evidence_sha256", "run_capture_admission_sha256",
    "run_admission_sha256", "storage_admission_status", "reason_codes",
})
TERMINAL_FIELDS = frozenset({
    "schema", "run_id", "dispatch_id", "outcome", "finished_at",
    "lock_acquired", "duration_status", "duration_seconds",
    "route_cohort_id", "started_sha256", "verification_sha256",
    "runtime_evidence_sha256", "run_capture_admission_sha256",
    "run_admission_sha256", "storage_admission_status",
    "typed_source_manifest_sha256", "route_cost_evidence_sha256",
    "joint_pointer_sha256", "reason_code",
})
SERVICE_FIELDS = frozenset({
    "schema", "service_kind", "dispatch_id", "run_id", "attempt_id",
    "unit_name", "invocation_id", "terminal_sha256",
    "runtime_evidence_sha256", "service_result", "exit_code", "exit_status",
    "normalized_outcome", "started_at", "finished_at", "reason_code",
})
LEDGER_MEMBERS = frozenset({
    "started.json", "verification.json", "terminal.json", "service.json",
    "runtime.json", "candidate.json", "candidate-primary-guard.json",
    "candidate-schedule-envelope.json", "candidate-commit.json",
})
RESERVED_CANDIDATE_MEMBERS = frozenset({
    "candidate.json", "candidate-primary-guard.json",
    "candidate-schedule-envelope.json", "candidate-commit.json",
})
_LIMITS = {
    "started.json": 4096,
    "verification.json": 8192,
    "terminal.json": 4096,
    "service.json": 4096,
}
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", re.ASCII)
_HEX32 = re.compile(r"[0-9a-f]{32}\Z", re.ASCII)
_HEX64 = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_UTC = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)\Z",
    re.ASCII,
)
_TOKEN = re.compile(r"[A-Za-z0-9._:+-]{1,128}\Z", re.ASCII)
_STAGES = frozenset({
    "none", "input_capture", "universe", "collection", "core", "audit",
    "joint_pointer",
})
_FAILURE_CLASSES = frozenset({
    "none", "transient_collection", "source_generation_drift",
    "lineage_invalid", "unsafe_path", "resource_limit", "oom", "timeout",
    "orphan_core", "pointer_interference", "runtime_limits_unverified",
    "unexplained",
})
_REASONS = frozenset({
    "storage_not_evaluated", "input_capture_unavailable",
    "collection_lock_busy", "pre_started_lock_state_unknown",
    "service_evidence_gap", "service_success_without_joint_pointer",
    "source_generation_drift", "lineage_invalid", "unsafe_path",
    "resource_limit", "runtime_limits_unverified", "pointer_interference",
    "orphan_core", "oom", "timeout", "transient_collection", "unexplained",
})
_CANARY_TOKEN_ALLOWLIST = frozenset({
    "PEPE", "CAKE", "SHIB", "SUSHI", "ZK", "SNX", "GRT", "COMP",
    "ENS", "STRK",
})


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _route_cost_registry_snapshot() -> Dict[str, Any]:
    """Capture the two fixed production registries before any source read."""
    try:
        from scripts.route_cost_evidence import (
            load_route_cost_adapter_registry,
            load_route_cost_connector_key_registry,
        )
    except ModuleNotFoundError:  # pragma: no cover - direct script execution
        from route_cost_evidence import (  # type: ignore
            load_route_cost_adapter_registry,
            load_route_cost_connector_key_registry,
        )
    adapter_registry = load_route_cost_adapter_registry()
    connector_key_registry = load_route_cost_connector_key_registry()
    return {
        "adapter_registry": adapter_registry,
        "adapter_registry_sha256": _sha256(_canonical_bytes(adapter_registry)),
        "connector_key_registry": connector_key_registry,
        "connector_key_registry_sha256": _sha256(
            _canonical_bytes(connector_key_registry)
        ),
    }


def _trusted_utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("route Shadow clock must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_from_monotonic(
    started_at: datetime, start_ns: int, current_ns: int
) -> datetime:
    """Project one monotonic sample onto the run's sole trusted UTC anchor."""
    if (
        started_at.tzinfo is None
        or started_at.utcoffset() is None
        or type(start_ns) is not int
        or type(current_ns) is not int
        or start_ns < 0
        or current_ns < start_ns
    ):
        raise ValueError("route Shadow monotonic UTC projection is invalid")
    seconds, nanoseconds = divmod(current_ns - start_ns, 1_000_000_000)
    try:
        return started_at.astimezone(timezone.utc) + timedelta(
            seconds=seconds, microseconds=nanoseconds // 1_000
        )
    except (OverflowError, ValueError) as error:
        raise ValueError(
            "route Shadow monotonic UTC projection is out of range"
        ) from error


def _utc_whole_second_ceiling(value: datetime) -> datetime:
    """Return the earliest whole UTC second that is not before ``value``."""
    normalized = _utc_from_monotonic(value, 0, 0)
    if normalized.microsecond == 0:
        return normalized
    try:
        return normalized.replace(microsecond=0) + timedelta(seconds=1)
    except (OverflowError, ValueError) as error:
        raise ValueError("route Shadow audit timestamp is out of range") from error


class _RunTimeline:
    """Derive later run timestamps without a second wall-clock read."""

    def __init__(self, started_at: datetime, start_ns: int) -> None:
        self._started_at = _utc_from_monotonic(started_at, start_ns, start_ns)
        self._start_ns = start_ns
        self.last_utc = self._started_at
        self.last_ns = start_ns

    def sample(self) -> Tuple[datetime, int]:
        current_ns = time.monotonic_ns()
        projected = _utc_from_monotonic(
            self._started_at, self._start_ns, current_ns
        )
        self.last_utc = projected
        self.last_ns = current_ns
        return projected, current_ns

    def wall_clock(self) -> datetime:
        return self.sample()[0]


def _boot_id() -> str:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="ascii"
        ).strip().replace("-", "").lower()
    except OSError:
        value = uuid.uuid4().hex
    if _HEX32.fullmatch(value) is None:
        raise ValueError("boot ID is invalid")
    return value


def _validate_run_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or _RUN_ID.fullmatch(value) is None
        or value in {".", ".."}
    ):
        raise ValueError("route Shadow run ID is invalid")
    return value


def _new_manual_run_id() -> str:
    return "manual-{}".format(uuid.uuid4().hex)


def _timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str) or _UTC.fullmatch(value) is None:
        raise ValueError("{} is invalid".format(field))
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("{} is invalid".format(field)) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("{} is invalid".format(field))
    return value


def _optional_sha(value: Any, field: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise ValueError("{} is invalid".format(field))
    return value


def _hex32(value: Any, field: str) -> str:
    if not isinstance(value, str) or _HEX32.fullmatch(value) is None:
        raise ValueError("{} is invalid".format(field))
    return value


def _optional_hex32(value: Any, field: str) -> Optional[str]:
    if value is None:
        return None
    return _hex32(value, field)


def _cohort_id(value: Any, *, optional: bool = True) -> Optional[str]:
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or re.fullmatch(r"cohort:[0-9a-f]{64}", value, re.ASCII) is None
    ):
        raise ValueError("route cohort ID is invalid")
    return value


def validate_started(value: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != STARTED_FIELDS or value.get("schema") != RUN_STARTED_SCHEMA:
        raise ValueError("route Shadow started schema is invalid")
    result = dict(value)
    run_id = _validate_run_id(result.get("run_id"))
    dispatch = result.get("dispatch_id")
    invocation = result.get("invocation_id")
    if dispatch is None:
        if invocation is not None:
            raise ValueError("manual invocation ID must be null")
    elif (
        not isinstance(dispatch, str)
        or _HEX32.fullmatch(dispatch) is None
        or not isinstance(invocation, str)
        or _HEX32.fullmatch(invocation) is None
        or invocation != run_id
    ):
        raise ValueError("scheduled run identity is invalid")
    if result.get("phase") not in {"canary", "full"}:
        raise ValueError("route Shadow phase is invalid")
    if _optional_sha(result.get("phase_state_sha256"), "phase state SHA") is None:
        raise ValueError("phase state SHA is missing")
    transition = result.get("phase_transition_id")
    if result["phase"] == "canary" and transition is not None:
        raise ValueError("canary transition must be null")
    if result["phase"] == "full" and _optional_sha(transition, "transition ID") is None:
        raise ValueError("full transition is missing")
    _timestamp(result.get("started_at"), "started_at")
    _hex32(result.get("boot_id"), "boot ID")
    if type(result.get("monotonic_ns")) is not int or result["monotonic_ns"] < 0:
        raise ValueError("monotonic clock is invalid")
    return result


def validate_verification(value: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != VERIFICATION_FIELDS or value.get("schema") != RUN_VERIFICATION_SCHEMA:
        raise ValueError("route Shadow verification schema is invalid")
    result = dict(value)
    _validate_run_id(result.get("run_id"))
    dispatch = result.get("dispatch_id")
    _optional_hex32(dispatch, "verification dispatch ID")
    if _optional_sha(result.get("started_sha256"), "started SHA") is None:
        raise ValueError("verification started SHA is missing")
    _timestamp(result.get("verified_at"), "verified_at")
    if result.get("primary_failure_class") not in _FAILURE_CLASSES:
        raise ValueError("primary failure class is invalid")
    count_fields = (
        "collector_process_started_count", "collector_process_reaped_count",
        "orphan_process_count", "primary_publication_interference_count",
        "core_orphan_count", "pointer_interference_count",
        "lineage_error_count", "unsafe_path_error_count",
        "source_generation_error_count", "resource_limit_error_count",
        "runtime_limit_error_count",
    )
    for field in count_fields:
        if type(result.get(field)) is not int or not 0 <= result[field] <= 1_000_000:
            raise ValueError("verification count is invalid")
    if result["collector_process_started_count"] != result["collector_process_reaped_count"] + result["orphan_process_count"]:
        raise ValueError("collector process counts are inconsistent")
    if result.get("last_completed_stage") not in _STAGES:
        raise ValueError("verification stage is invalid")
    if result.get("result_status") not in {"verified", "failed", "not_evaluated"}:
        raise ValueError("verification result is invalid")
    for field in (
        "typed_source_manifest_sha256", "route_cost_evidence_sha256",
        "run_capture_admission_sha256", "run_admission_sha256",
    ):
        _optional_sha(result.get(field), field)
    if result.get("storage_admission_status") not in {"verified", "not_evaluated"}:
        raise ValueError("storage admission status is invalid")
    reasons = result.get("reason_codes")
    if not isinstance(reasons, list) or reasons != sorted(set(reasons)) or any(reason not in _REASONS for reason in reasons):
        raise ValueError("verification reason codes are invalid")
    failure_class = result["primary_failure_class"]
    stage = result["last_completed_stage"]
    status = result["result_status"]
    typed_sha = result["typed_source_manifest_sha256"]
    cost_sha = result["route_cost_evidence_sha256"]
    capture_sha = result["run_capture_admission_sha256"]
    admission_sha = result["run_admission_sha256"]
    storage_status = result["storage_admission_status"]
    if storage_status == "not_evaluated":
        if capture_sha is not None or admission_sha is not None or not (
            {"storage_not_evaluated", "input_capture_unavailable"} & set(reasons)
        ):
            raise ValueError("unevaluated storage verification is inconsistent")
    elif capture_sha is None or admission_sha is None:
        raise ValueError("verified storage evidence is incomplete")
    if stage in {"none", "input_capture", "universe"} and (
        typed_sha is not None or cost_sha is not None
    ):
        raise ValueError("verification evidence precedes its completed stage")
    if stage in {"core", "audit", "joint_pointer"} and (
        typed_sha is None or cost_sha is None
    ):
        raise ValueError("verification evidence is incomplete for its stage")
    if failure_class == "none":
        if (
            status != "verified"
            or stage != "joint_pointer"
            or any(result[field] != 0 for field in count_fields[2:])
            or typed_sha is None
            or cost_sha is None
            or storage_status != "verified"
            or reasons
        ):
            raise ValueError("successful verification facts are inconsistent")
    elif status == "verified":
        raise ValueError("failed verification cannot be verified")
    class_evidence = {
        "source_generation_drift": result["source_generation_error_count"] > 0,
        "lineage_invalid": result["lineage_error_count"] > 0,
        "unsafe_path": result["unsafe_path_error_count"] > 0,
        "resource_limit": result["resource_limit_error_count"] > 0,
        "runtime_limits_unverified": result["runtime_limit_error_count"] > 0,
        "pointer_interference": result["pointer_interference_count"] > 0,
        "orphan_core": result["core_orphan_count"] > 0,
        "oom": "oom" in reasons,
        "timeout": "timeout" in reasons,
        "transient_collection": "transient_collection" in reasons,
        "unexplained": bool(set(reasons) & {"unexplained", "storage_not_evaluated", "input_capture_unavailable"}),
    }
    if failure_class != "none" and not class_evidence.get(failure_class, False):
        raise ValueError("primary failure class has no matching evidence")
    return result


def validate_terminal(value: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != TERMINAL_FIELDS or value.get("schema") != RUN_TERMINAL_SCHEMA:
        raise ValueError("route Shadow terminal schema is invalid")
    result = dict(value)
    _validate_run_id(result.get("run_id"))
    dispatch = result.get("dispatch_id")
    _optional_hex32(dispatch, "terminal dispatch ID")
    if result.get("outcome") not in {"success", "failed", "timeout", "oom", "unexplained", "skipped_locked"}:
        raise ValueError("terminal outcome is invalid")
    _timestamp(result.get("finished_at"), "finished_at")
    if result.get("lock_acquired") not in {True, False, None}:
        raise ValueError("terminal lock state is invalid")
    if result.get("duration_status") not in {"evaluated", "not_evaluated"}:
        raise ValueError("duration status is invalid")
    duration = result.get("duration_seconds")
    if result["duration_status"] == "evaluated":
        if not isinstance(duration, str) or re.fullmatch(r"(?:0|[1-9]\d*)(?:\.\d+)?", duration) is None:
            raise ValueError("duration is invalid")
    elif duration is not None:
        raise ValueError("unevaluated duration must be null")
    for field in (
        "started_sha256", "verification_sha256", "runtime_evidence_sha256",
        "run_capture_admission_sha256", "run_admission_sha256",
        "typed_source_manifest_sha256", "route_cost_evidence_sha256",
        "joint_pointer_sha256",
    ):
        _optional_sha(result.get(field), field)
    _cohort_id(result.get("route_cohort_id"))
    if result.get("storage_admission_status") not in {"verified", "not_evaluated"}:
        raise ValueError("terminal storage status is invalid")
    if result.get("reason_code") not in _REASONS | {None}:
        raise ValueError("terminal reason is invalid")
    lock_state = result["lock_acquired"]
    if lock_state is False:
        empty_fields = (
            "verification_sha256", "runtime_evidence_sha256", "route_cohort_id",
            "joint_pointer_sha256", "typed_source_manifest_sha256",
            "route_cost_evidence_sha256", "run_capture_admission_sha256",
            "run_admission_sha256",
        )
        if result["outcome"] != "skipped_locked" or result["duration_status"] != "not_evaluated" or any(result[field] is not None for field in empty_fields) or result["storage_admission_status"] != "not_evaluated":
            raise ValueError("busy terminal matrix is invalid")
    elif lock_state is None:
        if dispatch is None or result["outcome"] != "unexplained" or result["started_sha256"] is not None or result["verification_sha256"] is not None or result["duration_status"] != "not_evaluated" or result["reason_code"] != "pre_started_lock_state_unknown" or any(result[field] is not None for field in ("runtime_evidence_sha256", "route_cohort_id", "run_capture_admission_sha256", "run_admission_sha256", "typed_source_manifest_sha256", "route_cost_evidence_sha256", "joint_pointer_sha256")) or result["storage_admission_status"] != "not_evaluated":
            raise ValueError("synthetic terminal matrix is invalid")
    else:
        if result["started_sha256"] is None or result["verification_sha256"] is None:
            raise ValueError("acquired terminal evidence is missing")
        if result["outcome"] == "success":
            if (
                result["duration_status"] != "evaluated"
                or result["duration_seconds"] is None
                or result["route_cohort_id"] is None
                or result["typed_source_manifest_sha256"] is None
                or result["route_cost_evidence_sha256"] is None
                or result["joint_pointer_sha256"] is None
                or result["run_capture_admission_sha256"] is None
                or result["run_admission_sha256"] is None
                or result["storage_admission_status"] != "verified"
                or result["reason_code"] is not None
            ):
                raise ValueError("success terminal evidence is incomplete")
        elif result["reason_code"] is None:
            raise ValueError("failed acquired terminal reason is missing")
        if result["outcome"] == "timeout" and result["reason_code"] != "timeout":
            raise ValueError("timeout terminal reason is invalid")
        if result["outcome"] == "oom" and result["reason_code"] != "oom":
            raise ValueError("OOM terminal reason is invalid")
        if result["storage_admission_status"] == "not_evaluated" and (
            result["run_capture_admission_sha256"] is not None
            or result["run_admission_sha256"] is not None
            or result["reason_code"] not in {
                "storage_not_evaluated", "input_capture_unavailable",
                # A cross-process reconciler cannot compare the worker's
                # monotonic clock, but its exact systemd outcome is still the
                # terminal's primary reason. Storage remains independently
                # and explicitly not evaluated in the repeated status/SHA
                # fields and in verification.reason_codes.
                "timeout", "oom",
            }
        ):
            raise ValueError("terminal storage matrix is invalid")
    return result


def normalize_systemd_result(service_result: str, exit_code: str, exit_status: str) -> str:
    if (service_result, exit_code, exit_status) == ("success", "exited", "0"):
        return "success"
    if service_result == "timeout":
        return "timeout"
    if service_result == "oom-kill":
        return "oom"
    if service_result in {"exit-code", "signal", "core-dump", "watchdog", "resources", "protocol", "start-limit-hit"}:
        return "failed"
    return "unexplained"


def validate_service(value: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != SERVICE_FIELDS or value.get("schema") != SERVICE_SCHEMA or value.get("service_kind") not in {"dispatcher", "worker", "ops"} or value.get("normalized_outcome") not in {"success", "timeout", "oom", "failed", "unexplained"}:
        raise ValueError("route Shadow service schema is invalid")
    result = dict(value)
    dispatch = _hex32(result.get("dispatch_id"), "service dispatch ID")
    attempt = _hex32(result.get("attempt_id"), "service attempt ID")
    invocation = _hex32(result.get("invocation_id"), "service invocation ID")
    if _optional_sha(result.get("terminal_sha256"), "service terminal SHA") is None:
        raise ValueError("service terminal SHA is missing")
    _optional_sha(result.get("runtime_evidence_sha256"), "runtime SHA")
    started = _timestamp(result.get("started_at"), "service started_at")
    finished = _timestamp(result.get("finished_at"), "service finished_at")
    if datetime.fromisoformat(finished.replace("Z", "+00:00")) < datetime.fromisoformat(started.replace("Z", "+00:00")):
        raise ValueError("service timestamps are reversed")
    kind = result["service_kind"]
    run_id = result.get("run_id")
    if kind == "worker":
        if not isinstance(run_id, str) or _HEX32.fullmatch(run_id) is None or attempt != run_id or invocation != run_id or result.get("unit_name") != "cex-dex-route-shadow-worker@{}.service".format(run_id):
            raise ValueError("worker service identity is invalid")
    elif kind == "dispatcher":
        if run_id is not None or attempt != dispatch or invocation != dispatch or result.get("unit_name") != "cex-dex-route-shadow-dispatch-user.service":
            raise ValueError("dispatcher service identity is invalid")
    elif run_id is not None or result.get("unit_name") != "cex-dex-route-shadow-ops@{}.service".format(dispatch):
        raise ValueError("ops service identity is invalid")
    if result.get("exit_code") not in {"exited", "killed", "dumped", None}:
        raise ValueError("service exit code is invalid")
    if result.get("exit_status") is not None and (not isinstance(result["exit_status"], str) or _TOKEN.fullmatch(result["exit_status"]) is None):
        raise ValueError("service exit status is invalid")
    expected_normalized = normalize_systemd_result(
        result.get("service_result"), result.get("exit_code"),
        result.get("exit_status"),
    )
    if result["normalized_outcome"] != expected_normalized:
        raise ValueError("service normalization is inconsistent")
    reason = result.get("reason_code")
    if reason not in _REASONS | {None}:
        raise ValueError("service reason is invalid")
    if expected_normalized == "success" and reason is not None:
        raise ValueError("successful service reason must be null")
    if expected_normalized == "unexplained" and reason not in {
        "service_evidence_gap", "service_success_without_joint_pointer",
    }:
        raise ValueError("unexplained service reason is invalid")
    return result


def _ledger_root(data_dir: Path) -> Path:
    return Path(data_dir) / "routes/shadow/ledger"


def _ensure_ledger_root(data_dir: Path) -> Path:
    root = _ledger_root(data_dir)
    descriptor, _snapshots = open_verified_directory_chain(root, create=True)
    os.close(descriptor)
    return root


def _write_member(run_dir: Path, name: str, value: Mapping[str, Any]) -> bytes:
    if name not in _LIMITS:
        raise ValueError("ledger member is not writable by Task 3")
    payload = _canonical_bytes(value)
    if len(payload) > _LIMITS[name]:
        raise ValueError("ledger member exceeds its bound")
    run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = run_dir / name
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(str(target), flags, 0o600)
    except FileExistsError:
        current = target.read_bytes()
        if current != payload:
            raise ValueError("immutable ledger member conflicts")
        return current
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(fd, payload[offset:])
            if written <= 0:
                raise OSError("short ledger write")
            offset += written
        os.fsync(fd)
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise ValueError("ledger member is unsafe")
    finally:
        os.close(fd)
    directory_fd = os.open(str(run_dir), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return payload


def _read_member(run_dir: Path, name: str) -> Optional[Tuple[Dict[str, Any], bytes]]:
    path = run_dir / name
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    except FileNotFoundError:
        return None
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise ValueError("ledger member is unsafe or hard-linked")
        payload = os.read(fd, _LIMITS[name] + 1)
        if len(payload) > _LIMITS[name]:
            raise ValueError("ledger member exceeds its bound")
    finally:
        os.close(fd)
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("ledger JSON is invalid") from error
    if not isinstance(value, dict) or _canonical_bytes(value) != payload:
        raise ValueError("ledger JSON is not canonical")
    validator = {
        "started.json": validate_started,
        "verification.json": validate_verification,
        "terminal.json": validate_terminal,
        "service.json": validate_service,
    }[name]
    return validator(value), payload


def load_run_ledger(data_dir: Path, run_id: str) -> Dict[str, Any]:
    validated_run_id = _validate_run_id(run_id)
    run_dir = _ledger_root(data_dir) / validated_run_id
    try:
        members = set(os.listdir(run_dir))
    except FileNotFoundError as error:
        raise ValueError("route Shadow ledger run is missing") from error
    if members - LEDGER_MEMBERS:
        raise ValueError("route Shadow ledger has an unknown unsafe member")
    if members & RESERVED_CANDIDATE_MEMBERS:
        raise ValueError("candidate_contract_not_available")
    if "runtime.json" in members:
        raise ValueError("runtime_contract_not_available")
    loaded = {
        key[:-5]: _read_member(run_dir, key)
        for key in ("started.json", "verification.json", "terminal.json", "service.json")
    }
    started = loaded["started"]
    verification = loaded["verification"]
    terminal = loaded["terminal"]
    service = loaded["service"]
    if terminal is None:
        if service is not None:
            raise ValueError("service-only ledger closure is invalid")
        return {
            "run_id": validated_run_id,
            "started": None if started is None else started[0],
            "verification": None if verification is None else verification[0],
            "terminal": None,
            "service": None,
            "status": "pending",
        }
    terminal_value, terminal_bytes = terminal
    started_sha = None if started is None else _sha256(started[1])
    verification_sha = None if verification is None else _sha256(verification[1])
    if terminal_value["run_id"] != validated_run_id or terminal_value["started_sha256"] != started_sha or terminal_value["verification_sha256"] != verification_sha:
        raise ValueError("ledger member hashes are inconsistent")
    if started is not None and (started[0]["run_id"] != validated_run_id or started[0]["dispatch_id"] != terminal_value["dispatch_id"]):
        raise ValueError("started/terminal lineage differs")
    if verification is not None and (verification[0]["run_id"] != validated_run_id or verification[0]["dispatch_id"] != terminal_value["dispatch_id"] or verification[0]["started_sha256"] != started_sha):
        raise ValueError("verification lineage differs")
    if verification is not None:
        verification_value = verification[0]
        for field in (
            "typed_source_manifest_sha256",
            "route_cost_evidence_sha256",
            "run_capture_admission_sha256",
            "run_admission_sha256",
            "storage_admission_status",
        ):
            if verification_value[field] != terminal_value[field]:
                raise ValueError(
                    "verification/terminal {} lineage differs".format(field)
                )
        successful_verification = (
            verification_value["primary_failure_class"] == "none"
            and verification_value["result_status"] == "verified"
            and verification_value["last_completed_stage"] == "joint_pointer"
        )
        if (terminal_value["outcome"] == "success") != successful_verification:
            raise ValueError("verification/terminal success matrix differs")
    if service is not None and service[0]["terminal_sha256"] != _sha256(terminal_bytes):
        raise ValueError("service terminal binding differs")
    if service is not None:
        service_value = service[0]
        if (
            service_value["run_id"] != validated_run_id
            or service_value["dispatch_id"] != terminal_value["dispatch_id"]
            or service_value["runtime_evidence_sha256"]
            != terminal_value["runtime_evidence_sha256"]
        ):
            raise ValueError("service/run ledger lineage differs")
        if (
            service_value["normalized_outcome"] == "success"
            and terminal_value["outcome"] != "success"
        ):
            raise ValueError("service success has no successful terminal")
    if started is None and terminal_value["lock_acquired"] is not None:
        raise ValueError("terminal without started has invalid lock state")
    if terminal_value["lock_acquired"] is True and verification is None:
        raise ValueError("acquired run has no verification")
    return {
        "run_id": validated_run_id,
        "started": None if started is None else started[0],
        "verification": None if verification is None else verification[0],
        "terminal": terminal_value,
        "service": None if service is None else service[0],
        "status": "terminal",
    }


def _install_busy_closure(data_dir: Path, *, run_id: str, dispatch_id: Optional[str], invocation_id: Optional[str], started_at: str, boot_id: str, monotonic_ns: int) -> Dict[str, Any]:
    root = _ensure_ledger_root(data_dir)
    final = root / run_id
    if final.exists():
        ledger = load_run_ledger(data_dir, run_id)
        if ledger["terminal"] is None:
            raise ValueError("existing busy run is unterminated")
        return ledger["terminal"]
    stage = root / ".busy-{}-{}".format(run_id, uuid.uuid4().hex)
    stage.mkdir(mode=0o700)
    try:
        started = validate_started({
            "schema": RUN_STARTED_SCHEMA,
            "run_id": run_id,
            "dispatch_id": dispatch_id,
            "phase": "canary",
            "phase_state_sha256": hashlib.sha256(b"route-shadow-phase/implicit-canary/v1\n").hexdigest(),
            "phase_transition_id": None,
            "invocation_id": invocation_id,
            "started_at": started_at,
            "boot_id": boot_id,
            "monotonic_ns": monotonic_ns,
        })
        started_bytes = _write_member(stage, "started.json", started)
        terminal = validate_terminal({
            "schema": RUN_TERMINAL_SCHEMA,
            "run_id": run_id,
            "dispatch_id": dispatch_id,
            "outcome": "skipped_locked",
            "finished_at": started_at,
            "lock_acquired": False,
            "duration_status": "not_evaluated",
            "duration_seconds": None,
            "route_cohort_id": None,
            "started_sha256": _sha256(started_bytes),
            "verification_sha256": None,
            "runtime_evidence_sha256": None,
            "run_capture_admission_sha256": None,
            "run_admission_sha256": None,
            "storage_admission_status": "not_evaluated",
            "typed_source_manifest_sha256": None,
            "route_cost_evidence_sha256": None,
            "joint_pointer_sha256": None,
            "reason_code": "collection_lock_busy",
        })
        _write_member(stage, "terminal.json", terminal)
        os.rename(stage, final)
        parent_fd = os.open(str(root), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return terminal
    finally:
        if stage.exists():
            for child in stage.iterdir():
                child.unlink()
            stage.rmdir()


def _open_collection_lock(data_dir: Path) -> int:
    parent = Path(data_dir) / "collection"
    parent_fd, _ancestry = open_verified_directory_chain(parent, create=True)
    try:
        fd = open_verified_regular_at(
            parent_fd, "collection.lock", create=True, mode=0o600
        )
        # Creating the missing lock legitimately changes the collection
        # directory's ctime/mtime.  Reopen the pathname without following any
        # ancestor link and prove it still names this exact opened directory;
        # accepting a freshly sampled generation without the inode comparison
        # would turn a directory swap into a second lock domain.
        current_parent_fd, _current_ancestry = open_verified_directory_chain(
            parent
        )
        try:
            opened_parent = os.fstat(parent_fd)
            current_parent = os.fstat(current_parent_fd)
            if (
                opened_parent.st_dev,
                opened_parent.st_ino,
            ) != (
                current_parent.st_dev,
                current_parent.st_ino,
            ):
                raise ValueError("collection lock directory changed")
            current_lock = os.stat(
                "collection.lock",
                dir_fd=current_parent_fd,
                follow_symlinks=False,
            )
            opened_lock = os.fstat(fd)
            if (
                opened_lock.st_dev,
                opened_lock.st_ino,
            ) != (
                current_lock.st_dev,
                current_lock.st_ino,
            ):
                raise ValueError("collection lock file changed")
        finally:
            os.close(current_parent_fd)
        return fd
    except BaseException:
        if "fd" in locals():
            os.close(fd)
        raise
    finally:
        os.close(parent_fd)


def _install_shadow_run_sidecar(
    run_dir: Path, name: str, value: Mapping[str, Any], *, maximum: int
) -> Tuple[Dict[str, Any], str]:
    if (
        not isinstance(name, str)
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
    ):
        raise ValueError("route Shadow sidecar name is unsafe")
    payload = _canonical_bytes(value)
    if len(payload) > maximum:
        raise ValueError("route Shadow sidecar exceeds its bound")
    directory_fd, _chain = open_verified_directory_chain(run_dir)
    try:
        flags = (
            os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        )
        descriptor = os.open(name, flags, 0o600, dir_fd=directory_fd)
        try:
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise OSError("short route Shadow sidecar write")
                offset += written
            os.fsync(descriptor)
            details = os.fstat(descriptor)
            path_details = os.stat(
                name, dir_fd=directory_fd, follow_symlinks=False
            )
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_nlink != 1
                or details.st_size != len(payload)
                or (details.st_dev, details.st_ino)
                != (path_details.st_dev, path_details.st_ino)
            ):
                raise ValueError("route Shadow sidecar is unsafe")
        finally:
            os.close(descriptor)
        os.fsync(directory_fd)
        read_fd = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_fd,
        )
        try:
            read_details = os.fstat(read_fd)
            actual = os.read(read_fd, maximum + 1)
            if (
                len(actual) > maximum
                or not stat.S_ISREG(read_details.st_mode)
                or read_details.st_nlink != 1
                or actual != payload
            ):
                raise ValueError("route Shadow sidecar changed after install")
        finally:
            os.close(read_fd)
    finally:
        os.close(directory_fd)
    return dict(value), _sha256(payload)


def _duration_text(start_ns: int, end_ns: int) -> str:
    if type(start_ns) is not int or type(end_ns) is not int or end_ns < start_ns:
        raise ValueError("route Shadow monotonic duration is invalid")
    seconds = end_ns - start_ns
    whole, remainder = divmod(seconds, 1_000_000_000)
    if remainder == 0:
        return str(whole)
    return "{}.{:09d}".format(whole, remainder).rstrip("0")


def _verification_counts(process_counts: Mapping[str, Any]) -> Dict[str, int]:
    result = {}
    for field in (
        "collector_process_started_count",
        "collector_process_reaped_count",
        "orphan_process_count",
    ):
        value = process_counts.get(field, 0)
        if type(value) is not int or value < 0:
            raise ValueError("collector process evidence is invalid")
        result[field] = value
    return result


def _project_canary_universe(universe: Mapping[str, Any]) -> Dict[str, Any]:
    """Project the full deterministic universe onto the literal canary set."""
    if not isinstance(universe, Mapping):
        raise ValueError("route universe is invalid")
    raw_routes = universe.get("routes")
    raw_legs = universe.get("selected_legs")
    if not isinstance(raw_routes, list) or not isinstance(raw_legs, list):
        raise ValueError("route universe inventory is invalid")
    routes = []
    for raw in raw_routes:
        if not isinstance(raw, Mapping):
            raise ValueError("route universe route is invalid")
        symbol = raw.get("token_symbol")
        if symbol in _CANARY_TOKEN_ALLOWLIST:
            routes.append(dict(raw))
    present = {route.get("token_symbol") for route in routes}
    if present != _CANARY_TOKEN_ALLOWLIST:
        raise ValueError("canary route token inventory is incomplete")
    selected_market_ids = set()
    for route in routes:
        for field in ("buy_market_id", "sell_market_id"):
            market_id = route.get(field)
            if not isinstance(market_id, str) or not market_id:
                raise ValueError("canary route market identity is invalid")
            selected_market_ids.add(market_id)
    legs = []
    for raw in raw_legs:
        if not isinstance(raw, Mapping):
            raise ValueError("route universe leg is invalid")
        if raw.get("market_id") in selected_market_ids:
            legs.append(dict(raw))
    if {leg.get("market_id") for leg in legs} != selected_market_ids:
        raise ValueError("canary route leg inventory is incomplete")
    return {**dict(universe), "selected_legs": legs, "routes": routes}


def _run_shadow_owned(data_dir: Path, *, run_id: str, dispatch_id: Optional[str], invocation_id: Optional[str], started_at: datetime, monotonic_ns: int, boot_id: str, expected_phase: Optional[str], lock_fd: int) -> Dict[str, Any]:
    # Lazy imports keep the fixed public wrapper importable while Tasks 1/2/3
    # remain independently testable and avoid module-import cycles.
    try:
        from scripts.collect_route_cohort import (
            _default_dex_block_resolver,
            attach_typed_source_lineage,
            collect_route_cohort,
        )
        from scripts.route_cost_collector import (
            _collect_route_cost_evidence_manifest_with_capability,
        )
        from scripts.route_publication import (
            load_active_phase_state,
            load_latest_shadow_result,
            publish_route_cohort_bundle,
            publish_shadow_result,
        )
        from scripts.route_shadow_audit import build_shadow_audit
        from scripts.route_shadow_inputs import (
            PROJECT_ROOT,
            build_shadow_universe,
            current_source_generation,
            load_run_input_binding,
            write_run_universe,
        )
    except ModuleNotFoundError:  # pragma: no cover - direct script execution
        from collect_route_cohort import (  # type: ignore
            _default_dex_block_resolver,
            attach_typed_source_lineage,
            collect_route_cohort,
        )
        from route_cost_collector import (  # type: ignore
            _collect_route_cost_evidence_manifest_with_capability,
        )
        from route_publication import (  # type: ignore
            load_active_phase_state,
            load_latest_shadow_result,
            publish_route_cohort_bundle,
            publish_shadow_result,
        )
        from route_shadow_audit import build_shadow_audit  # type: ignore
        from route_shadow_inputs import (  # type: ignore
            PROJECT_ROOT,
            build_shadow_universe,
            current_source_generation,
            load_run_input_binding,
            write_run_universe,
        )

    root = Path(data_dir)
    timeline = _RunTimeline(started_at, monotonic_ns)
    shadow_root = root / "routes/shadow"
    core_root = root / "routes/core"
    raw_root = root / "raw/route-cohort"
    static_token_config = PROJECT_ROOT / "config/tokens.csv"
    run_dir = _ensure_ledger_root(root) / run_id
    phase_view = load_active_phase_state(shadow_root)
    phase_mismatch = (
        expected_phase is not None and phase_view["phase"] != expected_phase
    )
    started = validate_started({
        "schema": RUN_STARTED_SCHEMA,
        "run_id": run_id,
        "dispatch_id": dispatch_id,
        "phase": phase_view["phase"],
        "phase_state_sha256": phase_view["phase_state_sha256"],
        "phase_transition_id": phase_view["phase_transition_id"],
        "invocation_id": invocation_id,
        "started_at": _utc_text(started_at),
        "boot_id": boot_id,
        "monotonic_ns": monotonic_ns,
    })
    started_bytes = _write_member(run_dir, "started.json", started)
    started_sha = _sha256(started_bytes)
    process_counts: Dict[str, int] = {
        "collector_process_started_count": 0,
        "collector_process_reaped_count": 0,
        "orphan_process_count": 0,
    }
    stage = "none"
    cohort_id = None
    typed_sha = None
    cost_sha = None
    joint_sha = None
    failure_class = "unexplained"
    reason = "storage_not_evaluated"
    error_counts = {
        "primary_publication_interference_count": 0,
        "core_orphan_count": 0,
        "pointer_interference_count": 0,
        "lineage_error_count": 0,
        "unsafe_path_error_count": 0,
        "source_generation_error_count": 0,
        "resource_limit_error_count": 0,
        "runtime_limit_error_count": 0,
    }
    try:
        if phase_mismatch:
            raise ValueError(
                "active route Shadow phase differs from expected phase"
            )
        registry_snapshot = _route_cost_registry_snapshot()
        universe, baseline = build_shadow_universe(
            root, started_at, static_token_config=static_token_config
        )
        # Build the complete deterministic inventory first, then project both
        # route and leg inventories onto the literal canary denominator.
        if phase_view["phase"] == "canary":
            universe = _project_canary_universe(universe)
        stage = "input_capture"
        write_run_universe(shadow_root, run_id, universe, baseline)
        binding = load_run_input_binding(shadow_root, run_id)
        stage = "universe"
        generation = binding["candidate_source_generation"]

        def generation_reader() -> str:
            try:
                current_registries = _route_cost_registry_snapshot()
            except BaseException as error:
                raise ValueError(
                    "route-cost registry generation changed"
                ) from error
            if current_registries != registry_snapshot:
                raise ValueError("route-cost registry generation changed")
            return current_source_generation(
                root, static_token_config=static_token_config
            )

        cohort = collect_route_cohort(
            binding["universe"],
            deadline_seconds=60,
            max_workers=2 if phase_view["phase"] == "canary" else 4,
            cex_workers_per_venue=1,
            dex_workers_per_chain=1,
            target_observed_at=_utc_text(started_at),
            dex_block_resolver=_default_dex_block_resolver,
            source_generation_reader=generation_reader,
            expected_source_generation=generation,
            raw_root=raw_root,
            snapshot_id=run_id,
            child_close_fds=(lock_fd,),
            process_evidence_sink=process_counts,
            wall_clock=timeline.wall_clock,
        )
        process_counts = _verification_counts(process_counts)
        stage = "collection"
        cohort, typed_publication = attach_typed_source_lineage(
            cohort, raw_root=raw_root
        )
        typed_sha = typed_publication["typed_source_manifest_sha256"]
        cohort_id = cohort["route_cohort_id"]
        cost_evidence = _collect_route_cost_evidence_manifest_with_capability(
            root,
            universe=binding["universe"],
            cohort=cohort,
            run_id=run_id,
            phase=phase_view["phase"],
            route_universe_sha256=binding["route_universe_sha256"],
            capability=timeline.wall_clock,
        )
        if any(
            cost_evidence.get(field) != registry_snapshot[field]
            for field in (
                "adapter_registry", "adapter_registry_sha256",
                "connector_key_registry", "connector_key_registry_sha256",
            )
        ):
            raise ValueError("route-cost registry generation changed")
        _cost, cost_sha = _install_shadow_run_sidecar(
            shadow_root / "runs" / run_id,
            "route-cost-evidence.json",
            cost_evidence,
            maximum=32 * 1024 * 1024,
        )
        # The collector owns start/completion checks. The orchestrator adds
        # exactly the pre-core and pre-joint checks against the immutable
        # binding, for four total source-generation checkpoints.
        if generation_reader() != generation:
            raise ValueError("collection input generation changed")
        core_pointer = publish_route_cohort_bundle(
            cohort, core_root=core_root
        )
        stage = "core"
        if generation_reader() != generation:
            error_counts["core_orphan_count"] = 1
            raise ValueError("collection input generation changed")
        audit_finished_at = _utc_text(
            _utc_whole_second_ceiling(timeline.sample()[0])
        )
        audit = build_shadow_audit(
            cohort,
            core_pointer=core_pointer,
            run={
                "run_id": run_id,
                "phase_state_sha256": phase_view["phase_state_sha256"],
                "phase_transition_id": phase_view["phase_transition_id"],
                "route_universe_sha256": binding["route_universe_sha256"],
                "baseline_manifest_sha256": binding[
                    "baseline_manifest_sha256"
                ],
                "candidate_source_generation": generation,
                "route_cost_evidence_sha256": cost_sha,
            },
            phase=phase_view["phase"],
            audit_finished_at=audit_finished_at,
        )
        stage = "audit"
        published = publish_shadow_result(
            shadow_root, core_pointer=core_pointer, audit=audit
        )
        loaded = load_latest_shadow_result(shadow_root)
        if (
            loaded.get("pointer_sha256") != published.get("pointer_sha256")
            or loaded.get("pointer", {}).get("run_id") != run_id
        ):
            error_counts["pointer_interference_count"] = 1
            raise ValueError("joint route Shadow pointer changed")
        joint_sha = published["pointer_sha256"]
        stage = "joint_pointer"
        # Task 5 storage A0/B does not exist yet.  Task 3 therefore exercises
        # and proves the complete private mechanics but cannot truthfully emit
        # a successful, promotion-countable terminal.
        failure_class = "unexplained"
        reason = "storage_not_evaluated"
    except BaseException as error:
        message = str(error).lower()
        if "generation" in message:
            failure_class = "source_generation_drift"
            error_counts["source_generation_error_count"] = 1
            reason = "source_generation_drift"
        elif "unsafe" in message or "symlink" in message or "hard-link" in message:
            failure_class = "unsafe_path"
            error_counts["unsafe_path_error_count"] = 1
            reason = "unsafe_path"
        elif "resource" in message or "exceeds" in message or "too large" in message:
            failure_class = "resource_limit"
            error_counts["resource_limit_error_count"] = 1
            reason = "resource_limit"
        elif "pointer" in message:
            failure_class = "pointer_interference"
            error_counts["pointer_interference_count"] = 1
            reason = "pointer_interference"
        elif "collection" in message or "source failure" in message:
            failure_class = "transient_collection"
            reason = "transient_collection"
        else:
            failure_class = "lineage_invalid"
            error_counts["lineage_error_count"] = 1
            reason = "lineage_invalid"
    verified_at = _utc_text(timeline.sample()[0])
    process_counts = _verification_counts(process_counts)
    reason_codes = sorted({reason, "storage_not_evaluated"})
    verification = validate_verification({
        "schema": RUN_VERIFICATION_SCHEMA,
        "run_id": run_id,
        "dispatch_id": dispatch_id,
        "started_sha256": started_sha,
        "verified_at": verified_at,
        "primary_failure_class": failure_class,
        **process_counts,
        **error_counts,
        "last_completed_stage": stage,
        "result_status": "failed",
        "typed_source_manifest_sha256": typed_sha,
        "route_cost_evidence_sha256": cost_sha,
        "run_capture_admission_sha256": None,
        "run_admission_sha256": None,
        "storage_admission_status": "not_evaluated",
        "reason_codes": reason_codes,
    })
    verification_bytes = _write_member(
        run_dir, "verification.json", verification
    )
    finished_at, finished_ns = timeline.sample()
    terminal = validate_terminal({
        "schema": RUN_TERMINAL_SCHEMA,
        "run_id": run_id,
        "dispatch_id": dispatch_id,
        "outcome": "failed",
        "finished_at": _utc_text(finished_at),
        "lock_acquired": True,
        "duration_status": "evaluated",
        "duration_seconds": _duration_text(monotonic_ns, finished_ns),
        "route_cohort_id": cohort_id,
        "started_sha256": started_sha,
        "verification_sha256": _sha256(verification_bytes),
        "runtime_evidence_sha256": None,
        "run_capture_admission_sha256": None,
        "run_admission_sha256": None,
        "storage_admission_status": "not_evaluated",
        "typed_source_manifest_sha256": typed_sha,
        "route_cost_evidence_sha256": cost_sha,
        "joint_pointer_sha256": joint_sha,
        "reason_code": "storage_not_evaluated",
    })
    _write_member(run_dir, "terminal.json", terminal)
    return {"status": "terminal", **terminal}


def _authority_view(value: Mapping[str, Any]) -> Dict[str, Any]:
    expected = {"schema", "status", "transaction_id", "authority_sha256", "primary_unit_projection_sha256", "reason_code"}
    if not isinstance(value, Mapping) or set(value) != expected or value.get("schema") != "route_shadow_authority_view/v1" or value.get("status") not in {"enabled", "disabled", "invalid"}:
        raise ValueError("route Shadow authority view is invalid")
    return dict(value)


def _run_shadow_with_capability(data_dir: Path, *, expected_phase: Optional[str], clock: Callable[[], datetime], run_id_factory: Callable[[], str]) -> Dict[str, Any]:
    authority = _authority_view(load_committed_route_shadow_authority(Path(data_dir)))
    if authority["status"] != "enabled":
        return {"schema": "route_shadow_run_result/v1", "status": authority["status"], "reason_code": authority["reason_code"]}
    invocation = os.environ.get("INVOCATION_ID")
    dispatch = os.environ.get("ROUTE_SHADOW_DISPATCH_ID")
    explicit = os.environ.get("ROUTE_SHADOW_RUN_ID")
    scheduled = dispatch is not None
    if scheduled and explicit is not None and explicit != invocation:
        raise ValueError("explicit run ID and INVOCATION_ID differ")
    if scheduled and (
        _HEX32.fullmatch(dispatch) is None
        or _HEX32.fullmatch(invocation or "") is None
    ):
        raise ValueError("scheduled route Shadow identity is invalid")
    run_id = _validate_run_id(
        invocation if scheduled else (
            explicit if explicit is not None else run_id_factory()
        )
    )
    dispatch_id = dispatch if scheduled else None
    invocation_id = invocation if scheduled else None
    lock_fd = _open_collection_lock(data_dir)
    acquired = False
    owner_nonce = None
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            now = clock()
            terminal = _install_busy_closure(data_dir, run_id=run_id, dispatch_id=dispatch_id, invocation_id=invocation_id, started_at=_utc_text(now), boot_id=_boot_id(), monotonic_ns=time.monotonic_ns())
            return {"status": "terminal", **terminal}
        authoritative = _authority_view(
            load_committed_route_shadow_authority(Path(data_dir))
        )
        if authoritative["status"] != "enabled":
            return {
                "schema": "route_shadow_run_result/v1",
                "status": authoritative["status"],
                "reason_code": authoritative["reason_code"],
            }
        if authoritative != authority:
            return {
                "schema": "route_shadow_run_result/v1",
                "status": "invalid",
                "reason_code": "authority_changed_during_lock_acquisition",
            }
        now = clock()  # sole trusted wall-clock sample for an owned run
        boot_id = _boot_id()
        monotonic_ns = time.monotonic_ns()
        owner_nonce = uuid.uuid4().hex
        write_shadow_lock_owner(lock_fd, run_id=run_id, boot_id=boot_id, nonce=owner_nonce)
        return _run_shadow_owned(data_dir, run_id=run_id, dispatch_id=dispatch_id, invocation_id=invocation_id, started_at=now, monotonic_ns=monotonic_ns, boot_id=boot_id, expected_phase=expected_phase, lock_fd=lock_fd)
    finally:
        if acquired:
            if owner_nonce is not None:
                clear_shadow_lock_owner(lock_fd, nonce=owner_nonce)
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def run_shadow_once(data_dir: Path, *, expected_phase: Optional[str] = None) -> dict:
    """Run one fixed, nonblocking route Shadow attempt."""
    return _run_shadow_with_capability(Path(data_dir), expected_phase=expected_phase, clock=_trusted_utc_now, run_id_factory=_new_manual_run_id)


def reconcile_shadow_run(data_dir: Path, *, run_id: str, dispatch_id: str, service_result: str, exit_code: str, exit_status: str) -> Dict[str, Any]:
    validated_run_id = _validate_run_id(run_id)
    if _HEX32.fullmatch(validated_run_id) is None or _HEX32.fullmatch(dispatch_id or "") is None:
        raise ValueError("scheduled reconciliation IDs are invalid")
    now = _utc_text(_trusted_utc_now())
    run_dir = _ensure_ledger_root(data_dir) / validated_run_id
    try:
        existing = load_run_ledger(data_dir, validated_run_id)
    except ValueError as error:
        if "missing" not in str(error):
            raise
        existing = None
    normalized = normalize_systemd_result(service_result, exit_code, exit_status)
    if existing is not None and existing["service"] is not None:
        service = existing["service"]
        if (
            existing["terminal"] is None
            or service["dispatch_id"] != dispatch_id
            or service["service_result"] != service_result
            or service["exit_code"] != (exit_code or None)
            or service["exit_status"] != (exit_status or None)
        ):
            raise ValueError("conflicting route Shadow reconciliation retry")
        return {"terminal": existing["terminal"], "service": service}
    if existing is None:
        terminal = validate_terminal({
            "schema": RUN_TERMINAL_SCHEMA,
            "run_id": validated_run_id,
            "dispatch_id": dispatch_id,
            "outcome": "unexplained",
            "finished_at": now,
            "lock_acquired": None,
            "duration_status": "not_evaluated",
            "duration_seconds": None,
            "route_cohort_id": None,
            "started_sha256": None,
            "verification_sha256": None,
            "runtime_evidence_sha256": None,
            "run_capture_admission_sha256": None,
            "run_admission_sha256": None,
            "storage_admission_status": "not_evaluated",
            "typed_source_manifest_sha256": None,
            "route_cost_evidence_sha256": None,
            "joint_pointer_sha256": None,
            "reason_code": "pre_started_lock_state_unknown",
        })
        terminal_bytes = _write_member(run_dir, "terminal.json", terminal)
        service_result = ""
        exit_code = ""
        exit_status = ""
        service_normalized = "unexplained"
        reason = "service_evidence_gap"
        started_at = now
    else:
        terminal = existing["terminal"]
        if terminal is None:
            started_value = existing["started"]
            if started_value is None:
                raise ValueError("route Shadow run has no started evidence")
            if started_value["dispatch_id"] != dispatch_id:
                raise ValueError("route Shadow reconciliation dispatch differs")
            started_member = _read_member(run_dir, "started.json")
            if started_member is None:
                raise ValueError("route Shadow started evidence disappeared")
            started_sha = _sha256(started_member[1])
            if normalized == "timeout":
                outcome = "timeout"
                failure_class = "timeout"
                terminal_reason = "timeout"
                reasons = ["storage_not_evaluated", "timeout"]
            elif normalized == "oom":
                outcome = "oom"
                failure_class = "oom"
                terminal_reason = "oom"
                reasons = ["oom", "storage_not_evaluated"]
            else:
                outcome = "failed" if normalized == "failed" else "unexplained"
                failure_class = "unexplained"
                terminal_reason = "storage_not_evaluated"
                reasons = ["storage_not_evaluated", "unexplained"]
            verification = validate_verification({
                "schema": RUN_VERIFICATION_SCHEMA,
                "run_id": validated_run_id,
                "dispatch_id": dispatch_id,
                "started_sha256": started_sha,
                "verified_at": now,
                "primary_failure_class": failure_class,
                "collector_process_started_count": 0,
                "collector_process_reaped_count": 0,
                "orphan_process_count": 0,
                "primary_publication_interference_count": 0,
                "core_orphan_count": 0,
                "pointer_interference_count": 0,
                "lineage_error_count": 0,
                "unsafe_path_error_count": 0,
                "source_generation_error_count": 0,
                "resource_limit_error_count": 0,
                "runtime_limit_error_count": 0,
                "last_completed_stage": "none",
                "result_status": "failed",
                "typed_source_manifest_sha256": None,
                "route_cost_evidence_sha256": None,
                "run_capture_admission_sha256": None,
                "run_admission_sha256": None,
                "storage_admission_status": "not_evaluated",
                "reason_codes": sorted(reasons),
            })
            verification_bytes = _write_member(
                run_dir, "verification.json", verification
            )
            terminal = validate_terminal({
                "schema": RUN_TERMINAL_SCHEMA,
                "run_id": validated_run_id,
                "dispatch_id": dispatch_id,
                "outcome": outcome,
                "finished_at": now,
                "lock_acquired": True,
                # A different process cannot compare the worker's monotonic
                # start sample. Never manufacture an SLA duration from wall
                # time or serialize a misleading zero.
                "duration_status": "not_evaluated",
                "duration_seconds": None,
                "route_cohort_id": None,
                "started_sha256": started_sha,
                "verification_sha256": _sha256(verification_bytes),
                "runtime_evidence_sha256": None,
                "run_capture_admission_sha256": None,
                "run_admission_sha256": None,
                "storage_admission_status": "not_evaluated",
                "typed_source_manifest_sha256": None,
                "route_cost_evidence_sha256": None,
                "joint_pointer_sha256": None,
                "reason_code": terminal_reason,
            })
            terminal_bytes = _write_member(run_dir, "terminal.json", terminal)
        else:
            terminal_bytes = _canonical_bytes(terminal)
        service_normalized = normalized
        reason = None if normalized != "unexplained" else "service_evidence_gap"
        started_at = existing["started"]["started_at"] if existing["started"] is not None else now
        if normalized == "success" and terminal["outcome"] != "success":
            service_normalized = "unexplained"
            reason = "service_success_without_joint_pointer"
    service = validate_service({
        "schema": SERVICE_SCHEMA,
        "service_kind": "worker",
        "dispatch_id": dispatch_id,
        "run_id": validated_run_id,
        "attempt_id": validated_run_id,
        "unit_name": "cex-dex-route-shadow-worker@{}.service".format(validated_run_id),
        "invocation_id": validated_run_id,
        "terminal_sha256": _sha256(terminal_bytes),
        "runtime_evidence_sha256": terminal["runtime_evidence_sha256"],
        "service_result": service_result,
        "exit_code": exit_code or None,
        "exit_status": exit_status or None,
        "normalized_outcome": service_normalized,
        "started_at": started_at,
        "finished_at": now,
        "reason_code": reason,
    })
    _write_member(run_dir, "service.json", service)
    return {"terminal": terminal, "service": service}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--data-dir", type=Path, required=True)
    run.add_argument("--run-id")
    run.add_argument("--expected-phase", choices=("canary", "full"))
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--data-dir", type=Path, required=True)
    reconcile.add_argument("--run-id", required=True)
    reconcile.add_argument("--dispatch-id", required=True)
    reconcile.add_argument("--service-result", required=True)
    reconcile.add_argument("--exit-code", required=True)
    reconcile.add_argument("--exit-status", required=True)
    return parser


def main(argv: Optional[list] = None) -> Dict[str, Any]:
    args = build_parser().parse_args(argv)
    if args.command == "run":
        if args.run_id is not None:
            os.environ["ROUTE_SHADOW_RUN_ID"] = _validate_run_id(args.run_id)
        result = run_shadow_once(args.data_dir, expected_phase=args.expected_phase)
    else:
        result = reconcile_shadow_run(
            args.data_dir,
            run_id=args.run_id,
            dispatch_id=args.dispatch_id,
            service_result=args.service_result,
            exit_code=args.exit_code,
            exit_status=args.exit_status,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


if __name__ == "__main__":  # pragma: no cover
    main()
