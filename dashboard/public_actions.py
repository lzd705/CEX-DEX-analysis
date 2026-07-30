"""Narrow, abuse-bounded public actions for the market dashboard.

This module deliberately does not expose the administrator surface.  It only
coordinates the two user-facing mutations approved for public use:

* onboarding one contract-qualified Token; and
* retrying one exact window from the current daily quality report.

Request payload validation and the final trusted-window check remain in the
HTTP/service layers.  The policy here provides process-local rate limits and
concurrency gates, plus persisted daily accepted-job budgets.
"""

from __future__ import annotations

import hashlib
import math
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any, Callable, Iterator, Mapping

try:
    from dashboard.admin import environment_flag
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from admin import environment_flag


PUBLIC_TOKEN_RESOLVE_PATH = "/api/actions/tokens/resolve"
PUBLIC_TOKEN_ADD_PATH = "/api/actions/tokens"
PUBLIC_QUALITY_RETRYABLE_PATH = "/api/actions/quality/retryable"
PUBLIC_QUALITY_RETRY_PATH = "/api/actions/quality/retry"
PUBLIC_JOB_STATUS_PREFIX = "/api/actions/jobs/"
PUBLIC_ACTION_PATHS = frozenset(
    {
        PUBLIC_TOKEN_RESOLVE_PATH,
        PUBLIC_TOKEN_ADD_PATH,
        PUBLIC_QUALITY_RETRYABLE_PATH,
        PUBLIC_QUALITY_RETRY_PATH,
    }
)

TOKEN_ACTION_PATHS = frozenset(
    {PUBLIC_TOKEN_RESOLVE_PATH, PUBLIC_TOKEN_ADD_PATH}
)
QUALITY_ACTION_PATHS = frozenset(
    {PUBLIC_QUALITY_RETRYABLE_PATH, PUBLIC_QUALITY_RETRY_PATH}
)

PUBLIC_ADD_TOKEN_ACTOR = "public:add_token"
PUBLIC_QUALITY_RETRY_ACTOR = "public:quality_retry"
PUBLIC_TOKEN_HISTORY_DAYS = 30
MAX_RATE_LIMIT_BUCKETS = 4_096


class PublicActionError(ValueError):
    """Stable error response for the intentionally narrow public surface."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: HTTPStatus = HTTPStatus.BAD_REQUEST,
        retryable: bool = False,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": self.message,
            "error_code": self.code,
            "retryable": self.retryable,
        }


@dataclass(frozen=True)
class ActionLimit:
    """Fixed server-side limits; none are request-controlled."""

    requests: int
    window_seconds: int
    concurrency_groups: tuple[str, ...]
    daily_job_budget: int | None = None
    requested_by: str | None = None
    job_type: str | None = None


ACTION_LIMITS: Mapping[str, ActionLimit] = {
    "token_resolve": ActionLimit(
        requests=12,
        window_seconds=15 * 60,
        concurrency_groups=("token_source",),
    ),
    "token_add": ActionLimit(
        requests=3,
        window_seconds=60 * 60,
        concurrency_groups=("public_mutation", "token_source"),
        daily_job_budget=3,
        requested_by=PUBLIC_ADD_TOKEN_ACTOR,
        job_type="token_onboarding",
    ),
    "quality_retryable": ActionLimit(
        requests=30,
        window_seconds=60,
        concurrency_groups=("quality_read",),
    ),
    "quality_retry": ActionLimit(
        requests=6,
        window_seconds=60 * 60,
        concurrency_groups=("public_mutation",),
        daily_job_budget=12,
        requested_by=PUBLIC_QUALITY_RETRY_ACTOR,
        job_type="retry_failed",
    ),
    "job_status": ActionLimit(
        requests=60,
        window_seconds=60,
        concurrency_groups=("quality_read",),
    ),
}

CONCURRENCY_LIMITS: Mapping[str, int] = {
    "public_mutation": 1,
    "token_source": 1,
    "quality_read": 4,
}


class PublicActionPolicy:
    """Apply public feature flags, rate limits, concurrency, and job budgets."""

    def __init__(
        self,
        *,
        add_token_enabled: bool | None = None,
        quality_retry_enabled: bool | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        utc_now: Callable[[], datetime] | None = None,
    ) -> None:
        self.add_token_enabled = (
            environment_flag("PUBLIC_ADD_TOKEN_ENABLED")
            if add_token_enabled is None
            else add_token_enabled
        )
        self.quality_retry_enabled = (
            environment_flag("PUBLIC_QUALITY_RETRY_ENABLED")
            if quality_retry_enabled is None
            else quality_retry_enabled
        )
        self._monotonic = monotonic
        self._utc_now = utc_now or (lambda: datetime.now(timezone.utc))
        self._lock = threading.Lock()
        self._request_times: dict[tuple[str, str], list[float]] = {}
        self._active_groups: dict[str, int] = {}

    @staticmethod
    def _client_key(client_address: str) -> str:
        """Keep only a one-way process-local identifier for rate accounting."""
        return hashlib.sha256(
            str(client_address).encode("utf-8", errors="replace")
        ).hexdigest()

    def enabled_for_path(self, path: str) -> bool:
        if path in TOKEN_ACTION_PATHS:
            return self.add_token_enabled
        if path in QUALITY_ACTION_PATHS:
            return self.quality_retry_enabled
        return False

    @staticmethod
    def disabled_error() -> PublicActionError:
        return PublicActionError(
            "public_action_disabled",
            "This public action is not enabled",
            status=HTTPStatus.NOT_FOUND,
        )

    @contextmanager
    def permit(
        self,
        action: str,
        client_address: str,
        *,
        service: Any | None = None,
    ) -> Iterator[None]:
        """Reserve one bounded in-flight request and enforce accepted-job budget."""
        try:
            limit = ACTION_LIMITS[action]
        except KeyError as error:  # pragma: no cover - programmer error
            raise RuntimeError("Unknown public action policy") from error

        now = self._monotonic()
        key = (action, self._client_key(client_address))
        with self._lock:
            recent = [
                timestamp
                for timestamp in self._request_times.get(key, [])
                if now - timestamp < limit.window_seconds
            ]
            if len(recent) >= limit.requests:
                retry_after = max(
                    1,
                    int(limit.window_seconds - (now - recent[0])),
                )
                raise PublicActionError(
                    "public_rate_limit_exceeded",
                    "Too many requests for this public action",
                    status=HTTPStatus.TOO_MANY_REQUESTS,
                    retryable=True,
                    retry_after_seconds=retry_after,
                )

            for group in limit.concurrency_groups:
                if self._active_groups.get(group, 0) >= CONCURRENCY_LIMITS[group]:
                    raise PublicActionError(
                        "public_action_busy",
                        "This public action is already in progress",
                        status=HTTPStatus.CONFLICT,
                        retryable=True,
                        retry_after_seconds=5,
                    )

            if limit.daily_job_budget is not None:
                if service is None:  # pragma: no cover - programmer error
                    raise RuntimeError("Job-backed public action requires a service")
                current_time = self._utc_now().astimezone(timezone.utc)
                current_day = current_time.date()
                accepted = service.count_jobs_created_on(
                    requested_by=limit.requested_by,
                    job_type=limit.job_type,
                    created_on=current_day,
                )
                if accepted >= limit.daily_job_budget:
                    next_day = datetime.combine(
                        current_day + timedelta(days=1),
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    )
                    raise PublicActionError(
                        "public_daily_budget_exhausted",
                        "The daily accepted-job budget for this action is exhausted",
                        status=HTTPStatus.TOO_MANY_REQUESTS,
                        retryable=True,
                        retry_after_seconds=max(
                            1,
                            math.ceil(
                                (next_day - current_time).total_seconds()
                            ),
                        ),
                    )

            if (
                key not in self._request_times
                and len(self._request_times) >= MAX_RATE_LIMIT_BUCKETS
            ):
                oldest_key = next(iter(self._request_times))
                self._request_times.pop(oldest_key, None)
            recent.append(now)
            self._request_times[key] = recent
            for group in limit.concurrency_groups:
                self._active_groups[group] = self._active_groups.get(group, 0) + 1

        try:
            yield
        finally:
            with self._lock:
                for group in limit.concurrency_groups:
                    remaining = self._active_groups.get(group, 0) - 1
                    if remaining > 0:
                        self._active_groups[group] = remaining
                    else:
                        self._active_groups.pop(group, None)


def require_exact_string_fields(
    payload: Mapping[str, Any],
    field_limits: Mapping[str, int],
) -> dict[str, str]:
    """Reject missing, extra, non-string, empty, and oversized inputs."""
    actual = set(payload)
    expected = set(field_limits)
    if actual != expected:
        raise PublicActionError(
            "invalid_public_action_request",
            "Request fields do not match the public action contract",
        )
    normalized: dict[str, str] = {}
    for field, maximum_length in field_limits.items():
        value = payload.get(field)
        if not isinstance(value, str):
            raise PublicActionError(
                "invalid_public_action_request",
                f"{field} must be a string",
            )
        stripped = value.strip()
        if not stripped or len(stripped) > maximum_length:
            raise PublicActionError(
                "invalid_public_action_request",
                f"{field} is empty or exceeds its size limit",
            )
        normalized[field] = stripped
    return normalized


def public_retry_window(window: Mapping[str, Any]) -> dict[str, Any]:
    """Return only the fields a public client needs to request an exact retry."""
    values: dict[str, str] = {}
    for field, maximum_length in (
        ("token_symbol", 32),
        ("start_date", 10),
        ("end_date", 10),
        ("queue_type", 32),
    ):
        value = window.get(field)
        if (
            not isinstance(value, str)
            or not value
            or len(value) > maximum_length
        ):
            raise ValueError("Quality retry window has an invalid contract")
        values[field] = value
    try:
        start = date.fromisoformat(values["start_date"])
        end = date.fromisoformat(values["end_date"])
        if (
            start.isoformat() != values["start_date"]
            or end.isoformat() != values["end_date"]
            or start > end
        ):
            raise ValueError
    except ValueError as error:
        raise ValueError(
            "Quality retry window has an invalid contract"
        ) from error
    if values["queue_type"] not in {
        "latest_completed_day",
        "historical_gap",
    }:
        raise ValueError("Quality retry window has an invalid contract")
    raw_reason_codes = window.get("reason_codes") or []
    if (
        not isinstance(raw_reason_codes, list)
        or len(raw_reason_codes) > 100
        or any(
            not isinstance(code, str) or not code or len(code) > 128
            for code in raw_reason_codes
        )
    ):
        raise ValueError("Quality retry window has an invalid contract")
    raw_market_types = window.get("market_types") or []
    if (
        not isinstance(raw_market_types, list)
        or not raw_market_types
        or any(
            market_type not in {"cex", "dex"}
            for market_type in raw_market_types
        )
    ):
        raise ValueError("Quality retry window has an invalid contract")
    return {
        **values,
        "market_types": sorted(set(raw_market_types)),
        "reason_codes": list(raw_reason_codes),
    }


def public_token_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Project the resolved preview onto an explicit, source-only contract."""
    identity = candidate.get("identity")
    discovery = candidate.get("discovery")
    capabilities = candidate.get("capabilities")
    if (
        not isinstance(identity, Mapping)
        or not isinstance(discovery, Mapping)
        or not isinstance(capabilities, Mapping)
    ):
        raise ValueError("Resolved Token preview has an invalid contract")
    raw_pools = discovery.get("top_pools")
    if not isinstance(raw_pools, list) or any(
        not isinstance(pool, Mapping) for pool in raw_pools
    ):
        raise ValueError("Resolved Token preview has an invalid contract")
    identity_fields = (
        "chain",
        "contract_address",
        "token_symbol",
        "token_name",
        "decimals",
        "coingecko_id",
        "source",
        "source_token_id",
    )
    pool_fields = (
        "chain",
        "dex",
        "pool_address",
        "pool_name",
        "target_side",
        "base_token_id",
        "quote_token_id",
        "tvl_usd",
        "volume_24h_usd",
    )
    capability_fields = ("dex_daily", "tvl", "dex_depth", "cex")
    return {
        "identity": {
            field: identity.get(field)
            for field in identity_fields
        },
        "discovery": {
            "usable_pool_count": discovery.get("usable_pool_count"),
            "top_pools": [
                {field: pool.get(field) for field in pool_fields}
                for pool in raw_pools
            ],
        },
        "capabilities": {
            field: capabilities.get(field)
            for field in capability_fields
        },
        "already_configured": bool(
            candidate.get("already_configured")
        ),
    }


def public_job(job: Mapping[str, Any]) -> dict[str, Any]:
    """Strip worker, audit, and internal quality identity fields from a job."""
    allowed = (
        "job_id",
        "job_type",
        "token_symbol",
        "chain",
        "contract_address",
        "start_date",
        "end_date",
        "status",
        "stage",
        "created_at",
        "started_at",
        "finished_at",
        "error_code",
        "retryable",
        "publication_committed",
    )
    response = {field: job.get(field) for field in allowed if field in job}
    result = job.get("result")
    if isinstance(result, Mapping):
        summary_fields = (
            "already_configured",
            "daily",
            "dex_daily",
            "tvl",
            "dex_depth",
            "cex",
            "collection_scope",
            "observed_count",
            "confirmed_absence_count",
            "unresolved_count",
        )
        result_summary = {
            field: result[field]
            for field in summary_fields
            if field in result
            and (
                isinstance(result[field], (bool, int))
                or (
                    isinstance(result[field], str)
                    and len(result[field]) <= 80
                )
            )
        }
        if result_summary:
            response["result_summary"] = result_summary
    return response
