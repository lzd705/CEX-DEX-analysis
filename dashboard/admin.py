"""Server-side authentication and refresh jobs for the administrator page."""

from __future__ import annotations

import base64
import csv
import fcntl
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

try:
    from dashboard.token_onboarding import (
        TokenOnboardingError,
        build_registry_record,
        resolve_token_candidate,
    )
    from scripts.token_registry import (
        DEFAULT_REGISTRY_PATH,
        TokenRegistry,
        TokenRegistryError,
        token_identity_key,
        utc_now_text,
    )
    from scripts.fact_quality import cex_market, dex_market
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    project_root = str(Path(__file__).resolve().parents[1])
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from dashboard.token_onboarding import (  # type: ignore[no-redef]
        TokenOnboardingError,
        build_registry_record,
        resolve_token_candidate,
    )
    from scripts.token_registry import (  # type: ignore[no-redef]
        DEFAULT_REGISTRY_PATH,
        TokenRegistry,
        TokenRegistryError,
        token_identity_key,
        utc_now_text,
    )
    from scripts.fact_quality import (  # type: ignore[no-redef]
        cex_market,
        dex_market,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOKEN_CONFIG_PATH = PROJECT_ROOT / "config/tokens.csv"
TOKEN_CHAIN_CONFIG_PATH = PROJECT_ROOT / "config/token_chains.csv"
DEFAULT_JOB_DIR = PROJECT_ROOT / "data/local/admin/jobs"
DEFAULT_QUALITY_REPORT_PATH = PROJECT_ROOT / "data/local/quality/daily-latest.json"
DEFAULT_DATABASE_PATH = PROJECT_ROOT / "data/local/market_facts.sqlite3"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data/local"
DEFAULT_COLLECTION_LOCK_PATH = DEFAULT_DATA_DIR / "collection/collection.lock"
PASSWORD_ALGORITHM = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 600_000
SESSION_SECONDS = 8 * 60 * 60
LOGIN_WINDOW_SECONDS = 15 * 60
MAX_LOGIN_FAILURES = 5
OPEN_ADMIN_USERNAME = "open-admin"
TRUE_VALUES = {"1", "true", "yes", "on"}
QUALITY_REPORT_SCHEMA = "fact_quality_report/v1"
MAX_QUALITY_REPORT_BYTES = 8 * 1024 * 1024
MAX_REJECTION_POINTER_BYTES = 64 * 1024
PUBLIC_ADD_TOKEN_REQUESTER = "public:add_token"
PUBLIC_QUALITY_RETRY_REQUESTER = "public:quality_retry"
PUBLIC_COMMAND_TIMEOUT_SECONDS = 20 * 60
DEFAULT_COMMAND_TIMEOUT_SECONDS = 2 * 60 * 60


def _read_bounded_bytes(path: Path, maximum_bytes: int) -> bytes:
    with path.open("rb") as handle:
        payload = handle.read(maximum_bytes + 1)
    if len(payload) > maximum_bytes:
        raise ValueError("bounded file exceeds its size limit")
    return payload


def _valid_quality_report_structure(report: Any) -> bool:
    if (
        not isinstance(report, dict)
        or report.get("schema") != QUALITY_REPORT_SCHEMA
        or not isinstance(report.get("publication"), dict)
        or not isinstance(report.get("issues"), list)
        or any(not isinstance(issue, dict) for issue in report["issues"])
    ):
        return False
    for field in ("retry_windows_by_token", "backfill_windows_by_token"):
        grouped = report.get(field)
        if grouped is None:
            continue
        if not isinstance(grouped, dict):
            return False
        if any(
            not isinstance(token, str)
            or not isinstance(windows, list)
            or any(not isinstance(window, dict) for window in windows)
            for token, windows in grouped.items()
        ):
            return False
    manual_review_queue = report.get("manual_review_queue")
    if manual_review_queue is not None and (
        not isinstance(manual_review_queue, list)
        or any(not isinstance(item, dict) for item in manual_review_queue)
    ):
        return False
    return True


def environment_flag(name: str, default: bool = False) -> bool:
    """Read an explicit boolean flag without treating arbitrary text as truthy."""
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in TRUE_VALUES


def password_hash_is_configured(encoded: str) -> bool:
    """Reject empty, placeholder, malformed, or materially weakened verifiers."""
    try:
        algorithm, iterations_text, salt_text, digest_text = encoded.split("$", 3)
        iterations = int(iterations_text)
        salt = decode_part(salt_text)
        digest = decode_part(digest_text)
    except (ValueError, TypeError):
        return False
    return (
        algorithm == PASSWORD_ALGORITHM
        and iterations >= 100_000
        and len(salt) >= 16
        and len(digest) == hashlib.sha256().digest_size
    )


def encode_part(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def decode_part(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def hash_password(password: str, *, salt: bytes | None = None, iterations: int = PASSWORD_ITERATIONS) -> str:
    """Create a versioned PBKDF2 password verifier for environment storage."""
    if len(password) < 12:
        raise ValueError("Administrator password must contain at least 12 characters")
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"{PASSWORD_ALGORITHM}${iterations}${encode_part(salt)}${encode_part(digest)}"


def verify_password(password: str, encoded: str) -> bool:
    """Verify a password without storing or logging the plaintext."""
    try:
        algorithm, iterations_text, salt_text, digest_text = encoded.split("$", 3)
        if algorithm != PASSWORD_ALGORITHM:
            return False
        iterations = int(iterations_text)
        expected = decode_part(digest_text)
        actual = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            decode_part(salt_text),
            iterations,
        )
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(actual, expected)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AdminActionError(ValueError):
    """Stable service-layer validation error for narrow action endpoints."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class AdminJobBusyError(RuntimeError):
    """A collection job is already active in this process."""


class AdminWorkerStartError(RuntimeError):
    """The background worker could not be started."""


class AdminService:
    """Own authentication state and one-at-a-time background refresh jobs."""

    def __init__(
        self,
        *,
        username: str | None = None,
        password_hash: str | None = None,
        job_dir: Path | None = None,
        login_required: bool | None = None,
        enabled: bool | None = None,
        allow_open_local: bool | None = None,
        registry_path: Path | None = None,
        quality_report_path: Path | None = None,
        database_path: Path | None = None,
        data_dir: Path | None = None,
        collection_lock_path: Path | None = None,
    ) -> None:
        self.username = username if username is not None else os.environ.get("ADMIN_USERNAME", "admin")
        self.password_hash = password_hash if password_hash is not None else os.environ.get("ADMIN_PASSWORD_HASH", "")
        self.enabled = environment_flag("ADMIN_ENABLED") if enabled is None else enabled
        if login_required is None:
            login_required = os.environ.get("ADMIN_LOGIN_REQUIRED", "true").lower() not in {
                "0",
                "false",
                "no",
            }
        self.login_required = login_required
        self.allow_open_local = (
            environment_flag("ADMIN_ALLOW_OPEN_LOCAL")
            if allow_open_local is None
            else allow_open_local
        )
        configured_data_dir = os.environ.get("MARKET_DATA_DIR")
        self.data_dir = (
            Path(data_dir).expanduser().resolve()
            if data_dir is not None
            else Path(database_path).expanduser().resolve().parent
            if database_path is not None
            else Path(configured_data_dir).expanduser().resolve()
            if configured_data_dir
            else DEFAULT_DATA_DIR
        )
        configured_job_dir = os.environ.get("ADMIN_JOB_DIR")
        self.job_dir = job_dir or (
            Path(configured_job_dir).expanduser()
            if configured_job_dir
            else self.data_dir / "admin/jobs"
        )
        configured_registry_path = os.environ.get("TOKEN_REGISTRY_PATH")
        self.registry = TokenRegistry(
            registry_path
            or (
                Path(configured_registry_path).expanduser()
                if configured_registry_path
                else self.data_dir / "admin/token_registry.json"
            )
        )
        self.quality_report_path = (
            quality_report_path
            or self.data_dir / "quality/daily-latest.json"
        )
        self.database_path = (
            database_path
            or self.data_dir / "market_facts.sqlite3"
        )
        self.collection_lock_path = (
            collection_lock_path
            or self.data_dir / "collection/collection.lock"
        )
        self.sessions: dict[str, dict[str, Any]] = {}
        self.failed_logins: dict[str, list[float]] = {}
        self.jobs: dict[str, dict[str, Any]] = {}
        self.registry_error: str | None = None
        self.state_lock = threading.Lock()
        self.worker_lock = threading.Lock()
        self._load_jobs()

    @property
    def configured(self) -> bool:
        if not self.enabled or self.registry_error is not None:
            return False
        if self.login_required:
            return bool(self.username and password_hash_is_configured(self.password_hash))
        return self.allow_open_local

    @property
    def open_mode(self) -> bool:
        """Open mode is an explicit, local-development-only escape hatch."""
        return self.configured and not self.login_required

    @property
    def available(self) -> bool:
        """The HTTP surface stays absent until every required control is configured."""
        return self.configured

    def _load_jobs(self) -> None:
        if self.job_dir.exists():
            for path in self.job_dir.glob("*.json"):
                try:
                    job = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(job, dict) or "job_id" not in job:
                    continue
                if job.get("status") in {"queued", "running"}:
                    job["status"] = "interrupted"
                    job["finished_at"] = utc_now().isoformat()
                    job["stage"] = "interrupted"
                    job["error"] = (
                        "The administrator process stopped before the job completed."
                    )
                    job["error_code"] = "process_interrupted"
                    job["retryable"] = True
                    try:
                        self._save_job(job)
                    except OSError:
                        pass
                self.jobs[job["job_id"]] = job
        try:
            pending_records = self.registry.list_records(statuses={"pending"})
        except (OSError, TokenRegistryError) as error:
            self.registry_error = str(error)
            return
        for record in pending_records:
            last_job_id = record.get("last_job_id")
            job = self.jobs.get(last_job_id) if last_job_id else None
            if job and job.get("status") in {"queued", "running"}:
                continue
            record["status"] = "needs_review"
            try:
                self.registry.upsert(record)
            except (OSError, TokenRegistryError):
                continue

    def _clean_failures(self, client_ip: str) -> list[float]:
        cutoff = time.monotonic() - LOGIN_WINDOW_SECONDS
        failures = [stamp for stamp in self.failed_logins.get(client_ip, []) if stamp >= cutoff]
        self.failed_logins[client_ip] = failures
        return failures

    def login(self, client_ip: str, username: str, password: str) -> tuple[str, dict[str, Any]]:
        if not self.enabled:
            raise RuntimeError("Administrator surface is disabled")
        if not self.login_required:
            raise RuntimeError("Administrator login is disabled")
        if not self.configured:
            raise RuntimeError("Administrator authentication is not configured")
        with self.state_lock:
            if len(self._clean_failures(client_ip)) >= MAX_LOGIN_FAILURES:
                raise PermissionError("Too many login attempts; try again later")
        valid = hmac.compare_digest(username, self.username) and verify_password(password, self.password_hash)
        if not valid:
            with self.state_lock:
                self.failed_logins.setdefault(client_ip, []).append(time.monotonic())
            raise ValueError("Invalid username or password")

        session_token = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(24)
        expires_at = utc_now() + timedelta(seconds=SESSION_SECONDS)
        session = {
            "username": self.username,
            "csrf_token": csrf_token,
            "expires_at": expires_at,
        }
        with self.state_lock:
            self.failed_logins.pop(client_ip, None)
            self.sessions[session_token] = session
        return session_token, self.public_session(session)

    def get_session(self, session_token: str | None) -> dict[str, Any] | None:
        if not session_token:
            return None
        with self.state_lock:
            session = self.sessions.get(session_token)
            if not session:
                return None
            if session["expires_at"] <= utc_now():
                self.sessions.pop(session_token, None)
                return None
            return session

    def logout(self, session_token: str | None) -> None:
        if not session_token:
            return
        with self.state_lock:
            self.sessions.pop(session_token, None)

    def public_session(self, session: dict[str, Any] | None) -> dict[str, Any]:
        if not self.available:
            return {
                "authenticated": False,
                "configured": False,
                "enabled": self.enabled,
                "login_required": self.login_required,
            }
        if self.open_mode:
            return {
                "authenticated": True,
                "configured": True,
                "enabled": True,
                "login_required": False,
                "username": OPEN_ADMIN_USERNAME,
                "csrf_token": "",
                "expires_at": None,
            }
        if not session:
            return {
                "authenticated": False,
                "configured": self.configured,
                "enabled": True,
                "login_required": True,
            }
        return {
            "authenticated": True,
            "configured": self.configured,
            "enabled": True,
            "login_required": True,
            "username": session["username"],
            "csrf_token": session["csrf_token"],
            "expires_at": session["expires_at"].isoformat(),
        }

    def _static_token_rows(self) -> list[dict[str, str]]:
        with TOKEN_CONFIG_PATH.open("r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def _static_identity_keys(self) -> dict[str, str]:
        with TOKEN_CHAIN_CONFIG_PATH.open("r", newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        identities = {}
        for row in rows:
            try:
                key = token_identity_key(row.get("chain"), row.get("contract_address"))
            except TokenRegistryError:
                continue
            identities[key] = str(row.get("token_symbol") or "").strip().upper()
        return identities

    def configured_token_records(self) -> list[dict[str, Any]]:
        records = [
            {
                "token_symbol": row["token_symbol"].strip().upper(),
                "chain": row.get("chain", "").strip().lower(),
                "contract_address": row.get("contract_address", "").strip(),
                "origin": "static_catalog",
                "status": "active",
                "cex_mapping_status": "configured",
            }
            for row in self._static_token_rows()
        ]
        records.extend(
            {
                **record,
                "origin": "admin_runtime",
                "cex_mapping_status": record["cex_mapping"]["status"],
            }
            for record in self.registry.list_records()
        )
        return sorted(
            records,
            key=lambda record: (
                record["token_symbol"],
                record["origin"],
                record.get("chain", ""),
            ),
        )

    def configured_tokens(self) -> list[str]:
        symbols = {
            row["token_symbol"].strip().upper()
            for row in self._static_token_rows()
        }
        symbols.update(
            record["token_symbol"]
            for record in self.registry.list_records(statuses={"active"})
        )
        return sorted(symbols)

    def _read_quality_report(self, *, required: bool = False) -> dict[str, Any] | None:
        if not self.quality_report_path.exists():
            if required:
                raise ValueError("Daily quality report is unavailable")
            return None
        try:
            raw = _read_bounded_bytes(
                self.quality_report_path,
                MAX_QUALITY_REPORT_BYTES,
            )
        except ValueError as error:
            raise ValueError(
                "Daily quality report exceeds the operator size limit"
            ) from error
        except OSError as error:
            raise ValueError("Daily quality report is unreadable") from error
        try:
            report = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
            raise ValueError("Daily quality report is unreadable") from error
        if not _valid_quality_report_structure(report):
            raise ValueError("Daily quality report has an invalid contract")
        return report

    def resolve_token(
        self,
        chain: Any,
        contract_address: Any,
    ) -> dict[str, Any]:
        candidate = resolve_token_candidate(chain, contract_address)
        identity = candidate["identity"]
        identity_key = token_identity_key(
            identity["chain"],
            identity["contract_address"],
        )
        static_identities = self._static_identity_keys()
        static_symbols = {
            row["token_symbol"].strip().upper()
            for row in self._static_token_rows()
        }
        runtime_record = self.registry.get(
            identity["chain"],
            identity["contract_address"],
        )
        static_symbol = static_identities.get(identity_key)
        if static_symbol is not None and static_symbol != identity["token_symbol"]:
            raise TokenRegistryError(
                "identity_conflict",
                "This contract is configured with a different Token symbol",
                {
                    "configured_symbol": static_symbol,
                    "resolved_symbol": identity["token_symbol"],
                },
            )
        if static_symbol is None and identity["token_symbol"] in static_symbols:
            raise TokenRegistryError(
                "symbol_collision",
                "Resolved Token symbol is already assigned to another configured contract",
                {"token_symbol": identity["token_symbol"]},
            )
        for record in self.registry.list_records():
            if (
                record["token_symbol"] == identity["token_symbol"]
                and token_identity_key(
                    record["chain"],
                    record["contract_address"],
                )
                != identity_key
            ):
                raise TokenRegistryError(
                    "symbol_collision",
                    "Resolved Token symbol is already assigned to another runtime contract",
                    {"token_symbol": identity["token_symbol"]},
                )
        if runtime_record is not None and runtime_record["token_symbol"] != identity["token_symbol"]:
            raise TokenRegistryError(
                "identity_conflict",
                "This runtime contract is assigned to a different Token symbol",
            )
        origin = (
            "static_catalog"
            if static_symbol is not None
            else "admin_runtime"
            if runtime_record is not None
            else None
        )
        status = (
            "active"
            if static_symbol is not None
            else runtime_record.get("status")
            if runtime_record is not None
            else None
        )
        return {
            **candidate,
            "already_configured": origin is not None,
            "registration": {
                "origin": origin,
                "status": status,
                "cex_mapping_status": (
                    "configured"
                    if static_symbol is not None
                    else runtime_record["cex_mapping"]["status"]
                    if runtime_record is not None
                    else "requires_manual_review"
                ),
            },
        }

    def retryable_windows(
        self,
        *,
        required: bool = False,
    ) -> list[dict[str, Any]]:
        report = self._read_quality_report(required=required)
        if report is None:
            return []
        publication = report.get("publication") or {}
        if not isinstance(publication, dict):
            raise ValueError("Daily quality report has an invalid publication contract")
        issues_by_id = {
            str(issue.get("issue_id")): issue
            for issue in report.get("issues") or []
            if isinstance(issue, dict) and issue.get("issue_id")
        }
        windows = []
        for report_field, queue_type in (
            ("retry_windows_by_token", "latest_completed_day"),
            ("backfill_windows_by_token", "historical_gap"),
        ):
            grouped = report.get(report_field) or {}
            if not isinstance(grouped, dict):
                raise ValueError("Daily quality report has an invalid retry-window contract")
            if grouped:
                publication_status = publication.get("status")
                allowed_statuses = (
                    {"published_with_retry_queue"}
                    if queue_type == "latest_completed_day"
                    else {
                        "published_with_backfill",
                        "published_with_retry_queue",
                    }
                )
                if publication_status not in allowed_statuses:
                    raise ValueError(
                        "Daily quality report publication does not authorize "
                        "this retry queue"
                    )
                expected_import_run_id = publication.get("import_run_id")
                if not expected_import_run_id:
                    raise ValueError(
                        "Daily quality report has no publication identity"
                    )
                try:
                    database_import_run_id = self._database_import_run_id()
                except (OSError, sqlite3.Error, RuntimeError) as error:
                    raise ValueError(
                        "Published database identity is unavailable"
                    ) from error
                if database_import_run_id != expected_import_run_id:
                    raise ValueError(
                        "Daily quality report does not match the published database"
                    )
            for token, token_windows in grouped.items():
                for window in token_windows or []:
                    normalized_token = str(token).strip().upper()
                    window_start = str(window.get("start_date") or "")
                    window_end = str(window.get("end_date") or "")
                    window_market_ids = window.get("market_ids")
                    window_reason_codes = window.get("reason_codes")
                    if (
                        not normalized_token
                        or not isinstance(window_market_ids, list)
                        or not window_market_ids
                        or not isinstance(window_reason_codes, list)
                        or not window_reason_codes
                    ):
                        raise ValueError(
                            "Daily quality report has an invalid retry-window scope"
                        )
                    expected_observations = []
                    for issue_id in window.get("issue_ids") or []:
                        issue = issues_by_id.get(str(issue_id))
                        market = issue.get("market") if issue else None
                        expected_category = (
                            "d1_active_gap"
                            if queue_type == "latest_completed_day"
                            else "historical_gap"
                        )
                        market_id = (
                            str(market.get("market_id") or "")
                            if isinstance(market, dict)
                            else ""
                        )
                        issue_date = str(issue.get("date") or "") if issue else ""
                        if (
                            not isinstance(issue, dict)
                            or not isinstance(market, dict)
                            or issue.get("retryable") is not True
                            or issue.get("category") != expected_category
                            or str(
                                market.get("token_symbol") or ""
                            ).strip().upper()
                            != normalized_token
                            or market_id not in window_market_ids
                            or str(issue.get("reason_code") or "")
                            not in window_reason_codes
                            or not window_start
                            <= issue_date
                            <= window_end
                        ):
                            raise ValueError(
                                "Daily quality report retry window does not "
                                "match its referenced issues"
                            )
                        expected_observations.append(
                            {
                                "market_id": market_id,
                                "date": issue_date,
                            }
                        )
                    if not expected_observations:
                        raise ValueError(
                            "Daily quality report retry window has no audited issues"
                        )
                    windows.append(
                        {
                            **window,
                            "token_symbol": normalized_token,
                            "queue_type": queue_type,
                            "quality_dataset_snapshot_id": publication.get(
                                "dataset_snapshot_id"
                            ),
                            "quality_import_run_id": publication.get(
                                "import_run_id"
                            ),
                            "expected_observations": sorted(
                                expected_observations,
                                key=lambda item: (
                                    item["date"],
                                    item["market_id"],
                                ),
                            ),
                        }
                    )
        return sorted(
            windows,
            key=lambda window: (
                window["end_date"],
                window["token_symbol"],
                window["start_date"],
            ),
            reverse=True,
        )

    @staticmethod
    def _review_text(value: Any, *, maximum: int) -> str | None:
        if value is None:
            return None
        normalized = " ".join(str(value).split())
        if not normalized:
            return None
        return normalized[:maximum]

    @staticmethod
    def _review_source_hint(value: Any) -> str | None:
        if not isinstance(value, str) or len(value) > 2_000:
            return None
        try:
            parsed = urlsplit(value.strip())
        except ValueError:
            return None
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
        ):
            return None
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.query,
                "",
            )
        )[:1_000]

    def manual_review_items(self) -> list[dict[str, Any]]:
        """Return a small, read-only projection of non-retryable audit items."""
        reports: list[tuple[dict[str, Any], bool, str | None]] = []
        report = self._read_quality_report()
        if report is not None:
            reports.append((report, False, None))
        rejected = self._read_latest_rejected_quality_report()
        if rejected is not None:
            rejected_report, rejection_id = rejected
            reports.append((rejected_report, True, rejection_id))
        items = []
        for source_report, candidate_rejected, rejection_id in reports:
            queue = source_report.get("manual_review_queue") or []
            if not isinstance(queue, list):
                if candidate_rejected:
                    continue
                raise ValueError(
                    "Daily quality report has an invalid manual-review contract"
                )
            issues_by_id = {
                str(issue.get("issue_id")): issue
                for issue in source_report.get("issues") or []
                if isinstance(issue, dict) and issue.get("issue_id")
            }
            for raw_item in queue:
                if not isinstance(raw_item, dict):
                    continue
                category = self._review_text(
                    raw_item.get("category"),
                    maximum=80,
                )
                issue_id = self._review_text(
                    raw_item.get("issue_id"),
                    maximum=160,
                )
                source_issue = issues_by_id.get(issue_id or "")
                base_allowed_categories = (
                    {"hard_invalid"}
                    if candidate_rejected
                    else {"hard_invalid", "stale_market_unknown"}
                )
                lineage_matched_needs_review = bool(
                    not candidate_rejected
                    and isinstance(source_issue, dict)
                    and source_issue.get("category") == category
                    and source_issue.get("status") == "needs_review"
                    and source_issue.get("retryable") is False
                )
                if (
                    category not in base_allowed_categories
                    and not lineage_matched_needs_review
                ):
                    continue
                token_symbol = self._review_text(
                    raw_item.get("token_symbol"),
                    maximum=40,
                )
                market_id = self._review_text(
                    raw_item.get("market_id"),
                    maximum=240,
                )
                reason_code = self._review_text(
                    raw_item.get("reason_code"),
                    maximum=120,
                )
                review_date = self._review_text(
                    raw_item.get("date"),
                    maximum=10,
                )
                if not all(
                    (
                        issue_id,
                        token_symbol,
                        market_id,
                        reason_code,
                        review_date,
                    )
                ):
                    continue
                try:
                    date.fromisoformat(review_date)
                except ValueError:
                    continue
                source_hints = []
                for raw_hint in raw_item.get("source_url_hints") or []:
                    hint = self._review_source_hint(raw_hint)
                    if hint and hint not in source_hints:
                        source_hints.append(hint)
                    if len(source_hints) == 5:
                        break
                items.append(
                    {
                        "review_id": self._review_text(
                            raw_item.get("review_id"),
                            maximum=180,
                        )
                        or f"review-{issue_id}",
                        "review_status": "pending",
                        "issue_id": issue_id,
                        "token_symbol": token_symbol.upper(),
                        "market_id": market_id,
                        "date": review_date,
                        "category": category,
                        "reason_code": reason_code,
                        "reason_message": self._review_text(
                            source_issue.get("message"),
                            maximum=500,
                        )
                        if isinstance(source_issue, dict)
                        else None,
                        "source_url_hints": source_hints,
                        "retryable": False,
                        "action": "manual_primary_source_review",
                        "candidate_rejected": candidate_rejected,
                        "rejection_id": rejection_id,
                    }
                )
        return sorted(
            items,
            key=lambda item: (
                item["date"],
                item["token_symbol"],
                item["market_id"],
                item["reason_code"],
            ),
            reverse=True,
        )

    def _read_latest_rejected_quality_report(
        self,
    ) -> tuple[dict[str, Any], str] | None:
        """Read only an integrity-checked rejection bundle below data_dir."""
        rejected_root = self.data_dir / "quality/rejected"
        pointer_path = rejected_root / "latest.json"
        try:
            pointer = json.loads(
                _read_bounded_bytes(
                    pointer_path,
                    MAX_REJECTION_POINTER_BYTES,
                ).decode("utf-8")
            )
            if (
                not isinstance(pointer, dict)
                or pointer.get("schema")
                != "fact_quality_rejection_pointer/v1"
            ):
                return None
            rejection_id = str(pointer.get("rejection_id") or "")
            relative_report = Path(str(pointer.get("report") or ""))
            expected_hash = str(pointer.get("report_sha256") or "").lower()
            if (
                not rejection_id
                or relative_report.is_absolute()
                or relative_report.parts != (rejection_id, "report.json")
                or len(expected_hash) != 64
                or any(character not in "0123456789abcdef" for character in expected_hash)
            ):
                return None
            root_resolved = rejected_root.resolve()
            report_path = (rejected_root / relative_report).resolve()
            try:
                report_path.relative_to(root_resolved)
            except ValueError:
                return None
            report_bytes = _read_bounded_bytes(
                report_path,
                MAX_QUALITY_REPORT_BYTES,
            )
            if not hmac.compare_digest(
                hashlib.sha256(report_bytes).hexdigest(),
                expected_hash,
            ):
                return None
            report = json.loads(report_bytes.decode("utf-8"))
            rejection = report.get("rejection") if isinstance(report, dict) else None
            if (
                not isinstance(report, dict)
                or report.get("schema") != QUALITY_REPORT_SCHEMA
                or not isinstance(rejection, dict)
                or rejection.get("schema") != "fact_quality_rejection/v1"
                or rejection.get("rejection_id") != rejection_id
                or rejection.get("status") != "rejected_hard_invalid"
            ):
                return None
            return report, rejection_id
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return None

    def validate_job(self, payload: dict[str, Any], *, today: date | None = None) -> dict[str, str]:
        token = str(payload.get("token_symbol", "")).strip().upper()
        if token not in self.configured_tokens():
            raise ValueError("Token is not present in the active Token catalog")
        start = date.fromisoformat(str(payload.get("start_date", "")))
        end = date.fromisoformat(str(payload.get("end_date", "")))
        latest_complete_day = (today or utc_now().date()) - timedelta(days=1)
        if end != latest_complete_day:
            raise ValueError(f"Current collectors require end_date={latest_complete_day.isoformat()}")
        days = (end - start).days + 1
        if days < 1 or days > 180:
            raise ValueError("Refresh window must contain between 1 and 180 days")
        return {
            "token_symbol": token,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        }

    def validate_retry_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        token = str(payload.get("token_symbol", "")).strip().upper()
        start = str(payload.get("start_date", "")).strip()
        end = str(payload.get("end_date", "")).strip()
        queue_type = str(payload.get("queue_type", "")).strip()
        candidates = [
            window
            for window in self.retryable_windows(required=True)
            if window["token_symbol"] == token
            and window["start_date"] == start
            and window["end_date"] == end
        ]
        if queue_type:
            candidates = [
                window
                for window in candidates
                if window["queue_type"] == queue_type
            ]
        match = next(
            iter(candidates),
            None,
        )
        if match is None or len(candidates) != 1:
            raise AdminActionError(
                "retry_window_not_approved",
                "Retry window is not present in the current quality report",
            )
        try:
            start_day = date.fromisoformat(str(match["start_date"]))
            end_day = date.fromisoformat(str(match["end_date"]))
        except (KeyError, TypeError, ValueError) as error:
            raise AdminActionError(
                "quality_retry_unavailable",
                "Retry window has an invalid date contract",
                retryable=True,
            ) from error
        if (
            start_day.isoformat() != match["start_date"]
            or end_day.isoformat() != match["end_date"]
            or start_day > end_day
            or (end_day - start_day).days >= 180
        ):
            raise AdminActionError(
                "quality_retry_unavailable",
                "Retry window has an invalid date contract",
                retryable=True,
            )
        market_ids = match.get("market_ids")
        if (
            not isinstance(market_ids, list)
            or not market_ids
            or any(
                not isinstance(market_id, str) or not market_id
                for market_id in market_ids
            )
        ):
            raise AdminActionError(
                "quality_retry_unavailable",
                "Retry window has an invalid market scope",
                retryable=True,
            )
        derived_market_types = {
            market_id.split(":", 1)[0]
            for market_id in market_ids
            if ":" in market_id
        }
        configured_market_types = match.get("market_types")
        if configured_market_types is None:
            configured_market_types = sorted(derived_market_types)
        if (
            not isinstance(configured_market_types, list)
            or not configured_market_types
            or any(
                not isinstance(market_type, str)
                for market_type in configured_market_types
            )
            or set(configured_market_types) != derived_market_types
            or not derived_market_types.issubset({"cex", "dex"})
        ):
            raise AdminActionError(
                "quality_retry_unavailable",
                "Retry window has an invalid market-type scope",
                retryable=True,
            )
        expected_observations = match.get("expected_observations")
        if (
            not isinstance(expected_observations, list)
            or not expected_observations
        ):
            raise AdminActionError(
                "quality_retry_unavailable",
                "Retry window has no exact audited observations",
                retryable=True,
            )
        for observation in expected_observations:
            if not isinstance(observation, dict):
                raise AdminActionError(
                    "quality_retry_unavailable",
                    "Retry window has an invalid observation scope",
                    retryable=True,
                )
            market_id = observation.get("market_id")
            observation_date = observation.get("date")
            try:
                parsed_observation_date = date.fromisoformat(
                    str(observation_date)
                )
            except (TypeError, ValueError):
                parsed_observation_date = None
            if (
                market_id not in market_ids
                or not isinstance(observation_date, str)
                or parsed_observation_date is None
                or parsed_observation_date.isoformat() != observation_date
                or not match["start_date"]
                <= observation_date
                <= match["end_date"]
            ):
                raise AdminActionError(
                    "quality_retry_unavailable",
                    "Retry window has an invalid observation scope",
                    retryable=True,
                )
        match = {
            **match,
            "market_types": sorted(derived_market_types),
        }
        if (
            not match.get("quality_dataset_snapshot_id")
            or not match.get("quality_import_run_id")
        ):
            raise AdminActionError(
                "quality_retry_unavailable",
                "Retry window is missing its published quality-report identity",
                retryable=True,
            )
        return dict(match)

    @staticmethod
    def _retry_authorization_fingerprint(
        authorization: dict[str, Any],
    ) -> tuple[Any, ...]:
        """Canonicalize only the trusted fields that authorize retry scope."""
        expected_observations = sorted(
            (
                str(item.get("market_id") or ""),
                str(item.get("date") or ""),
            )
            for item in authorization.get("expected_observations") or []
            if isinstance(item, dict)
        )
        return (
            str(authorization.get("token_symbol") or ""),
            str(authorization.get("start_date") or ""),
            str(authorization.get("end_date") or ""),
            str(authorization.get("queue_type") or ""),
            str(authorization.get("quality_dataset_snapshot_id") or ""),
            str(authorization.get("quality_import_run_id") or ""),
            tuple(
                sorted(
                    str(value)
                    for value in authorization.get("market_types") or []
                )
            ),
            tuple(
                sorted(
                    str(value)
                    for value in authorization.get("market_ids") or []
                )
            ),
            tuple(
                sorted(
                    str(value)
                    for value in authorization.get("reason_codes") or []
                )
            ),
            tuple(
                sorted(
                    str(value)
                    for value in authorization.get("issue_ids") or []
                )
            ),
            tuple(expected_observations),
        )

    def revalidate_retry_authorization(
        self,
        job: dict[str, Any],
    ) -> None:
        """Require the queued retry authorization to remain byte-scope equivalent."""
        current = self.validate_retry_job(job)
        if self._retry_authorization_fingerprint(
            current
        ) != self._retry_authorization_fingerprint(job):
            raise AdminActionError(
                "retry_authorization_changed",
                "The quality publication changed after this retry was queued",
                retryable=True,
            )

    def create_job(self, payload: dict[str, Any], username: str) -> dict[str, Any]:
        job_type = str(payload.get("job_type") or "refresh").strip().lower()
        if job_type == "refresh":
            request: dict[str, Any] = self.validate_job(payload)
        elif job_type == "retry_failed":
            request = self.validate_retry_job(payload)
        else:
            raise ValueError("job_type must be refresh or retry_failed")
        job_id = secrets.token_hex(16)
        job = {
            "job_id": job_id,
            **request,
            "job_type": job_type,
            "requested_by": username,
            "status": "queued",
            "stage": "queued",
            "created_at": utc_now().isoformat(),
            "started_at": None,
            "finished_at": None,
            "error": None,
            "error_code": None,
            "retryable": False,
            "publication_committed": False,
            "result": None,
        }
        with self.state_lock:
            if any(existing.get("status") in {"queued", "running"} for existing in self.jobs.values()):
                raise AdminJobBusyError(
                    "Another refresh job is already queued or running"
                )
            self._save_job(job)
            self.jobs[job_id] = job
            response = dict(job)
        self._start_job_thread(job_id)
        return response

    def create_onboarding_job(
        self,
        payload: dict[str, Any],
        username: str,
    ) -> dict[str, Any]:
        history_days = int(payload.get("history_days") or 180)
        if history_days < 1 or history_days > 180:
            raise ValueError("history_days must contain between 1 and 180 days")
        candidate = self.resolve_token(
            payload.get("chain"),
            payload.get("contract_address"),
        )
        symbol = candidate["identity"]["token_symbol"]
        expected_symbol = str(payload.get("expected_token_symbol") or "").strip().upper()
        if not expected_symbol or expected_symbol != symbol:
            raise TokenOnboardingError(
                "identity_changed",
                "Resolved Token symbol no longer matches the confirmed preview",
            )
        job_id = secrets.token_hex(16)
        completed_day = utc_now().date() - timedelta(days=1)
        start_day = completed_day - timedelta(days=history_days - 1)
        job = {
            "job_id": job_id,
            "job_type": "token_onboarding",
            "token_symbol": symbol,
            "chain": candidate["identity"]["chain"],
            "contract_address": candidate["identity"]["contract_address"],
            "start_date": start_day.isoformat(),
            "end_date": completed_day.isoformat(),
            "history_days": history_days,
            "requested_by": username,
            "status": "queued",
            "stage": "resolve_identity",
            "created_at": utc_now().isoformat(),
            "started_at": None,
            "finished_at": None,
            "error": None,
            "error_code": None,
            "retryable": False,
            "publication_committed": False,
            "result": None,
        }
        registration = candidate["registration"]
        if registration["origin"] in {"static_catalog", "admin_runtime"} and registration["status"] == "active":
            job.update(
                {
                    "status": "succeeded",
                    "stage": "complete",
                    "finished_at": utc_now().isoformat(),
                    "publication_committed": True,
                    "result": {
                        "already_configured": True,
                        "cex": registration["cex_mapping_status"],
                    },
                }
            )
            return dict(job)
        record = build_registry_record(
            candidate,
            created_by=username,
            status="pending",
            job_id=job_id,
        )
        with self.state_lock:
            if any(existing.get("status") in {"queued", "running"} for existing in self.jobs.values()):
                raise AdminJobBusyError(
                    "Another refresh job is already queued or running"
                )
            self._save_job(job)
            try:
                self.registry.upsert(
                    record,
                    reserved_symbols={
                        row["token_symbol"]
                        for row in self._static_token_rows()
                    },
                )
            except (OSError, TokenRegistryError):
                try:
                    (self.job_dir / f"{job_id}.json").unlink(missing_ok=True)
                except OSError:
                    pass
                raise
            self.jobs[job_id] = job
            response = dict(job)
        self._start_job_thread(job_id)
        return response

    def _save_job(self, job: dict[str, Any]) -> None:
        self.job_dir.mkdir(parents=True, exist_ok=True)
        target = self.job_dir / f"{job['job_id']}.json"
        temporary = target.with_suffix(".tmp")
        try:
            temporary.write_text(
                json.dumps(job, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)

    def _start_job_thread(self, job_id: str) -> None:
        try:
            threading.Thread(
                target=self._run_job,
                args=(job_id,),
                daemon=True,
            ).start()
        except Exception as error:
            self._mark_pending_onboarding_needs_review(job_id)
            self._fail_job(
                job_id,
                stage="start_worker",
                error_code="worker_start_failed",
                message="The refresh worker could not be started.",
                retryable=True,
            )
            raise AdminWorkerStartError(
                "The refresh worker could not be started"
            ) from error

    def _set_job(self, job_id: str, **updates: Any) -> dict[str, Any]:
        with self.state_lock:
            job = self.jobs[job_id]
            job.update(updates)
            self._save_job(job)
            return dict(job)

    def _runtime_record_for_symbol(self, token_symbol: str) -> dict[str, Any] | None:
        return next(
            (
                record
                for record in self.registry.list_records()
                if record["token_symbol"] == token_symbol
            ),
            None,
        )

    def _daily_pipeline_command(
        self,
        job: dict[str, Any],
        *,
        dex_only: bool,
    ) -> list[str]:
        command = [
            sys.executable,
            str(PROJECT_ROOT / "scripts/run_fact_pipeline.py"),
        ]
        retry_market_types = (
            set(job.get("market_types") or [])
            if job.get("job_type") == "retry_failed"
            else set()
        )
        if retry_market_types == {"cex"}:
            command.append("--cex-only")
        elif retry_market_types == {"dex"}:
            command.append("--dex-only")
        elif retry_market_types == {"cex", "dex"}:
            pass
        elif retry_market_types:
            raise ValueError("Retry job has an invalid market-type scope")
        elif dex_only:
            command.append("--dex-only")
        command.extend(
            [
                "--tokens",
                job["token_symbol"],
                "--start",
                job["start_date"],
                "--end",
                job["end_date"],
                "--append",
                "--publish-local",
                "--data-dir",
                str(self.data_dir),
            ]
        )
        return command

    def _run_command(
        self,
        command: list[str],
        log_path: Path,
        *,
        stage: str,
        extra_environment: dict[str, str] | None = None,
        timeout_seconds: int = DEFAULT_COMMAND_TIMEOUT_SECONDS,
    ) -> None:
        command_environment = os.environ.copy()
        command_environment.pop("TOKEN_ONBOARDING_JOB_ID", None)
        command_environment["TOKEN_REGISTRY_PATH"] = str(self.registry.path)
        if extra_environment:
            command_environment.update(extra_environment)
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"\n[{utc_now().isoformat()}] {stage}\n")
            log.flush()
            subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=timeout_seconds,
                env=command_environment,
            )

    def _fail_job(
        self,
        job_id: str,
        *,
        stage: str,
        error_code: str,
        message: str,
        retryable: bool,
        publication_committed: bool = False,
        status: str = "failed",
        result: dict[str, Any] | None = None,
    ) -> None:
        self._set_job(
            job_id,
            status=status,
            stage=stage,
            finished_at=utc_now().isoformat(),
            error=message,
            error_code=error_code,
            retryable=retryable,
            publication_committed=publication_committed,
            result=result,
        )

    def _run_refresh_job(
        self,
        job_id: str,
        job: dict[str, Any],
        log_path: Path,
    ) -> None:
        runtime_record = self._runtime_record_for_symbol(job["token_symbol"])
        dex_only = bool(
            runtime_record
            and runtime_record["cex_mapping"]["status"] != "approved"
        )
        baseline = self._refresh_publication_baseline()
        try:
            self._run_command(
                self._daily_pipeline_command(job, dex_only=dex_only),
                log_path,
                stage="collect_daily_facts",
                timeout_seconds=(
                    PUBLIC_COMMAND_TIMEOUT_SECONDS
                    if job.get("requested_by")
                    == PUBLIC_QUALITY_RETRY_REQUESTER
                    else DEFAULT_COMMAND_TIMEOUT_SECONDS
                ),
            )
        except (OSError, subprocess.SubprocessError):
            rejected_outcome = self._new_rejected_candidate_outcome(
                job,
                baseline,
            )
            if rejected_outcome is not None:
                self._fail_job(
                    job_id,
                    stage="verify_publication",
                    error_code=rejected_outcome["error_code"],
                    message=rejected_outcome["message"],
                    retryable=False,
                    publication_committed=False,
                    status="partial",
                    result=rejected_outcome["result"],
                )
                return
            self._fail_job(
                job_id,
                stage="collect_daily_facts",
                error_code="collection_failed",
                message="Daily refresh failed. Review the server-side job log.",
                retryable=True,
            )
            return
        retry_verification_error = None
        retry_result: dict[str, Any] = {}
        if job.get("job_type") == "retry_failed":
            retry_verification_error, retry_result = (
                self._retry_resolution_evidence(job)
            )
        if retry_verification_error:
            self._fail_job(
                job_id,
                stage="verify_retry",
                error_code="retry_not_resolved",
                message=retry_verification_error,
                retryable=True,
                publication_committed=True,
                status="partial",
                result={
                    "daily": "published_but_retry_unresolved",
                    "quality_report": "quality/daily-latest.json",
                    **retry_result,
                },
            )
            return
        if job.get("job_type") != "retry_failed":
            postcheck = self._verify_refresh_publication(job, baseline)
            if postcheck["error_code"]:
                self._fail_job(
                    job_id,
                    stage="verify_publication",
                    error_code=postcheck["error_code"],
                    message=postcheck["message"],
                    retryable=postcheck["retryable"],
                    publication_committed=postcheck[
                        "publication_committed"
                    ],
                    status="partial",
                    result=postcheck["result"],
                )
                return
            refresh_result = postcheck["result"]
        else:
            refresh_result = retry_result
        self._set_job(
            job_id,
            status="succeeded",
            stage="complete",
            finished_at=utc_now().isoformat(),
            error=None,
            error_code=None,
            retryable=False,
            publication_committed=True,
            result={
                "daily": "published",
                "collection_scope": (
                    "cex_only"
                    if set(job.get("market_types") or []) == {"cex"}
                    else "dex_only"
                    if (
                        set(job.get("market_types") or []) == {"dex"}
                        or dex_only
                    )
                    else "cex_and_dex"
                ),
                "quality_report": "quality/daily-latest.json",
                **refresh_result,
            },
        )

    def _refresh_publication_baseline(self) -> dict[str, str | None]:
        quality_import_run_id = None
        try:
            report = self._read_quality_report()
            publication = report.get("publication") if report else None
            if isinstance(publication, dict) and publication.get("import_run_id"):
                quality_import_run_id = str(publication["import_run_id"])
        except ValueError:
            pass
        database_import_run_id = None
        try:
            database_import_run_id = self._database_import_run_id()
        except (OSError, sqlite3.Error, RuntimeError):
            pass
        rejected = self._read_latest_rejected_quality_report()
        return {
            "quality_import_run_id": quality_import_run_id,
            "database_import_run_id": database_import_run_id,
            "rejection_id": rejected[1] if rejected else None,
        }

    @staticmethod
    def _quality_issues_for_job(
        report: dict[str, Any],
        job: dict[str, Any],
    ) -> list[dict[str, Any]]:
        matching = []
        for issue in report.get("issues") or []:
            if not isinstance(issue, dict):
                continue
            market = issue.get("market")
            market_token = (
                market.get("token_symbol")
                if isinstance(market, dict)
                else issue.get("token_symbol")
            )
            issue_date = str(issue.get("date") or "")
            if (
                str(market_token or "").strip().upper()
                == job["token_symbol"]
                and job["start_date"] <= issue_date <= job["end_date"]
            ):
                matching.append(issue)
        return matching

    @staticmethod
    def _quality_outcome_summary(
        issues: list[dict[str, Any]],
    ) -> dict[str, Any]:
        structural_reasons = {
            "unsupported",
            "not_supported",
            "source_unsupported",
            "not_listed",
            "no_candle",
            "no_candles",
        }
        failure_issues = []
        structural_issues = []
        review_issues = []
        for issue in issues:
            category = str(issue.get("category") or "unknown")
            status = str(issue.get("status") or "unknown")
            reason = (
                str(issue.get("reason_code") or "unknown")
                .strip()
                .lower()
                .replace("-", "_")
            )
            if reason in structural_reasons:
                structural_issues.append(issue)
            elif (
                category == "hard_invalid"
                or status == "collection_failed"
                or issue.get("retryable") is True
            ):
                failure_issues.append(issue)
            elif status == "needs_review":
                review_issues.append(issue)
        return {
            "issue_count": len(issues),
            "blocking_issue_count": len(failure_issues),
            "structural_non_error_count": len(structural_issues),
            "review_required_count": len(review_issues),
            "reason_counts": dict(
                sorted(
                    Counter(
                        str(issue.get("reason_code") or "unknown")
                        for issue in issues
                    ).items()
                )
            ),
            "category_counts": dict(
                sorted(
                    Counter(
                        str(issue.get("category") or "unknown")
                        for issue in issues
                    ).items()
                )
            ),
            "status_counts": dict(
                sorted(
                    Counter(
                        str(issue.get("status") or "unknown")
                        for issue in issues
                    ).items()
                )
            ),
            "has_retryable_failure": any(
                issue.get("retryable") is True
                for issue in failure_issues
            ),
        }

    def _verify_refresh_publication(
        self,
        job: dict[str, Any],
        baseline: dict[str, str | None],
    ) -> dict[str, Any]:
        rejected_outcome = self._new_rejected_candidate_outcome(
            job,
            baseline,
        )
        if rejected_outcome is not None:
            return rejected_outcome
        try:
            report = self._read_quality_report(required=True)
        except ValueError:
            return {
                "error_code": "refresh_quality_unreadable",
                "message": (
                    "The collector finished, but no readable quality report "
                    "could verify the publication."
                ),
                "retryable": True,
                "publication_committed": False,
                "result": {"daily": "publication_unverified"},
            }
        assert report is not None
        publication = report.get("publication")
        current_quality_import_id = (
            str(publication.get("import_run_id"))
            if isinstance(publication, dict)
            and publication.get("import_run_id")
            else None
        )
        issues = self._quality_issues_for_job(report, job)
        quality_outcomes = self._quality_outcome_summary(issues)
        try:
            current_database_import_id, observed = (
                self._published_market_dates(
                    job["token_symbol"],
                    job["start_date"],
                    job["end_date"],
                )
            )
        except (OSError, sqlite3.Error, RuntimeError):
            return {
                "error_code": "refresh_database_unreadable",
                "message": (
                    "The collector finished, but the published database could "
                    "not verify the requested Token and date window."
                ),
                "retryable": True,
                "publication_committed": False,
                "result": {
                    "daily": "publication_unverified",
                    "quality_outcomes": quality_outcomes,
                },
            }
        result = {
            "observed_market_date_count": len(observed),
            "publication_import_run_id": current_quality_import_id,
            "quality_outcomes": quality_outcomes,
        }
        if (
            not current_quality_import_id
            or current_database_import_id != current_quality_import_id
        ):
            return {
                "error_code": "refresh_publication_mismatch",
                "message": (
                    "The collector finished, but the quality report and "
                    "database do not identify one consistent publication."
                ),
                "retryable": True,
                "publication_committed": False,
                "result": {"daily": "publication_unverified", **result},
            }
        prior_ids = {
            identity
            for identity in (
                baseline.get("quality_import_run_id"),
                baseline.get("database_import_run_id"),
            )
            if identity
        }
        if current_quality_import_id in prior_ids:
            return {
                "error_code": "refresh_publication_unchanged",
                "message": (
                    "The collector finished, but no new publication identity "
                    "was committed."
                ),
                "retryable": True,
                "publication_committed": False,
                "result": {"daily": "publication_unchanged", **result},
            }
        if not observed:
            return {
                "error_code": "refresh_no_observations",
                "message": (
                    "A new publication was committed, but it contains no "
                    "successful row for the requested Token and date window."
                ),
                "retryable": bool(
                    quality_outcomes["has_retryable_failure"]
                ),
                "publication_committed": True,
                "result": {"daily": "published_without_observations", **result},
            }
        if quality_outcomes["blocking_issue_count"]:
            return {
                "error_code": "refresh_quality_incomplete",
                "message": (
                    "A new publication was committed, but collection failures, "
                    "retryable gaps, or hard-invalid facts remain in the "
                    "requested Token and date window."
                ),
                "retryable": bool(
                    quality_outcomes["has_retryable_failure"]
                ),
                "publication_committed": True,
                "result": {"daily": "published_with_failures", **result},
            }
        return {
            "error_code": None,
            "message": None,
            "retryable": False,
            "publication_committed": True,
            "result": result,
        }

    def _new_rejected_candidate_outcome(
        self,
        job: dict[str, Any],
        baseline: dict[str, str | None],
    ) -> dict[str, Any] | None:
        rejected = self._read_latest_rejected_quality_report()
        if (
            rejected is not None
            and rejected[1] != baseline.get("rejection_id")
        ):
            rejected_issues = self._quality_issues_for_job(
                rejected[0],
                job,
            )
            rejected_summary = self._quality_outcome_summary(
                rejected_issues
            )
            if rejected_summary["category_counts"].get("hard_invalid", 0):
                return {
                    "error_code": "refresh_candidate_rejected",
                    "message": (
                        "The collector finished, but the candidate publication "
                        "was rejected for hard-invalid facts."
                    ),
                    "retryable": False,
                    "publication_committed": False,
                    "result": {
                        "daily": "candidate_rejected",
                        "rejection_id": rejected[1],
                        "quality_outcomes": rejected_summary,
                    },
                }
        return None

    def _verify_retry_resolution(self, job: dict[str, Any]) -> str | None:
        return self._retry_resolution_evidence(job)[0]

    def _retry_resolution_evidence(
        self,
        job: dict[str, Any],
    ) -> tuple[str | None, dict[str, Any]]:
        try:
            report = self._read_quality_report(required=True)
        except ValueError:
            return (
                "The collector completed, but no readable post-refresh quality "
                "report was published.",
                {
                    "observed_count": 0,
                    "confirmed_absence_count": 0,
                    "unresolved_count": len(
                        job.get("expected_observations") or []
                    ),
                },
            )
        assert report is not None
        publication = report.get("publication") or {}
        current_import_run_id = (
            publication.get("import_run_id")
            if isinstance(publication, dict)
            else None
        )
        if (
            not current_import_run_id
            or current_import_run_id == job.get("quality_import_run_id")
        ):
            return (
                "The collector completed, but the quality report was not "
                "re-published for this retry.",
                {
                    "observed_count": 0,
                    "confirmed_absence_count": 0,
                    "unresolved_count": len(
                        job.get("expected_observations") or []
                    ),
                },
            )

        try:
            database_import_run_id, observed = self._published_market_dates(
                job["token_symbol"],
                job["start_date"],
                job["end_date"],
            )
        except (OSError, sqlite3.Error, RuntimeError):
            return (
                "The collector completed, but the newly published database "
                "could not verify the requested market/date rows.",
                {
                    "observed_count": 0,
                    "confirmed_absence_count": 0,
                    "unresolved_count": len(
                        job.get("expected_observations") or []
                    ),
                },
            )
        if database_import_run_id != current_import_run_id:
            return (
                "The collector completed, but the quality report and database "
                "do not identify the same publication.",
                {
                    "observed_count": 0,
                    "confirmed_absence_count": 0,
                    "unresolved_count": len(
                        job.get("expected_observations") or []
                    ),
                },
            )
        expected = {
            (str(item.get("market_id") or ""), str(item.get("date") or ""))
            for item in job.get("expected_observations") or []
        }
        if not expected:
            return (
                "The retry job did not retain its exact audited market/date "
                "observations.",
                {
                    "observed_count": 0,
                    "confirmed_absence_count": 0,
                    "unresolved_count": 0,
                },
            )
        explicit_absence_reasons = {
            "unsupported",
            "not_supported",
            "source_unsupported",
            "not_listed",
            "source_no_observation",
            "source_range_unavailable",
            "no_candle",
            "no_candles",
        }
        confirmed_absences: set[tuple[str, str]] = set()
        for issue in report.get("issues") or []:
            if not isinstance(issue, dict):
                continue
            market = issue.get("market")
            if not isinstance(market, dict):
                continue
            pair = (
                str(market.get("market_id") or ""),
                str(issue.get("date") or ""),
            )
            reason = (
                str(issue.get("reason_code") or "")
                .strip()
                .lower()
                .replace("-", "_")
            )
            if (
                pair in expected
                and issue.get("retryable") is False
                and str(issue.get("status") or "") != "collection_failed"
                and reason in explicit_absence_reasons
            ):
                confirmed_absences.add(pair)
        observed_expected = expected.intersection(observed)
        confirmed_absences.difference_update(observed_expected)
        resolved = observed_expected.union(confirmed_absences)
        unresolved = expected - resolved
        result = {
            "observed_count": len(observed_expected),
            "confirmed_absence_count": len(confirmed_absences),
            "unresolved_count": len(unresolved),
            "confirmed_absence_reason_counts": dict(
                sorted(
                    Counter(
                        str(issue.get("reason_code") or "unknown")
                        for issue in report.get("issues") or []
                        if isinstance(issue, dict)
                        and isinstance(issue.get("market"), dict)
                        and (
                            str(issue["market"].get("market_id") or ""),
                            str(issue.get("date") or ""),
                        )
                        in confirmed_absences
                    ).items()
                )
            ),
        }
        if unresolved:
            return (
                "The collector completed, but one or more requested "
                "market/date facts remain neither observed nor confirmed as "
                "a non-retryable source absence.",
                result,
            )
        return None, result

    def _published_market_dates(
        self,
        token_symbol: str,
        start_date: str,
        end_date: str,
    ) -> tuple[str, set[tuple[str, str]]]:
        if not self.database_path.exists():
            raise RuntimeError("Published market database is unavailable")
        connection = sqlite3.connect(
            f"{self.database_path.resolve().as_uri()}?mode=ro",
            uri=True,
        )
        try:
            state = connection.execute(
                "SELECT import_run_id FROM dataset_state WHERE singleton_id = 1"
            ).fetchone()
            if not state or not state[0]:
                raise RuntimeError("Published database has no import identity")
            observed: set[tuple[str, str]] = set()
            for day, exchange, cex_symbol in connection.execute(
                """
                SELECT date, exchange, cex_symbol
                FROM cex_market_daily
                WHERE token_symbol = ? AND date BETWEEN ? AND ?
                """,
                (token_symbol, start_date, end_date),
            ):
                market = cex_market(
                    {
                        "token_symbol": token_symbol,
                        "exchange": exchange,
                        "cex_symbol": cex_symbol,
                    }
                )
                observed.add(
                    (str(market["market_id"]), str(day))
                )
            for day, chain, dex, pool_address in connection.execute(
                """
                SELECT date, chain, dex, pool_address
                FROM dex_pool_daily
                WHERE token_symbol = ? AND date BETWEEN ? AND ?
                """,
                (token_symbol, start_date, end_date),
            ):
                market = dex_market(
                    {
                        "token_symbol": token_symbol,
                        "chain": chain,
                        "dex": dex,
                        "pool_address": pool_address,
                    }
                )
                observed.add(
                    (str(market["market_id"]), str(day))
                )
            return str(state[0]), observed
        finally:
            connection.close()

    def _database_import_run_id(self) -> str:
        if not self.database_path.exists():
            raise RuntimeError("Published market database is unavailable")
        connection = sqlite3.connect(
            f"{self.database_path.resolve().as_uri()}?mode=ro",
            uri=True,
        )
        try:
            state = connection.execute(
                "SELECT import_run_id FROM dataset_state WHERE singleton_id = 1"
            ).fetchone()
            if not state or not state[0]:
                raise RuntimeError("Published database has no import identity")
            return str(state[0])
        finally:
            connection.close()

    def _mark_pending_onboarding_needs_review(self, job_id: str) -> None:
        job = self.jobs.get(job_id)
        if not job or job.get("job_type") != "token_onboarding":
            return
        try:
            record = self._runtime_record_for_symbol(job["token_symbol"])
            if record and record.get("status") == "pending":
                self._update_runtime_status(job, "needs_review")
        except (OSError, TokenRegistryError):
            return

    def _published_dex_row_count(self, token_symbol: str) -> int:
        if not self.database_path.exists():
            raise RuntimeError("Published market database is unavailable")
        connection = sqlite3.connect(
            f"{self.database_path.resolve().as_uri()}?mode=ro",
            uri=True,
        )
        try:
            row = connection.execute(
                "SELECT COUNT(*) FROM dex_pool_daily WHERE token_symbol = ?",
                (token_symbol,),
            ).fetchone()
        finally:
            connection.close()
        return int(row[0]) if row else 0

    def _update_runtime_status(
        self,
        job: dict[str, Any],
        status: str,
        *,
        activated: bool = False,
    ) -> None:
        record = self.registry.get(job["chain"], job["contract_address"])
        if record is None:
            raise TokenRegistryError(
                "registry_record_missing",
                "Runtime Token registry record disappeared during collection",
            )
        record["status"] = status
        record["last_job_id"] = job["job_id"]
        if activated:
            record["activated_at"] = utc_now_text()
        self.registry.upsert(record)

    def _run_onboarding_job(
        self,
        job_id: str,
        job: dict[str, Any],
        log_path: Path,
    ) -> None:
        result = {
            "dex_daily": "pending",
            "tvl": "pending",
            "dex_depth": "pending",
            "cex": "requires_manual_mapping",
        }
        processed_dir = (
            PROJECT_ROOT / "data/processed"
            if self.data_dir == DEFAULT_DATA_DIR.resolve()
            else self.data_dir.parent / f".{self.data_dir.name}-processed"
        )
        raw_root = self.data_dir / "raw"
        try:
            self._set_job(job_id, stage="collect_dex_daily")
            self._run_command(
                self._daily_pipeline_command(job, dex_only=True),
                log_path,
                stage="collect_dex_daily",
                extra_environment={
                    "TOKEN_ONBOARDING_JOB_ID": job["job_id"],
                },
                timeout_seconds=(
                    PUBLIC_COMMAND_TIMEOUT_SECONDS
                    if job.get("requested_by")
                    == PUBLIC_ADD_TOKEN_REQUESTER
                    else DEFAULT_COMMAND_TIMEOUT_SECONDS
                ),
            )
        except (OSError, subprocess.SubprocessError):
            try:
                self._update_runtime_status(job, "failed")
            except (OSError, TokenRegistryError):
                pass
            self._fail_job(
                job_id,
                stage="collect_dex_daily",
                error_code="token_onboarding_daily_failed",
                message="DEX daily onboarding failed; the Token was not activated.",
                retryable=True,
                result=result,
            )
            return

        result["dex_daily"] = "published_unverified"
        try:
            self._set_job(
                job_id,
                stage="verify_daily_publication",
                publication_committed=True,
                result=dict(result),
            )
            if self._published_dex_row_count(job["token_symbol"]) < 1:
                raise RuntimeError("Published database contains no DEX daily rows")
        except (OSError, sqlite3.Error, RuntimeError):
            try:
                self._update_runtime_status(job, "needs_review")
            except (OSError, TokenRegistryError):
                pass
            self._fail_job(
                job_id,
                stage="verify_daily_publication",
                error_code="token_onboarding_verification_failed",
                message=(
                    "DEX daily collection returned success, but the published "
                    "Token rows could not be verified. Operator reconciliation "
                    "is required before activation."
                ),
                retryable=True,
                publication_committed=True,
                status="partial",
                result=result,
            )
            return

        result["dex_daily"] = "observed"
        self._set_job(
            job_id,
            stage="activate_registry",
            publication_committed=True,
            result=dict(result),
        )
        try:
            self._update_runtime_status(job, "active", activated=True)
        except (OSError, TokenRegistryError):
            self._fail_job(
                job_id,
                stage="activate_registry",
                error_code="registry_activation_failed",
                message=(
                    "DEX daily facts were published, but the runtime Token "
                    "registry could not be activated. Operator reconciliation is required."
                ),
                retryable=True,
                publication_committed=True,
                status="partial",
                result=result,
            )
            return

        if job.get("requested_by") == PUBLIC_ADD_TOKEN_REQUESTER:
            result["tvl"] = "deferred_to_scheduled_collection"
            result["dex_depth"] = "deferred_to_scheduled_collection"
            self._set_job(
                job_id,
                status="succeeded",
                stage="complete",
                finished_at=utc_now().isoformat(),
                error=None,
                error_code=None,
                retryable=False,
                publication_committed=True,
                result=result,
            )
            return

        try:
            self._set_job(job_id, stage="refresh_tvl")
            self._run_command(
                [
                    sys.executable,
                    str(PROJECT_ROOT / "scripts/fetch_tvl.py"),
                    "--database",
                    str(self.database_path),
                    "--dex-csv",
                    str(self.data_dir / "dex_pool_volume_daily.csv"),
                    "--publish-dir",
                    str(self.data_dir),
                    "--output-dir",
                    str(processed_dir),
                    "--raw-root",
                    str(raw_root / "tvl"),
                ],
                log_path,
                stage="refresh_tvl",
            )
            result["tvl"] = "observed_or_explicit_source_status"
        except (OSError, subprocess.SubprocessError):
            result["tvl"] = "fetch_failed"
            result["dex_depth"] = "not_attempted_without_fresh_tvl"
            self._fail_job(
                job_id,
                stage="refresh_tvl",
                error_code="tvl_refresh_failed",
                message="DEX daily facts were published, but TVL refresh failed.",
                retryable=True,
                publication_committed=True,
                status="partial",
                result=result,
            )
            return

        try:
            self._set_job(job_id, stage="refresh_dex_depth", result=dict(result))
            self._run_command(
                [
                    sys.executable,
                    str(PROJECT_ROOT / "scripts/fetch_dex_depth.py"),
                    "--tvl-csv",
                    str(self.data_dir / "dex_pool_tvl_latest.csv"),
                    "--publish-dir",
                    str(self.data_dir),
                    "--output-dir",
                    str(processed_dir),
                    "--raw-root",
                    str(raw_root / "dex-depth"),
                ],
                log_path,
                stage="refresh_dex_depth",
            )
            result["dex_depth"] = "observed_or_explicit_unsupported"
        except (OSError, subprocess.SubprocessError):
            result["dex_depth"] = "fetch_failed"
            self._fail_job(
                job_id,
                stage="refresh_dex_depth",
                error_code="dex_depth_refresh_failed",
                message="DEX daily and TVL facts were published, but DEX depth refresh failed.",
                retryable=True,
                publication_committed=True,
                status="partial",
                result=result,
            )
            return

        self._set_job(
            job_id,
            status="succeeded",
            stage="complete",
            finished_at=utc_now().isoformat(),
            error=None,
            error_code=None,
            retryable=False,
            publication_committed=True,
            result=result,
        )

    def _run_job(self, job_id: str) -> None:
        with self.worker_lock:
            self.collection_lock_path.parent.mkdir(parents=True, exist_ok=True)
            with self.collection_lock_path.open("a+", encoding="utf-8") as lock_handle:
                try:
                    fcntl.flock(
                        lock_handle.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                except BlockingIOError:
                    self._mark_pending_onboarding_needs_review(job_id)
                    self._fail_job(
                        job_id,
                        stage="acquire_collection_lock",
                        error_code="collection_in_progress",
                        message=(
                            "Another scheduled or administrator collection is "
                            "already publishing market facts."
                        ),
                        retryable=True,
                    )
                    return
                try:
                    queued_job = self.jobs[job_id]
                    if queued_job.get("job_type") == "retry_failed":
                        try:
                            self.revalidate_retry_authorization(queued_job)
                        except (OSError, ValueError):
                            self._fail_job(
                                job_id,
                                stage="authorize_retry",
                                error_code="retry_authorization_expired",
                                message=(
                                    "The audited retry authorization changed "
                                    "before collection began. Refresh the "
                                    "quality queue before trying again."
                                ),
                                retryable=True,
                            )
                            return
                    job = self._set_job(
                        job_id,
                        status="running",
                        stage=(
                            "resolve_identity"
                            if self.jobs[job_id].get("job_type")
                            == "token_onboarding"
                            else "collect_daily_facts"
                        ),
                        started_at=utc_now().isoformat(),
                    )
                    log_path = self.job_dir / f"{job_id}.log"
                    log_path.parent.mkdir(parents=True, exist_ok=True)
                    log_path.write_text("", encoding="utf-8")
                    if job.get("job_type") == "token_onboarding":
                        self._run_onboarding_job(job_id, job, log_path)
                    else:
                        self._run_refresh_job(job_id, job, log_path)
                except Exception:
                    self._mark_pending_onboarding_needs_review(job_id)
                    self._fail_job(
                        job_id,
                        stage="unexpected_worker_failure",
                        error_code="unexpected_worker_failure",
                        message=(
                            "The refresh worker stopped unexpectedly. Review "
                            "the server-side job log before retrying."
                        ),
                        retryable=True,
                        publication_committed=bool(
                            self.jobs[job_id].get("publication_committed")
                        ),
                    )

    def list_jobs(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.state_lock:
            jobs = sorted(self.jobs.values(), key=lambda job: job["created_at"], reverse=True)
            return [dict(job) for job in jobs[: max(1, min(limit, 100))]]

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        """Return one server-owned job copy without exposing the jobs mapping."""
        with self.state_lock:
            job = self.jobs.get(job_id)
            return dict(job) if job is not None else None

    def count_jobs_created_on(
        self,
        *,
        requested_by: str | None,
        job_type: str | None,
        created_on: date,
    ) -> int:
        """Count persisted accepted jobs for one UTC actor/type/day budget."""
        if not requested_by or not job_type:
            raise ValueError("Job budget identity is incomplete")
        count = 0
        with self.state_lock:
            for job in self.jobs.values():
                if (
                    job.get("requested_by") != requested_by
                    or job.get("job_type") != job_type
                ):
                    continue
                try:
                    created_at = datetime.fromisoformat(str(job["created_at"]))
                    if created_at.tzinfo is None:
                        continue
                    created_date = created_at.astimezone(timezone.utc).date()
                except (KeyError, TypeError, ValueError):
                    continue
                if created_date == created_on:
                    count += 1
        return count
