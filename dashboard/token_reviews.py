"""Fail-closed durable review ledger for proposed Token contracts."""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from scripts.token_registry import (
    TokenRegistryError,
    normalize_chain,
    normalize_contract_address,
    normalize_token_symbol,
)


SCHEMA_VERSION = "1"
MAX_LIST_RECORDS = 50
MAX_AUDIT_EVENTS = 50
MAX_CANDIDATE_BYTES = 64 * 1024
MAX_HISTORY_DAYS = 180
REQUEST_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
SAFE_CODE_PATTERN = re.compile(r"^[a-z0-9_]{1,64}$")
STATUSES = {
    "pending_review",
    "approved",
    "rejected",
    "onboarding_starting",
    "onboarding_queued",
}
NOTIFICATION_STATUSES = {
    "pending",
    "disabled",
    "unconfigured",
    "sending",
    "sent",
    "failed",
}
TRANSITIONS = {
    "pending_review": {"approved", "rejected"},
    "approved": {"onboarding_starting"},
    "rejected": set(),
    "onboarding_starting": {"approved", "onboarding_queued"},
    "onboarding_queued": set(),
}


class TokenReviewError(ValueError):
    """Stable, non-sensitive error contract for the review boundary."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": self.message,
            "error_code": self.code,
            "retryable": self.retryable,
        }


def _error_from_registry(error: TokenRegistryError) -> TokenReviewError:
    return TokenReviewError(error.code, error.message)


def _text(value: Any, *, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str):
        raise TokenReviewError("invalid_review_request", "%s is invalid" % field)
    text = value.strip()
    if not text or len(text) > maximum or "\r" in text or "\n" in text:
        raise TokenReviewError("invalid_review_request", "%s is invalid" % field)
    return text


def _utc_timestamp(value: Any = None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    if not isinstance(value, str) or len(value) > 64:
        raise TokenReviewError("invalid_review_request", "UTC timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise TokenReviewError("invalid_review_request", "UTC timestamp is invalid") from error
    if parsed.tzinfo is None:
        raise TokenReviewError("invalid_review_request", "UTC timestamp is invalid")
    return parsed.astimezone(timezone.utc).isoformat()


def _json_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 12:
        raise TokenReviewError("invalid_candidate", "Candidate is too deeply nested")
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise TokenReviewError("invalid_candidate", "Candidate contains an invalid number")
        return value
    if isinstance(value, str):
        if len(value) > 512 or "\x00" in value:
            raise TokenReviewError("invalid_candidate", "Candidate contains invalid text")
        return value
    if isinstance(value, list):
        if len(value) > 100:
            raise TokenReviewError("invalid_candidate", "Candidate contains too many values")
        return [_json_value(item, depth=depth + 1) for item in value]
    if isinstance(value, Mapping):
        if len(value) > 100:
            raise TokenReviewError("invalid_candidate", "Candidate contains too many fields")
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > 128:
                raise TokenReviewError("invalid_candidate", "Candidate contains an invalid field")
            normalized[key] = _json_value(item, depth=depth + 1)
        return normalized
    raise TokenReviewError("invalid_candidate", "Candidate contains an invalid value")


def _canonical_json(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:  # defensive after _json_value
        raise TokenReviewError("invalid_candidate", "Candidate cannot be serialized") from error
    if len(encoded.encode("ascii")) > MAX_CANDIDATE_BYTES:
        raise TokenReviewError("invalid_candidate", "Candidate exceeds the allowed size")
    return encoded


def review_candidate_snapshot(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Canonicalize bounded candidate evidence and return its digest."""
    if not isinstance(candidate, Mapping):
        raise TokenReviewError("invalid_candidate", "Candidate must be an object")
    normalized = _json_value(candidate)
    identity = normalized.get("identity")
    if not isinstance(identity, dict):
        raise TokenReviewError("invalid_candidate", "Candidate is missing identity")
    try:
        chain = normalize_chain(identity.get("chain"))
        address = normalize_contract_address(chain, identity.get("contract_address"))
        symbol = normalize_token_symbol(identity.get("token_symbol"))
    except TokenRegistryError as error:
        raise _error_from_registry(error)
    identity["chain"] = chain
    identity["contract_address"] = address
    identity["token_symbol"] = symbol
    encoded = _canonical_json(normalized)
    return {
        "chain": chain,
        "contract_address": address,
        "token_symbol": symbol,
        "candidate": json.loads(encoded),
        "candidate_sha256": hashlib.sha256(encoded.encode("ascii")).hexdigest(),
    }


def _safe_code(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or SAFE_CODE_PATTERN.fullmatch(value) is None:
        raise TokenReviewError("invalid_review_request", "%s is invalid" % field)
    return value


def _request_id(value: Any) -> str:
    if not isinstance(value, str) or REQUEST_ID_PATTERN.fullmatch(value) is None:
        raise TokenReviewError("invalid_request_id", "Review request id is invalid")
    return value


def _history_days(value: Any) -> int:
    if isinstance(value, bool):
        raise TokenReviewError("invalid_history_days", "History days are invalid")
    try:
        days = int(value)
    except (TypeError, ValueError) as error:
        raise TokenReviewError("invalid_history_days", "History days are invalid") from error
    if str(days) != str(value).strip() or not 1 <= days <= MAX_HISTORY_DAYS:
        raise TokenReviewError("invalid_history_days", "History days are invalid")
    return days


def _audit_event(event: str, at: str, actor: str) -> dict[str, str]:
    return {"event": event, "at": at, "actor": actor}


class TokenReviewStore:
    """SQLite-backed ledger with dedupe and revision-checked transitions."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        existed = self.path.exists()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as connection:
                if not existed:
                    self._create_schema(connection)
                self._validate_schema(connection)
        except (OSError, sqlite3.DatabaseError, TokenReviewError) as error:
            if isinstance(error, TokenReviewError):
                raise
            raise TokenReviewError(
                "invalid_review_database",
                "Token review database is invalid",
            ) from error

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(
            self.path,
            timeout=10,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE token_review_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE token_reviews (
                request_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL,
                chain TEXT NOT NULL,
                contract_address TEXT NOT NULL,
                token_symbol TEXT NOT NULL,
                requested_history_days INTEGER NOT NULL,
                submitter TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                candidate_sha256 TEXT NOT NULL,
                notification_status TEXT NOT NULL,
                notification_attempts INTEGER NOT NULL,
                notification_error_code TEXT,
                notification_retry_at TEXT,
                notification_sent_at TEXT,
                reviewer TEXT,
                reviewed_at TEXT,
                onboarding_job_id TEXT,
                onboarding_error_code TEXT,
                UNIQUE(chain, contract_address)
            );
            CREATE TABLE token_review_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL REFERENCES token_reviews(request_id),
                event TEXT NOT NULL,
                at TEXT NOT NULL,
                actor TEXT NOT NULL
            );
            CREATE INDEX token_reviews_created_at ON token_reviews(created_at);
            CREATE INDEX token_review_audit_request_id ON token_review_audit(request_id, id);
            """
        )
        connection.execute(
            "INSERT INTO token_review_meta(key, value) VALUES (?, ?)",
            ("schema_version", SCHEMA_VERSION),
        )

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        def invalid() -> None:
            raise TokenReviewError(
                "invalid_review_database", "Token review database is invalid"
            )

        def columns_for(table: str) -> dict[str, sqlite3.Row]:
            return {
                str(row["name"]): row
                for row in connection.execute("PRAGMA table_info(%s)" % table)
            }

        expected = {
            "token_review_meta",
            "token_reviews",
            "token_review_audit",
        }
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
        tables = {str(row["name"]) for row in rows}
        if not expected.issubset(tables):
            invalid()
        version = connection.execute(
            "SELECT value FROM token_review_meta WHERE key = ?", ("schema_version",)
        ).fetchone()
        if version is None or version["value"] != SCHEMA_VERSION:
            invalid()
        required_columns = {
            "token_reviews": {
                "request_id", "revision", "chain", "contract_address", "token_symbol",
                "requested_history_days", "submitter", "created_at", "status",
                "candidate_json", "candidate_sha256", "notification_status",
                "notification_attempts", "notification_error_code", "notification_retry_at",
                "notification_sent_at", "reviewer", "reviewed_at", "onboarding_job_id",
                "onboarding_error_code",
            },
            "token_review_audit": {"id", "request_id", "event", "at", "actor"},
        }
        table_columns: dict[str, dict[str, sqlite3.Row]] = {}
        for table, columns in required_columns.items():
            present = columns_for(table)
            table_columns[table] = present
            if not columns.issubset(present):
                invalid()

        meta_columns = columns_for("token_review_meta")
        if (
            "key" not in meta_columns
            or "value" not in meta_columns
            or meta_columns["key"]["pk"] != 1
            or not meta_columns["value"]["notnull"]
        ):
            invalid()
        if table_columns["token_reviews"]["request_id"]["pk"] != 1:
            invalid()
        if table_columns["token_review_audit"]["id"]["pk"] != 1:
            invalid()
        for column in {
            "revision", "chain", "contract_address", "token_symbol",
            "requested_history_days", "submitter", "created_at", "status",
            "candidate_json", "candidate_sha256", "notification_status",
            "notification_attempts",
        }:
            if not table_columns["token_reviews"][column]["notnull"]:
                invalid()
        for column in {"request_id", "event", "at", "actor"}:
            if not table_columns["token_review_audit"][column]["notnull"]:
                invalid()
        unique_identity = False
        for index in connection.execute("PRAGMA index_list(token_reviews)"):
            if not index["unique"] or index["partial"]:
                continue
            index_name = str(index["name"])
            index_columns = tuple(
                str(item["name"])
                for item in connection.execute("PRAGMA index_info(%s)" % index_name)
            )
            if index_columns == ("chain", "contract_address"):
                unique_identity = True
                break
        if not unique_identity:
            invalid()
        foreign_keys = connection.execute("PRAGMA foreign_key_list(token_review_audit)").fetchall()
        if not any(
            row["table"] == "token_reviews"
            and row["from"] == "request_id"
            and row["to"] == "request_id"
            for row in foreign_keys
        ):
            invalid()

    @staticmethod
    def _append_audit(
        connection: sqlite3.Connection,
        request_id: str,
        *,
        event: str,
        at: str,
        actor: str,
    ) -> None:
        count = connection.execute(
            "SELECT COUNT(*) AS count FROM token_review_audit WHERE request_id = ?",
            (request_id,),
        ).fetchone()["count"]
        if count >= MAX_AUDIT_EVENTS:
            raise TokenReviewError("audit_limit", "Review audit event limit reached")
        connection.execute(
            "INSERT INTO token_review_audit(request_id, event, at, actor) VALUES (?, ?, ?, ?)",
            (request_id, event, at, actor),
        )

    def _load(self, connection: sqlite3.Connection, request_id: str) -> dict[str, Any]:
        row = connection.execute(
            "SELECT * FROM token_reviews WHERE request_id = ?", (request_id,)
        ).fetchone()
        if row is None:
            raise TokenReviewError("review_not_found", "Review request was not found")
        try:
            candidate = json.loads(row["candidate_json"])
            snapshot = review_candidate_snapshot(candidate)
            if snapshot["candidate_sha256"] != row["candidate_sha256"]:
                raise ValueError("digest")
            if (
                snapshot["chain"] != row["chain"]
                or snapshot["contract_address"] != row["contract_address"]
                or snapshot["token_symbol"] != row["token_symbol"]
            ):
                raise ValueError("identity")
            request_id = _request_id(row["request_id"])
            revision = int(row["revision"])
            if revision < 1 or row["status"] not in STATUSES:
                raise ValueError("state")
            days = _history_days(row["requested_history_days"])
            submitter = _text(row["submitter"], field="submitter", maximum=128)
            created_at = _utc_timestamp(row["created_at"])
            notification_status = row["notification_status"]
            attempts = int(row["notification_attempts"])
            if notification_status not in NOTIFICATION_STATUSES or not 0 <= attempts <= 3:
                raise ValueError("notification")
            audit_rows = connection.execute(
                "SELECT event, at, actor FROM token_review_audit WHERE request_id = ? ORDER BY id ASC",
                (request_id,),
            ).fetchall()
            if not audit_rows or len(audit_rows) > MAX_AUDIT_EVENTS:
                raise ValueError("audit")
            audit = []
            for audit_row in audit_rows:
                event = _text(audit_row["event"], field="audit event", maximum=64)
                audit.append(_audit_event(
                    event,
                    _utc_timestamp(audit_row["at"]),
                    _text(audit_row["actor"], field="audit actor", maximum=128),
                ))
            reviewer = (
                _text(row["reviewer"], field="reviewer", maximum=128)
                if row["reviewer"] is not None else None
            )
            reviewed_at = (
                _utc_timestamp(row["reviewed_at"])
                if row["reviewed_at"] is not None else None
            )
            onboarding_job_id = (
                _text(row["onboarding_job_id"], field="onboarding job", maximum=128)
                if row["onboarding_job_id"] is not None else None
            )
            audit_events = {event["event"] for event in audit}
            status = row["status"]
            if status == "pending_review" and (reviewer is not None or reviewed_at is not None):
                raise ValueError("pending decision")
            if status in {"approved", "rejected"} and (
                reviewer is None
                or reviewed_at is None
                or "state_%s" % status not in audit_events
            ):
                raise ValueError("decision evidence")
            if status == "onboarding_starting" and (
                reviewer is None
                or reviewed_at is None
                or "state_approved" not in audit_events
                or "state_onboarding_starting" not in audit_events
            ):
                raise ValueError("onboarding start evidence")
            if status == "onboarding_queued" and (
                reviewer is None
                or reviewed_at is None
                or onboarding_job_id is None
                or "state_approved" not in audit_events
                or "state_onboarding_starting" not in audit_events
                or "state_onboarding_queued" not in audit_events
            ):
                raise ValueError("onboarding queue evidence")
            return {
                "request_id": request_id,
                "revision": revision,
                "chain": snapshot["chain"],
                "contract_address": snapshot["contract_address"],
                "token_symbol": snapshot["token_symbol"],
                "requested_history_days": days,
                "submitter": submitter,
                "created_at": created_at,
                "status": status,
                "candidate": snapshot["candidate"],
                "candidate_sha256": snapshot["candidate_sha256"],
                "notification": {
                    "status": notification_status,
                    "attempts": attempts,
                    "error_code": _safe_code(row["notification_error_code"], field="notification error"),
                    "retry_at": (
                        _utc_timestamp(row["notification_retry_at"])
                        if row["notification_retry_at"] is not None else None
                    ),
                    "sent_at": (
                        _utc_timestamp(row["notification_sent_at"])
                        if row["notification_sent_at"] is not None else None
                    ),
                },
                "reviewer": reviewer,
                "reviewed_at": reviewed_at,
                "onboarding_job_id": onboarding_job_id,
                "onboarding_error_code": _safe_code(
                    row["onboarding_error_code"], field="onboarding error"
                ),
                "audit": audit,
            }
        except (TypeError, ValueError, KeyError, json.JSONDecodeError, TokenReviewError) as error:
            raise TokenReviewError(
                "invalid_review_database", "Token review database is invalid"
            ) from error

    def create_or_get(
        self,
        candidate: Mapping[str, Any],
        *,
        requested_history_days: Any,
        submitter: Any,
        created_at: Any = None,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically create a pending review or return its canonical duplicate."""
        snapshot = review_candidate_snapshot(candidate)
        days = _history_days(requested_history_days)
        actor = _text(submitter, field="submitter", maximum=128)
        now = _utc_timestamp(created_at)
        request_id = secrets.token_hex(16)
        encoded = _canonical_json(snapshot["candidate"])
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT request_id FROM token_reviews WHERE chain = ? AND contract_address = ?",
                    (snapshot["chain"], snapshot["contract_address"]),
                ).fetchone()
                if existing is not None:
                    review = self._load(connection, existing["request_id"])
                    connection.execute("COMMIT")
                    return review, True
                connection.execute(
                    """INSERT INTO token_reviews (
                        request_id, revision, chain, contract_address, token_symbol,
                        requested_history_days, submitter, created_at, status,
                        candidate_json, candidate_sha256, notification_status,
                        notification_attempts, notification_error_code,
                        notification_retry_at, notification_sent_at, reviewer,
                        reviewed_at, onboarding_job_id, onboarding_error_code
                    ) VALUES (?, 1, ?, ?, ?, ?, ?, ?, 'pending_review', ?, ?, 'pending',
                        0, NULL, NULL, NULL, NULL, NULL, NULL, NULL)""",
                    (
                        request_id, snapshot["chain"], snapshot["contract_address"],
                        snapshot["token_symbol"], days, actor, now, encoded,
                        snapshot["candidate_sha256"],
                    ),
                )
                self._append_audit(connection, request_id, event="created", at=now, actor=actor)
                review = self._load(connection, request_id)
                connection.execute("COMMIT")
                return review, False
        except sqlite3.DatabaseError as error:
            raise TokenReviewError(
                "invalid_review_database", "Token review database is invalid"
            ) from error

    create_or_get_review = create_or_get

    def get(self, request_id: Any) -> dict[str, Any]:
        identity = _request_id(request_id)
        try:
            with self._connect() as connection:
                return self._load(connection, identity)
        except sqlite3.DatabaseError as error:
            raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error

    get_review = get

    def list_reviews(self, *, limit: Any = MAX_LIST_RECORDS) -> list[dict[str, Any]]:
        if isinstance(limit, bool):
            raise TokenReviewError("invalid_list_limit", "Review list limit is invalid")
        try:
            requested_limit = int(limit)
        except (TypeError, ValueError) as error:
            raise TokenReviewError("invalid_list_limit", "Review list limit is invalid") from error
        if requested_limit < 1:
            raise TokenReviewError("invalid_list_limit", "Review list limit is invalid")
        requested_limit = min(requested_limit, MAX_LIST_RECORDS)
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT request_id FROM token_reviews ORDER BY created_at DESC, request_id DESC LIMIT ?",
                    (requested_limit,),
                ).fetchall()
                return [self._load(connection, row["request_id"]) for row in rows]
        except sqlite3.DatabaseError as error:
            raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error

    def count_created_on(self, value: date | str) -> int:
        day = value.isoformat() if isinstance(value, date) else str(value)
        try:
            date.fromisoformat(day)
        except ValueError as error:
            raise TokenReviewError("invalid_review_date", "Review date is invalid") from error
        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM token_reviews WHERE substr(created_at, 1, 10) = ?",
                    (day,),
                ).fetchone()
                return int(row["count"])
        except sqlite3.DatabaseError as error:
            raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error

    def transition(
        self,
        request_id: Any,
        target_status: Any,
        *,
        expected_revision: Any,
        actor: Any,
        onboarding_job_id: Any = None,
        onboarding_error_code: Any = None,
        at: Any = None,
    ) -> dict[str, Any]:
        """Apply one state transition only when its revision still matches."""
        identity = _request_id(request_id)
        if target_status not in STATUSES:
            raise TokenReviewError("invalid_review_status", "Review status is invalid")
        if isinstance(expected_revision, bool):
            raise TokenReviewError("invalid_revision", "Review revision is invalid")
        try:
            revision = int(expected_revision)
        except (TypeError, ValueError) as error:
            raise TokenReviewError("invalid_revision", "Review revision is invalid") from error
        if revision < 1 or str(revision) != str(expected_revision).strip():
            raise TokenReviewError("invalid_revision", "Review revision is invalid")
        actor_text = _text(actor, field="actor", maximum=128)
        timestamp = _utc_timestamp(at)
        job_id = (
            _text(onboarding_job_id, field="onboarding job", maximum=128)
            if onboarding_job_id is not None else None
        )
        error_code = _safe_code(onboarding_error_code, field="onboarding error")
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current = connection.execute(
                    "SELECT status, revision FROM token_reviews WHERE request_id = ?", (identity,)
                ).fetchone()
                if current is None:
                    raise TokenReviewError("review_not_found", "Review request was not found")
                self._load(connection, identity)
                if int(current["revision"]) != revision:
                    raise TokenReviewError("stale_revision", "Review has changed; reload it")
                if target_status not in TRANSITIONS.get(current["status"], set()):
                    raise TokenReviewError("invalid_review_transition", "Review state transition is invalid")
                reviewer = actor_text if current["status"] == "pending_review" else None
                reviewed_at = timestamp if reviewer is not None else None
                cursor = connection.execute(
                    """UPDATE token_reviews SET
                        revision = ?, status = ?, reviewer = COALESCE(?, reviewer),
                        reviewed_at = COALESCE(?, reviewed_at),
                        onboarding_job_id = COALESCE(?, onboarding_job_id),
                        onboarding_error_code = ?
                    WHERE request_id = ? AND revision = ?""",
                    (
                        revision + 1, target_status, reviewer, reviewed_at, job_id,
                        error_code, identity, revision,
                    ),
                )
                if cursor.rowcount != 1:
                    raise TokenReviewError("stale_revision", "Review has changed; reload it")
                self._append_audit(
                    connection, identity, event="state_%s" % target_status,
                    at=timestamp, actor=actor_text,
                )
                review = self._load(connection, identity)
                connection.execute("COMMIT")
                return review
        except sqlite3.DatabaseError as error:
            raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error

    def claim_notification_attempt(
        self,
        request_id: Any,
        *,
        expected_revision: Any,
        actor: Any,
        at: Any = None,
    ) -> dict[str, Any]:
        """Reserve one SMTP attempt before a mail adapter has any side effect."""
        identity = _request_id(request_id)
        if isinstance(expected_revision, bool):
            raise TokenReviewError("invalid_revision", "Review revision is invalid")
        try:
            revision = int(expected_revision)
        except (TypeError, ValueError) as error:
            raise TokenReviewError("invalid_revision", "Review revision is invalid") from error
        if revision < 1 or str(revision) != str(expected_revision).strip():
            raise TokenReviewError("invalid_revision", "Review revision is invalid")
        actor_text = _text(actor, field="actor", maximum=128)
        timestamp = _utc_timestamp(at)
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current = connection.execute(
                    "SELECT revision, notification_status, notification_attempts "
                    "FROM token_reviews WHERE request_id = ?",
                    (identity,),
                ).fetchone()
                if current is None:
                    raise TokenReviewError("review_not_found", "Review request was not found")
                if int(current["revision"]) != revision:
                    raise TokenReviewError("stale_revision", "Review has changed; reload it")
                if current["notification_status"] not in {
                    "pending", "failed", "disabled", "unconfigured"
                }:
                    raise TokenReviewError(
                        "invalid_notification_transition",
                        "Notification state transition is invalid",
                    )
                attempts = int(current["notification_attempts"])
                if attempts >= 3:
                    raise TokenReviewError(
                        "notification_attempt_limit", "Notification attempt limit reached"
                    )
                cursor = connection.execute(
                    """UPDATE token_reviews SET revision = ?, notification_status = 'sending',
                        notification_attempts = ?, notification_error_code = NULL,
                        notification_retry_at = NULL
                    WHERE request_id = ? AND revision = ?""",
                    (revision + 1, attempts + 1, identity, revision),
                )
                if cursor.rowcount != 1:
                    raise TokenReviewError("stale_revision", "Review has changed; reload it")
                self._append_audit(
                    connection, identity, event="notification_claimed", at=timestamp,
                    actor=actor_text,
                )
                review = self._load(connection, identity)
                connection.execute("COMMIT")
                return review
        except sqlite3.DatabaseError as error:
            raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error

    claim_notification = claim_notification_attempt

    def record_notification(
        self,
        request_id: Any,
        *,
        expected_revision: Any,
        status: Any,
        actor: Any,
        error_code: Any = None,
        retry_at: Any = None,
        at: Any = None,
    ) -> dict[str, Any]:
        """Persist a completed notification attempt with a compare-and-swap revision."""
        identity = _request_id(request_id)
        if status not in {"sent", "failed", "disabled", "unconfigured"}:
            raise TokenReviewError(
                "invalid_notification_transition", "Notification state transition is invalid"
            )
        if isinstance(expected_revision, bool):
            raise TokenReviewError("invalid_revision", "Review revision is invalid")
        try:
            revision = int(expected_revision)
        except (TypeError, ValueError) as error:
            raise TokenReviewError("invalid_revision", "Review revision is invalid") from error
        if revision < 1 or str(revision) != str(expected_revision).strip():
            raise TokenReviewError("invalid_revision", "Review revision is invalid")
        actor_text = _text(actor, field="actor", maximum=128)
        timestamp = _utc_timestamp(at)
        code = _safe_code(error_code, field="notification error")
        retry_timestamp = _utc_timestamp(retry_at) if retry_at is not None else None
        if status == "failed" and code is None:
            raise TokenReviewError("invalid_notification_error", "Notification error is invalid")
        if status != "failed" and code is not None:
            raise TokenReviewError("invalid_notification_error", "Notification error is invalid")
        if status == "sent" and retry_timestamp is not None:
            raise TokenReviewError("invalid_notification_error", "Notification error is invalid")
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current = connection.execute(
                    "SELECT revision, notification_status FROM token_reviews WHERE request_id = ?",
                    (identity,),
                ).fetchone()
                if current is None:
                    raise TokenReviewError("review_not_found", "Review request was not found")
                if int(current["revision"]) != revision:
                    raise TokenReviewError("stale_revision", "Review has changed; reload it")
                unavailable_without_attempt = (
                    status in {"disabled", "unconfigured"}
                    and current["notification_status"] in {"pending", "failed", "disabled", "unconfigured"}
                )
                if current["notification_status"] != "sending" and not unavailable_without_attempt:
                    raise TokenReviewError(
                        "invalid_notification_transition",
                        "Notification state transition is invalid",
                    )
                sent_at = timestamp if status == "sent" else None
                cursor = connection.execute(
                    """UPDATE token_reviews SET revision = ?, notification_status = ?,
                        notification_error_code = ?, notification_retry_at = ?,
                        notification_sent_at = ?
                    WHERE request_id = ? AND revision = ?""",
                    (
                        revision + 1, status, code, retry_timestamp, sent_at,
                        identity, revision,
                    ),
                )
                if cursor.rowcount != 1:
                    raise TokenReviewError("stale_revision", "Review has changed; reload it")
                self._append_audit(
                    connection, identity, event="notification_%s" % status,
                    at=timestamp, actor=actor_text,
                )
                review = self._load(connection, identity)
                connection.execute("COMMIT")
                return review
        except sqlite3.DatabaseError as error:
            raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error


def public_token_review(review: Mapping[str, Any], *, deduplicated: bool = False) -> dict[str, Any]:
    """Return the intentionally small receipt allowed on public surfaces."""
    if not isinstance(review, Mapping):
        raise TokenReviewError("invalid_review_database", "Token review database is invalid")
    request_id = _request_id(review.get("request_id"))
    status = review.get("status")
    if status not in STATUSES:
        raise TokenReviewError("invalid_review_database", "Token review database is invalid")
    notification = review.get("notification")
    if not isinstance(notification, Mapping) or notification.get("status") not in NOTIFICATION_STATUSES:
        raise TokenReviewError("invalid_review_database", "Token review database is invalid")
    try:
        chain = normalize_chain(review.get("chain"))
        address = normalize_contract_address(chain, review.get("contract_address"))
        symbol = normalize_token_symbol(review.get("token_symbol"))
    except TokenRegistryError as error:
        raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error
    return {
        "request_id": request_id,
        "chain": chain,
        "contract_address": address,
        "token_symbol": symbol,
        "status": status,
        "created_at": _utc_timestamp(review.get("created_at")),
        "revision": int(review.get("revision")),
        "deduplicated": bool(deduplicated),
        "notification": {"status": notification["status"]},
    }
