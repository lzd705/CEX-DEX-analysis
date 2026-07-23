"""Server-side authentication and refresh jobs for the administrator page."""

from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import json
import os
import secrets
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOKEN_CONFIG_PATH = PROJECT_ROOT / "config/tokens.csv"
DEFAULT_JOB_DIR = PROJECT_ROOT / "data/local/admin/jobs"
PASSWORD_ALGORITHM = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 600_000
SESSION_SECONDS = 8 * 60 * 60
LOGIN_WINDOW_SECONDS = 15 * 60
MAX_LOGIN_FAILURES = 5


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


class AdminService:
    """Own authentication state and one-at-a-time background refresh jobs."""

    def __init__(
        self,
        *,
        username: str | None = None,
        password_hash: str | None = None,
        job_dir: Path | None = None,
    ) -> None:
        self.username = username if username is not None else os.environ.get("ADMIN_USERNAME", "admin")
        self.password_hash = password_hash if password_hash is not None else os.environ.get("ADMIN_PASSWORD_HASH", "")
        configured_job_dir = os.environ.get("ADMIN_JOB_DIR")
        self.job_dir = job_dir or (Path(configured_job_dir).expanduser() if configured_job_dir else DEFAULT_JOB_DIR)
        self.sessions: dict[str, dict[str, Any]] = {}
        self.failed_logins: dict[str, list[float]] = {}
        self.jobs: dict[str, dict[str, Any]] = {}
        self.state_lock = threading.Lock()
        self.worker_lock = threading.Lock()
        self._load_jobs()

    @property
    def configured(self) -> bool:
        return bool(self.username and self.password_hash)

    def _load_jobs(self) -> None:
        if not self.job_dir.exists():
            return
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
            self.jobs[job["job_id"]] = job

    def _clean_failures(self, client_ip: str) -> list[float]:
        cutoff = time.monotonic() - LOGIN_WINDOW_SECONDS
        failures = [stamp for stamp in self.failed_logins.get(client_ip, []) if stamp >= cutoff]
        self.failed_logins[client_ip] = failures
        return failures

    def login(self, client_ip: str, username: str, password: str) -> tuple[str, dict[str, Any]]:
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
        if not session:
            return {"authenticated": False, "configured": self.configured}
        return {
            "authenticated": True,
            "configured": self.configured,
            "username": session["username"],
            "csrf_token": session["csrf_token"],
            "expires_at": session["expires_at"].isoformat(),
        }

    def configured_tokens(self) -> list[str]:
        with TOKEN_CONFIG_PATH.open("r", newline="", encoding="utf-8") as handle:
            return sorted(row["token_symbol"] for row in csv.DictReader(handle))

    def validate_job(self, payload: dict[str, Any], *, today: date | None = None) -> dict[str, str]:
        token = str(payload.get("token_symbol", "")).strip().upper()
        if token not in self.configured_tokens():
            raise ValueError("Token is not present in config/tokens.csv")
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

    def create_job(self, payload: dict[str, Any], username: str) -> dict[str, Any]:
        request = self.validate_job(payload)
        job_id = secrets.token_hex(8)
        job = {
            "job_id": job_id,
            **request,
            "requested_by": username,
            "status": "queued",
            "created_at": utc_now().isoformat(),
            "started_at": None,
            "finished_at": None,
            "error": None,
        }
        with self.state_lock:
            self.jobs[job_id] = job
            self._save_job(job)
        threading.Thread(target=self._run_job, args=(job_id,), daemon=True).start()
        return dict(job)

    def _save_job(self, job: dict[str, Any]) -> None:
        self.job_dir.mkdir(parents=True, exist_ok=True)
        target = self.job_dir / f"{job['job_id']}.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(job, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(target)

    def _set_job(self, job_id: str, **updates: Any) -> dict[str, Any]:
        with self.state_lock:
            job = self.jobs[job_id]
            job.update(updates)
            self._save_job(job)
            return dict(job)

    def _run_job(self, job_id: str) -> None:
        with self.worker_lock:
            job = self._set_job(job_id, status="running", started_at=utc_now().isoformat())
            command = [
                sys.executable,
                str(PROJECT_ROOT / "scripts/run_fact_pipeline.py"),
                "--tokens",
                job["token_symbol"],
                "--start",
                job["start_date"],
                "--end",
                job["end_date"],
                "--append",
                "--publish-local",
            ]
            log_path = self.job_dir / f"{job_id}.log"
            try:
                with log_path.open("w", encoding="utf-8") as log:
                    subprocess.run(
                        command,
                        cwd=PROJECT_ROOT,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                        timeout=2 * 60 * 60,
                    )
            except (OSError, subprocess.SubprocessError):
                self._set_job(
                    job_id,
                    status="failed",
                    finished_at=utc_now().isoformat(),
                    error="Refresh failed. Review the server-side job log.",
                )
                return
            self._set_job(
                job_id,
                status="succeeded",
                finished_at=utc_now().isoformat(),
                error=None,
            )

    def list_jobs(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.state_lock:
            jobs = sorted(self.jobs.values(), key=lambda job: job["created_at"], reverse=True)
            return [dict(job) for job in jobs[: max(1, min(limit, 100))]]
