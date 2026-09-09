"""Server-configured, notification-only email adapter for Token reviews."""

from __future__ import annotations

import os
import re
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import parseaddr
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit, urlunsplit

from dashboard.token_reviews import TokenReviewError
from scripts.token_registry import (
    TokenRegistryError,
    normalize_chain,
    normalize_contract_address,
    normalize_token_symbol,
)


EMAIL_SUBJECT = "Token contract review requested"
REQUEST_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+$")
HOST_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")
TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off", ""}


def _configuration_error() -> TokenReviewError:
    return TokenReviewError(
        "invalid_token_review_email_config",
        "Token review email configuration is invalid",
    )


def _plain_setting(value: Any, *, required: bool = True, maximum: int = 512) -> str | None:
    if value is None:
        if required:
            raise _configuration_error()
        return None
    if not isinstance(value, str) or "\r" in value or "\n" in value:
        raise _configuration_error()
    text = value.strip()
    if (required and not text) or len(text) > maximum:
        raise _configuration_error()
    return text or None


def _email(value: Any) -> str:
    text = _plain_setting(value)
    assert text is not None
    _name, parsed = parseaddr(text)
    if parsed != text or EMAIL_PATTERN.fullmatch(text) is None:
        raise _configuration_error()
    return text


def _enabled_flag(value: Any) -> bool:
    text = _plain_setting(value, required=False, maximum=16)
    if text is None:
        return False
    normalized = text.lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise _configuration_error()


def _base_url(value: Any) -> str:
    text = _plain_setting(value)
    assert text is not None
    parsed = urlsplit(text)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise _configuration_error()
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


@dataclass(frozen=True)
class TokenReviewEmailSettings:
    """Validated server-owned SMTP and authenticated-review settings."""

    enabled: bool
    reviewer_email: str | None = None
    base_url: str | None = None
    smtp_host: str | None = None
    smtp_port: int | None = None
    smtp_starttls: bool = False
    smtp_from: str | None = None
    smtp_username: str | None = None
    smtp_password: str | None = None

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "TokenReviewEmailSettings":
        values = os.environ if environment is None else environment
        enabled = _enabled_flag(values.get("TOKEN_REVIEW_EMAIL_ENABLED"))
        if not enabled:
            return cls(enabled=False)
        reviewer_email = _email(values.get("TOKEN_REVIEWER_EMAIL"))
        base_url = _base_url(values.get("TOKEN_REVIEW_BASE_URL"))
        smtp_host = _plain_setting(values.get("TOKEN_REVIEW_SMTP_HOST"), maximum=253)
        assert smtp_host is not None
        if HOST_PATTERN.fullmatch(smtp_host) is None:
            raise _configuration_error()
        port_text = _plain_setting(values.get("TOKEN_REVIEW_SMTP_PORT"), maximum=5)
        assert port_text is not None
        if not port_text.isdecimal():
            raise _configuration_error()
        smtp_port = int(port_text)
        if not 1 <= smtp_port <= 65535:
            raise _configuration_error()
        smtp_starttls = _enabled_flag(values.get("TOKEN_REVIEW_SMTP_STARTTLS"))
        smtp_from = _email(values.get("TOKEN_REVIEW_SMTP_FROM"))
        username = _plain_setting(
            values.get("TOKEN_REVIEW_SMTP_USERNAME"), required=False, maximum=256
        )
        password = _plain_setting(
            values.get("TOKEN_REVIEW_SMTP_PASSWORD"), required=False, maximum=512
        )
        if (username is None) != (password is None):
            raise _configuration_error()
        return cls(
            enabled=True,
            reviewer_email=reviewer_email,
            base_url=base_url,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_starttls=smtp_starttls,
            smtp_from=smtp_from,
            smtp_username=username,
            smtp_password=password,
        )

    def review_url(self, request_id: Any) -> str:
        if not self.enabled or self.base_url is None:
            raise _configuration_error()
        if not isinstance(request_id, str) or REQUEST_ID_PATTERN.fullmatch(request_id) is None:
            raise TokenReviewError("invalid_request_id", "Review request id is invalid")
        parsed = urlsplit(self.base_url)
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                "%s/admin.html" % parsed.path.rstrip("/"),
                "",
                "token-review=%s" % request_id,
            )
        )


def _review_fields(review: Mapping[str, Any]) -> tuple[str, str, str, str, int, str]:
    if not isinstance(review, Mapping):
        raise TokenReviewError("invalid_review_database", "Token review database is invalid")
    request_id = review.get("request_id")
    if not isinstance(request_id, str) or REQUEST_ID_PATTERN.fullmatch(request_id) is None:
        raise TokenReviewError("invalid_review_database", "Token review database is invalid")
    try:
        chain = normalize_chain(review.get("chain"))
        address = normalize_contract_address(chain, review.get("contract_address"))
        symbol = normalize_token_symbol(review.get("token_symbol"))
    except TokenRegistryError as error:
        raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error
    try:
        days = int(review.get("requested_history_days"))
    except (TypeError, ValueError) as error:
        raise TokenReviewError("invalid_review_database", "Token review database is invalid") from error
    digest = review.get("candidate_sha256")
    if (
        isinstance(days, bool)
        or not 1 <= days <= 180
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        raise TokenReviewError("invalid_review_database", "Token review database is invalid")
    return request_id, chain, address, symbol, days, digest


def build_token_review_message(
    settings: TokenReviewEmailSettings,
    review: Mapping[str, Any],
) -> EmailMessage:
    """Build a plain-text notification from persisted evidence and server settings."""
    if not isinstance(settings, TokenReviewEmailSettings) or not settings.enabled:
        raise _configuration_error()
    if settings.reviewer_email is None or settings.smtp_from is None:
        raise _configuration_error()
    request_id, chain, address, symbol, days, digest = _review_fields(review)
    message = EmailMessage()
    message["To"] = settings.reviewer_email
    message["From"] = settings.smtp_from
    message["Subject"] = EMAIL_SUBJECT
    message["Auto-Submitted"] = "auto-generated"
    message.set_content(
        "A Token contract is awaiting authenticated review.\n\n"
        "Request ID: %s\n"
        "Chain: %s\n"
        "Contract address: %s\n"
        "Symbol: %s\n"
        "Requested history: %s days\n"
        "Candidate digest: %s\n"
        "Review URL: %s\n"
        % (request_id, chain, address, symbol, days, digest, settings.review_url(request_id)),
        subtype="plain",
        charset="utf-8",
    )
    return message


class SmtpTokenReviewMailer:
    """Small injectable SMTP adapter; callers persist delivery state themselves."""

    def __init__(
        self,
        settings: TokenReviewEmailSettings,
        *,
        smtp_factory: Callable[..., Any] = smtplib.SMTP,
        timeout: float = 30,
    ) -> None:
        if not isinstance(settings, TokenReviewEmailSettings) or not settings.enabled:
            raise _configuration_error()
        if timeout <= 0:
            raise ValueError("SMTP timeout must be positive")
        self.settings = settings
        self.smtp_factory = smtp_factory
        self.timeout = timeout

    def send(self, review: Mapping[str, Any]) -> EmailMessage:
        message = build_token_review_message(self.settings, review)
        if self.settings.smtp_host is None or self.settings.smtp_port is None:
            raise _configuration_error()
        with self.smtp_factory(
            self.settings.smtp_host,
            self.settings.smtp_port,
            self.timeout,
        ) as client:
            if self.settings.smtp_starttls:
                client.starttls()
            if self.settings.smtp_username is not None:
                client.login(self.settings.smtp_username, self.settings.smtp_password)
            client.send_message(message)
        return message
