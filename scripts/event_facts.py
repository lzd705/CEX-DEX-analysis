"""Validate and publish source-backed Event Facts without doing event studies.

This module deliberately stops at the fact boundary:

* the taxonomy is limited to unlock, airdrop, and CEX spot listing;
* announcement time, effective time, precision, lifecycle, evidence status,
  and revision history are explicit;
* every row is backed by an HTTPS source and a versioned source-check record;
* unknown amounts remain blank/null and USD amounts must be source-reported;
* no return, impact, sentiment, importance, or causal label is calculated.

Curated rows are normalized into an immutable bundle containing revision and
latest CSVs, a small indexed SQLite database, and a manifest.  ``latest.json``
is the single atomic pointer used to publish a completed bundle.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOKEN_CONFIG = PROJECT_ROOT / "config/tokens.csv"
DEFAULT_INPUT = PROJECT_ROOT / "data/curated/event_facts.csv"
DEFAULT_RECORD_ROOT = PROJECT_ROOT / "data/evidence/events"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data/processed/events"
DEFAULT_PUBLISH_ROOT = PROJECT_ROOT / "data/local/events"
SCHEMA_PATH = PROJECT_ROOT / "data/schema/event_facts_v1.sql"

EVENT_FACT_SCHEMA = "event_facts/v1"
EVENT_BUNDLE_SCHEMA = "event_fact_bundle/v1"
EVENT_POINTER_SCHEMA = "event_fact_pointer/v1"
EVENT_TYPES = {"unlock", "airdrop", "cex_listing"}
LIFECYCLES = {
    "scheduled",
    "occurred",
    "postponed",
    "cancelled",
    "superseded",
}
EVIDENCE_STATUSES = {
    "primary_confirmed",
    "cross_checked",
    "onchain_observed",
}
TIME_PRECISIONS = {"second", "minute", "day", "month"}
SOURCE_KINDS = {
    "official_project",
    "official_governance",
    "official_exchange",
    "onchain_transaction",
}
EVENT_SUBTYPES = {
    "unlock": {"scheduled_release"},
    "airdrop": {"claim_start"},
    "cex_listing": {"spot_trading_start"},
}
SIZE_RELATIONS = {"exact", "up_to", "approximately"}

CURATED_COLUMNS = [
    "event_id",
    "revision",
    "token_symbol",
    "event_type",
    "event_subtype",
    "event_name",
    "lifecycle",
    "announced_at",
    "announced_at_precision",
    "effective_at",
    "effective_at_precision",
    "amount_token",
    "amount_usd",
    "amount_usd_basis",
    "percent_of_supply",
    "size_relation",
    "venue",
    "market_symbol",
    "market_id",
    "chain",
    "related_address",
    "related_tx_hash",
    "source_kind",
    "evidence_status",
    "source_url",
    "source_published_at",
    "source_published_at_precision",
    "source_checked_at_utc",
    "source_record_file",
    "record_locator",
    "recorded_at_utc",
    "revision_reason",
    "notes",
]
OUTPUT_COLUMNS = CURATED_COLUMNS + ["record_sha256"]
MATERIAL_REVISION_COLUMNS = [
    column
    for column in OUTPUT_COLUMNS
    if column not in {"revision", "recorded_at_utc", "revision_reason"}
]

_EVENT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{2,127}$")
_UTC_SECOND_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SECOND_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})$"
)
_MINUTE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})$"
)
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
_BUNDLE_ID_RE = re.compile(r"^[0-9a-f]{24}$")


class EventFactValidationError(ValueError):
    """Raised when a curated Event Fact cannot pass the publication contract."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def utc_now_text() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _clean_text(value: Any, *, field: str, required: bool = False) -> str:
    text = "" if value is None else str(value).strip()
    if required and not text:
        raise EventFactValidationError(f"{field} is required")
    if "\x00" in text or "\r" in text or "\n" in text:
        raise EventFactValidationError(f"{field} must be a single text line")
    return text


def _parse_positive_integer(value: Any, *, field: str) -> int:
    text = _clean_text(value, field=field, required=True)
    if not re.fullmatch(r"[1-9]\d*", text):
        raise EventFactValidationError(f"{field} must be a positive integer")
    return int(text)


def _canonical_decimal(
    value: Any,
    *,
    field: str,
    maximum: Decimal | None = None,
) -> str:
    text = _clean_text(value, field=field)
    if not text:
        return ""
    try:
        number = Decimal(text)
    except InvalidOperation as error:
        raise EventFactValidationError(
            f"{field} must be a finite positive decimal or blank"
        ) from error
    if not number.is_finite() or number <= 0:
        raise EventFactValidationError(
            f"{field} must be a finite positive decimal or blank"
        )
    if maximum is not None and number > maximum:
        raise EventFactValidationError(f"{field} cannot exceed {maximum}")
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    return canonical


def _parse_utc_second(value: Any, *, field: str) -> tuple[str, datetime]:
    text = _clean_text(value, field=field, required=True)
    if not _UTC_SECOND_RE.fullmatch(text):
        raise EventFactValidationError(
            f"{field} must be an exact UTC timestamp like 2026-07-29T08:30:00Z"
        )
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise EventFactValidationError(f"{field} is not a valid timestamp") from error
    return text, parsed


def normalize_precise_time(
    value: Any,
    precision: Any,
    *,
    field: str,
    required: bool,
) -> tuple[str, str]:
    """Normalize a precision-bearing time without manufacturing precision."""

    text = _clean_text(value, field=field, required=required)
    precision_text = _clean_text(
        precision,
        field=f"{field}_precision",
        required=bool(text) or required,
    ).lower()
    if not text:
        if precision_text:
            raise EventFactValidationError(
                f"{field}_precision must be blank when {field} is blank"
            )
        return "", ""
    if precision_text not in TIME_PRECISIONS:
        raise EventFactValidationError(
            f"{field}_precision must be one of {', '.join(sorted(TIME_PRECISIONS))}"
        )

    try:
        if precision_text == "month":
            if not _MONTH_RE.fullmatch(text):
                raise ValueError
            parsed_month = date.fromisoformat(text + "-01")
            return parsed_month.strftime("%Y-%m"), precision_text
        if precision_text == "day":
            if not _DAY_RE.fullmatch(text):
                raise ValueError
            return date.fromisoformat(text).isoformat(), precision_text

        pattern = _SECOND_RE if precision_text == "second" else _MINUTE_RE
        if not pattern.fullmatch(text):
            raise ValueError
        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        parsed_time = datetime.fromisoformat(iso_text)
        if parsed_time.tzinfo is None:
            raise ValueError
        parsed_time = parsed_time.astimezone(timezone.utc)
        timespec = "seconds" if precision_text == "second" else "minutes"
        return (
            parsed_time.isoformat(timespec=timespec).replace("+00:00", "Z"),
            precision_text,
        )
    except ValueError as error:
        examples = {
            "month": "2026-07",
            "day": "2026-07-29",
            "minute": "2026-07-29T08:30Z",
            "second": "2026-07-29T08:30:00Z",
        }
        raise EventFactValidationError(
            f"{field} does not match {precision_text} precision; "
            f"use a value like {examples[precision_text]}"
        ) from error


def effective_date_bounds(value: str, precision: str) -> tuple[str, str]:
    """Return the UTC calendar interval represented by a normalized time."""

    if precision == "month":
        year, month = (int(part) for part in value.split("-"))
        last_day = calendar.monthrange(year, month)[1]
        return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"
    if precision == "day":
        return value, value
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    observed_date = parsed.date().isoformat()
    return observed_date, observed_date


def effective_datetime_interval(
    value: str,
    precision: str,
) -> tuple[datetime, datetime | None, str]:
    """Return the exact instant or UTC calendar interval for one Event Fact.

    Minute/second precision is an exact instant and therefore has no invented
    interval end. Day/month precision uses its complete UTC calendar interval
    with an exclusive end. The stored Event Fact remains unchanged.
    """
    normalized_value, normalized_precision = normalize_precise_time(
        value,
        precision,
        field="effective_at",
        required=True,
    )
    if normalized_precision in {"second", "minute"}:
        exact_text = (
            normalized_value
            if normalized_precision == "second"
            else normalized_value[:-1] + ":00Z"
        )
        exact = datetime.fromisoformat(exact_text[:-1] + "+00:00")
        return exact, None, "exact_instant"
    start_date, end_date = effective_date_bounds(
        normalized_value,
        normalized_precision,
    )
    start = datetime.fromisoformat(start_date + "T00:00:00+00:00")
    end_exclusive = (
        datetime.fromisoformat(end_date + "T00:00:00+00:00")
        + timedelta(days=1)
    )
    return start, end_exclusive, "effective_date_interval"


def _validate_https_url(value: Any, *, field: str) -> str:
    text = _clean_text(value, field=field, required=True)
    parsed = urlsplit(text)
    if (
        parsed.scheme.lower() != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise EventFactValidationError(
            f"{field} must be a public HTTPS URL without embedded credentials"
        )
    return text


def _source_record_path(value: Any, record_root: Path) -> tuple[str, Path]:
    relative_text = _clean_text(value, field="source_record_file", required=True)
    relative = Path(relative_text)
    if relative.is_absolute():
        raise EventFactValidationError(
            "source_record_file must be relative to the source-record root"
        )
    root = record_root.expanduser().resolve()
    resolved = (root / relative).resolve()
    try:
        normalized_relative = resolved.relative_to(root)
    except ValueError as error:
        raise EventFactValidationError(
            "source_record_file must remain inside the source-record root"
        ) from error
    if not resolved.is_file():
        raise EventFactValidationError(
            f"source_record_file does not exist: {normalized_relative.as_posix()}"
        )
    return normalized_relative.as_posix(), resolved


def _resolve_record_locator(record: Any, locator: str) -> Any:
    """Resolve a dot-delimited dict/list path and reject missing evidence."""

    parts = locator.split(".")
    if not parts or any(not part for part in parts):
        raise EventFactValidationError(
            "record_locator must be a dot-delimited path without empty segments"
        )
    current = record
    for part in parts:
        if isinstance(current, Mapping):
            if part not in current:
                raise EventFactValidationError(
                    f"record_locator does not exist in source_record_file: {locator}"
                )
            current = current[part]
        elif isinstance(current, list) and re.fullmatch(r"\d+", part):
            index = int(part)
            if index >= len(current):
                raise EventFactValidationError(
                    f"record_locator does not exist in source_record_file: {locator}"
                )
            current = current[index]
        else:
            raise EventFactValidationError(
                f"record_locator does not exist in source_record_file: {locator}"
            )
    if current is None or current == "":
        raise EventFactValidationError(
            f"record_locator resolves to an empty value: {locator}"
        )
    return current


def load_allowed_tokens(path: Path = DEFAULT_TOKEN_CONFIG) -> set[str]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if "token_symbol" not in set(reader.fieldnames or []):
            raise EventFactValidationError(
                f"{path.name} is missing the token_symbol column"
            )
        tokens = {
            (row.get("token_symbol") or "").strip().upper()
            for row in reader
            if (row.get("token_symbol") or "").strip()
        }
    if not tokens:
        raise EventFactValidationError("Token configuration contains no tokens")
    return tokens


def load_allowed_cex_market_ids(
    path: Path = DEFAULT_TOKEN_CONFIG,
) -> dict[str, set[str]]:
    """Return the exact primary/secondary CEX markets configured per token."""

    required_columns = {
        "token_symbol",
        "cex_symbol",
        "primary_cex",
        "secondary_cex",
    }
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(required_columns - set(reader.fieldnames or []))
        if missing:
            raise EventFactValidationError(
                f"{path.name} is missing CEX catalog columns: "
                + ", ".join(missing)
            )
        markets: dict[str, set[str]] = {}
        for row in reader:
            token_symbol = (row.get("token_symbol") or "").strip().upper()
            market_symbol = (row.get("cex_symbol") or "").strip()
            if not token_symbol:
                continue
            configured = markets.setdefault(token_symbol, set())
            if not market_symbol:
                continue
            for venue_column in ("primary_cex", "secondary_cex"):
                venue = (row.get(venue_column) or "").strip().lower()
                if venue:
                    configured.add(f"cex:{venue}:{market_symbol}")
    if not markets:
        raise EventFactValidationError("Token configuration contains no tokens")
    return markets


def read_curated_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        actual = list(reader.fieldnames or [])
        missing = sorted(set(CURATED_COLUMNS) - set(actual))
        extra = sorted(set(actual) - set(CURATED_COLUMNS))
        if missing or extra:
            problems = []
            if missing:
                problems.append("missing columns: " + ", ".join(missing))
            if extra:
                problems.append("unexpected columns: " + ", ".join(extra))
            raise EventFactValidationError(
                f"{path.name} has an invalid schema ({'; '.join(problems)})"
            )
        return [dict(row) for row in reader]


def normalize_event_row(
    candidate: Mapping[str, Any],
    *,
    row_number: int,
    allowed_tokens: set[str],
    allowed_cex_market_ids: Mapping[str, set[str]] | None = None,
    record_root: Path,
) -> dict[str, str]:
    try:
        event_id = _clean_text(
            candidate.get("event_id"),
            field="event_id",
            required=True,
        ).lower()
        if not _EVENT_ID_RE.fullmatch(event_id):
            raise EventFactValidationError(
                "event_id must be 3-128 lowercase letters, digits, dot, colon, "
                "underscore, or hyphen"
            )
        revision = _parse_positive_integer(
            candidate.get("revision"),
            field="revision",
        )
        token_symbol = _clean_text(
            candidate.get("token_symbol"),
            field="token_symbol",
            required=True,
        ).upper()
        if token_symbol not in allowed_tokens:
            raise EventFactValidationError(
                "token_symbol is not present in config/tokens.csv"
            )
        event_type = _clean_text(
            candidate.get("event_type"),
            field="event_type",
            required=True,
        ).lower()
        if event_type not in EVENT_TYPES:
            raise EventFactValidationError(
                f"event_type must be one of {', '.join(sorted(EVENT_TYPES))}"
            )
        event_subtype = _clean_text(
            candidate.get("event_subtype"),
            field="event_subtype",
            required=True,
        ).lower()
        if event_subtype not in EVENT_SUBTYPES[event_type]:
            raise EventFactValidationError(
                f"{event_type} event_subtype must be one of "
                f"{', '.join(sorted(EVENT_SUBTYPES[event_type]))}"
            )
        event_name = _clean_text(
            candidate.get("event_name"),
            field="event_name",
            required=True,
        )
        lifecycle = _clean_text(
            candidate.get("lifecycle"),
            field="lifecycle",
            required=True,
        ).lower()
        if lifecycle not in LIFECYCLES:
            raise EventFactValidationError(
                f"lifecycle must be one of {', '.join(sorted(LIFECYCLES))}"
            )

        announced_at, announced_precision = normalize_precise_time(
            candidate.get("announced_at"),
            candidate.get("announced_at_precision"),
            field="announced_at",
            required=False,
        )
        effective_at, effective_precision = normalize_precise_time(
            candidate.get("effective_at"),
            candidate.get("effective_at_precision"),
            field="effective_at",
            required=True,
        )
        if lifecycle == "occurred" and effective_precision == "month":
            raise EventFactValidationError(
                "occurred events require day, minute, or second precision"
            )

        amount_token = _canonical_decimal(
            candidate.get("amount_token"),
            field="amount_token",
        )
        amount_usd = _canonical_decimal(
            candidate.get("amount_usd"),
            field="amount_usd",
        )
        amount_usd_basis = _clean_text(
            candidate.get("amount_usd_basis"),
            field="amount_usd_basis",
        ).lower()
        if amount_usd and amount_usd_basis != "source_reported":
            raise EventFactValidationError(
                "amount_usd requires amount_usd_basis=source_reported"
            )
        if not amount_usd and amount_usd_basis:
            raise EventFactValidationError(
                "amount_usd_basis must be blank when amount_usd is blank"
            )
        percent_of_supply = _canonical_decimal(
            candidate.get("percent_of_supply"),
            field="percent_of_supply",
            maximum=Decimal("100"),
        )
        size_relation = _clean_text(
            candidate.get("size_relation"),
            field="size_relation",
            required=bool(amount_token or amount_usd or percent_of_supply),
        ).lower()
        if size_relation and size_relation not in SIZE_RELATIONS:
            raise EventFactValidationError(
                f"size_relation must be one of {', '.join(sorted(SIZE_RELATIONS))}"
            )
        if not (amount_token or amount_usd or percent_of_supply) and size_relation:
            raise EventFactValidationError(
                "size_relation must be blank when every size field is blank"
            )

        venue = _clean_text(candidate.get("venue"), field="venue")
        market_symbol = _clean_text(
            candidate.get("market_symbol"),
            field="market_symbol",
        )
        market_id = _clean_text(candidate.get("market_id"), field="market_id")
        chain = _clean_text(candidate.get("chain"), field="chain").lower()
        related_address = _clean_text(
            candidate.get("related_address"),
            field="related_address",
        )
        related_tx_hash = _clean_text(
            candidate.get("related_tx_hash"),
            field="related_tx_hash",
        )
        source_kind = _clean_text(
            candidate.get("source_kind"),
            field="source_kind",
            required=True,
        ).lower()
        if source_kind not in SOURCE_KINDS:
            raise EventFactValidationError(
                f"source_kind must be one of {', '.join(sorted(SOURCE_KINDS))}"
            )
        evidence_status = _clean_text(
            candidate.get("evidence_status"),
            field="evidence_status",
            required=True,
        ).lower()
        if evidence_status not in EVIDENCE_STATUSES:
            raise EventFactValidationError(
                "evidence_status must be one of "
                + ", ".join(sorted(EVIDENCE_STATUSES))
            )
        if event_type == "cex_listing":
            if source_kind != "official_exchange":
                raise EventFactValidationError(
                    "cex_listing requires an official_exchange source"
                )
            if not venue or not market_symbol or not market_id:
                raise EventFactValidationError(
                    "cex_listing requires venue, market_symbol, and market_id"
                )
            expected_market_id = f"cex:{venue.lower()}:{market_symbol}"
            if market_id != expected_market_id:
                raise EventFactValidationError(
                    f"cex_listing market_id must equal {expected_market_id}"
                )
            if (
                allowed_cex_market_ids is not None
                and market_id
                not in allowed_cex_market_ids.get(token_symbol, set())
            ):
                allowed = sorted(
                    allowed_cex_market_ids.get(token_symbol, set())
                )
                detail = ", ".join(allowed) if allowed else "none"
                raise EventFactValidationError(
                    "cex_listing market_id is not catalog-compatible for "
                    f"{token_symbol}; configured markets: {detail}"
                )
            if amount_token or amount_usd or percent_of_supply:
                raise EventFactValidationError(
                    "cex_listing amount fields must remain blank"
                )
        elif venue or market_symbol or market_id:
            raise EventFactValidationError(
                "venue, market_symbol, and market_id are only valid for cex_listing"
            )
        if event_type in {"unlock", "airdrop"} and source_kind == "official_exchange":
            raise EventFactValidationError(
                f"{event_type} requires project, governance, or onchain evidence"
            )
        if source_kind == "onchain_transaction" and (
            not chain or not related_tx_hash
        ):
            raise EventFactValidationError(
                "onchain_transaction evidence requires chain and related_tx_hash"
            )
        if (
            source_kind == "onchain_transaction"
            and evidence_status != "onchain_observed"
        ):
            raise EventFactValidationError(
                "onchain_transaction requires evidence_status=onchain_observed"
            )
        if (
            evidence_status == "onchain_observed"
            and source_kind != "onchain_transaction"
        ):
            raise EventFactValidationError(
                "evidence_status=onchain_observed requires an onchain_transaction source"
            )

        source_url = _validate_https_url(
            candidate.get("source_url"),
            field="source_url",
        )
        source_published_at, source_published_precision = normalize_precise_time(
            candidate.get("source_published_at"),
            candidate.get("source_published_at_precision"),
            field="source_published_at",
            required=False,
        )
        source_checked_at, checked_time = _parse_utc_second(
            candidate.get("source_checked_at_utc"),
            field="source_checked_at_utc",
        )
        source_record_file, source_record_path = _source_record_path(
            candidate.get("source_record_file"),
            record_root,
        )
        try:
            source_record = json.loads(
                source_record_path.read_text(encoding="utf-8")
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise EventFactValidationError(
                "source_record_file must be a readable JSON object"
            ) from error
        if not isinstance(source_record, dict):
            raise EventFactValidationError(
                "source_record_file must contain a JSON object"
            )
        if source_record.get("record_schema") != "source_check/v1":
            raise EventFactValidationError(
                "source_record_file must use record_schema=source_check/v1"
            )
        if source_record.get("source_url") != source_url:
            raise EventFactValidationError(
                "source_record_file source_url does not match the curated row"
            )
        if source_record.get("checked_at_utc") != source_checked_at:
            raise EventFactValidationError(
                "source_record_file checked_at_utc does not match the curated row"
            )
        record_locator = _clean_text(
            candidate.get("record_locator"),
            field="record_locator",
            required=True,
        )
        if not record_locator.startswith("facts."):
            raise EventFactValidationError(
                "record_locator must identify a fact below facts.*"
            )
        located_fact = _resolve_record_locator(source_record, record_locator)
        if (
            not isinstance(located_fact, Mapping)
            or not _clean_text(
                located_fact.get("statement"),
                field="source_record fact statement",
            )
        ):
            raise EventFactValidationError(
                "record_locator must resolve to a fact object with a statement"
            )
        supported_lifecycle = _clean_text(
            located_fact.get("supported_lifecycle"),
            field="source_record fact supported_lifecycle",
        ).lower()
        if supported_lifecycle:
            if supported_lifecycle not in LIFECYCLES:
                raise EventFactValidationError(
                    "source_record fact supported_lifecycle must be one of "
                    + ", ".join(sorted(LIFECYCLES))
                )
            if supported_lifecycle != lifecycle:
                raise EventFactValidationError(
                    "source_record fact supported_lifecycle does not match "
                    "the curated lifecycle"
                )
        recorded_at, recorded_time = _parse_utc_second(
            candidate.get("recorded_at_utc"),
            field="recorded_at_utc",
        )
        if recorded_time < checked_time:
            raise EventFactValidationError(
                "recorded_at_utc cannot precede source_checked_at_utc"
            )
        if lifecycle == "occurred":
            if effective_precision == "day":
                occurs_after_record = (
                    date.fromisoformat(effective_at) > recorded_time.date()
                )
            else:
                effective_time = datetime.fromisoformat(
                    effective_at[:-1] + "+00:00"
                )
                occurs_after_record = effective_time > recorded_time
            if occurs_after_record:
                raise EventFactValidationError(
                    "occurred event effective_at cannot follow recorded_at_utc"
                )
        revision_reason = _clean_text(
            candidate.get("revision_reason"),
            field="revision_reason",
            required=True,
        )
        if revision == 1 and revision_reason.lower() != "initial":
            raise EventFactValidationError(
                "revision 1 must use revision_reason=initial"
            )
        if revision > 1 and revision_reason.lower() == "initial":
            raise EventFactValidationError(
                "revision_reason=initial is only valid for revision 1"
            )
        notes = _clean_text(candidate.get("notes"), field="notes")

        return {
            "event_id": event_id,
            "revision": str(revision),
            "token_symbol": token_symbol,
            "event_type": event_type,
            "event_subtype": event_subtype,
            "event_name": event_name,
            "lifecycle": lifecycle,
            "announced_at": announced_at,
            "announced_at_precision": announced_precision,
            "effective_at": effective_at,
            "effective_at_precision": effective_precision,
            "amount_token": amount_token,
            "amount_usd": amount_usd,
            "amount_usd_basis": amount_usd_basis,
            "percent_of_supply": percent_of_supply,
            "size_relation": size_relation,
            "venue": venue,
            "market_symbol": market_symbol,
            "market_id": market_id,
            "chain": chain,
            "related_address": related_address,
            "related_tx_hash": related_tx_hash,
            "source_kind": source_kind,
            "evidence_status": evidence_status,
            "source_url": source_url,
            "source_published_at": source_published_at,
            "source_published_at_precision": source_published_precision,
            "source_checked_at_utc": source_checked_at,
            "source_record_file": source_record_file,
            "record_locator": record_locator,
            "recorded_at_utc": recorded_at,
            "revision_reason": revision_reason,
            "notes": notes,
            "record_sha256": sha256_file(source_record_path),
        }
    except EventFactValidationError as error:
        raise EventFactValidationError(
            f"curated CSV row {row_number}: {error}"
        ) from error


def _material_fingerprint(row: Mapping[str, str]) -> str:
    payload = {column: row.get(column, "") for column in MATERIAL_REVISION_COLUMNS}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def validate_revision_history(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    materialized = list(rows)
    by_event: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen_keys: set[tuple[str, int]] = set()
    for row in materialized:
        key = (row["event_id"], int(row["revision"]))
        if key in seen_keys:
            raise EventFactValidationError(
                f"duplicate event revision: {row['event_id']} revision {row['revision']}"
            )
        seen_keys.add(key)
        by_event[row["event_id"]].append(row)

    for event_id, revisions in by_event.items():
        revisions.sort(key=lambda item: int(item["revision"]))
        actual = [int(item["revision"]) for item in revisions]
        expected = list(range(1, len(revisions) + 1))
        if actual != expected:
            raise EventFactValidationError(
                f"{event_id} revisions must be contiguous from 1; found {actual}"
            )
        identity = {
            (item["token_symbol"], item["event_type"])
            for item in revisions
        }
        if len(identity) != 1:
            raise EventFactValidationError(
                f"{event_id} cannot change token_symbol or event_type across revisions"
            )
        prior_recorded: datetime | None = None
        prior_fingerprint: str | None = None
        for item in revisions:
            _, recorded = _parse_utc_second(
                item["recorded_at_utc"],
                field="recorded_at_utc",
            )
            if prior_recorded is not None and recorded <= prior_recorded:
                raise EventFactValidationError(
                    f"{event_id} recorded_at_utc must increase with each revision"
                )
            fingerprint = _material_fingerprint(item)
            if prior_fingerprint == fingerprint:
                raise EventFactValidationError(
                    f"{event_id} revision {item['revision']} has no material change"
                )
            prior_recorded = recorded
            prior_fingerprint = fingerprint
    return sorted(
        materialized,
        key=lambda item: (item["event_id"], int(item["revision"])),
    )


def normalize_event_rows(
    candidates: Iterable[Mapping[str, Any]],
    *,
    allowed_tokens: set[str],
    allowed_cex_market_ids: Mapping[str, set[str]] | None = None,
    record_root: Path,
    allow_empty: bool = False,
) -> list[dict[str, str]]:
    normalized = [
        normalize_event_row(
            candidate,
            row_number=row_number,
            allowed_tokens=allowed_tokens,
            allowed_cex_market_ids=allowed_cex_market_ids,
            record_root=record_root,
        )
        for row_number, candidate in enumerate(candidates, start=2)
    ]
    if not normalized and not allow_empty:
        raise EventFactValidationError(
            "curated Event Fact input is empty; refusing to publish an empty snapshot"
        )
    return validate_revision_history(normalized)


def latest_event_rows(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    latest: dict[str, dict[str, str]] = {}
    for row in rows:
        existing = latest.get(row["event_id"])
        if existing is None or int(row["revision"]) > int(existing["revision"]):
            latest[row["event_id"]] = row
    return sorted(
        latest.values(),
        key=lambda item: (
            effective_date_bounds(
                item["effective_at"],
                item["effective_at_precision"],
            )[0],
            item["token_symbol"],
            item["event_type"],
            item["event_id"],
        ),
    )


def _write_csv(path: Path, rows: Iterable[Mapping[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=OUTPUT_COLUMNS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def _build_database(path: Path, rows: Iterable[Mapping[str, str]]) -> None:
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    connection = sqlite3.connect(path)
    try:
        connection.executescript(schema)
        columns = ", ".join(OUTPUT_COLUMNS)
        placeholders = ", ".join("?" for _ in OUTPUT_COLUMNS)
        connection.executemany(
            f"INSERT INTO event_fact_revisions ({columns}) VALUES ({placeholders})",
            (
                tuple(row.get(column, "") for column in OUTPUT_COLUMNS)
                for row in rows
            ),
        )
        connection.commit()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise EventFactValidationError(
                f"event-facts SQLite integrity check failed: {integrity}"
            )
    finally:
        connection.close()


def _bundle_identity(rows: Iterable[Mapping[str, str]]) -> str:
    canonical = [
        {column: row.get(column, "") for column in OUTPUT_COLUMNS}
        for row in rows
    ]
    payload = {
        "schema": EVENT_FACT_SCHEMA,
        "rows": canonical,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _read_json_file(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EventFactValidationError(
            f"{label} is not readable JSON: {path}"
        ) from error
    if not isinstance(payload, dict):
        raise EventFactValidationError(f"{label} must be a JSON object")
    return payload


def _validate_bundle_inventory(
    bundle_path: Path,
    manifest: Mapping[str, Any],
    *,
    label: str,
) -> None:
    """Fail closed unless every published bundle artifact matches its manifest."""

    files = manifest.get("files")
    if not isinstance(files, dict):
        raise EventFactValidationError(f"{label} has no file inventory")
    for filename in (
        "event_fact_revisions.csv",
        "event_facts_latest.csv",
        "event_facts.sqlite3",
    ):
        details = files.get(filename)
        file_path = bundle_path / filename
        if (
            not isinstance(details, dict)
            or not file_path.is_file()
            or details.get("sha256") != sha256_file(file_path)
        ):
            raise EventFactValidationError(
                f"{label} file failed checksum validation: {filename}"
            )


def _read_bundle_csv(path: Path, *, label: str) -> list[dict[str, str]]:
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if list(reader.fieldnames or []) != OUTPUT_COLUMNS:
                raise EventFactValidationError(
                    f"{label} has an invalid schema"
                )
            return [dict(row) for row in reader]
    except (OSError, UnicodeDecodeError, csv.Error) as error:
        raise EventFactValidationError(f"{label} is not readable") from error


def _load_prior_revisions(output_root: Path) -> list[dict[str, str]]:
    """Load the exact prior publication so revision history is append-only."""

    pointer_path = output_root / "latest.json"
    if not pointer_path.exists():
        return []
    pointer = _read_json_file(pointer_path, label="prior Event Fact pointer")
    if pointer.get("schema") != EVENT_POINTER_SCHEMA:
        raise EventFactValidationError(
            "prior Event Fact pointer has an unsupported schema"
        )
    bundle_id = str(pointer.get("bundle_id") or "")
    if not _BUNDLE_ID_RE.fullmatch(bundle_id):
        raise EventFactValidationError(
            "prior Event Fact pointer has an invalid bundle_id"
        )
    bundles_root = (output_root / "bundles").resolve()
    bundle_path = (bundles_root / bundle_id).resolve()
    try:
        bundle_path.relative_to(bundles_root)
    except ValueError as error:
        raise EventFactValidationError(
            "prior Event Fact bundle escapes its root"
        ) from error
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.is_file():
        raise EventFactValidationError(
            "prior Event Fact bundle has no manifest"
        )
    if pointer.get("manifest_sha256") != sha256_file(manifest_path):
        raise EventFactValidationError(
            "prior Event Fact manifest checksum does not match its pointer"
        )
    manifest = _read_json_file(
        manifest_path,
        label="prior Event Fact manifest",
    )
    if (
        manifest.get("schema") != EVENT_BUNDLE_SCHEMA
        or manifest.get("fact_schema") != EVENT_FACT_SCHEMA
        or manifest.get("bundle_id") != bundle_id
    ):
        raise EventFactValidationError(
            "prior Event Fact manifest identity is invalid"
        )
    _validate_bundle_inventory(
        bundle_path,
        manifest,
        label="prior Event Fact bundle",
    )
    files = manifest["files"]
    revisions_path = bundle_path / "event_fact_revisions.csv"
    details = files[revisions_path.name]
    rows = _read_bundle_csv(
        revisions_path,
        label="prior Event Fact revision file",
    )
    if details.get("rows") != len(rows):
        raise EventFactValidationError(
            "prior Event Fact revision count does not match its manifest"
        )
    return rows


def enforce_append_only_revisions(
    prior_rows: Iterable[Mapping[str, str]],
    candidate_rows: Iterable[Mapping[str, str]],
) -> None:
    """Reject deletion or in-place mutation of any published revision."""

    prior_index = {
        (str(row["event_id"]), int(row["revision"])): {
            column: str(row.get(column, ""))
            for column in OUTPUT_COLUMNS
        }
        for row in prior_rows
    }
    candidate_index = {
        (str(row["event_id"]), int(row["revision"])): {
            column: str(row.get(column, ""))
            for column in OUTPUT_COLUMNS
        }
        for row in candidate_rows
    }
    missing = sorted(set(prior_index) - set(candidate_index))
    if missing:
        event_id, revision = missing[0]
        raise EventFactValidationError(
            f"published revision cannot be deleted: {event_id} revision {revision}"
        )
    for key, prior in prior_index.items():
        if candidate_index[key] != prior:
            event_id, revision = key
            raise EventFactValidationError(
                f"published revision is immutable: {event_id} revision {revision}"
            )


def build_event_bundle(
    input_path: Path,
    *,
    record_root: Path,
    output_root: Path,
    token_config: Path = DEFAULT_TOKEN_CONFIG,
) -> dict[str, Any]:
    """Validate all rows, create an immutable bundle, then move its pointer."""

    input_path = input_path.expanduser().resolve()
    record_root = record_root.expanduser().resolve()
    output_root = output_root.expanduser().resolve()
    candidates = read_curated_rows(input_path)
    allowed_tokens = load_allowed_tokens(token_config)
    allowed_cex_market_ids = load_allowed_cex_market_ids(token_config)
    rows = normalize_event_rows(
        candidates,
        allowed_tokens=allowed_tokens,
        allowed_cex_market_ids=allowed_cex_market_ids,
        record_root=record_root,
    )
    enforce_append_only_revisions(
        _load_prior_revisions(output_root),
        rows,
    )
    latest = latest_event_rows(rows)
    covered_tokens = sorted({row["token_symbol"] for row in latest})
    uncovered_tokens = sorted(allowed_tokens - set(covered_tokens))
    bundle_id = _bundle_identity(rows)
    bundles_root = output_root / "bundles"
    bundle_path = bundles_root / bundle_id
    bundles_root.mkdir(parents=True, exist_ok=True)

    if not bundle_path.exists():
        temporary_path = Path(
            tempfile.mkdtemp(prefix=f".{bundle_id}.", dir=str(bundles_root))
        )
        try:
            revisions_path = temporary_path / "event_fact_revisions.csv"
            latest_path = temporary_path / "event_facts_latest.csv"
            database_path = temporary_path / "event_facts.sqlite3"
            _write_csv(revisions_path, rows)
            _write_csv(latest_path, latest)
            _build_database(database_path, rows)

            manifest = {
                "schema": EVENT_BUNDLE_SCHEMA,
                "fact_schema": EVENT_FACT_SCHEMA,
                "bundle_id": bundle_id,
                "built_at_utc": utc_now_text(),
                "input": {
                    "filename": input_path.name,
                    "sha256": sha256_file(input_path),
                },
                "revision_count": len(rows),
                "event_count": len(latest),
                "token_count": len(covered_tokens),
                "configured_token_count": len(allowed_tokens),
                "covered_tokens": covered_tokens,
                "uncovered_tokens": uncovered_tokens,
                "event_type_counts": dict(
                    sorted(Counter(row["event_type"] for row in latest).items())
                ),
                "lifecycle_counts": dict(
                    sorted(Counter(row["lifecycle"] for row in latest).items())
                ),
                "evidence_status_counts": dict(
                    sorted(
                        Counter(row["evidence_status"] for row in latest).items()
                    )
                ),
                "precision_counts": dict(
                    sorted(
                        Counter(
                            row["effective_at_precision"] for row in latest
                        ).items()
                    )
                ),
                "files": {
                    revisions_path.name: {
                        "sha256": sha256_file(revisions_path),
                        "rows": len(rows),
                    },
                    latest_path.name: {
                        "sha256": sha256_file(latest_path),
                        "rows": len(latest),
                    },
                    database_path.name: {
                        "sha256": sha256_file(database_path),
                    },
                },
            }
            manifest_path = temporary_path / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary_path, bundle_path)
        except BaseException:
            shutil.rmtree(temporary_path, ignore_errors=True)
            raise

    manifest_path = bundle_path / "manifest.json"
    manifest = _read_json_file(
        manifest_path,
        label=f"existing Event Fact bundle {bundle_id} manifest",
    )
    if (
        manifest.get("schema") != EVENT_BUNDLE_SCHEMA
        or manifest.get("fact_schema") != EVENT_FACT_SCHEMA
        or manifest.get("bundle_id") != bundle_id
    ):
        raise EventFactValidationError(
            f"existing Event Fact bundle {bundle_id} has an invalid manifest"
        )
    _validate_bundle_inventory(
        bundle_path,
        manifest,
        label=f"existing Event Fact bundle {bundle_id}",
    )
    if (
        manifest.get("revision_count") != len(rows)
        or manifest.get("event_count") != len(latest)
    ):
        raise EventFactValidationError(
            f"existing Event Fact bundle {bundle_id} has invalid row counts"
        )
    existing_revisions = _read_bundle_csv(
        bundle_path / "event_fact_revisions.csv",
        label=f"existing Event Fact bundle {bundle_id} revisions",
    )
    existing_latest = _read_bundle_csv(
        bundle_path / "event_facts_latest.csv",
        label=f"existing Event Fact bundle {bundle_id} latest rows",
    )
    if existing_revisions != rows or existing_latest != latest:
        raise EventFactValidationError(
            f"existing Event Fact bundle {bundle_id} content does not match "
            "the validated input"
        )
    _atomic_write_json(
        output_root / "latest.json",
        {
            "schema": EVENT_POINTER_SCHEMA,
            "bundle_id": bundle_id,
            "manifest_sha256": sha256_file(manifest_path),
        },
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and build a source-backed Event Fact bundle",
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--record-root", type=Path, default=DEFAULT_RECORD_ROOT)
    parser.add_argument("--token-config", type=Path, default=DEFAULT_TOKEN_CONFIG)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--publish-local",
        action="store_true",
        help="Publish the validated bundle under data/local/events",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_root = DEFAULT_PUBLISH_ROOT if args.publish_local else args.output_root
    manifest = build_event_bundle(
        args.input,
        record_root=args.record_root,
        output_root=output_root,
        token_config=args.token_config,
    )
    print(
        json.dumps(
            {
                "bundle_id": manifest["bundle_id"],
                "event_count": manifest["event_count"],
                "revision_count": manifest["revision_count"],
                "output_root": str(output_root),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
