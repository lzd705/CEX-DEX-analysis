"""Read validated Event Fact bundles and project the public API contract.

The main server can call this module without inheriting any event-study logic.
All rows come from the latest immutable bundle built by
``scripts/event_facts.py``.  The API projection keeps exact Decimal text,
explicit time precision and lifecycle, and source/revision lineage.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.event_facts import (  # noqa: E402
    EVENT_BUNDLE_SCHEMA,
    EVENT_FACT_SCHEMA,
    EVENT_POINTER_SCHEMA,
    LIFECYCLES,
    effective_date_bounds,
    sha256_file,
)


EVENT_API_SCHEMA = "event_facts_api/v1"
_BUNDLE_ID_RE = re.compile(r"^[0-9a-f]{24}$")


class EventBundleError(ValueError):
    """Raised when the published Event Fact pointer or bundle is invalid."""


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EventBundleError(f"{label} is not readable JSON: {path}") from error
    if not isinstance(payload, dict):
        raise EventBundleError(f"{label} must be a JSON object")
    return payload


def resolve_event_bundle(event_root: Path) -> dict[str, Any]:
    """Resolve and checksum-validate the exact bundle named by latest.json."""

    event_root = event_root.expanduser().resolve()
    pointer_path = event_root / "latest.json"
    if not pointer_path.exists():
        raise FileNotFoundError(f"No published Event Fact pointer at {pointer_path}")
    pointer = _read_json_object(pointer_path, label="Event Fact pointer")
    if pointer.get("schema") != EVENT_POINTER_SCHEMA:
        raise EventBundleError("Event Fact pointer has an unsupported schema")
    bundle_id = str(pointer.get("bundle_id") or "")
    if not _BUNDLE_ID_RE.fullmatch(bundle_id):
        raise EventBundleError("Event Fact pointer has an invalid bundle_id")

    bundle_path = (event_root / "bundles" / bundle_id).resolve()
    try:
        bundle_path.relative_to((event_root / "bundles").resolve())
    except ValueError as error:
        raise EventBundleError("Event Fact bundle escapes its root") from error
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.is_file():
        raise EventBundleError("Event Fact bundle has no manifest")
    manifest_sha256 = sha256_file(manifest_path)
    if pointer.get("manifest_sha256") != manifest_sha256:
        raise EventBundleError("Event Fact manifest checksum does not match pointer")

    manifest = _read_json_object(manifest_path, label="Event Fact manifest")
    if (
        manifest.get("schema") != EVENT_BUNDLE_SCHEMA
        or manifest.get("fact_schema") != EVENT_FACT_SCHEMA
        or manifest.get("bundle_id") != bundle_id
    ):
        raise EventBundleError("Event Fact manifest identity is invalid")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise EventBundleError("Event Fact manifest has no file inventory")
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
            raise EventBundleError(
                f"Event Fact bundle file failed checksum validation: {filename}"
            )
    return {
        "root": event_root,
        "path": bundle_path,
        "pointer": pointer,
        "manifest": manifest,
        "database_path": bundle_path / "event_facts.sqlite3",
    }


def load_latest_event_rows(event_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    bundle = resolve_event_bundle(event_root)
    database_path = bundle["database_path"]
    connection = sqlite3.connect(
        f"{database_path.resolve().as_uri()}?mode=ro",
        uri=True,
    )
    connection.row_factory = sqlite3.Row
    try:
        rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT *
                FROM event_facts_latest
                ORDER BY effective_at, token_symbol, event_type, event_id
                """
            ).fetchall()
        ]
    except sqlite3.Error as error:
        raise EventBundleError("Event Fact database cannot be queried") from error
    finally:
        connection.close()
    expected_count = bundle["manifest"].get("event_count")
    if expected_count != len(rows):
        raise EventBundleError(
            "Event Fact database row count does not match its manifest"
        )
    covered_tokens = sorted({str(row["token_symbol"]) for row in rows})
    manifest_covered = bundle["manifest"].get("covered_tokens")
    manifest_uncovered = bundle["manifest"].get("uncovered_tokens")
    configured_count = bundle["manifest"].get("configured_token_count")
    if manifest_covered is not None:
        if manifest_covered != covered_tokens:
            raise EventBundleError(
                "Event Fact covered-token inventory does not match its rows"
            )
        valid_uncovered = (
            isinstance(manifest_uncovered, list)
            and all(
                isinstance(token, str) and token
                for token in manifest_uncovered
            )
            and manifest_uncovered == sorted(set(manifest_uncovered))
        )
        if (
            not valid_uncovered
            or not isinstance(configured_count, int)
            or set(manifest_covered) & set(manifest_uncovered)
            or configured_count != len(manifest_covered) + len(manifest_uncovered)
        ):
            raise EventBundleError(
                "Event Fact configured-token coverage is internally inconsistent"
            )
    return rows, bundle["manifest"]


def _optional_text(value: Any) -> str | None:
    text = "" if value is None else str(value)
    return text if text else None


def public_event_row(row: Mapping[str, Any]) -> dict[str, Any]:
    start_date, end_date = effective_date_bounds(
        str(row["effective_at"]),
        str(row["effective_at_precision"]),
    )
    return {
        "event_id": row["event_id"],
        "revision": int(row["revision"]),
        "token_symbol": row["token_symbol"],
        "event_type": row["event_type"],
        "event_subtype": row["event_subtype"],
        "event_name": row["event_name"],
        "lifecycle": row["lifecycle"],
        "evidence_status": row["evidence_status"],
        "time": {
            "announced_at": _optional_text(row["announced_at"]),
            "announced_at_precision": _optional_text(
                row["announced_at_precision"]
            ),
            "effective_at": row["effective_at"],
            "effective_at_precision": row["effective_at_precision"],
            "effective_date_start": start_date,
            "effective_date_end": end_date,
        },
        "size": {
            "amount_token": _optional_text(row["amount_token"]),
            "amount_usd": _optional_text(row["amount_usd"]),
            "amount_usd_basis": _optional_text(row["amount_usd_basis"]),
            "percent_of_supply": _optional_text(row["percent_of_supply"]),
            "relation": _optional_text(row["size_relation"]),
        },
        "market": {
            "venue": _optional_text(row["venue"]),
            "market_symbol": _optional_text(row["market_symbol"]),
            "market_id": _optional_text(row["market_id"]),
        },
        "onchain": {
            "chain": _optional_text(row["chain"]),
            "related_address": _optional_text(row["related_address"]),
            "related_tx_hash": _optional_text(row["related_tx_hash"]),
        },
        "source": {
            "kind": row["source_kind"],
            "url": row["source_url"],
            "published_at": _optional_text(row["source_published_at"]),
            "published_at_precision": _optional_text(
                row["source_published_at_precision"]
            ),
            "checked_at_utc": row["source_checked_at_utc"],
            "record_sha256": row["record_sha256"],
            "record_locator": row["record_locator"],
        },
        "revision_lineage": {
            "recorded_at_utc": row["recorded_at_utc"],
            "reason": row["revision_reason"],
        },
        "notes": _optional_text(row["notes"]),
    }


def build_event_payload(
    rows: Iterable[Mapping[str, Any]],
    *,
    manifest: Mapping[str, Any],
    token: str | None = None,
    start: str | None = None,
    end: str | None = None,
    lifecycle: str | None = None,
) -> dict[str, Any]:
    """Filter latest facts by overlapping effective-date interval."""

    source_rows = list(rows)
    normalized_token = token.strip().upper() if token else None
    start_date = date.fromisoformat(start).isoformat() if start else None
    end_date = date.fromisoformat(end).isoformat() if end else None
    if start_date and end_date and end_date < start_date:
        raise ValueError("end must be on or after start")
    normalized_lifecycle = lifecycle.strip().lower() if lifecycle else None
    if normalized_lifecycle and normalized_lifecycle not in LIFECYCLES:
        raise ValueError(
            "lifecycle must be one of " + ", ".join(sorted(LIFECYCLES))
        )

    events = []
    for raw_row in source_rows:
        if normalized_token and raw_row.get("token_symbol") != normalized_token:
            continue
        if normalized_lifecycle and raw_row.get("lifecycle") != normalized_lifecycle:
            continue
        event_start, event_end = effective_date_bounds(
            str(raw_row["effective_at"]),
            str(raw_row["effective_at_precision"]),
        )
        if start_date and event_end < start_date:
            continue
        if end_date and event_start > end_date:
            continue
        events.append(public_event_row(raw_row))

    events.sort(
        key=lambda event: (
            event["time"]["effective_date_start"],
            event["token_symbol"],
            event["event_type"],
            event["event_id"],
        )
    )
    covered_tokens = list(manifest.get("covered_tokens") or sorted({
        str(row["token_symbol"])
        for row in source_rows
    }))
    uncovered_tokens = list(manifest.get("uncovered_tokens") or [])
    return {
        "schema": EVENT_API_SCHEMA,
        "fact_schema": EVENT_FACT_SCHEMA,
        "fact_boundary": (
            "Source-backed event facts only. No return, market-impact, "
            "importance, sentiment, or causal result is included."
        ),
        "bundle_id": manifest.get("bundle_id"),
        "built_at_utc": manifest.get("built_at_utc"),
        "coverage": {
            "configured_token_count": manifest.get("configured_token_count"),
            "covered_token_count": len(covered_tokens),
            "covered_tokens": covered_tokens,
            "uncovered_tokens": uncovered_tokens,
            "query_token_has_published_fact": (
                normalized_token in set(covered_tokens)
                if normalized_token
                else None
            ),
        },
        "query": {
            "token": normalized_token,
            "start": start_date,
            "end": end_date,
            "lifecycle": normalized_lifecycle,
        },
        "event_count": len(events),
        "event_type_counts": dict(
            sorted(Counter(event["event_type"] for event in events).items())
        ),
        "lifecycle_counts": dict(
            sorted(Counter(event["lifecycle"] for event in events).items())
        ),
        "evidence_status_counts": dict(
            sorted(
                Counter(event["evidence_status"] for event in events).items()
            )
        ),
        "events": events,
    }
