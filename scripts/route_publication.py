"""Immutable publication boundary for normalized route-cohort core bundles.

The route collector owns raw evidence and timing classification.  This module
owns the later, deliberately smaller trust boundary: normalize that cohort,
write the three audit inventories and an indexed SQLite copy, validate every
representation against every other representation, then move only the private
``routes/core/latest.json`` pointer.
"""

from __future__ import annotations

from collections import Counter
import ctypes
import csv
from datetime import date
import errno
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import sys
import tempfile
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

try:
    from scripts.route_cohort import (
        canonical_route_id,
        classify_route_timing,
        validate_route_cohort_rows,
    )
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from route_cohort import (  # type: ignore[no-redef]
        canonical_route_id,
        classify_route_timing,
        validate_route_cohort_rows,
    )
    from timestamp_contract import (  # type: ignore[no-redef]
        exact_rfc3339_epoch_seconds,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROUTE_CORE_ROOT = PROJECT_ROOT / "data/local/routes/core"

ROUTE_COHORT_SCHEMA = "route_cohort_collection/v1"
ROUTE_CORE_BUNDLE_STAGE = "route_cohort_core/v1"
ROUTE_CORE_MANIFEST_SCHEMA = "route_cohort_core_manifest/v1"
ROUTE_CORE_POINTER_SCHEMA = "route_cohort_core_pointer/v1"
ROUTE_CANDIDATE_CSV_SCHEMA = "route_candidates/v1"
ROUTE_LEG_CSV_SCHEMA = "route_legs/v1"
ROUTE_TIMING_CSV_SCHEMA = "route_timing/v1"
ROUTE_SQLITE_LOGICAL_SCHEMA = "route_cohort_sqlite/v1"

ROUTE_CANDIDATES_FILENAME = "route_candidates.csv"
ROUTE_LEGS_FILENAME = "route_legs.csv"
ROUTE_TIMING_FILENAME = "route_timing.csv"
ROUTE_SQLITE_FILENAME = "route_cohort.sqlite3"
MANIFEST_FILENAME = "manifest.json"

ROUTE_CORE_FILENAMES = frozenset({
    ROUTE_CANDIDATES_FILENAME,
    ROUTE_LEGS_FILENAME,
    ROUTE_TIMING_FILENAME,
    ROUTE_SQLITE_FILENAME,
    MANIFEST_FILENAME,
})
_MANIFEST_ARTIFACT_FILENAMES = frozenset(
    ROUTE_CORE_FILENAMES - {MANIFEST_FILENAME}
)

REQUESTED_NOTIONALS_USD = [1000, 5000, 10000, 50000, 100000]
_COHORT_ID = re.compile(r"cohort:[0-9a-f]{64}\Z")
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_CEX_MARKET_ID = re.compile(
    r"cex:([a-z0-9][a-z0-9._-]{0,63}):"
    r"([A-Z0-9][A-Z0-9._-]{0,63})/"
    r"([A-Z0-9][A-Z0-9._-]{0,63})\Z",
    flags=re.ASCII,
)
_DEX_MARKET_ID = re.compile(
    r"dex:([a-z0-9][a-z0-9._-]{0,63}):"
    r"([a-z0-9][a-z0-9._-]{0,127}):"
    r"([A-Za-z0-9][A-Za-z0-9._-]{0,255}):"
    r"([A-Z0-9][A-Z0-9._-]{0,63})\Z",
    flags=re.ASCII,
)
_CANONICAL_UTC_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?Z\Z"
)
_ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")

_ROUTE_MODES = frozenset({
    "prepositioned_inventory",
    "atomic_onchain",
    "research_only",
})
_ROUTE_CLASSES = frozenset({"candidate", "research_only"})
_TIMING_STATUSES = frozenset({"within_sla", "outside_sla", "unavailable"})
_TIMING_REASONS = frozenset({
    "route_deadline_exceeded",
    "execution_adapter_unsupported",
    "buy_leg_unavailable",
    "sell_leg_unavailable",
    "invalid_state_timestamp",
    "snapshot_skew_exceeded",
    "route_mode_not_executable",
})
_LEG_STATUSES = frozenset({
    "observed",
    "partial",
    "unsupported",
    "failed",
    "deadline_exceeded",
})
_LEG_REASONS = frozenset({
    "observed",
    "source_level_limit",
    "source_no_two_sided_book",
    "source_no_order_book",
    "source_invalid_order_book",
    "not_listed",
    "rate_limit",
    "source_unavailable",
    "source_rejected_request",
    "network",
    "parse",
    "unsupported_source",
    "collection_failed",
    "fixed_block_unavailable",
    "fixed_block_lineage_mismatch",
    "collector_identity_mismatch",
    "raw_evidence_missing",
    "raw_evidence_hash_mismatch",
    "raw_evidence_path_unsafe",
    "route_deadline_exceeded",
})

_TOP_LEVEL_FIELDS = frozenset({
    "schema",
    "candidate_source_generation",
    "collection_input_generation",
    "source_state",
    "raw_evidence_run_id",
    "target_observed_at",
    "collection_started_at",
    "collection_completed_at",
    "collection_deadline_at",
    "skew_sla_seconds",
    "route_age_sla_seconds",
    "selection_window",
    "requested_notionals_usd",
    "legs",
    "routes",
    "route_rows",
    "route_cohort_id",
    "fingerprint",
})
_ROUTE_FIELDS = frozenset({
    "token_symbol",
    "buy_market_id",
    "sell_market_id",
    "route_mode",
    "route_id",
    "route_class",
    "settlement_reason",
    "requested_notionals_usd",
    "candidate_source_generation",
})

CANDIDATE_COLUMNS = (
    "route_cohort_id",
    "route_id",
    "token_symbol",
    "buy_market_id",
    "sell_market_id",
    "route_mode",
    "route_class",
    "settlement_reason",
    "requested_notionals_usd",
    "candidate_source_generation",
    "row_json",
)
LEG_COLUMNS = (
    "route_cohort_id",
    "leg_id",
    "market_id",
    "market_type",
    "token_symbol",
    "status",
    "available",
    "reason_code",
    "state_observed_at",
    "snapshot_id",
    "source_endpoint",
    "raw_response_sha256",
    "fixed_block_number",
    "fixed_block_timestamp",
    "row_json",
)
TIMING_COLUMNS = (
    "route_cohort_id",
    "route_id",
    "skew_seconds",
    "timing_status",
    "reason_code",
    "row_json",
)

_MAX_JSON_BYTES = 16 * 1024 * 1024
_MAX_CSV_BYTES = 128 * 1024 * 1024
_MAX_SQLITE_BYTES = 512 * 1024 * 1024


class RoutePublicationError(ValueError):
    """Raised when a route cohort, bundle, or private pointer is invalid."""


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("route cohort contains invalid JSON data") from error


def _canonical_json_text(value: Any) -> str:
    return _canonical_json_bytes(value).decode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise RoutePublicationError(
            "route bundle file cannot be hashed: {}".format(path.name)
        ) from error
    return digest.hexdigest()


def _logical_rows_sha256(schema: str, rows: Sequence[Mapping[str, Any]]) -> str:
    return _sha256_bytes(_canonical_json_bytes({"schema": schema, "rows": rows}))


def _database_logical_sha256(cohort: Mapping[str, Any]) -> str:
    return _sha256_bytes(_canonical_json_bytes({
        "schema": ROUTE_SQLITE_LOGICAL_SCHEMA,
        "cohort": cohort,
    }))


def _nonempty_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RoutePublicationError("{} is invalid".format(field))
    return value


def _matches_requested_notional_grid(value: Any) -> bool:
    return (
        isinstance(value, list)
        and len(value) == len(REQUESTED_NOTIONALS_USD)
        and all(
            type(actual) is int and actual == expected
            for actual, expected in zip(value, REQUESTED_NOTIONALS_USD)
        )
    )


def _canonical_market_token(market_id: Any) -> str:
    if not isinstance(market_id, str) or market_id != market_id.strip():
        raise RoutePublicationError("route market identity is invalid")
    cex_match = _CEX_MARKET_ID.fullmatch(market_id)
    if cex_match is not None:
        return cex_match.group(2)
    dex_match = _DEX_MARKET_ID.fullmatch(market_id)
    if dex_match is not None:
        pool = dex_match.group(3)
        if pool.startswith("0x") and pool != pool.lower():
            raise RoutePublicationError("route market identity is invalid")
        return dex_match.group(4)
    raise RoutePublicationError("route market identity is invalid")


def _validate_timestamp(value: Any, field: str) -> str:
    text = _nonempty_text(value, field)
    if _CANONICAL_UTC_TIMESTAMP.fullmatch(text) is None:
        raise RoutePublicationError(
            "{} must be a canonical UTC timestamp ending in Z".format(field)
        )
    try:
        exact_rfc3339_epoch_seconds(text)
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("{} is invalid".format(field)) from error
    return text


def _clone_json(value: Any) -> Any:
    """Detach caller-owned state while rejecting NaN and non-JSON values."""
    return json.loads(_canonical_json_text(value))


def _safe_network_endpoint(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return False
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return False
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return False
    safe_host = "[{}]".format(hostname) if ":" in hostname else hostname
    if port is not None:
        safe_host = "{}:{}".format(safe_host, port)
    return urlunsplit(
        (parsed.scheme, safe_host, parsed.path, "", "")
    ) == value


def _unsafe_evidence_string(value: str) -> bool:
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return True
    if value == "~" or re.match(r"^~[^/\\]*[/\\]", value):
        return True
    if value.startswith(("./", "../", ".\\", "..\\")):
        return True
    if any(segment in {".", ".."} for segment in re.split(r"[/\\]", value)):
        return True
    if (
        os.path.isabs(value)
        or value.startswith("\\")
        or re.match(r"^[A-Za-z]:[\\/]", value, flags=re.ASCII) is not None
        or value.lower().startswith("file:")
    ):
        return True
    try:
        parsed = urlsplit(value)
    except ValueError:
        return True
    scheme = parsed.scheme.lower()
    if scheme in {"http", "https"}:
        return not _safe_network_endpoint(value)
    return bool(
        parsed.scheme
        and (
            parsed.netloc
            or "://" in value
            or "@" in parsed.path
            or parsed.query
            or parsed.fragment
        )
    )


def _sensitive_evidence_key(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", value.lower())
    if normalized == "path" or normalized.endswith("path"):
        return True
    if normalized in {
        "auth",
        "authorization",
        "bearer",
        "cookie",
        "credential",
        "error",
        "exception",
        "password",
        "privatekey",
        "rawpath",
        "secret",
        "session",
        "sessionid",
        "setcookie",
        "signature",
        "token",
        "traceback",
    }:
        return True
    return any(
        marker in normalized
        for marker in (
            "accesstoken",
            "apikey",
            "authorization",
            "cookie",
            "credential",
            "password",
            "privatekey",
            "refreshtoken",
            "secret",
            "session",
            "signature",
        )
    )


def _forbidden_row_keys(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if not isinstance(key, str):
                return True
            lowered = key.lower()
            if _sensitive_evidence_key(key) or _unsafe_evidence_string(key):
                return True
            if isinstance(nested, str):
                if _unsafe_evidence_string(nested):
                    return True
                endpoint_key = lowered in {"url", "endpoint"} or lowered.endswith(
                    ("_url", "_endpoint")
                )
                if endpoint_key and nested and not _safe_network_endpoint(nested):
                    return True
                if nested.startswith(("http://", "https://", "ws://", "wss://")):
                    if not _safe_network_endpoint(nested):
                        return True
            elif (
                nested not in (None, "")
                and (
                    lowered in {"url", "endpoint"}
                    or lowered.endswith(("_url", "_endpoint"))
                )
            ):
                return True
            if _forbidden_row_keys(nested):
                return True
    elif isinstance(value, list):
        return any(_forbidden_row_keys(item) for item in value)
    return False


def _validate_route_candidate(
    route: Mapping[str, Any],
    *,
    candidate_generation: str,
    requested_notionals: Sequence[int]
) -> str:
    if set(route) != _ROUTE_FIELDS:
        raise RoutePublicationError("route candidate schema is invalid")
    try:
        route_id = canonical_route_id(route)
    except ValueError as error:
        raise RoutePublicationError(str(error)) from error
    if route.get("route_id") != route_id:
        raise RoutePublicationError("route_id must be canonical")
    mode = route.get("route_mode")
    route_class = route.get("route_class")
    settlement_reason = route.get("settlement_reason")
    if mode not in _ROUTE_MODES or route_class not in _ROUTE_CLASSES:
        raise RoutePublicationError("route candidate enum is invalid")
    if mode == "research_only":
        if (
            route_class != "research_only"
            or settlement_reason != "unsupported_cross_chain_settlement"
        ):
            raise RoutePublicationError("route settlement lineage is invalid")
    elif route_class != "candidate" or settlement_reason is not None:
        raise RoutePublicationError("route settlement lineage is invalid")
    if route.get("candidate_source_generation") != candidate_generation:
        raise RoutePublicationError("candidate source lineage conflict")
    if (
        not _matches_requested_notional_grid(route.get("requested_notionals_usd"))
        or list(requested_notionals) != REQUESTED_NOTIONALS_USD
    ):
        raise RoutePublicationError("route requested notional lineage conflict")

    buy = str(route["buy_market_id"])
    sell = str(route["sell_market_id"])
    if (
        _canonical_market_token(buy) != route["token_symbol"]
        or _canonical_market_token(sell) != route["token_symbol"]
    ):
        raise RoutePublicationError("route market identity token is invalid")
    if mode == "prepositioned_inventory":
        if not (buy.startswith("cex:") or sell.startswith("cex:")):
            raise RoutePublicationError("route mode lineage is invalid")
    elif mode == "atomic_onchain":
        if not (buy.startswith("dex:") and sell.startswith("dex:")):
            raise RoutePublicationError("route mode lineage is invalid")
        if buy.split(":", 2)[1] != sell.split(":", 2)[1]:
            raise RoutePublicationError("route mode lineage is invalid")
    else:
        if not (buy.startswith("dex:") and sell.startswith("dex:")):
            raise RoutePublicationError("route mode lineage is invalid")
        if buy.split(":", 2)[1] == sell.split(":", 2)[1]:
            raise RoutePublicationError("route mode lineage is invalid")
    return route_id


def _validate_leg_rows(
    legs: Sequence[Mapping[str, Any]],
    *,
    raw_evidence_run_id: str,
    collection_completed_at: str,
    collection_deadline_at: str
) -> Dict[str, Mapping[str, Any]]:
    rows_by_market: Dict[str, Mapping[str, Any]] = {}
    token_by_market: Dict[str, str] = {}
    fixed_lineage: Dict[str, List[Optional[Tuple[str, str]]]] = {}
    completed_epoch = exact_rfc3339_epoch_seconds(collection_completed_at)
    fixed_timestamp_bound = min(
        completed_epoch,
        exact_rfc3339_epoch_seconds(collection_deadline_at),
    )
    for row in legs:
        if not isinstance(row, Mapping):
            raise RoutePublicationError("route leg must be a mapping")
        if _forbidden_row_keys(row):
            raise RoutePublicationError("route leg contains unsafe evidence")
        leg_id = _nonempty_text(row.get("leg_id"), "route leg identity")
        market_id = _nonempty_text(row.get("market_id"), "route leg identity")
        if leg_id != market_id:
            raise RoutePublicationError("route leg identity is inconsistent")
        if market_id in rows_by_market:
            raise RoutePublicationError("duplicate route leg")
        _canonical_market_token(market_id)
        if market_id.startswith("cex:"):
            inferred_type = "cex"
        elif market_id.startswith("dex:"):
            inferred_type = "dex"
        else:
            raise RoutePublicationError("route leg market type is invalid")
        if row.get("market_type") not in (None, "", inferred_type):
            raise RoutePublicationError("route leg market type is invalid")
        status_value = row.get("status")
        status_text = "" if status_value is None else status_value
        if status_text not in _LEG_STATUSES:
            raise RoutePublicationError("route leg status enum is invalid")
        available = row.get("available")
        if available is not None and type(available) is not bool:
            raise RoutePublicationError("route leg availability is invalid")
        if status_text in {"observed", "partial"} and available is False:
            raise RoutePublicationError("route leg availability conflicts with status")
        if status_text in {"unsupported", "failed", "deadline_exceeded"} and available is True:
            raise RoutePublicationError("route leg availability conflicts with status")
        reason = row.get("reason_code")
        if reason not in _LEG_REASONS | {None, ""}:
            raise RoutePublicationError("route leg reason enum is invalid")
        snapshot_id = row.get("snapshot_id")
        if snapshot_id not in (None, "", raw_evidence_run_id):
            raise RoutePublicationError("route leg snapshot lineage conflict")
        observed_at_value = row.get("state_observed_at")
        observed_at: Optional[str] = None
        if observed_at_value not in (None, ""):
            observed_at = _validate_timestamp(
                observed_at_value, "route leg state_observed_at"
            )
            if exact_rfc3339_epoch_seconds(observed_at) > completed_epoch:
                raise RoutePublicationError("route leg state timestamp is in the future")
        raw_hash = row.get("raw_response_sha256")
        if raw_hash not in (None, "") and (
            not isinstance(raw_hash, str)
            or _HEX_SHA256.fullmatch(raw_hash) is None
        ):
            raise RoutePublicationError("route leg raw evidence hash is invalid")
        if status_text in {"observed", "partial"}:
            if snapshot_id != raw_evidence_run_id:
                raise RoutePublicationError("route leg snapshot lineage conflict")
            if observed_at is None:
                raise RoutePublicationError("route leg state_observed_at is empty")
            if not isinstance(raw_hash, str) or _HEX_SHA256.fullmatch(raw_hash) is None:
                raise RoutePublicationError("route leg raw evidence hash is invalid")
        endpoint = row.get("source_endpoint")
        if endpoint not in (None, ""):
            if not _safe_network_endpoint(endpoint):
                raise RoutePublicationError("route leg contains unsafe evidence")
        token = row.get("token_symbol")
        if token not in (None, ""):
            token_by_market[market_id] = _nonempty_text(token, "route leg token")

        block_number = row.get("fixed_block_number")
        block_timestamp = row.get("fixed_block_timestamp")
        if (block_number in (None, "")) != (block_timestamp in (None, "")):
            raise RoutePublicationError("fixed block lineage is incomplete")
        if (
            inferred_type == "dex"
            and status_text in {"observed", "partial"}
            and block_number in (None, "")
        ):
            raise RoutePublicationError(
                "observed DEX leg requires fixed block lineage"
            )
        if block_number not in (None, ""):
            if inferred_type != "dex":
                raise RoutePublicationError("fixed block lineage is invalid for CEX")
            number_text = str(block_number)
            if not number_text.isdigit() or int(number_text) <= 0:
                raise RoutePublicationError("fixed block number is invalid")
            timestamp_text = _validate_timestamp(
                block_timestamp, "fixed block timestamp"
            )
            if exact_rfc3339_epoch_seconds(timestamp_text) > fixed_timestamp_bound:
                raise RoutePublicationError(
                    "fixed block timestamp exceeds collection bound"
                )
            chain = market_id.split(":", 2)[1]
            lineage = (number_text, timestamp_text)
            fixed_lineage.setdefault(chain, []).append(lineage)
        elif inferred_type == "dex":
            chain = market_id.split(":", 2)[1]
            fixed_lineage.setdefault(chain, []).append(None)
        rows_by_market[market_id] = row
    for chain_lineages in fixed_lineage.values():
        present = [lineage for lineage in chain_lineages if lineage is not None]
        if present and len(present) != len(chain_lineages):
            raise RoutePublicationError(
                "DEX chain fixed block lineage is incomplete"
            )
        if len(set(present)) > 1:
            raise RoutePublicationError("fixed block lineage conflict")
    return rows_by_market


def _normalize_and_validate_cohort(cohort: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(cohort, Mapping):
        raise RoutePublicationError("route cohort must be a mapping")
    cloned = _clone_json(cohort)
    if not isinstance(cloned, dict) or set(cloned) != _TOP_LEVEL_FIELDS:
        raise RoutePublicationError("route cohort has an invalid top-level schema")
    if cloned.get("schema") != ROUTE_COHORT_SCHEMA:
        raise RoutePublicationError("route cohort schema is unsupported")

    candidate_generation = _nonempty_text(
        cloned.get("candidate_source_generation"),
        "candidate_source_generation",
    )
    collection_generation = _nonempty_text(
        cloned.get("collection_input_generation"),
        "collection_input_generation",
    )
    source_state = cloned.get("source_state")
    expected_source_state = {
        "candidate_source_generation": candidate_generation,
        "collection_input_generation": collection_generation,
    }
    if source_state != expected_source_state:
        raise RoutePublicationError("route cohort source lineage conflict")
    raw_run_id = _nonempty_text(
        cloned.get("raw_evidence_run_id"), "raw_evidence_run_id"
    )
    if _SAFE_RUN_ID.fullmatch(raw_run_id) is None:
        raise RoutePublicationError("raw_evidence_run_id is path-unsafe")

    timestamp_fields = (
        "target_observed_at",
        "collection_started_at",
        "collection_completed_at",
        "collection_deadline_at",
    )
    for field in timestamp_fields:
        _validate_timestamp(cloned.get(field), field)
    target_epoch = exact_rfc3339_epoch_seconds(cloned["target_observed_at"])
    started_epoch = exact_rfc3339_epoch_seconds(cloned["collection_started_at"])
    completed_epoch = exact_rfc3339_epoch_seconds(cloned["collection_completed_at"])
    deadline_epoch = exact_rfc3339_epoch_seconds(cloned["collection_deadline_at"])
    if (
        completed_epoch < started_epoch
        or deadline_epoch < started_epoch
        or target_epoch > deadline_epoch
    ):
        raise RoutePublicationError("route cohort collection timeline is invalid")

    if cloned.get("skew_sla_seconds") != "60":
        raise RoutePublicationError("route cohort skew SLA is invalid")
    if cloned.get("route_age_sla_seconds") != "120":
        raise RoutePublicationError("route cohort route-age SLA is invalid")
    window = cloned.get("selection_window")
    if (
        not isinstance(window, dict)
        or set(window) != {"start", "end"}
        or not all(isinstance(window.get(key), str) and window.get(key) for key in window)
    ):
        raise RoutePublicationError("route cohort selection window is invalid")
    try:
        if any(_ISO_DATE.fullmatch(window[key]) is None for key in ("start", "end")):
            raise ValueError
        window_start = date.fromisoformat(window["start"])
        window_end = date.fromisoformat(window["end"])
    except ValueError as error:
        raise RoutePublicationError(
            "route cohort selection window is invalid"
        ) from error
    if window_start > window_end:
        raise RoutePublicationError("route cohort selection window is invalid")
    if not _matches_requested_notional_grid(cloned.get("requested_notionals_usd")):
        raise RoutePublicationError("route cohort requested notional grid is invalid")

    routes = cloned.get("routes")
    legs = cloned.get("legs")
    timing = cloned.get("route_rows")
    if not isinstance(routes, list) or not routes:
        raise RoutePublicationError("route cohort candidate inventory is empty")
    if not isinstance(legs, list) or not legs:
        raise RoutePublicationError("route cohort leg inventory is empty")
    if not isinstance(timing, list) or not timing:
        raise RoutePublicationError("route cohort timing inventory is empty")

    for route in routes:
        if not isinstance(route, Mapping):
            raise RoutePublicationError("route candidate must be a mapping")
        _validate_route_candidate(
            route,
            candidate_generation=candidate_generation,
            requested_notionals=REQUESTED_NOTIONALS_USD,
        )
    try:
        validate_route_cohort_rows(routes, legs)
    except (TypeError, ValueError) as error:
        raise RoutePublicationError(str(error)) from error
    rows_by_market = _validate_leg_rows(
        legs,
        raw_evidence_run_id=raw_run_id,
        collection_completed_at=cloned["collection_completed_at"],
        collection_deadline_at=cloned["collection_deadline_at"],
    )

    routes_by_id = {route["route_id"]: route for route in routes}
    if len(routes_by_id) != len(routes):
        raise RoutePublicationError("duplicate route candidate")
    timing_by_id: Dict[str, Mapping[str, Any]] = {}
    referenced_markets = set()
    for route in routes:
        referenced_markets.add(route["buy_market_id"])
        referenced_markets.add(route["sell_market_id"])
        if route["buy_market_id"] not in rows_by_market or route["sell_market_id"] not in rows_by_market:
            raise RoutePublicationError("route pair is incomplete")
        for market_id in (route["buy_market_id"], route["sell_market_id"]):
            token = rows_by_market[market_id].get("token_symbol")
            if token not in (None, "", route["token_symbol"]):
                raise RoutePublicationError("route leg token lineage conflict")
    if referenced_markets != set(rows_by_market):
        raise RoutePublicationError("route leg inventory does not match route pairs")

    timing_extra_fields = {
        "validated_at",
        "skew_seconds",
        "timing_status",
        "reason_code",
    }
    for row in timing:
        if not isinstance(row, Mapping):
            raise RoutePublicationError("route timing row must be a mapping")
        route_id = row.get("route_id")
        if route_id not in routes_by_id or route_id in timing_by_id:
            raise RoutePublicationError("route timing inventory is invalid")
        route = routes_by_id[route_id]
        if set(row) != set(route) | timing_extra_fields:
            raise RoutePublicationError("route timing schema is invalid")
        if any(row.get(key) != value for key, value in route.items()):
            raise RoutePublicationError("route timing candidate lineage conflict")
        if row.get("validated_at") != cloned["collection_completed_at"]:
            raise RoutePublicationError("route timing validation lineage conflict")
        if row.get("timing_status") not in _TIMING_STATUSES:
            raise RoutePublicationError("route timing status enum is invalid")
        if row.get("reason_code") not in _TIMING_REASONS | {None}:
            raise RoutePublicationError("route timing reason enum is invalid")
        candidate = dict(route)
        candidate["validated_at"] = row["validated_at"]
        candidate["skew_sla_seconds"] = cloned["skew_sla_seconds"]
        expected_timing = classify_route_timing(
            candidate,
            rows_by_market[route["buy_market_id"]],
            rows_by_market[route["sell_market_id"]],
        )
        actual_timing = {
            "route_id": row["route_id"],
            "skew_seconds": row["skew_seconds"],
            "timing_status": row["timing_status"],
            "reason_code": row["reason_code"],
        }
        if actual_timing != expected_timing:
            raise RoutePublicationError("route timing classification is inconsistent")
        timing_by_id[str(route_id)] = row
    if set(timing_by_id) != set(routes_by_id):
        raise RoutePublicationError("route timing inventory does not match candidates")

    cloned["routes"] = sorted(routes, key=lambda row: row["route_id"])
    cloned["legs"] = sorted(legs, key=lambda row: row["market_id"])
    cloned["route_rows"] = sorted(timing, key=lambda row: row["route_id"])
    without_hashes = {
        key: value
        for key, value in cloned.items()
        if key not in {"route_cohort_id", "fingerprint"}
    }
    expected_id = "cohort:" + _sha256_bytes(_canonical_json_bytes(without_hashes))
    route_cohort_id = cloned.get("route_cohort_id")
    if (
        not isinstance(route_cohort_id, str)
        or _COHORT_ID.fullmatch(route_cohort_id) is None
        or route_cohort_id != expected_id
    ):
        raise RoutePublicationError("route cohort ID does not match logical content")
    expected_fingerprint = _sha256_bytes(_canonical_json_bytes({
        **without_hashes,
        "route_cohort_id": expected_id,
    }))
    if cloned.get("fingerprint") != expected_fingerprint:
        raise RoutePublicationError("route cohort fingerprint does not match logical content")
    return cloned


def _candidate_csv_row(route_cohort_id: str, row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "route_cohort_id": route_cohort_id,
        "route_id": row["route_id"],
        "token_symbol": row["token_symbol"],
        "buy_market_id": row["buy_market_id"],
        "sell_market_id": row["sell_market_id"],
        "route_mode": row["route_mode"],
        "route_class": row["route_class"],
        "settlement_reason": "" if row.get("settlement_reason") is None else row["settlement_reason"],
        "requested_notionals_usd": _canonical_json_text(row["requested_notionals_usd"]),
        "candidate_source_generation": row["candidate_source_generation"],
        "row_json": _canonical_json_text(row),
    }


def _leg_csv_row(route_cohort_id: str, row: Mapping[str, Any]) -> Dict[str, Any]:
    market_id = str(row["market_id"])
    market_type = "cex" if market_id.startswith("cex:") else "dex"
    available = row.get("available")
    return {
        "route_cohort_id": route_cohort_id,
        "leg_id": row["leg_id"],
        "market_id": market_id,
        "market_type": row.get("market_type") or market_type,
        "token_symbol": row.get("token_symbol") or "",
        "status": row.get("status") or "",
        "available": "" if available is None else ("true" if available else "false"),
        "reason_code": row.get("reason_code") or "",
        "state_observed_at": row.get("state_observed_at") or "",
        "snapshot_id": row.get("snapshot_id") or "",
        "source_endpoint": row.get("source_endpoint") or "",
        "raw_response_sha256": row.get("raw_response_sha256") or "",
        "fixed_block_number": row.get("fixed_block_number") or "",
        "fixed_block_timestamp": row.get("fixed_block_timestamp") or "",
        "row_json": _canonical_json_text(row),
    }


def _timing_csv_row(route_cohort_id: str, row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "route_cohort_id": route_cohort_id,
        "route_id": row["route_id"],
        "skew_seconds": "" if row.get("skew_seconds") is None else row["skew_seconds"],
        "timing_status": row["timing_status"],
        "reason_code": row.get("reason_code") or "",
        "row_json": _canonical_json_text(row),
    }


def _write_all(fd: int, value: bytes) -> None:
    offset = 0
    while offset < len(value):
        written = os.write(fd, value[offset:])
        if written <= 0:
            raise OSError("short write")
        offset += written


def _write_new_bytes(path: Path, value: bytes) -> None:
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as error:
        raise RoutePublicationError(
            "refusing to replace route bundle file: {}".format(path.name)
        ) from error
    try:
        _write_all(fd, value)
        os.fsync(fd)
    except OSError:
        try:
            os.unlink(str(path))
        except OSError:
            pass
        raise
    finally:
        os.close(fd)


def _write_new_bytes_at(
    directory_fd: int,
    filename: str,
    value: bytes,
) -> None:
    _require_relative_basename(filename, "route bundle filename")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        fd = os.open(filename, flags, 0o600, dir_fd=directory_fd)
    except OSError as error:
        raise RoutePublicationError(
            "refusing to replace route bundle file: {}".format(filename)
        ) from error
    try:
        _write_all(fd, value)
        os.fsync(fd)
    except OSError:
        try:
            os.unlink(filename, dir_fd=directory_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(fd)


def _csv_bytes(columns: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=list(columns),
        lineterminator="\n",
        extrasaction="raise",
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _write_csv(
    path: Path, columns: Sequence[str], rows: Iterable[Mapping[str, Any]]
) -> None:
    _write_new_bytes(path, _csv_bytes(columns, rows))


def _fsync_file(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _directory_open_flags() -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return flags


def _fsync_directory(path: Path, *, directory_fd: Optional[int] = None) -> None:
    if directory_fd is not None:
        details = os.fstat(directory_fd)
        if not stat.S_ISDIR(details.st_mode):
            raise RoutePublicationError(
                "route publication directory descriptor is invalid"
            )
        os.fsync(directory_fd)
        return
    fd = os.open(str(path), _directory_open_flags())
    try:
        details = os.fstat(fd)
        if not stat.S_ISDIR(details.st_mode):
            raise RoutePublicationError(
                "route publication directory descriptor is invalid"
            )
        os.fsync(fd)
    finally:
        os.close(fd)


def _absolute_without_symlink_resolution(path: Path) -> Path:
    absolute = os.path.abspath(str(path.expanduser()))
    # macOS exposes these two stable system aliases as symlinks.  Canonicalize
    # only those aliases; all caller-controlled descendants are still checked
    # component-by-component with lstat below.
    if sys.platform == "darwin":
        if absolute == "/var" or absolute.startswith("/var/"):
            absolute = "/private" + absolute
        elif absolute == "/tmp" or absolute.startswith("/tmp/"):
            absolute = "/private" + absolute
    return Path(absolute)


def _ensure_real_directory(path: Path) -> Path:
    absolute = _absolute_without_symlink_resolution(Path(path))
    chain = list(reversed(absolute.parents)) + [absolute]
    for component in chain:
        try:
            details = os.lstat(str(component))
        except FileNotFoundError:
            try:
                os.mkdir(str(component), 0o700)
            except FileExistsError:
                details = os.lstat(str(component))
                if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
                    raise RoutePublicationError(
                        "route publication path is not a real directory"
                    )
            continue
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            raise RoutePublicationError(
                "route publication path is not a real directory"
            )
    return absolute


def _require_real_directory(path: Path, label: str) -> Path:
    absolute = _absolute_without_symlink_resolution(Path(path))
    for component in list(reversed(absolute.parents)) + [absolute]:
        try:
            details = os.lstat(str(component))
        except OSError as error:
            raise RoutePublicationError(
                "{} is not a readable directory".format(label)
            ) from error
        if stat.S_ISLNK(details.st_mode):
            raise RoutePublicationError(
                "{} path contains a symlink".format(label)
            )
        if not stat.S_ISDIR(details.st_mode):
            raise RoutePublicationError("{} is not a directory".format(label))
    return absolute


def _same_inode(first: os.stat_result, second: os.stat_result) -> bool:
    return first.st_dev == second.st_dev and first.st_ino == second.st_ino


def _stable_file_metadata(details: os.stat_result) -> Tuple[Any, ...]:
    """Metadata that must remain exact while one bundle snapshot is validated."""
    return (
        details.st_dev,
        details.st_ino,
        details.st_mode,
        details.st_nlink,
        details.st_uid,
        details.st_gid,
        details.st_size,
        getattr(details, "st_mtime_ns", None),
        getattr(details, "st_ctime_ns", None),
        getattr(details, "st_birthtime_ns", None),
        getattr(details, "st_flags", None),
    )


def _require_relative_basename(value: str, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or os.path.basename(value) != value
        or os.sep in value
        or (os.altsep is not None and os.altsep in value)
    ):
        raise RoutePublicationError("{} is path-unsafe".format(label))
    return value


def _open_verified_directory(
    path: Path,
    label: str,
) -> Tuple[Path, int, os.stat_result]:
    absolute = _require_real_directory(Path(path), label)
    parts = absolute.parts
    if not parts or parts[0] != os.sep:
        raise RoutePublicationError(
            "{} is not an absolute directory".format(label)
        )
    try:
        fd = os.open(os.sep, _directory_open_flags())
    except OSError as error:
        raise RoutePublicationError(
            "{} is not a readable directory".format(label)
        ) from error
    try:
        for component in parts[1:]:
            try:
                next_fd = os.open(
                    component,
                    _directory_open_flags(),
                    dir_fd=fd,
                )
            except OSError as error:
                raise RoutePublicationError(
                    "{} path contains an unreadable or symlinked directory".format(
                        label
                    )
                ) from error
            os.close(fd)
            fd = next_fd
        opened = os.fstat(fd)
        if not stat.S_ISDIR(opened.st_mode):
            raise RoutePublicationError("{} is not a directory".format(label))
        current = os.stat(str(absolute), follow_symlinks=False)
        if not stat.S_ISDIR(current.st_mode) or not _same_inode(opened, current):
            raise RoutePublicationError(
                "{} changed while it was opened".format(label)
            )
    except Exception:
        os.close(fd)
        raise
    return absolute, fd, opened


def _open_directory_at(
    parent_fd: int,
    name: str,
    label: str,
) -> Tuple[int, os.stat_result]:
    _require_relative_basename(name, label)
    try:
        fd = os.open(name, _directory_open_flags(), dir_fd=parent_fd)
    except OSError as error:
        raise RoutePublicationError(
            "{} is not a readable non-symlink directory".format(label)
        ) from error
    try:
        details = os.fstat(fd)
        if not stat.S_ISDIR(details.st_mode):
            raise RoutePublicationError(
                "{} is not a directory".format(label)
            )
    except Exception:
        os.close(fd)
        raise
    return fd, details


def _verify_directory_entry(
    parent_fd: int,
    name: str,
    opened: os.stat_result,
    label: str,
) -> None:
    try:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as error:
        raise RoutePublicationError(
            "{} changed during validation".format(label)
        ) from error
    if (
        not stat.S_ISDIR(current.st_mode)
        or not _same_inode(opened, current)
    ):
        raise RoutePublicationError(
            "{} changed during validation".format(label)
        )


def _verify_open_path_identity(
    path: Path,
    opened: os.stat_result,
    label: str,
) -> None:
    try:
        current = os.stat(str(path), follow_symlinks=False)
    except OSError as error:
        raise RoutePublicationError(
            "{} changed during validation".format(label)
        ) from error
    if (
        not stat.S_ISDIR(current.st_mode)
        or not _same_inode(opened, current)
    ):
        raise RoutePublicationError(
            "{} changed during validation".format(label)
        )


def _open_regular_file_at(
    directory_fd: int,
    filename: str,
    *,
    label: str,
) -> Tuple[int, os.stat_result]:
    _require_relative_basename(filename, label)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        fd = os.open(filename, flags, dir_fd=directory_fd)
    except OSError as error:
        raise RoutePublicationError(
            "{} must be a regular non-symlink file".format(label)
        ) from error
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode):
        os.close(fd)
        raise RoutePublicationError(
            "{} must be a regular non-symlink file".format(label)
        )
    return fd, before


def _read_bounded_open_file(
    fd: int,
    before: os.stat_result,
    *,
    limit: int,
    label: str,
) -> Tuple[bytes, str, os.stat_result]:
    if before.st_size > limit:
        raise RoutePublicationError(
            "{} exceeds its size limit".format(label)
        )
    chunks = []
    digest = hashlib.sha256()
    total = 0
    while True:
        chunk = os.read(fd, min(1024 * 1024, limit + 1 - total))
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise RoutePublicationError(
                "{} exceeds its size limit".format(label)
            )
        chunks.append(chunk)
        digest.update(chunk)
    after = os.fstat(fd)
    before_times = (
        getattr(before, "st_mtime_ns", None),
        getattr(before, "st_ctime_ns", None),
    )
    after_times = (
        getattr(after, "st_mtime_ns", None),
        getattr(after, "st_ctime_ns", None),
    )
    if (
        not _same_inode(before, after)
        or before.st_size != after.st_size
        or before_times != after_times
        or total != after.st_size
    ):
        raise RoutePublicationError(
            "{} changed while it was read".format(label)
        )
    return b"".join(chunks), digest.hexdigest(), after


def _read_bounded_bytes_at(
    directory_fd: int,
    filename: str,
    *,
    limit: int,
    label: str,
) -> Tuple[bytes, str, os.stat_result]:
    fd, before = _open_regular_file_at(
        directory_fd,
        filename,
        label=label,
    )
    try:
        return _read_bounded_open_file(
            fd,
            before,
            limit=limit,
            label=label,
        )
    finally:
        os.close(fd)


def _rename_directory_noreplace_at(
    source_dir_fd: int,
    source_name: str,
    destination_dir_fd: int,
    destination_name: str,
    *,
    destination_display: Path,
) -> None:
    """Atomically rename one directory while refusing every existing target.

    POSIX ``rename`` may replace an empty destination directory, so a prior
    ``lexists`` check is not an immutability boundary.  Darwin and Linux expose
    no-replace rename flags; unsupported platforms fail closed.
    """
    _require_relative_basename(source_name, "route bundle stage name")
    _require_relative_basename(destination_name, "route cohort ID")
    for directory_fd in (source_dir_fd, destination_dir_fd):
        if not stat.S_ISDIR(os.fstat(directory_fd).st_mode):
            raise RoutePublicationError(
                "route bundle directory descriptor is invalid"
            )
    library = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source_name)
    destination_bytes = os.fsencode(destination_name)
    operation = None
    arguments: Tuple[Any, ...]
    if sys.platform == "darwin":
        try:
            operation = library.renameatx_np
        except AttributeError as error:
            raise RoutePublicationError(
                "atomic no-replace directory rename is unsupported"
            ) from error
        operation.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        operation.restype = ctypes.c_int
        # <sys/stdio.h>: RENAME_EXCL
        arguments = (
            source_dir_fd,
            source_bytes,
            destination_dir_fd,
            destination_bytes,
            0x00000004,
        )
    elif sys.platform.startswith("linux"):
        try:
            operation = library.renameat2
        except AttributeError as error:
            raise RoutePublicationError(
                "atomic no-replace directory rename is unsupported"
            ) from error
        operation.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        operation.restype = ctypes.c_int
        # <linux/fs.h>: RENAME_NOREPLACE
        arguments = (
            source_dir_fd,
            source_bytes,
            destination_dir_fd,
            destination_bytes,
            1,
        )
    else:
        raise RoutePublicationError(
            "atomic no-replace directory rename is unsupported"
        )

    ctypes.set_errno(0)
    result = operation(*arguments)
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise RoutePublicationError("immutable route cohort ID already exists")
    if error_number in {errno.ENOSYS, errno.ENOTSUP}:
        raise RoutePublicationError(
            "atomic no-replace directory rename is unsupported"
        )
    raise OSError(
        error_number,
        os.strerror(error_number),
        str(destination_display),
    )


def _rename_directory_noreplace(
    source: Path,
    destination: Path,
    *,
    source_dir_fd: Optional[int] = None,
    destination_dir_fd: Optional[int] = None,
) -> None:
    """Path-compatible wrapper around the dirfd-relative rename primitive."""
    source_path = Path(source)
    destination_path = Path(destination)
    owned_fds: List[int] = []
    try:
        if source_dir_fd is None:
            _unused, source_dir_fd, _source_details = _open_verified_directory(
                source_path.parent,
                "route bundle source root",
            )
            owned_fds.append(source_dir_fd)
        if destination_dir_fd is None:
            if source_path.parent == destination_path.parent:
                destination_dir_fd = source_dir_fd
            else:
                _unused, destination_dir_fd, _destination_details = (
                    _open_verified_directory(
                        destination_path.parent,
                        "route bundle destination root",
                    )
                )
                owned_fds.append(destination_dir_fd)
        _rename_directory_noreplace_at(
            source_dir_fd,
            source_path.name,
            destination_dir_fd,
            destination_path.name,
            destination_display=destination_path,
        )
    finally:
        for fd in reversed(owned_fds):
            os.close(fd)


def _require_regular_file(path: Path, label: str) -> None:
    try:
        details = os.lstat(str(path))
    except OSError as error:
        raise RoutePublicationError("{} is missing".format(label)) from error
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise RoutePublicationError("{} must be a regular non-symlink file".format(label))


def _read_bounded_bytes(path: Path, *, limit: int, label: str) -> bytes:
    _require_regular_file(path, label)
    try:
        size = os.lstat(str(path)).st_size
        if size > limit:
            raise RoutePublicationError("{} exceeds its size limit".format(label))
        with path.open("rb") as handle:
            value = handle.read(limit + 1)
    except OSError as error:
        raise RoutePublicationError("{} is not readable".format(label)) from error
    if len(value) > limit:
        raise RoutePublicationError("{} exceeds its size limit".format(label))
    return value


def _read_json_object(path: Path, *, label: str) -> Dict[str, Any]:
    value = _read_bounded_bytes(path, limit=_MAX_JSON_BYTES, label=label)
    return _decode_json_object_bytes(value, label=label)


def _decode_json_object_bytes(value: bytes, *, label: str) -> Dict[str, Any]:
    try:
        payload = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RoutePublicationError("{} is not readable JSON".format(label)) from error
    if not isinstance(payload, dict):
        raise RoutePublicationError("{} must be a JSON object".format(label))
    return payload


def _sqlite_candidate_values(route_cohort_id: str, row: Mapping[str, Any]) -> Tuple[Any, ...]:
    projected = _candidate_csv_row(route_cohort_id, row)
    return tuple(projected[column] for column in CANDIDATE_COLUMNS)


def _sqlite_leg_values(route_cohort_id: str, row: Mapping[str, Any]) -> Tuple[Any, ...]:
    projected = _leg_csv_row(route_cohort_id, row)
    available = projected["available"]
    projected["available"] = None if available == "" else (1 if available == "true" else 0)
    return tuple(projected[column] for column in LEG_COLUMNS)


def _sqlite_timing_values(route_cohort_id: str, row: Mapping[str, Any]) -> Tuple[Any, ...]:
    projected = _timing_csv_row(route_cohort_id, row)
    return tuple(projected[column] for column in TIMING_COLUMNS)


def _remove_sqlite_artifacts(path: Path) -> None:
    for candidate in (
        path,
        Path(str(path) + "-journal"),
        Path(str(path) + "-wal"),
        Path(str(path) + "-shm"),
    ):
        if not os.path.lexists(str(candidate)):
            continue
        try:
            details = os.lstat(str(candidate))
            if stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode):
                os.unlink(str(candidate))
        except OSError:
            pass


def _build_route_cohort_sqlite_file(
    path: Path,
    normalized: Mapping[str, Any],
) -> str:
    """Build SQLite only at a private, controlled, nonexistent pathname."""
    route_cohort_id = normalized["route_cohort_id"]
    connection: Optional[sqlite3.Connection] = None
    try:
        connection = sqlite3.connect(str(path))
        connection.execute("PRAGMA page_size = 4096")
        connection.execute("PRAGMA auto_vacuum = NONE")
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA application_id = 1380142920")
        connection.execute("PRAGMA user_version = 1")
        connection.executescript(
            """
            CREATE TABLE cohort_metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value_json TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE route_candidates (
                route_cohort_id TEXT NOT NULL,
                route_id TEXT PRIMARY KEY NOT NULL,
                token_symbol TEXT NOT NULL,
                buy_market_id TEXT NOT NULL,
                sell_market_id TEXT NOT NULL,
                route_mode TEXT NOT NULL,
                route_class TEXT NOT NULL,
                settlement_reason TEXT NOT NULL,
                requested_notionals_usd TEXT NOT NULL,
                candidate_source_generation TEXT NOT NULL,
                row_json TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE route_legs (
                route_cohort_id TEXT NOT NULL,
                leg_id TEXT NOT NULL,
                market_id TEXT PRIMARY KEY NOT NULL,
                market_type TEXT NOT NULL,
                token_symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                available INTEGER,
                reason_code TEXT NOT NULL,
                state_observed_at TEXT NOT NULL,
                snapshot_id TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                raw_response_sha256 TEXT NOT NULL,
                fixed_block_number TEXT NOT NULL,
                fixed_block_timestamp TEXT NOT NULL,
                row_json TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE route_timing (
                route_cohort_id TEXT NOT NULL,
                route_id TEXT PRIMARY KEY NOT NULL,
                skew_seconds TEXT NOT NULL,
                timing_status TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                row_json TEXT NOT NULL,
                FOREIGN KEY (route_id) REFERENCES route_candidates(route_id)
            ) WITHOUT ROWID;
            CREATE INDEX route_candidates_token_idx
                ON route_candidates(token_symbol, route_id);
            CREATE INDEX route_candidates_markets_idx
                ON route_candidates(buy_market_id, sell_market_id, route_id);
            CREATE INDEX route_legs_token_idx
                ON route_legs(token_symbol, market_id);
            CREATE INDEX route_timing_status_idx
                ON route_timing(timing_status, route_id);
            """
        )
        connection.execute(
            "INSERT INTO cohort_metadata (key, value_json) VALUES (?, ?)",
            ("cohort", _canonical_json_text(normalized)),
        )
        candidate_placeholders = ",".join("?" for _unused in CANDIDATE_COLUMNS)
        leg_placeholders = ",".join("?" for _unused in LEG_COLUMNS)
        timing_placeholders = ",".join("?" for _unused in TIMING_COLUMNS)
        connection.executemany(
            "INSERT INTO route_candidates ({}) VALUES ({})".format(
                ",".join(CANDIDATE_COLUMNS), candidate_placeholders
            ),
            (
                _sqlite_candidate_values(route_cohort_id, row)
                for row in normalized["routes"]
            ),
        )
        connection.executemany(
            "INSERT INTO route_legs ({}) VALUES ({})".format(
                ",".join(LEG_COLUMNS), leg_placeholders
            ),
            (
                _sqlite_leg_values(route_cohort_id, row)
                for row in normalized["legs"]
            ),
        )
        connection.executemany(
            "INSERT INTO route_timing ({}) VALUES ({})".format(
                ",".join(TIMING_COLUMNS), timing_placeholders
            ),
            (
                _sqlite_timing_values(route_cohort_id, row)
                for row in normalized["route_rows"]
            ),
        )
        connection.commit()
        foreign_key_failures = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_failures:
            raise RoutePublicationError("route cohort SQLite foreign keys are invalid")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if not integrity or integrity[0] != "ok":
            raise RoutePublicationError("route cohort SQLite integrity check failed")
        connection.execute("VACUUM")
        connection.close()
        connection = None
        _fsync_file(path)
        logical_sha256 = _database_logical_sha256(normalized)
    except Exception:
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass
            connection = None
        _remove_sqlite_artifacts(path)
        raise
    finally:
        if connection is not None:
            connection.close()
    return logical_sha256


def build_route_cohort_sqlite(
    database_path: Path,
    cohort: Mapping[str, Any],
) -> str:
    """Write a deterministic indexed cohort copy and return its logical hash."""
    normalized = _normalize_and_validate_cohort(cohort)
    requested_path = _absolute_without_symlink_resolution(Path(database_path))
    _require_relative_basename(
        requested_path.name,
        "route cohort SQLite filename",
    )
    parent = _ensure_real_directory(requested_path.parent)
    parent, parent_fd, parent_details = _open_verified_directory(
        parent,
        "route cohort SQLite parent",
    )
    published = False
    try:
        if _entry_exists_at(parent_fd, requested_path.name):
            raise RoutePublicationError(
                "refusing to replace an existing SQLite file"
            )
        with tempfile.TemporaryDirectory(
            prefix="route-cohort-sqlite-build-"
        ) as temporary:
            controlled_path = Path(temporary) / ROUTE_SQLITE_FILENAME
            logical_sha256 = _build_route_cohort_sqlite_file(
                controlled_path,
                normalized,
            )
            value = _read_bounded_bytes(
                controlled_path,
                limit=_MAX_SQLITE_BYTES,
                label="controlled route cohort SQLite",
            )
        _write_new_bytes_at(parent_fd, requested_path.name, value)
        published = True
        try:
            _fsync_directory(parent, directory_fd=parent_fd)
        except Exception as original_error:
            try:
                os.unlink(requested_path.name, dir_fd=parent_fd)
                published = False
                _fsync_directory(parent, directory_fd=parent_fd)
            except Exception as rollback_error:
                raise RoutePublicationError(
                    "route cohort SQLite publication state uncertain"
                ) from rollback_error
            raise original_error
        reread, reread_sha256, _reread_details = _read_bounded_bytes_at(
            parent_fd,
            requested_path.name,
            limit=_MAX_SQLITE_BYTES,
            label="route cohort SQLite",
        )
        if reread != value or reread_sha256 != _sha256_bytes(value):
            raise RoutePublicationError(
                "route cohort SQLite changed during publication"
            )
        _verify_open_path_identity(
            parent,
            parent_details,
            "route cohort SQLite parent",
        )
        return logical_sha256
    except Exception:
        if published:
            try:
                os.unlink(requested_path.name, dir_fd=parent_fd)
                _fsync_directory(parent, directory_fd=parent_fd)
            except OSError:
                pass
        raise
    finally:
        os.close(parent_fd)


def _artifact_details(
    path: Path,
    *,
    schema: str,
    logical_sha256: str,
    row_count: int
) -> Dict[str, Any]:
    return {
        "schema": schema,
        "sha256": _sha256_file(path),
        "logical_sha256": logical_sha256,
        "row_count": row_count,
    }


def _artifact_details_bytes(
    value: bytes,
    *,
    schema: str,
    logical_sha256: str,
    row_count: int,
) -> Dict[str, Any]:
    return {
        "schema": schema,
        "sha256": _sha256_bytes(value),
        "logical_sha256": logical_sha256,
        "row_count": row_count,
    }


def _observation_bounds(legs: Sequence[Mapping[str, Any]]) -> Dict[str, Optional[str]]:
    values = [
        row.get("state_observed_at")
        for row in legs
        if isinstance(row.get("state_observed_at"), str)
        and row.get("state_observed_at")
    ]
    valid = []
    for value in values:
        try:
            valid.append((exact_rfc3339_epoch_seconds(value), value))
        except (TypeError, ValueError):
            continue
    valid.sort(key=lambda item: item[0])
    return {
        "minimum_state_observed_at": valid[0][1] if valid else None,
        "maximum_state_observed_at": valid[-1][1] if valid else None,
    }


def _manifest_payload(
    cohort: Mapping[str, Any], files: Mapping[str, Mapping[str, Any]]
) -> Dict[str, Any]:
    timing_counts = Counter(row["timing_status"] for row in cohort["route_rows"])
    return {
        "schema": ROUTE_CORE_MANIFEST_SCHEMA,
        "bundle_stage": ROUTE_CORE_BUNDLE_STAGE,
        "cohort_schema": ROUTE_COHORT_SCHEMA,
        "route_cohort_id": cohort["route_cohort_id"],
        "cohort_fingerprint": cohort["fingerprint"],
        "candidate_source_generation": cohort["candidate_source_generation"],
        "collection_input_generation": cohort["collection_input_generation"],
        "raw_evidence_run_id": cohort["raw_evidence_run_id"],
        "target_observed_at": cohort["target_observed_at"],
        "collection_started_at": cohort["collection_started_at"],
        "collection_completed_at": cohort["collection_completed_at"],
        "collection_deadline_at": cohort["collection_deadline_at"],
        "skew_sla_seconds": cohort["skew_sla_seconds"],
        "route_age_sla_seconds": cohort["route_age_sla_seconds"],
        "selection_window": cohort["selection_window"],
        "requested_notionals_usd": cohort["requested_notionals_usd"],
        "counts": {
            "candidates": len(cohort["routes"]),
            "legs": len(cohort["legs"]),
            "timing": len(cohort["route_rows"]),
        },
        "timing_status_counts": {
            status: timing_counts.get(status, 0)
            for status in sorted(_TIMING_STATUSES)
        },
        "observation_bounds": _observation_bounds(cohort["legs"]),
        "files": {name: dict(files[name]) for name in sorted(files)},
    }


def _write_bundle_artifacts(
    stage_path: Path,
    cohort: Mapping[str, Any],
    *,
    stage_fd: Optional[int] = None,
) -> None:
    route_cohort_id = str(cohort["route_cohort_id"])
    candidate_bytes = _csv_bytes(
        CANDIDATE_COLUMNS,
        (_candidate_csv_row(route_cohort_id, row) for row in cohort["routes"]),
    )
    leg_bytes = _csv_bytes(
        LEG_COLUMNS,
        (_leg_csv_row(route_cohort_id, row) for row in cohort["legs"]),
    )
    timing_bytes = _csv_bytes(
        TIMING_COLUMNS,
        (_timing_csv_row(route_cohort_id, row) for row in cohort["route_rows"]),
    )
    with tempfile.TemporaryDirectory(prefix="route-cohort-sqlite-build-") as temporary:
        database_path = Path(temporary) / ROUTE_SQLITE_FILENAME
        sqlite_logical = build_route_cohort_sqlite(database_path, cohort)
        database_bytes = _read_bounded_bytes(
            database_path,
            limit=_MAX_SQLITE_BYTES,
            label="controlled route cohort SQLite",
        )

    files = {
        ROUTE_CANDIDATES_FILENAME: _artifact_details_bytes(
            candidate_bytes,
            schema=ROUTE_CANDIDATE_CSV_SCHEMA,
            logical_sha256=_logical_rows_sha256(
                ROUTE_CANDIDATE_CSV_SCHEMA, cohort["routes"]
            ),
            row_count=len(cohort["routes"]),
        ),
        ROUTE_LEGS_FILENAME: _artifact_details_bytes(
            leg_bytes,
            schema=ROUTE_LEG_CSV_SCHEMA,
            logical_sha256=_logical_rows_sha256(
                ROUTE_LEG_CSV_SCHEMA, cohort["legs"]
            ),
            row_count=len(cohort["legs"]),
        ),
        ROUTE_TIMING_FILENAME: _artifact_details_bytes(
            timing_bytes,
            schema=ROUTE_TIMING_CSV_SCHEMA,
            logical_sha256=_logical_rows_sha256(
                ROUTE_TIMING_CSV_SCHEMA, cohort["route_rows"]
            ),
            row_count=len(cohort["route_rows"]),
        ),
        ROUTE_SQLITE_FILENAME: _artifact_details_bytes(
            database_bytes,
            schema=ROUTE_SQLITE_LOGICAL_SCHEMA,
            logical_sha256=sqlite_logical,
            row_count=(
                len(cohort["routes"])
                + len(cohort["legs"])
                + len(cohort["route_rows"])
            ),
        ),
    }
    manifest = _manifest_payload(cohort, files)
    artifact_bytes = {
        ROUTE_CANDIDATES_FILENAME: candidate_bytes,
        ROUTE_LEGS_FILENAME: leg_bytes,
        ROUTE_TIMING_FILENAME: timing_bytes,
        ROUTE_SQLITE_FILENAME: database_bytes,
        MANIFEST_FILENAME: json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8") + b"\n",
    }
    owned_fd: Optional[int] = None
    try:
        if stage_fd is None:
            _unused, owned_fd, _stage_details = _open_verified_directory(
                stage_path,
                "route cohort stage",
            )
            stage_fd = owned_fd
        for filename in sorted(artifact_bytes):
            _write_new_bytes_at(stage_fd, filename, artifact_bytes[filename])
    finally:
        if owned_fd is not None:
            os.close(owned_fd)


def _read_csv_rows(
    path: Path,
    *,
    columns: Sequence[str],
    label: str
) -> List[Dict[str, str]]:
    value = _read_bounded_bytes(path, limit=_MAX_CSV_BYTES, label=label)
    return _read_csv_rows_bytes(value, columns=columns, label=label)


def _read_csv_rows_bytes(
    value: bytes,
    *,
    columns: Sequence[str],
    label: str,
) -> List[Dict[str, str]]:
    try:
        text = value.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        if tuple(reader.fieldnames or ()) != tuple(columns):
            raise RoutePublicationError("{} has an invalid CSV schema".format(label))
        rows = [dict(row) for row in reader]
    except (UnicodeDecodeError, csv.Error) as error:
        raise RoutePublicationError("{} is not readable CSV".format(label)) from error
    if any(None in row for row in rows):
        raise RoutePublicationError("{} has extra CSV fields".format(label))
    return rows


def _decode_row_json(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, str):
        raise RoutePublicationError("{} has no row_json".format(label))
    try:
        row = json.loads(value)
    except json.JSONDecodeError as error:
        raise RoutePublicationError("{} row_json is invalid".format(label)) from error
    if not isinstance(row, dict) or _canonical_json_text(row) != value:
        raise RoutePublicationError("{} row_json is not canonical".format(label))
    return row


def _validate_csv_projection(
    csv_rows: Sequence[Mapping[str, str]],
    logical_rows: Sequence[Mapping[str, Any]],
    *,
    route_cohort_id: str,
    projector: Any,
    label: str
) -> None:
    if len(csv_rows) != len(logical_rows):
        raise RoutePublicationError("{} row count is invalid".format(label))
    for csv_row, logical_row in zip(csv_rows, logical_rows):
        expected = projector(route_cohort_id, logical_row)
        if dict(csv_row) != expected:
            raise RoutePublicationError("{} projection does not match row_json".format(label))


def _sqlite_read_only_uri(path: Path) -> str:
    return path.absolute().as_uri() + "?mode=ro"


def _sqlite_table_definition(
    connection: sqlite3.Connection,
    table: str,
) -> Tuple[Tuple[Any, ...], ...]:
    return tuple(tuple(row[:6]) for row in connection.execute(
        "PRAGMA table_info({})".format(table)
    ).fetchall())


def _expected_sqlite_table_definitions() -> Dict[str, Tuple[Tuple[Any, ...], ...]]:
    def columns(
        names: Sequence[str],
        *,
        primary_key: str,
        integer_columns: Iterable[str] = (),
        nullable_columns: Iterable[str] = (),
    ) -> Tuple[Tuple[Any, ...], ...]:
        integers = set(integer_columns)
        nullable = set(nullable_columns)
        return tuple(
            (
                index,
                name,
                "INTEGER" if name in integers else "TEXT",
                0 if name in nullable else 1,
                None,
                1 if name == primary_key else 0,
            )
            for index, name in enumerate(names)
        )

    return {
        "cohort_metadata": columns(
            ("key", "value_json"),
            primary_key="key",
        ),
        "route_candidates": columns(
            CANDIDATE_COLUMNS,
            primary_key="route_id",
        ),
        "route_legs": columns(
            LEG_COLUMNS,
            primary_key="market_id",
            integer_columns=("available",),
            nullable_columns=("available",),
        ),
        "route_timing": columns(
            TIMING_COLUMNS,
            primary_key="route_id",
        ),
    }


def _read_and_validate_controlled_sqlite(
    path: Path,
    *,
    route_cohort_id: str
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Query only a private copy created from bytes read through a safe fd."""
    _require_regular_file(path, "route cohort SQLite")
    try:
        connection = sqlite3.connect(_sqlite_read_only_uri(path), uri=True)
        connection.execute("PRAGMA query_only = ON")
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if tables != {
            "cohort_metadata",
            "route_candidates",
            "route_legs",
            "route_timing",
        }:
            raise RoutePublicationError("route cohort SQLite table inventory is invalid")
        expected_columns = _expected_sqlite_table_definitions()
        if any(
            _sqlite_table_definition(connection, table) != columns
            for table, columns in expected_columns.items()
        ):
            raise RoutePublicationError("route cohort SQLite schema is invalid")
        table_sql = {
            row[0]: row[1]
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if any(
            not isinstance(table_sql.get(table), str)
            or re.search(
                r"\)\s+WITHOUT\s+ROWID\s*\Z",
                table_sql[table],
                flags=re.IGNORECASE,
            ) is None
            for table in expected_columns
        ):
            raise RoutePublicationError("route cohort SQLite schema is invalid")
        expected_foreign_keys = {
            "cohort_metadata": (),
            "route_candidates": (),
            "route_legs": (),
            "route_timing": (
                (
                    0,
                    0,
                    "route_candidates",
                    "route_id",
                    "route_id",
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                ),
            ),
        }
        if any(
            tuple(
                tuple(row)
                for row in connection.execute(
                    "PRAGMA foreign_key_list({})".format(table)
                ).fetchall()
            ) != expected
            for table, expected in expected_foreign_keys.items()
        ):
            raise RoutePublicationError("route cohort SQLite schema is invalid")
        declared_objects = {
            (row[0], row[1])
            for row in connection.execute(
                """
                SELECT type, name
                FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }
        expected_objects = {
            ("table", "cohort_metadata"),
            ("table", "route_candidates"),
            ("table", "route_legs"),
            ("table", "route_timing"),
            ("index", "route_candidates_token_idx"),
            ("index", "route_candidates_markets_idx"),
            ("index", "route_legs_token_idx"),
            ("index", "route_timing_status_idx"),
        }
        expected_index_columns = {
            "route_candidates_token_idx": ("token_symbol", "route_id"),
            "route_candidates_markets_idx": (
                "buy_market_id", "sell_market_id", "route_id"
            ),
            "route_legs_token_idx": ("token_symbol", "market_id"),
            "route_timing_status_idx": ("timing_status", "route_id"),
        }
        if declared_objects != expected_objects or any(
            tuple(row[2] for row in connection.execute(
                "PRAGMA index_info({})".format(index_name)
            ).fetchall()) != columns
            for index_name, columns in expected_index_columns.items()
        ):
            raise RoutePublicationError("route cohort SQLite schema is invalid")
        if (
            connection.execute("PRAGMA application_id").fetchone()[0]
            != 1380142920
            or connection.execute("PRAGMA user_version").fetchone()[0] != 1
            or connection.execute("PRAGMA page_size").fetchone()[0] != 4096
        ):
            raise RoutePublicationError("route cohort SQLite schema is invalid")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if not integrity or integrity[0] != "ok":
            raise RoutePublicationError("route cohort SQLite integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise RoutePublicationError("route cohort SQLite foreign keys are invalid")
        metadata_rows = connection.execute(
            "SELECT key, value_json FROM cohort_metadata ORDER BY key"
        ).fetchall()
        if len(metadata_rows) != 1 or metadata_rows[0][0] != "cohort":
            raise RoutePublicationError("route cohort SQLite metadata is invalid")
        cohort_json = metadata_rows[0][1]
        cohort = _decode_row_json(cohort_json, "route cohort SQLite metadata")

        candidate_records = connection.execute(
            "SELECT {} FROM route_candidates ORDER BY route_id".format(
                ",".join(CANDIDATE_COLUMNS)
            )
        ).fetchall()
        leg_records = connection.execute(
            "SELECT {} FROM route_legs ORDER BY market_id".format(
                ",".join(LEG_COLUMNS)
            )
        ).fetchall()
        timing_records = connection.execute(
            "SELECT {} FROM route_timing ORDER BY route_id".format(
                ",".join(TIMING_COLUMNS)
            )
        ).fetchall()
    except sqlite3.Error as error:
        raise RoutePublicationError("route cohort SQLite cannot be queried") from error
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            pass

    candidates = [_decode_row_json(row[-1], "route candidate SQLite row") for row in candidate_records]
    legs = [_decode_row_json(row[-1], "route leg SQLite row") for row in leg_records]
    timing = [_decode_row_json(row[-1], "route timing SQLite row") for row in timing_records]
    expected_candidate_records = [
        _sqlite_candidate_values(route_cohort_id, row) for row in candidates
    ]
    expected_leg_records = [
        _sqlite_leg_values(route_cohort_id, row) for row in legs
    ]
    expected_timing_records = [
        _sqlite_timing_values(route_cohort_id, row) for row in timing
    ]
    if list(candidate_records) != expected_candidate_records:
        raise RoutePublicationError("route candidate SQLite projection is inconsistent")
    if list(leg_records) != expected_leg_records:
        raise RoutePublicationError("route leg SQLite projection is inconsistent")
    if list(timing_records) != expected_timing_records:
        raise RoutePublicationError("route timing SQLite projection is inconsistent")
    return cohort, candidates, legs, timing


def _read_and_validate_sqlite_at(
    bundle_fd: int,
    filename: str,
    *,
    route_cohort_id: str,
    source_snapshot: Optional[
        Tuple[int, os.stat_result, bytes, str]
    ] = None,
) -> Tuple[
    bytes,
    str,
    Dict[str, Any],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
]:
    """Validate an untrusted SQLite file without reopening its pathname."""
    label = "route cohort SQLite"
    owns_source_fd = source_snapshot is None
    source_fd = -1
    try:
        if source_snapshot is None:
            source_fd, before = _open_regular_file_at(
                bundle_fd,
                filename,
                label=label,
            )
            value, source_sha256, after_first_read = _read_bounded_open_file(
                source_fd,
                before,
                limit=_MAX_SQLITE_BYTES,
                label=label,
            )
        else:
            source_fd, after_first_read, value, source_sha256 = source_snapshot
            before = after_first_read
        with tempfile.TemporaryDirectory(
            prefix="route-cohort-sqlite-validate-"
        ) as temporary:
            controlled_path = Path(temporary) / ROUTE_SQLITE_FILENAME
            _write_new_bytes(controlled_path, value)
            cohort, candidates, legs, timing = _read_and_validate_controlled_sqlite(
                controlled_path,
                route_cohort_id=route_cohort_id,
            )

        if owns_source_fd:
            os.lseek(source_fd, 0, os.SEEK_SET)
            value_after, hash_after, final_details = _read_bounded_open_file(
                source_fd,
                after_first_read,
                limit=_MAX_SQLITE_BYTES,
                label=label,
            )
            if (
                value_after != value
                or hash_after != source_sha256
                or _stable_file_metadata(before)
                != _stable_file_metadata(final_details)
            ):
                raise RoutePublicationError(
                    "route cohort SQLite changed during validation"
                )
        return (
            value,
            source_sha256,
            cohort,
            candidates,
            legs,
            timing,
        )
    finally:
        if owns_source_fd and source_fd >= 0:
            os.close(source_fd)


def _verify_bundle_file_snapshots(
    bundle_fd: int,
    read_specs: Mapping[str, Tuple[int, str]],
    file_fds: Mapping[str, int],
    file_details: Mapping[str, os.stat_result],
    file_bytes: Mapping[str, bytes],
    file_hashes: Mapping[str, str],
) -> None:
    try:
        if set(os.listdir(bundle_fd)) != ROUTE_CORE_FILENAMES:
            raise RoutePublicationError(
                "route cohort bundle file inventory changed during validation"
            )
    except OSError as error:
        raise RoutePublicationError(
            "route cohort bundle file inventory changed during validation"
        ) from error

    for filename, (limit, label) in read_specs.items():
        try:
            entry_details = os.stat(
                filename,
                dir_fd=bundle_fd,
                follow_symlinks=False,
            )
        except OSError as error:
            raise RoutePublicationError(
                "{} changed during validation".format(label)
            ) from error
        if _stable_file_metadata(entry_details) != _stable_file_metadata(
            file_details[filename]
        ):
            raise RoutePublicationError(
                "{} changed during validation".format(label)
            )
        os.lseek(file_fds[filename], 0, os.SEEK_SET)
        value_after, hash_after, details_after = _read_bounded_open_file(
            file_fds[filename],
            file_details[filename],
            limit=limit,
            label=label,
        )
        if (
            value_after != file_bytes[filename]
            or hash_after != file_hashes[filename]
            or _stable_file_metadata(details_after)
            != _stable_file_metadata(file_details[filename])
        ):
            raise RoutePublicationError(
                "{} changed during validation".format(label)
            )

    try:
        if set(os.listdir(bundle_fd)) != ROUTE_CORE_FILENAMES:
            raise RoutePublicationError(
                "route cohort bundle file inventory changed during validation"
            )
        for filename, (_limit, label) in read_specs.items():
            entry_details = os.stat(
                filename,
                dir_fd=bundle_fd,
                follow_symlinks=False,
            )
            if _stable_file_metadata(entry_details) != _stable_file_metadata(
                file_details[filename]
            ):
                raise RoutePublicationError(
                    "{} changed during validation".format(label)
                )
    except OSError as error:
        raise RoutePublicationError(
            "route cohort bundle file inventory changed during validation"
        ) from error


def _validate_route_cohort_bundle_at(
    parent_fd: int,
    bundle_name: str,
    bundle_path: Path,
    *,
    expected_route_cohort_id: Optional[str],
    expected_manifest_sha256: Optional[str],
    require_directory_identity: bool,
) -> Dict[str, Any]:
    bundle_fd, bundle_details = _open_directory_at(
        parent_fd,
        bundle_name,
        "route cohort bundle",
    )
    file_fds: Dict[str, int] = {}
    try:
        try:
            entries = set(os.listdir(bundle_fd))
        except OSError as error:
            raise RoutePublicationError(
                "route cohort bundle is not readable"
            ) from error
        if entries != ROUTE_CORE_FILENAMES:
            raise RoutePublicationError(
                "route cohort bundle file inventory is invalid"
            )

        file_bytes: Dict[str, bytes] = {}
        file_hashes: Dict[str, str] = {}
        read_specs = {
            MANIFEST_FILENAME: (_MAX_JSON_BYTES, "route cohort manifest"),
            ROUTE_CANDIDATES_FILENAME: (
                _MAX_CSV_BYTES,
                "route candidate CSV",
            ),
            ROUTE_LEGS_FILENAME: (_MAX_CSV_BYTES, "route leg CSV"),
            ROUTE_TIMING_FILENAME: (_MAX_CSV_BYTES, "route timing CSV"),
            ROUTE_SQLITE_FILENAME: (
                _MAX_SQLITE_BYTES,
                "route cohort SQLite",
            ),
        }
        file_details: Dict[str, os.stat_result] = {}
        for filename, (limit, label) in read_specs.items():
            source_fd, before = _open_regular_file_at(
                bundle_fd,
                filename,
                label=label,
            )
            file_fds[filename] = source_fd
            value, physical_sha256, after = _read_bounded_open_file(
                source_fd,
                before,
                limit=limit,
                label=label,
            )
            try:
                entry_details = os.stat(
                    filename,
                    dir_fd=bundle_fd,
                    follow_symlinks=False,
                )
            except OSError as error:
                raise RoutePublicationError(
                    "{} changed during validation".format(label)
                ) from error
            if _stable_file_metadata(entry_details) != _stable_file_metadata(
                after
            ):
                raise RoutePublicationError(
                    "{} changed during validation".format(label)
                )
            file_bytes[filename] = value
            file_hashes[filename] = physical_sha256
            file_details[filename] = after

        actual_manifest_sha256 = file_hashes[MANIFEST_FILENAME]
        if expected_manifest_sha256 is not None:
            if (
                not isinstance(expected_manifest_sha256, str)
                or _HEX_SHA256.fullmatch(expected_manifest_sha256) is None
                or expected_manifest_sha256 != actual_manifest_sha256
            ):
                raise RoutePublicationError(
                    "route manifest hash does not match pointer"
                )
        manifest = _decode_json_object_bytes(
            file_bytes[MANIFEST_FILENAME],
            label="route cohort manifest",
        )
        route_cohort_id = manifest.get("route_cohort_id")
        if (
            not isinstance(route_cohort_id, str)
            or _COHORT_ID.fullmatch(route_cohort_id) is None
        ):
            raise RoutePublicationError(
                "route manifest has an invalid cohort ID"
            )
        if (
            expected_route_cohort_id is not None
            and route_cohort_id != expected_route_cohort_id
        ):
            raise RoutePublicationError(
                "route manifest cohort ID does not match pointer"
            )
        if require_directory_identity and bundle_name != route_cohort_id:
            raise RoutePublicationError(
                "route bundle directory identity is invalid"
            )
        files = manifest.get("files")
        if (
            not isinstance(files, dict)
            or set(files) != _MANIFEST_ARTIFACT_FILENAMES
        ):
            raise RoutePublicationError(
                "route manifest file inventory is invalid"
            )
        for filename, details in files.items():
            if not isinstance(details, dict) or set(details) != {
                "schema", "sha256", "logical_sha256", "row_count"
            }:
                raise RoutePublicationError(
                    "route manifest file details are invalid"
                )

        (
            sqlite_bytes,
            sqlite_physical_sha256,
            sqlite_cohort,
            sqlite_candidates,
            sqlite_legs,
            sqlite_timing,
        ) = _read_and_validate_sqlite_at(
            bundle_fd,
            ROUTE_SQLITE_FILENAME,
            route_cohort_id=route_cohort_id,
            source_snapshot=(
                file_fds[ROUTE_SQLITE_FILENAME],
                file_details[ROUTE_SQLITE_FILENAME],
                file_bytes[ROUTE_SQLITE_FILENAME],
                file_hashes[ROUTE_SQLITE_FILENAME],
            ),
        )
        file_bytes[ROUTE_SQLITE_FILENAME] = sqlite_bytes
        file_hashes[ROUTE_SQLITE_FILENAME] = sqlite_physical_sha256
        for filename, details in files.items():
            if details.get("sha256") != file_hashes[filename]:
                raise RoutePublicationError(
                    "route bundle file failed checksum validation: {}".format(
                        filename
                    )
                )

        raw_candidates = _read_csv_rows_bytes(
            file_bytes[ROUTE_CANDIDATES_FILENAME],
            columns=CANDIDATE_COLUMNS,
            label="route candidate CSV",
        )
        raw_legs = _read_csv_rows_bytes(
            file_bytes[ROUTE_LEGS_FILENAME],
            columns=LEG_COLUMNS,
            label="route leg CSV",
        )
        raw_timing = _read_csv_rows_bytes(
            file_bytes[ROUTE_TIMING_FILENAME],
            columns=TIMING_COLUMNS,
            label="route timing CSV",
        )
        csv_candidates = [
            _decode_row_json(row["row_json"], "route candidate CSV")
            for row in raw_candidates
        ]
        csv_legs = [
            _decode_row_json(row["row_json"], "route leg CSV")
            for row in raw_legs
        ]
        csv_timing = [
            _decode_row_json(row["row_json"], "route timing CSV")
            for row in raw_timing
        ]
        _validate_csv_projection(
            raw_candidates,
            csv_candidates,
            route_cohort_id=route_cohort_id,
            projector=_candidate_csv_row,
            label="route candidate CSV",
        )
        _validate_csv_projection(
            raw_legs,
            csv_legs,
            route_cohort_id=route_cohort_id,
            projector=_leg_csv_row,
            label="route leg CSV",
        )
        _validate_csv_projection(
            raw_timing,
            csv_timing,
            route_cohort_id=route_cohort_id,
            projector=_timing_csv_row,
            label="route timing CSV",
        )

        normalized = _normalize_and_validate_cohort(sqlite_cohort)
        if normalized["route_cohort_id"] != route_cohort_id:
            raise RoutePublicationError(
                "route SQLite cohort identity is invalid"
            )
        if (
            csv_candidates != sqlite_candidates
            or csv_legs != sqlite_legs
            or csv_timing != sqlite_timing
        ):
            raise RoutePublicationError(
                "CSV and SQLite route inventories do not match"
            )
        if (
            csv_candidates != normalized["routes"]
            or csv_legs != normalized["legs"]
            or csv_timing != normalized["route_rows"]
        ):
            raise RoutePublicationError(
                "route artifact inventories do not match cohort metadata"
            )

        expected_files = {
            ROUTE_CANDIDATES_FILENAME: _artifact_details_bytes(
                file_bytes[ROUTE_CANDIDATES_FILENAME],
                schema=ROUTE_CANDIDATE_CSV_SCHEMA,
                logical_sha256=_logical_rows_sha256(
                    ROUTE_CANDIDATE_CSV_SCHEMA,
                    csv_candidates,
                ),
                row_count=len(csv_candidates),
            ),
            ROUTE_LEGS_FILENAME: _artifact_details_bytes(
                file_bytes[ROUTE_LEGS_FILENAME],
                schema=ROUTE_LEG_CSV_SCHEMA,
                logical_sha256=_logical_rows_sha256(
                    ROUTE_LEG_CSV_SCHEMA,
                    csv_legs,
                ),
                row_count=len(csv_legs),
            ),
            ROUTE_TIMING_FILENAME: _artifact_details_bytes(
                file_bytes[ROUTE_TIMING_FILENAME],
                schema=ROUTE_TIMING_CSV_SCHEMA,
                logical_sha256=_logical_rows_sha256(
                    ROUTE_TIMING_CSV_SCHEMA,
                    csv_timing,
                ),
                row_count=len(csv_timing),
            ),
            ROUTE_SQLITE_FILENAME: _artifact_details_bytes(
                file_bytes[ROUTE_SQLITE_FILENAME],
                schema=ROUTE_SQLITE_LOGICAL_SCHEMA,
                logical_sha256=_database_logical_sha256(normalized),
                row_count=(
                    len(csv_candidates) + len(csv_legs) + len(csv_timing)
                ),
            ),
        }
        expected_manifest = _manifest_payload(normalized, expected_files)
        if manifest != expected_manifest:
            raise RoutePublicationError(
                "route manifest does not match bundle content"
            )
        _verify_bundle_file_snapshots(
            bundle_fd,
            read_specs,
            file_fds,
            file_details,
            file_bytes,
            file_hashes,
        )
        _verify_directory_entry(
            parent_fd,
            bundle_name,
            bundle_details,
            "route cohort bundle",
        )
        return {
            "path": bundle_path,
            "manifest_sha256": actual_manifest_sha256,
            "manifest": manifest,
            "cohort": normalized,
            "candidates": csv_candidates,
            "legs": csv_legs,
            "timing": csv_timing,
            "database_path": bundle_path / ROUTE_SQLITE_FILENAME,
        }
    finally:
        for file_fd in file_fds.values():
            os.close(file_fd)
        os.close(bundle_fd)


def _validate_route_cohort_bundle(
    bundle_path: Path,
    *,
    expected_route_cohort_id: Optional[str],
    expected_manifest_sha256: Optional[str],
    require_directory_identity: bool,
    parent_fd: Optional[int] = None,
) -> Dict[str, Any]:
    bundle = _absolute_without_symlink_resolution(Path(bundle_path))
    _require_relative_basename(bundle.name, "route cohort bundle name")
    owned_parent_fd: Optional[int] = None
    parent_details: Optional[os.stat_result] = None
    parent_path: Optional[Path] = None
    try:
        if parent_fd is None:
            parent_path, owned_parent_fd, parent_details = (
                _open_verified_directory(
                    bundle.parent,
                    "route cohort bundle root",
                )
            )
            parent_fd = owned_parent_fd
        result = _validate_route_cohort_bundle_at(
            parent_fd,
            bundle.name,
            bundle,
            expected_route_cohort_id=expected_route_cohort_id,
            expected_manifest_sha256=expected_manifest_sha256,
            require_directory_identity=require_directory_identity,
        )
        if parent_path is not None and parent_details is not None:
            _verify_open_path_identity(
                parent_path,
                parent_details,
                "route cohort bundle root",
            )
        return result
    finally:
        if owned_parent_fd is not None:
            os.close(owned_parent_fd)


def validate_route_cohort_bundle(
    bundle_path: Path,
    *,
    expected_route_cohort_id: Optional[str] = None,
    expected_manifest_sha256: Optional[str] = None
) -> Dict[str, Any]:
    """Fully reread and cross-validate one final immutable core bundle."""
    if expected_route_cohort_id is not None and (
        not isinstance(expected_route_cohort_id, str)
        or _COHORT_ID.fullmatch(expected_route_cohort_id) is None
    ):
        raise RoutePublicationError("expected route cohort ID is invalid")
    return _validate_route_cohort_bundle(
        bundle_path,
        expected_route_cohort_id=expected_route_cohort_id,
        expected_manifest_sha256=expected_manifest_sha256,
        require_directory_identity=True,
    )


def _replace_pointer_bytes_at(
    core_fd: int,
    value: bytes,
) -> None:
    temporary_name = ".latest.{}.tmp".format(secrets.token_hex(12))
    _write_new_bytes_at(core_fd, temporary_name, value)
    try:
        os.replace(
            temporary_name,
            "latest.json",
            src_dir_fd=core_fd,
            dst_dir_fd=core_fd,
        )
    except BaseException:
        try:
            os.unlink(temporary_name, dir_fd=core_fd)
        except OSError:
            pass
        raise


def _optional_pointer_snapshot_at(
    core_fd: int,
) -> Optional[Tuple[bytes, os.stat_result]]:
    try:
        details = os.stat(
            "latest.json",
            dir_fd=core_fd,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None
    except OSError as error:
        raise RoutePublicationError(
            "route core pointer is not readable"
        ) from error
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise RoutePublicationError(
            "route core pointer must be a regular non-symlink file"
        )
    value, _physical_sha256, read_details = _read_bounded_bytes_at(
        core_fd,
        "latest.json",
        limit=_MAX_JSON_BYTES,
        label="route core pointer",
    )
    return value, read_details


def _optional_pointer_bytes_at(core_fd: int) -> Optional[bytes]:
    snapshot = _optional_pointer_snapshot_at(core_fd)
    return None if snapshot is None else snapshot[0]


def _pointer_snapshot_is_owned(
    current: Optional[Tuple[bytes, os.stat_result]],
    expected: Tuple[bytes, os.stat_result],
) -> bool:
    return (
        current is not None
        and current[0] == expected[0]
        and _stable_file_metadata(current[1])
        == _stable_file_metadata(expected[1])
    )


def _atomic_replace_pointer_at(
    core_fd: int,
    core_path: Path,
    payload: Mapping[str, Any],
) -> None:
    try:
        fcntl.flock(core_fd, fcntl.LOCK_EX)
    except Exception as error:
        raise RoutePublicationError(
            "route core pointer lock acquisition failed"
        ) from error

    replace_succeeded = False
    operation_error: Optional[BaseException] = None
    operation_traceback = None
    try:
        value = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8") + b"\n"

        _replace_pointer_bytes_at(core_fd, value)
        replace_succeeded = True
        try:
            new_snapshot = _optional_pointer_snapshot_at(core_fd)
            if new_snapshot is None or new_snapshot[0] != value:
                raise RoutePublicationError("pointer state uncertain")
            _fsync_directory(core_path, directory_fd=core_fd)
            current_snapshot = _optional_pointer_snapshot_at(core_fd)
            if not _pointer_snapshot_is_owned(
                current_snapshot,
                new_snapshot,
            ):
                raise RoutePublicationError("pointer state uncertain")
        except Exception as error:
            if (
                isinstance(error, RoutePublicationError)
                and str(error) == "pointer state uncertain"
            ):
                raise
            raise RoutePublicationError("pointer state uncertain") from error
    except BaseException as error:
        operation_error = error
        operation_traceback = error.__traceback__

    try:
        fcntl.flock(core_fd, fcntl.LOCK_UN)
    except Exception as unlock_error:
        if operation_error is not None:
            raise operation_error.with_traceback(operation_traceback) \
                from operation_error.__cause__
        if replace_succeeded:
            raise RoutePublicationError(
                "route core pointer lock release failed after pointer commit"
            ) from unlock_error
        raise RoutePublicationError(
            "route core pointer lock release failed"
        ) from unlock_error

    if operation_error is not None:
        raise operation_error.with_traceback(operation_traceback)


def _atomic_replace_pointer(
    path: Path,
    payload: Mapping[str, Any],
    *,
    core_fd: Optional[int] = None,
) -> None:
    pointer_path = _absolute_without_symlink_resolution(Path(path))
    if pointer_path.name != "latest.json":
        raise RoutePublicationError("route core pointer path is invalid")
    owned_fd: Optional[int] = None
    try:
        if core_fd is None:
            _unused, owned_fd, _core_details = _open_verified_directory(
                pointer_path.parent,
                "route core root",
            )
            core_fd = owned_fd
        _atomic_replace_pointer_at(core_fd, pointer_path.parent, payload)
    finally:
        if owned_fd is not None:
            os.close(owned_fd)


def _ensure_directory_at(
    parent_fd: int,
    name: str,
    label: str,
) -> Tuple[int, os.stat_result]:
    _require_relative_basename(name, label)
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    except OSError as error:
        raise RoutePublicationError(
            "{} cannot be created".format(label)
        ) from error
    return _open_directory_at(parent_fd, name, label)


def _make_unique_directory_at(
    parent_fd: int,
    *,
    prefix: str,
    display_parent: Path,
) -> Tuple[str, Path, int, os.stat_result]:
    for _attempt in range(128):
        name = "{}{}".format(prefix, secrets.token_hex(12))
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
        except FileExistsError:
            continue
        fd, details = _open_directory_at(
            parent_fd,
            name,
            "route cohort stage",
        )
        return name, display_parent / name, fd, details
    raise RoutePublicationError("cannot allocate a unique route cohort stage")


def _entry_exists_at(parent_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False


def _remove_stage_directory_at(
    bundles_fd: int,
    stage_name: str,
    expected: os.stat_result,
) -> None:
    if not _entry_exists_at(bundles_fd, stage_name):
        return
    stage_fd, current = _open_directory_at(
        bundles_fd,
        stage_name,
        "route cohort stage",
    )
    try:
        if not _same_inode(expected, current):
            raise RoutePublicationError(
                "route cohort stage changed before cleanup"
            )
        for entry in os.listdir(stage_fd):
            details = os.stat(
                entry,
                dir_fd=stage_fd,
                follow_symlinks=False,
            )
            if not (
                stat.S_ISREG(details.st_mode)
                or stat.S_ISLNK(details.st_mode)
            ):
                raise RoutePublicationError(
                    "route cohort stage contains an unsafe cleanup entry"
                )
            os.unlink(entry, dir_fd=stage_fd)
    finally:
        os.close(stage_fd)
    os.rmdir(stage_name, dir_fd=bundles_fd)


def publish_route_cohort_bundle(
    cohort: Mapping[str, Any],
    *,
    core_root: Path = DEFAULT_ROUTE_CORE_ROOT
) -> Dict[str, Any]:
    """Stage, validate, rename, reread, and point at one immutable core bundle."""
    normalized = _normalize_and_validate_cohort(cohort)
    core = _ensure_real_directory(Path(core_root))
    core, core_fd, core_details = _open_verified_directory(
        core,
        "route core root",
    )
    route_cohort_id = normalized["route_cohort_id"]
    if _COHORT_ID.fullmatch(route_cohort_id) is None:
        os.close(core_fd)
        raise RoutePublicationError("route cohort ID is path-unsafe")
    bundles_fd: Optional[int] = None
    stage_fd: Optional[int] = None
    stage_name: Optional[str] = None
    stage_details: Optional[os.stat_result] = None
    renamed = False
    try:
        bundles_fd, bundles_details = _ensure_directory_at(
            core_fd,
            "bundles",
            "route core bundles root",
        )
        bundles = core / "bundles"
        final_path = bundles / route_cohort_id
        if _entry_exists_at(bundles_fd, route_cohort_id):
            raise RoutePublicationError(
                "immutable route cohort ID already exists"
            )
        (
            stage_name,
            stage_path,
            stage_fd,
            stage_details,
        ) = _make_unique_directory_at(
            bundles_fd,
            prefix=".route-cohort-",
            display_parent=bundles,
        )
        _write_bundle_artifacts(
            stage_path,
            normalized,
            stage_fd=stage_fd,
        )
        _validate_route_cohort_bundle(
            stage_path,
            expected_route_cohort_id=route_cohort_id,
            expected_manifest_sha256=None,
            require_directory_identity=False,
            parent_fd=bundles_fd,
        )
        _fsync_directory(stage_path, directory_fd=stage_fd)
        if _entry_exists_at(bundles_fd, route_cohort_id):
            raise RoutePublicationError("immutable route cohort ID already exists")
        _rename_directory_noreplace(
            stage_path,
            final_path,
            source_dir_fd=bundles_fd,
            destination_dir_fd=bundles_fd,
        )
        renamed = True
        _verify_directory_entry(
            bundles_fd,
            route_cohort_id,
            stage_details,
            "route cohort bundle",
        )
        _fsync_directory(bundles, directory_fd=bundles_fd)
        validated = _validate_route_cohort_bundle(
            final_path,
            expected_route_cohort_id=route_cohort_id,
            expected_manifest_sha256=None,
            require_directory_identity=True,
            parent_fd=bundles_fd,
        )
        pointer = {
            "schema": ROUTE_CORE_POINTER_SCHEMA,
            "bundle_stage": ROUTE_CORE_BUNDLE_STAGE,
            "route_cohort_id": route_cohort_id,
            "manifest_sha256": validated["manifest_sha256"],
        }
        _verify_directory_entry(
            core_fd,
            "bundles",
            bundles_details,
            "route core bundles root",
        )
        _verify_open_path_identity(core, core_details, "route core root")
        _atomic_replace_pointer(
            core / "latest.json",
            pointer,
            core_fd=core_fd,
        )
        return pointer
    finally:
        if stage_fd is not None:
            os.close(stage_fd)
        if (
            not renamed
            and bundles_fd is not None
            and stage_name is not None
            and stage_details is not None
        ):
            _remove_stage_directory_at(
                bundles_fd,
                stage_name,
                stage_details,
            )
        if bundles_fd is not None:
            os.close(bundles_fd)
        os.close(core_fd)


def load_latest_route_cohort(
    core_root: Path = DEFAULT_ROUTE_CORE_ROOT,
) -> Dict[str, Any]:
    """Resolve only the validated private core pointer and its exact bundle."""
    core, core_fd, core_details = _open_verified_directory(
        Path(core_root),
        "route core root",
    )
    bundles_fd: Optional[int] = None
    try:
        pointer_bytes = _optional_pointer_bytes_at(core_fd)
        if pointer_bytes is None:
            raise RoutePublicationError("route core pointer is missing")
        pointer = _decode_json_object_bytes(
            pointer_bytes,
            label="route core pointer",
        )
        if set(pointer) != {
            "schema", "bundle_stage", "route_cohort_id", "manifest_sha256"
        }:
            raise RoutePublicationError("route core pointer schema is invalid")
        if (
            pointer.get("schema") != ROUTE_CORE_POINTER_SCHEMA
            or pointer.get("bundle_stage") != ROUTE_CORE_BUNDLE_STAGE
        ):
            raise RoutePublicationError(
                "route core pointer schema is unsupported"
            )
        route_cohort_id = pointer.get("route_cohort_id")
        manifest_sha256 = pointer.get("manifest_sha256")
        if (
            not isinstance(route_cohort_id, str)
            or _COHORT_ID.fullmatch(route_cohort_id) is None
        ):
            raise RoutePublicationError(
                "route core pointer cohort ID is path-unsafe"
            )
        if (
            not isinstance(manifest_sha256, str)
            or _HEX_SHA256.fullmatch(manifest_sha256) is None
        ):
            raise RoutePublicationError(
                "route core pointer manifest hash is invalid"
            )
        bundles_fd, bundles_details = _open_directory_at(
            core_fd,
            "bundles",
            "route core bundles root",
        )
        bundle = core / "bundles" / route_cohort_id
        validated = _validate_route_cohort_bundle(
            bundle,
            expected_route_cohort_id=route_cohort_id,
            expected_manifest_sha256=manifest_sha256,
            require_directory_identity=True,
            parent_fd=bundles_fd,
        )
        if _optional_pointer_bytes_at(core_fd) != pointer_bytes:
            raise RoutePublicationError(
                "route core pointer changed during validation"
            )
        _verify_directory_entry(
            core_fd,
            "bundles",
            bundles_details,
            "route core bundles root",
        )
        _verify_open_path_identity(core, core_details, "route core root")
        validated["pointer"] = pointer
        return validated
    finally:
        if bundles_fd is not None:
            os.close(bundles_fd)
        os.close(core_fd)
