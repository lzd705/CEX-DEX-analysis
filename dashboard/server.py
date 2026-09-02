"""Serve the fact-only CEX/DEX Market Monitor."""

from __future__ import annotations

import argparse
import copy
import csv
import gzip
import hashlib
import hmac
import io
import ipaddress
import json
import math
import os
import posixpath
import re
import sqlite3
import sys
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from email.utils import formatdate, parsedate_to_datetime
from functools import lru_cache
from http.cookies import SimpleCookie
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Tuple
from urllib.parse import parse_qs, unquote, urlparse

try:
    from dashboard.admin import (
        AdminJobBusyError,
        AdminService,
        AdminWorkerStartError,
        environment_flag,
    )
    from dashboard.freshness import (
        build_source_freshness,
        route_opportunity_freshness,
    )
    from dashboard.opportunity_facts import (
        HISTORICAL_OPPORTUNITY_SUMMARY_CONTRACT,
        OpportunityBundleInvalid,
        OpportunityBundleUnavailable,
        OpportunityQueryError,
        build_historical_opportunity_payload,
        build_opportunity_payload,
        build_unavailable_historical_opportunity_payload,
        build_unavailable_opportunity_payload,
        historical_opportunity_data_generation,
        load_latest_historical_opportunities,
        load_latest_opportunities,
        normalize_opportunity_filters,
        opportunity_publication_health,
        resolve_opportunity_bundle,
    )
    from dashboard.market_facts import (
        LIFECYCLE_QUALITY_FLAG_MESSAGES,
        MARKET_QUALITY_THRESHOLDS,
        attach_explicit_dex_counts,
        build_token_summaries as build_fact_token_summaries,
        catalog_contract,
        catalog_from_market_payload,
        cex_market_id,
        compare_daily_rows,
        dex_market_id,
        enrich_market_quality,
        market_series_statistics,
    )
    from scripts.quality_outcomes import (
        aggregate_daily_quality_status,
        canonical_quality_fact_action,
        canonical_quality_fact_rule,
        cex_reason_code,
        dex_depth_reason_code,
        normalize_cex_source_outcome,
        normalize_dex_depth_source_outcome,
        normalize_execution_source_outcome,
        normalize_tvl_source_outcome,
        quality_outcome_rule,
        sanitize_public_source_endpoint,
    )
    from scripts.static_asset_contract import PUBLIC_STATIC_ASSET_SOURCES
    from dashboard.public_actions import (
        PUBLIC_ACTION_PATHS,
        PUBLIC_ADD_TOKEN_ACTOR,
        PUBLIC_FACT_REFRESH_ACTOR,
        PUBLIC_FACT_REFRESH_PATH,
        PUBLIC_JOB_STATUS_PREFIX,
        PUBLIC_QUALITY_RETRY_ACTOR,
        PUBLIC_QUALITY_RETRYABLE_PATH,
        PUBLIC_QUALITY_RETRY_PATH,
        PUBLIC_TOKEN_ADD_PATH,
        PUBLIC_TOKEN_HISTORY_DAYS,
        PUBLIC_TOKEN_RESOLVE_PATH,
        PublicActionError,
        PublicActionPolicy,
        public_job,
        public_retry_window,
        public_token_candidate,
        require_exact_string_fields,
    )
except ModuleNotFoundError:
    from admin import (  # type: ignore[no-redef]
        AdminJobBusyError,
        AdminService,
        AdminWorkerStartError,
        environment_flag,
    )
    from freshness import (  # type: ignore[no-redef]
        build_source_freshness,
        route_opportunity_freshness,
    )
    from opportunity_facts import (  # type: ignore[no-redef]
        HISTORICAL_OPPORTUNITY_SUMMARY_CONTRACT,
        OpportunityBundleInvalid,
        OpportunityBundleUnavailable,
        OpportunityQueryError,
        build_historical_opportunity_payload,
        build_opportunity_payload,
        build_unavailable_historical_opportunity_payload,
        build_unavailable_opportunity_payload,
        historical_opportunity_data_generation,
        load_latest_historical_opportunities,
        load_latest_opportunities,
        normalize_opportunity_filters,
        opportunity_publication_health,
        resolve_opportunity_bundle,
    )
    from market_facts import (
        LIFECYCLE_QUALITY_FLAG_MESSAGES,
        MARKET_QUALITY_THRESHOLDS,
        attach_explicit_dex_counts,
        build_token_summaries as build_fact_token_summaries,
        catalog_contract,
        catalog_from_market_payload,
        cex_market_id,
        compare_daily_rows,
        dex_market_id,
        enrich_market_quality,
        market_series_statistics,
    )
    from scripts.quality_outcomes import (
        aggregate_daily_quality_status,
        canonical_quality_fact_action,
        canonical_quality_fact_rule,
        cex_reason_code,
        dex_depth_reason_code,
        normalize_cex_source_outcome,
        normalize_dex_depth_source_outcome,
        normalize_execution_source_outcome,
        normalize_tvl_source_outcome,
        quality_outcome_rule,
        sanitize_public_source_endpoint,
    )
    from scripts.static_asset_contract import PUBLIC_STATIC_ASSET_SOURCES
    from public_actions import (  # type: ignore[no-redef]
        PUBLIC_ACTION_PATHS,
        PUBLIC_ADD_TOKEN_ACTOR,
        PUBLIC_FACT_REFRESH_ACTOR,
        PUBLIC_FACT_REFRESH_PATH,
        PUBLIC_JOB_STATUS_PREFIX,
        PUBLIC_QUALITY_RETRY_ACTOR,
        PUBLIC_QUALITY_RETRYABLE_PATH,
        PUBLIC_QUALITY_RETRY_PATH,
        PUBLIC_TOKEN_ADD_PATH,
        PUBLIC_TOKEN_HISTORY_DAYS,
        PUBLIC_TOKEN_RESOLVE_PATH,
        PublicActionError,
        PublicActionPolicy,
        public_job,
        public_retry_window,
        public_token_candidate,
        require_exact_string_fields,
    )

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.execution_cost import (
    EXECUTION_COST_COLUMNS,
    EXECUTION_COST_CONTRACT_VERSION,
    EXECUTION_DIRECTIONS,
    EXECUTION_NOTIONALS_USD,
    NOTIONAL_DEFINITION,
    RESULT_NUMERIC_COLUMNS,
    USD_PRICE_SKEW_MAX_SECONDS,
    USD_PRICE_SKEW_WARNING_SECONDS,
    execution_api_rows,
    usd_price_timing,
    validate_execution_snapshot,
)
from scripts.cex_instrument_lifecycle import (
    configured_market_ids_sha256,
    load_cex_instrument_lifecycle_manifest,
    validate_cex_instrument_lifecycle_review,
)
from scripts.collect_cex_instrument_lifecycle import (
    load_configured_crypto_com_markets,
)
from scripts.fetch_tvl import validate_tvl_fact_rows
from scripts.token_registry import (
    cex_market_ids_sha256,
    configured_cex_market_ids,
)
from scripts.timestamp_contract import (
    parse_rfc3339_utc,
    validate_observation_bounds,
)
from dashboard.event_facts import (
    EVENT_CLOCK_STATES,
    EventBundleError,
    LIFECYCLES,
    build_event_payload,
    load_latest_event_rows,
)

DASHBOARD_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = DASHBOARD_ROOT / "static"
DEFAULT_DATA_DIRS = [
    PROJECT_ROOT / "data/local",
    PROJECT_ROOT / "data/processed",
    PROJECT_ROOT / "data/public/facts",
]
CEX_FILENAME = "cex_exchange_volume_daily.csv"
DEX_FILENAME = "dex_pool_volume_daily.csv"
DATABASE_FILENAME = "market_facts.sqlite3"
TVL_FILENAME = "dex_pool_tvl_latest.csv"
CEX_DEPTH_FILENAME = "cex_depth_latest.csv"
CEX_DEPTH_HISTORY_FILENAME = "cex_depth_history.csv"
DEX_DEPTH_FILENAME = "dex_depth_latest.csv"
CEX_EXECUTION_COST_FILENAME = "cex_execution_cost_latest.csv"
DEX_EXECUTION_COST_FILENAME = "dex_execution_cost_latest.csv"
DAILY_QUALITY_REPORT_RELATIVE_PATH = Path("quality") / "daily-latest.json"
DAILY_QUALITY_REPORT_SCHEMA = "fact_quality_report/v1"
CEX_INSTRUMENT_LIFECYCLE_PATH = (
    PROJECT_ROOT / "data" / "curated" / "cex_instrument_lifecycle.json"
)
CEX_LIFECYCLE_TOKEN_CONFIG_PATH = PROJECT_ROOT / "config" / "tokens.csv"
MAX_DAILY_QUALITY_REPORT_BYTES = 8 * 1024 * 1024
MAX_DAILY_QUALITY_REPORT_ISSUES = 50_000
CEX_LIFECYCLE_MAX_AGE_SECONDS = 36 * 60 * 60
CEX_LIFECYCLE_SOURCE_URL = (
    "https://api.crypto.com/exchange/v1/public/get-instruments"
)
CEX_CURRENT_FACT_WITHHELD_STATUSES = frozenset(
    {
        "absent_from_official_current_catalog",
        "official_catalog_evidence_stale",
    }
)


def _validated_release_sha(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{40,64}", normalized):
        return normalized
    return None


def _read_application_release_sha() -> str:
    """Return the deployed application revision without executing Git."""

    configured = _validated_release_sha(os.environ.get("CEX_DEX_RELEASE_SHA"))
    if configured:
        return configured

    git_marker = PROJECT_ROOT / ".git"
    git_directory = git_marker
    if git_marker.is_file():
        marker = git_marker.read_text(encoding="utf-8").strip()
        if marker.startswith("gitdir:"):
            candidate = Path(marker.split(":", 1)[1].strip())
            git_directory = (
                candidate
                if candidate.is_absolute()
                else (PROJECT_ROOT / candidate).resolve()
            )
    if not git_directory.is_dir():
        return "unavailable"

    head_path = git_directory / "HEAD"
    if not head_path.is_file():
        return "unavailable"
    head = head_path.read_text(encoding="utf-8").strip()
    direct = _validated_release_sha(head)
    if direct:
        return direct
    if not head.startswith("ref: "):
        return "unavailable"
    reference = head[5:].strip()
    if not reference or reference.startswith("/") or ".." in Path(reference).parts:
        return "unavailable"
    reference_roots = [git_directory]
    common_marker = git_directory / "commondir"
    if common_marker.is_file():
        common_candidate = Path(
            common_marker.read_text(encoding="utf-8").strip()
        )
        common_directory = (
            common_candidate
            if common_candidate.is_absolute()
            else (git_directory / common_candidate).resolve()
        )
        if common_directory.is_dir() and common_directory not in reference_roots:
            reference_roots.append(common_directory)
    for reference_root in reference_roots:
        reference_path = reference_root / reference
        if reference_path.is_file():
            resolved = _validated_release_sha(
                reference_path.read_text(encoding="utf-8").strip()
            )
            if resolved:
                return resolved
        packed_refs = reference_root / "packed-refs"
        if packed_refs.is_file():
            for line in packed_refs.read_text(encoding="utf-8").splitlines():
                if not line or line.startswith(("#", "^")):
                    continue
                fields = line.split(" ", 1)
                if len(fields) == 2 and fields[1] == reference:
                    resolved = _validated_release_sha(fields[0])
                    if resolved:
                        return resolved
    return "unavailable"


@dataclass(frozen=True)
class StaticRepresentation:
    raw: bytes
    gzip: bytes
    content_type: str
    last_modified: str


STATIC_CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
}
IMMUTABLE_STATIC_CACHE_CONTROL = "public, max-age=31536000, immutable"


def _build_static_representations() -> dict[str, StaticRepresentation]:
    representations = {}
    for served_name, source_name in PUBLIC_STATIC_ASSET_SOURCES:
        source_path = STATIC_ROOT / source_name
        with source_path.open("rb") as source:
            raw = source.read()
            source_stat = os.fstat(source.fileno())
        representations[f"/{served_name}"] = StaticRepresentation(
            raw=raw,
            gzip=gzip.compress(raw, compresslevel=5, mtime=0),
            content_type=STATIC_CONTENT_TYPES[source_path.suffix],
            last_modified=formatdate(source_stat.st_mtime, usegmt=True),
        )
    return representations


_STATIC_REPRESENTATIONS = _build_static_representations()


def _compute_static_asset_sha(
    representations: dict[str, StaticRepresentation] | None = None,
) -> str:
    """Fingerprint the exact frozen asset bundle served by this process."""

    if representations is None:
        representations = _build_static_representations()
    digest = hashlib.sha256()
    for served_name, _source_path in PUBLIC_STATIC_ASSET_SOURCES:
        digest.update(served_name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(representations[f"/{served_name}"].raw)
        digest.update(b"\0")
    return digest.hexdigest()


# Freeze release evidence when this Python process imports the application.
# A later checkout update must not let an old process claim the new revision.
_PROCESS_APPLICATION_SHA = _read_application_release_sha()
_PROCESS_STATIC_ASSET_SHA = _compute_static_asset_sha(_STATIC_REPRESENTATIONS)


def application_release_sha() -> str:
    """Return the immutable application revision loaded by this process."""
    return _PROCESS_APPLICATION_SHA


def static_asset_sha() -> str:
    """Return the immutable served-asset fingerprint for this process."""
    return _PROCESS_STATIC_ASSET_SHA


def static_asset_version() -> str:
    release = application_release_sha()
    asset = static_asset_sha()
    release_component = release[:12] if release != "unavailable" else "unavailable"
    return f"{release_component}-{asset[:12]}"


def render_versioned_html(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace(
        "__ASSET_VERSION__",
        static_asset_version(),
    )


def static_representation(path: str) -> StaticRepresentation | None:
    """Return the immutable public representation for an exact asset path."""

    parsed = urlparse(path)
    if parsed.params:
        return None
    return _STATIC_REPRESENTATIONS.get(parsed.path)


_ACCEPT_ENCODING_QUALITY = re.compile(r"(?:0(?:\.\d{0,3})?|1(?:\.0{0,3})?)$")


def client_accepts_gzip(value: str) -> bool:
    """Return whether an Accept-Encoding header permits gzip without ambiguity."""

    gzip_qualities = []
    wildcard_qualities = []
    for item in value.split(","):
        parts = [part.strip() for part in item.split(";")]
        coding = parts[0].casefold()
        if not coding:
            return False
        quality = 1.0
        saw_quality = False
        for parameter in parts[1:]:
            name, separator, raw_quality = parameter.partition("=")
            if (
                separator != "="
                or name.strip().casefold() != "q"
                or saw_quality
                or not _ACCEPT_ENCODING_QUALITY.fullmatch(raw_quality.strip())
            ):
                return False
            saw_quality = True
            quality = float(raw_quality.strip())
        if coding == "gzip":
            gzip_qualities.append(quality)
        elif coding == "*":
            wildcard_qualities.append(quality)

    if gzip_qualities:
        return all(quality > 0 for quality in gzip_qualities)
    if wildcard_qualities:
        return all(quality > 0 for quality in wildcard_qualities)
    return False


def exact_static_version(path: str) -> bool:
    """Return true only for one unescaped current version query on a public path."""

    parsed = urlparse(path)
    if parsed.params or parsed.fragment or is_admin_surface_path(parsed.path):
        return False
    return parsed.query == f"v={static_asset_version()}"


VENDOR_FILES = {
    "/vendor/lucide.js": STATIC_ROOT / "vendor/lucide.min.js",
}
PUBLIC_DATA_UNAVAILABLE_MESSAGE = (
    "Market fact data is temporarily unavailable. Retry after the next refresh."
)
PUBLIC_DATA_VALIDATION_ERROR_CODE = "public_data_validation_failed"
PUBLIC_DATA_VALIDATION_ERROR_MESSAGE = (
    "Published market fact data failed validation. "
    "Retry after the next refresh."
)
OPPORTUNITY_DATA_VALIDATION_ERROR_CODE = (
    "opportunity_bundle_validation_failed"
)
OPPORTUNITY_DATA_VALIDATION_ERROR_MESSAGE = (
    "Published route opportunity data failed validation. "
    "Retry after the next complete publication."
)
NON_RETRYABLE_CEX_DEPTH_REASON_CODES = {
    "source_no_two_sided_book",
    "source_no_order_book",
    "source_invalid_order_book",
    "not_listed",
    "source_rejected_request",
    "unsupported_source",
}
CEX_DEPTH_REASON_CODES = NON_RETRYABLE_CEX_DEPTH_REASON_CODES | {
    "observed",
    "source_level_limit",
    "rate_limit",
    "source_unavailable",
    "network",
    "parse",
    "collection_failed",
}
API_FRESHNESS_CACHE_SECONDS = 60
LARGE_PAYLOAD_CACHE_SIZE = 8
SERIALIZED_RESPONSE_CACHE_SIZE = 64
OPPORTUNITY_RESPONSE_MAX_PROJECTIONS = 3
CATALOG_SUMMARY_VERSION = 3
SourceSignature = Tuple[Tuple[Any, ...], ...]
HistoricalPublicationSignature = Tuple[Tuple[Any, ...], ...]
PUBLIC_API_ROUTE_LOCKS = defaultdict(threading.RLock)
SOURCE_CACHE_GENERATION_LOCK = threading.RLock()
HISTORICAL_PUBLICATION_IDENTITY_LOCK = threading.RLock()
_HISTORICAL_POINTER_PUBLICATION_IDENTITIES: dict[
    str, HistoricalPublicationSignature
] = {}
_SOURCE_CACHE_GENERATION: SourceSignature | None = None
_PUBLIC_RESPONSE_CACHE_GENERATION: (
    tuple[SourceSignature, int] | None
) = None
SUMMARY_WARMUP_STATE_LOCK = threading.RLock()
_SUMMARY_WARMUP_STATE: dict[str, Any] | None = None
_SUMMARY_WARMUP_RUN_ID = 0
PUBLIC_API_QUERY_FIELDS = {
    "catalog": ("token", "start", "end"),
    "summary": ("start", "end"),
    "market": ("start", "end"),
    "compare": ("token", "market_a", "market_b", "start", "end"),
    "execution_cost": ("token", "market_a", "market_b"),
    "quality": (
        "token",
        "scope",
        "market_a",
        "market_b",
        "start",
        "end",
    ),
    "events": ("token", "start", "end", "lifecycle", "clock_state"),
    "opportunities": (
        "token",
        "venue",
        "notional",
        "class",
        "route_type",
        "availability",
        "sort",
        "dir",
    ),
    "opportunities_historical": (
        "token",
        "venue",
        "notional",
        "class",
        "route_type",
        "availability",
        "sort",
        "dir",
    ),
}


def require_stable_historical_pointer_publication_identity(
    *,
    pointer_sha256: str,
    publication_signature: HistoricalPublicationSignature,
) -> None:
    """Permanently bind one observed pointer hash to its physical descendants."""

    if (
        not isinstance(pointer_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", pointer_sha256) is None
        or type(publication_signature) is not tuple
        or not publication_signature
    ):
        raise OpportunityBundleInvalid()
    roles = set()
    pointer_digest = None
    for row in publication_signature:
        if (
            type(row) is not tuple
            or len(row) < 5
            or not isinstance(row[0], str)
            or not row[0]
            or row[0] in roles
            or not isinstance(row[1], str)
            or re.fullmatch(r"[0-9a-f]{64}", row[1]) is None
            or type(row[2]) is not int
            or row[2] <= 0
        ):
            raise OpportunityBundleInvalid()
        roles.add(row[0])
        if row[0] == "pointer:latest.json":
            pointer_digest = row[1]
    if pointer_digest != pointer_sha256:
        raise OpportunityBundleInvalid()
    with HISTORICAL_PUBLICATION_IDENTITY_LOCK:
        prior = _HISTORICAL_POINTER_PUBLICATION_IDENTITIES.get(
            pointer_sha256
        )
        if prior is None:
            _HISTORICAL_POINTER_PUBLICATION_IDENTITIES[
                pointer_sha256
            ] = publication_signature
            return
        if prior != publication_signature:
            raise OpportunityBundleInvalid()


def _reset_historical_pointer_publication_identities_for_tests() -> None:
    """Reset the process guard only for an isolated test process."""

    with HISTORICAL_PUBLICATION_IDENTITY_LOCK:
        _HISTORICAL_POINTER_PUBLICATION_IDENTITIES.clear()


class SourceGenerationChanged(FileNotFoundError):
    """Signal temporary source unavailability across one response build."""


class OpportunityResponseUnstable(FileNotFoundError):
    """Fail closed when route deadlines keep crossing during serialization."""


class DepthExecutionCohortError(RuntimeError):
    """A depth/execution publication does not form one exact cohort."""


class PublicClientRequestError(ValueError):
    """A bounded public GET parameter error that is safe to return as HTTP 400."""

ADMIN_STATIC_PATHS = {"/admin.html", "/admin.js", "/admin.css"}
SPA_TOKEN_PAGES = {"markets", "compare", "liquidity", "events", "quality"}
SPA_TOKEN_ROUTE = re.compile(
    r"/tokens/[A-Za-z0-9][A-Za-z0-9._-]*/"
    r"(?:markets|compare|liquidity|events|quality)/?"
)
SPA_METHODOLOGY_ROUTE = re.compile(
    r"/methodology/[a-z0-9]+(?:-[a-z0-9]+)*/?"
)
PUBLIC_JOB_STATUS_ROUTE = re.compile(
    rf"^{re.escape(PUBLIC_JOB_STATUS_PREFIX)}([0-9a-f]{{32}})$"
)


def load_local_environment(path: Path) -> None:
    """Load simple KEY=VALUE entries without executing the local env file."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_local_environment(PROJECT_ROOT / ".env")
ADMIN_SERVICE = AdminService()
PUBLIC_ACTION_POLICY = PublicActionPolicy()
TRUST_LOOPBACK_PROXY_CLIENT_IP = environment_flag(
    "TRUST_LOOPBACK_PROXY_CLIENT_IP"
)


def parse_number(value: str | float | int | None) -> float | None:
    """Parse a finite number while preserving missing values as null."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def parse_iso_date(value: str | None) -> str | None:
    """Validate an optional ISO date and return its normalized form."""
    if not value:
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except (TypeError, ValueError) as error:
        raise PublicClientRequestError(
            "date must use YYYY-MM-DD"
        ) from error


def resolve_data_paths() -> tuple[Path, Path]:
    """Resolve detailed CEX and DEX facts without relying on the current cwd."""
    explicit_cex = os.environ.get("MARKET_CEX_DATA")
    explicit_dex = os.environ.get("MARKET_DEX_DATA")
    if explicit_cex or explicit_dex:
        if not explicit_cex or not explicit_dex:
            raise FileNotFoundError("MARKET_CEX_DATA and MARKET_DEX_DATA must be set together")
        cex_path = Path(explicit_cex).expanduser().resolve()
        dex_path = Path(explicit_dex).expanduser().resolve()
        if not cex_path.exists() or not dex_path.exists():
            raise FileNotFoundError("Configured CEX or DEX data file does not exist")
        return cex_path, dex_path

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = [Path(configured_dir).expanduser().resolve()] if configured_dir else DEFAULT_DATA_DIRS
    for data_dir in candidates:
        cex_path = data_dir / CEX_FILENAME
        dex_path = data_dir / DEX_FILENAME
        if cex_path.exists() and dex_path.exists():
            return cex_path, dex_path
    checked = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"No detailed market snapshot found. Checked: {checked}")


def resolve_database_path() -> Path | None:
    """Prefer the published SQLite snapshot unless explicit CSV files are configured."""
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None

    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        database_path = Path(explicit_database).expanduser().resolve()
        if not database_path.exists():
            raise FileNotFoundError(f"Configured market database does not exist: {database_path}")
        return database_path

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = [Path(configured_dir).expanduser().resolve()] if configured_dir else DEFAULT_DATA_DIRS
    for data_dir in candidates:
        database_path = data_dir / DATABASE_FILENAME
        if database_path.exists():
            return database_path
    return None


def resolve_cex_instrument_lifecycle_path() -> Path:
    """Resolve the reviewed current-instrument manifest outside request input."""
    configured = os.environ.get("MARKET_CEX_INSTRUMENT_LIFECYCLE")
    path = (
        Path(configured).expanduser().resolve()
        if configured
        else CEX_INSTRUMENT_LIFECYCLE_PATH
    )
    if not path.is_file():
        raise FileNotFoundError("CEX instrument lifecycle manifest is unavailable")
    return path


def resolve_runtime_token_registry_path() -> Path:
    """Resolve the same runtime Token registry used by Add Token and collection."""
    configured = os.environ.get("TOKEN_REGISTRY_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    configured_data_dir = os.environ.get("MARKET_DATA_DIR")
    if configured_data_dir:
        data_dir = Path(configured_data_dir).expanduser().resolve()
    else:
        configured_database = os.environ.get("MARKET_DATABASE")
        data_dir = (
            Path(configured_database).expanduser().resolve().parent
            if configured_database
            else PROJECT_ROOT / "data" / "local"
        )
    return data_dir / "admin" / "token_registry.json"


def current_configured_crypto_com_market_ids() -> tuple[str, ...]:
    """Rebuild exact Crypto.com identities from the live static/runtime catalog."""
    markets = load_configured_crypto_com_markets(
        CEX_LIFECYCLE_TOKEN_CONFIG_PATH,
        resolve_runtime_token_registry_path(),
    )
    return tuple(market["market_id"] for market in markets)


def current_configured_upbit_market_ids() -> tuple[str, ...]:
    """Return exact Upbit identities from static and active runtime config."""
    return configured_cex_market_ids(
        CEX_LIFECYCLE_TOKEN_CONFIG_PATH,
        resolve_runtime_token_registry_path(),
        exchange="upbit",
    )


def attach_configured_cex_identity_metadata(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Publish the authority used to distinguish exact Upbit quote markets."""
    market_ids = current_configured_upbit_market_ids()
    result = {
        **payload,
        "metadata": {**payload["metadata"]},
    }
    result["metadata"]["configured_cex_market_identities"] = {
        "schema": "configured_cex_market_identities/v1",
        "upbit": {
            "market_count": len(market_ids),
            "market_ids": list(market_ids),
            "market_ids_sha256": cex_market_ids_sha256(
                market_ids,
                exchange="upbit",
            ),
        },
    }
    return result


def resolve_daily_quality_report_path() -> Path | None:
    """Resolve the quality report beside the exact published daily dataset.

    The report is optional.  It is deliberately derived from the selected
    database/CSV root instead of accepting a request-controlled path, and a
    symlink that leaves that root is ignored.
    """

    database_path = resolve_database_path()
    if database_path is not None:
        data_root = database_path.parent.resolve()
    else:
        cex_path, dex_path = resolve_data_paths()
        cex_parent = cex_path.parent.resolve()
        dex_parent = dex_path.parent.resolve()
        if cex_parent != dex_parent:
            return None
        data_root = cex_parent

    candidate = data_root / DAILY_QUALITY_REPORT_RELATIVE_PATH
    try:
        if candidate.is_symlink():
            return None
        resolved_candidate = candidate.resolve()
        resolved_candidate.relative_to(data_root)
    except (OSError, RuntimeError, ValueError):
        return None
    return candidate


def resolve_event_data_root() -> Path:
    """Resolve the optional Event Fact publication root.

    Unlike point-in-time market snapshots, absence is a valid state for this
    independently published, manually reviewed feed.
    """

    configured_root = os.environ.get("MARKET_EVENT_DATA_DIR")
    if configured_root:
        try:
            return Path(configured_root).expanduser().resolve()
        except (OSError, RuntimeError):
            candidate = Path(configured_root)
            return candidate if candidate.is_absolute() else Path.cwd() / candidate
    return (PROJECT_ROOT / "data/local/events").resolve()


def resolve_tvl_path() -> Path | None:
    """Resolve an optional point-in-time TVL snapshot independently of OHLCV."""
    explicit_tvl = os.environ.get("MARKET_TVL_DATA")
    if explicit_tvl:
        tvl_path = Path(explicit_tvl).expanduser().resolve()
        if not tvl_path.exists():
            raise FileNotFoundError(f"Configured TVL snapshot does not exist: {tvl_path}")
        return tvl_path
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None
    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        sibling = Path(explicit_database).expanduser().resolve().parent / TVL_FILENAME
        return sibling if sibling.exists() else None

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = [Path(configured_dir).expanduser().resolve()] if configured_dir else DEFAULT_DATA_DIRS
    for data_dir in candidates:
        tvl_path = data_dir / TVL_FILENAME
        if tvl_path.exists():
            return tvl_path
    return None


def resolve_cex_depth_path() -> Path | None:
    """Resolve an optional point-in-time CEX depth snapshot."""
    explicit_depth = os.environ.get("MARKET_CEX_DEPTH_DATA")
    if explicit_depth:
        depth_path = Path(explicit_depth).expanduser().resolve()
        if not depth_path.exists():
            raise FileNotFoundError(f"Configured CEX depth snapshot does not exist: {depth_path}")
        return depth_path
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None
    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        sibling = Path(explicit_database).expanduser().resolve().parent / CEX_DEPTH_FILENAME
        return sibling if sibling.exists() else None

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = [Path(configured_dir).expanduser().resolve()] if configured_dir else DEFAULT_DATA_DIRS
    for data_dir in candidates:
        depth_path = data_dir / CEX_DEPTH_FILENAME
        if depth_path.exists():
            return depth_path
    return None


def resolve_cex_depth_history_path(depth_path: Path | None) -> Path | None:
    """Resolve the retained normalized history beside the latest snapshot."""
    if depth_path is None:
        return None
    history_path = depth_path.with_name(CEX_DEPTH_HISTORY_FILENAME)
    return history_path if history_path.exists() else None


def resolve_dex_depth_path() -> Path | None:
    """Resolve an optional point-in-time DEX pool-state depth snapshot."""
    explicit_depth = os.environ.get("MARKET_DEX_DEPTH_DATA")
    if explicit_depth:
        depth_path = Path(explicit_depth).expanduser().resolve()
        if not depth_path.exists():
            raise FileNotFoundError(
                f"Configured DEX depth snapshot does not exist: {depth_path}"
            )
        return depth_path
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None
    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        sibling = (
            Path(explicit_database).expanduser().resolve().parent
            / DEX_DEPTH_FILENAME
        )
        return sibling if sibling.exists() else None

    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = (
        [Path(configured_dir).expanduser().resolve()]
        if configured_dir
        else DEFAULT_DATA_DIRS
    )
    for data_dir in candidates:
        depth_path = data_dir / DEX_DEPTH_FILENAME
        if depth_path.exists():
            return depth_path
    return None


def resolve_execution_cost_path(
    filename: str,
    environment_key: str,
) -> Path | None:
    """Resolve an optional long-form execution-cost snapshot."""
    explicit = os.environ.get(environment_key)
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(
                f"Configured execution-cost snapshot does not exist: {path}"
            )
        return path
    if os.environ.get("MARKET_CEX_DATA") or os.environ.get("MARKET_DEX_DATA"):
        return None
    explicit_database = os.environ.get("MARKET_DATABASE")
    if explicit_database:
        sibling = Path(explicit_database).expanduser().resolve().parent / filename
        return sibling if sibling.exists() else None
    configured_dir = os.environ.get("MARKET_DATA_DIR")
    candidates = (
        [Path(configured_dir).expanduser().resolve()]
        if configured_dir
        else DEFAULT_DATA_DIRS
    )
    for data_dir in candidates:
        path = data_dir / filename
        if path.exists():
            return path
    return None


def resolve_cex_execution_cost_path() -> Path | None:
    return resolve_execution_cost_path(
        CEX_EXECUTION_COST_FILENAME,
        "MARKET_CEX_EXECUTION_COST_DATA",
    )


def resolve_dex_execution_cost_path() -> Path | None:
    return resolve_execution_cost_path(
        DEX_EXECUTION_COST_FILENAME,
        "MARKET_DEX_EXECUTION_COST_DATA",
    )


def connect_database(database_path: Path) -> sqlite3.Connection:
    """Open the published database read-only so web requests cannot mutate facts."""
    connection = sqlite3.connect(f"{database_path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def iter_csv(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)


def dataset_bounds(paths: Iterable[Path]) -> tuple[str, str]:
    dates: list[str] = []
    for path in paths:
        dates.extend(row["date"] for row in iter_csv(path) if row.get("date"))
    if not dates:
        raise ValueError("Market data files contain no dated rows")
    return min(dates), max(dates)


def csv_date_bounds(path: Path) -> dict[str, str]:
    dates = [row["date"] for row in iter_csv(path) if row.get("date")]
    if not dates:
        raise ValueError(f"{path.name} contains no dated rows")
    return {
        "available_start": min(dates),
        "available_end": max(dates),
    }


def rows_in_window(path: Path, start_date: str, end_date: str) -> list[dict[str, str]]:
    return [
        row
        for row in iter_csv(path)
        if row.get("date") and start_date <= row["date"] <= end_date
    ]


def _cex_historical_lineage_from_rows(
    rows: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Return canonical CEX identity/count/date lineage from all daily rows."""
    identities: dict[str, tuple[str, str, str]] = {}
    observed_dates: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        token = row.get("token_symbol")
        exchange = row.get("exchange")
        instrument = row.get("cex_symbol")
        market_id = cex_market_id(exchange, instrument)
        identities[market_id] = (token, exchange, instrument)
        day = row.get("date")
        close = parse_number(row.get("close"))
        if (
            isinstance(day, str)
            and parse_iso_date(day) is not None
            and close is not None
            and close > 0
        ):
            observed_dates[market_id].add(day)

    inventory = {}
    for market_id, (token, exchange, instrument) in identities.items():
        dates = sorted(observed_dates[market_id])
        inventory[market_id] = {
            "token_symbol": token,
            "market": "cex",
            "venue": exchange,
            "instrument": instrument,
            "observation_count": len(dates),
            "observation_days": len(dates),
            "first_observed_date": dates[0] if dates else None,
            "latest_observed_date": dates[-1] if dates else None,
            "latest_date": dates[-1] if dates else None,
        }
    return inventory


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _load_cex_historical_lineage_cached(
    database_path_text: str,
    cex_path_text: str,
    _signature: SourceSignature,
) -> dict[str, dict[str, Any]]:
    """Load the full-history CEX identity inventory once per source generation."""
    if database_path_text:
        connection = connect_database(Path(database_path_text))
        try:
            rows = (
                dict(row)
                for row in connection.execute(
                    """
                    SELECT date, token_symbol, exchange, cex_symbol, close
                    FROM cex_market_daily
                    ORDER BY date, token_symbol, exchange, cex_symbol
                    """
                )
            )
            return _cex_historical_lineage_from_rows(rows)
        finally:
            connection.close()
    return _cex_historical_lineage_from_rows(iter_csv(Path(cex_path_text)))


def latest_non_null(rows: list[dict[str, Any]], field: str) -> float | None:
    for row in sorted(rows, key=lambda item: item["date"], reverse=True):
        value = parse_number(row.get(field))
        if value is not None:
            return value
    return None


def sum_observed(rows: list[dict[str, Any]], field: str) -> float | None:
    """Sum finite observations while preserving an all-missing series as null."""
    values = [
        value
        for row in rows
        if (value := parse_number(row.get(field))) is not None
    ]
    return sum(values) if values else None


def price_observations(rows: list[dict[str, Any]]) -> list[tuple[str, float]]:
    """Return sorted finite, positive daily closes."""
    observations = [
        (row["date"], parse_number(row.get("close")))
        for row in rows
        if (
            parse_number(row.get("close")) is not None
            and parse_number(row.get("close")) > 0
        )
    ]
    return sorted(observations)


def price_statistics(rows: list[dict[str, Any]]) -> tuple[float | None, float | None, float | None]:
    """Backward-compatible tuple backed by the auditable series contract."""
    result = market_series_statistics(rows)
    return (
        result["price_usd"],
        result["window_return"],
        result["daily_volatility"],
    )


def summarize_cex(
    rows: list[dict[str, str]],
    requested_start: str | None = None,
    requested_end: str | None = None,
    source_available_start: str | None = None,
    coverage_from_first_observation: bool = False,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[(row["token_symbol"], row["exchange"], row["cex_symbol"])].append(row)

    summaries = []
    for (token, exchange, symbol), market_rows in groups.items():
        statistics_payload = market_series_statistics(
            market_rows,
            requested_start=requested_start,
            requested_end=requested_end,
            source_available_start=source_available_start,
            coverage_from_first_observation=coverage_from_first_observation,
        )
        summaries.append(
            {
                "token_symbol": token,
                "market": "cex",
                "venue": exchange,
                "instrument": symbol,
                **statistics_payload,
                "volume_usd": sum_observed(market_rows, "quote_volume_usd"),
                "tvl_usd": None,
                "observation_days": statistics_payload["observation_count"],
                "latest_date": statistics_payload["latest_observed_date"],
                "price_points": [{"date": day, "price_usd": value} for day, value in price_observations(market_rows)],
            }
        )
    return sorted(
        summaries,
        key=lambda row: (
            row["token_symbol"],
            -(row["volume_usd"] if row["volume_usd"] is not None else 0.0),
            row["venue"],
        ),
    )


def summarize_dex(
    rows: list[dict[str, str]],
    requested_start: str | None = None,
    requested_end: str | None = None,
    source_available_start: str | None = None,
    coverage_from_first_observation: bool = False,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[
            (
                row["token_symbol"],
                row["chain"],
                row["dex"],
                row["pool_address"],
            )
        ].append(row)

    summaries = []
    for (token, chain, dex, address), pool_rows in groups.items():
        latest_row = max(
            pool_rows,
            key=lambda row: (row["date"], row.get("pool_name", "")),
        )
        pool_name = latest_row.get("pool_name") or address
        statistics_payload = market_series_statistics(
            pool_rows,
            requested_start=requested_start,
            requested_end=requested_end,
            source_available_start=source_available_start,
            coverage_from_first_observation=coverage_from_first_observation,
        )
        summaries.append(
            {
                "token_symbol": token,
                "market": "dex",
                "venue": f"{chain} / {dex}",
                "instrument": pool_name,
                "pool_address": address,
                **statistics_payload,
                "volume_usd": sum_observed(pool_rows, "dex_volume_usd"),
                "tvl_usd": latest_non_null(pool_rows, "pool_tvl_usd"),
                "observation_days": statistics_payload["observation_count"],
                "latest_date": statistics_payload["latest_observed_date"],
                "price_points": [{"date": day, "price_usd": value} for day, value in price_observations(pool_rows)],
            }
        )
    return sorted(
        summaries,
        key=lambda row: (
            row["token_symbol"],
            -(row["volume_usd"] if row["volume_usd"] is not None else 0.0),
            row["venue"],
        ),
    )


def select_primary(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for row in rows:
        token = row["token_symbol"]
        if token not in selected or row["volume_usd"] > selected[token]["volume_usd"]:
            selected[token] = row
    return selected


def common_price_comparison(
    cex: dict[str, Any] | None,
    dex: dict[str, Any] | None,
) -> tuple[str | None, float | None, float | None, float | None]:
    """Compare prices only on the latest date observed by both selected markets."""
    if not cex or not dex:
        return None, None, None, None
    cex_prices = {point["date"]: point["price_usd"] for point in cex["price_points"]}
    dex_prices = {point["date"]: point["price_usd"] for point in dex["price_points"]}
    common_dates = sorted(set(cex_prices) & set(dex_prices))
    if not common_dates:
        return None, None, None, None
    comparison_date = common_dates[-1]
    cex_price = cex_prices[comparison_date]
    dex_price = dex_prices[comparison_date]
    spread = dex_price / cex_price - 1 if cex_price else None
    return comparison_date, cex_price, dex_price, spread


def build_token_summaries(
    cex_markets: list[dict[str, Any]],
    dex_pools: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return build_fact_token_summaries(cex_markets, dex_pools)


def file_metadata(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    stat = path.stat()
    return {
        "name": path.name,
        "bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        "sha256": digest.hexdigest()[:16],
    }


def data_signature(paths: Iterable[Path]) -> SourceSignature:
    """Return a cache key that changes whenever a published data file changes."""
    signature = []
    for path in paths:
        stat = path.stat()
        signature.append(
            (
                str(path),
                stat.st_mtime_ns,
                stat.st_size,
                stat.st_ctime_ns,
                stat.st_ino,
            )
        )
    return tuple(signature)


def _inventory_observed_at_bounds(
    rows: Iterable[dict[str, Any]],
    field: str = "observed_at",
) -> tuple[str | None, str | None]:
    """Return canonical UTC bounds across valid full-inventory timestamps."""
    observed = []
    for row in rows:
        value = str(row.get(field) or "").strip()
        if not value:
            continue
        try:
            parsed = parse_rfc3339_utc(value)
        except (OverflowError, ValueError):
            continue
        observed.append(parsed)
    if not observed:
        return None, None
    return min(observed).isoformat(), max(observed).isoformat()


def _parse_cohort_timestamp(
    value: Any,
    label: str,
    *,
    empty_is_missing: bool = False,
) -> datetime | None:
    """Parse one timezone-aware cohort timestamp or fail closed."""
    if value is None or (empty_is_missing and value == ""):
        return None
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
    ):
        raise DepthExecutionCohortError(
            f"{label} must be a timezone-aware ISO timestamp"
        )
    try:
        return parse_rfc3339_utc(value)
    except (OverflowError, ValueError) as error:
        raise DepthExecutionCohortError(
            f"{label} must be a timezone-aware ISO timestamp"
        ) from error


def _cohort_observed_at_bounds(
    rows: Iterable[dict[str, Any]],
    field: str = "observed_at",
) -> tuple[str | None, str | None]:
    """Canonicalize valid row timestamps and return their exact UTC bounds."""
    observed = []
    for index, row in enumerate(rows):
        value = row.get(field)
        if field == "observed_at" and value in (None, ""):
            raise DepthExecutionCohortError(
                f"Cohort row {index + 1} {field} must be a "
                "timezone-aware ISO timestamp"
            )
        parsed = _parse_cohort_timestamp(
            value,
            f"Cohort row {index + 1} {field}",
            empty_is_missing=True,
        )
        if parsed is not None:
            row[field] = parsed.isoformat()
            observed.append(parsed)
    if not observed:
        return None, None
    return min(observed).isoformat(), max(observed).isoformat()


def _one_raw_cohort_id(
    rows: Iterable[dict[str, Any]],
    field: str,
    label: str,
) -> str:
    """Validate every raw lineage value before deriving its unique ID."""
    values = [row.get(field) for row in rows]
    if not values or not all(
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        for value in values
    ):
        raise DepthExecutionCohortError(
            f"{label} must contain one exact non-empty snapshot ID"
        )
    unique_values = set(values)
    if len(unique_values) != 1:
        raise DepthExecutionCohortError(
            f"{label} must contain one exact non-empty snapshot ID"
        )
    return next(iter(unique_values))


def observation_span_seconds(
    observed_at_min: str | None,
    observed_at_max: str | None,
) -> float | None:
    """Return the ordered span for canonical timestamp bounds."""
    if observed_at_min is None or observed_at_max is None:
        return None
    lower = _parse_cohort_timestamp(
        observed_at_min,
        "Cohort observed_at_min",
    )
    upper = _parse_cohort_timestamp(
        observed_at_max,
        "Cohort observed_at_max",
    )
    if lower is None or upper is None:  # pragma: no cover - guarded above
        return None
    span = (upper - lower).total_seconds()
    if span < 0:
        raise DepthExecutionCohortError(
            "Cohort observation bounds are reversed"
        )
    return span


@lru_cache(maxsize=8)
def _load_tvl_snapshot_cached(
    path_text: str,
    _signature: SourceSignature,
) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "snapshot_id",
            "observed_at",
            "token_symbol",
            "chain",
            "pool_address",
            "tvl_usd",
            "tvl_method",
            "source",
            "source_endpoint",
            "raw_response_sha256",
            "status",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{path.name} is missing TVL columns: {', '.join(missing)}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no TVL rows")

    observed_at_min, observed_at_max = validate_observation_bounds(rows)
    validate_tvl_fact_rows(rows)

    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (
            row["token_symbol"].upper(),
            row["chain"].lower(),
            row["pool_address"].lower()
            if row["pool_address"].startswith("0x")
            else row["pool_address"],
        )
        if key not in latest or row["observed_at"] > latest[key]["observed_at"]:
            latest[key] = row
    snapshot_ids = sorted({row["snapshot_id"] for row in rows if row.get("snapshot_id")})
    return {
        "path": path,
        "rows": latest,
        "snapshot_ids": snapshot_ids,
        "observed_at": observed_at_min,
        "observed_at_min": observed_at_min,
        "observed_at_max": observed_at_max,
        "status_counts": {
            status: sum(row.get("status") == status for row in rows)
            for status in ("observed", "missing", "not_found", "failed")
        },
    }


def _copy_payload_for_overlay(payload: dict[str, Any]) -> dict[str, Any]:
    """Copy only the payload parts an overlay is allowed to mutate."""
    return {
        **payload,
        "metadata": copy.deepcopy(payload["metadata"]),
        "cex_markets": [market.copy() for market in payload["cex_markets"]],
        "dex_pools": [pool.copy() for pool in payload["dex_pools"]],
    }


@lru_cache(maxsize=8)
def _load_cex_instrument_lifecycle_cached(
    path_text: str,
    _signature: SourceSignature,
) -> dict[str, Any]:
    return load_cex_instrument_lifecycle_manifest(Path(path_text))


def current_cex_facts_withheld(market: dict[str, Any]) -> bool:
    return (
        str(market.get("current_listing_status") or "").strip().lower()
        in CEX_CURRENT_FACT_WITHHELD_STATUSES
    )


def _lifecycle_only_cex_market(
    market_id: str,
    review: dict[str, Any],
    historical_lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    token = review.get("token_symbol")
    exchange = review.get("exchange")
    instrument = review.get("instrument")
    if not all(
        isinstance(value, str) and value == value.strip() and bool(value)
        for value in (token, exchange, instrument)
    ):
        raise ValueError("CEX lifecycle review identity is incomplete")
    expected_market_id = cex_market_id(exchange, instrument)
    if (
        exchange != "crypto_com"
        or expected_market_id != market_id
        or review.get("market_id") != market_id
    ):
        raise ValueError("CEX lifecycle review identity does not match the market")
    seed = {
        "token_symbol": token,
        "market": "cex",
        "venue": exchange,
        "instrument": instrument,
        "observation_count": 0,
        "observation_days": 0,
        "price_points": [],
    }
    if historical_lineage is None:
        return seed
    if (
        historical_lineage.get("token_symbol") != token
        or historical_lineage.get("venue") != exchange
        or historical_lineage.get("instrument") != instrument
    ):
        raise ValueError("CEX historical lineage identity does not match the market")
    observation_count = historical_lineage.get("observation_count")
    if (
        isinstance(observation_count, bool)
        or not isinstance(observation_count, int)
        or observation_count < 0
    ):
        raise ValueError("CEX historical lineage observation count is invalid")
    seed.update({
        "observation_count": observation_count,
        "observation_days": observation_count,
        "first_observed_date": historical_lineage.get(
            "first_observed_date"
        ),
        "latest_observed_date": historical_lineage.get(
            "latest_observed_date"
        ),
        "latest_date": historical_lineage.get("latest_date"),
    })
    return seed


def _validate_explicit_lifecycle_injection(
    reviews: dict[str, dict[str, Any]],
    lifecycle_evidence: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Validate direct review injection with manifest-equivalent guarantees."""
    if not isinstance(reviews, dict):
        raise ValueError("CEX lifecycle reviews must be keyed by market ID")
    validated_reviews = {}
    for market_id, raw_review in reviews.items():
        review = validate_cex_instrument_lifecycle_review(raw_review)
        if market_id != review["market_id"]:
            raise ValueError("CEX lifecycle review identity does not match the market")
        validated_reviews[market_id] = review

    if not isinstance(lifecycle_evidence, dict):
        raise ValueError("CEX lifecycle root evidence is invalid")
    checked_at_text = lifecycle_evidence.get("checked_at_utc")
    checked_at = parse_rfc3339_utc(checked_at_text)
    if checked_at.isoformat() != checked_at_text:
        raise ValueError("CEX lifecycle root timestamp is invalid")
    response_sha256 = lifecycle_evidence.get("response_sha256")
    if (
        not isinstance(response_sha256, str)
        or not re.fullmatch(r"[0-9a-f]{64}", response_sha256)
    ):
        raise ValueError("CEX lifecycle root response hash is invalid")
    inventory_count = lifecycle_evidence.get("inventory_count")
    configured_market_count = lifecycle_evidence.get(
        "configured_market_count"
    )
    if (
        isinstance(inventory_count, bool)
        or not isinstance(inventory_count, int)
        or inventory_count <= 0
    ):
        raise ValueError("CEX lifecycle root inventory count is invalid")
    if (
        isinstance(configured_market_count, bool)
        or not isinstance(configured_market_count, int)
        or configured_market_count <= 0
        or len(validated_reviews) > configured_market_count
    ):
        raise ValueError("CEX lifecycle configured market count is invalid")
    configured_hash = lifecycle_evidence.get("configured_market_ids_sha256")
    if (
        not isinstance(configured_hash, str)
        or not re.fullmatch(r"[0-9a-f]{64}", configured_hash)
    ):
        raise ValueError("CEX lifecycle configured market hash is invalid")
    if (
        validated_reviews
        and configured_market_count == len(validated_reviews)
        and configured_hash
        != configured_market_ids_sha256(validated_reviews)
    ):
        raise ValueError("CEX lifecycle configured market hash is inconsistent")
    for review in validated_reviews.values():
        if (
            review["checked_at_utc"] != checked_at_text
            or review["response_sha256"] != response_sha256
            or review["inventory_count"] != inventory_count
        ):
            raise ValueError("CEX lifecycle review evidence differs from root")
    return validated_reviews


def overlay_cex_instrument_lifecycle(
    payload: dict[str, Any],
    reviews: dict[str, dict[str, Any]] | None = None,
    *,
    lifecycle_evidence: dict[str, Any] | None = None,
    historical_cex_inventory: dict[str, dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Withhold stale current facts for officially absent CEX instruments.

    Historical source rows remain in the protected dataset. The public current
    projection preserves only their bounded observation count/date lineage and
    never infers a delisting date.
    """
    result = _copy_payload_for_overlay(payload)
    lifecycle_path = None
    if reviews is None:
        lifecycle_path = resolve_cex_instrument_lifecycle_path()
        manifest = _load_cex_instrument_lifecycle_cached(
            str(lifecycle_path),
            data_signature([lifecycle_path]),
        )
        reviews = {
            review["market_id"]: review
            for review in manifest["reviews"]
        }
        configured_market_ids = current_configured_crypto_com_market_ids()
        current_configured_count = len(configured_market_ids)
        current_configured_hash = configured_market_ids_sha256(
            configured_market_ids
        )
        if manifest["configured_market_count"] != current_configured_count:
            raise ValueError(
                "CEX lifecycle configured market count does not match current catalog"
            )
        if (
            manifest["configured_market_ids_sha256"]
            != current_configured_hash
        ):
            raise ValueError(
                "CEX lifecycle configured market set hash does not match current catalog"
            )
        unknown_reviews = set(reviews) - set(configured_market_ids)
        if unknown_reviews:
            raise ValueError(
                "CEX lifecycle absence review is outside the current catalog"
            )
        lifecycle_evidence = {
            "checked_at_utc": manifest["checked_at_utc"],
            "response_sha256": manifest["response_sha256"],
            "inventory_count": manifest["inventory_count"],
            "configured_market_count": manifest[
                "configured_market_count"
            ],
            "configured_market_ids_sha256": manifest[
                "configured_market_ids_sha256"
            ],
        }
    else:
        if not isinstance(reviews, dict) or not reviews:
            if lifecycle_evidence is None:
                raise ValueError("CEX lifecycle review root evidence is missing")
        elif lifecycle_evidence is None:
            checked_times = sorted({
                review.get("checked_at_utc")
                for review in reviews.values()
            })
            response_hashes = sorted({
                review.get("response_sha256")
                for review in reviews.values()
            })
            inventory_counts = {
                review.get("inventory_count")
                for review in reviews.values()
            }
            if (
                len(checked_times) != 1
                or len(response_hashes) != 1
                or len(inventory_counts) != 1
            ):
                raise ValueError("CEX lifecycle review root evidence is inconsistent")
            lifecycle_evidence = {
                "checked_at_utc": checked_times[0],
                "response_sha256": response_hashes[0],
                "inventory_count": next(iter(inventory_counts)),
                "configured_market_count": len(reviews),
                "configured_market_ids_sha256": configured_market_ids_sha256(
                    reviews
                ),
            }
        assert lifecycle_evidence is not None
        reviews = _validate_explicit_lifecycle_injection(
            reviews,
            lifecycle_evidence,
        )
    assert lifecycle_evidence is not None
    root_checked_at_text = lifecycle_evidence["checked_at_utc"]
    root_checked_at = parse_rfc3339_utc(root_checked_at_text)
    configured_market_count = lifecycle_evidence[
        "configured_market_count"
    ]
    official_inventory_count = lifecycle_evidence["inventory_count"]
    withheld_payload_count = 0
    reference_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    root_evidence_age_seconds = (
        reference_time - root_checked_at
    ).total_seconds()
    root_evidence_is_fresh = (
        -300 <= root_evidence_age_seconds <= CEX_LIFECYCLE_MAX_AGE_SECONDS
    )
    stale_evidence_count = (
        0 if root_evidence_is_fresh else configured_market_count
    )
    selected_window_market_ids = {
        cex_market_id(market.get("venue"), market.get("instrument"))
        for market in result["cex_markets"]
    }
    existing_market_ids = {
        cex_market_id(market.get("venue"), market.get("instrument"))
        for market in result["cex_markets"]
    }
    for market_id in sorted(reviews):
        if market_id in existing_market_ids:
            continue
        result["cex_markets"].append(
            _lifecycle_only_cex_market(
                market_id,
                reviews[market_id],
                (historical_cex_inventory or {}).get(market_id),
            )
        )
        existing_market_ids.add(market_id)
    for market in result["cex_markets"]:
        market_id = cex_market_id(market.get("venue"), market.get("instrument"))
        review = reviews.get(market_id)
        catalog_wide_stale = (
            not root_evidence_is_fresh
            and market.get("venue") == "crypto_com"
        )
        if review is None and not catalog_wide_stale:
            continue
        if review is not None:
            if (
                market.get("token_symbol") != review.get("token_symbol")
                or market.get("venue") != review.get("exchange")
                or market.get("instrument") != review.get("instrument")
            ):
                raise ValueError(
                    "CEX lifecycle review identity does not match the market"
                )
            checked_at_text = review["checked_at_utc"]
            source_url = review["source_url"]
            response_sha256 = review["response_sha256"]
        else:
            checked_at_text = root_checked_at_text
            source_url = CEX_LIFECYCLE_SOURCE_URL
            response_sha256 = lifecycle_evidence["response_sha256"]
        withheld_payload_count += 1
        checked_at = parse_rfc3339_utc(checked_at_text)
        evidence_age_seconds = (reference_time - checked_at).total_seconds()
        evidence_is_fresh = (
            -300 <= evidence_age_seconds <= CEX_LIFECYCLE_MAX_AGE_SECONDS
        )
        if review is not None and evidence_is_fresh and root_evidence_is_fresh:
            listing_status = review["current_listing_status"]
            listing_reason = review["reason_code"]
            depth_status = "source_no_observation"
        else:
            listing_status = "official_catalog_evidence_stale"
            listing_reason = "official_catalog_evidence_stale"
            depth_status = "needs_review"
        if market_id in selected_window_market_ids:
            selected_window_observation_count = market.get(
                "observation_count"
            )
            if (
                isinstance(selected_window_observation_count, bool)
                or not isinstance(selected_window_observation_count, int)
                or selected_window_observation_count < 0
            ):
                price_points = market.get("price_points")
                selected_window_observation_count = (
                    len(price_points) if isinstance(price_points, list) else 0
                )
        else:
            selected_window_observation_count = 0
        market["selected_window_observation_count"] = (
            selected_window_observation_count
        )
        market["historical_observation_count"] = market.get(
            "observation_count",
            market.get("observation_days"),
        )
        market["historical_first_observed_date"] = market.get(
            "first_observed_date"
        )
        market["historical_latest_observed_date"] = market.get(
            "latest_observed_date",
            market.get("latest_date"),
        )
        market["current_listing_status"] = listing_status
        market["current_listing_reason_code"] = listing_reason
        market["current_listing_checked_at"] = checked_at_text
        market["current_listing_evidence_age_seconds"] = evidence_age_seconds
        market["current_listing_source"] = source_url
        market["current_listing_response_sha256"] = response_sha256
        for field in (
            "price_usd",
            "volume_usd",
            "window_return",
            "daily_volatility",
            "coverage_ratio",
            "observation_count",
            "observation_days",
            "calendar_span_days",
            "requested_window_days",
            "missing_calendar_days",
            "return_interval_count",
            "skipped_gap_interval_count",
            "max_gap_days",
            "first_observed_date",
            "latest_observed_date",
            "latest_date",
            "coverage_expected_start",
            "coverage_expected_end",
        ):
            market[field] = None
        market["price_points"] = []
        for field in (
            "best_bid",
            "best_ask",
            "midpoint",
            "spread_quote",
            "spread_bps",
            "bid_depth_10bps_usd",
            "ask_depth_10bps_usd",
            "total_depth_10bps_usd",
            "bid_depth_25bps_usd",
            "ask_depth_25bps_usd",
            "total_depth_25bps_usd",
            "bid_depth_50bps_usd",
            "ask_depth_50bps_usd",
            "total_depth_50bps_usd",
            "bid_depth_100bps_usd",
            "ask_depth_100bps_usd",
            "total_depth_100bps_usd",
        ):
            market[field] = None
        for field in (
            "depth_10bps_complete",
            "depth_25bps_complete",
            "depth_50bps_complete",
            "depth_100bps_complete",
        ):
            market[field] = False
        market["depth_status"] = depth_status
        market["depth_source_status"] = None
        market["depth_reason_code"] = listing_reason
        market["depth_error"] = None
        market["depth_observed_at"] = checked_at_text
        market["depth_method"] = "official_current_instrument_catalog_membership"
        market["depth_snapshot_id"] = None
        market["depth_source"] = "Official current exchange instrument catalog"
        market["depth_source_endpoint"] = source_url
        market["depth_raw_response_sha256"] = response_sha256
        market["depth_source_instrument"] = None
        market["depth_source_quote_asset"] = None
        market["depth_quote_conversion_method"] = None

    metadata = result["metadata"]
    if "token_count" in metadata:
        metadata["token_count"] = len(
            {
                market["token_symbol"]
                for market in result["cex_markets"] + result["dex_pools"]
            }
        )
    metadata["cex_instrument_lifecycle"] = {
        "schema": "cex_instrument_lifecycle/v1",
        "reviewed_market_count": configured_market_count,
        "absence_market_count": len(reviews),
        # Fresh evidence applies only the exact absence set. Once the shared
        # root evidence expires, the fail-closed policy applies to the whole
        # configured Crypto.com catalog until a new official check succeeds.
        # A selected date window may contain fewer rows, so actual withholding
        # remains a separate payload count.
        "applied_market_count": (
            len(reviews)
            if root_evidence_is_fresh
            else configured_market_count
        ),
        "withheld_payload_market_count": withheld_payload_count,
        "stale_evidence_market_count": stale_evidence_count,
        "official_inventory_count": official_inventory_count,
        "response_sha256": lifecycle_evidence["response_sha256"],
        "configured_market_ids_sha256": lifecycle_evidence.get(
            "configured_market_ids_sha256"
        ),
        "freshness_max_age_seconds": CEX_LIFECYCLE_MAX_AGE_SECONDS,
        "checked_at_min": root_checked_at_text,
        "checked_at_max": root_checked_at_text,
        "semantics": (
            "Current official catalog absence withholds current facts but does "
            "not establish an exact delisting date."
        ),
    }
    if lifecycle_path is not None:
        source = file_metadata(lifecycle_path)
        metadata["cex_instrument_lifecycle"]["source"] = source
        metadata.setdefault("sources", []).append(source)
    return result


def overlay_tvl_snapshot(payload: dict[str, Any], tvl_path: Path | None) -> dict[str, Any]:
    """Overlay current source-reported TVL without rewriting historical OHLCV."""
    result = _copy_payload_for_overlay(payload)
    if tvl_path is None:
        for pool in result["dex_pools"]:
            pool["tvl_status"] = (
                "legacy_ohlcv_snapshot"
                if pool.get("tvl_usd") is not None
                else "unavailable"
            )
            pool["tvl_observed_at"] = None
            pool["tvl_method"] = "legacy_geckoterminal_reserve_in_usd"
            pool["tvl_snapshot_id"] = None
            pool["tvl_source"] = None
            pool["tvl_source_endpoint"] = None
            pool["tvl_raw_response_sha256"] = None
            pool["tvl_reason_code"] = (
                "legacy_ohlcv_snapshot"
                if pool["tvl_status"] == "legacy_ohlcv_snapshot"
                else "tvl_snapshot_unavailable"
            )
            pool["tvl_error"] = None
        return result

    snapshot = _load_tvl_snapshot_cached(
        str(tvl_path),
        data_signature([tvl_path]),
    )
    matched = 0
    for pool in result["dex_pools"]:
        chain, _ = pool["venue"].split(" / ", 1)
        address = pool["pool_address"]
        key = (
            pool["token_symbol"].upper(),
            chain.lower(),
            address.lower() if address.startswith("0x") else address,
        )
        tvl_row = snapshot["rows"].get(key)
        if tvl_row is None:
            pool["tvl_usd"] = None
            pool["tvl_status"] = "not_cataloged_in_snapshot"
            pool["tvl_observed_at"] = None
            pool["tvl_method"] = None
            pool["tvl_snapshot_id"] = None
            pool["tvl_source"] = None
            pool["tvl_source_endpoint"] = None
            pool["tvl_raw_response_sha256"] = None
            pool["tvl_reason_code"] = "tvl_market_not_cataloged_in_snapshot"
            pool["tvl_error"] = None
            continue
        matched += 1
        tvl_status, tvl_reason_code = normalize_tvl_source_outcome(
            tvl_row.get("status"),
            tvl_row.get("reason_code"),
            tvl_row.get("error"),
        )
        pool["tvl_usd"] = (
            parse_number(tvl_row.get("tvl_usd"))
            if tvl_row.get("status") == "observed"
            else None
        )
        pool["tvl_source_status"] = tvl_row.get("status")
        pool["tvl_status"] = tvl_status
        pool["tvl_reason_code"] = tvl_reason_code
        pool["tvl_observed_at"] = tvl_row.get("observed_at") or None
        pool["tvl_method"] = tvl_row.get("tvl_method") or None
        pool["tvl_snapshot_id"] = tvl_row.get("snapshot_id") or None
        pool["tvl_source"] = tvl_row.get("source") or None
        pool["tvl_source_endpoint"] = tvl_row.get("source_endpoint") or None
        pool["tvl_raw_response_sha256"] = tvl_row.get("raw_response_sha256") or None
        pool["tvl_error"] = tvl_row.get("error") or None

    metadata = result["metadata"]
    metadata["tvl_note"] = (
        "Pool TVL is a separate point-in-time GeckoTerminal reserve_in_usd "
        "snapshot. It is not historical daily TVL and it is not market depth."
    )
    metadata["tvl_snapshot"] = {
        "snapshot_ids": snapshot["snapshot_ids"],
        "observed_at": snapshot["observed_at"],
        "observed_at_min": snapshot["observed_at_min"],
        "observed_at_max": snapshot["observed_at_max"],
        "pool_rows": len(snapshot["rows"]),
        "matched_market_rows": matched,
        "status_counts": snapshot["status_counts"],
        "source": file_metadata(tvl_path),
        "method": "geckoterminal_reserve_in_usd",
    }
    metadata["sources"].append(file_metadata(tvl_path))
    return result


@lru_cache(maxsize=8)
def _load_cex_depth_snapshot_cached(
    path_text: str,
    _signature: SourceSignature,
) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "snapshot_id",
            "observed_at",
            "response_received_at",
            "token_symbol",
            "exchange",
            "cex_symbol",
            "source_instrument",
            "source_quote_asset",
            "quote_conversion_method",
            "best_bid",
            "best_ask",
            "midpoint",
            "spread_quote",
            "spread_bps",
            "bid_depth_10bps_usd",
            "ask_depth_10bps_usd",
            "total_depth_10bps_usd",
            "bid_depth_25bps_usd",
            "ask_depth_25bps_usd",
            "total_depth_25bps_usd",
            "bid_depth_50bps_usd",
            "ask_depth_50bps_usd",
            "total_depth_50bps_usd",
            "bid_depth_100bps_usd",
            "ask_depth_100bps_usd",
            "total_depth_100bps_usd",
            "depth_10bps_complete",
            "depth_25bps_complete",
            "depth_50bps_complete",
            "depth_100bps_complete",
            "depth_method",
            "source_endpoint",
            "raw_response_sha256",
            "status",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{path.name} is missing CEX depth columns: {', '.join(missing)}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no CEX depth rows")

    snapshot_id = _one_raw_cohort_id(
        rows,
        "snapshot_id",
        "CEX depth publication",
    )
    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (
            row["token_symbol"].upper(),
            row["exchange"].lower(),
            row["cex_symbol"].upper(),
        )
        if key in latest:
            raise DepthExecutionCohortError(
                "CEX depth publication contains a duplicate market identity"
            )
        latest[key] = row
    observed_at_min, observed_at_max = _cohort_observed_at_bounds(
        latest.values()
    )
    return {
        "path": path,
        "rows": latest,
        "snapshot_ids": [snapshot_id],
        "observed_at": observed_at_min,
        "observed_at_min": observed_at_min,
        "observed_at_max": observed_at_max,
        "observation_span_seconds": observation_span_seconds(
            observed_at_min,
            observed_at_max,
        ),
        "status_counts": {
            status: sum(
                row.get("status") == status for row in latest.values()
            )
            for status in ("observed", "partial", "failed")
        },
    }


def _cex_depth_history_summary(history_path: Path | None) -> dict[str, Any]:
    unavailable = {
        "retained_history_status": "unavailable",
        "retained_history_available_from": None,
        "retained_history_row_count": None,
        "retained_history_source": None,
    }
    if history_path is None:
        return unavailable
    try:
        with history_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            required = {
                "snapshot_id",
                "observed_at",
                "token_symbol",
                "exchange",
                "cex_symbol",
            }
            if required - set(reader.fieldnames or []):
                return unavailable
            rows = list(reader)
        if not rows or any(
            not isinstance(row.get(field), str)
            or not row[field]
            or row[field] != row[field].strip()
            for row in rows
            for field in required
        ):
            return unavailable
        available_from, _available_to = _cohort_observed_at_bounds(rows)
        if available_from is None:
            return unavailable
        return {
            "retained_history_status": "available",
            "retained_history_available_from": available_from,
            "retained_history_row_count": len(rows),
            "retained_history_source": file_metadata(history_path),
        }
    except (OSError, ValueError, csv.Error, DepthExecutionCohortError):
        return unavailable


def cex_depth_reason_code(depth_row: dict[str, str]) -> str | None:
    """Prefer the stable collector code and classify legacy rows conservatively."""
    classified_reason = cex_reason_code(
        depth_row.get("reason_code"), depth_row.get("error")
    )
    if classified_reason:
        return classified_reason
    status = str(depth_row.get("status") or "").strip()
    if status == "observed":
        return "observed"
    if status == "partial":
        return "source_level_limit"
    if status == "failed":
        return "collection_failed"
    return None


def overlay_cex_depth_snapshot(
    payload: dict[str, Any],
    depth_path: Path | None,
    history_path: Path | None,
) -> dict[str, Any]:
    """Overlay current real order-book depth without rewriting daily OHLCV."""
    result = _copy_payload_for_overlay(payload)
    if depth_path is None:
        for market in result["cex_markets"]:
            market["depth_status"] = "unavailable"
            market["depth_observed_at"] = None
            market["depth_method"] = None
            market["depth_snapshot_id"] = None
            market["depth_source"] = None
            market["depth_source_endpoint"] = None
            market["depth_raw_response_sha256"] = None
            market["depth_reason_code"] = "depth_snapshot_unavailable"
            market["depth_source_status"] = None
            market["depth_error"] = None
        result["metadata"]["cex_depth_note"] = (
            "CEX depth snapshot is unavailable. Daily volume is not used as a depth proxy."
        )
        return result

    snapshot = _load_cex_depth_snapshot_cached(
        str(depth_path),
        data_signature([depth_path]),
    )
    matched = 0
    numeric_fields = (
        "best_bid",
        "best_ask",
        "midpoint",
        "spread_quote",
        "spread_bps",
        "bid_depth_10bps_usd",
        "ask_depth_10bps_usd",
        "total_depth_10bps_usd",
        "bid_depth_25bps_usd",
        "ask_depth_25bps_usd",
        "total_depth_25bps_usd",
        "bid_depth_50bps_usd",
        "ask_depth_50bps_usd",
        "total_depth_50bps_usd",
        "bid_depth_100bps_usd",
        "ask_depth_100bps_usd",
        "total_depth_100bps_usd",
    )
    completeness_fields = (
        "depth_10bps_complete",
        "depth_25bps_complete",
        "depth_50bps_complete",
        "depth_100bps_complete",
    )
    for market in result["cex_markets"]:
        key = (
            market["token_symbol"].upper(),
            market["venue"].lower(),
            market["instrument"].upper(),
        )
        depth_row = snapshot["rows"].get(key)
        if depth_row is None:
            market["depth_status"] = "not_cataloged_in_snapshot"
            market["depth_observed_at"] = None
            market["depth_method"] = None
            market["depth_snapshot_id"] = None
            market["depth_source"] = None
            market["depth_source_endpoint"] = None
            market["depth_raw_response_sha256"] = None
            market["depth_reason_code"] = (
                "depth_market_not_cataloged_in_snapshot"
            )
            market["depth_source_status"] = None
            market["depth_error"] = None
            for field in numeric_fields:
                market[field] = None
            for field in completeness_fields:
                market[field] = False
            continue

        matched += 1
        source_status = depth_row.get("status")
        depth_status, depth_reason_code = normalize_cex_source_outcome(
            source_status,
            depth_row.get("reason_code"),
            depth_row.get("error"),
        )
        market["depth_source_status"] = source_status
        market["depth_status"] = depth_status
        market["depth_observed_at"] = depth_row.get("observed_at") or None
        market["depth_method"] = depth_row.get("depth_method") or None
        market["depth_snapshot_id"] = depth_row.get("snapshot_id") or None
        market["depth_source"] = depth_row.get("source") or None
        market["depth_source_instrument"] = depth_row.get("source_instrument") or None
        market["depth_source_quote_asset"] = depth_row.get("source_quote_asset") or None
        market["depth_quote_conversion_method"] = (
            depth_row.get("quote_conversion_method") or None
        )
        market["depth_source_endpoint"] = depth_row.get("source_endpoint") or None
        market["depth_raw_response_sha256"] = (
            depth_row.get("raw_response_sha256") or None
        )
        market["depth_reason_code"] = depth_reason_code
        market["depth_error"] = depth_row.get("error") or None
        observed = source_status in {"observed", "partial"}
        for field in numeric_fields:
            market[field] = parse_number(depth_row.get(field)) if observed else None
        for field in completeness_fields:
            market[field] = observed and depth_row.get(field) == "1"

    metadata = result["metadata"]
    metadata["cex_depth_note"] = (
        "CEX depth is a separate point-in-time public spot order-book snapshot. "
        "USD values are quote notional inside symmetric midpoint bands. Partial "
        "bands are observed lower bounds, not complete depth."
    )
    metadata["cex_depth_snapshot"] = {
        "scope": "point_in_time",
        "snapshot_ids": snapshot["snapshot_ids"],
        "observed_at": snapshot["observed_at"],
        "observed_at_min": snapshot["observed_at_min"],
        "observed_at_max": snapshot["observed_at_max"],
        "observation_span_seconds": snapshot[
            "observation_span_seconds"
        ],
        "market_rows": len(snapshot["rows"]),
        "matched_market_rows": matched,
        "status_counts": snapshot["status_counts"],
        "bands_bps": [10, 25, 50, 100],
        "source": file_metadata(depth_path),
        "method": "midpoint_symmetric_quote_notional",
        **_cex_depth_history_summary(history_path),
    }
    metadata["sources"].append(file_metadata(depth_path))
    return result


@lru_cache(maxsize=8)
def _load_dex_depth_snapshot_cached(
    path_text: str,
    _signature: SourceSignature,
) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "snapshot_id",
            "observed_at",
            "response_received_at",
            "token_symbol",
            "chain",
            "dex",
            "pool_address",
            "protocol_model",
            "block_number",
            "fee_bps",
            "pool_state_price_usd",
            "source_target_price_usd",
            "price_difference_bps",
            "sell_depth_10bps_usd",
            "buy_depth_10bps_usd",
            "total_depth_10bps_usd",
            "sell_depth_25bps_usd",
            "buy_depth_25bps_usd",
            "total_depth_25bps_usd",
            "sell_depth_50bps_usd",
            "buy_depth_50bps_usd",
            "total_depth_50bps_usd",
            "sell_depth_100bps_usd",
            "buy_depth_100bps_usd",
            "total_depth_100bps_usd",
            "depth_10bps_complete",
            "depth_25bps_complete",
            "depth_50bps_complete",
            "depth_100bps_complete",
            "depth_method",
            "source_endpoint",
            "raw_response_sha256",
            "status",
            "error",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(
                f"{path.name} is missing DEX depth columns: {', '.join(missing)}"
            )
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no DEX depth rows")

    allowed_reasons_by_status = {
        "observed": {"observed"},
        "complete": {"observed"},
        "partial": {"measurement_limit"},
        "unsupported": {
            "source_range_unavailable",
            "unsupported_chain",
            "unsupported_protocol",
            "unsupported_method",
            "unsupported_source",
            "unsupported_protocol_or_chain",
        },
        "failed": {
            "network",
            "rate_limit",
            "source_unavailable",
            "parse",
            "validation",
            "collection_failed",
            "depth_usd_price_time_mismatch",
        },
    }
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        if status not in allowed_reasons_by_status:
            raise ValueError("DEX depth publication status is invalid")
        supplied_reason = str(row.get("reason_code") or "").strip().lower()
        if "reason_code" in row and not supplied_reason:
            raise ValueError("DEX depth publication reason code is missing")
        bounded_reason = dex_depth_reason_code(supplied_reason)
        if supplied_reason and bounded_reason is None:
            raise ValueError("DEX depth publication reason code is invalid")
        if (
            bounded_reason
            and bounded_reason not in allowed_reasons_by_status[status]
        ):
            raise ValueError(
                "DEX depth publication status and reason code conflict"
            )
        if status == "failed":
            source_hash = str(row.get("raw_response_sha256") or "")
            if (
                len(source_hash) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in source_hash
                )
            ):
                raise ValueError(
                    "DEX depth publication failed row source hash is invalid"
                )

    snapshot_id = _one_raw_cohort_id(
        rows,
        "snapshot_id",
        "DEX depth publication",
    )
    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        address = row["pool_address"]
        key = (
            row["token_symbol"].upper(),
            row["chain"].lower(),
            address.lower() if address.startswith("0x") else address,
        )
        if key in latest:
            raise DepthExecutionCohortError(
                "DEX depth publication contains a duplicate market identity"
            )
        latest[key] = row
    observed_at_min, observed_at_max = _cohort_observed_at_bounds(
        latest.values()
    )
    return {
        "path": path,
        "rows": latest,
        "snapshot_ids": [snapshot_id],
        "observed_at": observed_at_min,
        "observed_at_min": observed_at_min,
        "observed_at_max": observed_at_max,
        "observation_span_seconds": observation_span_seconds(
            observed_at_min,
            observed_at_max,
        ),
        "status_counts": dict(
            Counter(
                row.get("status") or "missing_status"
                for row in latest.values()
            )
        ),
    }


def overlay_dex_depth_snapshot(
    payload: dict[str, Any],
    depth_path: Path | None,
) -> dict[str, Any]:
    """Overlay fixed-block DEX pool-state depth without using TVL as a proxy."""
    result = _copy_payload_for_overlay(payload)
    if depth_path is None:
        for pool in result["dex_pools"]:
            pool["dex_depth_status"] = "unavailable"
            pool["dex_depth_observed_at"] = None
            pool["dex_depth_method"] = None
            pool["dex_depth_snapshot_id"] = None
            pool["dex_depth_source"] = None
            pool["dex_depth_source_endpoint"] = None
            pool["dex_depth_raw_response_sha256"] = None
            pool["dex_depth_reason_code"] = "depth_snapshot_unavailable"
            pool["dex_depth_error"] = None
            pool["dex_depth_block_timestamp"] = None
            pool["dex_depth_usd_price_source_snapshot_id"] = None
            pool["dex_depth_usd_price_observed_at"] = None
            pool["dex_depth_usd_price_skew_seconds"] = None
            pool["dex_depth_usd_price_freshness_status"] = "unavailable"
            pool["depth_requires_usd_price_alignment"] = False
        result["metadata"]["dex_depth_note"] = (
            "DEX pool-state depth snapshot is unavailable. TVL and daily volume "
            "are not used as depth proxies."
        )
        return result

    snapshot = _load_dex_depth_snapshot_cached(
        str(depth_path),
        data_signature([depth_path]),
    )
    matched = 0
    numeric_fields = [
        "fee_bps",
        "pool_state_price_usd",
        "source_target_price_usd",
        "price_difference_bps",
    ] + [
        f"{side}_depth_{band}bps_usd"
        for band in (10, 25, 50, 100)
        for side in ("sell", "buy", "total")
    ]
    completeness_fields = [
        f"depth_{band}bps_complete"
        for band in (10, 25, 50, 100)
    ]
    for pool in result["dex_pools"]:
        chain, _ = pool["venue"].split(" / ", 1)
        address = pool["pool_address"]
        key = (
            pool["token_symbol"].upper(),
            chain.lower(),
            address.lower() if address.startswith("0x") else address,
        )
        depth_row = snapshot["rows"].get(key)
        if depth_row is None:
            pool["dex_depth_status"] = "not_cataloged_in_snapshot"
            pool["dex_depth_observed_at"] = None
            pool["dex_depth_method"] = None
            pool["dex_depth_snapshot_id"] = None
            pool["dex_depth_source"] = None
            pool["dex_depth_source_endpoint"] = None
            pool["dex_depth_raw_response_sha256"] = None
            pool["dex_depth_reason_code"] = (
                "depth_market_not_cataloged_in_snapshot"
            )
            pool["dex_depth_error"] = None
            pool["dex_depth_block_timestamp"] = None
            pool["dex_depth_usd_price_source_snapshot_id"] = None
            pool["dex_depth_usd_price_observed_at"] = None
            pool["dex_depth_usd_price_skew_seconds"] = None
            pool["dex_depth_usd_price_freshness_status"] = "unavailable"
            pool["depth_requires_usd_price_alignment"] = False
            for field in numeric_fields:
                pool[field] = None
            for field in completeness_fields:
                pool[field] = False
            continue

        matched += 1
        source_status = depth_row.get("status")
        depth_status, depth_reason_code = normalize_dex_depth_source_outcome(
            source_status,
            depth_row.get("reason_code"),
            depth_row.get("error"),
        )
        pool["dex_depth_status"] = depth_status
        pool["dex_depth_reason_code"] = depth_reason_code
        pool["dex_depth_observed_at"] = depth_row.get("observed_at") or None
        pool["dex_depth_method"] = depth_row.get("depth_method") or None
        pool["dex_depth_snapshot_id"] = depth_row.get("snapshot_id") or None
        pool["dex_depth_source"] = depth_row.get("source") or None
        pool["dex_depth_protocol_model"] = (
            depth_row.get("protocol_model") or None
        )
        pool["dex_depth_block_number"] = (
            int(depth_row["block_number"])
            if depth_row.get("block_number")
            else None
        )
        pool["dex_depth_block_timestamp"] = (
            depth_row.get("block_timestamp") or None
        )
        pool["dex_depth_usd_price_source_snapshot_id"] = (
            depth_row.get("usd_price_source_snapshot_id") or None
        )
        pool["dex_depth_usd_price_observed_at"] = (
            depth_row.get("usd_price_observed_at") or None
        )
        price_timing = usd_price_timing(
            pool["dex_depth_block_timestamp"],
            pool["dex_depth_usd_price_observed_at"],
        )
        pool["dex_depth_usd_price_skew_seconds"] = price_timing[
            "skew_seconds"
        ]
        pool["dex_depth_usd_price_freshness_status"] = price_timing[
            "status"
        ]
        pool["dex_depth_source_endpoint"] = (
            depth_row.get("source_endpoint") or None
        )
        pool["dex_depth_raw_response_sha256"] = (
            depth_row.get("raw_response_sha256") or None
        )
        pool["dex_depth_error"] = depth_row.get("error") or None
        pool["depth_requires_usd_price_alignment"] = str(
            depth_row.get("depth_requires_usd_price_alignment") or ""
        ).strip().lower() in {"1", "true", "yes"}
        measured = (
            source_status in {"observed", "partial"}
            and (
                not pool["depth_requires_usd_price_alignment"]
                or price_timing["usable"]
            )
        )
        pool["dex_depth_source_status"] = source_status
        if (
            source_status in {"observed", "partial"}
            and pool["depth_requires_usd_price_alignment"]
            and not price_timing["usable"]
        ):
            pool["dex_depth_status"] = "collection_failed"
            pool["dex_depth_reason_code"] = (
                "depth_usd_price_time_mismatch"
            )
            pool["dex_depth_error"] = price_timing["reason"]
        for field in numeric_fields:
            pool[field] = (
                parse_number(depth_row.get(field)) if measured else None
            )
        for field in completeness_fields:
            pool[field] = measured and depth_row.get(field) == "1"

    metadata = result["metadata"]
    metadata["dex_depth_note"] = (
        "DEX depth is measured from one fixed EVM block by integrating each "
        "supported pool's actual invariant and active tick liquidity to marginal "
        "price bands. Unsupported protocols remain null; TVL is never substituted."
    )
    metadata["dex_depth_snapshot"] = {
        "snapshot_ids": snapshot["snapshot_ids"],
        "observed_at": snapshot["observed_at"],
        "observed_at_min": snapshot["observed_at_min"],
        "observed_at_max": snapshot["observed_at_max"],
        "observation_span_seconds": snapshot[
            "observation_span_seconds"
        ],
        "pool_rows": len(snapshot["rows"]),
        "matched_market_rows": matched,
        "status_counts": snapshot["status_counts"],
        "bands_bps": [10, 25, 50, 100],
        "source": file_metadata(depth_path),
        "method": "fixed_block_pool_state_marginal_price_band",
    }
    metadata["sources"].append(file_metadata(depth_path))
    return result


def finalize_fact_contract(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach quality, aggregation, and count semantics after all snapshots."""
    cex_markets = [enrich_market_quality(row) for row in payload["cex_markets"]]
    dex_pools = [enrich_market_quality(row) for row in payload["dex_pools"]]
    metadata = {
        **catalog_contract(),
        **payload["metadata"],
    }
    metadata = attach_explicit_dex_counts(metadata, dex_pools)
    return {
        **payload,
        "metadata": metadata,
        "tokens": build_fact_token_summaries(cex_markets, dex_pools),
        "cex_markets": cex_markets,
        "dex_pools": dex_pools,
    }


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _build_market_payload_cached(
    start: str | None,
    end: str | None,
    cex_path_text: str,
    dex_path_text: str,
    _signature: SourceSignature,
) -> dict[str, Any]:
    cex_path = Path(cex_path_text)
    dex_path = Path(dex_path_text)
    cex_bounds = csv_date_bounds(cex_path)
    dex_bounds = csv_date_bounds(dex_path)
    available_start = min(
        cex_bounds["available_start"],
        dex_bounds["available_start"],
    )
    available_end = max(
        cex_bounds["available_end"],
        dex_bounds["available_end"],
    )
    effective_end = parse_iso_date(end) or available_end
    default_start = (date.fromisoformat(effective_end) - timedelta(days=29)).isoformat()
    effective_start = parse_iso_date(start) or max(available_start, default_start)
    if effective_start > effective_end:
        raise PublicClientRequestError("start date must not be after end date")
    if effective_start < available_start or effective_end > available_end:
        raise PublicClientRequestError(
            f"date window must be within {available_start} and {available_end}"
        )

    full_catalog_window = (
        effective_start == available_start
        and effective_end == available_end
    )
    cex_markets = summarize_cex(
        rows_in_window(cex_path, effective_start, effective_end),
        effective_start,
        effective_end,
        cex_bounds["available_start"],
        full_catalog_window,
    )
    dex_pools = summarize_dex(
        rows_in_window(dex_path, effective_start, effective_end),
        effective_start,
        effective_end,
        dex_bounds["available_start"],
        full_catalog_window,
    )
    if not cex_markets and not dex_pools:
        raise PublicClientRequestError(
            "No market observations exist in the selected time window"
        )

    return {
        "metadata": {
            "available_start": available_start,
            "available_end": available_end,
            "source_date_ranges": {
                "cex_daily": cex_bounds,
                "dex_daily": dex_bounds,
            },
            "start_date": effective_start,
            "end_date": effective_end,
            "token_count": len({row["token_symbol"] for row in cex_markets + dex_pools}),
            "grain": "venue/pool summary over selected daily observations",
            "sources": [file_metadata(cex_path), file_metadata(dex_path)],
            "storage": {"engine": "csv"},
            "tvl_note": "Pool TVL is a latest-fetch snapshot, not a historical daily series.",
        },
        "tokens": build_token_summaries(cex_markets, dex_pools),
        "cex_markets": cex_markets,
        "dex_pools": dex_pools,
    }


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _build_database_payload_cached(
    start: str | None,
    end: str | None,
    database_path_text: str,
    _signature: SourceSignature,
) -> dict[str, Any]:
    database_path = Path(database_path_text)
    connection = connect_database(database_path)
    try:
        state = connection.execute(
            """
            SELECT s.*, r.run_id, r.imported_at
            FROM dataset_state state
            JOIN dataset_snapshots s ON s.snapshot_id = state.snapshot_id
            JOIN import_runs r ON r.run_id = state.import_run_id
            WHERE state.singleton_id = 1
            """
        ).fetchone()
        if state is None:
            raise ValueError("Market database does not contain a published dataset state")

        available_start = state["available_start"]
        available_end = state["available_end"]
        effective_end = parse_iso_date(end) or available_end
        default_start = (date.fromisoformat(effective_end) - timedelta(days=29)).isoformat()
        effective_start = parse_iso_date(start) or max(available_start, default_start)
        if effective_start > effective_end:
            raise PublicClientRequestError("start date must not be after end date")
        if effective_start < available_start or effective_end > available_end:
            raise PublicClientRequestError(
                f"date window must be within {available_start} and {available_end}"
            )

        cex_bounds_row = connection.execute(
            "SELECT MIN(date) AS available_start, MAX(date) AS available_end "
            "FROM cex_market_daily"
        ).fetchone()
        dex_bounds_row = connection.execute(
            "SELECT MIN(date) AS available_start, MAX(date) AS available_end "
            "FROM dex_pool_daily"
        ).fetchone()
        cex_bounds = {
            "available_start": cex_bounds_row["available_start"],
            "available_end": cex_bounds_row["available_end"],
        }
        dex_bounds = {
            "available_start": dex_bounds_row["available_start"],
            "available_end": dex_bounds_row["available_end"],
        }
        cex_rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT date, token_symbol, exchange, cex_symbol, open, high, low,
                       close, base_volume, quote_volume_usd
                FROM cex_market_daily
                WHERE date BETWEEN ? AND ?
                ORDER BY date, token_symbol, exchange, cex_symbol
                """,
                (effective_start, effective_end),
            )
        ]
        dex_rows = [
            dict(row)
            for row in connection.execute(
                """
                SELECT date, token_symbol, chain, dex, pool_address, pool_name,
                       open, high, low, close, dex_volume_usd, pool_tvl_usd
                FROM dex_pool_daily
                WHERE date BETWEEN ? AND ?
                ORDER BY date, token_symbol, chain, dex, pool_address
                """,
                (effective_start, effective_end),
            )
        ]
    finally:
        connection.close()

    full_catalog_window = (
        effective_start == available_start
        and effective_end == available_end
    )
    cex_markets = summarize_cex(
        cex_rows,
        effective_start,
        effective_end,
        cex_bounds["available_start"],
        full_catalog_window,
    )
    dex_pools = summarize_dex(
        dex_rows,
        effective_start,
        effective_end,
        dex_bounds["available_start"],
        full_catalog_window,
    )
    if not cex_markets and not dex_pools:
        raise PublicClientRequestError(
            "No market observations exist in the selected time window"
        )

    return {
        "metadata": {
            "available_start": available_start,
            "available_end": available_end,
            "source_date_ranges": {
                "cex_daily": cex_bounds,
                "dex_daily": dex_bounds,
            },
            "start_date": effective_start,
            "end_date": effective_end,
            "token_count": len({row["token_symbol"] for row in cex_markets + dex_pools}),
            "grain": "venue/pool summary over selected daily observations",
            "sources": [
                {
                    "name": state["cex_source_name"],
                    "bytes": state["cex_source_bytes"],
                    "ingested_at": state["imported_at"],
                    "sha256": state["cex_sha256"][:16],
                },
                {
                    "name": state["dex_source_name"],
                    "bytes": state["dex_source_bytes"],
                    "ingested_at": state["imported_at"],
                    "sha256": state["dex_sha256"][:16],
                },
            ],
            "storage": {
                "engine": "sqlite",
                "schema_version": 1,
                "snapshot_id": state["snapshot_id"],
                "import_run_id": state["run_id"],
                "imported_at": state["imported_at"],
            },
            "tvl_note": "Pool TVL is a latest-fetch snapshot, not a historical daily series.",
        },
        "tokens": build_token_summaries(cex_markets, dex_pools),
        "cex_markets": cex_markets,
        "dex_pools": dex_pools,
    }


def execution_freshness_observed_at(path: Path | None) -> str | None:
    """Read the execution fact's own state time without borrowing depth time."""
    if path is None:
        return None
    try:
        snapshot = load_execution_cost_snapshot(path)
    except (OSError, ValueError, DepthExecutionCohortError):
        return None
    return snapshot.get("state_observed_at_min") if snapshot else None


def attach_freshness_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach dynamic freshness without mutating the cached fact payload."""
    result = {
        **payload,
        "metadata": {**payload["metadata"]},
    }
    metadata = result["metadata"]
    cex_execution_path = resolve_cex_execution_cost_path()
    dex_execution_path = resolve_dex_execution_cost_path()
    metadata["freshness"] = build_source_freshness(
        metadata.get("source_date_ranges", {}),
        tvl_observed_at=(
            metadata.get("tvl_snapshot", {}).get("observed_at")
            if metadata.get("tvl_snapshot")
            else None
        ),
        depth_observed_at=(
            metadata.get("cex_depth_snapshot", {}).get("observed_at")
            if metadata.get("cex_depth_snapshot")
            else None
        ),
        dex_depth_observed_at=(
            metadata.get("dex_depth_snapshot", {}).get("observed_at")
            if metadata.get("dex_depth_snapshot")
            else None
        ),
        cex_execution_observed_at=execution_freshness_observed_at(
            cex_execution_path
        ),
        dex_execution_observed_at=execution_freshness_observed_at(
            dex_execution_path
        ),
    )
    return result


def _optional_signature(path: Path | None) -> SourceSignature:
    return data_signature([path]) if path is not None else ()


def market_payload_cache_key(
    start: str | None,
    end: str | None,
) -> tuple[Any, ...]:
    """Resolve every fact source into a hashable, invalidation-aware cache key."""
    tvl_path = resolve_tvl_path()
    depth_path = resolve_cex_depth_path()
    depth_history_path = resolve_cex_depth_history_path(depth_path)
    dex_depth_path = resolve_dex_depth_path()
    database_path = resolve_database_path()
    if database_path is not None:
        cex_path = None
        dex_path = None
        daily_signature = data_signature([database_path])
    else:
        cex_path, dex_path = resolve_data_paths()
        daily_signature = data_signature([cex_path, dex_path])
    return (
        start,
        end,
        str(database_path) if database_path is not None else "",
        str(cex_path) if cex_path is not None else "",
        str(dex_path) if dex_path is not None else "",
        daily_signature,
        str(tvl_path) if tvl_path is not None else "",
        _optional_signature(tvl_path),
        str(depth_path) if depth_path is not None else "",
        _optional_signature(depth_path),
        str(depth_history_path) if depth_history_path is not None else "",
        _optional_signature(depth_history_path),
        str(dex_depth_path) if dex_depth_path is not None else "",
        _optional_signature(dex_depth_path),
    )


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _build_enriched_payload_cached(cache_key: tuple[Any, ...]) -> dict[str, Any]:
    (
        start,
        end,
        database_path_text,
        cex_path_text,
        dex_path_text,
        daily_signature,
        tvl_path_text,
        _tvl_signature,
        depth_path_text,
        _depth_signature,
        depth_history_path_text,
        _depth_history_signature,
        dex_depth_path_text,
        _dex_depth_signature,
    ) = cache_key
    if database_path_text:
        payload = _build_database_payload_cached(
            start,
            end,
            database_path_text,
            daily_signature,
        )
    else:
        payload = _build_market_payload_cached(
            start,
            end,
            cex_path_text,
            dex_path_text,
            daily_signature,
        )
    payload = overlay_tvl_snapshot(payload, Path(tvl_path_text) if tvl_path_text else None)
    payload = overlay_cex_depth_snapshot(
        payload,
        Path(depth_path_text) if depth_path_text else None,
        Path(depth_history_path_text) if depth_history_path_text else None,
    )
    payload = overlay_dex_depth_snapshot(
        payload,
        Path(dex_depth_path_text) if dex_depth_path_text else None,
    )
    return payload


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _build_lifecycle_payload_cached(
    cache_key: tuple[Any, ...],
    _source_signature: SourceSignature,
    freshness_bucket: int,
) -> dict[str, Any]:
    """Project wall-clock lifecycle evidence outside source-only caches.

    Lifecycle status changes when official catalog evidence crosses its TTL,
    even if no file signature changes.  Binding this projection to the same
    short freshness bucket used by public responses prevents a long-running
    process from claiming that stale evidence is still current.
    """
    reference_time = datetime.fromtimestamp(
        freshness_bucket * API_FRESHNESS_CACHE_SECONDS,
        tz=timezone.utc,
    )
    (
        _start,
        _end,
        database_path_text,
        cex_path_text,
        _dex_path_text,
        daily_signature,
        *_snapshot_keys,
    ) = cache_key
    historical_cex_inventory = _load_cex_historical_lineage_cached(
        database_path_text,
        cex_path_text,
        daily_signature,
    )
    payload = overlay_cex_instrument_lifecycle(
        _build_enriched_payload_cached(cache_key),
        historical_cex_inventory=historical_cex_inventory,
        now=reference_time,
    )
    payload = attach_configured_cex_identity_metadata(payload)
    return finalize_fact_contract(payload)


def summary_lifecycle_projection_state(freshness_bucket: int) -> str:
    """Return the wall-clock state that can change the compact Summary core.

    The Summary projection does not expose the continuously changing lifecycle
    evidence age.  Its expensive market core therefore only needs rebuilding
    when the validated official-catalog evidence crosses the same fresh/stale
    boundary used by ``overlay_cex_instrument_lifecycle``.
    """
    lifecycle_path = resolve_cex_instrument_lifecycle_path()
    manifest = _load_cex_instrument_lifecycle_cached(
        str(lifecycle_path),
        data_signature([lifecycle_path]),
    )
    checked_at = parse_rfc3339_utc(manifest["checked_at_utc"])
    reference_time = datetime.fromtimestamp(
        freshness_bucket * API_FRESHNESS_CACHE_SECONDS,
        tz=timezone.utc,
    )
    evidence_age_seconds = (reference_time - checked_at).total_seconds()
    if -300 <= evidence_age_seconds <= CEX_LIFECYCLE_MAX_AGE_SECONDS:
        return "fresh"
    return "stale"


def build_market_payload(
    start: str | None = None,
    end: str | None = None,
    *,
    _freshness_bucket: int | None = None,
) -> dict[str, Any]:
    # Source-bearing cache keys and the final signature check make a cold build
    # safe without holding the generation lock for its full duration.  An old
    # in-flight LRU insertion remains unreachable by a newer signature and is
    # bounded by the cache size.
    for _attempt in range(3):
        source_signature = api_source_signature()
        ensure_source_cache_generation(source_signature)
        cache_key = market_payload_cache_key(start, end)
        freshness_bucket = (
            api_freshness_bucket()
            if _freshness_bucket is None
            else _freshness_bucket
        )
        payload = attach_freshness_metadata(
            _build_lifecycle_payload_cached(
                cache_key,
                source_signature,
                freshness_bucket,
            )
        )
        if api_source_signature() == source_signature:
            return payload
    raise SourceGenerationChanged(
        "Published fact sources changed repeatedly during market assembly"
    )


@lru_cache(maxsize=8)
def _build_market_catalog_cached(
    cache_key: tuple[Any, ...],
    source_signature: SourceSignature,
    freshness_bucket: int,
) -> dict[str, Any]:
    return catalog_from_market_payload(
        _build_lifecycle_payload_cached(
            cache_key,
            source_signature,
            freshness_bucket,
        )
    )


def build_market_catalog(
    *,
    source_signature: SourceSignature | None = None,
) -> dict[str, Any]:
    """Return every observed market plus the versioned fact contract."""
    freshness_bucket = api_freshness_bucket()
    default_payload = build_market_payload(
        _freshness_bucket=freshness_bucket,
    )
    metadata = default_payload["metadata"]
    cache_key = market_payload_cache_key(
        metadata["available_start"],
        metadata["available_end"],
    )
    generation_signature = (
        source_signature
        if source_signature is not None
        else api_source_signature()
    )
    catalog = _build_market_catalog_cached(
        cache_key,
        generation_signature,
        freshness_bucket,
    )
    generation_metadata = {
        **catalog["metadata"],
        "cex_instrument_lifecycle": metadata.get(
            "cex_instrument_lifecycle"
        ),
    }
    return {
        **catalog,
        "metadata": {
            **catalog["metadata"],
            "freshness": metadata.get("freshness"),
            "data_generation": _public_data_generation(
                generation_metadata,
                generation_signature,
            ),
        },
    }


def _catalog_default_workspace_token(
    token_summaries: list[dict[str, Any]],
) -> str:
    """Choose a deterministic Token with the strongest comparable market coverage."""
    if not token_summaries:
        return ""
    for summary in token_summaries:
        measured = summary["measured_depth_market_counts"]
        quality = summary["quality_status_counts"]
        if (
            measured["cex"] > 0
            and measured["dex"] > 0
            and quality.get("critical", 0) == 0
        ):
            return summary["token_symbol"]
    for summary in token_summaries:
        measured = summary["measured_depth_market_counts"]
        if measured["cex"] > 0 and measured["dex"] > 0:
            return summary["token_symbol"]
    for summary in token_summaries:
        market_types = summary["market_type_counts"]
        if market_types["cex"] > 0 and market_types["dex"] > 0:
            return summary["token_symbol"]
    return token_summaries[0]["token_symbol"]


SCREENING_QUALITY_FLAG_CODES = frozenset(
    {
        "inactive_cex_instrument",
        "stale_cex_lifecycle_evidence",
        "depth_unavailable",
        "depth_unsupported",
        "depth_partial",
        "depth_failed",
        "zero_depth_10bps",
        "tiny_pool",
        "off_market_pool_state_price",
        "wide_quoted_spread",
        "low_daily_coverage",
    }
)
SCREENING_QUALITY_FLAG_MESSAGES = {
    **LIFECYCLE_QUALITY_FLAG_MESSAGES,
    "depth_unavailable": "No executable-depth observation is available.",
    "depth_unsupported": "Executable depth is unsupported for this market.",
    "depth_partial": (
        "Depth is a measured lower bound because one or more bands are incomplete."
    ),
    "depth_failed": "The most recent executable-depth collection failed.",
    "zero_depth_10bps": (
        "No executable notional was observed inside the ±10 bps band."
    ),
    "tiny_pool": "The point-in-time pool TVL is below the quality threshold.",
    "off_market_pool_state_price": (
        "Pool-state price deviates materially from the source target price."
    ),
    "wide_quoted_spread": "Quoted CEX spread exceeds the quality threshold.",
    "low_daily_coverage": (
        "Daily close coverage is below the declared primary-market threshold."
    ),
}
SCREENING_QUALITY_CATEGORIES = frozenset(
    {
        "data_health",
        "availability",
        "capability",
        "measurement_limit",
        "market_condition",
    }
)
SCREENING_QUALITY_SEVERITIES = frozenset({"info", "warning", "critical"})


def screening_quality_projection(market: dict[str, Any]) -> dict[str, Any]:
    """Return the bounded catalog-quality projection used by Screener contracts."""
    status = market.get("quality_status")
    if status not in {"ok", "info", "warning", "critical"}:
        status = "ok"
    flags = []
    for detail in market.get("quality_flag_details") or []:
        if not isinstance(detail, dict):
            continue
        code = detail.get("code")
        severity = detail.get("severity")
        if (
            code not in SCREENING_QUALITY_FLAG_CODES
            or severity not in SCREENING_QUALITY_SEVERITIES
        ):
            continue
        category = detail.get("category")
        flags.append(
            {
                "code": code,
                "severity": severity,
                "category": (
                    category
                    if category in SCREENING_QUALITY_CATEGORIES
                    else "data_health"
                ),
                "message": SCREENING_QUALITY_FLAG_MESSAGES[code],
                "observed_value": detail.get("observed_value"),
                "threshold": detail.get("threshold"),
            }
        )
    if not flags and status in SCREENING_QUALITY_SEVERITIES:
        flags.append(
            {
                "code": "catalog_quality_status",
                "severity": status,
                "category": "data_health",
                "message": (
                    "The catalog reports a non-OK quality status without a "
                    "structured reason."
                ),
                "observed_value": None,
                "threshold": None,
            }
        )
    return {
        "status": status,
        "flags": flags,
        "scope": "catalog",
        "evaluation_window": {
            "start": market.get("coverage_expected_start"),
            "end": market.get("coverage_expected_end"),
            "method": market.get("coverage_start_method"),
        },
    }


def catalog_summary_from_catalog(catalog: dict[str, Any]) -> dict[str, Any]:
    """Project the audit catalog into a small all-Token screener contract."""
    markets_by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for market in catalog.get("markets", []):
        token = market.get("token_symbol")
        market_type = market.get("market_type")
        quality_status = market.get("quality_status")
        if not token or market_type not in {"cex", "dex"}:
            raise ValueError("Catalog market is missing its Token or market type")
        if quality_status not in {"ok", "info", "warning", "critical"}:
            raise ValueError("Catalog market is missing a recognized quality status")
        markets_by_token[token].append(market)

    token_summaries: list[dict[str, Any]] = []
    for token in catalog.get("tokens", []):
        token_markets = markets_by_token.get(token, [])
        if not token_markets:
            raise ValueError(f"Catalog Token has no markets: {token}")
        market_type_counts = Counter(
            market.get("market_type") or "unknown" for market in token_markets
        )
        screening_projections = [
            screening_quality_projection(market) for market in token_markets
        ]
        quality_status_counts = Counter(
            projection["status"] for projection in screening_projections
        )
        quality_alert_counts: Counter[str] = Counter()
        for projection in screening_projections:
            quality_alert_counts.update(
                flag["severity"] for flag in projection["flags"]
            )
        measured_depth_market_counts = Counter(
            market.get("market_type") or "unknown"
            for market in token_markets
            if market.get("depth_status") in {"observed", "complete", "partial"}
        )
        token_summaries.append(
            {
                "token_symbol": token,
                "market_count": len(token_markets),
                "market_type_counts": {
                    "cex": market_type_counts.get("cex", 0),
                    "dex": market_type_counts.get("dex", 0),
                },
                "measured_depth_market_counts": {
                    "cex": measured_depth_market_counts.get("cex", 0),
                    "dex": measured_depth_market_counts.get("dex", 0),
                },
                "quality_status_counts": dict(sorted(quality_status_counts.items())),
                "quality_alert_counts": dict(sorted(quality_alert_counts.items())),
            }
        )

    metadata = catalog.get("metadata", {})
    default_workspace_token = _catalog_default_workspace_token(token_summaries)
    return {
        "metadata": {
            "summary_version": CATALOG_SUMMARY_VERSION,
            "catalog_version": metadata.get("catalog_version"),
            "catalog_scope": "all_tokens_summary",
            "token_count": len(token_summaries),
            "market_count": sum(
                summary["market_count"] for summary in token_summaries
            ),
            "default_workspace_token": default_workspace_token,
            "freshness": metadata.get("freshness"),
            "data_generation": metadata.get("data_generation"),
        },
        "tokens": [summary["token_symbol"] for summary in token_summaries],
        "token_summaries": token_summaries,
    }


def token_catalog_from_catalog(
    catalog: dict[str, Any],
    token_symbol: str | None,
) -> dict[str, Any]:
    """Return one exact Token's markets without pretending it is a global catalog."""
    token = (token_symbol or "").strip().upper()
    if not token:
        raise PublicClientRequestError(
            "Token is required for a single-Token market catalog"
        )
    catalog_tokens = set(catalog.get("tokens", []))
    if token not in catalog_tokens:
        raise PublicClientRequestError("Token is not cataloged")
    markets = []
    for market in catalog.get("markets", []):
        if market.get("token_symbol") != token:
            continue
        screening = screening_quality_projection(market)
        markets.append(
            {
                **market,
                "screening_quality_status": screening["status"],
                "screening_quality_flags": screening["flags"],
                "screening_quality_scope": screening["scope"],
                "screening_quality_window": screening["evaluation_window"],
            }
        )
    return {
        "metadata": {
            **catalog.get("metadata", {}),
            "catalog_scope": "single_token",
            "market_count": len(markets),
            "snapshot_metadata_population_scope": "all_catalog_markets",
        },
        "token_symbol": token,
        "markets": markets,
    }


def build_market_catalog_summary() -> dict[str, Any]:
    """Return only the global fields needed before a Token workspace is opened."""
    return catalog_summary_from_catalog(build_market_catalog())


SCREENER_MARKET_FIELDS = (
    "token_symbol",
    "venue",
    "instrument",
    "pool_address",
    "price_usd",
    "volume_usd",
    "window_return",
    "daily_volatility",
    "first_observed_date",
    "latest_observed_date",
    "observation_days",
    "coverage_ratio",
    "total_depth_10bps_usd",
    "total_depth_25bps_usd",
    "total_depth_50bps_usd",
    "total_depth_100bps_usd",
    "depth_100bps_complete",
    "depth_observed_at",
    "tvl_usd",
    "tvl_status",
    "tvl_observed_at",
    "quality_status",
    "quality_flags",
)

SCREENER_REQUIRED_MARKET_FIELDS = frozenset(
    {
        "token_symbol",
        "venue",
        "market_type",
        "refresh_market_id",
        "tvl_status",
        "tvl_na_reason",
        "tvl_retryable",
        "depth_status",
        "depth_na_reason",
        "depth_retryable",
    }
)

WINDOW_MARKET_FIELDS = (
    "price_usd",
    "volume_usd",
    "window_return",
    "daily_volatility",
    "first_observed_date",
    "latest_observed_date",
    "observation_days",
    "observation_count",
    "selected_window_observation_count",
    "historical_observation_count",
    "requested_window_days",
    "missing_calendar_days",
    "coverage_ratio",
    "coverage_expected_start",
    "coverage_expected_end",
    "coverage_start_method",
    "tvl_usd",
    "tvl_status",
)


def _payload_market_identity(
    row: dict[str, Any],
    market_type: str,
) -> str | None:
    if market_type == "cex":
        venue = row.get("venue")
        instrument = row.get("instrument")
        return f"{venue}|{instrument}" if venue and instrument else None
    return row.get("pool_address")


def _canonical_screener_market_identity(
    row: dict[str, Any],
    market_type: str,
) -> str | None:
    """Return the exact catalog ID accepted by quality and refresh APIs."""
    if market_type == "cex":
        venue = row.get("venue")
        instrument = row.get("instrument")
        return cex_market_id(venue, instrument) if venue and instrument else None
    venue = row.get("venue")
    pool_address = row.get("pool_address")
    token_symbol = row.get("token_symbol")
    if not venue or " / " not in venue or not pool_address or not token_symbol:
        return None
    chain, dex = venue.split(" / ", 1)
    return dex_market_id(chain, dex, pool_address, token_symbol)


def _compact_screener_market(
    row: dict[str, Any] | None,
    market_type: str,
) -> dict[str, Any] | None:
    if row is None:
        return None
    compact = {field: row.get(field) for field in SCREENER_MARKET_FIELDS}
    compact["market_type"] = market_type
    compact["market_id"] = _payload_market_identity(row, market_type)
    compact["refresh_market_id"] = _canonical_screener_market_identity(
        row,
        market_type,
    )
    compact["depth_status"] = (
        row.get("depth_status")
        if market_type == "cex"
        else row.get("dex_depth_status", row.get("depth_status"))
    )
    quality_market = {
        **row,
        "market_type": market_type,
        "depth_status": compact["depth_status"],
        "depth_error": (
            row.get("depth_error")
            if market_type == "cex"
            else row.get("dex_depth_error", row.get("depth_error"))
        ),
    }
    tvl_fact = _tvl_quality_fact(quality_market)
    depth_fact = _depth_quality_fact(quality_market)
    compact["tvl_status"] = tvl_fact["status"]
    compact["depth_status"] = depth_fact["status"]
    compact["tvl_retryable"] = tvl_fact["retryable"]
    compact["tvl_na_reason"] = tvl_fact.get("reason_code")
    compact["depth_retryable"] = depth_fact["retryable"]
    compact["depth_na_reason"] = depth_fact.get("reason_code")
    return {
        field: value
        for field, value in compact.items()
        if value is not None or field in SCREENER_REQUIRED_MARKET_FIELDS
    }


def _public_data_generation(
    metadata: dict[str, Any],
    source_signature: SourceSignature | None = None,
) -> str:
    """Return a path-free identifier for browser cache invalidation."""
    if source_signature is None:
        sources = [
            {
                "name": source.get("name"),
                "sha256": source.get("sha256"),
            }
            for source in metadata.get("sources", [])
        ]
    else:
        sources = []
        for entry in source_signature:
            path, modified_ns, size, *file_identity = entry
            source = {
                "name": Path(path).name,
                "path_identity": hashlib.sha256(
                    str(Path(path).resolve()).encode("utf-8")
                ).hexdigest()[:16],
                "modified_ns": modified_ns,
                "size": size,
            }
            if file_identity:
                source["changed_ns"] = file_identity[0]
            if len(file_identity) > 1:
                source["inode"] = file_identity[1]
            sources.append(source)
    lifecycle = metadata.get("cex_instrument_lifecycle")
    lifecycle_projection_state = (
        lifecycle.get("stale_evidence_market_count")
        if isinstance(lifecycle, dict)
        else None
    )
    material = {
        "response_contract": "screener-summary-and-token-catalog",
        "summary_version": CATALOG_SUMMARY_VERSION,
        "catalog_version": metadata.get("catalog_version"),
        "available_end": metadata.get("available_end"),
        # The official-catalog evidence can cross its TTL without any source
        # file changing.  Bind that dynamic fail-closed projection so browser
        # catalog caches cannot survive a fresh -> stale transition.
        "lifecycle_stale_evidence_market_count": lifecycle_projection_state,
        "sources": sources,
    }
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def market_summary_from_payload(
    payload: dict[str, Any],
    catalog: dict[str, Any],
    *,
    data_generation: str | None = None,
) -> dict[str, Any]:
    """Project the full fact payload into the exact rows used by the Screener."""
    catalog_summary = catalog_summary_from_catalog(catalog)
    catalog_tokens = {
        summary["token_symbol"]: summary
        for summary in catalog_summary["token_summaries"]
    }
    cex_by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)
    dex_by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in payload.get("cex_markets", []):
        cex_by_token[row["token_symbol"]].append(row)
    for row in payload.get("dex_pools", []):
        dex_by_token[row["token_symbol"]].append(row)

    summaries = []
    for token_summary in payload.get("tokens", []):
        token = token_summary["token_symbol"]
        primary_cex = next(
            (
                row
                for row in cex_by_token.get(token, [])
                if _payload_market_identity(row, "cex")
                == token_summary.get("primary_cex_id")
            ),
            None,
        )
        primary_dex = next(
            (
                row
                for row in dex_by_token.get(token, [])
                if _payload_market_identity(row, "dex")
                == token_summary.get("primary_dex_id")
            ),
            None,
        )
        if token_summary.get("primary_cex_id") and primary_cex is None:
            raise ValueError(
                f"Primary CEX market is missing from the Screener payload: {token}"
            )
        if token_summary.get("primary_dex_id") and primary_dex is None:
            raise ValueError(
                f"Primary DEX market is missing from the Screener payload: {token}"
            )
        catalog_token = catalog_tokens.get(token, {})
        if not catalog_token:
            raise ValueError(
                f"Screener Token is missing from the market catalog: {token}"
            )
        market_types = catalog_token.get("market_type_counts", {})
        quality_counts = catalog_token.get("quality_status_counts")
        quality_alert_counts = catalog_token.get("quality_alert_counts") or {}
        market_count = catalog_token.get("market_count")
        if (
            not isinstance(market_count, int)
            or market_count <= 0
            or market_types.get("cex", 0) + market_types.get("dex", 0)
            != market_count
            or not isinstance(quality_counts, dict)
            or sum(quality_counts.values()) != market_count
        ):
            raise ValueError(f"Catalog counts are inconsistent for Token: {token}")
        summaries.append(
            {
                "token_symbol": token,
                "aggregate_cex_volume_usd": token_summary.get(
                    "aggregate_cex_volume_usd"
                ),
                "aggregate_dex_volume_usd": token_summary.get(
                    "aggregate_dex_volume_usd"
                ),
                "aggregate_volume_usd": token_summary.get("aggregate_volume_usd"),
                "aggregate_dex_volume_share": token_summary.get(
                    "aggregate_dex_volume_share"
                ),
                "volume_aggregation_method": token_summary.get(
                    "volume_aggregation_method"
                ),
                "price_spread": token_summary.get("price_spread"),
                "price_spread_method": token_summary.get(
                    "price_spread_method"
                ),
                "absolute_price_gap": token_summary.get(
                    "absolute_price_gap"
                ),
                "absolute_price_gap_method": token_summary.get(
                    "absolute_price_gap_method"
                ),
                "spread_date": token_summary.get("spread_date"),
                "maximum_absolute_price_spread": token_summary.get(
                    "maximum_absolute_price_spread"
                ),
                "mean_absolute_price_spread": token_summary.get(
                    "mean_absolute_price_spread"
                ),
                "median_absolute_price_spread": token_summary.get(
                    "median_absolute_price_spread"
                ),
                "spread_comparable_days": token_summary.get(
                    "spread_comparable_days"
                ),
                "primary_cex_id": token_summary.get("primary_cex_id"),
                "primary_dex_id": token_summary.get("primary_dex_id"),
                "primary_cex": _compact_screener_market(primary_cex, "cex"),
                "primary_dex": _compact_screener_market(primary_dex, "dex"),
                "market_count": market_count,
                "cex_market_count": market_types["cex"],
                "dex_market_count": market_types["dex"],
                "quality_status_counts": quality_counts,
                "quality_alert_counts": quality_alert_counts,
            }
        )

    summary_tokens = [summary["token_symbol"] for summary in summaries]
    default_workspace_token = catalog_summary["metadata"]["default_workspace_token"]
    if default_workspace_token not in summary_tokens:
        default_workspace_token = summary_tokens[0] if summary_tokens else ""
    metadata = {
        **payload.get("metadata", {}),
        "summary_version": CATALOG_SUMMARY_VERSION,
        "response_scope": "screener_summary",
        "catalog_version": catalog.get("metadata", {}).get("catalog_version"),
        "catalog_market_count": catalog_summary["metadata"]["market_count"],
        "default_workspace_token": default_workspace_token,
        "fact_scopes": {
            "daily_metrics": "requested_start_end_utc_window",
            "catalog_counts": "full_available_daily_range_plus_latest_snapshots",
            "tvl_depth_execution": "latest_independent_point_in_time_snapshots",
        },
    }
    metadata["data_generation"] = (
        data_generation or _public_data_generation(metadata)
    )
    return {
        "metadata": metadata,
        "tokens": summaries,
    }


def build_market_summary(
    start: str | None = None,
    end: str | None = None,
    *,
    source_signature: SourceSignature | None = None,
) -> dict[str, Any]:
    """Return one compact, window-aware response for the all-Token Screener."""
    payload = build_market_payload(start, end)
    catalog = build_market_catalog(source_signature=source_signature)
    generation = catalog.get("metadata", {}).get("data_generation")
    if not isinstance(generation, str) or not generation:
        raise ValueError("Market catalog is missing its public data generation")
    return market_summary_from_payload(
        payload,
        catalog,
        data_generation=generation,
    )


def _payload_row_for_catalog_market(
    payload: dict[str, Any],
    market: dict[str, Any],
) -> dict[str, Any] | None:
    rows = (
        payload.get("cex_markets", [])
        if market.get("market_type") == "cex"
        else payload.get("dex_pools", [])
    )
    for row in rows:
        if row.get("token_symbol") != market.get("token_symbol"):
            continue
        if market.get("market_type") == "cex":
            if (
                row.get("venue") == market.get("venue")
                and row.get("instrument") == market.get("instrument")
            ):
                return row
        elif (
            row.get("venue") == market.get("venue")
            and row.get("pool_address") == market.get("pool_address")
        ):
            return row
    return None


def build_token_market_catalog(
    token_symbol: str | None,
    start: str | None = None,
    end: str | None = None,
    *,
    source_signature: SourceSignature | None = None,
    allow_empty_window: bool = False,
) -> dict[str, Any]:
    """Return complete catalog facts plus compact selected-window metrics for one Token."""
    token_catalog = token_catalog_from_catalog(
        build_market_catalog(source_signature=source_signature),
        token_symbol,
    )
    try:
        window_payload = build_market_payload(start, end)
    except ValueError as error:
        if not allow_empty_window or str(error) != (
            "No market observations exist in the selected time window"
        ):
            raise
        default_payload = build_market_payload()
        metadata = default_payload["metadata"]
        effective_start, effective_end = validate_fact_window(
            start,
            end,
            metadata["available_start"],
            metadata["available_end"],
        )
        window_payload = {
            "metadata": {
                **metadata,
                "start_date": effective_start,
                "end_date": effective_end,
            },
            "cex_markets": [],
            "dex_pools": [],
            "tokens": [],
        }
    markets = []
    for market in token_catalog["markets"]:
        row = _payload_row_for_catalog_market(window_payload, market)
        tvl_fact = _tvl_quality_fact(market)
        depth_fact = _depth_quality_fact(market)
        markets.append(
            {
                **market,
                "tvl_retryable": tvl_fact["retryable"],
                "tvl_na_reason": tvl_fact.get("reason_code"),
                "depth_retryable": depth_fact["retryable"],
                "depth_na_reason": depth_fact.get("reason_code"),
                "window_metrics": (
                    {field: row.get(field) for field in WINDOW_MARKET_FIELDS}
                    if row is not None
                    else None
                ),
            }
        )
    token = token_catalog["token_symbol"]
    token_summary = next(
        (
            summary
            for summary in window_payload.get("tokens", [])
            if summary.get("token_symbol") == token
        ),
        None,
    )
    return {
        **token_catalog,
        "metadata": {
            **token_catalog["metadata"],
            "window_start": window_payload["metadata"].get("start_date"),
            "window_end": window_payload["metadata"].get("end_date"),
            "window_metric_scope": "selected_start_end_utc_window",
            "snapshot_scope": "latest_independent_point_in_time_snapshots",
            "data_generation": token_catalog["metadata"]["data_generation"],
        },
        "token_summary": token_summary,
        "markets": markets,
    }


def validate_fact_window(
    start: str | None,
    end: str | None,
    available_start: str,
    available_end: str,
) -> tuple[str, str]:
    effective_end = parse_iso_date(end) or available_end
    default_start = (date.fromisoformat(effective_end) - timedelta(days=29)).isoformat()
    effective_start = parse_iso_date(start) or max(available_start, default_start)
    if effective_start > effective_end:
        raise PublicClientRequestError("start date must not be after end date")
    if effective_start < available_start or effective_end > available_end:
        raise PublicClientRequestError(
            f"date window must be within {available_start} and {available_end}"
        )
    return effective_start, effective_end


def database_market_rows(
    database_path: Path,
    market: dict[str, Any],
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    connection = connect_database(database_path)
    try:
        if market["market_type"] == "cex":
            rows = connection.execute(
                """
                SELECT date, close AS price_usd, quote_volume_usd AS volume_usd
                FROM cex_market_daily
                WHERE token_symbol = ? AND exchange = ? AND cex_symbol = ?
                  AND date BETWEEN ? AND ?
                ORDER BY date
                """,
                (
                    market["token_symbol"],
                    market["exchange"],
                    market["instrument"],
                    start,
                    end,
                ),
            )
        else:
            rows = connection.execute(
                """
                SELECT date, close AS price_usd, dex_volume_usd AS volume_usd
                FROM dex_pool_daily
                WHERE token_symbol = ? AND chain = ? AND pool_address = ?
                  AND date BETWEEN ? AND ?
                ORDER BY date
                """,
                (
                    market["token_symbol"],
                    market["chain"],
                    market["pool_address"],
                    start,
                    end,
                ),
            )
        return [
            {
                "date": row["date"],
                "price_usd": parse_number(row["price_usd"]),
                "volume_usd": parse_number(row["volume_usd"]),
            }
            for row in rows
        ]
    finally:
        connection.close()


def csv_market_rows(
    market: dict[str, Any],
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    cex_path, dex_path = resolve_data_paths()
    path = cex_path if market["market_type"] == "cex" else dex_path
    result = []
    for row in rows_in_window(path, start, end):
        if row.get("token_symbol") != market["token_symbol"]:
            continue
        if market["market_type"] == "cex":
            if row.get("exchange") != market["exchange"] or row.get("cex_symbol") != market["instrument"]:
                continue
            volume = row.get("quote_volume_usd")
        else:
            if row.get("chain") != market["chain"] or row.get("pool_address") != market["pool_address"]:
                continue
            volume = row.get("dex_volume_usd")
        result.append(
            {
                "date": row["date"],
                "price_usd": parse_number(row.get("close")),
                "volume_usd": parse_number(volume),
            }
        )
    return sorted(result, key=lambda row: row["date"])


def selected_market_rows(
    market: dict[str, Any],
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    if current_cex_facts_withheld(market):
        return []
    database_path = resolve_database_path()
    if database_path is not None:
        return database_market_rows(database_path, market, start, end)
    return csv_market_rows(market, start, end)


def build_market_comparison(
    token_symbol: str | None,
    market_a_id: str | None,
    market_b_id: str | None,
    start: str | None = None,
    end: str | None = None,
    *,
    source_signature: SourceSignature | None = None,
) -> dict[str, Any]:
    """Return aligned raw daily facts for two selected markets."""
    if not token_symbol or not market_a_id or not market_b_id:
        raise PublicClientRequestError(
            "token, market_a, and market_b are required"
        )
    token = token_symbol.upper()
    if market_a_id == market_b_id:
        raise PublicClientRequestError(
            "market_a and market_b must be different"
        )

    catalog = build_market_catalog(source_signature=source_signature)
    metadata = catalog["metadata"]
    effective_start, effective_end = validate_fact_window(
        start,
        end,
        metadata["available_start"],
        metadata["available_end"],
    )
    markets = {
        market["market_id"]: market
        for market in catalog["markets"]
        if market["token_symbol"] == token
    }
    market_a = markets.get(market_a_id)
    market_b = markets.get(market_b_id)
    if market_a is None or market_b is None:
        raise PublicClientRequestError(
            "Selected market is not cataloged for the requested token"
        )

    rows_a = selected_market_rows(market_a, effective_start, effective_end)
    rows_b = selected_market_rows(market_b, effective_start, effective_end)
    statistics_a = market_series_statistics(
        rows_a,
        price_field="price_usd",
        requested_start=effective_start,
        requested_end=effective_end,
    )
    statistics_b = market_series_statistics(
        rows_b,
        price_field="price_usd",
        requested_start=effective_start,
        requested_end=effective_end,
    )
    observations = compare_daily_rows(rows_a, rows_b)
    comparable = [row for row in observations if row["spread_bps"] is not None]
    return {
        "metadata": {
            **catalog_contract(),
            "available_start": metadata["available_start"],
            "available_end": metadata["available_end"],
            "source_date_ranges": metadata.get("source_date_ranges", {}),
            "freshness": metadata.get("freshness"),
            "start_date": effective_start,
            "end_date": effective_end,
            "sources": metadata["sources"],
            "storage": metadata["storage"],
            "data_generation": metadata["data_generation"],
            "comparison_generation": metadata["data_generation"],
            "comparison_days": len(comparable),
            "union_observation_days": len(observations),
        },
        "token_symbol": token,
        "market_a": market_a,
        "market_b": market_b,
        "market_a_statistics": statistics_a,
        "market_b_statistics": statistics_b,
        "latest_comparable_observation": comparable[-1] if comparable else None,
        "observations": observations,
    }


@lru_cache(maxsize=8)
def _load_execution_cost_snapshot_cached(
    path_text: str,
    _signature: SourceSignature,
) -> dict[str, Any]:
    path = Path(path_text)
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(set(EXECUTION_COST_COLUMNS) - set(reader.fieldnames or []))
        if missing:
            raise ValueError(
                f"{path.name} is missing execution-cost columns: "
                + ", ".join(missing)
            )
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no execution-cost rows")
    snapshot_id = _one_raw_cohort_id(
        rows,
        "snapshot_id",
        "Execution publication snapshot lineage",
    )
    source_snapshot_id = _one_raw_cohort_id(
        rows,
        "source_snapshot_id",
        "Execution publication source lineage",
    )
    observed_at_min, observed_at_max = _cohort_observed_at_bounds(rows)
    state_observed_at_min, state_observed_at_max = (
        _cohort_observed_at_bounds(rows, "state_observed_at")
    )
    market_ids = {row["market_id"] for row in rows if row.get("market_id")}
    validate_execution_snapshot(market_ids, rows)
    by_market: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_market[row["market_id"]].append(row)
    for market_rows in by_market.values():
        market_rows.sort(
            key=lambda row: (
                float(row["requested_notional_usd"]),
                EXECUTION_DIRECTIONS.index(row["direction"]),
            )
        )
    return {
        "path": path,
        "rows": rows,
        "by_market": by_market,
        "snapshot_ids": [snapshot_id],
        "source_snapshot_ids": [source_snapshot_id],
        "observed_at": observed_at_min,
        "observed_at_min": observed_at_min,
        "observed_at_max": observed_at_max,
        "observation_span_seconds": observation_span_seconds(
            observed_at_min,
            observed_at_max,
        ),
        "state_observed_at": state_observed_at_min,
        "state_observed_at_min": state_observed_at_min,
        "state_observed_at_max": state_observed_at_max,
        "market_count": len(by_market),
        "row_count": len(rows),
        "status_counts": dict(
            Counter(row.get("status") or "missing_status" for row in rows)
        ),
    }


def load_execution_cost_snapshot(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return _load_execution_cost_snapshot_cached(
        str(path),
        data_signature([path]),
    )


def _timestamp_seconds(value: str | None) -> float | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def _execution_temporal_alignment(
    rows: list[dict[str, str]],
) -> dict[str, Any]:
    """Summarize whether measured execution can use its USD conversion input."""
    measured = [
        row
        for row in rows
        if row.get("status") in {"observed", "partial"}
    ]

    def one_value(field: str) -> str | None:
        values = {
            str(row.get(field))
            for row in rows
            if row.get(field) not in (None, "")
        }
        return next(iter(values)) if len(values) == 1 else None

    base = {
        "state_observed_at": one_value("state_observed_at"),
        "usd_price_observed_at": one_value("usd_price_observed_at"),
        "usd_price_source_snapshot_id": one_value(
            "usd_price_source_snapshot_id"
        ),
        "usd_conversion_status": one_value("usd_conversion_status"),
        "source_quote_asset": one_value("source_quote_asset"),
        "usd_price_state_skew_seconds": None,
        "warning_usd_price_state_skew_seconds": (
            USD_PRICE_SKEW_WARNING_SECONDS
        ),
        "max_usd_price_state_skew_seconds": USD_PRICE_SKEW_MAX_SECONDS,
        "status": "not_evaluated",
        "usable": True,
        "reason": None,
    }
    if not measured:
        return base

    conversion_status = base["usd_conversion_status"]
    if (
        conversion_status in {"identity_usd", "proxy_usdt_equals_usd"}
        or (
            one_value("market_type") == "cex"
            and base["source_quote_asset"] in {"USD", "USDT"}
        )
    ):
        base["status"] = "not_applicable"
        base["reason"] = "USD conversion has no independent price response"
        return base

    timing = usd_price_timing(
        base["state_observed_at"],
        base["usd_price_observed_at"],
    )
    base.update(
        {
            "usd_price_state_skew_seconds": timing["skew_seconds"],
            "status": timing["status"],
            "usable": timing["usable"],
            "reason": timing["reason"],
        }
    )
    return base


def _execution_public_rows(
    rows: list[dict[str, str]],
    timing: dict[str, Any],
) -> list[dict[str, Any]]:
    """Fail closed in the public API while retaining the original lineage."""
    public_rows = execution_api_rows(rows, number_parser=parse_number)
    measured = any(
        row.get("status") in {"observed", "partial"}
        for row in rows
    )
    if not measured or timing["usable"]:
        return public_rows
    for source_row, public_row in zip(rows, public_rows):
        if source_row.get("status") not in {"observed", "partial"}:
            continue
        public_row["source_status"] = public_row.get("status")
        public_row["source_status_reason"] = public_row.get("status_reason")
        public_row["status"] = "failed"
        public_row["status_reason"] = "execution_usd_price_time_mismatch"
        public_row["error"] = None
        for field in RESULT_NUMERIC_COLUMNS:
            public_row[field] = None
    return public_rows


def _execution_snapshot_metadata(
    snapshot: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    return {
        "snapshot_ids": snapshot["snapshot_ids"],
        "source_snapshot_ids": snapshot["source_snapshot_ids"],
        "observed_at": snapshot["observed_at"],
        "observed_at_min": snapshot["observed_at_min"],
        "observed_at_max": snapshot["observed_at_max"],
        "observation_span_seconds": snapshot[
            "observation_span_seconds"
        ],
        "state_observed_at": snapshot["state_observed_at"],
        "state_observed_at_min": snapshot["state_observed_at_min"],
        "state_observed_at_max": snapshot["state_observed_at_max"],
        "market_count": snapshot["market_count"],
        "row_count": snapshot["row_count"],
        "status_counts": snapshot["status_counts"],
        "source": file_metadata(snapshot["path"]),
    }


def _one_cohort_id(value: Any, label: str) -> str:
    if (
        not isinstance(value, list)
        or len(value) != 1
        or not isinstance(value[0], str)
        or not value[0]
        or value[0] != value[0].strip()
    ):
        raise DepthExecutionCohortError(
            f"{label} must contain exactly one non-empty snapshot ID"
        )
    return value[0]


def _validate_cohort_observation_metadata(
    value: dict[str, Any],
    label: str,
) -> None:
    """Require canonical, internally consistent cohort observation bounds."""
    required = {
        "observed_at",
        "observed_at_min",
        "observed_at_max",
        "observation_span_seconds",
    }
    if not required.issubset(value):
        raise DepthExecutionCohortError(
            f"{label} observation metadata is incomplete"
        )
    observed_at = value.get("observed_at")
    observed_at_min = value.get("observed_at_min")
    observed_at_max = value.get("observed_at_max")
    span = value.get("observation_span_seconds")
    if observed_at is None and observed_at_min is None and observed_at_max is None:
        if span is not None:
            raise DepthExecutionCohortError(
                f"{label} observation span has no timestamp bounds"
            )
        return
    if (
        observed_at is None
        or observed_at_min is None
        or observed_at_max is None
    ):
        raise DepthExecutionCohortError(
            f"{label} observation bounds are incomplete"
        )
    lower = _parse_cohort_timestamp(
        observed_at_min,
        f"{label} observed_at_min",
    )
    upper = _parse_cohort_timestamp(
        observed_at_max,
        f"{label} observed_at_max",
    )
    first = _parse_cohort_timestamp(
        observed_at,
        f"{label} observed_at",
    )
    if lower is None or upper is None or first is None:  # pragma: no cover
        raise DepthExecutionCohortError(
            f"{label} observation bounds are incomplete"
        )
    if (
        lower.isoformat() != observed_at_min
        or upper.isoformat() != observed_at_max
        or first.isoformat() != observed_at
        or first != lower
    ):
        raise DepthExecutionCohortError(
            f"{label} observation bounds are not canonical"
        )
    expected_span = observation_span_seconds(
        observed_at_min,
        observed_at_max,
    )
    if (
        type(span) not in {int, float}
        or not math.isfinite(span)
        or span < 0
        or span != expected_span
    ):
        raise DepthExecutionCohortError(
            f"{label} observation span differs from its bounds"
        )


def validate_depth_execution_cohort(
    metadata: dict[str, Any],
    snapshot: dict[str, Any],
    market_type: str,
) -> dict[str, Any]:
    """Validate one exact depth/execution cohort and return a bounded lineage."""
    normalized_type = str(market_type or "").strip().lower()
    if normalized_type not in {"cex", "dex"}:
        raise DepthExecutionCohortError("Market type is not CEX or DEX")
    depth = metadata.get(f"{normalized_type}_depth_snapshot")
    if not isinstance(depth, dict):
        raise DepthExecutionCohortError(
            f"{normalized_type.upper()} depth cohort metadata is unavailable"
        )
    _validate_cohort_observation_metadata(
        depth,
        f"{normalized_type.upper()} depth cohort",
    )
    _validate_cohort_observation_metadata(
        snapshot,
        f"{normalized_type.upper()} execution cohort",
    )
    depth_snapshot_id = _one_cohort_id(
        depth.get("snapshot_ids"),
        f"{normalized_type.upper()} depth snapshot_ids",
    )
    execution_snapshot_id = _one_cohort_id(
        snapshot.get("snapshot_ids"),
        f"{normalized_type.upper()} execution snapshot_ids",
    )
    execution_source_snapshot_id = _one_cohort_id(
        snapshot.get("source_snapshot_ids"),
        f"{normalized_type.upper()} execution source_snapshot_ids",
    )
    if len(
        {
            depth_snapshot_id,
            execution_snapshot_id,
            execution_source_snapshot_id,
        }
    ) != 1:
        raise DepthExecutionCohortError(
            f"{normalized_type.upper()} depth/execution snapshot IDs differ"
        )

    depth_count_field = (
        "market_rows" if normalized_type == "cex" else "pool_rows"
    )
    depth_market_count = depth.get(depth_count_field)
    execution_market_count = snapshot.get("market_count")
    if (
        type(depth_market_count) is not int
        or depth_market_count < 1
        or type(execution_market_count) is not int
        or execution_market_count < 1
        or execution_market_count != depth_market_count
    ):
        raise DepthExecutionCohortError(
            f"{normalized_type.upper()} depth/execution market counts differ"
        )
    if (
        depth.get("observed_at_min") is None
        or snapshot.get("observed_at_min") is None
    ):
        raise DepthExecutionCohortError(
            f"{normalized_type.upper()} positive cohort inventory lacks "
            "observation bounds"
        )
    return {
        "market_type": normalized_type,
        "depth_snapshot_id": depth_snapshot_id,
        "execution_snapshot_id": execution_snapshot_id,
        "execution_source_snapshot_id": execution_source_snapshot_id,
        "depth_market_count": depth_market_count,
        "execution_market_count": execution_market_count,
    }


def build_execution_cost_comparison(
    token_symbol: str | None,
    market_a_id: str | None,
    market_b_id: str | None,
    *,
    source_signature: SourceSignature | None = None,
) -> dict[str, Any]:
    """Return source-backed fixed-notional facts for two exact catalog markets."""
    if not token_symbol or not market_a_id or not market_b_id:
        raise PublicClientRequestError(
            "token, market_a, and market_b are required"
        )
    if market_a_id == market_b_id:
        raise PublicClientRequestError(
            "market_a and market_b must be different"
        )
    token = token_symbol.upper()
    catalog = build_market_catalog(source_signature=source_signature)
    markets = {
        market["market_id"]: market
        for market in catalog["markets"]
        if market["token_symbol"] == token
    }
    market_a = markets.get(market_a_id)
    market_b = markets.get(market_b_id)
    if market_a is None or market_b is None:
        raise PublicClientRequestError(
            "Selected market is not cataloged for the requested token"
        )

    selected_market_types = {
        market_a["market_type"],
        market_b["market_type"],
    }
    snapshot_paths = {
        "cex": resolve_cex_execution_cost_path,
        "dex": resolve_dex_execution_cost_path,
    }
    snapshots = {
        market_type: load_execution_cost_snapshot(
            snapshot_paths[market_type]()
        )
        for market_type in selected_market_types
    }
    cohort_lineage = {
        market_type: validate_depth_execution_cohort(
            catalog["metadata"],
            snapshot,
            market_type,
        )
        for market_type, snapshot in snapshots.items()
        if snapshot is not None
    }

    def market_result(market: dict[str, Any]) -> dict[str, Any]:
        if current_cex_facts_withheld(market):
            stale = (
                market.get("current_listing_status")
                == "official_catalog_evidence_stale"
            )
            return {
                "market": market,
                "status": "needs_review" if stale else "source_no_observation",
                "reason_code": (
                    "official_catalog_evidence_stale"
                    if stale
                    else "instrument_absent_from_current_catalog"
                ),
                "retryable": False,
                "rows": [],
                "timing": None,
                "publication_status": "withheld",
            }
        snapshot = snapshots.get(market["market_type"])
        if snapshot is None:
            return {
                "market": market,
                "status": "unavailable",
                "reason_code": "execution_snapshot_unavailable",
                "retryable": False,
                "rows": [],
                "timing": None,
                "publication_status": "unavailable",
            }
        rows = snapshot["by_market"].get(market["market_id"])
        if rows is None:
            return {
                "market": market,
                "status": "not_cataloged_in_snapshot",
                "reason_code": "execution_market_not_cataloged_in_snapshot",
                "retryable": False,
                "rows": [],
                "timing": None,
                "publication_status": "unavailable",
            }
        timing = _execution_temporal_alignment(rows)
        measured = any(
            row.get("status") in {"observed", "partial"}
            for row in rows
        )
        return {
            "market": market,
            "status": "available",
            "reason_code": None,
            "retryable": False,
            "rows": _execution_public_rows(rows, timing),
            "timing": timing,
            "publication_status": (
                "withheld"
                if measured and not timing["usable"]
                else "published"
            ),
        }

    result_a = market_result(market_a)
    result_b = market_result(market_b)
    times = []
    for result in (result_a, result_b):
        if not result["rows"]:
            times.append(None)
            continue
        values = {
            row.get("state_observed_at")
            for row in result["rows"]
            if row.get("state_observed_at")
        }
        times.append(_timestamp_seconds(max(values)) if values else None)
    skew = (
        abs(times[0] - times[1])
        if times[0] is not None and times[1] is not None
        else None
    )
    return {
        "metadata": {
            "contract_version": EXECUTION_COST_CONTRACT_VERSION,
            "data_generation": catalog["metadata"]["data_generation"],
            "execution_generation": catalog["metadata"]["data_generation"],
            "notionals_usd": [int(value) for value in EXECUTION_NOTIONALS_USD],
            "directions": list(EXECUTION_DIRECTIONS),
            "notional_definition": NOTIONAL_DEFINITION,
            "reference_prices": {
                "cex": "same-snapshot best bid/ask midpoint",
                "dex": "same-block pre-trade pre-fee marginal pool price",
            },
            "formula": {
                "sell_token": (
                    "(reference_notional_usd - quote_amount_usd) / "
                    "reference_notional_usd * 10000"
                ),
                "buy_token": (
                    "(quote_amount_usd - reference_notional_usd) / "
                    "reference_notional_usd * 10000"
                ),
            },
            "numeric_encoding": {
                "requested_notional_usd": "JSON number",
                "measured_decimal_fields": (
                    "exact base-10 JSON strings; null when unavailable"
                ),
            },
            "cost_scope": (
                "Source-mechanics quoted cost, not realized or all-in cost. "
                "CEX account taker fees are excluded. Supported DEX V2 quotes "
                "include pool swap fees while gas, router fees, transfer "
                "taxes, and MEV are excluded. DEX V3 execution is explicitly "
                "unsupported in this release."
            ),
            "missing_value_rule": (
                "Partial, unsupported, failed, unavailable, and not-cataloged "
                "full-request cost fields remain null; they are never zero-filled "
                "or interpolated from depth bands."
            ),
            "temporal_contract_version": 1,
            "warning_usd_price_state_skew_seconds": (
                USD_PRICE_SKEW_WARNING_SECONDS
            ),
            "max_usd_price_state_skew_seconds": USD_PRICE_SKEW_MAX_SECONDS,
            "temporal_rule": (
                "A measured execution that needs an observed USD conversion is "
                "published only when the conversion response is no more than "
                "two hours from market state. Missing or older inputs are "
                "withheld as N/A, never zero. USD and USDT identity/proxy "
                "conversions have no independent price timestamp."
            ),
            "snapshot_skew_seconds": skew,
            "cohort_observation_model": "bounded_sequential_observations",
            "cohort_lineage": cohort_lineage,
            "snapshots": {
                market_type: _execution_snapshot_metadata(snapshot)
                for market_type, snapshot in snapshots.items()
            },
        },
        "token_symbol": token,
        "market_a": result_a,
        "market_b": result_b,
    }


QUALITY_CONTRACT_VERSION = 4
QUALITY_STATUS_SEMANTICS = {
    "observed": "A source-backed fact is present.",
    "provisional": "The current UTC day is not finalized.",
    "partial": "Only part of the requested execution or depth is proved.",
    "unsupported": (
        "The current source or project-validated method does not support this "
        "fact, market model, or requested history range."
    ),
    "source_no_observation": (
        "The source responded successfully but supplied no observation."
    ),
    "collection_failed": "A supported source request failed.",
    "needs_review": (
        "The source outcome needs protected operator review and is not an "
        "automatic retry."
    ),
    "backfill_pending": (
        "One or more expected historical observations are missing and can be retried."
    ),
    "invalid": "A source value was received but failed the fact contract.",
    "stale": "The latest source-backed observation is outside its freshness limit.",
    "missing_unexplained": (
        "No observation is present and the pipeline has not yet proved why."
    ),
    "failed": "A supported collection or calculation failed.",
    "unavailable": "No current snapshot is configured or published.",
    "not_cataloged_in_snapshot": (
        "A current snapshot exists, but it contains no row for this market."
    ),
    "not_applicable": "This fact is not defined for this market type.",
}
DAILY_QUALITY_STATUS_PRIORITY = {
    "collection_failed": 0,
    "needs_review": 1,
    "backfill_pending": 2,
    "source_no_observation": 3,
    "unsupported": 4,
}
DAILY_QUALITY_CATEGORIES = {
    "historical_gap",
    "d1_active_gap",
    "stale_market_unknown",
    "source_no_observation",
}
DAILY_QUALITY_PUBLICATION_STATUSES = {
    "published",
    "published_with_backfill",
    "published_with_retry_queue",
}


def _parse_quality_timestamp(value: Any) -> str:
    text = str(value or "")
    if len(text) > 64:
        raise ValueError("Daily quality timestamp is invalid")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("Daily quality timestamp must include a timezone")
    return text


def _parse_quality_day(value: Any) -> str:
    text = str(value or "")
    if len(text) != 10 or date.fromisoformat(text).isoformat() != text:
        raise ValueError("Daily quality date is invalid")
    return text


def _daily_quality_path_is_safe(path: Path) -> bool:
    if path.is_symlink():
        return False
    try:
        expected_path = resolve_daily_quality_report_path()
        if expected_path is None or expected_path != path:
            return False
        data_root = path.parent.parent.resolve()
        path.resolve().relative_to(data_root)
    except (OSError, RuntimeError, ValueError):
        return False
    return True


@lru_cache(maxsize=8)
def _load_daily_quality_report_cached(
    path_text: str,
    _signature: SourceSignature,
) -> dict[str, Any]:
    """Read and normalize only the bounded fields used by the public API."""

    path = Path(path_text)
    if not _daily_quality_path_is_safe(path):
        raise ValueError("Daily quality report path is invalid")
    stat = path.stat()
    if not path.is_file() or stat.st_size > MAX_DAILY_QUALITY_REPORT_BYTES:
        raise ValueError("Daily quality report is not a bounded regular file")
    with path.open("rb") as handle:
        raw = handle.read(MAX_DAILY_QUALITY_REPORT_BYTES + 1)
    if not _daily_quality_path_is_safe(path):
        raise ValueError("Daily quality report path changed during read")
    if len(raw) > MAX_DAILY_QUALITY_REPORT_BYTES:
        raise ValueError("Daily quality report exceeds the size limit")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Daily quality report is not valid JSON") from error
    if not isinstance(payload, dict):
        raise ValueError("Daily quality report root must be an object")
    if payload.get("schema") != DAILY_QUALITY_REPORT_SCHEMA:
        raise ValueError("Daily quality report schema is unsupported")

    publication = payload.get("publication")
    if not isinstance(publication, dict):
        raise ValueError("Daily quality report publication is missing")
    publication_status = str(publication.get("status") or "")
    if publication_status not in DAILY_QUALITY_PUBLICATION_STATUSES:
        raise ValueError("Daily quality report is not a published snapshot")
    import_run_id = str(publication.get("import_run_id") or "")
    if not re.fullmatch(r"[0-9a-f]{32}", import_run_id):
        raise ValueError("Daily quality report import identity is invalid")

    generated_at = _parse_quality_timestamp(payload.get("generated_at_utc"))
    audit_day = _parse_quality_day(payload.get("audit_date"))
    latest_completed_day = _parse_quality_day(
        payload.get("latest_completed_utc_day")
    )
    issues = payload.get("issues")
    if (
        not isinstance(issues, list)
        or len(issues) > MAX_DAILY_QUALITY_REPORT_ISSUES
    ):
        raise ValueError("Daily quality report issues are invalid")
    summary = payload.get("summary")
    if (
        not isinstance(summary, dict)
        or summary.get("issue_count") != len(issues)
    ):
        raise ValueError("Daily quality report issue count is inconsistent")

    normalized_issues = []
    seen = set()
    seen_status_by_market_day_reason = {}
    for issue in issues:
        if not isinstance(issue, dict):
            raise ValueError("Daily quality issue is not an object")
        category = str(issue.get("category") or "")
        reason_code = str(issue.get("reason_code") or "")
        status = str(issue.get("status") or "")
        retryable = issue.get("retryable")
        market = issue.get("market")
        day_value = issue.get("date")
        if (
            len(category) > 64
            or len(reason_code) > 64
            or len(status) > 64
            or not isinstance(retryable, bool)
            or not isinstance(market, dict)
        ):
            raise ValueError("Daily quality issue fields are invalid")
        market_id = market.get("market_id")
        if market_id is not None and (
            not isinstance(market_id, str)
            or not market_id
            or len(market_id) > 512
        ):
            raise ValueError("Daily quality market identity is invalid")
        if day_value is not None:
            day_value = _parse_quality_day(day_value)

        if (
            category not in DAILY_QUALITY_CATEGORIES
            or market_id is None
            or day_value is None
        ):
            # Hard-invalid details and other non-daily evidence remain in the
            # protected operator report, never in the public projection.
            continue
        raw_key = (market_id, day_value, reason_code)
        previous_status = seen_status_by_market_day_reason.get(raw_key)
        if previous_status is not None and previous_status != status:
            raise ValueError(
                "Daily quality issue status conflicts for one market/date/reason"
            )
        seen_status_by_market_day_reason[raw_key] = status
        rule = quality_outcome_rule(status, reason_code)
        if rule is None or retryable is not rule.retryable:
            status = "needs_review"
            reason_code = "daily_quality_outcome_invalid"
            rule = quality_outcome_rule(status, reason_code)
            assert rule is not None
        key = (market_id, day_value, reason_code)
        if key in seen:
            continue
        seen.add(key)
        normalized_issues.append(
            {
                "market_id": market_id,
                "date": day_value,
                "category": category,
                "status": status,
                "reason_code": reason_code,
                "retryable": rule.retryable,
            }
        )

    return {
        "schema": DAILY_QUALITY_REPORT_SCHEMA,
        "generated_at_utc": generated_at,
        "audit_date": audit_day,
        "latest_completed_utc_day": latest_completed_day,
        "publication_status": publication_status,
        "import_run_id": import_run_id,
        "issues": normalized_issues,
    }


def _daily_quality_report_state(
    metadata: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return safe public state plus normalized issues, or inference fallback."""

    try:
        path = resolve_daily_quality_report_path()
    except (OSError, RuntimeError, FileNotFoundError, ValueError):
        path = None
    if path is None or not path.exists():
        return (
            {
                "status": "unavailable",
                "reason_code": "daily_quality_report_unavailable",
                "evidence_mode": "catalog_window_inference",
                "identity_status": "not_verified",
            },
            [],
        )
    try:
        report = _load_daily_quality_report_cached(
            str(path),
            _safe_path_signature(path),
        )
    except (OSError, RuntimeError, ValueError):
        return (
            {
                "status": "ignored_invalid",
                "reason_code": "daily_quality_report_invalid",
                "evidence_mode": "catalog_window_inference",
                "identity_status": "not_verified",
            },
            [],
        )

    current_import_run_id = (
        metadata.get("storage", {}).get("import_run_id")
        if isinstance(metadata.get("storage"), dict)
        else None
    )
    if not isinstance(current_import_run_id, str) or not current_import_run_id:
        return (
            {
                "status": "ignored_identity_unavailable",
                "reason_code": "published_import_identity_unavailable",
                "evidence_mode": "catalog_window_inference",
                "identity_status": "unavailable",
            },
            [],
        )
    if report["import_run_id"] != current_import_run_id:
        return (
            {
                "status": "ignored_identity_mismatch",
                "reason_code": "publication_import_identity_mismatch",
                "evidence_mode": "catalog_window_inference",
                "identity_status": "mismatch",
            },
            [],
        )
    return (
        {
            "status": "matched",
            "reason_code": None,
            "evidence_mode": "published_daily_audit",
            "identity_status": "matched_current_import",
            "schema": report["schema"],
            "generated_at_utc": report["generated_at_utc"],
            "audit_date": report["audit_date"],
            "latest_completed_utc_day": report[
                "latest_completed_utc_day"
            ],
            "publication_status": report["publication_status"],
        },
        report["issues"],
    )


def _daily_quality_issues_for_window(
    issues: Iterable[dict[str, Any]],
    *,
    market_ids: set[str],
    window_start: str,
    window_end: str,
) -> list[dict[str, Any]]:
    return sorted(
        (
            issue
            for issue in issues
            if issue["market_id"] in market_ids
            and window_start <= issue["date"] <= window_end
        ),
        key=lambda issue: (
            issue["market_id"],
            issue["date"],
            issue["reason_code"],
        ),
    )


def _daily_issue_outcome_counts(
    issues: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    counts = Counter(
        (issue["status"], issue["reason_code"])
        for issue in issues
    )
    return [
        {
            "status": status,
            "reason_code": reason_code,
            "count": count,
        }
        for (status, reason_code), count in sorted(counts.items())
    ]


def _overlay_daily_quality_report(
    fact: dict[str, Any],
    issues: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    issues = list(issues)
    if not issues:
        return {
            **fact,
            "reason_code_counts": {},
            "issue_status_counts": {},
            "issue_outcome_counts": [],
            "affected_dates": [],
            "affected_date_count": 0,
            "daily_evidence_mode": "published_daily_audit",
        }
    status_counts = Counter(issue["status"] for issue in issues)
    reason_counts = Counter(issue["reason_code"] for issue in issues)
    affected_dates = sorted({issue["date"] for issue in issues})
    status = aggregate_daily_quality_status(status_counts)
    retryable = any(issue["retryable"] for issue in issues)
    has_manual_review = bool(status_counts.get("needs_review"))
    if retryable and has_manual_review:
        action = "operator_review_retry_and_manual_queues"
    elif retryable:
        action = "operator_review_retry_queue"
    elif status == "needs_review":
        action = "operator_manual_review"
    else:
        action = "operator_review_source_outcome"
    reason_code = (
        next(iter(reason_counts))
        if len(reason_counts) == 1
        else "multiple_daily_quality_reasons"
    )
    fixed_reasons = {
        "collection_failed": (
            "A published collection attempt failed for one or more affected "
            "dates. An operator must review the protected retry queue."
        ),
        "source_no_observation": (
            "The source responded but supplied no candle for one or more "
            "affected dates. This is not an automatic retry."
        ),
        "unsupported": (
            "The declared source cannot serve one or more affected historical "
            "dates under its current public range. Values remain N/A."
        ),
        "needs_review": (
            "The source reported a listing or lifecycle condition that "
            "requires protected operator review."
        ),
        "backfill_pending": (
            "One or more expected dates are absent without a matching "
            "collection-attempt explanation."
        ),
    }
    flags = list(fact.get("quality_flags") or [])
    known_flag_codes = {flag.get("code") for flag in flags}
    for issue_status in sorted(
        status_counts,
        key=lambda candidate: DAILY_QUALITY_STATUS_PRIORITY[candidate],
    ):
        report_flag = {
            "code": "daily_{}".format(issue_status),
            "severity": (
                "critical"
                if issue_status == "collection_failed"
                else "warning"
                if issue_status in {"needs_review", "backfill_pending"}
                else "info"
            ),
            "category": "data_health",
            "message": fixed_reasons[issue_status],
            "observed_value": len(
                {
                    issue["date"]
                    for issue in issues
                    if issue["status"] == issue_status
                }
            ),
            "threshold": 0,
        }
        if report_flag["code"] not in known_flag_codes:
            flags.append(report_flag)
            known_flag_codes.add(report_flag["code"])
    return {
        **fact,
        "status": status,
        "reason": fixed_reasons[status],
        "reason_code": reason_code,
        "retryable": retryable,
        "action": action,
        "reason_code_counts": dict(sorted(reason_counts.items())),
        "issue_status_counts": dict(sorted(status_counts.items())),
        "issue_outcome_counts": _daily_issue_outcome_counts(issues),
        "affected_dates": affected_dates,
        "affected_date_count": len(affected_dates),
        "daily_evidence_mode": "published_daily_audit",
        "quality_flags": flags,
    }


def _reconcile_daily_fact_without_report_issue(
    fact: dict[str, Any],
) -> dict[str, Any]:
    """Do not advertise a retry outside the published report's exact queue."""

    if fact.get("status") not in {
        "backfill_pending",
        "missing_unexplained",
    }:
        return _overlay_daily_quality_report(fact, [])
    flags = list(fact.get("quality_flags") or [])
    code = "daily_needs_review"
    message = (
        "The selected window has an inferred daily gap, but the matching "
        "published audit contains no exact issue for this market/date window. "
        "An operator must reconcile it before any retry."
    )
    if not any(flag.get("code") == code for flag in flags):
        flags.append(
            {
                "code": code,
                "severity": "warning",
                "category": "data_health",
                "message": message,
                "observed_value": fact.get("missing_calendar_days"),
                "threshold": 0,
            }
        )
    return {
        **fact,
        "status": "needs_review",
        "reason": message,
        "reason_code": "daily_audit_no_matching_issue",
        "retryable": False,
        "action": "operator_manual_review",
        "reason_code_counts": {
            "daily_audit_no_matching_issue": 1,
        },
        "issue_status_counts": {},
        "issue_outcome_counts": [],
        "affected_dates": [],
        "affected_date_count": 0,
        "daily_evidence_mode": "catalog_report_reconciliation",
        "quality_flags": flags,
    }


def _quality_lineage(
    *,
    status: str,
    observed_at: str | None = None,
    source: str | None = None,
    source_endpoint: str | None = None,
    method: str | None = None,
    reason: str | None = None,
    snapshot_id: str | None = None,
    dataset_sha256: str | None = None,
    raw_response_sha256: str | None = None,
    reason_code: str | None = None,
    retryable: bool = False,
    action: str | None = None,
) -> dict[str, Any]:
    """Return one stable set of fields shared by every quality fact."""
    return {
        "status": status,
        "observed_at": observed_at,
        "source": source,
        "source_endpoint": sanitize_public_source_endpoint(source_endpoint),
        "method": method,
        "reason": reason,
        "reason_code": reason_code or reason,
        "retryable": bool(retryable),
        "action": action,
        "snapshot_id": snapshot_id,
        "dataset_sha256": dataset_sha256,
        "raw_response_sha256": raw_response_sha256,
    }


def _validated_public_quality_fact(
    fact: dict[str, Any],
    *,
    market_type: str | None = None,
    fact_name: str | None = None,
) -> dict[str, Any]:
    """Fail closed when a public status/reason/retryability tuple is invalid."""
    normalized_fact = {
        **fact,
        "quality_flags": list(fact.get("quality_flags") or []),
    }
    rule = (
        canonical_quality_fact_rule(
            market_type,
            fact_name,
            normalized_fact.get("status"),
            normalized_fact.get("reason_code"),
        )
        if market_type is not None and fact_name is not None
        else quality_outcome_rule(
            normalized_fact.get("status"),
            normalized_fact.get("reason_code"),
        )
    )
    if (
        rule is not None
        and normalized_fact.get("retryable") is rule.retryable
    ):
        if market_type is not None and fact_name is not None:
            normalized_fact["action"] = canonical_quality_fact_action(
                market_type,
                fact_name,
                normalized_fact.get("status"),
                normalized_fact.get("reason_code"),
                normalized_fact.get("retryable"),
                daily_evidence_mode=normalized_fact.get(
                    "daily_evidence_mode"
                ),
                manual_review_present=bool(
                    (normalized_fact.get("issue_status_counts") or {}).get(
                        "needs_review"
                    )
                ),
            )
        return normalized_fact
    flags = normalized_fact["quality_flags"]
    if not any(
        flag.get("code") == "daily_quality_outcome_invalid"
        for flag in flags
        if isinstance(flag, dict)
    ):
        flags.append(
            {
                "code": "daily_quality_outcome_invalid",
                "severity": "warning",
                "category": "data_health",
                "message": (
                    "The public quality outcome failed its exact bounded "
                    "status/reason contract and requires operator review."
                ),
                "observed_value": None,
                "threshold": None,
            }
        )
    return {
        **normalized_fact,
        "status": "needs_review",
        "reason": "daily_quality_outcome_invalid",
        "reason_code": "daily_quality_outcome_invalid",
        "retryable": False,
        "action": "operator_manual_review",
        "quality_flags": flags,
    }


def _dataset_source_for_market(
    metadata: dict[str, Any],
    market_type: str,
) -> dict[str, Any] | None:
    expected_name = CEX_FILENAME if market_type == "cex" else DEX_FILENAME
    sources = metadata.get("sources") or []
    for source in sources:
        if source.get("name") == expected_name:
            return source
    daily_sources = sources[:2]
    fallback_index = 0 if market_type == "cex" else 1
    return (
        daily_sources[fallback_index]
        if len(daily_sources) > fallback_index
        else None
    )


def _quality_flags_for_fact(
    market: dict[str, Any],
    fact: str,
) -> list[dict[str, Any]]:
    details = market.get("quality_flag_details") or []
    if fact == "daily":
        codes = {"low_daily_coverage"}
    elif fact == "tvl":
        codes = {"tiny_pool"}
    elif fact == "depth":
        codes = {
            "inactive_cex_instrument",
            "stale_cex_lifecycle_evidence",
            "depth_unavailable",
            "depth_unsupported",
            "unsupported_depth",
            "depth_partial",
            "partial_depth",
            "depth_failed",
            "failed_depth",
            "depth_not_cataloged",
            "zero_depth_10bps",
            "zero_depth_inside_spread",
            "off_market_pool_state_price",
            "off_market_price",
            "wide_quoted_spread",
        }
    else:
        codes = set()
    return [
        detail
        for detail in details
        if detail.get("code") in codes
    ]


def _daily_quality_fact(
    market: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    if current_cex_facts_withheld(market):
        stale = (
            market.get("current_listing_status")
            == "official_catalog_evidence_stale"
        )
        lifecycle_flag_code = (
            "stale_cex_lifecycle_evidence"
            if stale
            else "inactive_cex_instrument"
        )
        reason_code = (
            "official_catalog_evidence_stale"
            if stale
            else "instrument_absent_from_current_catalog"
        )
        return {
            **_quality_lineage(
                status="needs_review" if stale else "source_no_observation",
                observed_at=market.get("current_listing_checked_at"),
                source="Official current exchange instrument catalog",
                source_endpoint=market.get("current_listing_source"),
                method="official_current_instrument_catalog_membership",
                reason=(
                    "The last official instrument-catalog evidence is older "
                    "than the 36-hour freshness contract. Current facts remain "
                    "withheld until the listing state is rechecked."
                    if stale
                    else "The instrument was absent from the official current "
                    "exchange catalog at the recorded check. Historical "
                    "candles are withheld from current facts; no exact "
                    "delisting date is inferred."
                ),
                reason_code=reason_code,
                retryable=False,
                action=None,
                raw_response_sha256=market.get(
                    "current_listing_response_sha256"
                ),
            ),
            "observed_start": None,
            "observed_end": None,
            "observation_days": None,
            "requested_window_days": None,
            "missing_calendar_days": None,
            "coverage_expected_start": None,
            "coverage_expected_end": None,
            "coverage_ratio": None,
            "quality_flags": [
                {
                    "code": lifecycle_flag_code,
                    "severity": "critical",
                    "category": "data_health",
                    "message": SCREENING_QUALITY_FLAG_MESSAGES[
                        lifecycle_flag_code
                    ],
                    "observed_value": market.get(
                        "current_listing_checked_at"
                    ),
                    "threshold": (
                        "official_catalog_check_within_36_hours"
                        if stale
                        else "present_in_official_current_catalog"
                    ),
                }
            ],
        }
    window_metrics = market.get("window_metrics")
    metrics = window_metrics if isinstance(window_metrics, dict) else {}
    observation_days = metrics.get(
        "observation_count",
        metrics.get("observation_days"),
    )
    requested_window_days = metrics.get("requested_window_days")
    missing_calendar_days = metrics.get("missing_calendar_days")
    expected_start = metrics.get("coverage_expected_start")
    expected_end = metrics.get("coverage_expected_end")
    try:
        window_start_day = date.fromisoformat(metadata.get("window_start", ""))
        window_end_day = date.fromisoformat(metadata.get("window_end", ""))
        market_start_day = date.fromisoformat(
            market.get("observed_start")
            or market.get("first_observed_date", "")
        )
        market_latest_day = date.fromisoformat(
            market.get("observed_end")
            or market.get("latest_observed_date", "")
        )
    except (TypeError, ValueError):
        lifecycle_overlap = None
    else:
        expected_start_day = max(window_start_day, market_start_day)
        expected_end_day = window_end_day
        lifecycle_overlap = expected_start_day <= expected_end_day
        if lifecycle_overlap:
            expected_start = expected_start_day.isoformat()
            expected_end = expected_end_day.isoformat()
            requested_window_days = (
                expected_end_day - expected_start_day
            ).days + 1
            numeric_observations = (
                int(observation_days)
                if isinstance(observation_days, (int, float))
                and not isinstance(observation_days, bool)
                else 0
            )
            missing_calendar_days = max(
                requested_window_days - numeric_observations,
                0,
            )
        else:
            requested_window_days = 0
            missing_calendar_days = 0
            expected_start = None
            expected_end = None
    observed = (
        isinstance(observation_days, (int, float))
        and not isinstance(observation_days, bool)
        and observation_days > 0
    )
    has_gap = (
        isinstance(missing_calendar_days, (int, float))
        and not isinstance(missing_calendar_days, bool)
        and missing_calendar_days > 0
    )
    if lifecycle_overlap is False:
        status = "not_applicable"
        reason = "selected_window_before_first_market_observation"
    elif (
        not observed
        and lifecycle_overlap is True
        and window_start_day > market_latest_day
    ):
        status = "missing_unexplained"
        reason = "no_daily_observations_after_latest_observed_market_date"
    elif not observed and lifecycle_overlap is True:
        status = "backfill_pending"
        reason = "missing_daily_observations_inside_observed_market_lifecycle"
    elif not observed:
        status = "missing_unexplained"
        reason = "no_daily_observations_in_selected_window"
    elif has_gap:
        status = "backfill_pending"
        reason = "missing_daily_observations_in_selected_window"
    else:
        status = "observed"
        reason = "observed"
    dataset_source = _dataset_source_for_market(
        metadata,
        market["market_type"],
    )
    coverage_ratio = (
        observation_days / requested_window_days
        if isinstance(observation_days, (int, float))
        and not isinstance(observation_days, bool)
        and requested_window_days
        else metrics.get("coverage_ratio")
        if lifecycle_overlap is None
        else None
    )
    daily_flags = []
    threshold = MARKET_QUALITY_THRESHOLDS[
        "minimum_primary_coverage_ratio"
    ]
    if (
        isinstance(coverage_ratio, (int, float))
        and not isinstance(coverage_ratio, bool)
        and coverage_ratio < threshold
    ):
        daily_flags.append(
            {
                "code": "low_daily_coverage",
                "severity": "warning",
                "category": "data_health",
                "message": (
                    "Selected-window daily coverage is below the declared threshold."
                ),
                "observed_value": coverage_ratio,
                "threshold": threshold,
            }
        )
    return {
        **_quality_lineage(
            status=status,
            observed_at=metrics.get(
                "latest_observed_date",
                metrics.get("observed_end")
                or market.get("observed_end")
                or market.get("latest_observed_date"),
            ),
            source=market.get("source"),
            method="daily_close_no_fill",
            reason=reason,
            reason_code=reason,
            retryable=status in {"backfill_pending", "missing_unexplained"},
            action=(
                "operator_review_retry_queue"
                if status in {"backfill_pending", "missing_unexplained"}
                else None
            ),
            dataset_sha256=(
                dataset_source.get("sha256") if dataset_source else None
            ),
        ),
        "observed_start": metrics.get(
            "first_observed_date",
            metrics.get("observed_start")
            or market.get("observed_start")
            or market.get("first_observed_date"),
        ),
        "observed_end": metrics.get(
            "latest_observed_date",
            metrics.get("observed_end")
            or market.get("observed_end")
            or market.get("latest_observed_date"),
        ),
        "observation_days": observation_days,
        "requested_window_days": requested_window_days,
        "missing_calendar_days": missing_calendar_days,
        "coverage_expected_start": expected_start,
        "coverage_expected_end": expected_end,
        "coverage_ratio": coverage_ratio,
        "quality_flags": daily_flags,
    }


def _tvl_quality_fact(market: dict[str, Any]) -> dict[str, Any]:
    if market["market_type"] == "cex":
        return {
            **_quality_lineage(
                status="not_applicable",
                reason="cex_markets_do_not_have_pool_tvl",
                reason_code="cex_markets_do_not_have_pool_tvl",
            ),
            "value_usd": None,
            "quality_flags": [],
        }
    status, reason_code = normalize_tvl_source_outcome(
        market.get("tvl_status") or "unavailable",
        market.get("tvl_reason_code"),
        market.get("tvl_error"),
    )
    outcome = quality_outcome_rule(status, reason_code)
    assert outcome is not None
    retryable = outcome.retryable
    return {
        **_quality_lineage(
            status=status,
            observed_at=market.get("tvl_observed_at"),
            source=market.get("tvl_source"),
            source_endpoint=market.get("tvl_source_endpoint"),
            method=market.get("tvl_method"),
            reason=reason_code,
            reason_code=reason_code,
            retryable=retryable,
            action="retry_tvl_collection" if retryable else None,
            snapshot_id=market.get("tvl_snapshot_id"),
            raw_response_sha256=market.get("tvl_raw_response_sha256"),
        ),
        # An observed zero is a real value.  Do not use truthiness here.
        "value_usd": market.get("tvl_usd"),
        "quality_flags": _quality_flags_for_fact(market, "tvl"),
    }


def _depth_quality_fact(market: dict[str, Any]) -> dict[str, Any]:
    market_type = market["market_type"]
    raw_status = str(market.get("depth_status") or "unavailable").lower()
    if market_type == "cex":
        status, reason_code = normalize_cex_source_outcome(
            raw_status,
            market.get("depth_reason_code"),
            market.get("depth_error"),
        )
    else:
        status, reason_code = normalize_dex_depth_source_outcome(
            raw_status,
            market.get("depth_reason_code"),
            market.get("depth_error"),
        )
    outcome = quality_outcome_rule(status, reason_code)
    assert outcome is not None
    retryable = outcome.retryable
    temporal_alignment = {
        "state_observed_at": (
            market.get("depth_block_timestamp")
            if market_type == "dex"
            else market.get("depth_observed_at")
        ),
        "usd_price_observed_at": (
            market.get("depth_usd_price_observed_at")
            if market_type == "dex"
            else None
        ),
        "usd_price_source_snapshot_id": (
            market.get("depth_usd_price_source_snapshot_id")
            if market_type == "dex"
            else None
        ),
        "usd_price_state_skew_seconds": (
            market.get("depth_usd_price_skew_seconds")
            if market_type == "dex"
            else None
        ),
        "warning_usd_price_state_skew_seconds": (
            USD_PRICE_SKEW_WARNING_SECONDS
        ),
        "max_usd_price_state_skew_seconds": USD_PRICE_SKEW_MAX_SECONDS,
        "status": (
            market.get("depth_usd_price_freshness_status")
            if market_type == "dex"
            else "not_applicable"
        ),
    }
    quality_flags = list(_quality_flags_for_fact(market, "depth"))
    if status == "source_no_observation":
        quality_flags = [
            flag for flag in quality_flags
            if flag.get("code") not in {"depth_failed", "failed_depth"}
        ]
        quality_flags.append(
            {
                "code": "depth_source_no_observation",
                "severity": "info",
                "category": "source_outcome",
                "message": (
                    "The source responded successfully but supplied no "
                    "two-sided executable order-book observation."
                ),
                "observed_value": None,
                "threshold": None,
            }
        )
    timing_status = temporal_alignment["status"]
    measured = (
        market_type == "dex"
        and raw_status in {"observed", "partial", "complete"}
        and any(
            parse_number(market.get("total_depth_{}bps_usd".format(band)))
            is not None
            for band in (10, 25, 50, 100)
        )
    )
    alignment_applicable = (
        measured and bool(market.get("depth_requires_usd_price_alignment"))
    )
    if alignment_applicable and timing_status in {"stale", "unavailable"}:
        quality_flags.append(
            {
                "code": "depth_usd_price_time_mismatch",
                "severity": "critical",
                "category": "data_health",
                "message": (
                    "USD depth is withheld because its price response is "
                    "unavailable or more than two hours from pool state."
                ),
                "observed_value": temporal_alignment[
                    "usd_price_state_skew_seconds"
                ],
                "threshold": USD_PRICE_SKEW_MAX_SECONDS,
            }
        )
    elif alignment_applicable and timing_status == "warning":
        quality_flags.append(
            {
                "code": "depth_usd_price_time_warning",
                "severity": "warning",
                "category": "data_health",
                "message": (
                    "The USD price response is more than 15 minutes, but no "
                    "more than two hours, from pool state."
                ),
                "observed_value": temporal_alignment[
                    "usd_price_state_skew_seconds"
                ],
                "threshold": USD_PRICE_SKEW_WARNING_SECONDS,
            }
        )
    bands = {}
    for band in (10, 25, 50, 100):
        sell_prefix = "bid" if market_type == "cex" else "sell"
        buy_prefix = "ask" if market_type == "cex" else "buy"
        bands[str(band)] = {
            "sell_token_usd": (
                None if status == "source_no_observation" else market.get(
                    f"{sell_prefix}_depth_{band}bps_usd"
                )
            ),
            "buy_token_usd": (
                None if status == "source_no_observation" else market.get(
                    f"{buy_prefix}_depth_{band}bps_usd"
                )
            ),
            "total_usd": (
                None if status == "source_no_observation" else market.get(
                    f"total_depth_{band}bps_usd"
                )
            ),
            "complete": (
                False if status == "source_no_observation" else bool(
                    market.get(f"depth_{band}bps_complete")
                )
            ),
        }
    return {
        **_quality_lineage(
            status=status,
            observed_at=market.get("depth_observed_at"),
            source=market.get("depth_source"),
            source_endpoint=market.get("depth_source_endpoint"),
            method=market.get("depth_method"),
            reason=reason_code,
            reason_code=reason_code,
            retryable=retryable,
            action="retry_depth_collection" if retryable else None,
            snapshot_id=market.get("depth_snapshot_id"),
            raw_response_sha256=market.get(
                "depth_raw_response_sha256"
            ),
        ),
        "block_number": market.get("depth_block_number"),
        "state_observed_at": temporal_alignment["state_observed_at"],
        "temporal_alignment": temporal_alignment,
        "protocol_model": market.get("depth_protocol_model"),
        "bands_bps": bands,
        "quality_flags": quality_flags,
    }


def _execution_quality_source(
    resolver,
) -> dict[str, Any]:
    try:
        path = resolver()
    except OSError:
        return {
            "snapshot": None,
            "error_code": "execution_snapshot_invalid",
        }
    if path is None:
        return {"snapshot": None, "error_code": None}
    try:
        return {
            "snapshot": load_execution_cost_snapshot(path),
            "error_code": None,
        }
    except (OSError, ValueError):
        return {
            "snapshot": None,
            "error_code": "execution_snapshot_invalid",
        }


def _one_execution_value(
    rows: list[dict[str, str]],
    field: str,
) -> str | None:
    values = sorted(
        {
            str(row.get(field))
            for row in rows
            if row.get(field) not in (None, "")
        }
    )
    return values[0] if len(values) == 1 else None


def _execution_quality_fact(
    market: dict[str, Any],
    source_state: dict[str, Any],
) -> dict[str, Any]:
    if current_cex_facts_withheld(market):
        stale = (
            market.get("current_listing_status")
            == "official_catalog_evidence_stale"
        )
        lifecycle_flag_code = (
            "stale_cex_lifecycle_evidence"
            if stale
            else "inactive_cex_instrument"
        )
        status = "needs_review" if stale else "source_no_observation"
        reason_code = (
            "official_catalog_evidence_stale"
            if stale
            else "instrument_absent_from_current_catalog"
        )
        return {
            **_quality_lineage(
                status=status,
                observed_at=market.get("current_listing_checked_at"),
                source="Official current exchange instrument catalog",
                source_endpoint=market.get("current_listing_source"),
                method="official_current_instrument_catalog_membership",
                reason=reason_code,
                reason_code=reason_code,
                retryable=False,
                action=None,
                raw_response_sha256=market.get(
                    "current_listing_response_sha256"
                ),
            ),
            "source_snapshot_id": None,
            "temporal_alignment": None,
            "quality_flags": [
                {
                    "code": lifecycle_flag_code,
                    "severity": "critical",
                    "category": "data_health",
                    "message": SCREENING_QUALITY_FLAG_MESSAGES[
                        lifecycle_flag_code
                    ],
                    "observed_value": market.get(
                        "current_listing_checked_at"
                    ),
                    "threshold": (
                        "official_catalog_check_within_36_hours"
                        if stale
                        else "present_in_official_current_catalog"
                    ),
                }
            ],
            "published_at": None,
            "status_counts": {status: 1},
            "status_reason_counts": {reason_code: 1},
            "scenario_count": 0,
            "directions": [],
            "notionals_usd": [],
        }
    snapshot = source_state["snapshot"]
    load_error_code = source_state["error_code"]
    if load_error_code is not None:
        return {
            **_quality_lineage(
                status="failed",
                reason=(
                    "The execution snapshot could not be loaded or validated. "
                    "An operator must inspect the protected service logs."
                ),
                reason_code=load_error_code,
                retryable=True,
                action="retry_execution_collection",
            ),
            "published_at": None,
            "status_counts": {"failed": 1},
            "status_reason_counts": {
                "execution_snapshot_invalid": 1,
            },
            "scenario_count": 0,
        }
    if snapshot is None:
        return {
            **_quality_lineage(
                status="unavailable",
                reason="execution_snapshot_unavailable",
                reason_code="execution_snapshot_unavailable",
            ),
            "published_at": None,
            "status_counts": {},
            "status_reason_counts": {},
            "scenario_count": 0,
        }
    rows = snapshot["by_market"].get(market["market_id"])
    if rows is None:
        status = "not_cataloged_in_snapshot"
        reason_code = "execution_market_not_cataloged_in_snapshot"
        outcome = quality_outcome_rule(status, reason_code)
        assert outcome is not None
        return {
            **_quality_lineage(
                status=status,
                reason=reason_code,
                reason_code=reason_code,
                retryable=outcome.retryable,
                action=(
                    "retry_execution_collection"
                    if outcome.retryable
                    else None
                ),
            ),
            "published_at": snapshot.get("observed_at"),
            "status_counts": {},
            "status_reason_counts": {},
            "scenario_count": 0,
        }

    temporal_alignment = _execution_temporal_alignment(rows)
    market_type = str(
        market.get("market_type")
        or _one_execution_value(rows, "market_type")
        or ""
    ).lower()
    public_row_outcomes = [
        normalize_execution_source_outcome(
            market_type,
            row.get("status"),
            row.get("status_reason"),
            row.get("error"),
        )
        for row in rows
    ]
    status_counts = Counter(
        status for status, _reason_code in public_row_outcomes
    )
    status_priority = (
        "invalid",
        "failed",
        "collection_failed",
        "needs_review",
        "partial",
        "unsupported",
        "source_no_observation",
        "observed",
    )
    has_invalid_public_outcome = any(
        outcome == ("needs_review", "daily_quality_outcome_invalid")
        for outcome in public_row_outcomes
    )
    status = (
        "needs_review"
        if has_invalid_public_outcome
        else next(
            candidate
            for candidate in status_priority
            if status_counts.get(candidate)
        )
    )
    measured = bool(
        status_counts.get("observed") or status_counts.get("partial")
    )
    temporal_flags = []
    if (
        measured
        and not temporal_alignment["usable"]
        and not has_invalid_public_outcome
    ):
        status = "failed"
        temporal_flags.append(
            {
                "code": "execution_usd_price_time_mismatch",
                "severity": "critical",
                "category": "data_health",
                "message": (
                    "Execution values are withheld because the required USD "
                    "price response is unavailable or more than two hours from "
                    "market state."
                ),
                "observed_value": temporal_alignment[
                    "usd_price_state_skew_seconds"
                ],
                "threshold": USD_PRICE_SKEW_MAX_SECONDS,
            }
        )
    elif measured and temporal_alignment["status"] == "warning":
        temporal_flags.append(
            {
                "code": "execution_usd_price_time_warning",
                "severity": "warning",
                "category": "data_health",
                "message": (
                    "The USD price response is more than 15 minutes, but no "
                    "more than two hours, from execution market state."
                ),
                "observed_value": temporal_alignment[
                    "usd_price_state_skew_seconds"
                ],
                "threshold": USD_PRICE_SKEW_WARNING_SECONDS,
            }
        )
    reason_counts = Counter(
        reason_code for _status, reason_code in public_row_outcomes
    )
    state_times = sorted(
        {
            row["state_observed_at"]
            for row in rows
            if row.get("state_observed_at")
        }
    )
    published_times = sorted(
        {
            row["observed_at"]
            for row in rows
            if row.get("observed_at")
        }
    )
    status_reasons = sorted(
        {
            reason_code
            for outcome_status, reason_code in public_row_outcomes
            if outcome_status == status
        }
    )
    lineage_reason = (
        "execution_usd_price_time_mismatch"
        if measured and not temporal_alignment["usable"]
        and not has_invalid_public_outcome
        else status_reasons[0]
        if len(status_reasons) == 1
        else "multiple_daily_quality_reasons"
        if status == "collection_failed"
        and status_reasons
        and all(
            (
                rule := quality_outcome_rule(status, reason_code)
            ) is not None
            and rule.retryable
            for reason_code in status_reasons
        )
        else "execution_calculation_failed"
        if status == "failed"
        else "daily_quality_outcome_invalid"
    )
    status, lineage_reason = normalize_execution_source_outcome(
        market_type,
        status,
        lineage_reason,
    )
    public_outcome = quality_outcome_rule(status, lineage_reason)
    assert public_outcome is not None
    retryable = public_outcome.retryable
    if status == "source_no_observation":
        reason_counts = Counter({lineage_reason: sum(status_counts.values())})
        temporal_flags = [
            flag for flag in temporal_flags
            if flag.get("category") != "data_health"
        ]
        temporal_flags.append(
            {
                "code": "execution_source_no_observation",
                "severity": "info",
                "category": "source_outcome",
                "message": (
                    "The source responded successfully but supplied no "
                    "two-sided executable order-book observation."
                ),
                "observed_value": None,
                "threshold": None,
            }
        )
    return {
        **_quality_lineage(
            status=status,
            observed_at=state_times[-1] if state_times else None,
            source=_one_execution_value(rows, "source"),
            source_endpoint=_one_execution_value(
                rows,
                "source_endpoint",
            ),
            method=_one_execution_value(rows, "calculation_method"),
            reason=lineage_reason,
            reason_code=lineage_reason,
            retryable=retryable,
            action=(
                "retry_execution_collection"
                if retryable
                else None
            ),
            snapshot_id=_one_execution_value(rows, "snapshot_id"),
            raw_response_sha256=_one_execution_value(
                rows,
                "raw_response_sha256",
            ),
        ),
        "source_snapshot_id": _one_execution_value(
            rows,
            "source_snapshot_id",
        ),
        "temporal_alignment": temporal_alignment,
        "quality_flags": temporal_flags,
        "published_at": published_times[-1] if published_times else None,
        "status_counts": dict(sorted(status_counts.items())),
        "status_reason_counts": dict(sorted(reason_counts.items())),
        "scenario_count": len(rows),
        "directions": sorted(
            {row["direction"] for row in rows if row.get("direction")}
        ),
        "notionals_usd": sorted(
            {
                int(Decimal(row["requested_notional_usd"]))
                for row in rows
                if row.get("requested_notional_usd")
            }
        ),
    }


def build_market_quality(
    token_symbol: str | None,
    scope: str | None = None,
    market_a_id: str | None = None,
    market_b_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """Return a fact-by-market quality inventory for one exact Token."""
    if not token_symbol:
        raise PublicClientRequestError("token is required")
    token = token_symbol.strip().upper()
    normalized_scope = (scope or "all").strip().lower()
    if normalized_scope not in {"all", "selected"}:
        raise PublicClientRequestError("scope must be all or selected")

    catalog = build_token_market_catalog(
        token,
        start,
        end,
        allow_empty_window=True,
    )
    token_markets = [
        market
        for market in catalog["markets"]
        if market["token_symbol"] == token
    ]
    if not token_markets:
        raise PublicClientRequestError("Token is not cataloged")
    by_id = {market["market_id"]: market for market in token_markets}
    selected_ids: list[str] = []
    if normalized_scope == "selected":
        if not market_a_id or not market_b_id:
            raise PublicClientRequestError(
                "market_a and market_b are required for selected scope"
            )
        if market_a_id == market_b_id:
            raise PublicClientRequestError(
                "market_a and market_b must be different"
            )
        if market_a_id not in by_id or market_b_id not in by_id:
            raise PublicClientRequestError(
                "Selected market is not cataloged for the requested token"
            )
        selected_ids = [market_a_id, market_b_id]
        token_markets = [by_id[market_id] for market_id in selected_ids]

    quality_report_state, report_issues = _daily_quality_report_state(
        catalog["metadata"]
    )
    selected_report_issues = _daily_quality_issues_for_window(
        report_issues,
        market_ids={market["market_id"] for market in token_markets},
        window_start=catalog["metadata"]["window_start"],
        window_end=catalog["metadata"]["window_end"],
    )
    report_issues_by_market: dict[str, list[dict[str, Any]]] = defaultdict(
        list
    )
    for issue in selected_report_issues:
        report_issues_by_market[issue["market_id"]].append(issue)

    selected_market_types = {
        market["market_type"] for market in token_markets
    }
    execution_resolvers = {
        "cex": resolve_cex_execution_cost_path,
        "dex": resolve_dex_execution_cost_path,
    }
    execution_sources = {
        market_type: _execution_quality_source(
            execution_resolvers[market_type]
        )
        for market_type in selected_market_types
    }
    for market_type, source_state in execution_sources.items():
        snapshot = source_state["snapshot"]
        if snapshot is not None:
            validate_depth_execution_cohort(
                catalog["metadata"],
                snapshot,
                market_type,
            )
    quality_markets = []
    for market in token_markets:
        screening = screening_quality_projection(market)
        daily_fact = _daily_quality_fact(
            market,
            catalog["metadata"],
        )
        if quality_report_state["status"] == "matched":
            market_report_issues = report_issues_by_market.get(
                market["market_id"],
                [],
            )
            daily_fact = (
                _overlay_daily_quality_report(
                    daily_fact,
                    market_report_issues,
                )
                if market_report_issues
                else _reconcile_daily_fact_without_report_issue(daily_fact)
            )
        facts = {
            "daily": daily_fact,
            "tvl": _tvl_quality_fact(market),
            "depth": _depth_quality_fact(market),
            "execution": _execution_quality_fact(
                market,
                execution_sources[market["market_type"]],
            ),
        }
        facts = {
            name: _validated_public_quality_fact(
                fact,
                market_type=market["market_type"],
                fact_name=name,
            )
            for name, fact in facts.items()
        }
        source_no_observation_facts = {
            name for name, fact in facts.items()
            if fact.get("status") == "source_no_observation"
        }
        quality_flags = [
            flag
            for flag in (market.get("quality_flag_details") or [])
            if (
                flag.get("code") != "low_daily_coverage"
                and not (
                    "depth" in source_no_observation_facts
                    and flag.get("code") in {"depth_failed", "failed_depth"}
                )
                and not (
                    "execution" in source_no_observation_facts
                    and flag.get("code") in {
                        "execution_failed",
                        "failed_execution",
                        "execution_calculation_failed",
                        "execution_collection_failed",
                    }
                )
            )
        ]
        known_codes = {
            flag.get("code")
            for flag in quality_flags
            if flag.get("code")
        }
        for fact in facts.values():
            for flag in fact.get("quality_flags", []):
                if flag.get("code") in known_codes:
                    continue
                quality_flags.append(flag)
                if flag.get("code"):
                    known_codes.add(flag["code"])
        data_health_flags = [
            flag
            for flag in quality_flags
            if flag.get("category", "data_health") == "data_health"
        ]
        quality_status = "ok"
        if any(
            flag.get("severity") == "critical"
            for flag in data_health_flags
        ):
            quality_status = "critical"
        elif any(
            flag.get("severity") == "warning"
            for flag in data_health_flags
        ):
            quality_status = "warning"
        elif data_health_flags:
            quality_status = "info"
        retryable_facts = sorted(
            name
            for name, fact in facts.items()
            if fact.get("retryable")
        )
        structural_facts = sorted(
            name
            for name, fact in facts.items()
            if fact.get("status") in {"unsupported", "not_applicable"}
        )
        pending_facts = sorted(
            name
            for name, fact in facts.items()
            if fact.get("status") in {
                "backfill_pending",
                "missing_unexplained",
                "partial",
                "needs_review",
                "source_no_observation",
            }
        )
        quality_markets.append(
            {
                "market_id": market["market_id"],
                "token_symbol": market["token_symbol"],
                "market_type": market["market_type"],
                "venue": market["venue"],
                "instrument": market["instrument"],
                "chain": market.get("chain"),
                "pool_address": market.get("pool_address"),
                "quality_status": quality_status,
                "quality_flags": quality_flags,
                "screening_quality_status": screening["status"],
                "screening_quality_flags": screening["flags"],
                "screening_quality_scope": screening["scope"],
                "screening_quality_window": screening[
                    "evaluation_window"
                ],
                "market_conditions": [
                    flag
                    for flag in quality_flags
                    if flag.get("category") == "market_condition"
                ],
                "capability_flags": [
                    flag
                    for flag in quality_flags
                    if flag.get("category") == "capability"
                ],
                "retryable_facts": retryable_facts,
                "structural_facts": structural_facts,
                "pending_facts": pending_facts,
                "usability_status": (
                    "blocked"
                    if quality_status == "critical"
                    else "needs_recovery"
                    if retryable_facts
                    else "usable_with_limits"
                    if structural_facts or pending_facts
                    else "usable"
                ),
                "facts": facts,
            }
        )
    fact_status_counts = Counter(
        fact.get("status") or "unavailable"
        for market in quality_markets
        for fact in market["facts"].values()
    )
    retryable_count = sum(
        1
        for market in quality_markets
        for fact in market["facts"].values()
        if fact.get("retryable")
    )
    report_reason_counts = Counter(
        issue["reason_code"] for issue in selected_report_issues
    )
    report_status_counts = Counter(
        issue["status"] for issue in selected_report_issues
    )
    report_outcome_counts = _daily_issue_outcome_counts(
        selected_report_issues
    )
    report_affected_dates = sorted(
        {issue["date"] for issue in selected_report_issues}
    )
    report_market_rollups = []
    if quality_report_state["status"] == "matched":
        daily_facts_by_market = {
            market["market_id"]: market["facts"]["daily"]
            for market in quality_markets
        }
        for market in token_markets:
            market_issues = report_issues_by_market.get(
                market["market_id"],
                [],
            )
            market_status_counts = Counter(
                issue["status"] for issue in market_issues
            )
            market_reason_counts = Counter(
                issue["reason_code"] for issue in market_issues
            )
            market_affected_dates = sorted(
                {issue["date"] for issue in market_issues}
            )
            daily_fact = daily_facts_by_market[market["market_id"]]
            report_market_rollups.append(
                {
                    "market_id": market["market_id"],
                    "issue_count": len(market_issues),
                    "issue_outcome_counts": _daily_issue_outcome_counts(
                        market_issues
                    ),
                    "reason_code_counts": dict(
                        sorted(market_reason_counts.items())
                    ),
                    "status_counts": dict(
                        sorted(market_status_counts.items())
                    ),
                    "affected_date_count": len(
                        market_affected_dates
                    ),
                    "affected_dates": market_affected_dates,
                    "evidence_mode": daily_fact[
                        "daily_evidence_mode"
                    ],
                    "fact_outcome": {
                        "status": daily_fact["status"],
                        "reason_code": daily_fact["reason_code"],
                        "retryable": daily_fact["retryable"],
                        "action": daily_fact["action"],
                    },
                }
            )
    return {
        "metadata": {
            "contract_version": QUALITY_CONTRACT_VERSION,
            "data_generation": catalog["metadata"]["data_generation"],
            "scope": normalized_scope,
            "selected_market_ids": selected_ids,
            "facts": ["daily", "tvl", "depth", "execution"],
            "window_start": catalog["metadata"].get("window_start"),
            "window_end": catalog["metadata"].get("window_end"),
            "status_semantics": QUALITY_STATUS_SEMANTICS,
            "quality_dimensions": {
                "data_health": (
                    "Unexpected missing, failed, invalid, stale, or inconsistent facts."
                ),
                "capability": "Whether a validated adapter exists.",
                "measurement_limit": "Measured facts with an explicit lower bound.",
                "market_condition": (
                    "Observed liquidity conditions; not collection failures."
                ),
            },
            "fact_status_counts": dict(sorted(fact_status_counts.items())),
            "retryable_fact_count": retryable_count,
            "daily_quality_report": {
                **quality_report_state,
                "selected_window_issue_count": len(
                    selected_report_issues
                ),
                **(
                    {"issue_outcome_counts": report_outcome_counts}
                    if quality_report_state["status"] == "matched"
                    else {}
                ),
                "reason_code_counts": dict(
                    sorted(report_reason_counts.items())
                ),
                "status_counts": dict(
                    sorted(report_status_counts.items())
                ),
                "affected_date_count": len(report_affected_dates),
                "affected_dates": report_affected_dates,
                **(
                    {"market_issue_rollups": report_market_rollups}
                    if quality_report_state["status"] == "matched"
                    else {}
                ),
            },
            "freshness": catalog["metadata"].get("freshness"),
            "sources": catalog["metadata"].get("sources", []),
            "missing_value_rule": (
                "Measured zero remains zero. Missing, unavailable, failed, "
                "backfill-pending, unsupported, not-cataloged, and "
                "not-applicable facts remain "
                "distinct and are never zero-filled."
            ),
        },
        "token_symbol": token,
        "markets": quality_markets,
    }


def encode_json_payload(payload: Any, accept_encoding: str = "") -> tuple[bytes, bool]:
    """Serialize JSON and compress substantial responses when the client supports gzip."""
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(raw) >= 1024 and "gzip" in accept_encoding.lower():
        return gzip.compress(raw, compresslevel=5), True
    return raw, False


def _safe_path_signature(path: Path) -> SourceSignature:
    """Describe one optional source path without making it an availability gate."""

    try:
        stat = path.stat()
    except OSError:
        return (
            (
                str(path),
                "missing_or_unreadable",
                0,
                "missing_or_unreadable",
                0,
            ),
        )
    return (
        (
            str(path),
            stat.st_mtime_ns,
            stat.st_size,
            stat.st_ctime_ns,
            stat.st_ino,
        ),
    )


def event_source_signature() -> SourceSignature:
    """Track the independently published Event bundle without validating it.

    Validation belongs only to the Event endpoint.  Keeping signature discovery
    non-throwing ensures a damaged optional Event bundle cannot make price,
    depth, execution, or quality APIs unavailable.
    """

    event_root = resolve_event_data_root()
    pointer_path = event_root / "latest.json"
    signature = list(_safe_path_signature(pointer_path))
    try:
        pointer_stat = pointer_path.stat()
        if not pointer_path.is_file() or pointer_stat.st_size > 65_536:
            return tuple(signature)
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        bundle_id = (
            str(pointer.get("bundle_id") or "")
            if isinstance(pointer, dict)
            else ""
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return tuple(signature)
    if not re.fullmatch(r"[0-9a-f]{24}", bundle_id):
        return tuple(signature)
    bundle_path = event_root / "bundles" / bundle_id
    for filename in (
        "manifest.json",
        "event_fact_revisions.csv",
        "event_facts_latest.csv",
        "event_facts.sqlite3",
    ):
        signature.extend(_safe_path_signature(bundle_path / filename))
    return tuple(signature)


def route_source_signature() -> SourceSignature:
    """Track one optional complete route generation without validating it.

    The endpoint owns full five-file validation.  Signature discovery remains
    non-throwing so a missing or corrupt optional route feed cannot make the
    independent market-fact APIs unavailable.
    """

    root = resolve_opportunity_bundle()
    pointer_path = root / "latest.json"
    signature = list(_safe_path_signature(pointer_path))
    try:
        pointer_stat = pointer_path.stat()
        if not pointer_path.is_file() or pointer_stat.st_size > 65_536:
            return tuple(signature)
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        cohort_id = (
            str(pointer.get("route_cohort_id") or "")
            if isinstance(pointer, dict)
            else ""
        )
    except (OSError, RuntimeError, UnicodeDecodeError, json.JSONDecodeError):
        return tuple(signature)
    if re.fullmatch(r"cohort:[0-9a-f]{64}", cohort_id) is None:
        return tuple(signature)
    bundle_path = root / "bundles" / cohort_id
    for filename in (
        "manifest.json",
        "route_legs.csv",
        "cost_components.csv",
        "route_opportunities.csv",
        "route_cohort.sqlite3",
    ):
        signature.extend(_safe_path_signature(bundle_path / filename))
    return tuple(signature)


def build_event_facts(
    *,
    token: str | None = None,
    start: str | None = None,
    end: str | None = None,
    lifecycle: str | None = None,
    clock_state: str | None = None,
    clock_as_of: datetime | None = None,
) -> dict[str, Any]:
    """Return a validated Event bundle projection or explicit unavailability."""

    event_root = resolve_event_data_root()
    pointer_path = event_root / "latest.json"
    try:
        pointer_exists = pointer_path.exists()
        pointer_is_file = pointer_path.is_file() if pointer_exists else False
    except OSError as error:
        raise EventBundleError("Event Fact pointer cannot be inspected") from error
    if pointer_exists and not pointer_is_file:
        raise EventBundleError("Event Fact pointer is not a regular file")

    if not pointer_exists:
        payload = build_event_payload(
            [],
            manifest={"bundle_id": None, "built_at_utc": None},
            token=token,
            start=start,
            end=end,
            lifecycle=lifecycle,
            clock_state=clock_state,
            clock_as_of=clock_as_of,
        )
        payload["availability"] = {
            "status": "unavailable",
            "reason": "event_bundle_not_published",
        }
        return payload

    try:
        rows, manifest = load_latest_event_rows(event_root)
    except FileNotFoundError:
        # A missing pointer is an optional-feed state.  A manifest/file named by
        # an existing pointer is converted to EventBundleError by the validator.
        payload = build_event_payload(
            [],
            manifest={"bundle_id": None, "built_at_utc": None},
            token=token,
            start=start,
            end=end,
            lifecycle=lifecycle,
            clock_state=clock_state,
            clock_as_of=clock_as_of,
        )
        payload["availability"] = {
            "status": "unavailable",
            "reason": "event_bundle_not_published",
        }
        return payload
    except (OSError, RuntimeError) as error:
        raise EventBundleError("Event Fact publication cannot be resolved") from error
    payload = build_event_payload(
        rows,
        manifest=manifest,
        token=token,
        start=start,
        end=end,
        lifecycle=lifecycle,
        clock_state=clock_state,
        clock_as_of=clock_as_of,
    )
    payload["availability"] = {"status": "available", "reason": None}
    return payload


def build_route_opportunities(
    *,
    token: str | None = None,
    venue: str | None = None,
    notional: str | None = None,
    opportunity_class: str | None = None,
    route_type: str | None = None,
    availability: str | None = None,
    sort: str | None = None,
    direction: str | None = None,
) -> dict[str, Any]:
    """Return a compact view of only the validated complete route pointer."""

    arguments = {
        "token": token,
        "venue": venue,
        "notional_usd": notional,
        "opportunity_class": opportunity_class,
        "route_type": route_type,
        "availability": availability,
        "sort": sort,
        "direction": direction,
    }
    try:
        loaded = load_latest_opportunities()
    except OpportunityBundleUnavailable as error:
        return build_unavailable_opportunity_payload(
            reason=error.reason,
            **arguments,
        )
    try:
        return build_opportunity_payload(
            loaded["opportunities"],
            manifest=loaded["manifest"],
            legs=loaded["legs"],
            cost_components=loaded["cost_components"],
            route_candidates=loaded["bundle"]["routes"],
            manifest_sha256=loaded["manifest_sha256"],
            **arguments,
        )
    except OpportunityQueryError as error:
        raise PublicClientRequestError(str(error)) from None


def _historical_opportunity_arguments(
    query_items: tuple[tuple[str, str], ...],
) -> dict[str, str | None]:
    query = dict(query_items)
    return {
        "token": query.get("token"),
        "venue": query.get("venue"),
        "notional_usd": query.get("notional"),
        "opportunity_class": query.get("class"),
        "route_type": query.get("route_type"),
        "availability": query.get("availability"),
        "sort": query.get("sort"),
        "direction": query.get("dir"),
    }


def _close_historical_opportunity_view(loaded: Any) -> None:
    if isinstance(loaded, dict) or hasattr(loaded, "get"):
        view = loaded.get("validated_view")
        close = getattr(view, "close", None)
        if callable(close):
            close()


def _historical_loaded_publication_identity(
    loaded: Any,
) -> tuple[str, HistoricalPublicationSignature, str]:
    if not hasattr(loaded, "get"):
        raise OpportunityBundleInvalid()
    pointer_sha256 = loaded.get("pointer_sha256")
    publication_signature = loaded.get("publication_signature")
    if type(publication_signature) is not tuple:
        raise OpportunityBundleInvalid()
    generation = historical_opportunity_data_generation(loaded)
    require_stable_historical_pointer_publication_identity(
        pointer_sha256=pointer_sha256,
        publication_signature=publication_signature,
    )
    return pointer_sha256, publication_signature, generation


@lru_cache(maxsize=SERIALIZED_RESPONSE_CACHE_SIZE)
def _build_historical_opportunity_response_cached(
    query_items: tuple[tuple[str, str], ...],
    expected_pointer_sha256: str,
    expected_signature: HistoricalPublicationSignature,
    expected_generation: str,
    expected_contract: str,
    encoding: str,
) -> tuple[bytes, bool]:
    """Build one response only from the exact pre-cache publication identity."""

    if expected_contract != HISTORICAL_OPPORTUNITY_SUMMARY_CONTRACT:
        raise OpportunityBundleInvalid()
    try:
        loaded = load_latest_historical_opportunities()
    except OpportunityBundleUnavailable:
        raise SourceGenerationChanged() from None
    try:
        pointer, signature, generation = (
            _historical_loaded_publication_identity(loaded)
        )
        if pointer != expected_pointer_sha256:
            raise SourceGenerationChanged()
        if signature != expected_signature or generation != expected_generation:
            raise OpportunityBundleInvalid()
        try:
            payload = build_historical_opportunity_payload(
                loaded,
                **_historical_opportunity_arguments(query_items),
            )
        except OpportunityQueryError as error:
            raise PublicClientRequestError(str(error)) from None
        if (
            payload.get("metadata", {}).get("data_generation")
            != expected_generation
        ):
            raise OpportunityBundleInvalid()
        public_payload = _public_payload_projection(payload)
        return encode_json_payload(
            public_payload, "gzip" if encoding == "gzip" else ""
        )
    finally:
        _close_historical_opportunity_view(loaded)


def _build_stable_historical_opportunity_response(
    query_items: tuple[tuple[str, str], ...],
    accepts_gzip: bool,
) -> tuple[bytes, bool]:
    """Probe before cache lookup and again before returning any historical row."""

    encoding = "gzip" if accepts_gzip else "identity"
    for _attempt in range(3):
        try:
            loaded = load_latest_historical_opportunities()
        except OpportunityBundleUnavailable as error:
            try:
                payload = build_unavailable_historical_opportunity_payload(
                    reason=error.reason,
                    **_historical_opportunity_arguments(query_items),
                )
            except OpportunityQueryError as query_error:
                raise PublicClientRequestError(str(query_error)) from None
            try:
                appeared = load_latest_historical_opportunities()
            except OpportunityBundleUnavailable:
                return encode_json_payload(
                    _public_payload_projection(payload),
                    "gzip" if accepts_gzip else "",
                )
            else:
                _close_historical_opportunity_view(appeared)
                continue
        try:
            pointer, signature, generation = (
                _historical_loaded_publication_identity(loaded)
            )
        finally:
            _close_historical_opportunity_view(loaded)
        try:
            response = _build_historical_opportunity_response_cached(
                query_items,
                pointer,
                signature,
                generation,
                HISTORICAL_OPPORTUNITY_SUMMARY_CONTRACT,
                encoding,
            )
        except SourceGenerationChanged:
            continue
        try:
            after = load_latest_historical_opportunities()
        except OpportunityBundleUnavailable:
            continue
        try:
            after_pointer, after_signature, after_generation = (
                _historical_loaded_publication_identity(after)
            )
        finally:
            _close_historical_opportunity_view(after)
        if after_pointer != pointer:
            continue
        if after_signature != signature or after_generation != generation:
            raise OpportunityBundleInvalid()
        return response
    raise SourceGenerationChanged(
        "Historical publication changed repeatedly during response assembly"
    )


def api_source_signature() -> SourceSignature:
    """Return one signature covering every source that can change public facts."""
    database_path = resolve_database_path()
    cex_depth_path = resolve_cex_depth_path()
    paths: list[Path] = []
    if database_path is not None:
        paths.append(database_path)
    else:
        paths.extend(resolve_data_paths())
    for optional_path in (
        resolve_tvl_path(),
        cex_depth_path,
        resolve_cex_depth_history_path(cex_depth_path),
        resolve_dex_depth_path(),
        resolve_cex_execution_cost_path(),
        resolve_dex_execution_cost_path(),
    ):
        if optional_path is not None:
            paths.append(optional_path)
    quality_report_path = resolve_daily_quality_report_path()
    quality_signature = (
        _safe_path_signature(quality_report_path)
        if quality_report_path is not None
        else ()
    )
    lifecycle_signature = _safe_path_signature(
        resolve_cex_instrument_lifecycle_path()
    )
    configured_catalog_signature = (
        _safe_path_signature(CEX_LIFECYCLE_TOKEN_CONFIG_PATH)
        + _safe_path_signature(resolve_runtime_token_registry_path())
    )
    return (
        data_signature(paths)
        + quality_signature
        + lifecycle_signature
        + configured_catalog_signature
        + event_source_signature()
        + route_source_signature()
    )


def api_freshness_bucket() -> int:
    """Refresh wall-clock freshness while retaining short-lived response reuse."""
    return int(time.time() // API_FRESHNESS_CACHE_SECONDS)


def event_response_clock() -> datetime:
    """Own one exact UTC clock for each uncached Event API response."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def opportunity_response_clock() -> datetime:
    """Return the exact completion clock used by the route deadline gate."""
    return datetime.now(timezone.utc)


def _public_payload_projection(value: Any) -> Any:
    """Recursively remove protected evidence and reduce endpoints to origins."""
    if isinstance(value, dict):
        projected = {}
        for key, item in value.items():
            key_text = str(key)
            if (
                key_text in {"error", "errors"}
                or key_text.endswith("_error")
            ):
                continue
            if key_text == "endpoint" or key_text.endswith("_endpoint"):
                projected[key] = sanitize_public_source_endpoint(item)
            else:
                projected[key] = _public_payload_projection(item)
        return projected
    if isinstance(value, list):
        return [_public_payload_projection(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_public_payload_projection(item) for item in value)
    return value


def attach_public_action_capabilities(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Publish only the feature capability needed to render safe actions.

    Fact-level retryability and process-level action availability are separate
    contracts.  Keeping this projection outside the cached fact builders means
    the source payload is never mutated and a client cannot infer availability
    from a retryable quality state alone.
    """
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return payload
    return {
        **payload,
        "metadata": {
            **metadata,
            "public_actions": {
                "fact_refresh_enabled": bool(
                    PUBLIC_ACTION_POLICY.fact_refresh_enabled
                ),
            },
        },
    }


def _validate_public_api_client_query(
    route: str,
    query: dict[str, str],
) -> None:
    """Reject only bounded client-input errors before reading publications."""
    start = parse_iso_date(query.get("start"))
    end = parse_iso_date(query.get("end"))
    if start and end and start > end:
        message = (
            "end must be on or after start"
            if route == "events"
            else "start date must not be after end date"
        )
        raise PublicClientRequestError(message)
    if route == "catalog" and "token" in query and not query["token"].strip():
        raise PublicClientRequestError(
            "Token is required for a single-Token market catalog"
        )
    if route in {"compare", "execution_cost"}:
        if not all(query.get(field) for field in ("token", "market_a", "market_b")):
            raise PublicClientRequestError(
                "token, market_a, and market_b are required"
            )
        if query["market_a"] == query["market_b"]:
            raise PublicClientRequestError(
                "market_a and market_b must be different"
            )
    if route == "quality":
        if not query.get("token"):
            raise PublicClientRequestError("token is required")
        scope = (query.get("scope") or "all").strip().lower()
        if scope not in {"all", "selected"}:
            raise PublicClientRequestError("scope must be all or selected")
        if scope == "selected":
            if not query.get("market_a") or not query.get("market_b"):
                raise PublicClientRequestError(
                    "market_a and market_b are required for selected scope"
                )
            if query["market_a"] == query["market_b"]:
                raise PublicClientRequestError(
                    "market_a and market_b must be different"
                )
    if route == "events":
        lifecycle = (query.get("lifecycle") or "").strip().lower()
        if lifecycle and lifecycle not in LIFECYCLES:
            raise PublicClientRequestError(
                "lifecycle must be one of " + ", ".join(sorted(LIFECYCLES))
            )
        clock_state = (query.get("clock_state") or "").strip().lower()
        if clock_state and clock_state not in EVENT_CLOCK_STATES:
            raise PublicClientRequestError(
                "clock_state must be one of "
                + ", ".join(sorted(EVENT_CLOCK_STATES))
            )


def _build_public_api_payload(
    route: str,
    query_items: tuple[tuple[str, str], ...],
    source_signature: SourceSignature | None = None,
) -> dict[str, Any]:
    query = dict(query_items)
    _validate_public_api_client_query(route, query)
    if route == "catalog":
        if "token" in query:
            payload = build_token_market_catalog(
                query["token"],
                query.get("start"),
                query.get("end"),
                source_signature=source_signature,
            )
        else:
            payload = build_market_catalog(
                source_signature=source_signature,
            )
    elif route == "summary":
        payload = build_market_summary(
            query.get("start"),
            query.get("end"),
            source_signature=source_signature,
        )
    elif route == "market":
        payload = build_market_payload(query.get("start"), query.get("end"))
    elif route == "compare":
        payload = build_market_comparison(
            token_symbol=query.get("token"),
            market_a_id=query.get("market_a"),
            market_b_id=query.get("market_b"),
            start=query.get("start"),
            end=query.get("end"),
            source_signature=source_signature,
        )
    elif route == "execution_cost":
        payload = build_execution_cost_comparison(
            token_symbol=query.get("token"),
            market_a_id=query.get("market_a"),
            market_b_id=query.get("market_b"),
            source_signature=source_signature,
        )
    elif route == "quality":
        payload = build_market_quality(
            token_symbol=query.get("token"),
            scope=query.get("scope"),
            market_a_id=query.get("market_a"),
            market_b_id=query.get("market_b"),
            start=query.get("start"),
            end=query.get("end"),
        )
    elif route == "events":
        payload = build_event_facts(
            token=query.get("token"),
            start=query.get("start"),
            end=query.get("end"),
            lifecycle=query.get("lifecycle"),
            clock_state=query.get("clock_state"),
            clock_as_of=event_response_clock(),
        )
    elif route == "opportunities":
        try:
            payload = build_route_opportunities(
                token=query.get("token"),
                venue=query.get("venue"),
                notional=query.get("notional"),
                opportunity_class=query.get("class"),
                route_type=query.get("route_type"),
                availability=query.get("availability"),
                sort=query.get("sort"),
                direction=query.get("dir"),
            )
        except OpportunityQueryError as error:
            raise PublicClientRequestError(str(error)) from None
    else:
        raise ValueError(f"Unknown public API route: {route}")
    return _public_payload_projection(
        attach_public_action_capabilities(payload)
    )


@lru_cache(maxsize=LARGE_PAYLOAD_CACHE_SIZE)
def _build_summary_core_cached(
    query_items: tuple[tuple[str, str], ...],
    source_signature: SourceSignature,
    _lifecycle_projection_state: str,
) -> dict[str, Any]:
    """Build the compact Summary core once per fact and lifecycle state."""
    return _build_public_api_payload(
        "summary",
        query_items,
        source_signature=source_signature,
    )


def _build_summary_response_payload(
    query_items: tuple[tuple[str, str], ...],
    source_signature: SourceSignature,
    freshness_bucket: int,
) -> dict[str, Any]:
    """Reuse Summary facts while refreshing wall-clock metadata per response."""
    lifecycle_state = summary_lifecycle_projection_state(freshness_bucket)
    core = _build_summary_core_cached(
        query_items,
        source_signature,
        lifecycle_state,
    )
    return attach_freshness_metadata(core)


@lru_cache(maxsize=SERIALIZED_RESPONSE_CACHE_SIZE)
def _build_public_api_response_cached(
    route: str,
    query_items: tuple[tuple[str, str], ...],
    _source_signature: SourceSignature,
    _freshness_bucket: int,
    encoding: str = "gzip",
) -> tuple[bytes, bool]:
    if route == "summary":
        payload = _build_summary_response_payload(
            query_items,
            _source_signature,
            _freshness_bucket,
        )
    else:
        payload = _build_public_api_payload(
            route,
            query_items,
            source_signature=_source_signature,
        )
    if api_source_signature() != _source_signature:
        raise SourceGenerationChanged
    return encode_json_payload(payload, "gzip" if encoding == "gzip" else "")


def _summary_warmup_timestamp() -> str:
    """Return the public warmup clock in canonical UTC form."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _summary_warmup_source_generation_token() -> str | None:
    """Fingerprint the active source generation without exposing its paths."""
    try:
        signature = api_source_signature()
    except Exception:
        return None
    return hashlib.sha256(repr(signature).encode("utf-8")).hexdigest()


def _summary_warmup_generation(response: tuple[bytes, bool]) -> str | None:
    """Read only the bounded public generation from a completed Summary."""
    body, compressed = response
    try:
        raw = gzip.decompress(body) if compressed else body
        payload = json.loads(raw)
        generation = payload.get("metadata", {}).get("data_generation")
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if isinstance(generation, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", generation):
        return generation
    return None


def _begin_summary_warmup() -> int:
    """Publish a new in-process warmup attempt before its Summary build starts."""
    global _SUMMARY_WARMUP_STATE, _SUMMARY_WARMUP_RUN_ID
    started_at = _summary_warmup_timestamp()
    started_monotonic = time.perf_counter()
    source_generation_token = _summary_warmup_source_generation_token()
    with SUMMARY_WARMUP_STATE_LOCK:
        _SUMMARY_WARMUP_RUN_ID += 1
        run_id = _SUMMARY_WARMUP_RUN_ID
        _SUMMARY_WARMUP_STATE = {
            "run_id": run_id,
            "status": "warming",
            "generation": None,
            "source_generation_token": source_generation_token,
            "started_at": started_at,
            "finished_at": None,
            "elapsed_ms": None,
            "started_monotonic": started_monotonic,
        }
        return run_id


def _finish_summary_warmup(
    run_id: int,
    *,
    status: str,
    generation: str | None,
) -> None:
    """Complete only the newest warmup so an older worker cannot regress health."""
    global _SUMMARY_WARMUP_STATE
    finished_at = _summary_warmup_timestamp()
    current_source_generation_token = _summary_warmup_source_generation_token()
    with SUMMARY_WARMUP_STATE_LOCK:
        state = _SUMMARY_WARMUP_STATE
        if not isinstance(state, dict) or state.get("run_id") != run_id:
            return
        if status == "ready" and (
            state.get("source_generation_token") is None
            or state.get("source_generation_token")
            != current_source_generation_token
        ):
            status = "failed"
            generation = None
        elapsed = max(
            0,
            int(round((time.perf_counter() - state["started_monotonic"]) * 1000)),
        )
        _SUMMARY_WARMUP_STATE = {
            "run_id": run_id,
            "status": status,
            "generation": generation,
            "source_generation_token": current_source_generation_token,
            "started_at": state["started_at"],
            "finished_at": finished_at,
            "elapsed_ms": elapsed,
            "started_monotonic": state["started_monotonic"],
        }


def summary_warmup_health() -> dict[str, Any]:
    """Return the bounded, exception-free readiness metadata for `/health`."""
    with SUMMARY_WARMUP_STATE_LOCK:
        state = _SUMMARY_WARMUP_STATE
        if not isinstance(state, dict):
            return {
                "status": "warming",
                "generation": None,
                "started_at": None,
                "finished_at": None,
                "elapsed_ms": None,
            }
        result = {
            field: state.get(field)
            for field in (
                "status",
                "generation",
                "started_at",
                "finished_at",
                "elapsed_ms",
            )
        }
        source_generation_token = state.get("source_generation_token")
    if result["status"] == "ready" and (
        source_generation_token is None
        or source_generation_token != _summary_warmup_source_generation_token()
    ):
        result["status"] = "failed"
        result["generation"] = None
    return result


def warm_default_market_summary() -> None:
    """Populate the default Summary response through the normal cache path."""
    run_id = _begin_summary_warmup()
    try:
        response = build_public_api_response(
            "summary",
            (),
            True,
        )
    except Exception as error:
        _finish_summary_warmup(run_id, status="failed", generation=None)
        print(f"Default summary warmup failed: {type(error).__name__}")
    else:
        _finish_summary_warmup(
            run_id,
            status="ready",
            generation=_summary_warmup_generation(response),
        )


def start_default_market_summary_warmup() -> threading.Thread:
    """Start the best-effort Summary warmup without delaying HTTP serving."""
    thread = threading.Thread(
        target=warm_default_market_summary,
        name="market-summary-warmup",
        daemon=True,
    )
    thread.start()
    return thread


def clear_runtime_caches() -> None:
    """Drop every payload derived from a previous published source generation."""
    global _SOURCE_CACHE_GENERATION, _PUBLIC_RESPONSE_CACHE_GENERATION
    with SOURCE_CACHE_GENERATION_LOCK:
        for cached_builder in (
            _load_tvl_snapshot_cached,
            _load_cex_depth_snapshot_cached,
            _load_dex_depth_snapshot_cached,
            _load_execution_cost_snapshot_cached,
            _load_daily_quality_report_cached,
            _load_cex_historical_lineage_cached,
            _build_market_payload_cached,
            _build_database_payload_cached,
            _build_enriched_payload_cached,
            _build_lifecycle_payload_cached,
            _build_market_catalog_cached,
            _build_summary_core_cached,
            _build_public_api_response_cached,
            _build_historical_opportunity_response_cached,
        ):
            cached_builder.cache_clear()
        _SOURCE_CACHE_GENERATION = None
        _PUBLIC_RESPONSE_CACHE_GENERATION = None


def ensure_source_cache_generation(
    source_signature: SourceSignature,
) -> None:
    """Retain only one complete source generation instead of 32 large copies."""
    global _SOURCE_CACHE_GENERATION, _PUBLIC_RESPONSE_CACHE_GENERATION
    with SOURCE_CACHE_GENERATION_LOCK:
        if _SOURCE_CACHE_GENERATION is None:
            _SOURCE_CACHE_GENERATION = source_signature
            return
        if _SOURCE_CACHE_GENERATION == source_signature:
            return
        for cached_builder in (
            _load_tvl_snapshot_cached,
            _load_cex_depth_snapshot_cached,
            _load_dex_depth_snapshot_cached,
            _load_execution_cost_snapshot_cached,
            _load_cex_historical_lineage_cached,
            _build_market_payload_cached,
            _build_database_payload_cached,
            _build_enriched_payload_cached,
            _build_lifecycle_payload_cached,
            _build_market_catalog_cached,
            _build_summary_core_cached,
            _build_public_api_response_cached,
        ):
            cached_builder.cache_clear()
        _SOURCE_CACHE_GENERATION = source_signature
        _PUBLIC_RESPONSE_CACHE_GENERATION = None


def ensure_public_response_cache_generation(
    source_signature: SourceSignature,
    freshness_bucket: int,
) -> None:
    """Bound serialized responses to the active source and freshness minute."""
    global _PUBLIC_RESPONSE_CACHE_GENERATION
    generation = (source_signature, freshness_bucket)
    with SOURCE_CACHE_GENERATION_LOCK:
        if _PUBLIC_RESPONSE_CACHE_GENERATION == generation:
            return
        _build_public_api_response_cached.cache_clear()
        _PUBLIC_RESPONSE_CACHE_GENERATION = generation


def _opportunity_payload_requires_reprojection(
    payload: dict[str, Any],
    completed_at: datetime,
) -> bool:
    """Recheck every publicly available route at response completion time."""
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise OpportunityBundleInvalid()
    next_deadline = metadata.get("next_freshness_deadline_at")
    deadline_exclusive = metadata.get("next_freshness_deadline_exclusive")
    if next_deadline is not None:
        if not isinstance(next_deadline, str) or not next_deadline:
            raise OpportunityBundleInvalid()
        try:
            parsed_deadline = parse_rfc3339_utc(next_deadline)
        except (TypeError, ValueError):
            raise OpportunityBundleInvalid() from None
        if deadline_exclusive not in (None, True, False):
            raise OpportunityBundleInvalid()
        # Exact 120-second evidence remains current.  Reprojection begins only
        # after the unfiltered inventory crosses that inclusive boundary.
        # Cost valid_until is exclusive and must reproject at equality.
        if completed_at > parsed_deadline or (
            deadline_exclusive is True and completed_at == parsed_deadline
        ):
            return True
    elif deadline_exclusive is not None:
        raise OpportunityBundleInvalid()
    routes = payload.get("routes")
    if not isinstance(routes, list):
        raise OpportunityBundleInvalid()
    for route in routes:
        if not isinstance(route, dict):
            raise OpportunityBundleInvalid()
        availability = route.get("availability")
        if not isinstance(availability, dict):
            raise OpportunityBundleInvalid()
        status = availability.get("status")
        if status == "unavailable":
            continue
        if status != "available":
            raise OpportunityBundleInvalid()
        timestamps = route.get("leg_timestamps")
        if not isinstance(timestamps, dict):
            raise OpportunityBundleInvalid()
        try:
            freshness = route_opportunity_freshness(
                timestamps.get("buy"),
                timestamps.get("sell"),
                now=completed_at,
            )
        except (TypeError, ValueError):
            raise OpportunityBundleInvalid() from None
        if freshness.get("status") != "current":
            return True
    return False


def _build_deadline_safe_opportunity_response(
    query_items: tuple[tuple[str, str], ...],
    source_signature: SourceSignature,
    accepts_gzip: bool,
) -> tuple[bytes, bool]:
    """Serialize only a route projection that is current when assembly ends."""
    for _attempt in range(OPPORTUNITY_RESPONSE_MAX_PROJECTIONS):
        payload = _build_public_api_payload(
            "opportunities",
            query_items,
            source_signature=source_signature,
        )
        response = encode_json_payload(
            payload,
            "gzip" if accepts_gzip else "",
        )
        if api_source_signature() != source_signature:
            raise SourceGenerationChanged
        completed_at = opportunity_response_clock()
        if not _opportunity_payload_requires_reprojection(
            payload,
            completed_at,
        ):
            return response
    raise OpportunityResponseUnstable(
        "Route opportunity deadlines changed repeatedly during response assembly"
    )


def build_public_api_response(
    route: str,
    query_items: tuple[tuple[str, str], ...],
    accepts_gzip: bool,
) -> tuple[bytes, bool]:
    """Single-flight one route without blocking independent public endpoints."""
    with PUBLIC_API_ROUTE_LOCKS[route]:
        if route == "opportunities_historical":
            return _build_stable_historical_opportunity_response(
                query_items, accepts_gzip
            )
        for _attempt in range(3):
            source_signature = api_source_signature()
            freshness_bucket = api_freshness_bucket()
            ensure_source_cache_generation(source_signature)
            ensure_public_response_cache_generation(
                source_signature,
                freshness_bucket,
            )
            try:
                # These routes own second-level wall clocks.  Reusing the
                # minute-bucket serialized cache could retain a route past
                # its 120-second deadline or misclassify an Event clock.
                if route == "opportunities":
                    return _build_deadline_safe_opportunity_response(
                        query_items,
                        source_signature,
                        accepts_gzip,
                    )
                if route == "events":
                    payload = _build_public_api_payload(
                        route,
                        query_items,
                        source_signature=source_signature,
                    )
                    if api_source_signature() != source_signature:
                        raise SourceGenerationChanged
                    response = encode_json_payload(
                        payload,
                        "gzip" if accepts_gzip else "",
                    )
                elif route == "summary":
                    response = _build_public_api_response_cached(
                        route,
                        query_items,
                        source_signature,
                        freshness_bucket,
                        "gzip" if accepts_gzip else "identity",
                    )
                elif accepts_gzip:
                    response = _build_public_api_response_cached(
                        route,
                        query_items,
                        source_signature,
                        freshness_bucket,
                    )
                else:
                    payload = _build_public_api_payload(
                        route,
                        query_items,
                        source_signature=source_signature,
                    )
                    if api_source_signature() != source_signature:
                        raise SourceGenerationChanged
                    response = encode_json_payload(payload, "")
            except SourceGenerationChanged:
                continue
            if api_source_signature() == source_signature:
                return response
        raise SourceGenerationChanged(
            "Published fact sources changed repeatedly during response assembly"
        )


def public_api_query_items(
    route: str,
    query: dict[str, list[str]],
) -> tuple[tuple[str, str], ...]:
    """Normalize only supported fields so irrelevant query keys cannot fill the cache."""
    fields = PUBLIC_API_QUERY_FIELDS.get(route)
    if fields is None:
        raise ValueError(f"Unknown public API route: {route}")
    if route in {"opportunities", "opportunities_historical"}:
        raw = {
            name: (
                query[name][0]
                if query.get(name) and query[name][0] is not None
                else None
            )
            for name in fields
        }
        try:
            normalized = normalize_opportunity_filters(
                token=raw["token"] if "token" in query else None,
                venue=raw["venue"] if "venue" in query else None,
                notional_usd=(
                    raw["notional"] if "notional" in query else None
                ),
                opportunity_class=(
                    raw["class"] if "class" in query else None
                ),
                route_type=(
                    raw["route_type"] if "route_type" in query else None
                ),
                availability=(
                    raw["availability"]
                    if "availability" in query
                    else None
                ),
                sort=raw["sort"] if "sort" in query else None,
                direction=raw["dir"] if "dir" in query else None,
            )
        except OpportunityQueryError as error:
            raise PublicClientRequestError(str(error)) from None
        normalized_by_query_name = {
            "token": normalized["token"],
            "venue": normalized["venue"],
            "notional": normalized["notional_usd"],
            "class": normalized["opportunity_class"],
            "route_type": normalized["route_type"],
            "availability": normalized["availability"],
            "sort": normalized["sort"],
            "dir": normalized["direction"],
        }
        return tuple(
            (name, str(normalized_by_query_name[name]))
            for name in fields
            if (
                normalized_by_query_name[name] is not None
                and (
                    route == "opportunities_historical"
                    or name in query
                )
            )
        )
    if route == "catalog" and "token" not in query:
        fields = ()
    items = []
    for name in fields:
        if not query.get(name) or query[name][0] is None:
            continue
        value = query[name][0]
        if route in {"catalog", "events"} and name == "token":
            value = value.strip().upper()
        elif route == "events" and name in {"lifecycle", "clock_state"}:
            value = value.strip().lower()
        elif route == "events":
            value = value.strip()
        items.append((name, value))
    return tuple(items)


def is_loopback_host(host: str) -> bool:
    """Accept only an explicit loopback bind for the write-capable admin surface."""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def write_surface_enabled() -> bool:
    """Any enabled mutation surface requires the loopback HTTPS boundary."""
    return bool(
        ADMIN_SERVICE.available
        or PUBLIC_ACTION_POLICY.add_token_enabled
        or PUBLIC_ACTION_POLICY.quality_retry_enabled
        or PUBLIC_ACTION_POLICY.fact_refresh_enabled
    )


def is_spa_shell_path(path: str) -> bool:
    """Return true only for the dashboard's declared client-side routes."""
    decoded = unquote(urlparse(path).path)
    if decoded in {
        "/screener",
        "/screener/",
        "/opportunities",
        "/opportunities/",
        "/methodology",
        "/methodology/",
    }:
        return True
    token_match = SPA_TOKEN_ROUTE.fullmatch(decoded)
    if token_match:
        page = decoded.rstrip("/").rsplit("/", 1)[-1]
        return page in SPA_TOKEN_PAGES
    return SPA_METHODOLOGY_ROUTE.fullmatch(decoded) is not None


def is_admin_surface_path(path: str) -> bool:
    normalized = (
        "/" + posixpath.normpath(unquote(path)).lstrip("/")
    ).casefold()
    return (
        normalized in ADMIN_STATIC_PATHS
        or normalized == "/admin"
        or normalized == "/api/admin"
        or normalized.startswith("/api/admin/")
    )


class MarketMonitorHandler(SimpleHTTPRequestHandler):
    server_version = "CexDexMarketMonitor"
    sys_version = ""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._cache_control: str | None = None
        super().__init__(*args, directory=str(STATIC_ROOT), **kwargs)

    def send_json(
        self,
        payload: Any,
        status: HTTPStatus = HTTPStatus.OK,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body, compressed = encode_json_payload(
            payload,
            self.headers.get("Accept-Encoding", ""),
        )
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cache_control = "no-store"
        self.send_header("Vary", "Accept-Encoding")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def send_encoded_json(self, body: bytes, compressed: bool) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cache_control = "no-store"
        self.send_header("Vary", "Accept-Encoding")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        self.end_headers()
        self.wfile.write(body)

    def send_public_api(self, route: str, query: dict[str, list[str]]) -> None:
        query_items = public_api_query_items(route, query)
        body, compressed = build_public_api_response(
            route,
            query_items,
            "gzip" in self.headers.get("Accept-Encoding", "").lower(),
        )
        self.send_encoded_json(body, compressed)

    def send_public_data_validation_error(self) -> None:
        """Return one fixed response for protected publication failures."""
        self.send_json(
            {
                "code": PUBLIC_DATA_VALIDATION_ERROR_CODE,
                "message": PUBLIC_DATA_VALIDATION_ERROR_MESSAGE,
            },
            HTTPStatus.SERVICE_UNAVAILABLE,
        )

    def send_opportunity_data_validation_error(self) -> None:
        """Return one fixed response for protected route publication failures."""

        self.send_json(
            {
                "code": OPPORTUNITY_DATA_VALIDATION_ERROR_CODE,
                "message": OPPORTUNITY_DATA_VALIDATION_ERROR_MESSAGE,
            },
            HTTPStatus.SERVICE_UNAVAILABLE,
        )

    def read_json(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length < 0 or content_length > 16_384:
            raise ValueError("Request body is too large")
        payload = json.loads(self.rfile.read(content_length) or b"{}")
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def send_public_action_error(self, error: PublicActionError) -> None:
        headers = None
        if error.retry_after_seconds is not None:
            headers = {"Retry-After": str(error.retry_after_seconds)}
        self.send_json(
            error.as_dict(),
            error.status,
            extra_headers=headers,
        )

    def public_client_address(self) -> str:
        """Use the direct peer unless an explicitly trusted proxy overwrites IP."""
        peer_address = str(self.client_address[0])
        if (
            not TRUST_LOOPBACK_PROXY_CLIENT_IP
            or not is_loopback_host(peer_address)
        ):
            return peer_address
        forwarded_address = self.headers.get("X-Real-IP", "").strip()
        try:
            return str(ipaddress.ip_address(forwarded_address))
        except ValueError:
            return peer_address

    def send_public_token_error(self, error: BaseException) -> None:
        """Map known onboarding failures without exposing server internals."""
        error_code = str(getattr(error, "code", "invalid_token_request"))
        allowed_codes = {
            "identity_changed",
            "identity_conflict",
            "identity_mismatch",
            "invalid_chain",
            "invalid_contract_address",
            "invalid_token_request",
            "invalid_token_symbol",
            "no_usable_pool",
            "pool_token_mismatch",
            "source_invalid_response",
            "source_rate_limited",
            "source_unavailable",
            "symbol_collision",
            "token_not_found",
        }
        if error_code not in allowed_codes:
            self.send_public_action_error(
                PublicActionError(
                    "token_onboarding_unavailable",
                    "Token onboarding is temporarily unavailable",
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=False,
                )
            )
            return
        retryable = bool(getattr(error, "retryable", False))
        status = (
            HTTPStatus.NOT_FOUND
            if error_code == "token_not_found"
            else HTTPStatus.CONFLICT
            if error_code in {"symbol_collision", "identity_conflict"}
            else HTTPStatus.SERVICE_UNAVAILABLE
            if error_code
            in {
                "source_rate_limited",
                "source_unavailable",
                "source_invalid_response",
            }
            else HTTPStatus.UNPROCESSABLE_ENTITY
            if error_code
            in {
                "no_usable_pool",
                "pool_token_mismatch",
                "identity_mismatch",
                "identity_changed",
            }
            else HTTPStatus.BAD_REQUEST
        )
        response: dict[str, Any] = {
            "error": str(error),
            "error_code": error_code,
            "retryable": retryable,
        }
        self.send_json(response, status)

    @staticmethod
    def validate_public_retry_request(
        payload: dict[str, Any],
    ) -> dict[str, str]:
        request = require_exact_string_fields(
            payload,
            {
                "token_symbol": 32,
                "start_date": 10,
                "end_date": 10,
                "queue_type": 32,
            },
        )
        try:
            start = date.fromisoformat(request["start_date"]).isoformat()
            end = date.fromisoformat(request["end_date"]).isoformat()
        except ValueError as error:
            raise PublicActionError(
                "invalid_public_action_request",
                "start_date and end_date must be ISO calendar dates",
            ) from error
        if start != request["start_date"] or end != request["end_date"]:
            raise PublicActionError(
                "invalid_public_action_request",
                "start_date and end_date must use canonical YYYY-MM-DD format",
            )
        if request["queue_type"] not in {
            "latest_completed_day",
            "historical_gap",
        }:
            raise PublicActionError(
                "invalid_public_action_request",
                "queue_type is not an approved retry queue",
            )
        request["token_symbol"] = request["token_symbol"].upper()
        return request

    def handle_public_quality_retryable(self, parsed: Any) -> None:
        if not PUBLIC_ACTION_POLICY.enabled_for_path(parsed.path):
            self.send_public_action_error(PUBLIC_ACTION_POLICY.disabled_error())
            return
        if parsed.query:
            self.send_public_action_error(
                PublicActionError(
                    "invalid_public_action_request",
                    "This endpoint does not accept query parameters",
                )
            )
            return
        try:
            with PUBLIC_ACTION_POLICY.permit(
                "quality_retryable",
                self.public_client_address(),
            ):
                windows = ADMIN_SERVICE.retryable_windows(required=True)
                public_windows = [
                    public_retry_window(window) for window in windows
                ]
        except PublicActionError as error:
            self.send_public_action_error(error)
            return
        except (KeyError, OSError, TypeError, ValueError):
            self.send_public_action_error(
                PublicActionError(
                    "quality_retry_unavailable",
                    "The current quality retry queue is unavailable",
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=True,
                )
            )
            return
        self.send_json(
            {
                "windows": public_windows,
                "count": len(public_windows),
            }
        )

    def handle_public_job_status(self, parsed: Any) -> None:
        route_match = PUBLIC_JOB_STATUS_ROUTE.fullmatch(parsed.path)
        if route_match is None:
            self.send_public_action_error(
                PublicActionError(
                    "public_job_not_found",
                    "Public action job was not found",
                    status=HTTPStatus.NOT_FOUND,
                )
            )
            return
        if not (
            PUBLIC_ACTION_POLICY.add_token_enabled
            or PUBLIC_ACTION_POLICY.quality_retry_enabled
            or PUBLIC_ACTION_POLICY.fact_refresh_enabled
        ):
            self.send_public_action_error(PUBLIC_ACTION_POLICY.disabled_error())
            return
        if parsed.query:
            self.send_public_action_error(
                PublicActionError(
                    "invalid_public_action_request",
                    "This endpoint does not accept query parameters",
                )
            )
            return
        try:
            with PUBLIC_ACTION_POLICY.permit(
                "job_status",
                self.public_client_address(),
            ):
                job = ADMIN_SERVICE.get_job(route_match.group(1))
        except PublicActionError as error:
            self.send_public_action_error(error)
            return
        actor = job.get("requested_by") if job else None
        actor_enabled = (
            actor == PUBLIC_ADD_TOKEN_ACTOR
            and PUBLIC_ACTION_POLICY.add_token_enabled
        ) or (
            actor == PUBLIC_QUALITY_RETRY_ACTOR
            and PUBLIC_ACTION_POLICY.quality_retry_enabled
        ) or (
            actor == PUBLIC_FACT_REFRESH_ACTOR
            and PUBLIC_ACTION_POLICY.fact_refresh_enabled
        )
        if not job or not actor_enabled:
            self.send_public_action_error(
                PublicActionError(
                    "public_job_not_found",
                    "Public action job was not found",
                    status=HTTPStatus.NOT_FOUND,
                )
            )
            return
        self.send_json(public_job(job))

    def handle_public_action_post(
        self,
        path: str,
        payload: dict[str, Any],
    ) -> None:
        if path == PUBLIC_FACT_REFRESH_PATH:
            try:
                request = require_exact_string_fields(
                    payload,
                    {
                        "token_symbol": 32,
                        "market_id": 512,
                        "fact_type": 16,
                    },
                )
                request["token_symbol"] = request["token_symbol"].upper()
                request["fact_type"] = request["fact_type"].lower()
                if request["fact_type"] not in {"tvl", "depth"}:
                    raise PublicActionError(
                        "invalid_public_action_request",
                        "fact_type must be tvl or depth",
                    )
                quality = build_market_quality(request["token_symbol"])
                market = next(
                    (
                        item
                        for item in quality["markets"]
                        if item["market_id"] == request["market_id"]
                    ),
                    None,
                )
                fact = market and market.get("facts", {}).get(
                    request["fact_type"]
                )
                if not market or not fact:
                    raise PublicActionError(
                        "fact_refresh_not_found",
                        "The selected market fact is not cataloged",
                        status=HTTPStatus.UNPROCESSABLE_ENTITY,
                    )
                if fact.get("retryable") is not True:
                    raise PublicActionError(
                        "fact_refresh_not_retryable",
                        "The selected N/A is structural or already observed and cannot be refreshed",
                        status=HTTPStatus.UNPROCESSABLE_ENTITY,
                    )
                with PUBLIC_ACTION_POLICY.permit(
                    "fact_refresh",
                    self.public_client_address(),
                    service=ADMIN_SERVICE,
                ):
                    job = ADMIN_SERVICE.create_job(
                        {
                            **request,
                            "job_type": "snapshot_refresh",
                        },
                        PUBLIC_FACT_REFRESH_ACTOR,
                    )
            except PublicActionError as error:
                self.send_public_action_error(error)
                return
            except AdminJobBusyError:
                self.send_public_action_error(
                    PublicActionError(
                        "refresh_job_busy",
                        "Another collection job is already queued or running",
                        status=HTTPStatus.CONFLICT,
                        retryable=True,
                        retry_after_seconds=30,
                    )
                )
                return
            except (
                DepthExecutionCohortError,
                KeyError,
                OSError,
                TypeError,
                ValueError,
            ):
                self.send_public_action_error(
                    PublicActionError(
                        "fact_refresh_unavailable",
                        "The requested fact refresh is temporarily unavailable",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            self.send_json(public_job(job), HTTPStatus.ACCEPTED)
            return

        if path == PUBLIC_TOKEN_RESOLVE_PATH:
            try:
                request = require_exact_string_fields(
                    payload,
                    {"chain": 32, "contract_address": 128},
                )
                with PUBLIC_ACTION_POLICY.permit(
                    "token_resolve",
                    self.public_client_address(),
                ):
                    candidate = ADMIN_SERVICE.resolve_token(
                        request["chain"],
                        request["contract_address"],
                    )
            except PublicActionError as error:
                self.send_public_action_error(error)
                return
            except ValueError as error:
                self.send_public_token_error(error)
                return
            except (KeyError, OSError, TypeError):
                self.send_public_action_error(
                    PublicActionError(
                        "token_onboarding_unavailable",
                        "Token resolution is temporarily unavailable",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            try:
                response = public_token_candidate(candidate)
            except (TypeError, ValueError):
                self.send_public_action_error(
                    PublicActionError(
                        "token_onboarding_unavailable",
                        "Resolved Token preview is temporarily unavailable",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            self.send_json(response)
            return

        if path == PUBLIC_TOKEN_ADD_PATH:
            try:
                request = require_exact_string_fields(
                    payload,
                    {
                        "chain": 32,
                        "contract_address": 128,
                        "expected_token_symbol": 32,
                    },
                )
                with PUBLIC_ACTION_POLICY.permit(
                    "token_add",
                    self.public_client_address(),
                    service=ADMIN_SERVICE,
                ):
                    job = ADMIN_SERVICE.create_onboarding_job(
                        {
                            **request,
                            "history_days": PUBLIC_TOKEN_HISTORY_DAYS,
                        },
                        PUBLIC_ADD_TOKEN_ACTOR,
                    )
            except PublicActionError as error:
                self.send_public_action_error(error)
                return
            except AdminJobBusyError:
                self.send_public_action_error(
                    PublicActionError(
                        "refresh_job_busy",
                        "Another collection job is already queued or running",
                        status=HTTPStatus.CONFLICT,
                        retryable=True,
                        retry_after_seconds=30,
                    )
                )
                return
            except AdminWorkerStartError:
                self.send_public_action_error(
                    PublicActionError(
                        "public_worker_start_failed",
                        "The Token onboarding worker could not be started",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            except RuntimeError:
                self.send_public_action_error(
                    PublicActionError(
                        "token_onboarding_unavailable",
                        "Token onboarding is temporarily unavailable",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            except ValueError as error:
                self.send_public_token_error(error)
                return
            except (KeyError, OSError, TypeError):
                self.send_public_action_error(
                    PublicActionError(
                        "token_onboarding_unavailable",
                        "Token onboarding is temporarily unavailable",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            self.send_json(
                public_job(job),
                (
                    HTTPStatus.OK
                    if job.get("status") == "succeeded"
                    else HTTPStatus.ACCEPTED
                ),
            )
            return

        if path == PUBLIC_QUALITY_RETRY_PATH:
            try:
                request = self.validate_public_retry_request(payload)
                with PUBLIC_ACTION_POLICY.permit(
                    "quality_retry",
                    self.public_client_address(),
                    service=ADMIN_SERVICE,
                ):
                    job = ADMIN_SERVICE.create_job(
                        {
                            **request,
                            "job_type": "retry_failed",
                        },
                        PUBLIC_QUALITY_RETRY_ACTOR,
                    )
            except PublicActionError as error:
                self.send_public_action_error(error)
                return
            except AdminJobBusyError:
                self.send_public_action_error(
                    PublicActionError(
                        "refresh_job_busy",
                        "Another collection job is already queued or running",
                        status=HTTPStatus.CONFLICT,
                        retryable=True,
                        retry_after_seconds=30,
                    )
                )
                return
            except AdminWorkerStartError:
                self.send_public_action_error(
                    PublicActionError(
                        "public_worker_start_failed",
                        "The quality retry worker could not be started",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            except RuntimeError:
                self.send_public_action_error(
                    PublicActionError(
                        "quality_retry_unavailable",
                        "The quality retry worker is temporarily unavailable",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            except (KeyError, TypeError, ValueError) as error:
                error_code = str(
                    getattr(error, "code", "quality_retry_unavailable")
                )
                self.send_public_action_error(
                    PublicActionError(
                        error_code,
                        (
                            "The selected retry window is not approved by the "
                            "current quality report"
                            if error_code == "retry_window_not_approved"
                            else "The current quality retry queue is unavailable"
                        ),
                        status=(
                            HTTPStatus.UNPROCESSABLE_ENTITY
                            if error_code == "retry_window_not_approved"
                            else HTTPStatus.SERVICE_UNAVAILABLE
                        ),
                        retryable=bool(getattr(error, "retryable", True)),
                    )
                )
                return
            except OSError:
                self.send_public_action_error(
                    PublicActionError(
                        "quality_retry_unavailable",
                        "The quality retry job could not be persisted",
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                )
                return
            self.send_json(public_job(job), HTTPStatus.ACCEPTED)
            return

        self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)

    def admin_session_token(self) -> str | None:
        cookie = SimpleCookie()
        cookie.load(self.headers.get("Cookie", ""))
        morsel = cookie.get("admin_session")
        return morsel.value if morsel else None

    def admin_surface_available(self) -> bool:
        bound_host = str(self.server.server_address[0])
        return ADMIN_SERVICE.available and is_loopback_host(bound_host)

    def require_admin(self, *, csrf: bool = False) -> tuple[str, dict[str, Any]] | None:
        if not self.admin_surface_available():
            self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            return None
        if ADMIN_SERVICE.open_mode:
            return "", {
                "username": "open-admin",
                "csrf_token": "",
            }
        session_token = self.admin_session_token()
        session = ADMIN_SERVICE.get_session(session_token)
        if not session:
            self.send_json({"error": "Administrator authentication required"}, HTTPStatus.UNAUTHORIZED)
            return None
        if csrf and not hmac.compare_digest(
            self.headers.get("X-CSRF-Token", ""),
            session["csrf_token"],
        ):
            self.send_json({"error": "Invalid CSRF token"}, HTTPStatus.FORBIDDEN)
            return None
        return session_token or "", session

    def admin_cookie(self, session_token: str, *, clear: bool = False) -> str:
        secure = os.environ.get("ADMIN_COOKIE_SECURE", "").lower() in {"1", "true", "yes"}
        parts = [
            f"admin_session={'' if clear else session_token}",
            "Path=/api/admin",
            "HttpOnly",
            "SameSite=Strict",
            f"Max-Age={0 if clear else 8 * 60 * 60}",
        ]
        if secure:
            parts.append("Secure")
        return "; ".join(parts)

    def end_headers(self) -> None:
        self.send_header("Cache-Control", self._cache_control or "no-cache")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'",
        )
        super().end_headers()

    def list_directory(self, path: str) -> None:
        self.send_error(HTTPStatus.NOT_FOUND)
        return None

    def translate_path(self, path: str) -> str:
        request_path = urlparse(path).path
        if request_path in VENDOR_FILES:
            return str(VENDOR_FILES[request_path])
        if is_spa_shell_path(request_path):
            return str(STATIC_ROOT / "index.html")
        return super().translate_path(path)

    def send_head(self):
        """Render HTML asset fingerprints at request time; stream other files normally."""

        if (
            static_representation(self.path) is not None
            and exact_static_version(self.path)
        ):
            return self.send_static_representation(self.command == "HEAD")

        translated = Path(self.translate_path(self.path))
        try:
            is_static_html = (
                translated.suffix == ".html"
                and translated.resolve().is_relative_to(STATIC_ROOT.resolve())
            )
        except AttributeError:  # Python 3.8 compatibility.
            try:
                translated.resolve().relative_to(STATIC_ROOT.resolve())
                is_static_html = translated.suffix == ".html"
            except ValueError:
                is_static_html = False
        if not is_static_html or not translated.is_file():
            return super().send_head()

        body = render_versioned_html(translated).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header(
            "Last-Modified",
            self.date_time_string(translated.stat().st_mtime),
        )
        self.end_headers()
        return io.BytesIO(body)

    def send_static_representation(self, head_only: bool) -> BinaryIO | None:
        """Send the negotiated immutable representation for an exact asset URL."""

        representation = static_representation(self.path)
        if representation is None:  # pragma: no cover - guarded by send_head.
            return None
        compressed = client_accepts_gzip(self.headers.get("Accept-Encoding", ""))
        body = representation.gzip if compressed else representation.raw
        self._cache_control = IMMUTABLE_STATIC_CACHE_CONTROL
        modified_since = self.headers.get("If-Modified-Since")
        if modified_since:
            try:
                request_time = parsedate_to_datetime(modified_since)
                representation_time = parsedate_to_datetime(
                    representation.last_modified
                )
            except (IndexError, OverflowError, TypeError, ValueError):
                pass
            else:
                if (
                    request_time.tzinfo is not None
                    and representation_time <= request_time
                ):
                    self.send_response(HTTPStatus.NOT_MODIFIED)
                    self.send_header("Last-Modified", representation.last_modified)
                    self.send_header("Vary", "Accept-Encoding")
                    self.end_headers()
                    return None
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", representation.content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Last-Modified", representation.last_modified)
        self.send_header("Vary", "Accept-Encoding")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        self.end_headers()
        return None if head_only else io.BytesIO(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if is_admin_surface_path(parsed.path) and not self.admin_surface_available():
            if parsed.path.startswith("/api/"):
                self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
            return
        if parsed.path.startswith(PUBLIC_JOB_STATUS_PREFIX):
            self.handle_public_job_status(parsed)
            return
        if parsed.path in PUBLIC_ACTION_PATHS:
            if parsed.path == PUBLIC_QUALITY_RETRYABLE_PATH:
                self.handle_public_quality_retryable(parsed)
                return
            if not PUBLIC_ACTION_POLICY.enabled_for_path(parsed.path):
                self.send_public_action_error(
                    PUBLIC_ACTION_POLICY.disabled_error()
                )
                return
            self.send_public_action_error(
                PublicActionError(
                    "public_action_method_not_allowed",
                    "This public action requires POST",
                    status=HTTPStatus.METHOD_NOT_ALLOWED,
                )
            )
            return
        if parsed.path == "/api/markets/opportunities/historical":
            query = parse_qs(parsed.query, keep_blank_values=True)
            try:
                self.send_public_api("opportunities_historical", query)
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except (OpportunityBundleInvalid, SourceGenerationChanged):
                self.send_opportunity_data_validation_error()
            except (FileNotFoundError, TypeError, ValueError):
                self.send_opportunity_data_validation_error()
            return
        if parsed.path == "/api/markets/opportunities":
            query = parse_qs(parsed.query, keep_blank_values=True)
            try:
                self.send_public_api("opportunities", query)
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except OpportunityBundleInvalid:
                self.send_opportunity_data_validation_error()
            except FileNotFoundError:
                self.send_json(
                    {"error": PUBLIC_DATA_UNAVAILABLE_MESSAGE},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except (TypeError, ValueError):
                self.send_opportunity_data_validation_error()
            return
        if parsed.path == "/api/markets/catalog":
            query = parse_qs(parsed.query, keep_blank_values=True)
            try:
                self.send_public_api("catalog", query)
            except FileNotFoundError:
                self.send_json(
                    {"error": PUBLIC_DATA_UNAVAILABLE_MESSAGE},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except (DepthExecutionCohortError, ValueError):
                self.send_public_data_validation_error()
            return
        if parsed.path == "/api/markets/summary":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("summary", query)
            except FileNotFoundError:
                self.send_json(
                    {"error": PUBLIC_DATA_UNAVAILABLE_MESSAGE},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except (DepthExecutionCohortError, ValueError):
                self.send_public_data_validation_error()
            return
        if parsed.path == "/api/markets/compare":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("compare", query)
            except FileNotFoundError:
                self.send_json(
                    {"error": PUBLIC_DATA_UNAVAILABLE_MESSAGE},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except (DepthExecutionCohortError, ValueError):
                self.send_public_data_validation_error()
            return
        if parsed.path == "/api/markets/execution-cost":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("execution_cost", query)
            except FileNotFoundError:
                self.send_json(
                    {"error": PUBLIC_DATA_UNAVAILABLE_MESSAGE},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except DepthExecutionCohortError:
                self.send_public_data_validation_error()
            except ValueError:
                self.send_public_data_validation_error()
            return
        if parsed.path == "/api/markets/quality":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("quality", query)
            except FileNotFoundError:
                self.send_json(
                    {"error": PUBLIC_DATA_UNAVAILABLE_MESSAGE},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except DepthExecutionCohortError:
                self.send_public_data_validation_error()
            except ValueError:
                self.send_public_data_validation_error()
            return
        if parsed.path == "/api/markets/events":
            query = parse_qs(parsed.query, keep_blank_values=True)
            try:
                self.send_public_api("events", query)
            except EventBundleError:
                self.send_json(
                    {
                        "error": "Event Fact bundle failed validation",
                        "reason": "event_bundle_validation_failed",
                    },
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except FileNotFoundError:
                self.send_json(
                    {"error": PUBLIC_DATA_UNAVAILABLE_MESSAGE},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except ValueError:
                self.send_public_data_validation_error()
            return
        if parsed.path == "/api/market":
            query = parse_qs(parsed.query)
            try:
                self.send_public_api("market", query)
            except FileNotFoundError:
                self.send_json(
                    {"error": PUBLIC_DATA_UNAVAILABLE_MESSAGE},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except PublicClientRequestError as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            except (DepthExecutionCohortError, ValueError):
                self.send_public_data_validation_error()
            return
        if parsed.path == "/api/admin/session":
            session = ADMIN_SERVICE.get_session(self.admin_session_token())
            self.send_json(ADMIN_SERVICE.public_session(session))
            return
        if parsed.path == "/api/admin/tokens":
            authenticated = self.require_admin()
            if authenticated:
                self.send_json(
                    {
                        "tokens": ADMIN_SERVICE.configured_tokens(),
                        "records": ADMIN_SERVICE.configured_token_records(),
                    }
                )
            return
        if parsed.path == "/api/admin/quality/retryable":
            authenticated = self.require_admin()
            if authenticated:
                try:
                    windows = ADMIN_SERVICE.retryable_windows()
                except ValueError as error:
                    self.send_json(
                        {"error": str(error)},
                        HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                    return
                self.send_json(
                    {
                        "windows": windows,
                        "count": len(windows),
                    }
                )
            return
        if parsed.path == "/api/admin/quality/manual-review":
            authenticated = self.require_admin()
            if authenticated:
                try:
                    review_items = ADMIN_SERVICE.manual_review_items()
                except ValueError as error:
                    self.send_json(
                        {"error": str(error)},
                        HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                    return
                self.send_json(
                    {
                        "review_items": review_items,
                        "review_count": len(review_items),
                        "retryable": False,
                    }
                )
            return
        if parsed.path == "/api/admin/jobs":
            authenticated = self.require_admin()
            if authenticated:
                self.send_json({"jobs": ADMIN_SERVICE.list_jobs()})
            return
        if parsed.path == "/health":
            try:
                payload = build_market_payload()
                metadata = payload["metadata"]
                lifecycle = metadata.get("cex_instrument_lifecycle")
                lifecycle_is_current = (
                    isinstance(lifecycle, dict)
                    and lifecycle.get("stale_evidence_market_count") == 0
                    and lifecycle.get("applied_market_count")
                    == lifecycle.get("absence_market_count")
                )
                data_status = metadata["freshness"]["overall_status"]
                if not lifecycle_is_current:
                    data_status = "stale"
                self.send_json(
                    {
                        "status": "ok",
                        "data_ready": True,
                        "storage": metadata["storage"]["engine"],
                        "data_status": data_status,
                        "freshness": metadata["freshness"],
                        "cex_instrument_lifecycle": lifecycle,
                        "summary_warmup": summary_warmup_health(),
                        "route_opportunities": opportunity_publication_health(),
                        "application_sha": application_release_sha(),
                        "asset_sha": static_asset_sha(),
                        "asset_version": static_asset_version(),
                    }
                )
            except (
                FileNotFoundError,
                ValueError,
                DepthExecutionCohortError,
            ):
                self.send_json(
                    {
                        "status": "degraded",
                        "data_ready": False,
                        "error": PUBLIC_DATA_UNAVAILABLE_MESSAGE,
                    },
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if is_admin_surface_path(path) and not self.admin_surface_available():
            self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            return
        if path in PUBLIC_ACTION_PATHS:
            if not PUBLIC_ACTION_POLICY.enabled_for_path(path):
                self.send_public_action_error(
                    PUBLIC_ACTION_POLICY.disabled_error()
                )
                return
            if parsed.query:
                self.send_public_action_error(
                    PublicActionError(
                        "invalid_public_action_request",
                        "Public action endpoints do not accept query parameters",
                    )
                )
                return
            if path == PUBLIC_QUALITY_RETRYABLE_PATH:
                self.send_public_action_error(
                    PublicActionError(
                        "public_action_method_not_allowed",
                        "This public action requires GET",
                        status=HTTPStatus.METHOD_NOT_ALLOWED,
                    )
                )
                return
            content_type = (
                self.headers.get("Content-Type", "")
                .split(";", 1)[0]
                .strip()
                .lower()
            )
            if content_type != "application/json":
                self.send_public_action_error(
                    PublicActionError(
                        "public_action_json_required",
                        "Public action requests require application/json",
                        status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                    )
                )
                return
        try:
            payload = self.read_json()
        except (ValueError, json.JSONDecodeError) as error:
            if path in PUBLIC_ACTION_PATHS:
                self.send_public_action_error(
                    PublicActionError(
                        "invalid_public_action_request",
                        str(error),
                    )
                )
            else:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
            return

        if path in PUBLIC_ACTION_PATHS:
            self.handle_public_action_post(path, payload)
            return

        if path == "/api/admin/login":
            if ADMIN_SERVICE.open_mode:
                self.send_json({"error": "Administrator login is disabled"}, HTTPStatus.NOT_FOUND)
                return
            try:
                session_token, session = ADMIN_SERVICE.login(
                    self.client_address[0],
                    str(payload.get("username", ""))[:80],
                    str(payload.get("password", ""))[:512],
                )
            except RuntimeError as error:
                self.send_json({"error": str(error)}, HTTPStatus.SERVICE_UNAVAILABLE)
                return
            except PermissionError as error:
                self.send_json({"error": str(error)}, HTTPStatus.TOO_MANY_REQUESTS)
                return
            except ValueError as error:
                self.send_json({"error": str(error)}, HTTPStatus.UNAUTHORIZED)
                return
            self.send_json(session, extra_headers={"Set-Cookie": self.admin_cookie(session_token)})
            return

        if path == "/api/admin/logout":
            if ADMIN_SERVICE.open_mode:
                self.send_json(ADMIN_SERVICE.public_session(None))
                return
            authenticated = self.require_admin(csrf=True)
            if not authenticated:
                return
            session_token, _ = authenticated
            ADMIN_SERVICE.logout(session_token)
            self.send_json(
                {"authenticated": False},
                extra_headers={"Set-Cookie": self.admin_cookie(session_token, clear=True)},
            )
            return

        if path == "/api/admin/jobs":
            authenticated = self.require_admin(csrf=True)
            if not authenticated:
                return
            _, session = authenticated
            try:
                job = ADMIN_SERVICE.create_job(payload, session["username"])
            except RuntimeError as error:
                self.send_json({"error": str(error)}, HTTPStatus.CONFLICT)
                return
            except (ValueError, OSError) as error:
                self.send_json({"error": str(error)}, HTTPStatus.BAD_REQUEST)
                return
            self.send_json(job, HTTPStatus.ACCEPTED)
            return

        if path == "/api/admin/tokens/resolve":
            authenticated = self.require_admin(csrf=True)
            if not authenticated:
                return
            try:
                candidate = ADMIN_SERVICE.resolve_token(
                    payload.get("chain"),
                    payload.get("contract_address"),
                )
            except ValueError as error:
                error_code = getattr(error, "code", "invalid_token_request")
                response = {
                    "error": str(error),
                    "error_code": error_code,
                    "retryable": bool(getattr(error, "retryable", False)),
                }
                details = getattr(error, "details", None)
                if details:
                    response["details"] = details
                status = (
                    HTTPStatus.NOT_FOUND
                    if error_code == "token_not_found"
                    else HTTPStatus.CONFLICT
                    if error_code in {
                        "symbol_collision",
                        "identity_conflict",
                    }
                    else HTTPStatus.SERVICE_UNAVAILABLE
                    if error_code in {
                        "source_rate_limited",
                        "source_unavailable",
                        "source_invalid_response",
                    }
                    else HTTPStatus.UNPROCESSABLE_ENTITY
                    if error_code in {
                        "no_usable_pool",
                        "pool_token_mismatch",
                        "identity_mismatch",
                    }
                    else HTTPStatus.BAD_REQUEST
                )
                self.send_json(response, status)
                return
            self.send_json(candidate)
            return

        if path == "/api/admin/tokens":
            authenticated = self.require_admin(csrf=True)
            if not authenticated:
                return
            _, session = authenticated
            try:
                job = ADMIN_SERVICE.create_onboarding_job(
                    payload,
                    session["username"],
                )
            except RuntimeError as error:
                self.send_json({"error": str(error)}, HTTPStatus.CONFLICT)
                return
            except (ValueError, OSError) as error:
                error_code = getattr(error, "code", "invalid_token_request")
                response = {
                    "error": str(error),
                    "error_code": error_code,
                    "retryable": bool(getattr(error, "retryable", False)),
                }
                details = getattr(error, "details", None)
                if details:
                    response["details"] = details
                status = (
                    HTTPStatus.CONFLICT
                    if error_code in {
                        "symbol_collision",
                        "identity_conflict",
                    }
                    else HTTPStatus.SERVICE_UNAVAILABLE
                    if error_code in {
                        "source_rate_limited",
                        "source_unavailable",
                        "source_invalid_response",
                    }
                    else HTTPStatus.UNPROCESSABLE_ENTITY
                    if error_code in {
                        "no_usable_pool",
                        "pool_token_mismatch",
                        "identity_mismatch",
                        "identity_changed",
                    }
                    else HTTPStatus.BAD_REQUEST
                )
                self.send_json(response, status)
                return
            self.send_json(
                job,
                (
                    HTTPStatus.OK
                    if job.get("status") == "succeeded"
                    else HTTPStatus.ACCEPTED
                ),
            )
            return

        self.send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the fact-only CEX/DEX Market Monitor")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--data-dir", help="Directory containing detailed CEX and DEX CSV files")
    return parser.parse_args()


def main() -> None:
    global ADMIN_SERVICE
    args = parse_args()
    if args.data_dir:
        os.environ["MARKET_DATA_DIR"] = str(Path(args.data_dir).expanduser().resolve())
        ADMIN_SERVICE = AdminService()
    if write_surface_enabled() and not is_loopback_host(args.host):
        raise SystemExit(
            "Write-capable surfaces require a loopback bind. Run behind an "
            "HTTPS reverse proxy or disable administrator and public actions."
        )
    server = ThreadingHTTPServer((args.host, args.port), MarketMonitorHandler)
    print(f"Market Monitor running at http://{args.host}:{args.port}")
    start_default_market_summary_warmup()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
