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
from dataclasses import fields, is_dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
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
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

try:
    from scripts.cex_fee_facts import (
        collect_cex_fee_snapshot,
        load_validated_fee_profile,
    )
    from scripts.execution_cost_components import (
        COST_COMPONENT_CONTRACT_VERSION,
        COST_COMPONENT_COLUMNS,
        validate_cost_components,
    )
    from scripts.fetch_cex_depth import (
        CEX_DEPTH_REASON_CODES,
        parse_book,
        route_quantity_quote_for_book,
        source_request,
    )
    from scripts.fetch_dex_depth import (
        DEX_DEPTH_COLLECTION_FAILURE_REASON_CODES,
        DEX_DEPTH_UNSUPPORTED_REASON_CODES,
    )
    from scripts.route_cohort import (
        canonical_route_id,
        classify_route_timing,
        validate_route_cohort_rows,
    )
    from scripts.route_inventory import (
        classify_route_mode_evidence,
        inventory_capacity_for_route,
        load_validated_inventory_profile,
    )
    from scripts.route_cost_evidence import (
        RouteCostEvidenceError,
        validate_route_cost_evidence_manifest_for_publication,
    )
    from scripts.route_opportunity import (
        OPPORTUNITY_FIELDS,
        _issue_publication_attestation,
        _publication_binding_sha256,
        build_route_opportunity,
        route_opportunity_id,
    )
    from scripts.route_cost_topology import (
        HISTORICAL_ATOMIC_COMPONENT_MATRIX,
        live_complete_cost_component_keys,
    )
    from scripts.route_quantity import FeeSemantics, MarketRules, QuantityQuote
    from scripts.route_shadow_inputs import (
        TYPED_SOURCE_MANIFEST_FIELDS,
        TYPED_SOURCE_MANIFEST_MEMBER_FIELDS,
        TYPED_SOURCE_MANIFEST_SCHEMA,
        TYPED_SOURCE_ROLE_CONTRACTS,
        _validate_run_id as _validate_shadow_run_id,
        _validated_publication_manifest as _validate_shadow_baseline_manifest,
        typed_source_lineage_observed_members,
        validate_typed_source_lineage,
    )
    from scripts.route_universe import (
        ROUTE_UNIVERSE_SCHEMA,
        _selection_key as _route_universe_selection_key,
        route_universe_sha256,
    )
    from scripts.timestamp_contract import (
        exact_rfc3339_epoch_seconds,
        parse_rfc3339_utc,
    )
    from scripts.token_registry import (
        TokenRegistryError,
        normalize_chain,
        normalize_contract_address,
    )
except ModuleNotFoundError:
    from cex_fee_facts import (  # type: ignore[no-redef]
        collect_cex_fee_snapshot,
        load_validated_fee_profile,
    )
    from execution_cost_components import (  # type: ignore[no-redef]
        COST_COMPONENT_CONTRACT_VERSION,
        COST_COMPONENT_COLUMNS,
        validate_cost_components,
    )
    from fetch_cex_depth import (  # type: ignore[no-redef]
        CEX_DEPTH_REASON_CODES,
        parse_book,
        route_quantity_quote_for_book,
        source_request,
    )
    from fetch_dex_depth import (  # type: ignore[no-redef]
        DEX_DEPTH_COLLECTION_FAILURE_REASON_CODES,
        DEX_DEPTH_UNSUPPORTED_REASON_CODES,
    )
    from route_cohort import (  # type: ignore[no-redef]
        canonical_route_id,
        classify_route_timing,
        validate_route_cohort_rows,
    )
    from route_inventory import (  # type: ignore[no-redef]
        classify_route_mode_evidence,
        inventory_capacity_for_route,
        load_validated_inventory_profile,
    )
    from route_cost_evidence import (  # type: ignore[no-redef]
        RouteCostEvidenceError,
        validate_route_cost_evidence_manifest_for_publication,
    )
    from route_opportunity import (  # type: ignore[no-redef]
        OPPORTUNITY_FIELDS,
        _issue_publication_attestation,
        _publication_binding_sha256,
        build_route_opportunity,
        route_opportunity_id,
    )
    from route_cost_topology import (  # type: ignore[no-redef]
        HISTORICAL_ATOMIC_COMPONENT_MATRIX,
        live_complete_cost_component_keys,
    )
    from route_quantity import (  # type: ignore[no-redef]
        FeeSemantics,
        MarketRules,
        QuantityQuote,
    )
    from route_shadow_inputs import (  # type: ignore[no-redef]
        TYPED_SOURCE_MANIFEST_FIELDS,
        TYPED_SOURCE_MANIFEST_MEMBER_FIELDS,
        TYPED_SOURCE_MANIFEST_SCHEMA,
        TYPED_SOURCE_ROLE_CONTRACTS,
        _validate_run_id as _validate_shadow_run_id,
        _validated_publication_manifest as _validate_shadow_baseline_manifest,
        typed_source_lineage_observed_members,
        validate_typed_source_lineage,
    )
    from route_universe import (  # type: ignore[no-redef]
        ROUTE_UNIVERSE_SCHEMA,
        _selection_key as _route_universe_selection_key,
        route_universe_sha256,
    )
    from timestamp_contract import (  # type: ignore[no-redef]
        exact_rfc3339_epoch_seconds,
        parse_rfc3339_utc,
    )
    from token_registry import (  # type: ignore[no-redef]
        TokenRegistryError,
        normalize_chain,
        normalize_contract_address,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROUTE_CORE_ROOT = PROJECT_ROOT / "data/local/routes/core"
DEFAULT_ROUTE_ROOT = PROJECT_ROOT / "data/local/routes"

ROUTE_COHORT_SCHEMA = "route_cohort_collection/v1"
ROUTE_CORE_BUNDLE_STAGE = "route_cohort_core/v1"
ROUTE_CORE_MANIFEST_SCHEMA = "route_cohort_core_manifest/v1"
ROUTE_CORE_POINTER_SCHEMA = "route_cohort_core_pointer/v1"
ROUTE_CANDIDATE_CSV_SCHEMA = "route_candidates/v1"
ROUTE_LEG_CSV_SCHEMA = "route_legs/v1"
ROUTE_TIMING_CSV_SCHEMA = "route_timing/v1"
ROUTE_SQLITE_LOGICAL_SCHEMA = "route_cohort_sqlite/v1"

ROUTE_SHADOW_POINTER_SCHEMA = "route_shadow_pointer/v1"
ROUTE_SHADOW_PHASE_SCHEMA = "route_shadow_phase/v1"
ROUTE_SHADOW_AUDIT_FILENAME = "audit.json"
ROUTE_SHADOW_COST_EVIDENCE_FILENAME = "route-cost-evidence.json"
ROUTE_SHADOW_LATEST_FILENAME = "latest.json"
ROUTE_SHADOW_PHASE_FILENAME = "phase.json"
ROUTE_SHADOW_IMPLICIT_CANARY_BYTES = b"route-shadow-phase/implicit-canary/v1\n"
ROUTE_SHADOW_IMPLICIT_CANARY_SHA256 = hashlib.sha256(
    ROUTE_SHADOW_IMPLICIT_CANARY_BYTES
).hexdigest()

_ROUTE_SHADOW_POINTER_FIELDS = frozenset({
    "schema",
    "run_id",
    "phase",
    "route_cohort_id",
    "phase_state_sha256",
    "phase_transition_id",
    "core_pointer_sha256",
    "core_manifest_sha256",
    "route_universe_sha256",
    "route_cost_evidence_sha256",
    "baseline_manifest_sha256",
    "candidate_source_generation",
    "audit_sha256",
})
_ROUTE_SHADOW_PHASE_FIELDS = frozenset({
    "schema",
    "transition_id",
    "prior_phase",
    "phase",
    "evaluated_at",
    "gate_evidence_sha256",
    "storage_admission_sha256",
    "anchored_joint_pointer_sha256",
    "primary_schedule_guard_sha256",
    "schedule_envelope_sha256",
    "phase_identity_id",
})
_ROUTE_UNIVERSE_FIELDS = frozenset({
    "schema",
    "candidate_source_generation",
    "selection_window",
    "requested_notionals_usd",
    "selected_legs",
    "routes",
})
_ROUTE_UNIVERSE_LEG_FIELDS = frozenset({
    "market_id",
    "market_type",
    "token_symbol",
    "candidate_source_generation",
    "selection_window",
    "selection_inputs",
    "selection_rank",
})
_ROUTE_UNIVERSE_DEX_IDENTITY_FIELDS = frozenset({
    "collector_context",
    "target_token_address",
    "target_token_side",
})
_ROUTE_UNIVERSE_SELECTION_INPUT_FIELDS = frozenset({
    "execution_capability",
    "proved_execution_capacity_usd",
    "observed_100bps_depth_usd",
    "cex_selected_window_usd",
    "dex_24h_usd",
    "dex_tvl_usd",
})
_ROUTE_COLLECTOR_CONTEXT_FIELDS = frozenset({
    "schema",
    "snapshot_id",
    "request_started_at",
    "observed_at",
    "response_received_at",
    "status",
    "reason_code",
    "pool_name",
    "base_token_id",
    "quote_token_id",
    "base_token_price_usd",
    "quote_token_price_usd",
    "tvl_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
})
_MAX_ROUTE_COST_EVIDENCE_BYTES = 32 * 1024 * 1024

ROUTE_CANDIDATES_FILENAME = "route_candidates.csv"
ROUTE_LEGS_FILENAME = "route_legs.csv"
ROUTE_TIMING_FILENAME = "route_timing.csv"
ROUTE_SQLITE_FILENAME = "route_cohort.sqlite3"
MANIFEST_FILENAME = "manifest.json"

ROUTE_OPPORTUNITY_BUNDLE_STAGE = "route_opportunity/v1"
ROUTE_OPPORTUNITY_MANIFEST_SCHEMA = "route_opportunity_manifest/v1"
ROUTE_OPPORTUNITY_POINTER_SCHEMA = "route_opportunity_pointer/v1"
ROUTE_OPPORTUNITY_SQLITE_SCHEMA = "route_opportunity_sqlite/v1"
COST_COMPONENT_CSV_SCHEMA = "route_cost_components/v1"
ROUTE_OPPORTUNITY_CSV_SCHEMA = "route_opportunities/v1"
COST_COMPONENTS_FILENAME = "cost_components.csv"
ROUTE_OPPORTUNITIES_FILENAME = "route_opportunities.csv"
ROUTE_OPPORTUNITY_SQLITE_FILENAME = ROUTE_SQLITE_FILENAME

ROUTE_COMPLETE_FILENAMES = frozenset({
    ROUTE_LEGS_FILENAME,
    COST_COMPONENTS_FILENAME,
    ROUTE_OPPORTUNITIES_FILENAME,
    ROUTE_OPPORTUNITY_SQLITE_FILENAME,
    MANIFEST_FILENAME,
})
_COMPLETE_MANIFEST_ARTIFACT_FILENAMES = frozenset(
    ROUTE_COMPLETE_FILENAMES - {MANIFEST_FILENAME}
)

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
_SHADOW_DECIMAL_TEXT = re.compile(
    r"(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?\Z",
    flags=re.ASCII,
)

_ROUTE_MODES = frozenset({
    "prepositioned_inventory",
    "atomic_onchain",
    "rebalance_required",
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
    "usd_price_context_missing",
    "usd_price_context_not_found",
    "usd_price_context_failed",
    "measurement_limit",
}) | DEX_DEPTH_COLLECTION_FAILURE_REASON_CODES | DEX_DEPTH_UNSUPPORTED_REASON_CODES

# These failures are created by the bounded cohort orchestrator after (or
# instead of) the venue-specific DEX collector.  They are distinct from the
# collector's own closed failure vocabulary, but they still describe a failed
# DEX leg and must survive exact publication/replay.
_DEX_ORCHESTRATION_FAILURE_REASONS = frozenset({
    "fixed_block_unavailable",
    "fixed_block_lineage_mismatch",
    "collector_identity_mismatch",
    "raw_evidence_missing",
    "raw_evidence_hash_mismatch",
    "raw_evidence_path_unsafe",
    "usd_price_context_missing",
    "usd_price_context_not_found",
    "usd_price_context_failed",
})
_CEX_ORCHESTRATION_FAILURE_REASONS = frozenset({
    "collector_identity_mismatch",
    "raw_evidence_missing",
    "raw_evidence_hash_mismatch",
    "raw_evidence_path_unsafe",
})
_CEX_COLLECTOR_FAILURE_REASONS = frozenset(CEX_DEPTH_REASON_CODES) - {
    "observed",
    "source_level_limit",
}

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
    "buy_reference_volume_usd",
    "sell_reference_volume_usd",
    "route_volume_usd",
    "route_volume_basis",
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
    "buy_reference_volume_usd",
    "sell_reference_volume_usd",
    "route_volume_usd",
    "route_volume_basis",
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


def _validate_collector_timestamp(value: Any, field: str) -> str:
    """Validate the exact UTC representation emitted by fact collectors.

    Shadow runtime evidence is normalized to ``Z``.  The TVL fact contract is
    older and deliberately preserves ``datetime.isoformat()`` bytes using
    ``+00:00``.  Collector context is source evidence copied unchanged, so it
    must be checked against that producer contract instead of normalized here.
    """
    text = _nonempty_text(value, field)
    try:
        parsed = parse_rfc3339_utc(text)
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("{} is invalid".format(field)) from error
    if parsed.isoformat() != text:
        raise RoutePublicationError(
            "{} must use the collector's canonical UTC representation".format(
                field
            )
        )
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


def _canonical_shadow_decimal(
    raw: Any, *, positive: bool, label: str
) -> str:
    if (
        not isinstance(raw, str)
        or len(raw) > 256
        or _SHADOW_DECIMAL_TEXT.fullmatch(raw) is None
    ):
        raise RoutePublicationError("{} is invalid".format(label))
    try:
        amount = Decimal(raw)
    except (InvalidOperation, ValueError) as error:
        raise RoutePublicationError("{} is invalid".format(label)) from error
    canonical = format(amount, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if (
        not amount.is_finite()
        or amount < 0
        or (positive and amount <= 0)
        or raw != canonical
    ):
        raise RoutePublicationError("{} is invalid".format(label))
    return raw


def _collector_token_address(value: Any, *, chain: str, label: str) -> str:
    text = _nonempty_text(value, label)
    prefix, separator, address = text.partition("_")
    if not separator or prefix != chain or not address:
        raise RoutePublicationError("{} is invalid".format(label))
    try:
        normalized = normalize_contract_address(chain, address)
    except (TokenRegistryError, ValueError) as error:
        raise RoutePublicationError("{} is invalid".format(label)) from error
    if normalized != address:
        raise RoutePublicationError("{} is invalid".format(label))
    return address


def _validate_route_collector_context(
    context: Any, *, market_id: str
) -> Dict[str, Any]:
    """Validate the complete immutable TVL/USD collector projection."""
    if (
        not isinstance(context, dict)
        or set(context) != _ROUTE_COLLECTOR_CONTEXT_FIELDS
        or context.get("schema") != "route_collector_context/v1"
    ):
        raise RoutePublicationError("DEX collector context schema is invalid")
    match = _DEX_MARKET_ID.fullmatch(market_id)
    if match is None:
        raise RoutePublicationError("DEX collector context market is invalid")
    raw_chain = match.group(1)
    try:
        chain = normalize_chain(raw_chain)
    except (TokenRegistryError, ValueError) as error:
        raise RoutePublicationError("DEX collector context chain is invalid") from error
    if chain != raw_chain:
        raise RoutePublicationError("DEX collector context chain is invalid")
    for field in ("snapshot_id", "pool_name", "tvl_method", "source"):
        _nonempty_text(context.get(field), "DEX collector context " + field)
    if not _safe_network_endpoint(context.get("source_endpoint")):
        raise RoutePublicationError("DEX collector source endpoint is unsafe")
    if (
        not isinstance(context.get("raw_response_sha256"), str)
        or _HEX_SHA256.fullmatch(context["raw_response_sha256"]) is None
    ):
        raise RoutePublicationError("DEX collector raw-response hash is invalid")
    request_started_at = _validate_collector_timestamp(
        context.get("request_started_at"), "DEX collector request_started_at"
    )
    observed_at = _validate_collector_timestamp(
        context.get("observed_at"), "DEX collector observed_at"
    )
    response_received_at = _validate_collector_timestamp(
        context.get("response_received_at"),
        "DEX collector response_received_at",
    )
    if not (
        exact_rfc3339_epoch_seconds(request_started_at)
        <= exact_rfc3339_epoch_seconds(observed_at)
        <= exact_rfc3339_epoch_seconds(response_received_at)
    ):
        raise RoutePublicationError("DEX collector timestamps are unordered")

    status = context.get("status")
    reason = context.get("reason_code")
    address_prices: Dict[str, str] = {}
    if status == "observed":
        if reason != "observed":
            raise RoutePublicationError("observed DEX collector context is invalid")
        for token_field, price_field in (
            ("base_token_id", "base_token_price_usd"),
            ("quote_token_id", "quote_token_price_usd"),
        ):
            address = _collector_token_address(
                context.get(token_field), chain=chain,
                label="DEX collector " + token_field,
            )
            price = _canonical_shadow_decimal(
                context.get(price_field), positive=True,
                label="DEX collector " + price_field,
            )
            if address in address_prices:
                raise RoutePublicationError(
                    "DEX collector Token IDs must be distinct"
                )
            address_prices[address] = price
    elif status in {"missing", "not_found", "failed"}:
        allowed_reasons = {
            "missing": {"source_no_tvl_observation"},
            "not_found": {"source_pool_not_found"},
            "failed": {
                "network", "rate_limit", "source_unavailable", "parse",
                "validation", "collection_failed",
            },
        }
        if reason not in allowed_reasons[status] or any(
            context.get(field) is not None
            for field in (
                "base_token_id", "quote_token_id",
                "base_token_price_usd", "quote_token_price_usd",
            )
        ):
            raise RoutePublicationError(
                "unavailable DEX collector context is invalid"
            )
    else:
        raise RoutePublicationError("DEX collector context status is invalid")
    return {"context": context, "chain": chain, "address_prices": address_prices}


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

    reference_volumes: List[Optional[Decimal]] = []
    for field in ("buy_reference_volume_usd", "sell_reference_volume_usd"):
        value = route.get(field)
        if value is None:
            reference_volumes.append(None)
            continue
        if not isinstance(value, str):
            raise RoutePublicationError("route volume lineage is invalid")
        try:
            amount = Decimal(value)
        except (InvalidOperation, ValueError):
            raise RoutePublicationError("route volume lineage is invalid")
        canonical_amount = format(amount, "f")
        if "." in canonical_amount:
            canonical_amount = canonical_amount.rstrip("0").rstrip(".")
        if (
            not amount.is_finite()
            or amount <= 0
            or value != canonical_amount
        ):
            raise RoutePublicationError("route volume lineage is invalid")
        reference_volumes.append(amount)
    route_volume_value = route.get("route_volume_usd")
    if route_volume_value is None:
        route_volume = None
    else:
        if not isinstance(route_volume_value, str):
            raise RoutePublicationError("route volume lineage is invalid")
        try:
            route_volume = Decimal(route_volume_value)
        except (InvalidOperation, ValueError):
            raise RoutePublicationError("route volume lineage is invalid")
        canonical_route_volume = format(route_volume, "f")
        if "." in canonical_route_volume:
            canonical_route_volume = canonical_route_volume.rstrip("0").rstrip(".")
        if (
            not route_volume.is_finite()
            or route_volume <= 0
            or route_volume_value != canonical_route_volume
        ):
            raise RoutePublicationError("route volume lineage is invalid")
    expected_route_volume = (
        min(value for value in reference_volumes if value is not None)
        if all(value is not None for value in reference_volumes)
        else None
    )
    if (
        route.get("route_volume_basis")
        != "minimum_leg_source_horizon_usd"
        or route_volume != expected_route_volume
    ):
        raise RoutePublicationError("route volume lineage is invalid")

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
    elif mode == "research_only":
        if not (buy.startswith("dex:") and sell.startswith("dex:")):
            raise RoutePublicationError("route mode lineage is invalid")
        if buy.split(":", 2)[1] == sell.split(":", 2)[1]:
            raise RoutePublicationError("route mode lineage is invalid")
    elif mode == "rebalance_required":
        if buy.startswith("dex:") and sell.startswith("dex:"):
            if buy.split(":", 2)[1] != sell.split(":", 2)[1]:
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
        if "typed_source_lineage" in row:
            try:
                validate_typed_source_lineage(
                    row["typed_source_lineage"], market_type=inferred_type
                )
            except (TypeError, ValueError) as error:
                raise RoutePublicationError(
                    "route leg typed-source lineage is invalid"
                ) from error
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
        if inferred_type == "dex":
            allowed_dex_reasons = {
                "observed": {None, "", "observed"},
                "partial": {"measurement_limit"},
                "unsupported": set(DEX_DEPTH_UNSUPPORTED_REASON_CODES),
                "failed": set(DEX_DEPTH_COLLECTION_FAILURE_REASON_CODES)
                | set(_DEX_ORCHESTRATION_FAILURE_REASONS),
                "deadline_exceeded": {"route_deadline_exceeded"},
            }[status_text]
            if reason not in allowed_dex_reasons:
                raise RoutePublicationError(
                    "DEX leg status and reason conflict"
                )
        else:
            allowed_cex_reasons = {
                "observed": {None, "", "observed"},
                "partial": {"source_level_limit"},
                "unsupported": set(),
                "failed": set(_CEX_COLLECTOR_FAILURE_REASONS)
                | set(_CEX_ORCHESTRATION_FAILURE_REASONS),
                "deadline_exceeded": {"route_deadline_exceeded"},
            }[status_text]
            if reason not in allowed_cex_reasons:
                raise RoutePublicationError(
                    "CEX leg status and reason conflict"
                )
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
        "buy_reference_volume_usd": row.get("buy_reference_volume_usd") or "",
        "sell_reference_volume_usd": row.get("sell_reference_volume_usd") or "",
        "route_volume_usd": row.get("route_volume_usd") or "",
        "route_volume_basis": row["route_volume_basis"],
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


def _verify_directory_entry_snapshot(
    parent_fd: int,
    name: str,
    opened: os.stat_result,
    label: str,
) -> os.stat_result:
    """Require an opened child directory to keep its exact generation."""
    try:
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as error:
        raise RoutePublicationError(
            "{} changed during validation".format(label)
        ) from error
    if (
        not stat.S_ISDIR(current.st_mode)
        or _stable_file_metadata(current) != _stable_file_metadata(opened)
    ):
        raise RoutePublicationError(
            "{} changed during validation".format(label)
        )
    return current


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


def _verify_open_path_snapshot(
    path: Path,
    opened: os.stat_result,
    label: str,
) -> os.stat_result:
    """Require both pathname identity and the opened directory generation."""
    try:
        current = os.stat(str(path), follow_symlinks=False)
    except OSError as error:
        raise RoutePublicationError(
            "{} changed during validation".format(label)
        ) from error
    if (
        not stat.S_ISDIR(current.st_mode)
        or _stable_file_metadata(current) != _stable_file_metadata(opened)
    ):
        raise RoutePublicationError(
            "{} changed during validation".format(label)
        )
    return current


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
    except FileNotFoundError as error:
        # Absence of an expected immutable member is a lineage failure.  Keep
        # it distinct from a present symlink or other unsafe file type so the
        # run ledger does not misclassify ordinary evidence loss as an attack.
        raise RoutePublicationError("{} is missing".format(label)) from error
    except OSError as error:
        raise RoutePublicationError(
            "{} must be a regular non-symlink file".format(label)
        ) from error
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        os.close(fd)
        raise RoutePublicationError(
            "{} must be a regular non-symlink single-link file".format(label)
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
                buy_reference_volume_usd TEXT NOT NULL,
                sell_reference_volume_usd TEXT NOT NULL,
                route_volume_usd TEXT NOT NULL,
                route_volume_basis TEXT NOT NULL,
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


def _core_representation_artifact_bytes(
    cohort: Mapping[str, Any],
) -> Tuple[Dict[str, bytes], Dict[str, Dict[str, Any]]]:
    """Build the four existing core representations from a validated cohort."""
    normalized = _normalize_and_validate_cohort(cohort)
    return _core_representation_artifact_bytes_from_validated_cohort(
        normalized
    )


def _core_representation_artifact_bytes_from_validated_cohort(
    cohort: Mapping[str, Any],
) -> Tuple[Dict[str, bytes], Dict[str, Dict[str, Any]]]:
    """Serialize a cohort already closed by its profile-specific validator."""
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
        sqlite_logical = _build_route_cohort_sqlite_file(
            database_path, cohort
        )
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
    return {
        ROUTE_CANDIDATES_FILENAME: candidate_bytes,
        ROUTE_LEGS_FILENAME: leg_bytes,
        ROUTE_TIMING_FILENAME: timing_bytes,
        ROUTE_SQLITE_FILENAME: database_bytes,
    }, files


def _write_bundle_artifacts(
    stage_path: Path,
    cohort: Mapping[str, Any],
    *,
    stage_fd: Optional[int] = None,
) -> None:
    representation_bytes, files = _core_representation_artifact_bytes(cohort)
    manifest = _manifest_payload(cohort, files)
    artifact_bytes = {
        **representation_bytes,
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
    expected_inventory: frozenset = ROUTE_CORE_FILENAMES,
) -> None:
    try:
        if set(os.listdir(bundle_fd)) != expected_inventory:
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
        if set(os.listdir(bundle_fd)) != expected_inventory:
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


_COMPLETE_INPUT_FIELDS = frozenset({
    "classified_opportunity",
    "build_inputs",
    "source_members",
})
_OPPORTUNITY_BUILD_FIELDS = frozenset({
    "cohort_id",
    "route",
    "requested_notional_usd",
    "common_target",
    "buy_leg",
    "sell_leg",
    "buy_quote",
    "sell_quote",
    "buy_quote_evidence",
    "sell_quote_evidence",
    "buy_usd_projection",
    "sell_usd_projection",
    "cost_components",
    "mode_evidence",
    "now",
})
_STRICT_CEX_SOURCE_MEMBERS = frozenset({
    "buy_market_rules",
    "sell_market_rules",
    "buy_usd_conversion",
    "sell_usd_conversion",
})
_MARKET_RULES_SOURCE_FIELDS = frozenset({
    "schema",
    "market_id",
    "base_asset",
    "quote_asset",
    "base_unit_decimals",
    "quote_unit_decimals",
    "base_increment",
    "quote_increment",
    "min_base_quantity",
    "min_quote_notional",
    "observed_at",
    "valid_until",
})
_USD_SOURCE_FIELDS = frozenset({
    "schema",
    "quote_asset",
    "usd_per_quote",
    "observed_at",
    "valid_until",
    "source",
})
_COMPLETE_ADAPTER_VERSIONS = {
    "cex_request": "fetch_cex_depth/source_request/v1",
    "cex_book_parser": "fetch_cex_depth/parse_book/v1",
    "cex_quantity": "route_quantity_quote_for_book/v1",
    "cex_fee": "cex_fee_facts/private_profile/v1",
    "inventory": "route_inventory/private_profile/v1",
    "usd_conversion": "route_usd_conversion_source/v1",
}


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return "0" if text in {"", "-0"} else text
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_safe_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe_value(nested)
            for key, nested in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise RoutePublicationError("complete route input is not JSON-normalizable")


def _canonical_input_sha256(value: Any) -> str:
    return _sha256_bytes(_canonical_json_bytes(_json_safe_value(value)))


def _read_member_from_root(
    root_fd: int,
    name: Any,
    *,
    label: str,
) -> Tuple[Dict[str, Any], bytes, str]:
    filename = _require_relative_basename(str(name), label)
    value, physical_sha256, _details = _read_bounded_bytes_at(
        root_fd,
        filename,
        limit=_MAX_JSON_BYTES,
        label=label,
    )
    return _decode_json_object_bytes(value, label=label), value, physical_sha256


def _read_core_raw_members(
    raw_root: Path,
    cohort: Mapping[str, Any],
) -> Dict[str, Tuple[Dict[str, Any], bytes, str]]:
    raw, raw_fd, raw_details = _open_verified_directory(
        Path(raw_root),
        "route raw root",
    )
    run_fd: Optional[int] = None
    accepted_fd: Optional[int] = None
    members: Dict[str, Tuple[Dict[str, Any], bytes, str]] = {}
    try:
        run_name = _require_relative_basename(
            str(cohort["raw_evidence_run_id"]),
            "route raw run ID",
        )
        run_fd, run_details = _open_directory_at(
            raw_fd,
            run_name,
            "route raw run",
        )
        accepted_fd, accepted_details = _open_directory_at(
            run_fd,
            "accepted",
            "route accepted raw root",
        )
        for leg in cohort["legs"]:
            if leg.get("status") not in {"observed", "partial"}:
                continue
            market_id = str(leg["market_id"])
            entry = hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            member_fd, member_details = _open_directory_at(
                accepted_fd,
                entry,
                "route raw market member",
            )
            try:
                if set(os.listdir(member_fd)) != {"response.json"}:
                    raise RoutePublicationError(
                        "route raw market member inventory is invalid"
                    )
                value, physical_sha256, _file_details = _read_bounded_bytes_at(
                    member_fd,
                    "response.json",
                    limit=_MAX_JSON_BYTES,
                    label="route raw response",
                )
                if physical_sha256 != leg.get("raw_response_sha256"):
                    raise RoutePublicationError(
                        "route raw response hash does not match core leg"
                    )
                payload = _decode_json_object_bytes(
                    value,
                    label="route raw response",
                )
                members[market_id] = (payload, value, physical_sha256)
                _verify_directory_entry(
                    accepted_fd,
                    entry,
                    member_details,
                    "route raw market member",
                )
            finally:
                os.close(member_fd)
        _verify_directory_entry(
            run_fd,
            "accepted",
            accepted_details,
            "route accepted raw root",
        )
        _verify_directory_entry(raw_fd, run_name, run_details, "route raw run")
        _verify_open_path_identity(raw, raw_details, "route raw root")
        return members
    finally:
        if accepted_fd is not None:
            os.close(accepted_fd)
        if run_fd is not None:
            os.close(run_fd)
        os.close(raw_fd)


def _parse_cex_book_source(
    payload: Mapping[str, Any],
    raw_bytes: bytes,
    *,
    market_id: str,
    state_observed_at: str,
) -> Tuple[Dict[str, str], Dict[str, Any]]:
    parts = market_id.split(":", 2)
    if len(parts) != 3 or parts[0] != "cex":
        raise RoutePublicationError("typed CEX book source market is invalid")
    venue = parts[1]
    instrument = parts[2]
    if venue == "upbit":
        raise RoutePublicationError("typed CEX book source identity is invalid")
    try:
        endpoint, requested_instrument, quote_asset, full_book = source_request(
            venue,
            instrument,
        )
        normalized = parse_book(
            venue,
            payload,
            requested_instrument=requested_instrument,
        )
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("typed CEX book source cannot be replayed") from error
    observed_at = normalized.get("source_observed_at") or state_observed_at
    observed_at = _validate_timestamp(observed_at, "typed CEX observed_at")
    market = {
        "token_symbol": instrument.split("/", 1)[0],
        "exchange": venue,
        "cex_symbol": instrument,
    }
    book = {
        "bids": normalized["bids"],
        "asks": normalized["asks"],
        "source_instrument": normalized["source_instrument"],
        "source_quote_asset": quote_asset,
        "source_sequence": normalized["source_sequence"],
        "source_observed_at": observed_at,
        "source_endpoint": endpoint,
        "full_book_reported": full_book,
        "raw": raw_bytes,
    }
    return market, book


def _parse_market_rules_source(
    payload: Mapping[str, Any],
    physical_sha256: str,
    *,
    market_id: str,
) -> MarketRules:
    if (
        set(payload) != _MARKET_RULES_SOURCE_FIELDS
        or payload.get("schema") != "route_market_rules_source/v1"
        or payload.get("market_id") != market_id
    ):
        raise RoutePublicationError("typed market-rules source schema is invalid")
    try:
        return MarketRules(
            market_id=payload["market_id"],
            base_asset=payload["base_asset"],
            quote_asset=payload["quote_asset"],
            base_unit_decimals=payload["base_unit_decimals"],
            quote_unit_decimals=payload["quote_unit_decimals"],
            base_increment=Decimal(payload["base_increment"]),
            quote_increment=Decimal(payload["quote_increment"]),
            min_base_quantity=Decimal(payload["min_base_quantity"]),
            min_quote_notional=Decimal(payload["min_quote_notional"]),
            observed_at=payload["observed_at"],
            valid_until=payload["valid_until"],
            source_record_sha256=physical_sha256,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RoutePublicationError("typed market-rules source is invalid") from error


def _canonical_cost_set_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    canonical = sorted(
        (dict(row) for row in rows),
        key=lambda row: (row["leg"], row["component_type"]),
    )
    return _canonical_input_sha256(canonical)


def _validated_prepublication_input(
    raw: Mapping[str, Any],
    *,
    cohort: Mapping[str, Any],
    core_manifest_sha256: str,
    routes_by_id: Mapping[str, Mapping[str, Any]],
    legs_by_market: Mapping[str, Mapping[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    if not isinstance(raw, Mapping) or set(raw) != _COMPLETE_INPUT_FIELDS:
        raise RoutePublicationError("complete opportunity input schema is invalid")
    build_inputs = raw.get("build_inputs")
    classified = raw.get("classified_opportunity")
    if not isinstance(build_inputs, Mapping) or set(build_inputs) != _OPPORTUNITY_BUILD_FIELDS:
        raise RoutePublicationError("opportunity build input schema is invalid")
    if not isinstance(classified, Mapping) or set(classified) != OPPORTUNITY_FIELDS:
        raise RoutePublicationError("classified opportunity schema is invalid")
    if (
        classified.get("strict_eligible") is not False
        or classified.get("publication_attestation_sha256") is not None
    ):
        raise RoutePublicationError("prepublication opportunity must not be attested")
    try:
        rebuilt = build_route_opportunity(**dict(build_inputs))
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("classified opportunity cannot be replayed") from error
    if dict(classified) != rebuilt:
        raise RoutePublicationError("classified opportunity replay mismatch")

    route_id = str(classified["route_id"])
    route = routes_by_id.get(route_id)
    if route is None:
        raise RoutePublicationError("opportunity route is absent from core")
    for field in (
        "route_id", "token_symbol", "buy_market_id", "sell_market_id", "route_mode"
    ):
        if classified.get(field) != route.get(field):
            raise RoutePublicationError("opportunity route lineage mismatch")
    if classified.get("cohort_id") != cohort["route_cohort_id"]:
        raise RoutePublicationError("opportunity cohort lineage mismatch")
    notional = classified.get("requested_notional_usd")
    if str(notional) not in {str(value) for value in cohort["requested_notionals_usd"]}:
        raise RoutePublicationError("opportunity notional lineage mismatch")
    if classified.get("opportunity_id") != route_opportunity_id(route_id, notional):
        raise RoutePublicationError("opportunity identity is not canonical")
    if (
        classified.get("buy_core_manifest_sha256") != core_manifest_sha256
        or classified.get("sell_core_manifest_sha256") != core_manifest_sha256
    ):
        raise RoutePublicationError("opportunity core manifest lineage mismatch")

    for direction in ("buy", "sell"):
        quote = build_inputs.get(direction + "_quote")
        leg = build_inputs.get(direction + "_leg")
        market_id = str(route[direction + "_market_id"])
        core_leg = legs_by_market.get(market_id)
        if not isinstance(quote, QuantityQuote) or not isinstance(leg, Mapping) or core_leg is None:
            raise RoutePublicationError("opportunity leg input is invalid")
        if any(leg.get(key) != value for key, value in core_leg.items()):
            raise RoutePublicationError("opportunity leg does not match core")
        if (
            quote.market_id != market_id
            or quote.direction != direction
            or quote.raw_response_sha256 != core_leg.get("raw_response_sha256")
            or quote.snapshot_id != core_leg.get("snapshot_id")
            or quote.state_observed_at != core_leg.get("state_observed_at")
            or leg.get("state_id") != quote.state_id
            or classified.get(direction + "_state_id") != quote.state_id
        ):
            raise RoutePublicationError("opportunity quote lineage mismatch")

    raw_costs = build_inputs.get("cost_components")
    if isinstance(raw_costs, (str, bytes, Mapping)):
        raise RoutePublicationError("opportunity cost inventory is invalid")
    costs = [dict(row) for row in raw_costs]
    try:
        validate_cost_components(costs)
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("opportunity cost inventory is invalid") from error
    keys = {(str(row["leg"]), str(row["component_type"])) for row in costs}
    expected_keys = live_complete_cost_component_keys(route)
    if len(keys) != len(costs) or keys != expected_keys:
        raise RoutePublicationError("opportunity cost component set is not exact")
    if classified.get("cost_component_set_sha256") != _canonical_cost_set_sha256(costs):
        raise RoutePublicationError("opportunity cost component binding mismatch")
    return dict(classified), dict(build_inputs), costs


def _strict_cex_replay(
    *,
    classified: Mapping[str, Any],
    build_inputs: Mapping[str, Any],
    costs: Sequence[Mapping[str, Any]],
    source_members: Any,
    source_fd: int,
    raw_members: Mapping[str, Tuple[Dict[str, Any], bytes, str]],
    core_manifest_sha256: str,
    fee_profile_path: Path,
    fee_profile_id: str,
    inventory_rows: Sequence[Mapping[str, Any]],
    issue_attestation: bool = True,
) -> Dict[str, Any]:
    route = build_inputs["route"]
    if not _strict_cex_route_identity(route):
        return dict(classified)
    if (
        not isinstance(source_members, Mapping)
        or set(source_members) != _STRICT_CEX_SOURCE_MEMBERS
    ):
        return dict(classified)

    replayed_quotes: Dict[str, QuantityQuote] = {}
    for direction in ("buy", "sell"):
        market_id = str(route[direction + "_market_id"])
        raw_payload, raw_bytes, _raw_sha256 = raw_members[market_id]
        market, book = _parse_cex_book_source(
            raw_payload,
            raw_bytes,
            market_id=market_id,
            state_observed_at=build_inputs[direction + "_quote"].state_observed_at,
        )
        parsed_endpoint = urlsplit(book["source_endpoint"])
        safe_endpoint = urlunsplit((
            parsed_endpoint.scheme,
            parsed_endpoint.netloc,
            parsed_endpoint.path,
            "",
            "",
        ))
        if build_inputs[direction + "_leg"].get("source_endpoint") != safe_endpoint:
            raise RoutePublicationError("typed CEX endpoint does not match core leg")
        rules_payload, _rules_bytes, rules_sha256 = _read_member_from_root(
            source_fd,
            source_members[direction + "_market_rules"],
            label=direction + " market-rules source",
        )
        rules = _parse_market_rules_source(
            rules_payload,
            rules_sha256,
            market_id=market_id,
        )
        evidence = build_inputs[direction + "_quote_evidence"]
        supplied_fee = evidence.get("fee_semantics") if isinstance(evidence, Mapping) else None
        supplied_rules = evidence.get("market_rules") if isinstance(evidence, Mapping) else None
        supplied_book = evidence.get("book") if isinstance(evidence, Mapping) else None
        if supplied_rules != rules or supplied_book != book or not isinstance(supplied_fee, FeeSemantics):
            raise RoutePublicationError("typed CEX quote evidence does not match source")

        matching_costs = [
            row for row in costs
            if row["leg"] == direction and row["component_type"] == "venue_taker_fee"
        ]
        if len(matching_costs) != 1:
            raise RoutePublicationError("CEX venue fee component is not exact")
        venue = market_id.split(":", 2)[1]
        actual_fee = collect_cex_fee_snapshot(
            cohort_id=classified["cohort_id"],
            opportunity_id=classified["opportunity_id"],
            leg=direction,
            market_id=market_id,
            venue=venue,
            instrument=market_id.split(":", 2)[2],
            side=direction,
            requested_notional_usd=classified["requested_notional_usd"],
            target_token_quantity=classified["target_token_quantity"],
            now=build_inputs["now"],
            private_profile_path=fee_profile_path,
            profile_id=fee_profile_id,
        )
        if dict(actual_fee) != dict(matching_costs[0]):
            raise RoutePublicationError("CEX fee profile does not reproduce component")
        expected_basis = "received_base" if direction == "buy" else "received_quote"
        expected_increment = rules.base_increment if direction == "buy" else rules.quote_increment
        actual_basis = str(actual_fee["basis"])
        if "; fee_asset=" not in actual_basis:
            raise RoutePublicationError("CEX fee component has no exact fee asset")
        actual_fee_asset = actual_basis.rsplit("; fee_asset=", 1)[1]
        if (
            str(supplied_fee.rate_bps) != str(actual_fee["rate_bps"])
            or supplied_fee.fee_asset != actual_fee_asset
            or supplied_fee.source_record_sha256 != actual_fee["source_record_sha256"]
            or supplied_fee.observed_at != actual_fee["observed_at"]
            or supplied_fee.valid_until != actual_fee["valid_until"]
            or supplied_fee.charge_basis != expected_basis
            or supplied_fee.fee_increment != expected_increment
            or supplied_fee.rounding_mode != "ceiling"
            or supplied_fee.third_asset_quote_price is not None
        ):
            raise RoutePublicationError("CEX fee semantics do not match private profile")
        quote = route_quantity_quote_for_book(
            market,
            book,
            direction=direction,
            target_token_quantity=build_inputs["common_target"],
            market_rules=rules,
            fee_semantics=supplied_fee,
            snapshot_id=build_inputs[direction + "_quote"].snapshot_id,
            observed_at=build_inputs[direction + "_quote"].state_observed_at,
            cohort_now=build_inputs[direction + "_quote"].cohort_now,
            expected_state_id=build_inputs[direction + "_quote"].state_id,
        )
        if quote != build_inputs[direction + "_quote"]:
            raise RoutePublicationError("typed CEX book replay does not reproduce quote")
        replayed_quotes[direction] = quote

        usd_payload, _usd_bytes, usd_sha256 = _read_member_from_root(
            source_fd,
            source_members[direction + "_usd_conversion"],
            label=direction + " USD conversion source",
        )
        projection = build_inputs[direction + "_usd_projection"]
        quote_cash = (
            quote.quote_debit_quantity
            if direction == "buy"
            else quote.quote_received_quantity
        )
        if (
            set(usd_payload) != _USD_SOURCE_FIELDS
            or usd_payload.get("schema") != "route_usd_conversion_source/v1"
            or (quote_cash is None) != (projection is None)
            or (
                projection is not None
                and (
                    not isinstance(projection, Mapping)
                    or projection.get("source_record_sha256") != usd_sha256
                    or projection.get("quote_asset")
                    != usd_payload.get("quote_asset")
                    or projection.get("usd_per_quote")
                    != usd_payload.get("usd_per_quote")
                    or projection.get("observed_at")
                    != usd_payload.get("observed_at")
                    or projection.get("valid_until")
                    != usd_payload.get("valid_until")
                    or projection.get("source") != usd_payload.get("source")
                    or projection.get("core_manifest_sha256")
                    != core_manifest_sha256
                )
            )
        ):
            raise RoutePublicationError("typed USD conversion does not reproduce projection")

    buy_quote = replayed_quotes["buy"]
    sell_quote = replayed_quotes["sell"]
    if buy_quote.calculation_complete and sell_quote.calculation_complete:
        inventory = inventory_capacity_for_route(
            route,
            inventory_rows,
            buy_quote_asset=buy_quote.quote_debit_asset,
            buy_quote_quantity=buy_quote.quote_debit_quantity,
            sell_token_asset=sell_quote.target_base_asset,
            sell_net_token_quantity=sell_quote.base_debit_quantity,
            now=build_inputs["now"],
        )
        expected_request = {
            key: inventory[key]
            for key in (
                "route_id", "buy_market_id", "sell_market_id",
                "buy_quote_asset", "buy_quote_quantity", "sell_token_asset",
                "sell_net_token_quantity", "target_asset", "target_quantity",
            )
        }
        mode = classify_route_mode_evidence(
            route,
            expected_request=expected_request,
            inventory_evidence=inventory,
            now=build_inputs["now"],
        )
    else:
        mode = classify_route_mode_evidence(
            route,
            now=build_inputs["now"],
        )
    if (
        mode != build_inputs["mode_evidence"]
        or (issue_attestation and not mode.get("mode_evidence_eligible"))
    ):
        raise RoutePublicationError("inventory profile does not reproduce mode evidence")

    if not issue_attestation:
        return dict(classified)

    attestation = _issue_publication_attestation(
        cohort_id=classified["cohort_id"],
        opportunity_id=classified["opportunity_id"],
        route_id=classified["route_id"],
        target_token_quantity=classified["target_token_quantity"],
        buy_state_id=classified["buy_state_id"],
        sell_state_id=classified["sell_state_id"],
        buy_usd_projection_sha256=classified["buy_usd_projection_sha256"],
        sell_usd_projection_sha256=classified["sell_usd_projection_sha256"],
        cost_component_set_sha256=classified["cost_component_set_sha256"],
        mode_evidence_sha256=classified["mode_evidence_sha256"],
        core_manifest_sha256=core_manifest_sha256,
    )
    try:
        result = build_route_opportunity(
            **dict(build_inputs),
            publication_attestation=attestation,
        )
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("attested opportunity cannot be rebuilt") from error
    if not result.get("strict_eligible") or result.get("opportunity_class") != "executable_candidate":
        raise RoutePublicationError("attested opportunity did not become executable")
    return result


def build_complete_route_bundle(
    *,
    core_root: Path = DEFAULT_ROUTE_CORE_ROOT,
    raw_root: Path,
    opportunity_inputs: Iterable[Mapping[str, Any]],
    source_root: Optional[Path] = None,
    fee_profile_path: Optional[Path] = None,
    fee_profile_id: Optional[str] = None,
    inventory_profile_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Replay one pinned core into a closed complete opportunity generation."""
    core, core_fd, core_details = _open_verified_directory(
        Path(core_root),
        "route core root",
    )
    source_fd: Optional[int] = None
    source_details: Optional[os.stat_result] = None
    source_path: Optional[Path] = None
    try:
        try:
            fcntl.flock(core_fd, fcntl.LOCK_SH)
        except Exception as error:
            raise RoutePublicationError("route core lock acquisition failed") from error
        pointer_snapshot = _optional_pointer_snapshot_at(core_fd)
        if pointer_snapshot is None:
            raise RoutePublicationError("route core pointer is missing")
        loaded = load_latest_route_cohort(core)
        if not _pointer_snapshot_is_owned(
            _optional_pointer_snapshot_at(core_fd),
            pointer_snapshot,
        ):
            raise RoutePublicationError("route core pointer changed during finalization")
        cohort = loaded["cohort"]
        inputs = list(opportunity_inputs)
        expected_count = (
            len(cohort["routes"])
            * len(cohort["requested_notionals_usd"])
        )
        if (
            cohort["requested_notionals_usd"] != REQUESTED_NOTIONALS_USD
            or len(inputs) != expected_count
        ):
            raise RoutePublicationError(
                "every route must contain exactly five notional scenarios"
            )
        opportunity_now_values = {
            str(item.get("build_inputs", {}).get("now"))
            for item in inputs
            if isinstance(item, Mapping)
            and isinstance(item.get("build_inputs"), Mapping)
        }
        if len(opportunity_now_values) != 1:
            raise RoutePublicationError("opportunity evaluation time is not exact")
        opportunity_now = next(iter(opportunity_now_values))
        raw_members = _read_core_raw_members(raw_root, cohort)
        routes_by_id = {str(row["route_id"]): row for row in cohort["routes"]}
        legs_by_market = {str(row["market_id"]): row for row in cohort["legs"]}

        if source_root is not None:
            source_path, source_fd, source_details = _open_verified_directory(
                Path(source_root),
                "route typed-source root",
            )
        inventory_rows: Sequence[Mapping[str, Any]] = []
        if inventory_profile_path is not None:
            try:
                inventory_rows = load_validated_inventory_profile(
                    inventory_profile_path,
                    now=opportunity_now,
                )
            except (TypeError, ValueError) as error:
                raise RoutePublicationError("private inventory profile is invalid") from error
        fee_profile_rows: Sequence[Mapping[str, Any]] = []
        if fee_profile_path is not None:
            try:
                fee_profile_rows = load_validated_fee_profile(
                    fee_profile_path,
                    now=opportunity_now,
                )
            except (TypeError, ValueError) as error:
                raise RoutePublicationError("private fee profile is invalid") from error

        typed_source_records: Dict[Tuple[str, str], Dict[str, str]] = {}
        if source_fd is not None:
            for raw in inputs:
                members = raw.get("source_members") if isinstance(raw, Mapping) else None
                if not isinstance(members, Mapping):
                    continue
                for role, filename in members.items():
                    source_name = _require_relative_basename(
                        str(filename),
                        "typed source member",
                    )
                    _payload, _source_bytes, source_sha256 = _read_member_from_root(
                        source_fd,
                        source_name,
                        label="typed source member",
                    )
                    typed_source_records[(str(role), source_name)] = {
                        "role": str(role),
                        "filename": source_name,
                        "sha256": source_sha256,
                    }

        final_rows: List[Dict[str, Any]] = []
        all_costs: List[Dict[str, Any]] = []
        quote_inputs: List[Tuple[str, Decimal, str, Any]] = []
        scenario_keys = set()
        for raw in inputs:
            classified, build_inputs, costs = _validated_prepublication_input(
                raw,
                cohort=cohort,
                core_manifest_sha256=loaded["manifest_sha256"],
                routes_by_id=routes_by_id,
                legs_by_market=legs_by_market,
            )
            scenario_key = (
                classified["route_id"],
                str(classified["requested_notional_usd"]),
            )
            if scenario_key in scenario_keys:
                raise RoutePublicationError("duplicate route notional scenario")
            scenario_keys.add(scenario_key)
            source_members = raw.get("source_members")
            if (
                source_fd is None
                or fee_profile_path is None
                or fee_profile_id is None
                or inventory_profile_path is None
            ):
                final = dict(classified)
            else:
                final = _strict_cex_replay(
                    classified=classified,
                    build_inputs=build_inputs,
                    costs=costs,
                    source_members=source_members,
                    source_fd=source_fd,
                    raw_members=raw_members,
                    core_manifest_sha256=loaded["manifest_sha256"],
                    fee_profile_path=fee_profile_path,
                    fee_profile_id=fee_profile_id,
                    inventory_rows=inventory_rows,
                    issue_attestation=bool(
                        classified.get("strict_ready_for_publication")
                    ),
                )
            final_rows.append(final)
            all_costs.extend(costs)
            quote_inputs.extend([
                (
                    str(classified["route_id"]),
                    Decimal(str(classified["requested_notional_usd"])),
                    direction,
                    build_inputs[direction + "_quote"],
                )
                for direction in ("buy", "sell")
            ])

        expected_scenarios = {
            (str(route["route_id"]), str(notional))
            for route in cohort["routes"]
            for notional in cohort["requested_notionals_usd"]
        }
        if scenario_keys != expected_scenarios:
            raise RoutePublicationError(
                "every route must contain exactly five notional scenarios"
            )
        final_rows.sort(key=lambda row: (row["route_id"], Decimal(row["requested_notional_usd"])))
        all_costs.sort(key=lambda row: (
            row["opportunity_id"], row["leg"], row["component_type"]
        ))
        raw_members_after = _read_core_raw_members(raw_root, cohort)
        if any(
            raw_members_after[key][1:] != value[1:]
            for key, value in raw_members.items()
        ):
            raise RoutePublicationError("route raw evidence changed during finalization")
        if inventory_profile_path is not None:
            try:
                inventory_rows_after = load_validated_inventory_profile(
                    inventory_profile_path,
                    now=opportunity_now,
                )
            except (TypeError, ValueError) as error:
                raise RoutePublicationError("private inventory profile changed") from error
            if inventory_rows_after != inventory_rows:
                raise RoutePublicationError("private inventory profile changed")
        if fee_profile_path is not None:
            try:
                fee_profile_rows_after = load_validated_fee_profile(
                    fee_profile_path,
                    now=opportunity_now,
                )
            except (TypeError, ValueError) as error:
                raise RoutePublicationError("private fee profile changed") from error
            if fee_profile_rows_after != fee_profile_rows:
                raise RoutePublicationError("private fee profile changed")
        if source_fd is not None:
            for key in sorted(typed_source_records):
                record = typed_source_records[key]
                _payload, _value, digest = _read_member_from_root(
                    source_fd,
                    record["filename"],
                    label="typed source member",
                )
                if digest != record["sha256"]:
                    raise RoutePublicationError(
                        "typed source member changed during finalization"
                    )
            if source_path is None or source_details is None:
                raise RoutePublicationError("typed source root identity is invalid")
            _verify_open_path_identity(
                source_path, source_details, "route typed-source root"
            )
        reloaded = load_latest_route_cohort(core)
        if (
            reloaded["manifest_sha256"] != loaded["manifest_sha256"]
            or reloaded["cohort"] != cohort
            or not _pointer_snapshot_is_owned(
                _optional_pointer_snapshot_at(core_fd),
                pointer_snapshot,
            )
        ):
            raise RoutePublicationError("route core changed during finalization")
        _verify_open_path_identity(core, core_details, "route core root")

        generations = {
            "candidate_source_generation": cohort["candidate_source_generation"],
            "collection_input_generation": cohort["collection_input_generation"],
            "raw_evidence_run_id": cohort["raw_evidence_run_id"],
            "raw_evidence_generation": _canonical_input_sha256([
                {"market_id": market_id, "sha256": raw_members[market_id][2]}
                for market_id in sorted(raw_members)
            ]),
            "quantity_quote_generation": _canonical_input_sha256([
                record[3]
                for record in sorted(
                    quote_inputs,
                    key=lambda record: (record[0], record[1], record[2]),
                )
            ]),
            "cost_component_generation": _canonical_input_sha256(all_costs),
            "classified_opportunity_generation": _canonical_input_sha256(
                sorted(
                    (item["classified_opportunity"] for item in inputs),
                    key=lambda row: (
                        str(row["route_id"]),
                        Decimal(str(row["requested_notional_usd"])),
                    ),
                )
            ),
            "fee_profile_generation": _canonical_input_sha256(sorted(
                fee_profile_rows,
                key=_canonical_json_text,
            )),
            "inventory_profile_generation": _canonical_input_sha256(inventory_rows),
            "typed_source_generation": _canonical_input_sha256([
                typed_source_records[key] for key in sorted(typed_source_records)
            ]),
            "adapter_versions": dict(_COMPLETE_ADAPTER_VERSIONS),
        }
        return {
            "schema": "route_opportunity/v1",
            "route_cohort_id": cohort["route_cohort_id"],
            "core_manifest_sha256": loaded["manifest_sha256"],
            "core_pointer_sha256": _sha256_bytes(pointer_snapshot[0]),
            "core_context": {
                "candidate_source_generation": cohort["candidate_source_generation"],
                "collection_input_generation": cohort["collection_input_generation"],
                "raw_evidence_run_id": cohort["raw_evidence_run_id"],
                "collection_completed_at": cohort["collection_completed_at"],
                "collection_deadline_at": cohort["collection_deadline_at"],
            },
            "input_generations": generations,
            "routes": [dict(row) for row in cohort["routes"]],
            "legs": [dict(row) for row in cohort["legs"]],
            "cost_components": all_costs,
            "opportunities": final_rows,
        }
    finally:
        if source_fd is not None:
            os.close(source_fd)
        try:
            fcntl.flock(core_fd, fcntl.LOCK_UN)
        except Exception:
            pass
        os.close(core_fd)


COMPLETE_COST_COLUMNS = tuple(COST_COMPONENT_COLUMNS) + ("row_json",)
COMPLETE_OPPORTUNITY_COLUMNS = tuple(sorted(OPPORTUNITY_FIELDS)) + ("row_json",)


def _complete_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if type(value) is bool:
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        return _canonical_json_text(value)
    return str(value)


def _complete_cost_csv_row(row: Mapping[str, Any]) -> Dict[str, str]:
    result = {
        column: _complete_csv_value(row[column])
        for column in COST_COMPONENT_COLUMNS
    }
    result["row_json"] = _canonical_json_text(row)
    return result


def _complete_opportunity_csv_row(row: Mapping[str, Any]) -> Dict[str, str]:
    result = {
        column: _complete_csv_value(row[column])
        for column in sorted(OPPORTUNITY_FIELDS)
    }
    result["row_json"] = _canonical_json_text(row)
    return result


def _complete_database_logical_sha256(bundle: Mapping[str, Any]) -> str:
    return _sha256_bytes(_canonical_json_bytes({
        "schema": ROUTE_OPPORTUNITY_SQLITE_SCHEMA,
        "bundle": bundle,
    }))


_COMPLETE_SQLITE_DDL = (
    (
        "bundle_metadata",
        """CREATE TABLE bundle_metadata (
            key TEXT PRIMARY KEY NOT NULL,
            value_json TEXT NOT NULL
        ) WITHOUT ROWID""",
    ),
    (
        "route_legs",
        """CREATE TABLE route_legs (
            route_cohort_id TEXT NOT NULL,
            market_id TEXT PRIMARY KEY NOT NULL,
            row_json TEXT NOT NULL
        ) WITHOUT ROWID""",
    ),
    (
        "route_opportunities",
        """CREATE TABLE route_opportunities (
            cohort_id TEXT NOT NULL,
            route_id TEXT NOT NULL,
            opportunity_id TEXT PRIMARY KEY NOT NULL,
            requested_notional_usd TEXT NOT NULL,
            buy_market_id TEXT NOT NULL,
            sell_market_id TEXT NOT NULL,
            strict_eligible TEXT NOT NULL,
            row_json TEXT NOT NULL,
            FOREIGN KEY (buy_market_id) REFERENCES route_legs(market_id),
            FOREIGN KEY (sell_market_id) REFERENCES route_legs(market_id)
        ) WITHOUT ROWID""",
    ),
    (
        "cost_components",
        """CREATE TABLE cost_components (
            cohort_id TEXT NOT NULL,
            opportunity_id TEXT NOT NULL,
            leg TEXT NOT NULL,
            component_type TEXT NOT NULL,
            market_id TEXT NOT NULL,
            requested_notional_usd TEXT NOT NULL,
            row_json TEXT NOT NULL,
            PRIMARY KEY (opportunity_id, leg, component_type),
            FOREIGN KEY (opportunity_id)
                REFERENCES route_opportunities(opportunity_id)
        ) WITHOUT ROWID""",
    ),
    (
        "route_opportunities_route_idx",
        """CREATE INDEX route_opportunities_route_idx
            ON route_opportunities(route_id, requested_notional_usd)""",
    ),
    (
        "route_opportunities_strict_idx",
        """CREATE INDEX route_opportunities_strict_idx
            ON route_opportunities(strict_eligible, route_id)""",
    ),
    (
        "cost_components_market_idx",
        """CREATE INDEX cost_components_market_idx
            ON cost_components(market_id, component_type)""",
    ),
)


def _canonical_sqlite_ddl(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RoutePublicationError("complete SQLite schema is invalid")
    return " ".join(value.split())


def _build_complete_sqlite_file(
    path: Path,
    bundle: Mapping[str, Any],
) -> str:
    connection: Optional[sqlite3.Connection] = None
    try:
        connection = sqlite3.connect(str(path))
        connection.execute("PRAGMA page_size = 4096")
        connection.execute("PRAGMA auto_vacuum = NONE")
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA application_id = 1380929615")
        connection.execute("PRAGMA user_version = 1")
        connection.executescript(
            ";\n".join(statement for _name, statement in _COMPLETE_SQLITE_DDL)
            + ";\n"
        )
        connection.execute(
            "INSERT INTO bundle_metadata (key, value_json) VALUES (?, ?)",
            ("bundle", _canonical_json_text(bundle)),
        )
        cohort_id = str(bundle["route_cohort_id"])
        connection.executemany(
            "INSERT INTO route_legs VALUES (?, ?, ?)",
            (
                (cohort_id, row["market_id"], _canonical_json_text(row))
                for row in bundle["legs"]
            ),
        )
        connection.executemany(
            "INSERT INTO route_opportunities VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    row["cohort_id"], row["route_id"], row["opportunity_id"],
                    row["requested_notional_usd"], row["buy_market_id"],
                    row["sell_market_id"], _complete_csv_value(row["strict_eligible"]),
                    _canonical_json_text(row),
                )
                for row in bundle["opportunities"]
            ),
        )
        connection.executemany(
            "INSERT INTO cost_components VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    row["cohort_id"], row["opportunity_id"], row["leg"],
                    row["component_type"], row["market_id"],
                    row["requested_notional_usd"], _canonical_json_text(row),
                )
                for row in bundle["cost_components"]
            ),
        )
        connection.commit()
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise RoutePublicationError("complete SQLite foreign keys are invalid")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if not integrity or integrity[0] != "ok":
            raise RoutePublicationError("complete SQLite integrity check failed")
        connection.execute("VACUUM")
        connection.close()
        connection = None
        _fsync_file(path)
        return _complete_database_logical_sha256(bundle)
    except Exception:
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass
        _remove_sqlite_artifacts(path)
        raise


def _complete_manifest_payload(
    bundle: Mapping[str, Any],
    files: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    opportunities = bundle["opportunities"]
    return {
        "schema": ROUTE_OPPORTUNITY_MANIFEST_SCHEMA,
        "bundle_stage": ROUTE_OPPORTUNITY_BUNDLE_STAGE,
        "route_cohort_id": bundle["route_cohort_id"],
        "core_manifest_sha256": bundle["core_manifest_sha256"],
        "core_pointer_sha256": bundle["core_pointer_sha256"],
        "input_generations": dict(bundle["input_generations"]),
        "requested_notionals_usd": list(REQUESTED_NOTIONALS_USD),
        "counts": {
            "routes": len(bundle["routes"]),
            "markets": len(bundle["legs"]),
            "legs": len(bundle["legs"]),
            "opportunities": len(bundle["opportunities"]),
            "cost_components": len(bundle["cost_components"]),
            "classification": {
                "strict": sum(
                    row.get("strict_eligible") is True for row in opportunities
                ),
                "research": sum(
                    row.get("opportunity_class") == "research_estimate"
                    for row in opportunities
                ),
                "unavailable": sum(
                    row.get("opportunity_class") == "unavailable"
                    for row in opportunities
                ),
            },
            "cost_completeness": {
                "complete": sum(
                    row.get("cost_completeness") == "complete"
                    for row in opportunities
                ),
                "incomplete": sum(
                    row.get("cost_completeness") == "incomplete"
                    for row in opportunities
                ),
            },
            "scenario_cost_completeness": {
                "complete": sum(
                    row.get("scenario_cost_completeness") == "complete"
                    for row in opportunities
                ),
                "incomplete": sum(
                    row.get("scenario_cost_completeness") == "incomplete"
                    for row in opportunities
                ),
            },
        },
        "files": {name: dict(files[name]) for name in sorted(files)},
    }


def _complete_representation_artifact_bytes_from_validated_bundle(
    bundle: Mapping[str, Any],
) -> Tuple[Dict[str, bytes], Dict[str, Dict[str, Any]]]:
    leg_bytes = _csv_bytes(
        LEG_COLUMNS,
        (_leg_csv_row(bundle["route_cohort_id"], row) for row in bundle["legs"]),
    )
    cost_bytes = _csv_bytes(
        COMPLETE_COST_COLUMNS,
        (_complete_cost_csv_row(row) for row in bundle["cost_components"]),
    )
    opportunity_bytes = _csv_bytes(
        COMPLETE_OPPORTUNITY_COLUMNS,
        (_complete_opportunity_csv_row(row) for row in bundle["opportunities"]),
    )
    with tempfile.TemporaryDirectory(prefix="route-opportunity-sqlite-build-") as temporary:
        database = Path(temporary) / ROUTE_OPPORTUNITY_SQLITE_FILENAME
        sqlite_logical = _build_complete_sqlite_file(database, bundle)
        database_bytes = _read_bounded_bytes(
            database,
            limit=_MAX_SQLITE_BYTES,
            label="controlled complete route SQLite",
        )
    files = {
        ROUTE_LEGS_FILENAME: _artifact_details_bytes(
            leg_bytes,
            schema=ROUTE_LEG_CSV_SCHEMA,
            logical_sha256=_logical_rows_sha256(ROUTE_LEG_CSV_SCHEMA, bundle["legs"]),
            row_count=len(bundle["legs"]),
        ),
        COST_COMPONENTS_FILENAME: _artifact_details_bytes(
            cost_bytes,
            schema=COST_COMPONENT_CSV_SCHEMA,
            logical_sha256=_logical_rows_sha256(COST_COMPONENT_CSV_SCHEMA, bundle["cost_components"]),
            row_count=len(bundle["cost_components"]),
        ),
        ROUTE_OPPORTUNITIES_FILENAME: _artifact_details_bytes(
            opportunity_bytes,
            schema=ROUTE_OPPORTUNITY_CSV_SCHEMA,
            logical_sha256=_logical_rows_sha256(ROUTE_OPPORTUNITY_CSV_SCHEMA, bundle["opportunities"]),
            row_count=len(bundle["opportunities"]),
        ),
        ROUTE_OPPORTUNITY_SQLITE_FILENAME: _artifact_details_bytes(
            database_bytes,
            schema=ROUTE_OPPORTUNITY_SQLITE_SCHEMA,
            logical_sha256=sqlite_logical,
            row_count=(
                len(bundle["legs"])
                + len(bundle["cost_components"])
                + len(bundle["opportunities"])
            ),
        ),
    }
    return {
        ROUTE_LEGS_FILENAME: leg_bytes,
        COST_COMPONENTS_FILENAME: cost_bytes,
        ROUTE_OPPORTUNITIES_FILENAME: opportunity_bytes,
        ROUTE_OPPORTUNITY_SQLITE_FILENAME: database_bytes,
    }, files


def _complete_artifact_bytes(
    bundle: Mapping[str, Any],
) -> Tuple[Dict[str, bytes], Dict[str, Any]]:
    representation, files = (
        _complete_representation_artifact_bytes_from_validated_bundle(bundle)
    )
    manifest = _complete_manifest_payload(bundle, files)
    return {
        **representation,
        MANIFEST_FILENAME: json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8") + b"\n",
    }, manifest


_COMPLETE_BUNDLE_FIELDS = frozenset({
    "schema",
    "route_cohort_id",
    "core_manifest_sha256",
    "core_pointer_sha256",
    "core_context",
    "input_generations",
    "routes",
    "legs",
    "cost_components",
    "opportunities",
})
_COMPLETE_GENERATION_FIELDS = frozenset({
    "candidate_source_generation",
    "collection_input_generation",
    "raw_evidence_run_id",
    "raw_evidence_generation",
    "quantity_quote_generation",
    "cost_component_generation",
    "classified_opportunity_generation",
    "fee_profile_generation",
    "inventory_profile_generation",
    "typed_source_generation",
    "adapter_versions",
})
_COMPLETE_CONTEXT_FIELDS = frozenset({
    "candidate_source_generation",
    "collection_input_generation",
    "raw_evidence_run_id",
    "collection_completed_at",
    "collection_deadline_at",
})


def _opportunity_binding_sha256(row: Mapping[str, Any]) -> str:
    try:
        value = json.dumps(
            row,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise RoutePublicationError(
            "route opportunity contains invalid JSON data"
        ) from error
    return _sha256_bytes(value)


def _strict_cex_route_identity(route: Mapping[str, Any]) -> bool:
    buy_market_id = str(route.get("buy_market_id"))
    sell_market_id = str(route.get("sell_market_id"))
    return (
        route.get("route_mode") == "prepositioned_inventory"
        and buy_market_id.startswith("cex:")
        and sell_market_id.startswith("cex:")
        and not buy_market_id.startswith("cex:upbit:")
        and not sell_market_id.startswith("cex:upbit:")
        and buy_market_id.split(":", 2)[1]
        != sell_market_id.split(":", 2)[1]
    )


def _complete_opportunity_sort_key(row: Any) -> Tuple[str, Decimal]:
    if not isinstance(row, Mapping):
        raise RoutePublicationError("route opportunity must be an object")
    try:
        notional = Decimal(str(row.get("requested_notional_usd")))
    except Exception as error:
        raise RoutePublicationError("route opportunity notional is invalid") from error
    if not notional.is_finite():
        raise RoutePublicationError("route opportunity notional is invalid")
    return str(row.get("route_id")), notional


def _validate_complete_logical_bundle_shared(
    bundle: Any, *, historical_atomic: bool = False,
) -> Dict[str, Any]:
    if type(historical_atomic) is not bool:
        raise RoutePublicationError("complete route profile is invalid")
    if not isinstance(bundle, Mapping) or set(bundle) != _COMPLETE_BUNDLE_FIELDS:
        raise RoutePublicationError("complete route bundle schema is invalid")
    normalized = _clone_json(bundle)
    if _forbidden_row_keys(normalized):
        raise RoutePublicationError("complete route bundle contains unsafe evidence")
    if normalized.get("schema") != ROUTE_OPPORTUNITY_BUNDLE_STAGE:
        raise RoutePublicationError("complete route bundle stage is unsupported")
    cohort_id = normalized.get("route_cohort_id")
    if not isinstance(cohort_id, str) or _COHORT_ID.fullmatch(cohort_id) is None:
        raise RoutePublicationError("complete route cohort ID is invalid")
    for field in ("core_manifest_sha256", "core_pointer_sha256"):
        value = normalized.get(field)
        if not isinstance(value, str) or _HEX_SHA256.fullmatch(value) is None:
            raise RoutePublicationError("complete route core lineage is invalid")

    context = normalized.get("core_context")
    generations = normalized.get("input_generations")
    if not isinstance(context, dict) or set(context) != _COMPLETE_CONTEXT_FIELDS:
        raise RoutePublicationError("complete route core context is invalid")
    if not isinstance(generations, dict) or set(generations) != _COMPLETE_GENERATION_FIELDS:
        raise RoutePublicationError("complete route input generations are invalid")
    if (
        generations["candidate_source_generation"]
        != context["candidate_source_generation"]
        or generations["collection_input_generation"]
        != context["collection_input_generation"]
        or generations["raw_evidence_run_id"] != context["raw_evidence_run_id"]
    ):
        raise RoutePublicationError("complete route generation lineage conflicts")
    for field in (
        "raw_evidence_generation",
        "quantity_quote_generation",
        "cost_component_generation",
        "classified_opportunity_generation",
        "fee_profile_generation",
        "inventory_profile_generation",
        "typed_source_generation",
    ):
        value = generations.get(field)
        if not isinstance(value, str) or _HEX_SHA256.fullmatch(value) is None:
            raise RoutePublicationError("complete route generation hash is invalid")
    adapters = generations.get("adapter_versions")
    if adapters != _COMPLETE_ADAPTER_VERSIONS:
        raise RoutePublicationError("complete route adapter generation is invalid")

    routes = normalized.get("routes")
    legs = normalized.get("legs")
    costs = normalized.get("cost_components")
    opportunities = normalized.get("opportunities")
    if not all(isinstance(value, list) for value in (routes, legs, costs, opportunities)):
        raise RoutePublicationError("complete route inventories are invalid")
    if any(not isinstance(row, Mapping) for rows in (routes, legs, costs) for row in rows):
        raise RoutePublicationError("complete route inventory row is invalid")
    routes = sorted(routes, key=lambda row: str(row.get("route_id")))
    legs = sorted(legs, key=lambda row: str(row.get("market_id")))
    costs = sorted(costs, key=lambda row: (
        str(row.get("opportunity_id")), str(row.get("leg")),
        str(row.get("component_type")),
    ))
    opportunities = sorted(opportunities, key=_complete_opportunity_sort_key)
    if routes != normalized["routes"] or legs != normalized["legs"]:
        raise RoutePublicationError("complete route inventories are not canonical")
    if costs != normalized["cost_components"] or opportunities != normalized["opportunities"]:
        raise RoutePublicationError("complete opportunity inventories are not canonical")

    route_ids = set()
    for route in routes:
        route_id = _validate_route_candidate(
            route,
            candidate_generation=context["candidate_source_generation"],
            requested_notionals=REQUESTED_NOTIONALS_USD,
        )
        if route_id in route_ids:
            raise RoutePublicationError("duplicate complete route")
        route_ids.add(route_id)
    legs_by_market = _validate_leg_rows(
        legs,
        raw_evidence_run_id=context["raw_evidence_run_id"],
        collection_completed_at=context["collection_completed_at"],
        collection_deadline_at=context["collection_deadline_at"],
    )
    for route in routes:
        if (
            route["buy_market_id"] not in legs_by_market
            or route["sell_market_id"] not in legs_by_market
        ):
            raise RoutePublicationError("complete route leg inventory is not closed")

    if historical_atomic:
        if any(
            not isinstance(row, Mapping)
            or set(row) != set(COST_COMPONENT_COLUMNS)
            for row in costs
        ):
            raise RoutePublicationError("complete cost inventory is invalid")
    else:
        try:
            validate_cost_components(costs)
        except (TypeError, ValueError) as error:
            raise RoutePublicationError(
                "complete cost inventory is invalid"
            ) from error
    costs_by_opportunity: Dict[str, List[Dict[str, Any]]] = {}
    for row in costs:
        costs_by_opportunity.setdefault(str(row["opportunity_id"]), []).append(row)

    routes_by_id = {str(row["route_id"]): row for row in routes}
    observed_scenarios = set()
    opportunity_ids = set()
    for row in opportunities:
        if not isinstance(row, dict) or set(row) != OPPORTUNITY_FIELDS:
            raise RoutePublicationError("route opportunity schema is invalid")
        provided = dict(row)
        binding = provided.pop("evidence_binding_sha256", None)
        if (
            not isinstance(binding, str)
            or _HEX_SHA256.fullmatch(binding) is None
            or binding != _opportunity_binding_sha256(provided)
        ):
            raise RoutePublicationError("route opportunity evidence binding mismatch")
        route = routes_by_id.get(str(row["route_id"]))
        if route is None or any(
            row.get(field) != route.get(field)
            for field in (
                "route_id", "token_symbol", "buy_market_id",
                "sell_market_id", "route_mode",
            )
        ):
            raise RoutePublicationError("route opportunity lineage is invalid")
        if (
            row.get("cohort_id") != cohort_id
            or row.get("buy_core_manifest_sha256") != normalized["core_manifest_sha256"]
            or row.get("sell_core_manifest_sha256") != normalized["core_manifest_sha256"]
        ):
            raise RoutePublicationError("route opportunity core binding is invalid")
        if (
            type(row.get("strict_eligible")) is not bool
            or type(row.get("strict_ready_for_publication")) is not bool
            or row.get("opportunity_class") not in {
                "executable_candidate", "research_estimate", "unavailable"
            }
            or row.get("cost_completeness") not in {"complete", "incomplete"}
            or row.get("scenario_cost_completeness") not in {"complete", "incomplete"}
        ):
            raise RoutePublicationError("route opportunity classification is invalid")
        notional = str(row.get("requested_notional_usd"))
        scenario = (str(row["route_id"]), notional)
        if scenario in observed_scenarios:
            raise RoutePublicationError("duplicate route opportunity scenario")
        observed_scenarios.add(scenario)
        opportunity_id = route_opportunity_id(row["route_id"], notional)
        if row.get("opportunity_id") != opportunity_id or opportunity_id in opportunity_ids:
            raise RoutePublicationError("route opportunity identity is invalid")
        opportunity_ids.add(opportunity_id)
        component_rows = costs_by_opportunity.get(opportunity_id, [])
        expected_component_keys = (
            frozenset(
                (leg, component)
                for leg, component, _status, _embedded
                in HISTORICAL_ATOMIC_COMPONENT_MATRIX
            )
            if historical_atomic
            else live_complete_cost_component_keys(route)
        )
        historical_order = tuple(
            (leg, component)
            for leg, component, _status, _embedded
            in HISTORICAL_ATOMIC_COMPONENT_MATRIX
        )
        historical_rows = (
            sorted(
                (dict(item) for item in component_rows),
                key=lambda item: historical_order.index(
                    (item["leg"], item["component_type"])
                ),
            )
            if historical_atomic
            and {
                (item["leg"], item["component_type"])
                for item in component_rows
            } == expected_component_keys
            else []
        )
        expected_cost_binding = (
            _canonical_input_sha256(historical_rows)
            if historical_atomic
            else _canonical_cost_set_sha256(component_rows)
        )
        if (
            {(item["leg"], item["component_type"]) for item in component_rows}
            != expected_component_keys
            or historical_atomic and (
                len(component_rows) != len(HISTORICAL_ATOMIC_COMPONENT_MATRIX)
                or any(
                    type(item["embedded_in_leg_quote"]) is not bool
                    for item in historical_rows
                )
                or tuple(
                    (
                        item["leg"], item["component_type"],
                        item["value_status"],
                        item["embedded_in_leg_quote"],
                    )
                    for item in historical_rows
                ) != HISTORICAL_ATOMIC_COMPONENT_MATRIX
                or any(
                    item.get("contract_version")
                    != COST_COMPONENT_CONTRACT_VERSION
                    or item.get("cohort_id") != row["cohort_id"]
                    or item.get("opportunity_id")
                    != row["opportunity_id"]
                    or item.get("requested_notional_usd")
                    != row["requested_notional_usd"]
                    or item.get("target_token_quantity")
                    != row["target_token_quantity"]
                    or item.get("market_id") != (
                        row["buy_market_id"] if item["leg"] == "buy"
                        else row["sell_market_id"]
                        if item["leg"] == "sell" else ""
                    )
                    for item in historical_rows
                )
            )
            or row.get("cost_component_set_sha256")
            != expected_cost_binding
        ):
            raise RoutePublicationError("route opportunity cost binding is invalid")
        if row.get("strict_eligible"):
            strict_cex_route = _strict_cex_route_identity(route)
            try:
                expected_attestation = _publication_binding_sha256(
                    cohort_id=row["cohort_id"],
                    opportunity_id=row["opportunity_id"],
                    route_id=row["route_id"],
                    target_token_quantity=row["target_token_quantity"],
                    buy_state_id=row["buy_state_id"],
                    sell_state_id=row["sell_state_id"],
                    buy_usd_projection_sha256=row["buy_usd_projection_sha256"],
                    sell_usd_projection_sha256=row["sell_usd_projection_sha256"],
                    cost_component_set_sha256=row["cost_component_set_sha256"],
                    mode_evidence_sha256=row["mode_evidence_sha256"],
                    core_manifest_sha256=normalized["core_manifest_sha256"],
                )
            except (TypeError, ValueError) as error:
                raise RoutePublicationError(
                    "strict route opportunity attestation binding is invalid"
                ) from error
            if row.get("publication_attestation_sha256") != expected_attestation:
                raise RoutePublicationError(
                    "strict route opportunity attestation binding is invalid"
                )
            if (
                not strict_cex_route
                or row.get("opportunity_class") != "executable_candidate"
                or row.get("strict_ready_for_publication") is not True
                or not isinstance(row.get("publication_attestation_sha256"), str)
                or _HEX_SHA256.fullmatch(row["publication_attestation_sha256"]) is None
            ):
                raise RoutePublicationError("strict route opportunity is invalid")
        elif (
            row.get("publication_attestation_sha256") is not None
            or row.get("opportunity_class") == "executable_candidate"
        ):
            raise RoutePublicationError("research route opportunity is unexpectedly attested")

    expected_scenarios = {
        (str(route["route_id"]), str(notional))
        for route in routes for notional in REQUESTED_NOTIONALS_USD
    }
    if observed_scenarios != expected_scenarios:
        raise RoutePublicationError("every route must contain exactly five notional scenarios")
    if set(costs_by_opportunity) != opportunity_ids:
        raise RoutePublicationError("complete cost inventory contains orphan rows")
    normalized["routes"] = routes
    normalized["legs"] = legs
    normalized["cost_components"] = costs
    normalized["opportunities"] = opportunities
    return normalized


def _validate_complete_logical_bundle(bundle: Any) -> Dict[str, Any]:
    """Validate the unchanged live complete-bundle logical contract."""
    return _validate_complete_logical_bundle_shared(bundle)


def _read_complete_sqlite(
    value: bytes,
) -> Tuple[
    Dict[str, Any],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
]:
    with tempfile.TemporaryDirectory(prefix="route-complete-sqlite-read-") as temporary:
        database = Path(temporary) / ROUTE_OPPORTUNITY_SQLITE_FILENAME
        _write_new_bytes(database, value)
        connection: Optional[sqlite3.Connection] = None
        try:
            connection = sqlite3.connect(_sqlite_read_only_uri(database), uri=True)
            connection.execute("PRAGMA query_only = ON")
            if connection.execute("PRAGMA application_id").fetchone()[0] != 1380929615:
                raise RoutePublicationError("complete SQLite application ID is invalid")
            if connection.execute("PRAGMA user_version").fetchone()[0] != 1:
                raise RoutePublicationError("complete SQLite version is invalid")
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if not integrity or integrity[0] != "ok":
                raise RoutePublicationError("complete SQLite integrity check failed")
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise RoutePublicationError("complete SQLite foreign keys are invalid")
            schema_rows = connection.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master"
            ).fetchall()
            objects = {tuple(row[:3]) for row in schema_rows}
            expected_objects = {
                ("table", "bundle_metadata", "bundle_metadata"),
                ("table", "route_legs", "route_legs"),
                ("table", "route_opportunities", "route_opportunities"),
                ("table", "cost_components", "cost_components"),
                ("index", "route_opportunities_route_idx", "route_opportunities"),
                ("index", "route_opportunities_strict_idx", "route_opportunities"),
                ("index", "cost_components_market_idx", "cost_components"),
            }
            if objects != expected_objects:
                raise RoutePublicationError("complete SQLite schema is invalid")
            actual_ddl = {
                row[1]: _canonical_sqlite_ddl(row[3])
                for row in schema_rows
            }
            expected_ddl = {
                name: _canonical_sqlite_ddl(statement)
                for name, statement in _COMPLETE_SQLITE_DDL
            }
            if actual_ddl != expected_ddl:
                raise RoutePublicationError("complete SQLite schema is invalid")
            expected_columns = {
                "bundle_metadata": (
                    (0, "key", "TEXT", 1, None, 1),
                    (1, "value_json", "TEXT", 1, None, 0),
                ),
                "route_legs": (
                    (0, "route_cohort_id", "TEXT", 1, None, 0),
                    (1, "market_id", "TEXT", 1, None, 1),
                    (2, "row_json", "TEXT", 1, None, 0),
                ),
                "route_opportunities": (
                    (0, "cohort_id", "TEXT", 1, None, 0),
                    (1, "route_id", "TEXT", 1, None, 0),
                    (2, "opportunity_id", "TEXT", 1, None, 1),
                    (3, "requested_notional_usd", "TEXT", 1, None, 0),
                    (4, "buy_market_id", "TEXT", 1, None, 0),
                    (5, "sell_market_id", "TEXT", 1, None, 0),
                    (6, "strict_eligible", "TEXT", 1, None, 0),
                    (7, "row_json", "TEXT", 1, None, 0),
                ),
                "cost_components": (
                    (0, "cohort_id", "TEXT", 1, None, 0),
                    (1, "opportunity_id", "TEXT", 1, None, 1),
                    (2, "leg", "TEXT", 1, None, 2),
                    (3, "component_type", "TEXT", 1, None, 3),
                    (4, "market_id", "TEXT", 1, None, 0),
                    (5, "requested_notional_usd", "TEXT", 1, None, 0),
                    (6, "row_json", "TEXT", 1, None, 0),
                ),
            }
            for table, expected in expected_columns.items():
                actual = tuple(tuple(row[:6]) for row in connection.execute(
                    "PRAGMA table_info({})".format(table)
                ).fetchall())
                if actual != expected:
                    raise RoutePublicationError("complete SQLite schema is invalid")
            expected_indexes = {
                "route_opportunities_route_idx": (
                    "route_id", "requested_notional_usd"
                ),
                "route_opportunities_strict_idx": (
                    "strict_eligible", "route_id"
                ),
                "cost_components_market_idx": (
                    "market_id", "component_type"
                ),
            }
            for index, expected in expected_indexes.items():
                actual = tuple(
                    row[2] for row in connection.execute(
                        "PRAGMA index_info({})".format(index)
                    ).fetchall()
                )
                if actual != expected:
                    raise RoutePublicationError("complete SQLite schema is invalid")
            foreign_keys = {
                table: {
                    (row[3], row[2], row[4])
                    for row in connection.execute(
                        "PRAGMA foreign_key_list({})".format(table)
                    ).fetchall()
                }
                for table in ("route_legs", "route_opportunities", "cost_components")
            }
            if foreign_keys != {
                "route_legs": set(),
                "route_opportunities": {
                    ("buy_market_id", "route_legs", "market_id"),
                    ("sell_market_id", "route_legs", "market_id"),
                },
                "cost_components": {
                    ("opportunity_id", "route_opportunities", "opportunity_id"),
                },
            }:
                raise RoutePublicationError("complete SQLite foreign keys are invalid")
            metadata = connection.execute(
                "SELECT key, value_json FROM bundle_metadata"
            ).fetchall()
            if len(metadata) != 1 or metadata[0][0] != "bundle":
                raise RoutePublicationError("complete SQLite metadata inventory is invalid")
            bundle = _decode_json_object_bytes(
                metadata[0][1].encode("utf-8"),
                label="complete SQLite bundle metadata",
            )
            if _canonical_json_text(bundle) != metadata[0][1]:
                raise RoutePublicationError("complete SQLite metadata is not canonical")

            leg_records = connection.execute(
                "SELECT route_cohort_id, market_id, row_json FROM route_legs"
            ).fetchall()
            opportunity_records = connection.execute(
                "SELECT cohort_id, route_id, opportunity_id, requested_notional_usd, "
                "buy_market_id, sell_market_id, strict_eligible, row_json "
                "FROM route_opportunities"
            ).fetchall()
            cost_records = connection.execute(
                "SELECT cohort_id, opportunity_id, leg, component_type, market_id, "
                "requested_notional_usd, row_json FROM cost_components"
            ).fetchall()
        except sqlite3.Error as error:
            raise RoutePublicationError("complete SQLite is invalid") from error
        finally:
            if connection is not None:
                connection.close()

    legs = [_decode_row_json(row[-1], "complete SQLite route leg") for row in leg_records]
    opportunities = [
        _decode_row_json(row[-1], "complete SQLite opportunity")
        for row in opportunity_records
    ]
    costs = [_decode_row_json(row[-1], "complete SQLite cost") for row in cost_records]
    for record, row in zip(leg_records, legs):
        if record[:-1] != (bundle["route_cohort_id"], row["market_id"]):
            raise RoutePublicationError("complete SQLite route leg projection mismatch")
    for record, row in zip(opportunity_records, opportunities):
        if record[:-1] != (
            row["cohort_id"], row["route_id"], row["opportunity_id"],
            row["requested_notional_usd"], row["buy_market_id"],
            row["sell_market_id"], _complete_csv_value(row["strict_eligible"]),
        ):
            raise RoutePublicationError("complete SQLite opportunity projection mismatch")
    for record, row in zip(cost_records, costs):
        if record[:-1] != (
            row["cohort_id"], row["opportunity_id"], row["leg"],
            row["component_type"], row["market_id"],
            row["requested_notional_usd"],
        ):
            raise RoutePublicationError("complete SQLite cost projection mismatch")
    return (
        bundle,
        sorted(legs, key=lambda row: row["market_id"]),
        sorted(costs, key=lambda row: (
            row["opportunity_id"], row["leg"], row["component_type"],
        )),
        sorted(opportunities, key=_complete_opportunity_sort_key),
    )


def _read_complete_representation_bytes(
    *, file_bytes: Mapping[str, bytes], route_cohort_id: str,
) -> Tuple[
    Dict[str, Any],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
]:
    if set(file_bytes) != _COMPLETE_MANIFEST_ARTIFACT_FILENAMES:
        raise RoutePublicationError(
            "complete representation file inventory is invalid"
        )
    raw_legs = _read_csv_rows_bytes(
        file_bytes[ROUTE_LEGS_FILENAME], columns=LEG_COLUMNS,
        label="complete route leg CSV",
    )
    raw_costs = _read_csv_rows_bytes(
        file_bytes[COST_COMPONENTS_FILENAME], columns=COMPLETE_COST_COLUMNS,
        label="complete cost CSV",
    )
    raw_opportunities = _read_csv_rows_bytes(
        file_bytes[ROUTE_OPPORTUNITIES_FILENAME],
        columns=COMPLETE_OPPORTUNITY_COLUMNS,
        label="complete opportunity CSV",
    )
    legs = [
        _decode_row_json(row["row_json"], "complete route leg CSV")
        for row in raw_legs
    ]
    costs = [
        _decode_row_json(row["row_json"], "complete cost CSV")
        for row in raw_costs
    ]
    opportunities = [
        _decode_row_json(row["row_json"], "complete opportunity CSV")
        for row in raw_opportunities
    ]
    _validate_csv_projection(
        raw_legs, legs, route_cohort_id=route_cohort_id,
        projector=_leg_csv_row, label="complete route leg CSV",
    )
    if any(
        dict(csv_row) != _complete_cost_csv_row(row)
        for csv_row, row in zip(raw_costs, costs)
    ):
        raise RoutePublicationError("complete cost CSV projection mismatch")
    if any(
        dict(csv_row) != _complete_opportunity_csv_row(row)
        for csv_row, row in zip(raw_opportunities, opportunities)
    ):
        raise RoutePublicationError(
            "complete opportunity CSV projection mismatch"
        )

    (
        bundle,
        sqlite_legs,
        sqlite_costs,
        sqlite_opportunities,
    ) = _read_complete_sqlite(
        file_bytes[ROUTE_OPPORTUNITY_SQLITE_FILENAME],
    )
    if (
        sqlite_legs != legs
        or sqlite_costs != costs
        or sqlite_opportunities != opportunities
    ):
        raise RoutePublicationError(
            "complete CSV and SQLite inventories do not match"
        )
    return bundle, legs, costs, opportunities


def _validate_complete_route_bundle_at(
    parent_fd: int,
    bundle_name: str,
    bundle_path: Path,
    *,
    expected_route_cohort_id: Optional[str],
    expected_manifest_sha256: Optional[str],
    require_directory_identity: bool,
) -> Dict[str, Any]:
    bundle_fd, bundle_details = _open_directory_at(
        parent_fd, bundle_name, "complete route bundle"
    )
    file_fds: Dict[str, int] = {}
    try:
        if set(os.listdir(bundle_fd)) != ROUTE_COMPLETE_FILENAMES:
            raise RoutePublicationError("complete route bundle file inventory is invalid")
        read_specs = {
            MANIFEST_FILENAME: (_MAX_JSON_BYTES, "complete route manifest"),
            ROUTE_LEGS_FILENAME: (_MAX_CSV_BYTES, "complete route leg CSV"),
            COST_COMPONENTS_FILENAME: (_MAX_CSV_BYTES, "complete cost CSV"),
            ROUTE_OPPORTUNITIES_FILENAME: (_MAX_CSV_BYTES, "complete opportunity CSV"),
            ROUTE_OPPORTUNITY_SQLITE_FILENAME: (_MAX_SQLITE_BYTES, "complete route SQLite"),
        }
        file_bytes: Dict[str, bytes] = {}
        file_hashes: Dict[str, str] = {}
        file_details: Dict[str, os.stat_result] = {}
        for filename, (limit, label) in read_specs.items():
            source_fd, before = _open_regular_file_at(bundle_fd, filename, label=label)
            file_fds[filename] = source_fd
            value, digest, after = _read_bounded_open_file(
                source_fd, before, limit=limit, label=label
            )
            current = os.stat(filename, dir_fd=bundle_fd, follow_symlinks=False)
            if _stable_file_metadata(current) != _stable_file_metadata(after):
                raise RoutePublicationError(label + " changed during validation")
            file_bytes[filename] = value
            file_hashes[filename] = digest
            file_details[filename] = after

        manifest_sha256 = file_hashes[MANIFEST_FILENAME]
        if expected_manifest_sha256 is not None and manifest_sha256 != expected_manifest_sha256:
            raise RoutePublicationError("complete route manifest hash does not match pointer")
        manifest = _decode_json_object_bytes(
            file_bytes[MANIFEST_FILENAME], label="complete route manifest"
        )
        if set(manifest) != {
            "schema", "bundle_stage", "route_cohort_id", "core_manifest_sha256",
            "core_pointer_sha256", "input_generations", "requested_notionals_usd",
            "counts", "files",
        }:
            raise RoutePublicationError("complete route manifest schema is invalid")
        cohort_id = manifest.get("route_cohort_id")
        if not isinstance(cohort_id, str) or _COHORT_ID.fullmatch(cohort_id) is None:
            raise RoutePublicationError("complete route manifest cohort ID is invalid")
        if expected_route_cohort_id is not None and cohort_id != expected_route_cohort_id:
            raise RoutePublicationError("complete route manifest cohort ID does not match pointer")
        if require_directory_identity and bundle_name != cohort_id:
            raise RoutePublicationError("complete route bundle directory identity is invalid")
        files = manifest.get("files")
        if not isinstance(files, dict) or set(files) != _COMPLETE_MANIFEST_ARTIFACT_FILENAMES:
            raise RoutePublicationError("complete route manifest file inventory is invalid")
        for filename, details in files.items():
            if not isinstance(details, dict) or set(details) != {
                "schema", "sha256", "logical_sha256", "row_count"
            } or details.get("sha256") != file_hashes[filename]:
                raise RoutePublicationError("complete route file checksum is invalid")

        representation = {
            filename: file_bytes[filename]
            for filename in _COMPLETE_MANIFEST_ARTIFACT_FILENAMES
        }
        bundle, legs, costs, opportunities = (
            _read_complete_representation_bytes(
                file_bytes=representation, route_cohort_id=cohort_id,
            )
        )
        bundle = _validate_complete_logical_bundle(bundle)
        if (
            legs != bundle["legs"] or costs != bundle["cost_components"]
            or opportunities != bundle["opportunities"]
        ):
            raise RoutePublicationError("complete CSV and SQLite inventories do not match")
        expected_files = {
            ROUTE_LEGS_FILENAME: _artifact_details_bytes(
                file_bytes[ROUTE_LEGS_FILENAME], schema=ROUTE_LEG_CSV_SCHEMA,
                logical_sha256=_logical_rows_sha256(ROUTE_LEG_CSV_SCHEMA, legs),
                row_count=len(legs),
            ),
            COST_COMPONENTS_FILENAME: _artifact_details_bytes(
                file_bytes[COST_COMPONENTS_FILENAME], schema=COST_COMPONENT_CSV_SCHEMA,
                logical_sha256=_logical_rows_sha256(COST_COMPONENT_CSV_SCHEMA, costs),
                row_count=len(costs),
            ),
            ROUTE_OPPORTUNITIES_FILENAME: _artifact_details_bytes(
                file_bytes[ROUTE_OPPORTUNITIES_FILENAME], schema=ROUTE_OPPORTUNITY_CSV_SCHEMA,
                logical_sha256=_logical_rows_sha256(ROUTE_OPPORTUNITY_CSV_SCHEMA, opportunities),
                row_count=len(opportunities),
            ),
            ROUTE_OPPORTUNITY_SQLITE_FILENAME: _artifact_details_bytes(
                file_bytes[ROUTE_OPPORTUNITY_SQLITE_FILENAME], schema=ROUTE_OPPORTUNITY_SQLITE_SCHEMA,
                logical_sha256=_complete_database_logical_sha256(bundle),
                row_count=len(legs) + len(costs) + len(opportunities),
            ),
        }
        expected_manifest = _complete_manifest_payload(bundle, expected_files)
        if manifest != expected_manifest:
            raise RoutePublicationError("complete route manifest does not match bundle content")
        _verify_bundle_file_snapshots(
            bundle_fd, read_specs, file_fds, file_details, file_bytes, file_hashes,
            ROUTE_COMPLETE_FILENAMES,
        )
        _verify_directory_entry(
            parent_fd, bundle_name, bundle_details, "complete route bundle"
        )
        return {
            "path": bundle_path,
            "manifest_sha256": manifest_sha256,
            "manifest": manifest,
            "bundle": bundle,
            "legs": legs,
            "cost_components": costs,
            "opportunities": opportunities,
            "database_path": bundle_path / ROUTE_OPPORTUNITY_SQLITE_FILENAME,
        }
    finally:
        for file_fd in file_fds.values():
            os.close(file_fd)
        os.close(bundle_fd)


def _validate_complete_route_bundle(
    bundle_path: Path,
    *,
    expected_route_cohort_id: Optional[str],
    expected_manifest_sha256: Optional[str],
    require_directory_identity: bool,
    parent_fd: Optional[int] = None,
) -> Dict[str, Any]:
    bundle = _absolute_without_symlink_resolution(Path(bundle_path))
    _require_relative_basename(bundle.name, "complete route bundle name")
    owned_fd: Optional[int] = None
    parent_path: Optional[Path] = None
    parent_details: Optional[os.stat_result] = None
    try:
        if parent_fd is None:
            parent_path, owned_fd, parent_details = _open_verified_directory(
                bundle.parent, "complete route bundles root"
            )
            parent_fd = owned_fd
        result = _validate_complete_route_bundle_at(
            parent_fd, bundle.name, bundle,
            expected_route_cohort_id=expected_route_cohort_id,
            expected_manifest_sha256=expected_manifest_sha256,
            require_directory_identity=require_directory_identity,
        )
        if parent_path is not None and parent_details is not None:
            _verify_open_path_identity(
                parent_path, parent_details, "complete route bundles root"
            )
        return result
    finally:
        if owned_fd is not None:
            os.close(owned_fd)


def validate_complete_route_bundle(
    bundle_path: Path,
    *,
    expected_route_cohort_id: Optional[str] = None,
    expected_manifest_sha256: Optional[str] = None,
) -> Dict[str, Any]:
    """Fully reread all five members of one immutable opportunity bundle."""
    return _validate_complete_route_bundle(
        bundle_path,
        expected_route_cohort_id=expected_route_cohort_id,
        expected_manifest_sha256=expected_manifest_sha256,
        require_directory_identity=True,
    )


def _write_complete_bundle_artifacts_at(
    stage_fd: int,
    artifacts: Mapping[str, bytes],
) -> None:
    if set(artifacts) != ROUTE_COMPLETE_FILENAMES:
        raise RoutePublicationError("complete route artifact inventory is invalid")
    for filename in sorted(artifacts):
        _write_new_bytes_at(stage_fd, filename, artifacts[filename])


def _restore_pointer_after_failure(
    routes_fd: int,
    routes_path: Path,
    old_pointer: Optional[Tuple[bytes, os.stat_result]],
    attempted_pointer_bytes: bytes,
) -> None:
    current = _optional_pointer_snapshot_at(routes_fd)
    old_bytes = None if old_pointer is None else old_pointer[0]
    if (None if current is None else current[0]) == old_bytes:
        return
    if current is None or current[0] != attempted_pointer_bytes:
        raise RoutePublicationError(
            "complete route pointer commit is uncertain due to a concurrent writer"
        )
    if old_bytes is None:
        os.unlink("latest.json", dir_fd=routes_fd)
    else:
        _replace_pointer_bytes_at(routes_fd, old_bytes)
    _fsync_directory(routes_path, directory_fd=routes_fd)
    restored = _optional_pointer_snapshot_at(routes_fd)
    if (None if restored is None else restored[0]) != old_bytes:
        raise RoutePublicationError("complete route pointer rollback failed")


def _pointer_payload_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8") + b"\n"


def _canonical_core_pointer_sha256(
    route_cohort_id: str,
    core_manifest_sha256: str,
) -> str:
    """Hash the only canonical pointer bytes for one immutable core bundle."""
    return _sha256_bytes(_pointer_payload_bytes({
        "schema": ROUTE_CORE_POINTER_SCHEMA,
        "bundle_stage": ROUTE_CORE_BUNDLE_STAGE,
        "route_cohort_id": route_cohort_id,
        "manifest_sha256": core_manifest_sha256,
    }))


def _commit_complete_pointer_at_locked(
    routes_fd: int,
    routes_path: Path,
    pointer_bytes: bytes,
) -> None:
    _replace_pointer_bytes_at(routes_fd, pointer_bytes)
    committed = _optional_pointer_snapshot_at(routes_fd)
    if committed is None or committed[0] != pointer_bytes:
        raise RoutePublicationError("complete route pointer commit is uncertain")
    _fsync_directory(routes_path, directory_fd=routes_fd)
    if not _pointer_snapshot_is_owned(
        _optional_pointer_snapshot_at(routes_fd), committed
    ):
        raise RoutePublicationError("complete route pointer commit is uncertain")


def _verify_complete_core_lineage(
    core_root: Path,
    *,
    expected_manifest_sha256: str,
    expected_pointer_sha256: str,
) -> None:
    core, core_fd, core_details = _open_verified_directory(
        Path(core_root), "route core root"
    )
    try:
        try:
            fcntl.flock(core_fd, fcntl.LOCK_SH)
        except Exception as error:
            raise RoutePublicationError("route core lock acquisition failed") from error
        snapshot = _optional_pointer_snapshot_at(core_fd)
        if snapshot is None:
            raise RoutePublicationError("route core pointer is missing")
        loaded = load_latest_route_cohort(core)
        if (
            loaded["manifest_sha256"] != expected_manifest_sha256
            or _sha256_bytes(snapshot[0]) != expected_pointer_sha256
            or not _pointer_snapshot_is_owned(
                _optional_pointer_snapshot_at(core_fd), snapshot
            )
        ):
            raise RoutePublicationError("route core changed before public pointer commit")
        _verify_open_path_identity(core, core_details, "route core root")
    finally:
        try:
            fcntl.flock(core_fd, fcntl.LOCK_UN)
        except Exception as error:
            os.close(core_fd)
            raise RoutePublicationError("route core lock release failed") from error
        os.close(core_fd)


def publish_complete_route_bundle(
    *,
    core_root: Path = DEFAULT_ROUTE_CORE_ROOT,
    routes_root: Path = DEFAULT_ROUTE_ROOT,
    raw_root: Path,
    opportunity_inputs: Iterable[Mapping[str, Any]],
    source_root: Optional[Path] = None,
    fee_profile_path: Optional[Path] = None,
    fee_profile_id: Optional[str] = None,
    inventory_profile_path: Optional[Path] = None,
    precommit_validator: Optional[Callable[[], None]] = None,
) -> Dict[str, Any]:
    """Build, stage, validate, then validate auxiliaries and move latest.

    ``precommit_validator`` runs under the routes lock immediately before the
    public pointer commit.  Its result is deliberately absent from the bundle.
    """
    inputs = list(opportunity_inputs)
    bundle = build_complete_route_bundle(
        core_root=core_root,
        raw_root=raw_root,
        source_root=source_root,
        fee_profile_path=fee_profile_path,
        fee_profile_id=fee_profile_id,
        inventory_profile_path=inventory_profile_path,
        opportunity_inputs=inputs,
    )
    bundle = _validate_complete_logical_bundle(bundle)
    artifacts, _manifest = _complete_artifact_bytes(bundle)
    expected_manifest_sha256 = _sha256_bytes(artifacts[MANIFEST_FILENAME])

    routes = _ensure_real_directory(Path(routes_root))
    routes, routes_fd, routes_details = _open_verified_directory(
        routes, "complete routes root"
    )
    bundles_fd: Optional[int] = None
    stage_fd: Optional[int] = None
    stage_name: Optional[str] = None
    stage_details: Optional[os.stat_result] = None
    renamed = False
    routes_locked = False
    old_pointer: Optional[Tuple[bytes, os.stat_result]] = None
    try:
        try:
            fcntl.flock(routes_fd, fcntl.LOCK_EX)
            routes_locked = True
        except Exception as error:
            raise RoutePublicationError(
                "complete routes lock acquisition failed"
            ) from error
        old_pointer = _optional_pointer_snapshot_at(routes_fd)
        bundles_fd, bundles_details = _ensure_directory_at(
            routes_fd, "bundles", "complete route bundles root"
        )
        bundles = routes / "bundles"
        cohort_id = bundle["route_cohort_id"]
        final_path = bundles / cohort_id
        if _entry_exists_at(bundles_fd, cohort_id):
            validated = _validate_complete_route_bundle(
                final_path,
                expected_route_cohort_id=cohort_id,
                expected_manifest_sha256=expected_manifest_sha256,
                require_directory_identity=True,
                parent_fd=bundles_fd,
            )
        else:
            stage_name, stage_path, stage_fd, stage_details = _make_unique_directory_at(
                bundles_fd, prefix=".route-opportunity-", display_parent=bundles
            )
            _write_complete_bundle_artifacts_at(stage_fd, artifacts)
            _validate_complete_route_bundle(
                stage_path,
                expected_route_cohort_id=cohort_id,
                expected_manifest_sha256=expected_manifest_sha256,
                require_directory_identity=False,
                parent_fd=bundles_fd,
            )
            _fsync_directory(stage_path, directory_fd=stage_fd)
            _rename_directory_noreplace(
                stage_path, final_path,
                source_dir_fd=bundles_fd, destination_dir_fd=bundles_fd,
            )
            renamed = True
            _verify_directory_entry(
                bundles_fd, cohort_id, stage_details, "complete route bundle"
            )
            _fsync_directory(bundles, directory_fd=bundles_fd)
            validated = _validate_complete_route_bundle(
                final_path,
                expected_route_cohort_id=cohort_id,
                expected_manifest_sha256=expected_manifest_sha256,
                require_directory_identity=True,
                parent_fd=bundles_fd,
            )

        # The public pointer may only bind the exact core pointer observed by build.
        _verify_complete_core_lineage(
            Path(core_root),
            expected_manifest_sha256=bundle["core_manifest_sha256"],
            expected_pointer_sha256=bundle["core_pointer_sha256"],
        )
        pointer = {
            "schema": ROUTE_OPPORTUNITY_POINTER_SCHEMA,
            "bundle_stage": ROUTE_OPPORTUNITY_BUNDLE_STAGE,
            "route_cohort_id": cohort_id,
            "manifest_sha256": validated["manifest_sha256"],
            "core_manifest_sha256": bundle["core_manifest_sha256"],
            "core_pointer_sha256": bundle["core_pointer_sha256"],
        }
        _verify_directory_entry(
            routes_fd, "bundles", bundles_details, "complete route bundles root"
        )
        _verify_open_path_identity(routes, routes_details, "complete routes root")
        pointer_bytes = _pointer_payload_bytes(pointer)
        try:
            if precommit_validator is not None:
                precommit_validator()
            _commit_complete_pointer_at_locked(routes_fd, routes, pointer_bytes)
        except BaseException:
            _restore_pointer_after_failure(
                routes_fd, routes, old_pointer, pointer_bytes
            )
            raise
        return pointer
    finally:
        if stage_fd is not None:
            os.close(stage_fd)
        if (
            not renamed and bundles_fd is not None and stage_name is not None
            and stage_details is not None
        ):
            _remove_stage_directory_at(bundles_fd, stage_name, stage_details)
        if bundles_fd is not None:
            os.close(bundles_fd)
        if routes_locked:
            try:
                fcntl.flock(routes_fd, fcntl.LOCK_UN)
            except Exception as error:
                os.close(routes_fd)
                raise RoutePublicationError(
                    "complete routes lock release failed"
                ) from error
        os.close(routes_fd)


def load_latest_complete_route_bundle(
    routes_root: Path = DEFAULT_ROUTE_ROOT,
    *,
    core_root: Optional[Path] = None,
) -> Dict[str, Any]:
    """Resolve only the public complete-bundle pointer and fully validate it."""
    routes, routes_fd, routes_details = _open_verified_directory(
        Path(routes_root), "complete routes root"
    )
    bundles_fd: Optional[int] = None
    try:
        pointer_bytes = _optional_pointer_bytes_at(routes_fd)
        if pointer_bytes is None:
            raise RoutePublicationError("complete route pointer is missing")
        pointer = _decode_json_object_bytes(
            pointer_bytes, label="complete route pointer"
        )
        if set(pointer) != {
            "schema", "bundle_stage", "route_cohort_id", "manifest_sha256",
            "core_manifest_sha256", "core_pointer_sha256",
        } or (
            pointer.get("schema") != ROUTE_OPPORTUNITY_POINTER_SCHEMA
            or pointer.get("bundle_stage") != ROUTE_OPPORTUNITY_BUNDLE_STAGE
        ):
            raise RoutePublicationError("complete route pointer schema is unsupported")
        cohort_id = pointer.get("route_cohort_id")
        manifest_sha256 = pointer.get("manifest_sha256")
        if not isinstance(cohort_id, str) or _COHORT_ID.fullmatch(cohort_id) is None:
            raise RoutePublicationError("complete route pointer cohort ID is path-unsafe")
        if not isinstance(manifest_sha256, str) or _HEX_SHA256.fullmatch(manifest_sha256) is None:
            raise RoutePublicationError("complete route pointer manifest hash is invalid")
        bundles_fd, bundles_details = _open_directory_at(
            routes_fd, "bundles", "complete route bundles root"
        )
        validated = _validate_complete_route_bundle(
            routes / "bundles" / cohort_id,
            expected_route_cohort_id=cohort_id,
            expected_manifest_sha256=manifest_sha256,
            require_directory_identity=True,
            parent_fd=bundles_fd,
        )
        if (
            validated["bundle"]["core_manifest_sha256"]
            != pointer.get("core_manifest_sha256")
            or validated["bundle"]["core_pointer_sha256"]
            != pointer.get("core_pointer_sha256")
        ):
            raise RoutePublicationError("complete route pointer core lineage mismatch")
        if validated["bundle"]["core_pointer_sha256"] != (
            _canonical_core_pointer_sha256(
                cohort_id,
                validated["bundle"]["core_manifest_sha256"],
            )
        ):
            raise RoutePublicationError(
                "complete core pointer lineage hash is invalid"
            )
        if core_root is not None:
            core = validate_route_cohort_bundle(
                Path(core_root) / "bundles" / cohort_id,
                expected_route_cohort_id=cohort_id,
                expected_manifest_sha256=validated["bundle"][
                    "core_manifest_sha256"
                ],
            )
            if core["candidates"] != validated["bundle"]["routes"]:
                raise RoutePublicationError(
                    "complete routes differ from pinned core lineage"
                )
            if core["legs"] != validated["bundle"]["legs"]:
                raise RoutePublicationError(
                    "complete route legs differ from pinned core lineage"
                )
            expected_core_context = {
                field: core["cohort"][field]
                for field in _COMPLETE_CONTEXT_FIELDS
            }
            if expected_core_context != validated["bundle"]["core_context"]:
                raise RoutePublicationError(
                    "complete core context differs from pinned core lineage"
                )
        if _optional_pointer_bytes_at(routes_fd) != pointer_bytes:
            raise RoutePublicationError("complete route pointer changed during validation")
        _verify_directory_entry(
            routes_fd, "bundles", bundles_details, "complete route bundles root"
        )
        _verify_open_path_identity(routes, routes_details, "complete routes root")
        validated["pointer"] = pointer
        return validated
    finally:
        if bundles_fd is not None:
            os.close(bundles_fd)
        os.close(routes_fd)


def _strict_validate_shadow_audit(audit: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate an audit through the Task 2 audit module without a fallback.

    Keeping this import lazy avoids a module cycle while still failing closed
    when the independently versioned audit validator is not installed.
    """
    try:
        from scripts.route_shadow_audit import (
            AUDIT_FIELDS,
            ROUTE_SHADOW_AUDIT_SCHEMA,
            validate_shadow_audit,
        )
    except ModuleNotFoundError:
        try:
            from route_shadow_audit import (  # type: ignore[no-redef]
                AUDIT_FIELDS,
                ROUTE_SHADOW_AUDIT_SCHEMA,
                validate_shadow_audit,
            )
        except (ImportError, ModuleNotFoundError) as error:
            raise RoutePublicationError(
                "route shadow audit validator is unavailable"
            ) from error
    try:
        normalized = validate_shadow_audit(audit)
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("route shadow audit is invalid") from error
    if (
        not isinstance(normalized, dict)
        or set(normalized) != set(AUDIT_FIELDS)
        or normalized.get("schema") != ROUTE_SHADOW_AUDIT_SCHEMA
    ):
        raise RoutePublicationError("route shadow audit schema is invalid")
    return _clone_json(normalized)


def _read_canonical_object_at(
    directory_fd: int,
    filename: str,
    *,
    limit: int,
    label: str,
) -> Tuple[Dict[str, Any], bytes, str, os.stat_result]:
    value, physical_sha256, details = _read_bounded_bytes_at(
        directory_fd,
        filename,
        limit=limit,
        label=label,
    )
    payload = _decode_json_object_bytes(value, label=label)
    if value != _canonical_json_bytes(payload):
        raise RoutePublicationError("{} is not canonical JSON".format(label))
    try:
        current = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as error:
        raise RoutePublicationError("{} changed during validation".format(label)) from error
    if _stable_file_metadata(current) != _stable_file_metadata(details):
        raise RoutePublicationError("{} changed during validation".format(label))
    return payload, value, physical_sha256, details


def _optional_regular_snapshot_at(
    directory_fd: int,
    filename: str,
    *,
    limit: int,
    label: str,
) -> Optional[Tuple[bytes, os.stat_result]]:
    try:
        details = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise RoutePublicationError("{} is not readable".format(label)) from error
    if (
        not stat.S_ISREG(details.st_mode)
        or stat.S_ISLNK(details.st_mode)
        or details.st_nlink != 1
    ):
        raise RoutePublicationError(
            "{} must be a regular non-symlink single-link file".format(label)
        )
    value, _sha256, read_details = _read_bounded_bytes_at(
        directory_fd,
        filename,
        limit=limit,
        label=label,
    )
    return value, read_details


def _install_immutable_audit_at(
    run_fd: int,
    run_path: Path,
    audit_bytes: bytes,
) -> None:
    temporary_name = ".audit.{}.tmp".format(secrets.token_hex(12))
    installed = False
    _write_new_bytes_at(run_fd, temporary_name, audit_bytes)
    try:
        _rename_directory_noreplace_at(
            run_fd,
            temporary_name,
            run_fd,
            ROUTE_SHADOW_AUDIT_FILENAME,
            destination_display=run_path / ROUTE_SHADOW_AUDIT_FILENAME,
        )
        installed = True
        _fsync_directory(run_path, directory_fd=run_fd)
        _payload, installed_bytes, _sha256, _details = _read_canonical_object_at(
            run_fd,
            ROUTE_SHADOW_AUDIT_FILENAME,
            limit=_MAX_JSON_BYTES,
            label="route shadow audit",
        )
        if installed_bytes != audit_bytes:
            raise RoutePublicationError("installed route shadow audit changed")
    finally:
        if not installed:
            try:
                os.unlink(temporary_name, dir_fd=run_fd)
            except FileNotFoundError:
                pass
            except OSError:
                pass


def _validate_shadow_pointer(pointer: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(pointer, Mapping) or set(pointer) != _ROUTE_SHADOW_POINTER_FIELDS:
        raise RoutePublicationError("route shadow pointer schema is invalid")
    value = _clone_json(dict(pointer))
    if value.get("schema") != ROUTE_SHADOW_POINTER_SCHEMA:
        raise RoutePublicationError("route shadow pointer schema is unsupported")
    try:
        _validate_shadow_run_id(value.get("run_id"))
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("route shadow pointer run ID is invalid") from error
    if value.get("phase") not in {"canary", "full"}:
        raise RoutePublicationError("route shadow pointer phase is invalid")
    if (
        not isinstance(value.get("route_cohort_id"), str)
        or _COHORT_ID.fullmatch(value["route_cohort_id"]) is None
    ):
        raise RoutePublicationError("route shadow pointer cohort ID is invalid")
    for field in (
        "phase_state_sha256",
        "core_pointer_sha256",
        "core_manifest_sha256",
        "route_universe_sha256",
        "route_cost_evidence_sha256",
        "baseline_manifest_sha256",
        "candidate_source_generation",
        "audit_sha256",
    ):
        if (
            not isinstance(value.get(field), str)
            or _HEX_SHA256.fullmatch(value[field]) is None
        ):
            raise RoutePublicationError(
                "route shadow pointer {} is invalid".format(field)
            )
    transition_id = value.get("phase_transition_id")
    if value["phase"] == "canary":
        if transition_id is not None or (
            value["phase_state_sha256"] != ROUTE_SHADOW_IMPLICIT_CANARY_SHA256
        ):
            raise RoutePublicationError("route shadow canary phase binding is invalid")
    elif (
        not isinstance(transition_id, str)
        or _HEX_SHA256.fullmatch(transition_id) is None
    ):
        raise RoutePublicationError("route shadow full transition ID is invalid")
    return value


def _validate_core_pointer_mapping(pointer: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(pointer, Mapping) or set(pointer) != {
        "schema", "bundle_stage", "route_cohort_id", "manifest_sha256"
    }:
        raise RoutePublicationError("supplied route core pointer schema is invalid")
    value = _clone_json(dict(pointer))
    if (
        value.get("schema") != ROUTE_CORE_POINTER_SCHEMA
        or value.get("bundle_stage") != ROUTE_CORE_BUNDLE_STAGE
        or not isinstance(value.get("route_cohort_id"), str)
        or _COHORT_ID.fullmatch(value["route_cohort_id"]) is None
        or not isinstance(value.get("manifest_sha256"), str)
        or _HEX_SHA256.fullmatch(value["manifest_sha256"]) is None
    ):
        raise RoutePublicationError("supplied route core pointer is invalid")
    return value


def _validate_phase_state_payload(
    state: Mapping[str, Any],
    state_bytes: bytes,
) -> Dict[str, Any]:
    if set(state) != _ROUTE_SHADOW_PHASE_FIELDS:
        raise RoutePublicationError("route shadow phase schema is invalid")
    value = _clone_json(dict(state))
    if (
        value.get("schema") != ROUTE_SHADOW_PHASE_SCHEMA
        or value.get("prior_phase") != "canary"
        or value.get("phase") != "full"
    ):
        raise RoutePublicationError("route shadow phase transition is invalid")
    if state_bytes != _canonical_json_bytes(value):
        raise RoutePublicationError("route shadow phase is not canonical JSON")
    try:
        exact_rfc3339_epoch_seconds(value.get("evaluated_at"))
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("route shadow phase time is invalid") from error
    for field in (
        "transition_id",
        "gate_evidence_sha256",
        "storage_admission_sha256",
        "anchored_joint_pointer_sha256",
        "phase_identity_id",
    ):
        if (
            not isinstance(value.get(field), str)
            or _HEX_SHA256.fullmatch(value[field]) is None
        ):
            raise RoutePublicationError(
                "route shadow phase {} is invalid".format(field)
            )
    guard_sha = value.get("primary_schedule_guard_sha256")
    envelope_sha = value.get("schedule_envelope_sha256")
    if (guard_sha is None) == (envelope_sha is None):
        raise RoutePublicationError("route shadow phase ownership binding is invalid")
    for field_value in (guard_sha, envelope_sha):
        if field_value is not None and (
            not isinstance(field_value, str)
            or _HEX_SHA256.fullmatch(field_value) is None
        ):
            raise RoutePublicationError("route shadow phase ownership hash is invalid")
    identity = dict(value)
    transition_id = identity.pop("transition_id")
    if _sha256_bytes(_canonical_json_bytes(identity)) != transition_id:
        raise RoutePublicationError("route shadow transition ID is invalid")
    return value


def _load_full_phase_state(
    shadow_root: Path,
    *,
    phase_state_sha256: str,
    phase_transition_id: str,
) -> Dict[str, Any]:
    shadow, shadow_fd, shadow_details = _open_verified_directory(
        Path(shadow_root), "route shadow root"
    )
    transitions_fd: Optional[int] = None
    gates_fd: Optional[int] = None
    try:
        transitions_fd, transitions_details = _open_directory_at(
            shadow_fd, "transitions", "route shadow transitions root"
        )
        transition_name = phase_transition_id + ".json"
        state, state_bytes, actual_sha256, state_details = _read_canonical_object_at(
            transitions_fd,
            transition_name,
            limit=_MAX_JSON_BYTES,
            label="route shadow phase transition",
        )
        if actual_sha256 != phase_state_sha256:
            raise RoutePublicationError("route shadow phase-state hash mismatch")
        value = _validate_phase_state_payload(state, state_bytes)
        if value["transition_id"] != phase_transition_id:
            raise RoutePublicationError("route shadow transition path mismatch")
        gates_fd, gates_details = _open_directory_at(
            shadow_fd, "gates", "route shadow gates root"
        )
        gate_name = value["gate_evidence_sha256"] + ".json"
        _gate, gate_bytes, gate_sha256, gate_details = _read_canonical_object_at(
            gates_fd,
            gate_name,
            limit=_MAX_JSON_BYTES,
            label="route shadow gate evidence",
        )
        if gate_sha256 != value["gate_evidence_sha256"]:
            raise RoutePublicationError("route shadow gate evidence hash mismatch")
        if not _pointer_snapshot_is_owned(
            _optional_regular_snapshot_at(
                transitions_fd,
                transition_name,
                limit=_MAX_JSON_BYTES,
                label="route shadow phase transition",
            ),
            (state_bytes, state_details),
        ):
            raise RoutePublicationError("route shadow phase transition changed")
        if not _pointer_snapshot_is_owned(
            _optional_regular_snapshot_at(
                gates_fd,
                gate_name,
                limit=_MAX_JSON_BYTES,
                label="route shadow gate evidence",
            ),
            (gate_bytes, gate_details),
        ):
            raise RoutePublicationError("route shadow gate evidence changed")
        _verify_directory_entry_snapshot(
            shadow_fd, "transitions", transitions_details,
            "route shadow transitions root",
        )
        _verify_directory_entry_snapshot(
            shadow_fd, "gates", gates_details, "route shadow gates root"
        )
        _verify_open_path_snapshot(shadow, shadow_details, "route shadow root")
        return {
            "phase": "full",
            "phase_state_sha256": phase_state_sha256,
            "phase_transition_id": phase_transition_id,
            "state": value,
        }
    finally:
        _close_route_descriptor_group((
            (gates_fd, "route shadow gates root"),
            (transitions_fd, "route shadow transitions root"),
            (shadow_fd, "route shadow root"),
        ))


def load_historical_phase_state(
    shadow_root: Path,
    *,
    phase: str,
    phase_state_sha256: str,
    phase_transition_id: Optional[str],
) -> Dict[str, Any]:
    """Reconstruct one pointer's immutable phase without consulting active state."""
    if phase == "canary":
        if (
            phase_state_sha256 != ROUTE_SHADOW_IMPLICIT_CANARY_SHA256
            or phase_transition_id is not None
        ):
            raise RoutePublicationError("historical canary phase binding is invalid")
        return {
            "phase": "canary",
            "phase_state_sha256": ROUTE_SHADOW_IMPLICIT_CANARY_SHA256,
            "phase_transition_id": None,
            "state": None,
        }
    if (
        phase != "full"
        or not isinstance(phase_state_sha256, str)
        or _HEX_SHA256.fullmatch(phase_state_sha256) is None
        or not isinstance(phase_transition_id, str)
        or _HEX_SHA256.fullmatch(phase_transition_id) is None
    ):
        raise RoutePublicationError("historical route shadow phase is invalid")
    return _load_full_phase_state(
        Path(shadow_root),
        phase_state_sha256=phase_state_sha256,
        phase_transition_id=phase_transition_id,
    )


def _load_active_phase_state_with_snapshot(
    shadow_root: Path,
) -> Tuple[
    Dict[str, Any],
    Optional[Tuple[bytes, os.stat_result]],
    os.stat_result,
]:
    shadow, shadow_fd, shadow_details = _open_verified_directory(
        Path(shadow_root), "route shadow root"
    )
    try:
        snapshot = _optional_regular_snapshot_at(
            shadow_fd,
            ROUTE_SHADOW_PHASE_FILENAME,
            limit=_MAX_JSON_BYTES,
            label="route shadow phase",
        )
        if snapshot is None:
            view = {
                "phase": "canary",
                "phase_state_sha256": ROUTE_SHADOW_IMPLICIT_CANARY_SHA256,
                "phase_transition_id": None,
                "state": None,
            }
            if _optional_regular_snapshot_at(
                shadow_fd,
                ROUTE_SHADOW_PHASE_FILENAME,
                limit=_MAX_JSON_BYTES,
                label="route shadow phase",
            ) is not None:
                raise RoutePublicationError("active route shadow phase changed")
        else:
            state_bytes = snapshot[0]
            state = _decode_json_object_bytes(state_bytes, label="route shadow phase")
            value = _validate_phase_state_payload(state, state_bytes)
            view = load_historical_phase_state(
                shadow,
                phase="full",
                phase_state_sha256=_sha256_bytes(state_bytes),
                phase_transition_id=value["transition_id"],
            )
            if view["state"] != value:
                raise RoutePublicationError("active route shadow phase changed")
            current = _optional_regular_snapshot_at(
                shadow_fd,
                ROUTE_SHADOW_PHASE_FILENAME,
                limit=_MAX_JSON_BYTES,
                label="route shadow phase",
            )
            if not _pointer_snapshot_is_owned(current, snapshot):
                raise RoutePublicationError("active route shadow phase changed")
        current_shadow_details = _verify_open_path_snapshot(
            shadow, shadow_details, "route shadow root"
        )
        return view, snapshot, current_shadow_details
    finally:
        _close_route_descriptor_group(((shadow_fd, "route shadow root"),))


def load_active_phase_state(shadow_root: Path) -> Dict[str, Any]:
    """Load the sole active phase authority; absence is implicit canary."""
    view, _snapshot, _root_snapshot = _load_active_phase_state_with_snapshot(
        Path(shadow_root)
    )
    return view


def _validate_route_universe_payload(universe: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(universe, Mapping) or set(universe) != _ROUTE_UNIVERSE_FIELDS:
        raise RoutePublicationError("route universe schema is invalid")
    value = _clone_json(dict(universe))
    window = value.get("selection_window")
    if (
        value.get("schema") != ROUTE_UNIVERSE_SCHEMA
        or not isinstance(value.get("candidate_source_generation"), str)
        or _HEX_SHA256.fullmatch(value["candidate_source_generation"]) is None
        or not isinstance(window, dict)
        or set(window) != {"start", "end"}
        or not _matches_requested_notional_grid(value.get("requested_notionals_usd"))
        or not isinstance(value.get("selected_legs"), list)
        or not isinstance(value.get("routes"), list)
    ):
        raise RoutePublicationError("route universe contract is invalid")
    try:
        window_start = date.fromisoformat(window["start"])
        window_end = date.fromisoformat(window["end"])
    except (KeyError, TypeError, ValueError) as error:
        raise RoutePublicationError("route universe selection window is invalid") from error
    if (
        not isinstance(window["start"], str)
        or _ISO_DATE.fullmatch(window["start"]) is None
        or not isinstance(window["end"], str)
        or _ISO_DATE.fullmatch(window["end"]) is None
        or (window_end - window_start).days != 29
    ):
        raise RoutePublicationError("route universe selection window is invalid")

    def canonical_decimal(
        raw: Any,
        *,
        positive: bool,
        nullable: bool,
        label: str,
    ) -> Optional[str]:
        if raw is None and nullable:
            return None
        if (
            not isinstance(raw, str)
            or len(raw) > 256
            or _SHADOW_DECIMAL_TEXT.fullmatch(raw) is None
        ):
            raise RoutePublicationError("{} is invalid".format(label))
        try:
            amount = Decimal(raw)
        except (InvalidOperation, ValueError) as error:
            raise RoutePublicationError("{} is invalid".format(label)) from error
        canonical = format(amount, "f")
        if "." in canonical:
            canonical = canonical.rstrip("0").rstrip(".")
        if (
            not amount.is_finite()
            or amount < 0
            or (amount.is_zero() and amount.is_signed())
            or (positive and amount <= 0)
            or raw != canonical
        ):
            raise RoutePublicationError("{} is invalid".format(label))
        return raw

    market_ids = []
    ranked_groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for leg in value["selected_legs"]:
        if not isinstance(leg, dict):
            raise RoutePublicationError("route universe leg schema is invalid")
        market_type = leg.get("market_type")
        expected_leg_fields = (
            _ROUTE_UNIVERSE_LEG_FIELDS | _ROUTE_UNIVERSE_DEX_IDENTITY_FIELDS
            if market_type == "dex"
            else _ROUTE_UNIVERSE_LEG_FIELDS
        )
        if set(leg) != expected_leg_fields:
            raise RoutePublicationError("route universe leg schema is invalid")
        market_id = leg.get("market_id")
        if (
            not isinstance(market_id, str)
            or not market_id
            or market_type not in {"cex", "dex"}
            or not market_id.startswith(str(market_type) + ":")
            or leg.get("candidate_source_generation")
            != value["candidate_source_generation"]
            or leg.get("selection_window") != value["selection_window"]
            or not isinstance(leg.get("selection_inputs"), dict)
            or set(leg["selection_inputs"])
            != _ROUTE_UNIVERSE_SELECTION_INPUT_FIELDS
            or isinstance(leg.get("selection_rank"), bool)
            or not isinstance(leg.get("selection_rank"), int)
            or leg["selection_rank"] < 1
        ):
            raise RoutePublicationError("route universe leg is invalid")
        if _canonical_market_token(market_id) != leg.get("token_symbol"):
            raise RoutePublicationError("route universe leg token is invalid")
        inputs = leg["selection_inputs"]
        capability = inputs.get("execution_capability")
        if capability not in {"supported", "proved"}:
            raise RoutePublicationError("route universe capability is invalid")
        capacity = canonical_decimal(
            inputs.get("proved_execution_capacity_usd"),
            positive=True,
            nullable=True,
            label="route universe proved capacity",
        )
        if (capability == "proved") != (capacity is not None):
            raise RoutePublicationError("route universe capability lineage is invalid")
        canonical_decimal(
            inputs.get("observed_100bps_depth_usd"),
            positive=True,
            nullable=False,
            label="route universe observed depth",
        )
        if market_type == "cex":
            canonical_decimal(
                inputs.get("cex_selected_window_usd"),
                positive=False,
                nullable=True,
                label="route universe CEX volume",
            )
            if inputs.get("dex_24h_usd") is not None or inputs.get("dex_tvl_usd") is not None:
                raise RoutePublicationError("route universe market-type inputs conflict")
        else:
            target_address = leg.get("target_token_address")
            target_side = leg.get("target_token_side")
            market_match = _DEX_MARKET_ID.fullmatch(market_id)
            if market_match is None:
                raise RoutePublicationError(
                    "route universe DEX target identity is invalid"
                )
            raw_chain = market_match.group(1)
            try:
                chain = normalize_chain(raw_chain)
                normalized_target = normalize_contract_address(
                    chain, target_address
                )
            except (TokenRegistryError, ValueError) as error:
                raise RoutePublicationError(
                    "route universe DEX target identity is invalid"
                ) from error
            context_result = _validate_route_collector_context(
                leg.get("collector_context"), market_id=market_id
            )
            context = context_result["context"]
            if (
                not isinstance(target_address, str)
                or normalized_target != target_address
                or context_result["chain"] != chain
                or target_side not in {"base", "quote", None}
                or (context.get("status") == "observed")
                != (target_side is not None)
                or (
                    target_side is not None
                    and context.get(target_side + "_token_id")
                    != "{}_{}".format(chain, target_address)
                )
            ):
                raise RoutePublicationError(
                    "route universe DEX target identity is invalid"
                )
            if inputs.get("cex_selected_window_usd") is not None:
                raise RoutePublicationError("route universe market-type inputs conflict")
            canonical_decimal(
                inputs.get("dex_24h_usd"),
                positive=False,
                nullable=True,
                label="route universe DEX volume",
            )
            canonical_decimal(
                inputs.get("dex_tvl_usd"),
                positive=False,
                nullable=True,
                label="route universe DEX TVL",
            )
        market_ids.append(market_id)
        group = (leg["token_symbol"], market_type)
        ranked_groups.setdefault(group, []).append(leg)
    if len(market_ids) != len(set(market_ids)):
        raise RoutePublicationError("route universe contains duplicate markets")
    def exact_selection_key(
        row: Mapping[str, Any],
    ) -> Tuple[int, Decimal, Decimal, Decimal, Decimal, str]:
        inputs = row["selection_inputs"]
        capacity = Decimal(inputs["proved_execution_capacity_usd"] or "0")
        depth = Decimal(inputs["observed_100bps_depth_usd"])
        liquidity = capacity if capacity > 0 else depth

        def descending(field: str) -> Decimal:
            return Decimal(inputs[field] or "0").copy_negate()

        return (
            -({"supported": 0, "proved": 1}[inputs["execution_capability"]]),
            liquidity.copy_negate(),
            descending("cex_selected_window_usd"),
            descending("dex_24h_usd"),
            descending("dex_tvl_usd"),
            str(row["market_id"]),
        )

    for group_rows in ranked_groups.values():
        ranks = [row["selection_rank"] for row in group_rows]
        if (
            ranks != list(range(1, len(ranks) + 1))
            or len(ranks) > 3
            or group_rows != sorted(
                group_rows, key=exact_selection_key
            )
        ):
            raise RoutePublicationError("route universe selection ranks are invalid")
    if value["selected_legs"] != sorted(
        value["selected_legs"],
        key=lambda row: (
            row["token_symbol"],
            row["market_type"],
            row["selection_rank"],
            row["market_id"],
        ),
    ):
        raise RoutePublicationError("route universe legs are not canonically ordered")
    route_ids = []
    selected_by_market = {
        row["market_id"]: row for row in value["selected_legs"]
    }
    for route in value["routes"]:
        if not isinstance(route, dict):
            raise RoutePublicationError("route universe route is invalid")
        route_id = route.get("route_id")
        if (
            not isinstance(route_id, str)
            or not route_id
            or route.get("candidate_source_generation")
            != value["candidate_source_generation"]
            or route.get("requested_notionals_usd")
            != value["requested_notionals_usd"]
        ):
            raise RoutePublicationError("route universe route lineage is invalid")
        try:
            buy_leg = selected_by_market[route["buy_market_id"]]
            sell_leg = selected_by_market[route["sell_market_id"]]
        except (KeyError, TypeError) as error:
            raise RoutePublicationError(
                "route universe route leg is missing"
            ) from error

        def reference_volume(leg: Mapping[str, Any]) -> Optional[str]:
            inputs = leg["selection_inputs"]
            return (
                inputs["cex_selected_window_usd"]
                if leg["market_type"] == "cex"
                else inputs["dex_24h_usd"]
            )

        if (
            route.get("buy_reference_volume_usd") != reference_volume(buy_leg)
            or route.get("sell_reference_volume_usd")
            != reference_volume(sell_leg)
        ):
            raise RoutePublicationError(
                "route universe reference-volume lineage is invalid"
            )
        route_ids.append(route_id)
    if len(route_ids) != len(set(route_ids)):
        raise RoutePublicationError("route universe contains duplicate routes")
    if route_ids != sorted(route_ids):
        raise RoutePublicationError("route universe routes are not canonically ordered")
    return value


def _validate_cost_evidence_outer_lineage(
    cost_evidence: Mapping[str, Any],
    *,
    run_id: str,
    route_cohort_id: str,
    phase: str,
    candidate_source_generation: str,
    route_universe_sha256_value: str,
    universe: Mapping[str, Any],
    retained_typed_pool_state_members: Optional[
        Mapping[str, Mapping[str, Any]]
    ] = None,
) -> None:
    try:
        validated = validate_route_cost_evidence_manifest_for_publication(
            cost_evidence,
            universe=universe,
            expected_run_id=run_id,
            expected_route_cohort_id=route_cohort_id,
            expected_phase=phase,
            expected_candidate_source_generation=candidate_source_generation,
            expected_route_universe_sha256=route_universe_sha256_value,
            retained_typed_pool_state_members=(
                retained_typed_pool_state_members
            ),
        )
    except RouteCostEvidenceError as error:
        raise RoutePublicationError(
            "route-cost evidence replay failed: {}".format(error)
        ) from error
    if validated != cost_evidence:
        raise RoutePublicationError("route-cost evidence is not canonical")


def _load_retained_typed_source_members(
    shadow_root: Path,
    core: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Descriptor-reread the exact raw typed inventory bound by core legs."""
    cohort = core.get("cohort")
    legs = core.get("legs")
    if not isinstance(cohort, Mapping) or not isinstance(legs, list):
        raise RoutePublicationError("typed-source core evidence is invalid")
    raw_run_id = cohort.get("raw_evidence_run_id")
    try:
        validated_raw_run_id = _validate_shadow_run_id(raw_run_id)
    except (TypeError, ValueError) as error:
        raise RoutePublicationError(
            "typed-source raw evidence run ID is invalid"
        ) from error

    expected_members: List[Dict[str, Any]] = []
    for leg in legs:
        if not isinstance(leg, Mapping):
            raise RoutePublicationError("typed-source core leg is invalid")
        market_id = leg.get("market_id")
        market_type = leg.get("market_type")
        lineage = leg.get("typed_source_lineage")
        if not isinstance(market_id, str) or market_type not in {"cex", "dex"}:
            raise RoutePublicationError("typed-source core leg identity is invalid")
        try:
            observed = typed_source_lineage_observed_members(
                lineage, market_type=market_type
            )
        except (TypeError, ValueError) as error:
            raise RoutePublicationError(
                "typed-source core lineage is invalid"
            ) from error
        expected_members.extend(
            {"market_id": market_id, **member} for member in observed
        )
    expected_members.sort(key=lambda row: (row["market_id"], row["role"]))
    expected_keys = [
        (row["market_id"], row["role"]) for row in expected_members
    ]
    expected_filenames = [row["filename"] for row in expected_members]
    if (
        len(expected_keys) != len(set(expected_keys))
        or len(expected_filenames) != len(set(expected_filenames))
    ):
        raise RoutePublicationError("typed-source core inventory is invalid")

    shadow = _absolute_without_symlink_resolution(Path(shadow_root))
    raw_root_path = shadow.parent.parent / "raw" / "route-cohort"
    raw_root, raw_root_fd, raw_root_details = _open_verified_directory(
        raw_root_path, "typed-source raw root"
    )
    run_fd: Optional[int] = None
    typed_fd: Optional[int] = None
    try:
        run_fd, run_details = _open_directory_at(
            raw_root_fd,
            validated_raw_run_id,
            "typed-source raw run",
        )
        manifest, manifest_bytes, _manifest_sha, manifest_details = (
            _read_canonical_object_at(
                run_fd,
                "typed-manifest.json",
                limit=_MAX_JSON_BYTES,
                label="typed-source manifest",
            )
        )
        if (
            set(manifest) != TYPED_SOURCE_MANIFEST_FIELDS
            or manifest.get("schema") != TYPED_SOURCE_MANIFEST_SCHEMA
            or manifest.get("raw_evidence_run_id") != validated_raw_run_id
            or isinstance(manifest.get("member_count"), bool)
            or not isinstance(manifest.get("member_count"), int)
            or not isinstance(manifest.get("members"), list)
            or manifest["member_count"] != len(manifest["members"])
            or any(
                not isinstance(member, Mapping)
                or set(member) != TYPED_SOURCE_MANIFEST_MEMBER_FIELDS
                for member in manifest["members"]
            )
            or manifest["members"] != expected_members
        ):
            raise RoutePublicationError(
                "typed-source manifest/core inventory differs"
            )

        typed_fd, typed_details = _open_directory_at(
            run_fd, "typed", "typed-source member root"
        )
        try:
            actual_filenames = sorted(os.listdir(typed_fd))
        except OSError as error:
            raise RoutePublicationError(
                "typed-source member inventory is unreadable"
            ) from error
        if actual_filenames != sorted(expected_filenames):
            raise RoutePublicationError(
                "typed-source member directory inventory differs"
            )

        retained: Dict[str, Dict[str, Any]] = {}
        member_snapshots: List[Tuple[str, bytes, os.stat_result, int]] = []
        for descriptor in expected_members:
            contract = TYPED_SOURCE_ROLE_CONTRACTS.get(descriptor["role"])
            if contract is None:
                raise RoutePublicationError("typed-source role is invalid")
            payload, physical_sha256, member_details = _read_bounded_bytes_at(
                typed_fd,
                descriptor["filename"],
                limit=contract["max_bytes"],
                label="typed-source member",
            )
            if (
                len(payload) != descriptor["size"]
                or physical_sha256 != descriptor["sha256"]
            ):
                raise RoutePublicationError(
                    "typed-source member bytes differ from lineage"
                )
            member_snapshots.append((
                descriptor["filename"],
                payload,
                member_details,
                contract["max_bytes"],
            ))
            if descriptor["role"] == "dex_pool_state":
                retained[descriptor["market_id"]] = {
                    "descriptor": _clone_json(descriptor),
                    "payload": payload,
                }

        for filename, payload, details, limit in member_snapshots:
            if not _pointer_snapshot_is_owned(
                _optional_regular_snapshot_at(
                    typed_fd,
                    filename,
                    limit=limit,
                    label="typed-source member",
                ),
                (payload, details),
            ):
                raise RoutePublicationError(
                    "typed-source member changed during validation"
                )
        if not _pointer_snapshot_is_owned(
            _optional_regular_snapshot_at(
                run_fd,
                "typed-manifest.json",
                limit=_MAX_JSON_BYTES,
                label="typed-source manifest",
            ),
            (manifest_bytes, manifest_details),
        ):
            raise RoutePublicationError(
                "typed-source manifest changed during validation"
            )
        _verify_directory_entry_snapshot(
            run_fd, "typed", typed_details, "typed-source member root"
        )
        _verify_directory_entry_snapshot(
            raw_root_fd,
            validated_raw_run_id,
            run_details,
            "typed-source raw run",
        )
        _verify_open_path_snapshot(
            raw_root, raw_root_details, "typed-source raw root"
        )
        return retained
    finally:
        _close_route_descriptor_group((
            (typed_fd, "typed-source member root"),
            (run_fd, "typed-source raw run"),
            (raw_root_fd, "typed-source raw root"),
        ))


def _open_shadow_run_directory(
    shadow_root: Path,
    run_id: str,
) -> Tuple[
    Path,
    int,
    os.stat_result,
    int,
    os.stat_result,
    Path,
    int,
    os.stat_result,
]:
    try:
        validated_run_id = _validate_shadow_run_id(run_id)
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("route shadow run ID is invalid") from error
    shadow, shadow_fd, shadow_details = _open_verified_directory(
        Path(shadow_root), "route shadow root"
    )
    runs_fd: Optional[int] = None
    run_fd: Optional[int] = None
    try:
        runs_fd, runs_details = _open_directory_at(
            shadow_fd, "runs", "route shadow runs root"
        )
        run_fd, run_details = _open_directory_at(
            runs_fd, validated_run_id, "route shadow run"
        )
        return (
            shadow,
            shadow_fd,
            shadow_details,
            runs_fd,
            runs_details,
            shadow / "runs" / validated_run_id,
            run_fd,
            run_details,
        )
    except BaseException:
        _close_route_descriptor_group((
            (run_fd, "route shadow run"),
            (runs_fd, "route shadow runs root"),
            (shadow_fd, "route shadow root"),
        ))
        raise


def _read_shadow_run_evidence(
    shadow_root: Path,
    run_id: str,
) -> Dict[str, Any]:
    (
        shadow,
        shadow_fd,
        shadow_details,
        runs_fd,
        runs_details,
        run_path,
        run_fd,
        run_details,
    ) = _open_shadow_run_directory(Path(shadow_root), run_id)
    try:
        universe, universe_bytes, _universe_physical_sha, universe_details = (
            _read_canonical_object_at(
                run_fd,
                "route_universe.json",
                limit=_MAX_JSON_BYTES,
                label="route shadow universe",
            )
        )
        universe = _validate_route_universe_payload(universe)
        universe_logical_sha256 = route_universe_sha256(universe)
        baseline, baseline_bytes, baseline_sha256, baseline_details = (
            _read_canonical_object_at(
                run_fd,
                "baseline_manifest.json",
                limit=_MAX_JSON_BYTES,
                label="route shadow baseline manifest",
            )
        )
        try:
            normalized_baseline = _validate_shadow_baseline_manifest(
                universe, baseline
            )
        except (TypeError, ValueError) as error:
            raise RoutePublicationError(
                "route shadow baseline manifest is invalid"
            ) from error
        if normalized_baseline != baseline or (
            baseline.get("route_universe_sha256") != universe_logical_sha256
        ):
            raise RoutePublicationError("route shadow baseline lineage mismatch")
        selected_end = date.fromisoformat(universe["selection_window"]["end"])
        expected_end_exclusive = date.fromordinal(
            selected_end.toordinal() + 1
        ).isoformat()
        if baseline.get("filters") != {
            "window_days": 30,
            "calendar": "complete_utc_days",
            "cex_volume_aggregation": "sum_quote_volume_usd",
            "maximum_legs_per_token_market_type": 3,
        } or baseline.get("observation_bounds") != {
            "start_inclusive": universe["selection_window"]["start"]
            + "T00:00:00Z",
            "end_exclusive": expected_end_exclusive + "T00:00:00Z",
        }:
            raise RoutePublicationError(
                "route shadow baseline window contract is invalid"
            )
        cost, cost_bytes, cost_sha256, cost_details = _read_canonical_object_at(
            run_fd,
            ROUTE_SHADOW_COST_EVIDENCE_FILENAME,
            limit=_MAX_ROUTE_COST_EVIDENCE_BYTES,
            label="route-cost evidence",
        )
        audit_payload, audit_bytes, audit_sha256, audit_details = (
            _read_canonical_object_at(
                run_fd,
                ROUTE_SHADOW_AUDIT_FILENAME,
                limit=_MAX_JSON_BYTES,
                label="route shadow audit",
            )
        )
        audit = _strict_validate_shadow_audit(audit_payload)
        if audit != audit_payload or _canonical_json_bytes(audit) != audit_bytes:
            raise RoutePublicationError("route shadow audit normalization changed bytes")
        if (
            audit["run_id"] != run_id
            or audit["route_universe_sha256"] != universe_logical_sha256
            or audit["baseline_manifest_sha256"] != baseline_sha256
            or audit["route_cost_evidence_sha256"] != cost_sha256
            or audit["candidate_source_generation"]
            != universe["candidate_source_generation"]
            or baseline["candidate_source_generation"]
            != universe["candidate_source_generation"]
            or baseline["selection_window"] != universe["selection_window"]
        ):
            raise RoutePublicationError("route shadow run lineage mismatch")
        immutable_members = (
            (
                "route_universe.json",
                universe_bytes,
                universe_details,
                _MAX_JSON_BYTES,
                "route shadow universe",
            ),
            (
                "baseline_manifest.json",
                baseline_bytes,
                baseline_details,
                _MAX_JSON_BYTES,
                "route shadow baseline manifest",
            ),
            (
                ROUTE_SHADOW_COST_EVIDENCE_FILENAME,
                cost_bytes,
                cost_details,
                _MAX_ROUTE_COST_EVIDENCE_BYTES,
                "route-cost evidence",
            ),
            (
                ROUTE_SHADOW_AUDIT_FILENAME,
                audit_bytes,
                audit_details,
                _MAX_JSON_BYTES,
                "route shadow audit",
            ),
        )
        for filename, member_bytes, member_details, limit, label in immutable_members:
            if not _pointer_snapshot_is_owned(
                _optional_regular_snapshot_at(
                    run_fd,
                    filename,
                    limit=limit,
                    label=label,
                ),
                (member_bytes, member_details),
            ):
                raise RoutePublicationError("{} changed during validation".format(label))
        _verify_directory_entry_snapshot(
            runs_fd, run_id, run_details, "route shadow run"
        )
        _verify_directory_entry_snapshot(
            shadow_fd, "runs", runs_details, "route shadow runs root"
        )
        _verify_open_path_snapshot(shadow, shadow_details, "route shadow root")
        return {
            "audit": audit,
            "audit_bytes": audit_bytes,
            "audit_sha256": audit_sha256,
            "universe": universe,
            "universe_bytes": universe_bytes,
            "route_universe_sha256": universe_logical_sha256,
            "baseline": normalized_baseline,
            "baseline_bytes": baseline_bytes,
            "baseline_manifest_sha256": baseline_sha256,
            "cost_evidence": cost,
            "cost_evidence_bytes": cost_bytes,
            "route_cost_evidence_sha256": cost_sha256,
        }
    finally:
        _close_route_descriptor_group((
            (run_fd, "route shadow run"),
            (runs_fd, "route shadow runs root"),
            (shadow_fd, "route shadow root"),
        ))


def _validate_dex_collector_contexts(
    universe: Mapping[str, Any],
    core_legs: Sequence[Mapping[str, Any]],
) -> None:
    try:
        try:
            from scripts.quality_outcomes import tvl_reason_code
        except ModuleNotFoundError:
            from quality_outcomes import tvl_reason_code  # type: ignore[no-redef]
    except (ImportError, ModuleNotFoundError) as error:
        raise RoutePublicationError("TVL reason validator is unavailable") from error
    universe_by_market = {
        row["market_id"]: row for row in universe["selected_legs"]
    }
    for core_leg in core_legs:
        if core_leg.get("market_type") != "dex":
            continue
        market_id = str(core_leg.get("market_id") or "")
        universe_leg = universe_by_market.get(market_id)
        if universe_leg is None:
            raise RoutePublicationError("DEX collector context market is absent")
        context = universe_leg.get("collector_context")
        if (
            core_leg.get("collector_context") != context
        ):
            raise RoutePublicationError("DEX collector context lineage mismatch")
        context_result = _validate_route_collector_context(
            context, market_id=market_id
        )
        context = context_result["context"]
        redundant = {
            "snapshot_id": "usd_price_source_snapshot_id",
            "observed_at": "usd_price_observed_at",
            "source": "usd_price_source",
            "source_endpoint": "usd_price_source_endpoint",
            "raw_response_sha256": "usd_price_raw_response_sha256",
        }
        for context_field, core_field in redundant.items():
            if core_leg.get(core_field) != context.get(context_field):
                raise RoutePublicationError("DEX USD-price lineage mismatch")
        status_value = context.get("status")
        if status_value == "observed":
            if (
                context.get("reason_code") != "observed"
                or tvl_reason_code(context.get("reason_code")) != "observed"
            ):
                raise RoutePublicationError("observed DEX price context is unavailable")
            price_by_address = context_result["address_prices"]
            core_status = core_leg.get("status")
            if core_status in {"unsupported", "failed", "deadline_exceeded"}:
                if (
                    core_leg.get("available") is True
                    or any(
                        core_leg.get(field) is not None
                        for field in (
                            "token0_address", "token1_address",
                            "token0_price_usd", "token1_price_usd",
                        )
                    )
                ):
                    raise RoutePublicationError(
                        "terminal DEX leg cannot publish pool price evidence"
                    )
                continue
            if core_leg.get("available") is False:
                raise RoutePublicationError(
                    "observed DEX price context is unavailable"
                )
            core_price_by_address: Dict[str, str] = {}
            for address_field, price_field in (
                ("token0_address", "token0_price_usd"),
                ("token1_address", "token1_price_usd"),
            ):
                address = core_leg.get(address_field)
                price = core_leg.get(price_field)
                if (
                    not isinstance(address, str)
                    or re.fullmatch(r"0x[0-9a-f]{40}", address) is None
                    or not isinstance(price, str)
                ):
                    raise RoutePublicationError(
                        "DEX core address-price evidence is invalid"
                    )
                core_price_by_address[address] = price
            if core_price_by_address != price_by_address:
                raise RoutePublicationError("DEX address-price map mismatch")
        elif status_value in {"missing", "not_found", "failed"}:
            reason_code = context.get("reason_code")
            allowed_reasons = {
                "missing": {"source_no_tvl_observation"},
                "not_found": {"source_pool_not_found"},
                "failed": {
                    "network",
                    "rate_limit",
                    "source_unavailable",
                    "parse",
                    "validation",
                    "collection_failed",
                },
            }
            if (
                core_leg.get("available") is not False
                or core_leg.get("reason_code")
                != "usd_price_context_{}".format(status_value)
                or reason_code not in allowed_reasons[status_value]
                or tvl_reason_code(reason_code) != reason_code
                or any(
                    context.get(field) is not None
                    for field in (
                        "base_token_id",
                        "quote_token_id",
                        "base_token_price_usd",
                        "quote_token_price_usd",
                    )
                )
                or core_leg.get("token0_price_usd") is not None
                or core_leg.get("token1_price_usd") is not None
            ):
                raise RoutePublicationError("unavailable DEX price context is invalid")
        else:
            raise RoutePublicationError("DEX collector context status is invalid")


def _validate_joint_lineage(
    shadow_root: Path,
    evidence: Mapping[str, Any],
    core: Mapping[str, Any],
    phase_view: Mapping[str, Any],
) -> None:
    audit = evidence["audit"]
    universe = evidence["universe"]
    cohort = core["cohort"]
    if (
        audit["route_cohort_id"] != cohort["route_cohort_id"]
        or audit["core_manifest_sha256"] != core["manifest_sha256"]
        or audit["candidate_source_generation"]
        != cohort["candidate_source_generation"]
        or universe["candidate_source_generation"]
        != cohort["candidate_source_generation"]
        or universe["selection_window"] != cohort["selection_window"]
        or universe["requested_notionals_usd"]
        != cohort["requested_notionals_usd"]
        or universe["routes"] != core["candidates"]
        or audit["phase"] != phase_view["phase"]
        or audit["phase_state_sha256"] != phase_view["phase_state_sha256"]
        or audit["phase_transition_id"] != phase_view["phase_transition_id"]
    ):
        raise RoutePublicationError("joint route shadow lineage mismatch")
    core_leg_identity = sorted(
        (
            row.get("market_id"),
            row.get("market_type"),
            row.get("token_symbol"),
        )
        for row in core["legs"]
    )
    universe_leg_identity = sorted(
        (
            row.get("market_id"),
            row.get("market_type"),
            row.get("token_symbol"),
        )
        for row in universe["selected_legs"]
    )
    if core_leg_identity != universe_leg_identity:
        raise RoutePublicationError("joint route shadow leg inventory mismatch")
    for leg in core["legs"]:
        lineage = leg.get("typed_source_lineage")
        if lineage is None:
            raise RoutePublicationError(
                "joint route shadow leg typed-source lineage is missing"
            )
        try:
            validate_typed_source_lineage(
                lineage, market_type=str(leg.get("market_type"))
            )
        except (TypeError, ValueError) as error:
            raise RoutePublicationError(
                "joint route shadow leg typed-source lineage is invalid"
            ) from error
    retained_pool_states = _load_retained_typed_source_members(
        Path(shadow_root), core
    )
    _validate_cost_evidence_outer_lineage(
        evidence["cost_evidence"],
        run_id=audit["run_id"],
        route_cohort_id=audit["route_cohort_id"],
        phase=audit["phase"],
        candidate_source_generation=audit["candidate_source_generation"],
        route_universe_sha256_value=evidence["route_universe_sha256"],
        universe=universe,
        retained_typed_pool_state_members=retained_pool_states,
    )
    _validate_dex_collector_contexts(universe, core["legs"])
    try:
        try:
            from scripts.route_shadow_audit import build_shadow_audit
        except ModuleNotFoundError:
            from route_shadow_audit import (  # type: ignore[no-redef]
                build_shadow_audit,
            )
        rebuilt_audit = build_shadow_audit(
            cohort,
            core_pointer={
                "schema": ROUTE_CORE_POINTER_SCHEMA,
                "bundle_stage": ROUTE_CORE_BUNDLE_STAGE,
                "route_cohort_id": cohort["route_cohort_id"],
                "manifest_sha256": core["manifest_sha256"],
            },
            run={
                "run_id": audit["run_id"],
                "phase_state_sha256": audit["phase_state_sha256"],
                "phase_transition_id": audit["phase_transition_id"],
                "route_universe_sha256": audit["route_universe_sha256"],
                "baseline_manifest_sha256": audit["baseline_manifest_sha256"],
                "candidate_source_generation": audit[
                    "candidate_source_generation"
                ],
                "route_cost_evidence_sha256": audit[
                    "route_cost_evidence_sha256"
                ],
            },
            phase=audit["phase"],
            audit_finished_at=audit["audit_finished_at"],
        )
    except (ImportError, ModuleNotFoundError, TypeError, ValueError) as error:
        raise RoutePublicationError(
            "route shadow audit cannot be rebuilt from immutable facts"
        ) from error
    if rebuilt_audit != audit:
        raise RoutePublicationError(
            "route shadow audit metrics differ from immutable cohort facts"
        )


def _load_core_bundle_at_root(
    core: Path,
    core_fd: int,
    *,
    route_cohort_id: str,
    core_manifest_sha256: str,
) -> Dict[str, Any]:
    bundles_fd: Optional[int] = None
    try:
        bundles_fd, bundles_details = _open_directory_at(
            core_fd, "bundles", "route core bundles root"
        )
        validated = _validate_route_cohort_bundle(
            core / "bundles" / route_cohort_id,
            expected_route_cohort_id=route_cohort_id,
            expected_manifest_sha256=core_manifest_sha256,
            require_directory_identity=True,
            parent_fd=bundles_fd,
        )
        _verify_directory_entry(
            core_fd, "bundles", bundles_details, "route core bundles root"
        )
        return validated
    finally:
        _close_route_descriptor_group((
            (bundles_fd, "route core bundles root"),
        ))


def _pointer_from_shadow_evidence(evidence: Mapping[str, Any]) -> Dict[str, Any]:
    audit = evidence["audit"]
    return _validate_shadow_pointer({
        "schema": ROUTE_SHADOW_POINTER_SCHEMA,
        "run_id": audit["run_id"],
        "phase": audit["phase"],
        "route_cohort_id": audit["route_cohort_id"],
        "phase_state_sha256": audit["phase_state_sha256"],
        "phase_transition_id": audit["phase_transition_id"],
        "core_pointer_sha256": audit["core_pointer_sha256"],
        "core_manifest_sha256": audit["core_manifest_sha256"],
        "route_universe_sha256": audit["route_universe_sha256"],
        "route_cost_evidence_sha256": audit["route_cost_evidence_sha256"],
        "baseline_manifest_sha256": audit["baseline_manifest_sha256"],
        "candidate_source_generation": audit["candidate_source_generation"],
        "audit_sha256": evidence["audit_sha256"],
    })


def _joint_shadow_view(
    pointer: Mapping[str, Any],
    evidence: Mapping[str, Any],
    core: Mapping[str, Any],
) -> Dict[str, Any]:
    pointer_value = _clone_json(dict(pointer))
    pointer_bytes = _pointer_payload_bytes(pointer_value)
    return {
        "pointer": pointer_value,
        "pointer_sha256": _sha256_bytes(pointer_bytes),
        "audit": _clone_json(evidence["audit"]),
        "audit_sha256": evidence["audit_sha256"],
        "cohort": _clone_json(core["cohort"]),
        "manifest": _clone_json(core["manifest"]),
    }


def _load_shadow_result_evidence(
    shadow_root: Path,
    *,
    run_id: str,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    evidence = _read_shadow_run_evidence(Path(shadow_root), run_id)
    pointer = _pointer_from_shadow_evidence(evidence)
    phase_view = load_historical_phase_state(
        Path(shadow_root),
        phase=pointer["phase"],
        phase_state_sha256=pointer["phase_state_sha256"],
        phase_transition_id=pointer["phase_transition_id"],
    )
    core_root = _absolute_without_symlink_resolution(Path(shadow_root)).parent / "core"
    core, core_fd, core_details = _open_verified_directory(
        core_root, "route core root"
    )
    locked = False
    result: Optional[Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]] = None
    operation_error: Optional[BaseException] = None
    operation_traceback = None
    try:
        try:
            fcntl.flock(core_fd, fcntl.LOCK_SH)
            locked = True
        except Exception as error:
            raise RoutePublicationError("route core lock acquisition failed") from error
        loaded_core = _load_core_bundle_at_root(
            core,
            core_fd,
            route_cohort_id=pointer["route_cohort_id"],
            core_manifest_sha256=pointer["core_manifest_sha256"],
        )
        if (
            _canonical_core_pointer_sha256(
                pointer["route_cohort_id"], pointer["core_manifest_sha256"]
            ) != pointer["core_pointer_sha256"]
        ):
            raise RoutePublicationError("historical core pointer hash is invalid")
        _validate_joint_lineage(
            Path(shadow_root), evidence, loaded_core, phase_view
        )
        _verify_open_path_identity(core, core_details, "route core root")
        result = (pointer, evidence, loaded_core)
    except BaseException as error:
        operation_error = error
        operation_traceback = error.__traceback__
    cleanup_error = _release_route_lock_and_close(
        core_fd,
        locked=locked,
        label="route core",
    )
    if operation_error is not None:
        raise operation_error.with_traceback(operation_traceback)
    if cleanup_error is not None:
        raise cleanup_error
    if result is None:
        raise RoutePublicationError("historical route shadow load returned no result")
    return result


def load_shadow_result(
    shadow_root: Path,
    *,
    run_id: str,
    expected_pointer_sha256: str,
) -> Dict[str, Any]:
    """Load one immutable historical Shadow result without following latest."""
    try:
        validated_run_id = _validate_shadow_run_id(run_id)
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("route shadow run ID is invalid") from error
    if (
        not isinstance(expected_pointer_sha256, str)
        or _HEX_SHA256.fullmatch(expected_pointer_sha256) is None
    ):
        raise RoutePublicationError("expected route shadow pointer hash is invalid")
    pointer, evidence, core = _load_shadow_result_evidence(
        Path(shadow_root), run_id=validated_run_id
    )
    view = _joint_shadow_view(pointer, evidence, core)
    if view["pointer_sha256"] != expected_pointer_sha256:
        raise RoutePublicationError("route shadow pointer hash mismatch")
    return view


def _same_optional_snapshot(
    first: Optional[Tuple[bytes, os.stat_result]],
    second: Optional[Tuple[bytes, os.stat_result]],
) -> bool:
    if first is None or second is None:
        return first is None and second is None
    return _pointer_snapshot_is_owned(second, first)


def _restore_shadow_pointer_after_failure(
    shadow_fd: int,
    shadow_path: Path,
    old_pointer: Optional[Tuple[bytes, os.stat_result]],
    committed_pointer: Optional[Tuple[bytes, os.stat_result]],
) -> None:
    """Rollback only the exact pointer inode installed by this transaction."""
    current = _optional_pointer_snapshot_at(shadow_fd)
    if old_pointer is None:
        if current is None:
            return
    elif _pointer_snapshot_is_owned(current, old_pointer):
        return
    if (
        committed_pointer is None
        or not _pointer_snapshot_is_owned(current, committed_pointer)
    ):
        raise RoutePublicationError(
            "route shadow pointer commit is uncertain due to a concurrent writer"
        )
    if old_pointer is None:
        os.unlink(ROUTE_SHADOW_LATEST_FILENAME, dir_fd=shadow_fd)
        _fsync_directory(shadow_path, directory_fd=shadow_fd)
        if _optional_pointer_snapshot_at(shadow_fd) is not None:
            raise RoutePublicationError("route shadow pointer rollback failed")
        return
    _replace_pointer_bytes_at(shadow_fd, old_pointer[0])
    restored = _optional_pointer_snapshot_at(shadow_fd)
    if restored is None or restored[0] != old_pointer[0]:
        raise RoutePublicationError("route shadow pointer rollback failed")
    _fsync_directory(shadow_path, directory_fd=shadow_fd)
    if not _pointer_snapshot_is_owned(
        _optional_pointer_snapshot_at(shadow_fd), restored
    ):
        raise RoutePublicationError("route shadow pointer rollback failed")


def _commit_shadow_pointer_at_locked(
    shadow_fd: int,
    shadow_path: Path,
    pointer_bytes: bytes,
    *,
    commit_state: Optional[Dict[str, Any]] = None,
) -> Tuple[bytes, os.stat_result]:
    _replace_pointer_bytes_at(shadow_fd, pointer_bytes)
    committed = _optional_pointer_snapshot_at(shadow_fd)
    if committed is None or committed[0] != pointer_bytes:
        raise RoutePublicationError("route shadow pointer commit is uncertain")
    if commit_state is not None:
        commit_state["pointer_snapshot"] = committed
    _fsync_directory(shadow_path, directory_fd=shadow_fd)
    if not _pointer_snapshot_is_owned(
        _optional_pointer_snapshot_at(shadow_fd), committed
    ):
        raise RoutePublicationError("route shadow pointer commit is uncertain")
    directory_snapshot = os.fstat(shadow_fd)
    if commit_state is not None:
        commit_state["directory_snapshot"] = directory_snapshot
    return committed


def _release_route_lock_and_close(
    descriptor: int,
    *,
    locked: bool,
    label: str,
) -> Optional[RoutePublicationError]:
    """Attempt both cleanup steps and return the first cleanup failure."""
    first_error: Optional[RoutePublicationError] = None
    if locked:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        except BaseException as error:
            first_error = RoutePublicationError(
                "{} lock release failed".format(label)
            )
            first_error.__cause__ = error
    try:
        os.close(descriptor)
    except BaseException as error:
        if first_error is None:
            first_error = RoutePublicationError(
                "{} descriptor close failed".format(label)
            )
            first_error.__cause__ = error
    return first_error


def _close_route_descriptor_group(
    descriptors: Sequence[Tuple[Optional[int], str]],
) -> None:
    """Close every descriptor without masking an already-active exception."""
    primary_error_active = sys.exc_info()[0] is not None
    first_error: Optional[RoutePublicationError] = None
    for descriptor, label in descriptors:
        if descriptor is None:
            continue
        cleanup_error = _release_route_lock_and_close(
            descriptor,
            locked=False,
            label=label,
        )
        if first_error is None:
            first_error = cleanup_error
    if first_error is not None and not primary_error_active:
        raise first_error


def publish_shadow_result(
    shadow_root: Path,
    *,
    core_pointer: Mapping[str, Any],
    audit: Mapping[str, Any],
) -> Dict[str, Any]:
    """Install an audit and atomically commit one fully bound Shadow pointer."""
    normalized_audit = _strict_validate_shadow_audit(audit)
    validated_core_pointer = _validate_core_pointer_mapping(core_pointer)
    audit_bytes = _canonical_json_bytes(normalized_audit)
    core_pointer_bytes = _pointer_payload_bytes(validated_core_pointer)
    if (
        normalized_audit["core_pointer_sha256"]
        != _sha256_bytes(core_pointer_bytes)
        or normalized_audit["core_manifest_sha256"]
        != validated_core_pointer["manifest_sha256"]
        or normalized_audit["route_cohort_id"]
        != validated_core_pointer["route_cohort_id"]
    ):
        raise RoutePublicationError("audit and supplied core pointer differ")
    try:
        run_id = _validate_shadow_run_id(normalized_audit["run_id"])
    except (TypeError, ValueError) as error:
        raise RoutePublicationError("route shadow audit run ID is invalid") from error
    (
        initial_phase,
        initial_phase_snapshot,
        initial_phase_root_snapshot,
    ) = _load_active_phase_state_with_snapshot(Path(shadow_root))
    if (
        normalized_audit["phase"] != initial_phase["phase"]
        or normalized_audit["phase_state_sha256"]
        != initial_phase["phase_state_sha256"]
        or normalized_audit["phase_transition_id"]
        != initial_phase["phase_transition_id"]
    ):
        raise RoutePublicationError("audit phase does not match active phase")

    (
        shadow,
        shadow_install_fd,
        shadow_install_details,
        runs_fd,
        runs_details,
        run_path,
        run_fd,
        run_details,
    ) = _open_shadow_run_directory(Path(shadow_root), run_id)
    try:
        _install_immutable_audit_at(run_fd, run_path, audit_bytes)
        _verify_directory_entry(runs_fd, run_id, run_details, "route shadow run")
        _verify_directory_entry(
            shadow_install_fd, "runs", runs_details, "route shadow runs root"
        )
        _verify_open_path_snapshot(
            shadow, shadow_install_details, "route shadow root"
        )
    finally:
        primary_error_active = sys.exc_info()[0] is not None
        install_cleanup_error: Optional[RoutePublicationError] = None
        for descriptor, label in (
            (run_fd, "route shadow run"),
            (runs_fd, "route shadow runs root"),
            (shadow_install_fd, "route shadow install root"),
        ):
            current_cleanup_error = _release_route_lock_and_close(
                descriptor,
                locked=False,
                label=label,
            )
            if install_cleanup_error is None:
                install_cleanup_error = current_cleanup_error
        if install_cleanup_error is not None and not primary_error_active:
            raise install_cleanup_error

    core_root = _absolute_without_symlink_resolution(Path(shadow_root)).parent / "core"
    core, core_fd, core_details = _open_verified_directory(
        core_root, "route core root"
    )
    try:
        shadow, shadow_fd, shadow_details = _open_verified_directory(
            Path(shadow_root), "route shadow root"
        )
    except BaseException:
        _release_route_lock_and_close(
            core_fd,
            locked=False,
            label="route core",
        )
        raise
    core_locked = False
    shadow_locked = False
    old_pointer: Optional[Tuple[bytes, os.stat_result]] = None
    pointer_bytes = b""
    result: Optional[Dict[str, Any]] = None
    operation_error: Optional[BaseException] = None
    operation_traceback = None
    try:
        if (
            _stable_file_metadata(shadow_details)
            != _stable_file_metadata(initial_phase_root_snapshot)
        ):
            raise RoutePublicationError("active route shadow phase changed")
        try:
            fcntl.flock(core_fd, fcntl.LOCK_SH)
            core_locked = True
        except Exception as error:
            raise RoutePublicationError("route core lock acquisition failed") from error
        core_snapshot = _optional_pointer_snapshot_at(core_fd)
        if core_snapshot is None or core_snapshot[0] != core_pointer_bytes:
            raise RoutePublicationError("supplied route core pointer is not current")
        try:
            fcntl.flock(shadow_fd, fcntl.LOCK_EX)
            shadow_locked = True
        except Exception as error:
            raise RoutePublicationError("route shadow lock acquisition failed") from error
        old_pointer = _optional_pointer_snapshot_at(shadow_fd)
        evidence = _read_shadow_run_evidence(shadow, run_id)
        if evidence["audit"] != normalized_audit:
            raise RoutePublicationError("installed route shadow audit differs")
        loaded_core = _load_core_bundle_at_root(
            core,
            core_fd,
            route_cohort_id=validated_core_pointer["route_cohort_id"],
            core_manifest_sha256=validated_core_pointer["manifest_sha256"],
        )
        (
            final_phase,
            final_phase_snapshot,
            final_phase_root_snapshot,
        ) = _load_active_phase_state_with_snapshot(shadow)
        if (
            final_phase != initial_phase
            or not _same_optional_snapshot(
                initial_phase_snapshot, final_phase_snapshot
            )
            or _stable_file_metadata(final_phase_root_snapshot)
            != _stable_file_metadata(initial_phase_root_snapshot)
        ):
            raise RoutePublicationError("active route shadow phase changed")
        _validate_joint_lineage(shadow, evidence, loaded_core, final_phase)
        pointer = _pointer_from_shadow_evidence(evidence)
        pointer_bytes = _pointer_payload_bytes(pointer)
        _verify_open_path_identity(core, core_details, "route core root")
        _verify_open_path_snapshot(shadow, shadow_details, "route shadow root")
        if not _pointer_snapshot_is_owned(
            _optional_pointer_snapshot_at(core_fd), core_snapshot
        ):
            raise RoutePublicationError("route core changed before shadow commit")
        commit_state: Dict[str, Any] = {}
        try:
            _commit_shadow_pointer_at_locked(
                shadow_fd,
                shadow,
                pointer_bytes,
                commit_state=commit_state,
            )
            (
                post_phase,
                post_phase_snapshot,
                post_phase_root_snapshot,
            ) = _load_active_phase_state_with_snapshot(shadow)
            if (
                post_phase != final_phase
                or not _same_optional_snapshot(final_phase_snapshot, post_phase_snapshot)
                or _stable_file_metadata(post_phase_root_snapshot)
                != _stable_file_metadata(commit_state["directory_snapshot"])
                or not _pointer_snapshot_is_owned(
                    _optional_pointer_snapshot_at(core_fd), core_snapshot
                )
            ):
                raise RoutePublicationError(
                    "joint route shadow lineage changed during pointer commit"
                )
        except BaseException:
            _restore_shadow_pointer_after_failure(
                shadow_fd,
                shadow,
                old_pointer,
                commit_state.get("pointer_snapshot"),
            )
            raise
        result = _joint_shadow_view(pointer, evidence, loaded_core)
    except BaseException as error:
        operation_error = error
        operation_traceback = error.__traceback__

    cleanup_error = _release_route_lock_and_close(
        shadow_fd,
        locked=shadow_locked,
        label="route shadow",
    )
    core_cleanup_error = _release_route_lock_and_close(
        core_fd,
        locked=core_locked,
        label="route core",
    )
    if cleanup_error is None:
        cleanup_error = core_cleanup_error
    if operation_error is not None:
        raise operation_error.with_traceback(operation_traceback)
    if cleanup_error is not None:
        raise cleanup_error
    if result is None:
        raise RoutePublicationError("route shadow publication returned no result")
    return result


def load_latest_shadow_result(shadow_root: Path) -> Dict[str, Any]:
    """Snapshot latest, resolve it through the historical path, then recheck."""
    shadow, shadow_fd, shadow_details = _open_verified_directory(
        Path(shadow_root), "route shadow root"
    )
    result: Optional[Dict[str, Any]] = None
    operation_error: Optional[BaseException] = None
    operation_traceback = None
    try:
        snapshot = _optional_pointer_snapshot_at(shadow_fd)
        if snapshot is None:
            raise RoutePublicationError("route shadow pointer is missing")
        pointer_bytes = snapshot[0]
        pointer = _validate_shadow_pointer(
            _decode_json_object_bytes(pointer_bytes, label="route shadow pointer")
        )
        if pointer_bytes != _pointer_payload_bytes(pointer):
            raise RoutePublicationError("route shadow pointer is not canonical")
        loaded = load_shadow_result(
            shadow,
            run_id=pointer["run_id"],
            expected_pointer_sha256=_sha256_bytes(pointer_bytes),
        )
        if loaded["pointer"] != pointer or not _pointer_snapshot_is_owned(
            _optional_pointer_snapshot_at(shadow_fd), snapshot
        ):
            raise RoutePublicationError("route shadow pointer changed during validation")
        _verify_open_path_snapshot(shadow, shadow_details, "route shadow root")
        result = loaded
    except BaseException as error:
        operation_error = error
        operation_traceback = error.__traceback__
    cleanup_error = _release_route_lock_and_close(
        shadow_fd,
        locked=False,
        label="route shadow",
    )
    if operation_error is not None:
        raise operation_error.with_traceback(operation_traceback)
    if cleanup_error is not None:
        raise cleanup_error
    if result is None:
        raise RoutePublicationError("latest route shadow load returned no result")
    return result
