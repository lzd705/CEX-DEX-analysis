"""Build deterministic, publish-safe observations of available market data."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import stat
import tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit


SCHEMA_VERSION = "data_quality_snapshot/v1"
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SQLITE_SNAPSHOT_ID_RE = re.compile(r"^[0-9a-f]{24}$")
_SQLITE_IMPORT_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_TVL_GENERATION_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_LATEST_SNAPSHOT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_MAX_CSV_BYTES = 64 * 1024 * 1024
_MAX_SQLITE_BYTES = 512 * 1024 * 1024
_CEX_COLUMNS = (
    "date",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "quote_volume_usd",
)
_CEX_IDENTITY_FIELDS = _CEX_COLUMNS[:4]
_CEX_MEASUREMENT_FIELDS = _CEX_COLUMNS[4:]
_DEX_COLUMNS = (
    "date",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "open",
    "high",
    "low",
    "close",
    "dex_volume_usd",
    "pool_tvl_usd",
)
_DEX_IDENTITY_FIELDS = _DEX_COLUMNS[:6]
_DEX_REQUIRED_FIELDS = _DEX_COLUMNS[:-1]
_DEX_MEASUREMENT_FIELDS = _DEX_COLUMNS[6:]
_DEX_REQUIRED_MEASUREMENT_FIELDS = _DEX_COLUMNS[6:-1]
_TVL_COLUMNS = (
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "source_dex",
    "source_pool_name",
    "base_token_id",
    "quote_token_id",
    "tvl_usd",
    "base_token_price_usd",
    "quote_token_price_usd",
    "volume_24h_usd",
    "pool_created_at",
    "tvl_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
    "status",
    "reason_code",
    "error",
)
_TVL_REQUIRED_FIELDS = (
    "snapshot_id",
    "observed_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "tvl_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
    "status",
    "reason_code",
)
_TVL_MEASUREMENT_FIELDS = (
    "tvl_usd",
    "base_token_price_usd",
    "quote_token_price_usd",
    "volume_24h_usd",
)
_TVL_STATUSES = {"observed", "missing", "not_found", "failed"}
_TVL_STATUS_REASONS = {
    "observed": {"observed"},
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

_DEPTH_BANDS_BPS = (10, 25, 50, 100)
_CEX_DEPTH_COLUMNS = (
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "source_instrument",
    "base_asset",
    "source_quote_asset",
    "quote_to_usd",
    "quote_conversion_method",
    "quote_conversion_endpoint",
    "quote_conversion_response_sha256",
    "best_bid",
    "best_ask",
    "midpoint",
    "spread_quote",
    "spread_bps",
    "bid_levels_returned",
    "ask_levels_returned",
    "requested_level_limit",
    "full_book_reported",
) + tuple(
    field
    for band in _DEPTH_BANDS_BPS
    for field in (
        f"bid_depth_{band}bps_usd",
        f"ask_depth_{band}bps_usd",
        f"total_depth_{band}bps_usd",
        f"depth_{band}bps_complete",
    )
) + (
    "depth_method",
    "source",
    "source_endpoint",
    "source_sequence",
    "raw_response_sha256",
    "status",
    "reason_code",
    "error",
)
_CEX_DEPTH_REQUIRED_FIELDS = (
    "snapshot_id",
    "observed_at",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "status",
    "reason_code",
)
_CEX_DEPTH_MEASUREMENT_FIELDS = (
    "best_bid",
    "best_ask",
    "midpoint",
    "spread_quote",
    "spread_bps",
) + tuple(
    f"{side}_depth_{band}bps_usd"
    for band in _DEPTH_BANDS_BPS
    for side in ("bid", "ask", "total")
)
_CEX_DEPTH_STATUS_REASONS = {
    "observed": {"observed"},
    "partial": {"source_level_limit"},
    "failed": {
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
    },
}

_DEX_DEPTH_COLUMNS = (
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "protocol_model",
    "block_number",
    "block_timestamp",
    "target_token_address",
    "target_token_position",
    "token0_address",
    "token0_symbol",
    "token0_decimals",
    "token0_price_usd",
    "token1_address",
    "token1_symbol",
    "token1_decimals",
    "token1_price_usd",
    "fee_bps",
    "pool_state_price_usd",
    "source_target_price_usd",
    "price_difference_bps",
    "usd_price_source_snapshot_id",
    "usd_price_observed_at",
    "usd_price_skew_seconds",
    "usd_price_freshness_status",
    "usd_price_source",
    "usd_price_source_endpoint",
    "usd_price_raw_response_sha256",
) + tuple(
    field
    for band in _DEPTH_BANDS_BPS
    for field in (
        f"sell_depth_{band}bps_usd",
        f"buy_depth_{band}bps_usd",
        f"total_depth_{band}bps_usd",
        f"depth_{band}bps_complete",
    )
) + (
    "depth_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
    "status",
    "reason_code",
    "error",
)
_DEX_DEPTH_REQUIRED_FIELDS = (
    "snapshot_id",
    "observed_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "status",
    "reason_code",
)
_DEX_DEPTH_MEASURED_LINEAGE_FIELDS = (
    "block_number",
    "block_timestamp",
    "protocol_model",
    "target_token_address",
    "usd_price_source_snapshot_id",
    "usd_price_observed_at",
    "usd_price_raw_response_sha256",
    "raw_response_sha256",
)
_DEX_DEPTH_MEASUREMENT_FIELDS = tuple(
    f"{side}_depth_{band}bps_usd"
    for band in _DEPTH_BANDS_BPS
    for side in ("sell", "buy", "total")
)
_DEX_DEPTH_STATUS_REASONS = {
    "observed": {"observed"},
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

_EXECUTION_NOTIONALS = (
    Decimal("1000"),
    Decimal("5000"),
    Decimal("10000"),
    Decimal("50000"),
    Decimal("100000"),
)
_EXECUTION_NOTIONAL_DEFINITION = (
    "target Token quantity valued at the snapshot pre-trade reference price"
)
_EXECUTION_DIRECTIONS = ("sell_token", "buy_token")
_EXECUTION_STATUSES = {"observed", "partial", "unsupported", "failed"}
_EXECUTION_COLUMNS = (
    "snapshot_id",
    "source_snapshot_id",
    "contract_version",
    "calculation_method",
    "observed_at",
    "state_observed_at",
    "request_started_at",
    "response_received_at",
    "market_id",
    "market_type",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "source_instrument",
    "base_asset",
    "source_quote_asset",
    "chain",
    "dex",
    "pool_address",
    "block_number",
    "block_timestamp",
    "protocol_model",
    "target_token_address",
    "target_token_decimals",
    "quote_token_address",
    "quote_token_decimals",
    "direction",
    "requested_notional_usd",
    "notional_definition",
    "reference_price_method",
    "reference_price_quote_per_token",
    "quote_to_usd",
    "reference_price_usd_per_token",
    "reference_notional_usd",
    "usd_price_source_snapshot_id",
    "usd_price_observed_at",
    "target_token_quantity",
    "filled_token_quantity",
    "fill_ratio",
    "quote_amount",
    "quote_amount_usd",
    "filled_vwap_quote_per_token",
    "filled_vwap_usd_per_token",
    "quoted_execution_cost_usd",
    "quoted_execution_cost_bps",
    "levels_or_ticks_consumed",
    "ending_marginal_price_quote_per_token",
    "fee_status",
    "fee_rate_bps",
    "fee_amount_usd",
    "usd_conversion_status",
    "excluded_costs",
    "status",
    "status_reason",
    "source",
    "source_endpoint",
    "source_sequence",
    "raw_response_sha256",
    "error",
)
_EXECUTION_REQUIRED_FIELDS = (
    "snapshot_id",
    "source_snapshot_id",
    "observed_at",
    "market_id",
    "market_type",
    "token_symbol",
    "direction",
    "requested_notional_usd",
    "status",
    "status_reason",
)
_EXECUTION_MEASUREMENT_FIELDS = (
    "requested_notional_usd",
    "filled_token_quantity",
    "quoted_execution_cost_usd",
)
_EXECUTION_MEASURED_PROVENANCE_FIELDS = (
    "state_observed_at",
    "reference_price_method",
    "fee_status",
    "usd_conversion_status",
    "excluded_costs",
    "source",
    "source_endpoint",
    "raw_response_sha256",
)
_DEX_EXECUTION_LINEAGE_FIELDS = (
    "block_number",
    "block_timestamp",
    "protocol_model",
    "target_token_address",
    "target_token_decimals",
    "quote_token_address",
    "quote_token_decimals",
    "fee_rate_bps",
    "usd_price_source_snapshot_id",
    "usd_price_observed_at",
)

_EVENT_COLUMNS = (
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
)
_EVENT_REQUIRED_FIELDS = (
    "event_id",
    "revision",
    "token_symbol",
    "event_type",
    "event_subtype",
    "event_name",
    "lifecycle",
    "effective_at",
    "effective_at_precision",
    "source_kind",
    "evidence_status",
    "source_url",
    "source_checked_at_utc",
    "source_record_file",
    "record_locator",
    "recorded_at_utc",
    "revision_reason",
)
_EVENT_TYPES = {"unlock", "airdrop", "cex_listing"}
_EVENT_SUBTYPES = {
    "unlock": {"scheduled_release"},
    "airdrop": {"claim_start"},
    "cex_listing": {"spot_trading_start"},
}
_EVENT_LIFECYCLES = {
    "scheduled",
    "occurred",
    "postponed",
    "cancelled",
    "superseded",
}
_EVENT_EVIDENCE_STATUSES = {
    "primary_confirmed",
    "cross_checked",
    "onchain_observed",
}
_EVENT_SOURCE_KINDS = {
    "official_project",
    "official_governance",
    "official_exchange",
    "onchain_transaction",
}
_EVENT_MEASUREMENT_FIELDS = ("amount_token", "amount_usd", "percent_of_supply")
_MAX_EVENT_RECORD_BYTES = 1024 * 1024
_CEX_LIFECYCLE_ROOT_FIELDS = {
    "schema",
    "generated_at_utc",
    "checked_at_utc",
    "response_sha256",
    "inventory_count",
    "configured_market_count",
    "configured_market_ids_sha256",
    "review_count",
    "reviews",
}
_CEX_LIFECYCLE_REVIEW_FIELDS = {
    "market_id",
    "market_type",
    "token_symbol",
    "exchange",
    "instrument",
    "current_listing_status",
    "reason_code",
    "checked_at_utc",
    "source_url",
    "http_status",
    "response_sha256",
    "inventory_count",
    "instrument_present",
}
_CEX_LIFECYCLE_STATUS = "absent_from_official_current_catalog"
_CEX_LIFECYCLE_REASON = "instrument_absent_from_current_catalog"
_MARKET_LIFECYCLE_ROOT_FIELDS = {
    "schema",
    "generated_at_utc",
    "review_count",
    "reviews",
}
_MARKET_LIFECYCLE_REVISION_FIELDS = {
    "review_id",
    "revision",
    "supersedes_revision",
    "review_status",
    "reviewed_issue_id",
    "original_category",
    "original_reason_code",
    "market_id",
    "market_type",
    "token_symbol",
    "issue_date",
    "disposition_status",
    "disposition_reason_code",
    "market_lifecycle",
    "evidence_status",
    "review_method",
    "review_actor",
    "reviewed_at_utc",
    "disposition_note",
    "source_checks",
}
_MARKET_LIFECYCLE_SOURCE_FIELDS = {
    "source_kind",
    "url",
    "http_status",
    "response_sha256",
    "checked_at_utc",
    "observations",
}
_ROUTE_OPPORTUNITY_POINTER_FIELDS = {
    "schema",
    "bundle_stage",
    "route_cohort_id",
    "manifest_sha256",
    "core_manifest_sha256",
    "core_pointer_sha256",
}
_ROUTE_SHADOW_POINTER_FIELDS = {
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
}
_ROUTE_COHORT_ID_RE = re.compile(r"cohort:[0-9a-f]{64}")
_ROUTE_RUN_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_ROUTE_SHA256_RE = re.compile(r"[0-9a-f]{64}")


FAMILY_SPECS: Sequence[Mapping[str, Any]] = (
    {
        "name": "cex_daily_ohlcv",
        "grain": "utc_date_x_token_x_exchange_x_exact_instrument",
        "primary_key": ["date", "token_symbol", "exchange", "cex_symbol"],
        "time_fields": ["date"],
    },
    {
        "name": "cex_depth",
        "grain": "point_in_time_exact_cex_book",
        "primary_key": ["token_symbol", "exchange", "cex_symbol"],
        "time_fields": ["observed_at"],
    },
    {
        "name": "cex_execution_cost",
        "grain": "cex_market_x_direction_x_requested_notional",
        "primary_key": [
            "snapshot_id",
            "market_id",
            "direction",
            "requested_notional_usd",
        ],
        "time_fields": ["state_observed_at", "observed_at"],
    },
    {
        "name": "cex_instrument_lifecycle",
        "grain": "current_catalog_absence_review",
        "primary_key": ["market_id"],
        "time_fields": ["checked_at_utc"],
    },
    {
        "name": "dex_daily_ohlcv",
        "grain": "utc_date_x_token_perspective_x_chain_x_pool",
        "primary_key": ["date", "token_symbol", "chain", "pool_address"],
        "time_fields": ["date"],
    },
    {
        "name": "dex_depth",
        "grain": "fixed_block_pool_state_observation",
        "primary_key": ["token_symbol", "chain", "pool_address"],
        "time_fields": ["block_timestamp", "observed_at"],
    },
    {
        "name": "dex_execution_cost",
        "grain": "dex_market_x_direction_x_requested_notional",
        "primary_key": [
            "snapshot_id",
            "market_id",
            "direction",
            "requested_notional_usd",
        ],
        "time_fields": ["block_timestamp", "state_observed_at", "observed_at"],
    },
    {
        "name": "event_facts",
        "grain": "event_revision",
        "primary_key": ["event_id", "revision"],
        "time_fields": ["effective_at", "source_checked_at_utc", "recorded_at_utc"],
    },
    {
        "name": "market_lifecycle_reviews",
        "grain": "exact_issue_disposition_revision",
        "primary_key": ["review_id", "revision"],
        "time_fields": ["issue_date", "reviewed_at_utc"],
    },
    {
        "name": "route_cohort_opportunity",
        "grain": "route_x_requested_notional_opportunity",
        "primary_key": ["opportunity_id"],
        "time_fields": ["collection_started_at", "collection_completed_at"],
    },
    {
        "name": "route_shadow_route_cost_evidence",
        "grain": "route_notional_cost_binding",
        "primary_key": ["binding_key"],
        "time_fields": ["observation_started_at", "observation_completed_at"],
    },
    {
        "name": "tvl",
        "grain": "point_in_time_pool_observation",
        "primary_key": ["token_symbol", "chain", "pool_address"],
        "time_fields": ["observed_at"],
    },
)


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def canonical_snapshot_bytes(snapshot: Mapping[str, Any]) -> bytes:
    """Return the complete snapshot in its canonical JSON representation."""

    return _canonical_bytes(snapshot)


def write_snapshot(path: Path, snapshot: Mapping[str, Any]) -> None:
    """Atomically replace ``path`` with canonical snapshot bytes."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_path = tempfile.mkstemp(
        prefix="." + target.name + ".",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_snapshot_bytes(snapshot))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, target)
        temporary_path = ""
        try:
            directory_descriptor = os.open(str(target.parent), os.O_RDONLY)
        except OSError:
            directory_descriptor = None
        if directory_descriptor is not None:
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
    finally:
        if temporary_path:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass


def _parse_generated_at(value: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("generated_at_utc must be canonical RFC3339 UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise ValueError("generated_at_utc must be canonical RFC3339 UTC")
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("generated_at_utc must be canonical RFC3339 UTC")
    canonical = parsed.isoformat().replace("+00:00", "Z")
    if canonical != value:
        raise ValueError("generated_at_utc must be canonical RFC3339 UTC")
    return parsed.astimezone(timezone.utc)


def _empty_family(spec: Mapping[str, Any]) -> Dict[str, Any]:
    name = str(spec["name"])
    return {
        "name": name,
        "grain": spec["grain"],
        "primary_key": list(spec["primary_key"]),
        "time_fields": list(spec["time_fields"]),
        "state": "not_evaluated",
        "not_evaluated_reason": (
            "route_pointer_missing" if name.startswith("route_") else "source_file_missing"
        ),
        "failure_reason": None,
        "counts": {
            "expected": None,
            "observed": None,
            "usable": None,
            "expected_basis": None,
        },
        "coverage_bps": None,
        "duplicate_primary_key": {"count": None, "rate_bps": None},
        "required_field_null": {"count": None, "rate_bps": None},
        "measurements": {"null_count": None, "zero_count": None, "fields": {}},
        "status_counts": {},
        "reason_counts": {},
        "observation_time": {
            "min": None,
            "max": None,
            "freshness_lag_seconds": None,
        },
        "source": None,
    }


def _snapshot_hash(snapshot_without_hash: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(snapshot_without_hash)).hexdigest()


class _PublicDataError(Exception):
    def __init__(
        self,
        reason: str,
        *,
        duplicate_count: Optional[int] = None,
        duplicate_denominator: Optional[int] = None,
        required_null_count: Optional[int] = None,
        required_null_denominator: Optional[int] = None,
    ):
        super().__init__(reason)
        self.reason = reason
        self.duplicate_count = duplicate_count
        self.duplicate_denominator = duplicate_denominator
        self.required_null_count = required_null_count
        self.required_null_denominator = required_null_denominator


def _basis_points(numerator: int, denominator: int) -> Optional[int]:
    if denominator <= 0:
        return None
    value = (Decimal(numerator) * Decimal(10000) / Decimal(denominator)).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return int(value)


def _logical_source(logical_path: str, payload: bytes) -> Dict[str, Any]:
    return {
        "logical_path": logical_path,
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _candidate_paths(data_dir: Path, filename: str, nested: str) -> Sequence[Tuple[str, Path]]:
    return (
        (filename, data_dir / filename),
        (nested + "/" + filename, data_dir / nested / filename),
    )


def _capture_candidate(
    data_dir: Path,
    filename: str,
    nested: str,
    byte_limit: int,
) -> Optional[Dict[str, Any]]:
    present = [
        (logical_path, path)
        for logical_path, path in _candidate_paths(data_dir, filename, nested)
        if os.path.lexists(str(path))
    ]
    if len(present) > 1:
        raise _PublicDataError("ambiguous_source_candidates")
    if not present:
        return None

    logical_path, path = present[0]
    if logical_path.startswith(nested + "/"):
        try:
            parent_status = path.parent.lstat()
        except OSError:
            raise _PublicDataError("unsafe_source_file")
        if stat.S_ISLNK(parent_status.st_mode) or not stat.S_ISDIR(parent_status.st_mode):
            raise _PublicDataError("unsafe_source_file")
    try:
        before = path.lstat()
    except OSError:
        raise _PublicDataError("source_capture_failed")
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise _PublicDataError("unsafe_source_file")
    if before.st_size > byte_limit:
        raise _PublicDataError("source_file_too_large")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(str(path), flags)
    except OSError:
        raise _PublicDataError("source_capture_failed")
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise _PublicDataError("unsafe_source_file")
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise _PublicDataError("source_changed_during_capture")
        chunks = []
        total = 0
        while True:
            block = os.read(descriptor, min(1024 * 1024, byte_limit + 1 - total))
            if not block:
                break
            chunks.append(block)
            total += len(block)
            if total > byte_limit:
                raise _PublicDataError("source_file_too_large")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    stable_before = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
    stable_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if stable_before != stable_after or total != opened.st_size:
        raise _PublicDataError("source_changed_during_capture")
    payload = b"".join(chunks)
    return {"payload": payload, "identity": _logical_source(logical_path, payload)}


def _read_cex_inventory(
    database_payload: bytes,
    cex_source: Mapping[str, Any],
    csv_row_count: int,
    csv_markets: set[Tuple[str, str, str]],
) -> Tuple[List[Tuple[str, str, str]], Dict[str, Any]]:
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as handle:
            handle.write(database_payload)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = handle.name
        connection = sqlite3.connect(
            "file:" + temporary_path + "?mode=ro&immutable=1",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise _PublicDataError("authoritative_inventory_invalid")
            state = connection.execute(
                """
                SELECT snapshots.snapshot_id,
                       state.import_run_id,
                       runs.snapshot_id AS run_snapshot_id,
                       snapshots.cex_source_name,
                       snapshots.cex_source_bytes,
                       snapshots.cex_sha256,
                       snapshots.cex_row_count,
                       runs.status
                FROM dataset_state AS state
                JOIN dataset_snapshots AS snapshots
                  ON snapshots.snapshot_id = state.snapshot_id
                JOIN import_runs AS runs
                  ON runs.run_id = state.import_run_id
                WHERE state.singleton_id = 1
                """
            ).fetchone()
            if state is None or state["status"] != "published":
                raise _PublicDataError("authoritative_inventory_invalid")
            snapshot_id = state["snapshot_id"]
            import_run_id = state["import_run_id"]
            if (
                not isinstance(snapshot_id, str)
                or not _SQLITE_SNAPSHOT_ID_RE.fullmatch(snapshot_id)
                or not isinstance(import_run_id, str)
                or not _SQLITE_IMPORT_RUN_ID_RE.fullmatch(import_run_id)
                or state["run_snapshot_id"] != snapshot_id
            ):
                raise _PublicDataError("authoritative_inventory_invalid")
            if (
                state["cex_source_name"] != "cex_exchange_volume_daily.csv"
                or state["cex_source_bytes"] != cex_source["size_bytes"]
                or state["cex_sha256"] != cex_source["sha256"]
            ):
                raise _PublicDataError("authoritative_inventory_source_mismatch")
            actual_cex_row_count = connection.execute(
                "SELECT COUNT(*) FROM cex_market_daily"
            ).fetchone()[0]
            if (
                not isinstance(state["cex_row_count"], int)
                or state["cex_row_count"] != actual_cex_row_count
                or state["cex_row_count"] != csv_row_count
            ):
                raise _PublicDataError("authoritative_inventory_invalid")
            markets = sorted(
                {
                    (str(row[0]).strip().upper(), str(row[1]).strip(), str(row[2]).strip())
                    for row in connection.execute(
                    """
                    SELECT DISTINCT token_symbol, exchange, cex_symbol
                    FROM cex_market_daily
                    ORDER BY token_symbol, exchange, cex_symbol
                    """
                    ).fetchall()
                }
            )
        finally:
            connection.close()
    except _PublicDataError:
        raise
    except (OSError, sqlite3.Error, TypeError, ValueError):
        raise _PublicDataError("authoritative_inventory_invalid")
    finally:
        if temporary_path is not None:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
    if not markets or any(not all(market) for market in markets):
        raise _PublicDataError("authoritative_inventory_invalid")
    if set(markets) != csv_markets:
        raise _PublicDataError("authoritative_inventory_market_mismatch")
    generation = {
        "snapshot_id_sha256": _opaque_identifier_hash(
            "data_quality_snapshot/v1/dataset_snapshot", state["snapshot_id"]
        ),
        "import_run_id_sha256": _opaque_identifier_hash(
            "data_quality_snapshot/v1/import_run", state["import_run_id"]
        ),
    }
    return markets, generation


def _canonical_dex_market(
    token_symbol: Any,
    chain: Any,
    pool_address: Any,
) -> Tuple[str, str, str]:
    token = str(token_symbol).strip().upper()
    normalized_chain = str(chain).strip().lower()
    pool = str(pool_address).strip()
    if pool.startswith("0x"):
        pool = pool.lower()
    return token, normalized_chain, pool


def _read_dex_inventory(
    database_payload: bytes,
    dex_source: Mapping[str, Any],
    csv_row_count: int,
    csv_markets: set[Tuple[str, str, str]],
) -> Tuple[List[Tuple[str, str, str]], Dict[str, Any]]:
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as handle:
            handle.write(database_payload)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = handle.name
        connection = sqlite3.connect(
            "file:" + temporary_path + "?mode=ro&immutable=1",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise _PublicDataError("authoritative_inventory_invalid")
            state = connection.execute(
                """
                SELECT snapshots.snapshot_id,
                       state.import_run_id,
                       runs.snapshot_id AS run_snapshot_id,
                       snapshots.dex_source_name,
                       snapshots.dex_source_bytes,
                       snapshots.dex_sha256,
                       snapshots.dex_row_count,
                       runs.status
                FROM dataset_state AS state
                JOIN dataset_snapshots AS snapshots
                  ON snapshots.snapshot_id = state.snapshot_id
                JOIN import_runs AS runs
                  ON runs.run_id = state.import_run_id
                WHERE state.singleton_id = 1
                """
            ).fetchone()
            if state is None or state["status"] != "published":
                raise _PublicDataError("authoritative_inventory_invalid")
            snapshot_id = state["snapshot_id"]
            import_run_id = state["import_run_id"]
            if (
                not isinstance(snapshot_id, str)
                or not _SQLITE_SNAPSHOT_ID_RE.fullmatch(snapshot_id)
                or not isinstance(import_run_id, str)
                or not _SQLITE_IMPORT_RUN_ID_RE.fullmatch(import_run_id)
                or state["run_snapshot_id"] != snapshot_id
            ):
                raise _PublicDataError("authoritative_inventory_invalid")
            if (
                state["dex_source_name"] != "dex_pool_volume_daily.csv"
                or state["dex_source_bytes"] != dex_source["size_bytes"]
                or state["dex_sha256"] != dex_source["sha256"]
            ):
                raise _PublicDataError("authoritative_inventory_source_mismatch")
            actual_dex_row_count = connection.execute(
                "SELECT COUNT(*) FROM dex_pool_daily"
            ).fetchone()[0]
            if (
                not isinstance(state["dex_row_count"], int)
                or state["dex_row_count"] != actual_dex_row_count
                or state["dex_row_count"] != csv_row_count
            ):
                raise _PublicDataError("authoritative_inventory_invalid")
            markets = sorted(
                {
                    _canonical_dex_market(row[0], row[1], row[2])
                    for row in connection.execute(
                        """
                        SELECT DISTINCT token_symbol, chain, pool_address
                        FROM dex_pool_daily
                        ORDER BY token_symbol, chain, pool_address
                        """
                    ).fetchall()
                }
            )
        finally:
            connection.close()
    except _PublicDataError:
        raise
    except (OSError, sqlite3.Error, TypeError, ValueError):
        raise _PublicDataError("authoritative_inventory_invalid")
    finally:
        if temporary_path is not None:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
    if not markets or any(not all(market) for market in markets):
        raise _PublicDataError("authoritative_inventory_invalid")
    if set(markets) != csv_markets:
        raise _PublicDataError("authoritative_inventory_market_mismatch")
    generation = {
        "snapshot_id_sha256": _opaque_identifier_hash(
            "data_quality_snapshot/v1/dataset_snapshot", state["snapshot_id"]
        ),
        "import_run_id_sha256": _opaque_identifier_hash(
            "data_quality_snapshot/v1/import_run", state["import_run_id"]
        ),
    }
    return markets, generation


def _parse_canonical_day(value: str) -> date:
    if not value or value.strip() != value:
        raise _PublicDataError("invalid_observation_date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise _PublicDataError("invalid_observation_date")
    if parsed.isoformat() != value:
        raise _PublicDataError("invalid_observation_date")
    return parsed


def _parse_decimal(value: str) -> Optional[Decimal]:
    if value is None or value.strip() == "":
        return None
    try:
        parsed = Decimal(value.strip())
    except InvalidOperation:
        raise _PublicDataError("invalid_measurement")
    if not parsed.is_finite():
        raise _PublicDataError("invalid_measurement")
    return parsed


def _market_hash(market: Sequence[str]) -> str:
    payload = "data_quality_snapshot/v1\0cex_daily_ohlcv\0" + "\0".join(market)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _dex_market_hash(market: Sequence[str]) -> str:
    payload = "data_quality_snapshot/v1\0dex_daily_ohlcv\0" + "\0".join(market)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _opaque_identifier_hash(domain: str, value: str) -> str:
    return hashlib.sha256((domain + "\0" + value).encode("utf-8")).hexdigest()


def _set_failed(family: Dict[str, Any], reason: str) -> Dict[str, Any]:
    family["state"] = "failed"
    family["not_evaluated_reason"] = None
    family["failure_reason"] = reason
    return family


def _read_cex_csv(
    payload: bytes,
    window_start: date,
    window_end: date,
) -> Tuple[List[Tuple[Mapping[str, str], date, Tuple[str, str, str]]], set[Tuple[str, str, str]]]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise _PublicDataError("invalid_utf8")
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if tuple(reader.fieldnames or ()) != _CEX_COLUMNS:
        raise _PublicDataError("schema_mismatch")
    raw_rows = list(reader)
    normalized_rows = []
    csv_markets = set()
    keys = []
    structural_null = False
    required_null_count = 0
    window_row_count = 0
    for row in raw_rows:
        if None in row:
            raise _PublicDataError("schema_mismatch")
        if any(not isinstance(row.get(field), str) for field in _CEX_COLUMNS):
            raise _PublicDataError("schema_mismatch")
        if not row["date"].strip():
            structural_null = True
            continue
        day = _parse_canonical_day(row["date"])
        in_window = window_start <= day <= window_end
        if in_window:
            window_row_count += 1
            required_null_count += sum(
                1 for field in _CEX_COLUMNS if not row[field].strip()
            )
        if any(not row[field].strip() for field in _CEX_IDENTITY_FIELDS):
            structural_null = True
            continue
        market = (
            row["token_symbol"].strip().upper(),
            row["exchange"].strip(),
            row["cex_symbol"].strip(),
        )
        key = (day.isoformat(), market[0], market[1], market[2])
        keys.append(key)
        csv_markets.add(market)
        normalized_rows.append((row, day, market))
    if structural_null:
        raise _PublicDataError(
            "required_field_null",
            required_null_count=required_null_count,
            required_null_denominator=window_row_count * len(_CEX_COLUMNS),
        )
    duplicate_count = len(keys) - len(set(keys))
    if duplicate_count:
        raise _PublicDataError(
            "duplicate_primary_key",
            duplicate_count=duplicate_count,
            duplicate_denominator=len(keys),
        )
    return normalized_rows, csv_markets


def _read_dex_csv(
    payload: bytes,
    window_start: date,
    window_end: date,
) -> Tuple[List[Tuple[Mapping[str, str], date, Tuple[str, str, str]]], set[Tuple[str, str, str]]]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise _PublicDataError("invalid_utf8")
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if tuple(reader.fieldnames or ()) != _DEX_COLUMNS:
        raise _PublicDataError("schema_mismatch")
    raw_rows = list(reader)
    normalized_rows = []
    csv_markets = set()
    keys = []
    structural_null = False
    required_null_count = 0
    window_row_count = 0
    for row in raw_rows:
        if None in row:
            raise _PublicDataError("schema_mismatch")
        if any(not isinstance(row.get(field), str) for field in _DEX_COLUMNS):
            raise _PublicDataError("schema_mismatch")
        if not row["date"].strip():
            structural_null = True
            continue
        day = _parse_canonical_day(row["date"])
        in_window = window_start <= day <= window_end
        if in_window:
            window_row_count += 1
            required_null_count += sum(
                1 for field in _DEX_REQUIRED_FIELDS if not row[field].strip()
            )
        if any(not row[field].strip() for field in _DEX_IDENTITY_FIELDS):
            structural_null = True
            continue
        market = _canonical_dex_market(
            row["token_symbol"],
            row["chain"],
            row["pool_address"],
        )
        key = (day.isoformat(), market[0], market[1], market[2])
        keys.append(key)
        csv_markets.add(market)
        normalized_rows.append((row, day, market))
    if structural_null:
        raise _PublicDataError(
            "required_field_null",
            required_null_count=required_null_count,
            required_null_denominator=window_row_count * len(_DEX_REQUIRED_FIELDS),
        )
    duplicate_count = len(keys) - len(set(keys))
    if duplicate_count:
        raise _PublicDataError(
            "duplicate_primary_key",
            duplicate_count=duplicate_count,
            duplicate_denominator=len(keys),
        )
    return normalized_rows, csv_markets


def _evaluate_cex_daily(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
    window_start: date,
    window_end: date,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        csv_capture = _capture_candidate(
            data_dir,
            "cex_exchange_volume_daily.csv",
            "local",
            _MAX_CSV_BYTES,
        )
        database_capture = _capture_candidate(
            data_dir,
            "market_facts.sqlite3",
            "local",
            _MAX_SQLITE_BYTES,
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if csv_capture is None:
        return family
    family["source"] = {"inputs": [csv_capture["identity"]]}
    if database_capture is None:
        family["not_evaluated_reason"] = "authoritative_inventory_missing"
        return family
    family["source"]["inputs"].append(database_capture["identity"])
    family["source"]["inputs"].sort(key=lambda item: item["logical_path"])

    try:
        raw_rows, csv_markets = _read_cex_csv(
            csv_capture["payload"], window_start, window_end
        )
    except _PublicDataError as error:
        if error.duplicate_count is not None:
            family["duplicate_primary_key"] = {
                "count": error.duplicate_count,
                "rate_bps": _basis_points(
                    error.duplicate_count, error.duplicate_denominator or 0
                ),
            }
        if error.required_null_count is not None:
            family["required_field_null"] = {
                "count": error.required_null_count,
                "rate_bps": _basis_points(
                    error.required_null_count, error.required_null_denominator or 0
                ),
            }
        return _set_failed(family, error.reason)
    try:
        markets, generation = _read_cex_inventory(
            database_capture["payload"],
            csv_capture["identity"],
            len(raw_rows),
            csv_markets,
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    family["source"]["data_generation"] = generation
    inventory_payload = {
        "domain": "data_quality_snapshot/v1/cex_market_inventory",
        "markets": [list(market) for market in markets],
    }
    expected_basis = {
        "inventory_source": database_capture["identity"]["logical_path"],
        "inventory_sha256": database_capture["identity"]["sha256"],
        "snapshot_id_sha256": generation["snapshot_id_sha256"],
        "import_run_id_sha256": generation["import_run_id_sha256"],
        "market_count": len(markets),
        "market_inventory_sha256": _snapshot_hash(inventory_payload),
    }
    expected_dates = [
        window_start + timedelta(days=offset)
        for offset in range((window_end - window_start).days + 1)
    ]
    expected_keys = {
        (day.isoformat(), market[0], market[1], market[2])
        for market in markets
        for day in expected_dates
    }
    observed_rows = []
    keys = []
    required_null_count = 0
    measurement_fields = {
        field: {"null_count": 0, "zero_count": 0}
        for field in _CEX_MEASUREMENT_FIELDS
    }
    try:
        for row, day, market in raw_rows:
            if not window_start <= day <= window_end:
                continue
            required_null_count += sum(1 for field in _CEX_COLUMNS if not row[field].strip())
            measurements = {}
            usable = True
            for field in _CEX_MEASUREMENT_FIELDS:
                number = _parse_decimal(row[field])
                measurements[field] = number
                if number is None:
                    measurement_fields[field]["null_count"] += 1
                    usable = False
                elif number == 0:
                    measurement_fields[field]["zero_count"] += 1
                if number is not None:
                    if field in ("open", "high", "low", "close") and number <= 0:
                        raise _PublicDataError("invalid_measurement")
                    if field in ("base_volume", "quote_volume_usd") and number < 0:
                        raise _PublicDataError("invalid_measurement")
            key = (day.isoformat(), market[0], market[1], market[2])
            keys.append(key)
            observed_rows.append((key, usable))
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    observed_count = len(observed_rows)
    duplicate_count = observed_count - len(set(keys))
    family["duplicate_primary_key"] = {
        "count": duplicate_count,
        "rate_bps": _basis_points(duplicate_count, observed_count),
    }
    required_denominator = observed_count * len(_CEX_COLUMNS)
    family["required_field_null"] = {
        "count": required_null_count,
        "rate_bps": _basis_points(required_null_count, required_denominator),
    }
    family["measurements"] = {
        "null_count": sum(item["null_count"] for item in measurement_fields.values()),
        "zero_count": sum(item["zero_count"] for item in measurement_fields.values()),
        "fields": measurement_fields,
    }
    if duplicate_count:
        return _set_failed(family, "duplicate_primary_key")

    observed_key_set = set(keys)
    usable_keys = {key for key, usable in observed_rows if usable}
    expected_count = len(expected_keys)
    usable_count = len(usable_keys & expected_keys)
    missing_keys = expected_keys - observed_key_set
    observed_expected_count = len(observed_key_set & expected_keys)
    incomplete_markets = []
    complete_market_count = 0
    ranking_eligible_market_count = 0
    for market in markets:
        market_expected = {
            (day.isoformat(), market[0], market[1], market[2]) for day in expected_dates
        }
        market_missing = market_expected - observed_key_set
        if market_missing:
            incomplete_markets.append(
                {
                    "market_identity_sha256": _market_hash(market),
                    "missing_date_count": len(market_missing),
                }
            )
        else:
            complete_market_count += 1
        if market_expected <= usable_keys:
            ranking_eligible_market_count += 1
    incomplete_markets.sort(key=lambda item: item["market_identity_sha256"])
    incomplete_markets = incomplete_markets[:100]

    min_time = min((key[0] for key in observed_key_set), default=None)
    max_time = max((key[0] for key in observed_key_set), default=None)
    freshness = None
    if max_time is not None:
        next_day = datetime.combine(
            date.fromisoformat(max_time) + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        freshness = max(0, int((generated_at - next_day).total_seconds()))
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": expected_count,
                "observed": observed_count,
                "usable": usable_count,
                "expected_basis": expected_basis,
            },
            "coverage_bps": _basis_points(observed_expected_count, expected_count),
            "observation_time": {
                "min": min_time,
                "max": max_time,
                "freshness_lag_seconds": freshness,
            },
            "reason_counts": ({"stale_partition": 1} if freshness is not None and freshness > 86400 else {}),
            "daily_coverage": {
                "expected_market_date_count": expected_count,
                "observed_market_date_count": observed_expected_count,
                "complete_market_count": complete_market_count,
                "incomplete_market_count": len(markets) - complete_market_count,
                "ranking_eligible_market_count": ranking_eligible_market_count,
                "disposition_counts": {
                    "observed": observed_expected_count,
                    "pre_listing": 0,
                    "post_delisting": 0,
                    "structurally_unsupported": 0,
                    "source_no_observation": 0,
                    "collection_failed": 0,
                    "missing_unexplained": len(missing_keys),
                },
                "incomplete_markets": incomplete_markets,
                "completeness_state": "complete" if not missing_keys else "incomplete",
            },
        }
    )
    return family


def _evaluate_dex_daily(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
    window_start: date,
    window_end: date,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        csv_capture = _capture_candidate(
            data_dir,
            "dex_pool_volume_daily.csv",
            "local",
            _MAX_CSV_BYTES,
        )
        database_capture = _capture_candidate(
            data_dir,
            "market_facts.sqlite3",
            "local",
            _MAX_SQLITE_BYTES,
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if csv_capture is None:
        return family
    family["source"] = {"inputs": [csv_capture["identity"]]}
    if database_capture is None:
        family["not_evaluated_reason"] = "authoritative_inventory_missing"
        return family
    family["source"]["inputs"].append(database_capture["identity"])
    family["source"]["inputs"].sort(key=lambda item: item["logical_path"])

    try:
        raw_rows, csv_markets = _read_dex_csv(
            csv_capture["payload"], window_start, window_end
        )
    except _PublicDataError as error:
        if error.duplicate_count is not None:
            family["duplicate_primary_key"] = {
                "count": error.duplicate_count,
                "rate_bps": _basis_points(
                    error.duplicate_count, error.duplicate_denominator or 0
                ),
            }
        if error.required_null_count is not None:
            family["required_field_null"] = {
                "count": error.required_null_count,
                "rate_bps": _basis_points(
                    error.required_null_count, error.required_null_denominator or 0
                ),
            }
        return _set_failed(family, error.reason)
    try:
        markets, generation = _read_dex_inventory(
            database_capture["payload"],
            csv_capture["identity"],
            len(raw_rows),
            csv_markets,
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    family["source"]["data_generation"] = generation
    inventory_payload = {
        "domain": "data_quality_snapshot/v1/dex_market_inventory",
        "markets": [list(market) for market in markets],
    }
    expected_basis = {
        "inventory_source": database_capture["identity"]["logical_path"],
        "inventory_sha256": database_capture["identity"]["sha256"],
        "snapshot_id_sha256": generation["snapshot_id_sha256"],
        "import_run_id_sha256": generation["import_run_id_sha256"],
        "market_count": len(markets),
        "market_inventory_sha256": _snapshot_hash(inventory_payload),
    }
    expected_dates = [
        window_start + timedelta(days=offset)
        for offset in range((window_end - window_start).days + 1)
    ]
    expected_keys = {
        (day.isoformat(), market[0], market[1], market[2])
        for market in markets
        for day in expected_dates
    }
    observed_rows = []
    keys = []
    required_null_count = 0
    measurement_fields = {
        field: {"null_count": 0, "zero_count": 0}
        for field in _DEX_MEASUREMENT_FIELDS
    }
    try:
        for row, day, market in raw_rows:
            if not window_start <= day <= window_end:
                continue
            required_null_count += sum(
                1 for field in _DEX_REQUIRED_FIELDS if not row[field].strip()
            )
            measurements = {}
            usable = True
            for field in _DEX_MEASUREMENT_FIELDS:
                number = _parse_decimal(row[field])
                measurements[field] = number
                if number is None:
                    measurement_fields[field]["null_count"] += 1
                    if field in _DEX_REQUIRED_MEASUREMENT_FIELDS:
                        usable = False
                elif number == 0:
                    measurement_fields[field]["zero_count"] += 1
                if number is not None:
                    if field in ("open", "high", "low", "close") and number <= 0:
                        raise _PublicDataError("invalid_measurement")
                    if field in ("dex_volume_usd", "pool_tvl_usd") and number < 0:
                        raise _PublicDataError("invalid_measurement")
            key = (day.isoformat(), market[0], market[1], market[2])
            keys.append(key)
            observed_rows.append((key, usable))
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    observed_count = len(observed_rows)
    duplicate_count = observed_count - len(set(keys))
    family["duplicate_primary_key"] = {
        "count": duplicate_count,
        "rate_bps": _basis_points(duplicate_count, observed_count),
    }
    family["required_field_null"] = {
        "count": required_null_count,
        "rate_bps": _basis_points(
            required_null_count, observed_count * len(_DEX_REQUIRED_FIELDS)
        ),
    }
    family["measurements"] = {
        "null_count": sum(item["null_count"] for item in measurement_fields.values()),
        "zero_count": sum(item["zero_count"] for item in measurement_fields.values()),
        "fields": measurement_fields,
    }
    if duplicate_count:
        return _set_failed(family, "duplicate_primary_key")

    observed_key_set = set(keys)
    usable_keys = {key for key, usable in observed_rows if usable}
    expected_count = len(expected_keys)
    usable_count = len(usable_keys & expected_keys)
    missing_keys = expected_keys - observed_key_set
    observed_expected_count = len(observed_key_set & expected_keys)
    incomplete_markets = []
    complete_market_count = 0
    ranking_eligible_market_count = 0
    for market in markets:
        market_expected = {
            (day.isoformat(), market[0], market[1], market[2]) for day in expected_dates
        }
        market_missing = market_expected - observed_key_set
        if market_missing:
            incomplete_markets.append(
                {
                    "market_identity_sha256": _dex_market_hash(market),
                    "missing_date_count": len(market_missing),
                }
            )
        else:
            complete_market_count += 1
        if market_expected <= usable_keys:
            ranking_eligible_market_count += 1
    incomplete_markets.sort(key=lambda item: item["market_identity_sha256"])
    incomplete_markets = incomplete_markets[:100]

    min_time = min((key[0] for key in observed_key_set), default=None)
    max_time = max((key[0] for key in observed_key_set), default=None)
    freshness = None
    if max_time is not None:
        next_day = datetime.combine(
            date.fromisoformat(max_time) + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        freshness = max(0, int((generated_at - next_day).total_seconds()))
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": expected_count,
                "observed": observed_count,
                "usable": usable_count,
                "expected_basis": expected_basis,
            },
            "coverage_bps": _basis_points(observed_expected_count, expected_count),
            "observation_time": {
                "min": min_time,
                "max": max_time,
                "freshness_lag_seconds": freshness,
            },
            "reason_counts": (
                {"stale_partition": 1}
                if freshness is not None and freshness > 86400
                else {}
            ),
            "daily_coverage": {
                "expected_market_date_count": expected_count,
                "observed_market_date_count": observed_expected_count,
                "complete_market_count": complete_market_count,
                "incomplete_market_count": len(markets) - complete_market_count,
                "ranking_eligible_market_count": ranking_eligible_market_count,
                "disposition_counts": {
                    "observed": observed_expected_count,
                    "pre_listing": 0,
                    "post_delisting": 0,
                    "structurally_unsupported": 0,
                    "source_no_observation": 0,
                    "collection_failed": 0,
                    "missing_unexplained": len(missing_keys),
                },
                "incomplete_markets": incomplete_markets,
                "completeness_state": "complete" if not missing_keys else "incomplete",
            },
        }
    )
    return family


def _parse_observation_timestamp(value: str) -> Tuple[datetime, str]:
    if not isinstance(value, str) or not value.strip():
        raise _PublicDataError("required_field_null")
    text = value.strip()
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        raise _PublicDataError("invalid_observation_timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _PublicDataError("timezone_naive_timestamp")
    normalized = parsed.astimezone(timezone.utc)
    canonical = normalized.isoformat().replace("+00:00", "Z")
    return normalized, canonical


def _is_sha256(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", value))


def _evaluate_depth(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
    *,
    filename: str,
    columns: Sequence[str],
    required_fields: Sequence[str],
    measurement_fields: Sequence[str],
    status_reasons: Mapping[str, set[str]],
    market_type: str,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        capture = _capture_candidate(data_dir, filename, "local", _MAX_CSV_BYTES)
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if capture is None:
        return family
    family["source"] = {"inputs": [capture["identity"]]}
    try:
        text = capture["payload"].decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        if tuple(reader.fieldnames or ()) != tuple(columns):
            raise _PublicDataError("schema_mismatch")
        rows = list(reader)
    except UnicodeDecodeError:
        return _set_failed(family, "invalid_utf8")
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    observed_count = len(rows)
    required_null_count = 0
    measurement_counts = {
        field: {"null_count": 0, "zero_count": 0}
        for field in measurement_fields
    }
    snapshots = set()
    keys = []
    usable_count = 0
    status_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    observation_times: List[Tuple[datetime, str]] = []
    try:
        for row in rows:
            if None in row or any(
                not isinstance(row.get(field), str) for field in columns
            ):
                raise _PublicDataError("schema_mismatch")
            nulls = [field for field in required_fields if not row[field].strip()]
            required_null_count += len(nulls)
            if nulls:
                raise _PublicDataError("required_field_null")

            snapshot_id = row["snapshot_id"].strip()
            if not _LATEST_SNAPSHOT_ID_RE.fullmatch(snapshot_id):
                raise _PublicDataError("invalid_snapshot_id")
            snapshots.add(snapshot_id)
            observed_at, observed_text = _parse_observation_timestamp(
                row["observed_at"]
            )
            if observed_at > generated_at:
                raise _PublicDataError("future_observation_timestamp")

            token = row["token_symbol"].strip().upper()
            if market_type == "cex":
                key = (
                    token,
                    row["exchange"].strip().lower(),
                    row["cex_symbol"].strip().upper(),
                )
            else:
                key = _canonical_dex_market(
                    token,
                    row["chain"],
                    row["pool_address"],
                )
            keys.append(key)

            status = row["status"].strip().lower()
            reason = row["reason_code"].strip().lower()
            if status not in status_reasons:
                raise _PublicDataError("invalid_status")
            if reason not in status_reasons[status]:
                raise _PublicDataError("status_reason_conflict")
            status_counts[status] = status_counts.get(status, 0) + 1
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

            parsed_measurements = {}
            for field in measurement_fields:
                number = _parse_decimal(row[field])
                parsed_measurements[field] = number
                if number is None:
                    measurement_counts[field]["null_count"] += 1
                elif number == 0:
                    measurement_counts[field]["zero_count"] += 1
                if number is not None and number < 0:
                    raise _PublicDataError("invalid_measurement")

            measured = status in {"observed", "partial"}
            if market_type == "cex":
                observation_times.append((observed_at, observed_text))
            if not measured:
                continue
            if any(value is None for value in parsed_measurements.values()):
                continue
            if market_type == "cex":
                best_bid = parsed_measurements["best_bid"]
                best_ask = parsed_measurements["best_ask"]
                midpoint = parsed_measurements["midpoint"]
                if (
                    best_bid is None
                    or best_ask is None
                    or midpoint is None
                    or best_bid <= 0
                    or best_ask <= 0
                    or midpoint <= 0
                    or best_bid >= best_ask
                ):
                    raise _PublicDataError("invalid_measurement")
            else:
                previous_total = Decimal("-1")
                for band in _DEPTH_BANDS_BPS:
                    total = parsed_measurements[f"total_depth_{band}bps_usd"]
                    assert total is not None
                    if total + Decimal("1e-12") < previous_total:
                        raise _PublicDataError("invalid_measurement")
                    previous_total = total
                missing_lineage = [
                    field
                    for field in _DEX_DEPTH_MEASURED_LINEAGE_FIELDS
                    if not row[field].strip()
                ]
                if missing_lineage:
                    raise _PublicDataError("required_field_null")
                block_number = row["block_number"].strip()
                if not block_number.isdigit() or int(block_number) <= 0:
                    raise _PublicDataError("invalid_fixed_block_lineage")
                for hash_field in (
                    "usd_price_raw_response_sha256",
                    "raw_response_sha256",
                ):
                    if not _is_sha256(row[hash_field].strip()):
                        raise _PublicDataError("invalid_fixed_block_lineage")
                block_time, block_text = _parse_observation_timestamp(
                    row["block_timestamp"]
                )
                if block_time > generated_at:
                    raise _PublicDataError("future_observation_timestamp")
                usd_price_time, _ = _parse_observation_timestamp(
                    row["usd_price_observed_at"]
                )
                if usd_price_time > generated_at:
                    raise _PublicDataError("future_observation_timestamp")
                observation_times.append((block_time, block_text))
            usable_count += 1
    except _PublicDataError as error:
        family["required_field_null"] = {
            "count": required_null_count,
            "rate_bps": _basis_points(
                required_null_count, observed_count * len(required_fields)
            ),
        }
        return _set_failed(family, error.reason)

    if len(snapshots) > 1:
        return _set_failed(family, "mixed_snapshot_id")
    duplicate_count = observed_count - len(set(keys))
    family["duplicate_primary_key"] = {
        "count": duplicate_count,
        "rate_bps": _basis_points(duplicate_count, observed_count),
    }
    if duplicate_count:
        return _set_failed(family, "duplicate_primary_key")

    family["required_field_null"] = {
        "count": required_null_count,
        "rate_bps": _basis_points(
            required_null_count, observed_count * len(required_fields)
        ),
    }
    family["measurements"] = {
        "null_count": sum(item["null_count"] for item in measurement_counts.values()),
        "zero_count": sum(item["zero_count"] for item in measurement_counts.values()),
        "fields": measurement_counts,
    }
    minimum = min(observation_times, default=None)
    maximum = max(observation_times, default=None)
    freshness = (
        int((generated_at - maximum[0]).total_seconds())
        if maximum is not None
        else None
    )
    if freshness is not None and freshness > 86400:
        reason_counts["stale_partition"] = 1
    inventory_payload = {
        "domain": f"data_quality_snapshot/v1/{market_type}_depth_inventory",
        "markets": [list(key) for key in sorted(set(keys))],
    }
    snapshot_id = next(iter(snapshots), None)
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": observed_count,
                "observed": observed_count,
                "usable": usable_count,
                "expected_basis": {
                    "kind": "latest_file_inventory",
                    "snapshot_id_sha256": (
                        _opaque_identifier_hash(
                            f"data_quality_snapshot/v1/{market_type}_depth_snapshot",
                            snapshot_id,
                        )
                        if snapshot_id is not None
                        else None
                    ),
                    "market_count": len(set(keys)),
                    "market_inventory_sha256": _snapshot_hash(inventory_payload),
                },
            },
            "coverage_bps": _basis_points(usable_count, observed_count),
            "status_counts": dict(sorted(status_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
            "observation_time": {
                "min": minimum[1] if minimum is not None else None,
                "max": maximum[1] if maximum is not None else None,
                "freshness_lag_seconds": freshness,
            },
        }
    )
    return family


def _is_safe_public_reason(value: str) -> bool:
    if not value or len(value) > 160:
        return False
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return False
    lowered = value.lower()
    unsafe_fragments = (
        "api_key",
        "apikey",
        "authorization",
        "bearer ",
        "cookie",
        "password",
        "secret",
        "token=",
        "://",
    )
    if any(fragment in lowered for fragment in unsafe_fragments):
        return False
    if "/" in value or "\\" in value or "=" in value:
        return False
    return True


def _execution_market_identity(
    row: Mapping[str, str],
    expected_market_type: str,
) -> Tuple[str, Tuple[str, ...]]:
    market_type = row["market_type"].strip().lower()
    if market_type != expected_market_type or row["market_type"] != market_type:
        raise _PublicDataError("invalid_market_identity")
    token = row["token_symbol"].strip().upper()
    if market_type == "cex":
        exchange = row["exchange"].strip().lower()
        symbol = row["cex_symbol"].strip().upper()
        if not exchange or not symbol:
            raise _PublicDataError("required_field_null")
        expected_market_id = f"cex:{exchange}:{symbol}"
        identity = (token, exchange, symbol)
    else:
        chain = row["chain"].strip().lower()
        dex = row["dex"].strip().lower()
        pool = row["pool_address"].strip()
        if pool.startswith("0x"):
            pool = pool.lower()
        if not chain or not dex or not pool:
            raise _PublicDataError("required_field_null")
        expected_market_id = f"dex:{chain}:{dex}:{pool}:{token}"
        identity = (token, chain, dex, pool)
    if row["market_id"].strip() != expected_market_id:
        raise _PublicDataError("invalid_market_identity")
    return expected_market_id, identity


def _evaluate_execution(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
    *,
    filename: str,
    market_type: str,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        capture = _capture_candidate(data_dir, filename, "local", _MAX_CSV_BYTES)
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if capture is None:
        return family
    family["source"] = {"inputs": [capture["identity"]]}
    try:
        text = capture["payload"].decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        if tuple(reader.fieldnames or ()) != _EXECUTION_COLUMNS:
            raise _PublicDataError("schema_mismatch")
        rows = list(reader)
    except UnicodeDecodeError:
        return _set_failed(family, "invalid_utf8")
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if not rows:
        return _set_failed(family, "empty_inventory")

    observed_count = len(rows)
    required_null_count = 0
    measurement_counts = {
        field: {"null_count": 0, "zero_count": 0}
        for field in _EXECUTION_MEASUREMENT_FIELDS
    }
    snapshots = set()
    source_snapshots = set()
    keys = []
    market_identities: Dict[str, Tuple[str, ...]] = {}
    usable_count = 0
    status_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    observation_times: List[Tuple[datetime, str]] = []
    try:
        for row in rows:
            if None in row or any(
                not isinstance(row.get(field), str) for field in _EXECUTION_COLUMNS
            ):
                raise _PublicDataError("schema_mismatch")
            nulls = [
                field for field in _EXECUTION_REQUIRED_FIELDS if not row[field].strip()
            ]
            required_null_count += len(nulls)
            if nulls:
                raise _PublicDataError("required_field_null")

            snapshot_id = row["snapshot_id"].strip()
            source_snapshot_id = row["source_snapshot_id"].strip()
            if not _LATEST_SNAPSHOT_ID_RE.fullmatch(snapshot_id):
                raise _PublicDataError("invalid_snapshot_id")
            if not _LATEST_SNAPSHOT_ID_RE.fullmatch(source_snapshot_id):
                raise _PublicDataError("invalid_snapshot_id")
            snapshots.add(snapshot_id)
            source_snapshots.add(source_snapshot_id)

            observed_at, _ = _parse_observation_timestamp(row["observed_at"])
            if observed_at > generated_at:
                raise _PublicDataError("future_observation_timestamp")
            state_time = None
            state_text = None
            if row["state_observed_at"].strip():
                state_time, state_text = _parse_observation_timestamp(
                    row["state_observed_at"]
                )
                if state_time > generated_at:
                    raise _PublicDataError("future_observation_timestamp")

            market_id, identity = _execution_market_identity(row, market_type)
            market_identities[market_id] = identity
            direction = row["direction"].strip()
            if direction not in _EXECUTION_DIRECTIONS:
                raise _PublicDataError("invalid_direction")
            requested = _parse_decimal(row["requested_notional_usd"])
            if requested not in _EXECUTION_NOTIONALS:
                raise _PublicDataError("invalid_requested_notional")
            if row["contract_version"].strip() != "1":
                raise _PublicDataError("schema_mismatch")
            if row["notional_definition"].strip() != _EXECUTION_NOTIONAL_DEFINITION:
                raise _PublicDataError("schema_mismatch")
            keys.append((snapshot_id, market_id, direction, requested))

            status = row["status"].strip().lower()
            if status not in _EXECUTION_STATUSES:
                raise _PublicDataError("invalid_status")
            reason = row["status_reason"].strip()
            if not _is_safe_public_reason(reason):
                raise _PublicDataError("unsafe_public_value")
            status_counts[status] = status_counts.get(status, 0) + 1
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

            parsed_measurements = {}
            for field in _EXECUTION_MEASUREMENT_FIELDS:
                number = _parse_decimal(row[field])
                parsed_measurements[field] = number
                if number is None:
                    measurement_counts[field]["null_count"] += 1
                elif number == 0:
                    measurement_counts[field]["zero_count"] += 1
                if number is not None and number < 0:
                    raise _PublicDataError("invalid_measurement")

            measured = status in {"observed", "partial"}
            if not measured:
                if state_time is not None and state_text is not None:
                    observation_times.append((state_time, state_text))
                continue
            missing_provenance = [
                field
                for field in _EXECUTION_MEASURED_PROVENANCE_FIELDS
                if not row[field].strip()
            ]
            required_null_count += len(missing_provenance)
            if missing_provenance or state_time is None or state_text is None:
                raise _PublicDataError("required_field_null")
            if not _is_sha256(row["raw_response_sha256"].strip()):
                raise _PublicDataError("invalid_source_lineage")

            if market_type == "dex":
                missing_lineage = [
                    field
                    for field in _DEX_EXECUTION_LINEAGE_FIELDS
                    if not row[field].strip()
                ]
                required_null_count += len(missing_lineage)
                if missing_lineage:
                    raise _PublicDataError("required_field_null")
                block_number = row["block_number"].strip()
                if (
                    not block_number.isdigit()
                    or int(block_number) <= 0
                    or row["source_sequence"].strip() != block_number
                    or row["block_timestamp"].strip()
                    != row["state_observed_at"].strip()
                ):
                    raise _PublicDataError("invalid_fixed_block_lineage")
                block_time, _ = _parse_observation_timestamp(row["block_timestamp"])
                usd_price_time, _ = _parse_observation_timestamp(
                    row["usd_price_observed_at"]
                )
                if block_time > generated_at or usd_price_time > generated_at:
                    raise _PublicDataError("future_observation_timestamp")

            observation_times.append((state_time, state_text))
            if all(value is not None for value in parsed_measurements.values()):
                usable_count += 1
    except _PublicDataError as error:
        family["required_field_null"] = {
            "count": required_null_count,
            "rate_bps": _basis_points(
                required_null_count,
                observed_count * len(_EXECUTION_REQUIRED_FIELDS),
            ),
        }
        return _set_failed(family, error.reason)

    if len(snapshots) > 1:
        return _set_failed(family, "mixed_snapshot_id")
    if len(source_snapshots) > 1:
        return _set_failed(family, "mixed_source_snapshot_id")
    duplicate_count = observed_count - len(set(keys))
    family["duplicate_primary_key"] = {
        "count": duplicate_count,
        "rate_bps": _basis_points(duplicate_count, observed_count),
    }
    if duplicate_count:
        return _set_failed(family, "duplicate_primary_key")

    market_count = len(market_identities)
    expected_count = market_count * len(_EXECUTION_DIRECTIONS) * len(
        _EXECUTION_NOTIONALS
    )
    family["required_field_null"] = {
        "count": required_null_count,
        "rate_bps": _basis_points(
            required_null_count,
            observed_count * len(_EXECUTION_REQUIRED_FIELDS),
        ),
    }
    family["measurements"] = {
        "null_count": sum(item["null_count"] for item in measurement_counts.values()),
        "zero_count": sum(item["zero_count"] for item in measurement_counts.values()),
        "fields": measurement_counts,
    }
    minimum = min(observation_times, default=None)
    maximum = max(observation_times, default=None)
    freshness = (
        int((generated_at - maximum[0]).total_seconds())
        if maximum is not None
        else None
    )
    if freshness is not None and freshness > 86400:
        reason_counts["stale_partition"] = 1
    inventory_payload = {
        "domain": f"data_quality_snapshot/v1/{market_type}_execution_inventory",
        "markets": [
            list(market_identities[market_id])
            for market_id in sorted(market_identities)
        ],
    }
    snapshot_id = next(iter(snapshots))
    source_snapshot_id = next(iter(source_snapshots))
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": expected_count,
                "observed": observed_count,
                "usable": usable_count,
                "expected_basis": {
                    "kind": "fixed_execution_scenario_inventory",
                    "snapshot_id_sha256": _opaque_identifier_hash(
                        f"data_quality_snapshot/v1/{market_type}_execution_snapshot",
                        snapshot_id,
                    ),
                    "source_snapshot_id_sha256": _opaque_identifier_hash(
                        f"data_quality_snapshot/v1/{market_type}_execution_source_snapshot",
                        source_snapshot_id,
                    ),
                    "market_count": market_count,
                    "market_inventory_sha256": _snapshot_hash(inventory_payload),
                    "directions": list(_EXECUTION_DIRECTIONS),
                    "notionals_usd": [int(value) for value in _EXECUTION_NOTIONALS],
                },
            },
            "coverage_bps": _basis_points(usable_count, expected_count),
            "status_counts": dict(sorted(status_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
            "observation_time": {
                "min": minimum[1] if minimum is not None else None,
                "max": maximum[1] if maximum is not None else None,
                "freshness_lag_seconds": freshness,
            },
        }
    )
    return family


def _capture_event_record(data_dir: Path, relative_text: str) -> Dict[str, Any]:
    if (
        not relative_text
        or relative_text.startswith("/")
        or "\\" in relative_text
        or Path(relative_text).is_absolute()
    ):
        raise _PublicDataError("unsafe_evidence_path")
    parts = Path(relative_text).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise _PublicDataError("unsafe_evidence_path")
    if Path(*parts).as_posix() != relative_text:
        raise _PublicDataError("unsafe_evidence_path")

    root = data_dir / "evidence" / "events"
    for fixed_directory in (data_dir / "evidence", root):
        try:
            fixed_status = fixed_directory.lstat()
        except OSError:
            raise _PublicDataError("source_capture_failed")
        if stat.S_ISLNK(fixed_status.st_mode) or not stat.S_ISDIR(fixed_status.st_mode):
            raise _PublicDataError("unsafe_source_file")
    parent = root
    for component in parts[:-1]:
        parent = parent / component
        try:
            parent_status = parent.lstat()
        except OSError:
            raise _PublicDataError("source_capture_failed")
        if stat.S_ISLNK(parent_status.st_mode) or not stat.S_ISDIR(parent_status.st_mode):
            raise _PublicDataError("unsafe_source_file")

    path = root.joinpath(*parts)
    try:
        before = path.lstat()
    except OSError:
        raise _PublicDataError("source_capture_failed")
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise _PublicDataError("unsafe_source_file")
    if before.st_size > _MAX_EVENT_RECORD_BYTES:
        raise _PublicDataError("source_file_too_large")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(str(path), flags)
    except OSError:
        raise _PublicDataError("source_capture_failed")
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise _PublicDataError("source_changed_during_capture")
        chunks = []
        total = 0
        while True:
            block = os.read(
                descriptor,
                min(1024 * 1024, _MAX_EVENT_RECORD_BYTES + 1 - total),
            )
            if not block:
                break
            chunks.append(block)
            total += len(block)
            if total > _MAX_EVENT_RECORD_BYTES:
                raise _PublicDataError("source_file_too_large")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or total != opened.st_size
    ):
        raise _PublicDataError("source_changed_during_capture")
    payload = b"".join(chunks)
    opaque_name = _opaque_identifier_hash(
        "data_quality_snapshot/v1/event_source_record", relative_text
    )
    return {
        "payload": payload,
        "identity": _logical_source("event_source_record/" + opaque_name, payload),
    }


def _event_locator(record: Mapping[str, Any], locator: str) -> Mapping[str, Any]:
    parts = locator.split(".")
    if len(parts) < 2 or parts[0] != "facts" or any(not part for part in parts):
        raise _PublicDataError("invalid_record_locator")
    current: Any = record
    for part in parts:
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            raise _PublicDataError("invalid_record_locator")
    if not isinstance(current, Mapping) or not str(current.get("statement", "")).strip():
        raise _PublicDataError("invalid_record_locator")
    return current


def _evaluate_event_facts(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        capture = _capture_candidate(
            data_dir, "event_facts.csv", "curated", _MAX_CSV_BYTES
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if capture is None:
        return family
    family["source"] = {"inputs": [capture["identity"]]}
    try:
        text = capture["payload"].decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        if tuple(reader.fieldnames or ()) != _EVENT_COLUMNS:
            raise _PublicDataError("schema_mismatch")
        rows = list(reader)
    except UnicodeDecodeError:
        return _set_failed(family, "invalid_utf8")
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if not rows:
        return _set_failed(family, "empty_inventory")

    observed_count = len(rows)
    required_null_count = 0
    keys: List[Tuple[str, int]] = []
    revisions: Dict[str, List[Tuple[int, datetime, str]]] = {}
    immutable_identity: Dict[str, Tuple[str, str, str]] = {}
    status_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    observation_times: List[Tuple[datetime, str]] = []
    evidence_inputs: Dict[Tuple[str, str], Dict[str, Any]] = {}
    evidence_path_hashes: Dict[str, str] = {}
    measurement_counts = {
        field: {"null_count": 0, "zero_count": 0}
        for field in _EVENT_MEASUREMENT_FIELDS
    }
    try:
        for row in rows:
            if None in row or any(
                not isinstance(row.get(field), str) for field in _EVENT_COLUMNS
            ):
                raise _PublicDataError("schema_mismatch")
            nulls = [field for field in _EVENT_REQUIRED_FIELDS if not row[field].strip()]
            required_null_count += len(nulls)
            if nulls:
                raise _PublicDataError("required_field_null")

            event_id = row["event_id"].strip()
            revision_text = row["revision"].strip()
            if not revision_text.isdigit() or int(revision_text) <= 0:
                raise _PublicDataError("invalid_revision")
            revision = int(revision_text)
            keys.append((event_id, revision))

            event_type = row["event_type"].strip()
            event_subtype = row["event_subtype"].strip()
            if (
                event_type not in _EVENT_TYPES
                or event_subtype not in _EVENT_SUBTYPES.get(event_type, set())
            ):
                raise _PublicDataError("invalid_event_type")
            lifecycle = row["lifecycle"].strip()
            if lifecycle not in _EVENT_LIFECYCLES:
                raise _PublicDataError("invalid_status")
            evidence_status = row["evidence_status"].strip()
            if evidence_status not in _EVENT_EVIDENCE_STATUSES:
                raise _PublicDataError("invalid_evidence_status")
            if row["source_kind"].strip() not in _EVENT_SOURCE_KINDS:
                raise _PublicDataError("invalid_source_kind")
            source_url = row["source_url"].strip()
            parsed_source_url = urlsplit(source_url)
            if (
                parsed_source_url.scheme != "https"
                or not parsed_source_url.netloc
                or parsed_source_url.username is not None
                or parsed_source_url.password is not None
            ):
                raise _PublicDataError("invalid_source_url")

            checked_at, checked_text = _parse_observation_timestamp(
                row["source_checked_at_utc"]
            )
            recorded_at, recorded_text = _parse_observation_timestamp(
                row["recorded_at_utc"]
            )
            if checked_at > generated_at or recorded_at > generated_at:
                raise _PublicDataError("future_observation_timestamp")
            if recorded_at < checked_at:
                raise _PublicDataError("invalid_observation_timestamp")
            observation_times.append((recorded_at, recorded_text))

            record_capture = _capture_event_record(
                data_dir, row["source_record_file"].strip()
            )
            record_identity = record_capture["identity"]
            previous_record_hash = evidence_path_hashes.setdefault(
                record_identity["logical_path"], record_identity["sha256"]
            )
            if previous_record_hash != record_identity["sha256"]:
                raise _PublicDataError("source_changed_during_capture")
            evidence_key = (
                record_identity["logical_path"],
                record_identity["sha256"],
            )
            evidence_inputs[evidence_key] = record_identity
            try:
                record = json.loads(record_capture["payload"].decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise _PublicDataError("invalid_evidence_record")
            if not isinstance(record, Mapping) or record.get("record_schema") != "source_check/v1":
                raise _PublicDataError("invalid_evidence_record")
            if (
                record.get("source_url") != row["source_url"].strip()
                or record.get("checked_at_utc") != row["source_checked_at_utc"].strip()
            ):
                raise _PublicDataError("evidence_binding_mismatch")
            record_checked, _ = _parse_observation_timestamp(record["checked_at_utc"])
            if record_checked > generated_at:
                raise _PublicDataError("future_observation_timestamp")
            located = _event_locator(record, row["record_locator"].strip())
            supported_lifecycle = located.get("supported_lifecycle")
            if supported_lifecycle is not None and supported_lifecycle != lifecycle:
                raise _PublicDataError("evidence_binding_mismatch")

            identity = (
                row["token_symbol"].strip().upper(),
                event_type,
                event_subtype,
            )
            previous_identity = immutable_identity.setdefault(event_id, identity)
            if previous_identity != identity:
                raise _PublicDataError("revision_identity_conflict")
            revisions.setdefault(event_id, []).append(
                (revision, recorded_at, recorded_text)
            )
            status_counts[lifecycle] = status_counts.get(lifecycle, 0) + 1
            reason_counts[evidence_status] = reason_counts.get(evidence_status, 0) + 1

            for field in _EVENT_MEASUREMENT_FIELDS:
                value = _parse_decimal(row[field])
                if value is None:
                    measurement_counts[field]["null_count"] += 1
                elif value == 0:
                    measurement_counts[field]["zero_count"] += 1
                elif value < 0:
                    raise _PublicDataError("invalid_measurement")
    except _PublicDataError as error:
        family["required_field_null"] = {
            "count": required_null_count,
            "rate_bps": _basis_points(
                required_null_count, observed_count * len(_EVENT_REQUIRED_FIELDS)
            ),
        }
        return _set_failed(family, error.reason)

    duplicate_count = observed_count - len(set(keys))
    family["duplicate_primary_key"] = {
        "count": duplicate_count,
        "rate_bps": _basis_points(duplicate_count, observed_count),
    }
    if duplicate_count:
        return _set_failed(family, "duplicate_primary_key")
    for history in revisions.values():
        ordered = sorted(history)
        if [item[0] for item in ordered] != list(range(1, len(ordered) + 1)):
            return _set_failed(family, "noncontiguous_revision")
        if any(
            ordered[index][1] <= ordered[index - 1][1]
            for index in range(1, len(ordered))
        ):
            return _set_failed(family, "nonmonotonic_revision_time")

    family["source"]["inputs"].extend(evidence_inputs.values())
    family["source"]["inputs"].sort(key=lambda item: item["logical_path"])
    family["required_field_null"] = {
        "count": required_null_count,
        "rate_bps": _basis_points(
            required_null_count, observed_count * len(_EVENT_REQUIRED_FIELDS)
        ),
    }
    family["measurements"] = {
        "null_count": sum(item["null_count"] for item in measurement_counts.values()),
        "zero_count": sum(item["zero_count"] for item in measurement_counts.values()),
        "fields": measurement_counts,
    }
    minimum = min(observation_times)
    maximum = max(observation_times)
    freshness = int((generated_at - maximum[0]).total_seconds())
    if freshness > 86400:
        reason_counts["stale_partition"] = 1
    usable_count = len(revisions)
    inventory_payload = {
        "domain": "data_quality_snapshot/v1/event_revision_inventory",
        "revision_counts": sorted(len(history) for history in revisions.values()),
        "evidence_sha256": sorted(
            item["sha256"] for item in evidence_inputs.values()
        ),
    }
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": observed_count,
                "observed": observed_count,
                "usable": usable_count,
                "expected_basis": {
                    "kind": "curated_revision_inventory",
                    "revision_count": observed_count,
                    "event_count": usable_count,
                    "evidence_record_count": len(evidence_inputs),
                    "inventory_sha256": _snapshot_hash(inventory_payload),
                },
            },
            "coverage_bps": _basis_points(usable_count, observed_count),
            "status_counts": dict(sorted(status_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
            "observation_time": {
                "min": minimum[1],
                "max": maximum[1],
                "freshness_lag_seconds": freshness,
            },
        }
    )
    return family


def _evaluate_cex_lifecycle(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        capture = _capture_candidate(
            data_dir,
            "cex_instrument_lifecycle.json",
            "curated",
            256 * 1024,
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if capture is None:
        return family
    family["source"] = {"inputs": [capture["identity"]]}
    try:
        payload = json.loads(capture["payload"].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _set_failed(family, "invalid_json")
    try:
        if not isinstance(payload, dict) or set(payload) != _CEX_LIFECYCLE_ROOT_FIELDS:
            raise _PublicDataError("schema_mismatch")
        if payload["schema"] != "cex_instrument_lifecycle/v1":
            raise _PublicDataError("schema_mismatch")
        root_checked, root_checked_text = _parse_observation_timestamp(
            payload["checked_at_utc"]
        )
        root_generated, _ = _parse_observation_timestamp(payload["generated_at_utc"])
        if root_checked > generated_at or root_generated > generated_at:
            raise _PublicDataError("future_observation_timestamp")
        if payload["checked_at_utc"] != payload["generated_at_utc"]:
            raise _PublicDataError("inconsistent_root_timestamp")
        response_hash = payload["response_sha256"]
        configured_hash = payload["configured_market_ids_sha256"]
        if not _is_sha256(response_hash) or not _is_sha256(configured_hash):
            raise _PublicDataError("invalid_source_lineage")
        inventory_count = payload["inventory_count"]
        configured_count = payload["configured_market_count"]
        review_count = payload["review_count"]
        reviews = payload["reviews"]
        if (
            isinstance(inventory_count, bool)
            or not isinstance(inventory_count, int)
            or inventory_count <= 0
            or isinstance(configured_count, bool)
            or not isinstance(configured_count, int)
            or configured_count <= 0
            or configured_count > 1000
            or isinstance(review_count, bool)
            or not isinstance(review_count, int)
            or not isinstance(reviews, list)
            or review_count != len(reviews)
            or review_count > configured_count
            or review_count > 1000
        ):
            raise _PublicDataError("count_mismatch")

        keys = []
        for review in reviews:
            if not isinstance(review, dict) or set(review) != _CEX_LIFECYCLE_REVIEW_FIELDS:
                raise _PublicDataError("schema_mismatch")
            if review["current_listing_status"] != _CEX_LIFECYCLE_STATUS:
                raise _PublicDataError("invalid_status")
            if review["reason_code"] != _CEX_LIFECYCLE_REASON:
                raise _PublicDataError("status_reason_conflict")
            review_checked, _ = _parse_observation_timestamp(review["checked_at_utc"])
            if review_checked > generated_at:
                raise _PublicDataError("future_observation_timestamp")
            if (
                review["checked_at_utc"] != payload["checked_at_utc"]
                or review["response_sha256"] != response_hash
                or review["inventory_count"] != inventory_count
            ):
                if not _is_sha256(str(review["response_sha256"])):
                    raise _PublicDataError("invalid_source_lineage")
                raise _PublicDataError("root_review_mismatch")
            if not _is_sha256(review["response_sha256"]):
                raise _PublicDataError("invalid_source_lineage")
            token = review["token_symbol"]
            exchange = review["exchange"]
            instrument = review["instrument"]
            if (
                review["market_type"] != "cex"
                or not isinstance(token, str)
                or token != token.upper()
                or not isinstance(exchange, str)
                or not re.fullmatch(r"[a-z0-9_]{2,32}", exchange)
                or not isinstance(instrument, str)
                or instrument.split("/")[0] != token
                or review["market_id"] != f"cex:{exchange}:{instrument}"
            ):
                raise _PublicDataError("invalid_market_identity")
            if (
                exchange != "crypto_com"
                or review["source_url"]
                != "https://api.crypto.com/exchange/v1/public/get-instruments"
                or review["http_status"] != 200
                or review["instrument_present"] is not False
            ):
                raise _PublicDataError("invalid_source_lineage")
            keys.append(review["market_id"])
    except (KeyError, TypeError, ValueError):
        return _set_failed(family, "schema_mismatch")
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    duplicate_count = len(keys) - len(set(keys))
    family["duplicate_primary_key"] = {
        "count": duplicate_count,
        "rate_bps": _basis_points(duplicate_count, len(keys)),
    }
    if duplicate_count:
        return _set_failed(family, "duplicate_primary_key")
    freshness = int((generated_at - root_checked).total_seconds())
    reasons = {_CEX_LIFECYCLE_REASON: len(reviews)} if reviews else {}
    if freshness > 86400:
        reasons["stale_partition"] = 1
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": len(reviews),
                "observed": len(reviews),
                "usable": len(reviews),
                "expected_basis": {
                    "kind": "configured_market_inventory_context",
                    "configured_market_count": configured_count,
                    "configured_market_ids_sha256": configured_hash,
                    "catalog_inventory_count": inventory_count,
                },
            },
            "coverage_bps": _basis_points(len(reviews), len(reviews)),
            "duplicate_primary_key": {
                "count": 0,
                "rate_bps": _basis_points(0, len(reviews)),
            },
            "required_field_null": {
                "count": 0,
                "rate_bps": _basis_points(
                    0, len(reviews) * len(_CEX_LIFECYCLE_REVIEW_FIELDS)
                ),
            },
            "status_counts": (
                {_CEX_LIFECYCLE_STATUS: len(reviews)} if reviews else {}
            ),
            "reason_counts": dict(sorted(reasons.items())),
            "observation_time": {
                "min": root_checked_text,
                "max": root_checked_text,
                "freshness_lag_seconds": freshness,
            },
        }
    )
    return family


def _market_lifecycle_timestamp(value: Any) -> Tuple[datetime, str]:
    parsed, canonical = _parse_observation_timestamp(value)
    if value != canonical or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value
    ):
        raise _PublicDataError("invalid_observation_timestamp")
    return parsed, canonical


def _evaluate_market_lifecycle(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        capture = _capture_candidate(
            data_dir,
            "market_lifecycle_reviews.json",
            "curated",
            512 * 1024,
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if capture is None:
        return family
    family["source"] = {"inputs": [capture["identity"]]}
    try:
        payload = json.loads(capture["payload"].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _set_failed(family, "invalid_json")

    try:
        if not isinstance(payload, dict) or set(payload) != _MARKET_LIFECYCLE_ROOT_FIELDS:
            raise _PublicDataError("schema_mismatch")
        if payload["schema"] != "market_lifecycle_reviews/v1":
            raise _PublicDataError("schema_mismatch")
        manifest_time, _ = _market_lifecycle_timestamp(payload["generated_at_utc"])
        if manifest_time > generated_at:
            raise _PublicDataError("future_observation_timestamp")
        reviews = payload["reviews"]
        declared_count = payload["review_count"]
        if (
            not isinstance(reviews, list)
            or isinstance(declared_count, bool)
            or not isinstance(declared_count, int)
            or declared_count != len(reviews)
            or declared_count > 1000
        ):
            raise _PublicDataError("count_mismatch")
        if not reviews:
            raise _PublicDataError("empty_inventory")

        keys = []
        histories: Dict[str, List[Dict[str, Any]]] = {}
        issue_lineages: Dict[str, str] = {}
        status_counts: Dict[str, int] = {}
        reason_counts: Dict[str, int] = {}
        reviewed_times: List[Tuple[datetime, str]] = []
        for review in reviews:
            if (
                not isinstance(review, dict)
                or set(review) != _MARKET_LIFECYCLE_REVISION_FIELDS
            ):
                raise _PublicDataError("schema_mismatch")
            review_id = review["review_id"]
            revision = review["revision"]
            if (
                not isinstance(review_id, str)
                or not re.fullmatch(r"[a-z0-9][a-z0-9-]{2,95}", review_id)
                or isinstance(revision, bool)
                or not isinstance(revision, int)
                or revision < 1
            ):
                raise _PublicDataError("invalid_revision")
            expected_supersedes = None if revision == 1 else revision - 1
            if review["supersedes_revision"] != expected_supersedes:
                raise _PublicDataError("noncontiguous_revision")
            keys.append((review_id, revision))

            status = review["review_status"]
            if status not in {"disposed", "withdrawn"}:
                raise _PublicDataError("invalid_status")
            disposed_contract = (
                review["disposition_status"] == "source_no_observation"
                and review["disposition_reason_code"] == "no_candles"
                and review["market_lifecycle"]
                in {"pool_exists_dormant", "listed_quote_market_dormant"}
            )
            withdrawn_contract = all(
                review[field] is None
                for field in (
                    "disposition_status",
                    "disposition_reason_code",
                    "market_lifecycle",
                )
            )
            if (status == "disposed" and not disposed_contract) or (
                status == "withdrawn" and not withdrawn_contract
            ):
                raise _PublicDataError("invalid_disposition")
            status_counts[status] = status_counts.get(status, 0) + 1
            reason = "withdrawn" if status == "withdrawn" else "no_candles"
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

            issue_id = review["reviewed_issue_id"]
            if not isinstance(issue_id, str) or not re.fullmatch(r"[0-9a-f]{20}", issue_id):
                raise _PublicDataError("invalid_issue_identity")
            if (
                review["original_category"] != "stale_market_unknown"
                or review["original_reason_code"] != "stale_market_lifecycle_unknown"
            ):
                raise _PublicDataError("invalid_disposition")
            existing_lineage = issue_lineages.setdefault(issue_id, review_id)
            if existing_lineage != review_id:
                raise _PublicDataError("forked_review_lineage")

            token = review["token_symbol"]
            market_type = review["market_type"]
            market_id = review["market_id"]
            if (
                market_type not in {"cex", "dex"}
                or not isinstance(token, str)
                or not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{0,31}", token)
                or not isinstance(market_id, str)
                or not market_id.startswith(market_type + ":")
            ):
                raise _PublicDataError("invalid_market_identity")
            if market_type == "cex":
                parts = market_id.split(":")
                if (
                    len(parts) != 3
                    or parts[1] != "upbit"
                    or parts[2].split("/")[0] != token
                    or review["market_lifecycle"]
                    not in {None, "listed_quote_market_dormant"}
                    or review["evidence_status"] != "primary_confirmed"
                    or review["review_method"]
                    != "manual_primary_source_cross_check"
                ):
                    raise _PublicDataError("invalid_market_identity")
                allowed_kinds = {
                    "official_exchange_ticker",
                    "official_exchange_market_inventory",
                }
                required_kinds = {"official_exchange_ticker"}
                expected_host = "api.upbit.com"
            else:
                parts = market_id.split(":")
                if (
                    len(parts) != 5
                    or parts[4] != token
                    or review["market_lifecycle"] not in {None, "pool_exists_dormant"}
                    or review["evidence_status"] != "declared_source_confirmed"
                    or review["review_method"]
                    != "manual_declared_source_cross_check"
                ):
                    raise _PublicDataError("invalid_market_identity")
                allowed_kinds = {
                    "declared_dex_market_data_api",
                    "declared_dex_daily_ohlcv_api",
                }
                required_kinds = set(allowed_kinds)
                expected_host = "api.geckoterminal.com"

            try:
                issue_day = _parse_canonical_day(review["issue_date"])
            except _PublicDataError:
                raise _PublicDataError("invalid_issue_date")
            reviewed_at, reviewed_text = _market_lifecycle_timestamp(
                review["reviewed_at_utc"]
            )
            if reviewed_at > generated_at or reviewed_at > manifest_time:
                raise _PublicDataError("future_observation_timestamp")
            if issue_day > reviewed_at.date():
                raise _PublicDataError("invalid_issue_date")
            reviewed_times.append((reviewed_at, reviewed_text))

            checks = review["source_checks"]
            if not isinstance(checks, list) or not checks or len(checks) > 8:
                raise _PublicDataError("invalid_source_inventory")
            seen_kinds = set()
            seen_urls = set()
            for check in checks:
                if (
                    not isinstance(check, dict)
                    or set(check) != _MARKET_LIFECYCLE_SOURCE_FIELDS
                ):
                    raise _PublicDataError("schema_mismatch")
                kind = check["source_kind"]
                if kind not in allowed_kinds or kind in seen_kinds:
                    raise _PublicDataError("invalid_source_inventory")
                seen_kinds.add(kind)
                url = check["url"]
                if not isinstance(url, str) or url in seen_urls:
                    raise _PublicDataError("invalid_source_inventory")
                seen_urls.add(url)
                try:
                    parsed_url = urlsplit(url)
                    port = parsed_url.port
                except ValueError:
                    raise _PublicDataError("invalid_source_lineage")
                if (
                    parsed_url.scheme != "https"
                    or parsed_url.hostname != expected_host
                    or parsed_url.username is not None
                    or parsed_url.password is not None
                    or port is not None
                    or parsed_url.fragment
                ):
                    raise _PublicDataError("invalid_source_lineage")
                http_status = check["http_status"]
                if (
                    isinstance(http_status, bool)
                    or not isinstance(http_status, int)
                    or not 200 <= http_status <= 299
                ):
                    raise _PublicDataError("invalid_source_status")
                if not _is_sha256(check["response_sha256"]):
                    raise _PublicDataError("invalid_source_lineage")
                checked_at, _ = _market_lifecycle_timestamp(check["checked_at_utc"])
                if checked_at > generated_at:
                    raise _PublicDataError("future_observation_timestamp")
                if checked_at > reviewed_at or checked_at.date() <= issue_day:
                    raise _PublicDataError("invalid_source_clock")
                observations = check["observations"]
                if not isinstance(observations, dict) or not observations:
                    raise _PublicDataError("invalid_source_lineage")
                try:
                    observations_bytes = json.dumps(
                        observations,
                        ensure_ascii=False,
                        allow_nan=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                except (TypeError, ValueError):
                    raise _PublicDataError("invalid_source_lineage")
                if len(observations_bytes) > 16 * 1024:
                    raise _PublicDataError("invalid_source_lineage")
            if not required_kinds <= seen_kinds:
                raise _PublicDataError("invalid_source_inventory")

            histories.setdefault(review_id, []).append(
                {
                    "revision": revision,
                    "reviewed_at": reviewed_at,
                    "identity": (
                        issue_id,
                        review["original_category"],
                        review["original_reason_code"],
                        market_id,
                        market_type,
                        token,
                        review["issue_date"],
                    ),
                    "status": status,
                }
            )
    except (KeyError, TypeError, ValueError):
        return _set_failed(family, "schema_mismatch")
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    duplicate_count = len(keys) - len(set(keys))
    family["duplicate_primary_key"] = {
        "count": duplicate_count,
        "rate_bps": _basis_points(duplicate_count, len(keys)),
    }
    if duplicate_count:
        return _set_failed(family, "duplicate_primary_key")
    active_count = 0
    for history in histories.values():
        ordered = sorted(history, key=lambda item: item["revision"])
        if [item["revision"] for item in ordered] != list(
            range(1, len(ordered) + 1)
        ):
            return _set_failed(family, "noncontiguous_revision")
        if any(item["identity"] != ordered[0]["identity"] for item in ordered):
            return _set_failed(family, "revision_identity_conflict")
        if any(
            ordered[index]["reviewed_at"] <= ordered[index - 1]["reviewed_at"]
            for index in range(1, len(ordered))
        ):
            return _set_failed(family, "nonmonotonic_revision_time")
        if ordered[-1]["status"] == "disposed":
            active_count += 1

    minimum = min(reviewed_times)
    maximum = max(reviewed_times)
    freshness = int((generated_at - maximum[0]).total_seconds())
    if freshness > 86400:
        reason_counts["stale_partition"] = 1
    usable_count = len(histories)
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": declared_count,
                "observed": declared_count,
                "usable": usable_count,
                "expected_basis": {
                    "kind": "declared_revision_inventory",
                    "revision_count": declared_count,
                    "review_id_count": usable_count,
                    "active_disposition_count": active_count,
                },
            },
            "coverage_bps": _basis_points(usable_count, declared_count),
            "required_field_null": {
                "count": 0,
                "rate_bps": _basis_points(
                    0, declared_count * len(_MARKET_LIFECYCLE_REVISION_FIELDS)
                ),
            },
            "status_counts": dict(sorted(status_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
            "observation_time": {
                "min": minimum[1],
                "max": maximum[1],
                "freshness_lag_seconds": freshness,
            },
        }
    )
    return family


def _route_pointer_capture(
    data_dir: Path,
    *,
    shadow: bool,
) -> Optional[Dict[str, Any]]:
    suffix = ("routes", "shadow", "latest.json") if shadow else (
        "routes",
        "latest.json",
    )
    candidates = (
        (suffix, "/".join(suffix)),
        (("local",) + suffix, "/".join(("local",) + suffix)),
    )
    present = [
        (components, logical_path)
        for components, logical_path in candidates
        if os.path.lexists(str(data_dir.joinpath(*components)))
    ]
    if len(present) > 1:
        raise _PublicDataError("ambiguous_source_candidates")
    if not present:
        return None
    components, logical_path = present[0]
    return _capture_fixed_tree_file(
        data_dir,
        components,
        logical_path=logical_path,
        byte_limit=1024 * 1024,
    )


def _capture_fixed_tree_file(
    root: Path,
    components: Sequence[str],
    *,
    logical_path: str,
    byte_limit: int,
) -> Dict[str, Any]:
    if not components or any(
        not isinstance(component, str)
        or not component
        or component in {".", ".."}
        or "/" in component
        or "\\" in component
        for component in components
    ):
        raise _PublicDataError("unsafe_route_reference")
    directory = Path(root)
    try:
        root_status = directory.lstat()
    except OSError:
        raise _PublicDataError("route_sidecar_missing")
    if stat.S_ISLNK(root_status.st_mode) or not stat.S_ISDIR(root_status.st_mode):
        raise _PublicDataError("unsafe_source_file")
    for component in components[:-1]:
        directory = directory / component
        try:
            directory_status = directory.lstat()
        except OSError:
            raise _PublicDataError("route_sidecar_missing")
        if stat.S_ISLNK(directory_status.st_mode) or not stat.S_ISDIR(
            directory_status.st_mode
        ):
            raise _PublicDataError("unsafe_source_file")
    path = directory / components[-1]
    try:
        before = path.lstat()
    except OSError:
        raise _PublicDataError("route_sidecar_missing")
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise _PublicDataError("unsafe_source_file")
    if before.st_size > byte_limit:
        raise _PublicDataError("source_file_too_large")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(str(path), flags)
    except OSError:
        raise _PublicDataError("source_capture_failed")
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise _PublicDataError("source_changed_during_capture")
        chunks = []
        total = 0
        while True:
            block = os.read(descriptor, min(1024 * 1024, byte_limit + 1 - total))
            if not block:
                break
            chunks.append(block)
            total += len(block)
            if total > byte_limit:
                raise _PublicDataError("source_file_too_large")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or total != opened.st_size
    ):
        raise _PublicDataError("source_changed_during_capture")
    payload = b"".join(chunks)
    return {"payload": payload, "identity": _logical_source(logical_path, payload)}


def _validated_route_pointer(payload: bytes, *, shadow: bool) -> Dict[str, Any]:
    try:
        pointer = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _PublicDataError("invalid_route_pointer")
    if not isinstance(pointer, dict):
        raise _PublicDataError("invalid_route_pointer")
    expected_fields = (
        _ROUTE_SHADOW_POINTER_FIELDS
        if shadow
        else _ROUTE_OPPORTUNITY_POINTER_FIELDS
    )
    if set(pointer) != expected_fields:
        raise _PublicDataError("invalid_route_pointer")
    cohort_id = pointer.get("route_cohort_id")
    if (
        not isinstance(cohort_id, str)
        or _ROUTE_COHORT_ID_RE.fullmatch(cohort_id) is None
    ):
        raise _PublicDataError("unsafe_route_pointer")
    if shadow:
        run_id = pointer.get("run_id")
        if (
            not isinstance(run_id, str)
            or run_id in {".", ".."}
            or _ROUTE_RUN_ID_RE.fullmatch(run_id) is None
        ):
            raise _PublicDataError("unsafe_route_pointer")
        if (
            pointer.get("schema") != "route_shadow_pointer/v1"
            or pointer.get("phase") not in {"canary", "full"}
        ):
            raise _PublicDataError("invalid_route_pointer")
        hash_fields = expected_fields - {
            "schema",
            "run_id",
            "phase",
            "route_cohort_id",
            "phase_transition_id",
        }
        transition_id = pointer.get("phase_transition_id")
        if pointer["phase"] == "canary":
            if transition_id is not None:
                raise _PublicDataError("invalid_route_pointer")
        elif (
            not isinstance(transition_id, str)
            or _ROUTE_SHA256_RE.fullmatch(transition_id) is None
        ):
            raise _PublicDataError("invalid_route_pointer")
    else:
        if (
            pointer.get("schema") != "route_opportunity_pointer/v1"
            or pointer.get("bundle_stage") != "route_opportunity/v1"
        ):
            raise _PublicDataError("invalid_route_pointer")
        hash_fields = expected_fields - {
            "schema",
            "bundle_stage",
            "route_cohort_id",
        }
    if any(
        not isinstance(pointer.get(field), str)
        or _ROUTE_SHA256_RE.fullmatch(pointer[field]) is None
        for field in hash_fields
    ):
        raise _PublicDataError("invalid_route_pointer")
    return pointer


def _evaluate_route_opportunity(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        pointer_capture = _route_pointer_capture(data_dir, shadow=False)
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if pointer_capture is None:
        return family
    family["source"] = {"inputs": [pointer_capture["identity"]]}
    try:
        pointer = _validated_route_pointer(
            pointer_capture["payload"], shadow=False
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    routes_root = data_dir / Path(pointer_capture["identity"]["logical_path"]).parent
    try:
        from scripts.route_publication import (
            RoutePublicationError,
            load_latest_complete_route_bundle,
        )

        loaded = load_latest_complete_route_bundle(routes_root)
    except (ImportError, ModuleNotFoundError):
        return _set_failed(family, "route_validator_unavailable")
    except (RoutePublicationError, OSError, TypeError, ValueError):
        return _set_failed(family, "route_bundle_invalid")
    if loaded.get("pointer") != pointer:
        return _set_failed(family, "route_pointer_changed")

    cohort_id = pointer["route_cohort_id"]
    fixed_members = (
        ("manifest.json", 16 * 1024 * 1024),
        ("route_legs.csv", 128 * 1024 * 1024),
        ("cost_components.csv", 128 * 1024 * 1024),
        ("route_opportunities.csv", 128 * 1024 * 1024),
        ("route_cohort.sqlite3", 512 * 1024 * 1024),
    )
    captures: Dict[str, Dict[str, Any]] = {}
    try:
        for filename, byte_limit in fixed_members:
            captures[filename] = _capture_fixed_tree_file(
                routes_root,
                ("bundles", cohort_id, filename),
                logical_path="route_opportunity_bundle/" + filename,
                byte_limit=byte_limit,
            )
        pointer_after = _route_pointer_capture(data_dir, shadow=False)
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if (
        pointer_after is None
        or pointer_after["payload"] != pointer_capture["payload"]
        or pointer_after["identity"] != pointer_capture["identity"]
    ):
        return _set_failed(family, "route_pointer_changed")

    manifest = loaded["manifest"]
    declared_files = manifest.get("files", {})
    if (
        captures["manifest.json"]["identity"]["sha256"]
        != pointer["manifest_sha256"]
        or set(declared_files)
        != {filename for filename, _limit in fixed_members if filename != "manifest.json"}
        or any(
            captures[filename]["identity"]["sha256"]
            != declared_files[filename].get("sha256")
            for filename in declared_files
        )
    ):
        return _set_failed(family, "route_artifact_hash_mismatch")
    family["source"]["inputs"].extend(
        captures[filename]["identity"] for filename, _limit in fixed_members
    )
    family["source"]["inputs"].sort(key=lambda item: item["logical_path"])

    counts = manifest["counts"]
    opportunities = loaded["opportunities"]
    routes = loaded["bundle"]["routes"]
    legs = loaded["legs"]
    cost_components = loaded["cost_components"]
    if (
        counts["opportunities"] != len(opportunities)
        or counts["routes"] != len(routes)
        or counts["markets"] != len(legs)
        or counts["legs"] != len(legs)
        or counts["cost_components"] != len(cost_components)
    ):
        return _set_failed(family, "route_declared_count_mismatch")

    status_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    usable_opportunity_ids = set()
    route_ids = set()
    observation_times: List[Tuple[datetime, str]] = []
    try:
        for row in opportunities:
            status = str(row["opportunity_class"])
            reason = str(row["primary_reason"])
            status_counts[status] = status_counts.get(status, 0) + 1
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            route_ids.add(row["route_id"])
            if status != "unavailable":
                usable_opportunity_ids.add(row["opportunity_id"])
            for field in ("buy_state_observed_at", "sell_state_observed_at"):
                observed_at, observed_text = _parse_observation_timestamp(row[field])
                if observed_at > generated_at:
                    raise _PublicDataError("future_observation_timestamp")
                observation_times.append((observed_at, observed_text))
    except (KeyError, TypeError, ValueError):
        return _set_failed(family, "route_bundle_invalid")
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if not observation_times:
        return _set_failed(family, "route_observation_bounds_missing")
    minimum = min(observation_times)
    maximum = max(observation_times)
    freshness = int((generated_at - maximum[0]).total_seconds())
    if freshness > 86400:
        reason_counts["stale_partition"] = 1

    leg_status_counts: Dict[str, int] = {}
    leg_reason_counts: Dict[str, int] = {}
    usable_legs = 0
    for leg in legs:
        status = str(leg["status"])
        reason = str(leg["reason_code"])
        leg_status_counts[status] = leg_status_counts.get(status, 0) + 1
        leg_reason_counts[reason] = leg_reason_counts.get(reason, 0) + 1
        if status in {"observed", "partial"} and leg.get("available") is not False:
            usable_legs += 1
    component_status_counts: Dict[str, int] = {}
    for component in cost_components:
        status = str(component["value_status"])
        component_status_counts[status] = component_status_counts.get(status, 0) + 1
    usable_routes = len(
        {
            row["route_id"]
            for row in opportunities
            if row["opportunity_id"] in usable_opportunity_ids
        }
    )
    observed_count = len(opportunities)
    usable_count = len(usable_opportunity_ids)
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": counts["opportunities"],
                "observed": observed_count,
                "usable": usable_count,
                "expected_basis": {
                    "kind": "sealed_route_opportunity_manifest",
                    "manifest_sha256": pointer["manifest_sha256"],
                    "core_manifest_sha256": pointer["core_manifest_sha256"],
                    "core_pointer_sha256": pointer["core_pointer_sha256"],
                    "route_cohort_identity_sha256": _opaque_identifier_hash(
                        "data_quality_snapshot/v1/route_cohort", cohort_id
                    ),
                },
            },
            "coverage_bps": _basis_points(usable_count, counts["opportunities"]),
            "duplicate_primary_key": {
                "count": 0,
                "rate_bps": _basis_points(0, observed_count),
            },
            "required_field_null": {
                "count": 0,
                "rate_bps": _basis_points(0, observed_count),
            },
            "status_counts": dict(sorted(status_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
            "observation_time": {
                "min": minimum[1],
                "max": maximum[1],
                "freshness_lag_seconds": freshness,
            },
            "entities": {
                "opportunities": {
                    "expected": counts["opportunities"],
                    "observed": observed_count,
                    "usable": usable_count,
                },
                "routes": {
                    "expected": counts["routes"],
                    "observed": len(routes),
                    "usable": usable_routes,
                },
                "markets": {
                    "expected": counts["markets"],
                    "observed": len(legs),
                    "usable": usable_legs,
                },
                "legs": {
                    "expected": counts["legs"],
                    "observed": len(legs),
                    "usable": usable_legs,
                    "status_counts": dict(sorted(leg_status_counts.items())),
                    "reason_counts": dict(sorted(leg_reason_counts.items())),
                },
                "cost_components": {
                    "expected": counts["cost_components"],
                    "observed": len(cost_components),
                    "usable": len(cost_components),
                    "status_counts": dict(
                        sorted(component_status_counts.items())
                    ),
                },
            },
        }
    )
    return family


def _evaluate_route_shadow(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        pointer_capture = _route_pointer_capture(data_dir, shadow=True)
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if pointer_capture is None:
        return family
    family["source"] = {"inputs": [pointer_capture["identity"]]}
    try:
        pointer = _validated_route_pointer(pointer_capture["payload"], shadow=True)
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    shadow_root = data_dir / Path(pointer_capture["identity"]["logical_path"]).parent
    routes_root = shadow_root.parent
    core_root = routes_root / "core"
    try:
        initial_cost_capture = _capture_fixed_tree_file(
            shadow_root,
            ("runs", pointer["run_id"], "route-cost-evidence.json"),
            logical_path="route_shadow_bundle/route-cost-evidence.json",
            byte_limit=32 * 1024 * 1024,
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    try:
        from scripts.route_publication import (
            RoutePublicationError,
            _read_shadow_run_evidence,
            load_latest_shadow_result,
        )

        loaded = load_latest_shadow_result(shadow_root)
        evidence = _read_shadow_run_evidence(shadow_root, pointer["run_id"])
    except (ImportError, ModuleNotFoundError):
        return _set_failed(family, "route_validator_unavailable")
    except (RoutePublicationError, OSError, TypeError, ValueError):
        return _set_failed(family, "route_bundle_invalid")
    if loaded.get("pointer") != pointer:
        return _set_failed(family, "route_pointer_changed")

    run_members = (
        ("route_universe.json", 16 * 1024 * 1024),
        ("baseline_manifest.json", 16 * 1024 * 1024),
        ("route-cost-evidence.json", 32 * 1024 * 1024),
        ("audit.json", 16 * 1024 * 1024),
    )
    core_members = (
        ("manifest.json", 16 * 1024 * 1024),
        ("route_candidates.csv", 128 * 1024 * 1024),
        ("route_legs.csv", 128 * 1024 * 1024),
        ("route_timing.csv", 128 * 1024 * 1024),
        ("route_cohort.sqlite3", 512 * 1024 * 1024),
    )
    run_captures: Dict[str, Dict[str, Any]] = {}
    core_captures: Dict[str, Dict[str, Any]] = {}
    try:
        for filename, byte_limit in run_members:
            run_captures[filename] = _capture_fixed_tree_file(
                shadow_root,
                ("runs", pointer["run_id"], filename),
                logical_path="route_shadow_bundle/" + filename,
                byte_limit=byte_limit,
            )
        for filename, byte_limit in core_members:
            core_captures[filename] = _capture_fixed_tree_file(
                core_root,
                ("bundles", pointer["route_cohort_id"], filename),
                logical_path="route_shadow_core/" + filename,
                byte_limit=byte_limit,
            )
        pointer_after = _route_pointer_capture(data_dir, shadow=True)
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if (
        pointer_after is None
        or pointer_after["payload"] != pointer_capture["payload"]
        or pointer_after["identity"] != pointer_capture["identity"]
    ):
        return _set_failed(family, "route_pointer_changed")

    evidence_bytes = {
        "route_universe.json": evidence["universe_bytes"],
        "baseline_manifest.json": evidence["baseline_bytes"],
        "route-cost-evidence.json": evidence["cost_evidence_bytes"],
        "audit.json": evidence["audit_bytes"],
    }
    if any(
        run_captures[filename]["payload"] != evidence_bytes[filename]
        for filename, _limit in run_members
    ):
        return _set_failed(family, "route_artifact_changed")
    if initial_cost_capture != run_captures["route-cost-evidence.json"]:
        return _set_failed(family, "route_artifact_changed")
    run_expected_hashes = {
        "route_universe.json": pointer["route_universe_sha256"],
        "baseline_manifest.json": pointer["baseline_manifest_sha256"],
        "route-cost-evidence.json": pointer["route_cost_evidence_sha256"],
        "audit.json": pointer["audit_sha256"],
    }
    if any(
        run_captures[filename]["identity"]["sha256"] != expected_hash
        for filename, expected_hash in run_expected_hashes.items()
    ):
        return _set_failed(family, "route_artifact_hash_mismatch")

    manifest = loaded["manifest"]
    declared_files = manifest.get("files", {})
    if (
        core_captures["manifest.json"]["identity"]["sha256"]
        != pointer["core_manifest_sha256"]
        or set(declared_files)
        != {filename for filename, _limit in core_members if filename != "manifest.json"}
        or any(
            core_captures[filename]["identity"]["sha256"]
            != declared_files[filename].get("sha256")
            for filename in declared_files
        )
    ):
        return _set_failed(family, "route_artifact_hash_mismatch")
    family["source"]["inputs"].extend(
        run_captures[filename]["identity"] for filename, _limit in run_members
    )
    family["source"]["inputs"].extend(
        core_captures[filename]["identity"] for filename, _limit in core_members
    )
    family["source"]["inputs"].sort(key=lambda item: item["logical_path"])

    cost = evidence["cost_evidence"]
    audit = evidence["audit"]
    bindings = cost["bindings"]
    transcripts = cost["transcripts"]
    markets = cost["selected_markets"]
    if (
        cost["binding_count"] != len(bindings)
        or cost["transcript_count"] != len(transcripts)
        or cost["selected_market_count"] != len(markets)
    ):
        return _set_failed(family, "route_declared_count_mismatch")

    def row_counts(rows: Sequence[Mapping[str, Any]], field: str) -> Dict[str, int]:
        values: Dict[str, int] = {}
        for row in rows:
            value = str(row[field])
            values[value] = values.get(value, 0) + 1
        return dict(sorted(values.items()))

    try:
        binding_status_counts = row_counts(bindings, "status")
        binding_reason_counts = row_counts(bindings, "reason_code")
        transcript_status_counts = row_counts(transcripts, "status")
        transcript_reason_counts = row_counts(transcripts, "reason_code")
        market_status_counts = row_counts(markets, "structural_support_status")
        observation_values = (
            manifest["observation_bounds"]["minimum_state_observed_at"],
            manifest["observation_bounds"]["maximum_state_observed_at"],
            cost["evaluated_at"],
            audit["audit_finished_at"],
        )
        observation_times = []
        for value in observation_values:
            parsed, _canonical = _parse_observation_timestamp(value)
            if parsed > generated_at:
                raise _PublicDataError("future_observation_timestamp")
            observation_times.append((parsed, value))
    except (KeyError, TypeError, ValueError):
        return _set_failed(family, "route_bundle_invalid")
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    minimum = min(observation_times, key=lambda item: item[0])
    maximum = max(observation_times, key=lambda item: item[0])
    freshness = int((generated_at - maximum[0]).total_seconds())
    reason_counts = dict(binding_reason_counts)
    if freshness > 86400:
        reason_counts["stale_partition"] = 1
    usable_bindings = binding_status_counts.get("observed", 0)
    usable_transcripts = transcript_status_counts.get("observed", 0)
    usable_markets = market_status_counts.get("supported", 0)
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": cost["binding_count"],
                "observed": len(bindings),
                "usable": usable_bindings,
                "expected_basis": {
                    "kind": "sealed_route_cost_evidence",
                    "phase": pointer["phase"],
                    "phase_state_sha256": pointer["phase_state_sha256"],
                    "phase_transition_id": pointer["phase_transition_id"],
                    "audit_sha256": pointer["audit_sha256"],
                    "core_pointer_sha256": pointer["core_pointer_sha256"],
                    "core_manifest_sha256": pointer["core_manifest_sha256"],
                    "route_universe_sha256": pointer["route_universe_sha256"],
                    "route_cost_evidence_sha256": pointer[
                        "route_cost_evidence_sha256"
                    ],
                    "baseline_manifest_sha256": pointer[
                        "baseline_manifest_sha256"
                    ],
                    "candidate_source_generation": pointer[
                        "candidate_source_generation"
                    ],
                    "route_cohort_identity_sha256": _opaque_identifier_hash(
                        "data_quality_snapshot/v1/route_cohort",
                        pointer["route_cohort_id"],
                    ),
                    "run_identity_sha256": _opaque_identifier_hash(
                        "data_quality_snapshot/v1/route_shadow_run",
                        pointer["run_id"],
                    ),
                },
            },
            "coverage_bps": _basis_points(usable_bindings, cost["binding_count"]),
            "duplicate_primary_key": {
                "count": 0,
                "rate_bps": _basis_points(0, len(bindings)),
            },
            "required_field_null": {
                "count": 0,
                "rate_bps": _basis_points(0, len(bindings)),
            },
            "status_counts": binding_status_counts,
            "reason_counts": dict(sorted(reason_counts.items())),
            "observation_time": {
                "min": minimum[1],
                "max": maximum[1],
                "freshness_lag_seconds": freshness,
            },
            "entities": {
                "bindings": {
                    "expected": cost["binding_count"],
                    "observed": len(bindings),
                    "usable": usable_bindings,
                    "status_counts": binding_status_counts,
                },
                "runs": {
                    "expected": 1,
                    "observed": 1,
                    "usable": 1,
                    "status_counts": {pointer["phase"]: 1},
                },
                "markets": {
                    "expected": cost["selected_market_count"],
                    "observed": len(markets),
                    "usable": usable_markets,
                },
                "transcripts": {
                    "expected": cost["transcript_count"],
                    "observed": len(transcripts),
                    "usable": usable_transcripts,
                    "status_counts": transcript_status_counts,
                },
            },
        }
    )
    if binding_reason_counts:
        family["entities"]["bindings"]["reason_counts"] = binding_reason_counts
    if market_status_counts:
        family["entities"]["markets"]["status_counts"] = market_status_counts
    if transcript_reason_counts:
        family["entities"]["transcripts"]["reason_counts"] = transcript_reason_counts
    return family


def _evaluate_tvl(
    spec: Mapping[str, Any],
    data_dir: Path,
    generated_at: datetime,
) -> Dict[str, Any]:
    family = _empty_family(spec)
    try:
        capture = _capture_candidate(
            data_dir,
            "dex_pool_tvl_latest.csv",
            "local",
            _MAX_CSV_BYTES,
        )
    except _PublicDataError as error:
        return _set_failed(family, error.reason)
    if capture is None:
        return family
    family["source"] = {"inputs": [capture["identity"]]}
    try:
        text = capture["payload"].decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        if tuple(reader.fieldnames or ()) != _TVL_COLUMNS:
            raise _PublicDataError("schema_mismatch")
        raw_rows = list(reader)
    except UnicodeDecodeError:
        return _set_failed(family, "invalid_utf8")
    except _PublicDataError as error:
        return _set_failed(family, error.reason)

    observed_count = len(raw_rows)
    required_null_count = 0
    measurement_fields = {
        field: {"null_count": 0, "zero_count": 0}
        for field in _TVL_MEASUREMENT_FIELDS
    }
    snapshots = set()
    keys = []
    usable_count = 0
    status_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    observation_times: List[Tuple[datetime, str]] = []
    try:
        for row in raw_rows:
            if None in row:
                raise _PublicDataError("schema_mismatch")
            nulls = [
                field for field in _TVL_REQUIRED_FIELDS if not (row.get(field) or "").strip()
            ]
            required_null_count += len(nulls)
            if nulls:
                raise _PublicDataError("required_field_null")
            snapshot_id = row["snapshot_id"].strip()
            if not _TVL_GENERATION_ID_RE.fullmatch(snapshot_id):
                raise _PublicDataError("invalid_snapshot_id")
            token = row["token_symbol"].strip().upper()
            chain = row["chain"].strip()
            pool = row["pool_address"].strip()
            snapshots.add(snapshot_id)
            keys.append((token, chain, pool))
            observed_at, canonical_time = _parse_observation_timestamp(row["observed_at"])
            if observed_at > generated_at:
                raise _PublicDataError("future_observation_timestamp")
            observation_times.append((observed_at, canonical_time))
            status = row["status"].strip().lower()
            reason = row["reason_code"].strip().lower()
            if status not in _TVL_STATUSES:
                raise _PublicDataError("invalid_status")
            if reason not in _TVL_STATUS_REASONS[status]:
                raise _PublicDataError("status_reason_conflict")
            status_counts[status] = status_counts.get(status, 0) + 1
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            measurements = {}
            for field in _TVL_MEASUREMENT_FIELDS:
                number = _parse_decimal(row.get(field, ""))
                measurements[field] = number
                if number is None:
                    measurement_fields[field]["null_count"] += 1
                elif number == 0:
                    measurement_fields[field]["zero_count"] += 1
                if number is not None and number < 0:
                    raise _PublicDataError("invalid_measurement")
            tvl = measurements["tvl_usd"]
            if status == "observed":
                if tvl is None:
                    raise _PublicDataError("status_measurement_conflict")
                usable_count += 1
            elif tvl is not None:
                raise _PublicDataError("status_measurement_conflict")
    except _PublicDataError as error:
        family["required_field_null"] = {
            "count": required_null_count,
            "rate_bps": _basis_points(
                required_null_count, observed_count * len(_TVL_REQUIRED_FIELDS)
            ),
        }
        return _set_failed(family, error.reason)

    if len(snapshots) > 1:
        return _set_failed(family, "mixed_snapshot_id")
    duplicate_count = observed_count - len(set(keys))
    family["duplicate_primary_key"] = {
        "count": duplicate_count,
        "rate_bps": _basis_points(duplicate_count, observed_count),
    }
    if duplicate_count:
        return _set_failed(family, "duplicate_primary_key")

    family["required_field_null"] = {
        "count": required_null_count,
        "rate_bps": _basis_points(
            required_null_count, observed_count * len(_TVL_REQUIRED_FIELDS)
        ),
    }
    family["measurements"] = {
        "null_count": sum(item["null_count"] for item in measurement_fields.values()),
        "zero_count": sum(item["zero_count"] for item in measurement_fields.values()),
        "fields": measurement_fields,
    }
    minimum = min(observation_times, default=None)
    maximum = max(observation_times, default=None)
    freshness = (
        int((generated_at - maximum[0]).total_seconds()) if maximum is not None else None
    )
    if freshness is not None and freshness > 86400:
        reason_counts["stale_partition"] = 1
    family.update(
        {
            "state": "evaluated",
            "not_evaluated_reason": None,
            "failure_reason": None,
            "counts": {
                "expected": observed_count,
                "observed": observed_count,
                "usable": usable_count,
                "expected_basis": {
                    "kind": "latest_file_inventory",
                    "snapshot_id": next(iter(snapshots), None),
                },
            },
            "coverage_bps": _basis_points(usable_count, observed_count),
            "status_counts": dict(sorted(status_counts.items())),
            "reason_counts": dict(sorted(reason_counts.items())),
            "observation_time": {
                "min": minimum[1] if minimum is not None else None,
                "max": maximum[1] if maximum is not None else None,
                "freshness_lag_seconds": freshness,
            },
        }
    )
    return family


def build_snapshot(
    data_dir: Path,
    generated_at_utc: str,
    window_end: date,
    window_days: int,
    application_sha: str,
) -> Dict[str, Any]:
    """Evaluate fixed data families found below ``data_dir``."""

    data_path = Path(data_dir)
    generated_at = _parse_generated_at(generated_at_utc)
    if not isinstance(window_end, date) or isinstance(window_end, datetime):
        raise ValueError("window_end must be a date")
    if not isinstance(window_days, int) or isinstance(window_days, bool) or window_days <= 0:
        raise ValueError("window_days must be a positive integer")
    if not isinstance(application_sha, str) or not _FULL_SHA_RE.fullmatch(application_sha):
        raise ValueError("application_sha must be a full lowercase Git SHA")
    if window_end > generated_at.date():
        raise ValueError("window_end cannot be after generated_at_utc")

    window_start = window_end - timedelta(days=window_days - 1)
    families: List[Dict[str, Any]] = []
    for spec in FAMILY_SPECS:
        if spec["name"] == "cex_daily_ohlcv":
            family = _evaluate_cex_daily(
                spec,
                data_path,
                generated_at,
                window_start,
                window_end,
            )
        elif spec["name"] == "dex_daily_ohlcv":
            family = _evaluate_dex_daily(
                spec,
                data_path,
                generated_at,
                window_start,
                window_end,
            )
        elif spec["name"] == "cex_depth":
            family = _evaluate_depth(
                spec,
                data_path,
                generated_at,
                filename="cex_depth_latest.csv",
                columns=_CEX_DEPTH_COLUMNS,
                required_fields=_CEX_DEPTH_REQUIRED_FIELDS,
                measurement_fields=_CEX_DEPTH_MEASUREMENT_FIELDS,
                status_reasons=_CEX_DEPTH_STATUS_REASONS,
                market_type="cex",
            )
        elif spec["name"] == "dex_depth":
            family = _evaluate_depth(
                spec,
                data_path,
                generated_at,
                filename="dex_depth_latest.csv",
                columns=_DEX_DEPTH_COLUMNS,
                required_fields=_DEX_DEPTH_REQUIRED_FIELDS,
                measurement_fields=_DEX_DEPTH_MEASUREMENT_FIELDS,
                status_reasons=_DEX_DEPTH_STATUS_REASONS,
                market_type="dex",
            )
        elif spec["name"] == "cex_execution_cost":
            family = _evaluate_execution(
                spec,
                data_path,
                generated_at,
                filename="cex_execution_cost_latest.csv",
                market_type="cex",
            )
        elif spec["name"] == "dex_execution_cost":
            family = _evaluate_execution(
                spec,
                data_path,
                generated_at,
                filename="dex_execution_cost_latest.csv",
                market_type="dex",
            )
        elif spec["name"] == "event_facts":
            family = _evaluate_event_facts(spec, data_path, generated_at)
        elif spec["name"] == "cex_instrument_lifecycle":
            family = _evaluate_cex_lifecycle(spec, data_path, generated_at)
        elif spec["name"] == "market_lifecycle_reviews":
            family = _evaluate_market_lifecycle(spec, data_path, generated_at)
        elif spec["name"] == "route_cohort_opportunity":
            family = _evaluate_route_opportunity(spec, data_path, generated_at)
        elif spec["name"] == "route_shadow_route_cost_evidence":
            family = _evaluate_route_shadow(spec, data_path, generated_at)
        elif spec["name"] == "tvl":
            family = _evaluate_tvl(spec, data_path, generated_at)
        else:
            family = _empty_family(spec)
        families.append(family)

    source_identities = {}
    for family in families:
        source = family.get("source")
        if not isinstance(source, dict):
            continue
        for item in source.get("inputs", []):
            key = (item["logical_path"], item["size_bytes"], item["sha256"])
            source_identities[key] = dict(item)
    sorted_source_identities = [
        source_identities[key] for key in sorted(source_identities)
    ]
    identity_payload = {
        "application_sha": application_sha,
        "generated_at_utc": generated_at_utc,
        "input_sources": sorted_source_identities,
        "schema_version": SCHEMA_VERSION,
        "window_end": window_end.isoformat(),
        "window_days": window_days,
    }
    state_counts = {
        state: sum(1 for family in families if family["state"] == state)
        for state in ("evaluated", "failed", "not_evaluated")
    }
    snapshot: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "application": {"build_sha": application_sha},
        "publication": {"identity": _snapshot_hash(identity_payload)},
        "window": {
            "start_date": window_start.isoformat(),
            "end_date": window_end.isoformat(),
            "expected_days": window_days,
            "timezone": "UTC",
        },
        "summary": {
            "evaluated_family_count": state_counts["evaluated"],
            "failed_family_count": state_counts["failed"],
            "not_evaluated_family_count": state_counts["not_evaluated"],
            "total_family_count": len(families),
        },
        "families": families,
    }
    snapshot["snapshot_sha256"] = _snapshot_hash(snapshot)
    return snapshot
