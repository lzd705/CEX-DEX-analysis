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


SCHEMA_VERSION = "data_quality_snapshot/v1"
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
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
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


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
                       snapshots.cex_source_name,
                       snapshots.cex_source_bytes,
                       snapshots.cex_sha256,
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
            if (
                state["cex_source_name"] != "cex_exchange_volume_daily.csv"
                or state["cex_source_bytes"] != cex_source["size_bytes"]
                or state["cex_sha256"] != cex_source["sha256"]
            ):
                raise _PublicDataError("authoritative_inventory_source_mismatch")
            markets = [
                (str(row[0]).strip().upper(), str(row[1]).strip(), str(row[2]).strip())
                for row in connection.execute(
                    """
                    SELECT DISTINCT token_symbol, exchange, cex_symbol
                    FROM cex_market_daily
                    ORDER BY token_symbol, exchange, cex_symbol
                    """
                ).fetchall()
            ]
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
    generation = {
        "snapshot_id": str(state["snapshot_id"]),
        "import_run_id": str(state["import_run_id"]),
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


def _set_failed(family: Dict[str, Any], reason: str) -> Dict[str, Any]:
    family["state"] = "failed"
    family["not_evaluated_reason"] = None
    family["failure_reason"] = reason
    return family


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
        markets, generation = _read_cex_inventory(
            database_capture["payload"], csv_capture["identity"]
        )
        text = csv_capture["payload"].decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        if tuple(reader.fieldnames or ()) != _CEX_COLUMNS:
            raise _PublicDataError("schema_mismatch")
        raw_rows = list(reader)
    except UnicodeDecodeError:
        return _set_failed(family, "invalid_utf8")
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
        "snapshot_id": generation["snapshot_id"],
        "import_run_id": generation["import_run_id"],
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
    known_markets = set(markets)

    observed_rows = []
    keys = []
    required_null_count = 0
    measurement_fields = {
        field: {"null_count": 0, "zero_count": 0}
        for field in _CEX_MEASUREMENT_FIELDS
    }
    structural_null = False
    try:
        for row in raw_rows:
            required_null_count += sum(
                1 for field in _CEX_COLUMNS if (row.get(field) or "").strip() == ""
            )
            if any((row.get(field) or "").strip() == "" for field in _CEX_IDENTITY_FIELDS):
                structural_null = True
                continue
            day = _parse_canonical_day(row["date"])
            token = row["token_symbol"].strip().upper()
            exchange = row["exchange"].strip()
            cex_symbol = row["cex_symbol"].strip()
            if not token or not exchange or not cex_symbol:
                structural_null = True
                continue
            market = (token, exchange, cex_symbol)
            if window_start <= day <= window_end and market not in known_markets:
                raise _PublicDataError("market_not_in_authoritative_inventory")
            measurements = {}
            usable = True
            for field in _CEX_MEASUREMENT_FIELDS:
                number = _parse_decimal(row.get(field, ""))
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
            key = (day.isoformat(), token, exchange, cex_symbol)
            if window_start <= day <= window_end:
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
    if structural_null:
        return _set_failed(family, "required_field_null")
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
            nulls = [
                field for field in _TVL_REQUIRED_FIELDS if not (row.get(field) or "").strip()
            ]
            required_null_count += len(nulls)
            if nulls:
                raise _PublicDataError("required_field_null")
            snapshot_id = row["snapshot_id"].strip()
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
