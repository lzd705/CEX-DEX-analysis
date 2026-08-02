"""Fail-closed verification of one published snapshot fact."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from scripts.quality_outcomes import (
    cex_reason_code,
    dex_depth_reason_code,
    normalize_cex_source_outcome,
    normalize_dex_depth_source_outcome,
    normalize_tvl_source_outcome,
    quality_outcome_resolution_state,
    quality_outcome_rule,
    tvl_reason_code,
)
from scripts.timestamp_contract import canonical_rfc3339_utc


MAX_PUBLICATION_BYTES = 8 * 1024 * 1024
MAX_SNAPSHOT_ID_LENGTH = 128
MAX_MARKET_ID_LENGTH = 512
MAX_OUTCOME_LENGTH = 64
DEPTH_BANDS_BPS = (10, 25, 50, 100)


@dataclass(frozen=True)
class SnapshotFactState:
    market_id: str
    fact_type: str
    snapshot_id: Optional[str]
    dataset_sha256: str
    observed_at: Optional[str]
    status: str
    reason_code: Optional[str]
    retryable: bool
    publication_generation: str
    target_fingerprint: Optional[str] = None


@dataclass(frozen=True)
class SnapshotRefreshResult:
    succeeded: bool
    resolution: Optional[str]
    error_code: Optional[str]
    retryable: bool
    before: Optional[SnapshotFactState]
    after: Optional[SnapshotFactState]

    @classmethod
    def success(
        cls,
        resolution: str,
        before: SnapshotFactState,
        after: SnapshotFactState,
    ) -> "SnapshotRefreshResult":
        return cls(True, resolution, None, False, before, after)

    @classmethod
    def failure(
        cls,
        error_code: str,
        *,
        retryable: bool = False,
        before: Optional[SnapshotFactState] = None,
        after: Optional[SnapshotFactState] = None,
    ) -> "SnapshotRefreshResult":
        return cls(False, None, error_code, retryable, before, after)


_CEX_REQUIRED = {
    "snapshot_id", "observed_at", "response_received_at", "token_symbol",
    "exchange", "cex_symbol", "source_instrument", "source_quote_asset",
    "quote_conversion_method", "best_bid", "best_ask", "midpoint",
    "spread_quote", "spread_bps", "bid_depth_10bps_usd",
    "ask_depth_10bps_usd", "total_depth_10bps_usd", "bid_depth_25bps_usd",
    "ask_depth_25bps_usd", "total_depth_25bps_usd", "bid_depth_50bps_usd",
    "ask_depth_50bps_usd", "total_depth_50bps_usd", "bid_depth_100bps_usd",
    "ask_depth_100bps_usd", "total_depth_100bps_usd", "depth_10bps_complete",
    "depth_25bps_complete", "depth_50bps_complete", "depth_100bps_complete",
    "depth_method", "source_endpoint", "raw_response_sha256", "status",
    "reason_code", "error",
}
_DEX_DEPTH_REQUIRED = {
    "snapshot_id", "observed_at", "response_received_at", "token_symbol", "chain",
    "dex", "pool_address", "protocol_model", "block_number", "fee_bps",
    "pool_state_price_usd", "source_target_price_usd", "price_difference_bps",
    "sell_depth_10bps_usd", "buy_depth_10bps_usd", "total_depth_10bps_usd",
    "sell_depth_25bps_usd", "buy_depth_25bps_usd", "total_depth_25bps_usd",
    "sell_depth_50bps_usd", "buy_depth_50bps_usd", "total_depth_50bps_usd",
    "sell_depth_100bps_usd", "buy_depth_100bps_usd", "total_depth_100bps_usd",
    "depth_10bps_complete", "depth_25bps_complete", "depth_50bps_complete",
    "depth_100bps_complete", "depth_method", "source_endpoint",
    "raw_response_sha256", "status", "error",
}
_TVL_REQUIRED = {
    "snapshot_id", "observed_at", "token_symbol", "chain", "dex", "pool_address",
    "tvl_usd", "tvl_method", "source", "source_endpoint", "raw_response_sha256",
    "status", "error",
}
_CEX_NUMBERS = {
    "best_bid", "best_ask", "midpoint", "spread_quote", "spread_bps",
    "bid_depth_10bps_usd", "ask_depth_10bps_usd", "total_depth_10bps_usd",
    "bid_depth_25bps_usd", "ask_depth_25bps_usd", "total_depth_25bps_usd",
    "bid_depth_50bps_usd", "ask_depth_50bps_usd", "total_depth_50bps_usd",
    "bid_depth_100bps_usd", "ask_depth_100bps_usd", "total_depth_100bps_usd",
}
_DEX_DEPTH_FACT_NUMBERS = {
    "fee_bps", "pool_state_price_usd", "source_target_price_usd",
    "price_difference_bps", "sell_depth_10bps_usd", "buy_depth_10bps_usd",
    "total_depth_10bps_usd", "sell_depth_25bps_usd", "buy_depth_25bps_usd",
    "total_depth_25bps_usd", "sell_depth_50bps_usd", "buy_depth_50bps_usd",
    "total_depth_50bps_usd", "sell_depth_100bps_usd", "buy_depth_100bps_usd",
    "total_depth_100bps_usd",
}


def _text(value: Any, maximum: int = MAX_OUTCOME_LENGTH) -> str:
    result = str(value or "").strip()
    if len(result) > maximum:
        raise ValueError("snapshot field exceeds its size limit")
    return result


def _address(value: Any) -> str:
    result = _text(value, MAX_MARKET_ID_LENGTH)
    if not result:
        raise ValueError("snapshot market identity is empty")
    return result.lower() if result.startswith("0x") else result


def _timestamp(value: Any) -> str:
    text = _text(value, MAX_SNAPSHOT_ID_LENGTH)
    try:
        return canonical_rfc3339_utc(text)
    except (OverflowError, ValueError) as error:
        raise ValueError("snapshot timestamp is invalid") from error


def _finite_nonnegative(value: Any) -> float:
    text = _text(value, MAX_SNAPSHOT_ID_LENGTH)
    if not text:
        raise ValueError("observed snapshot value is blank")
    try:
        number = float(text)
    except (TypeError, ValueError) as error:
        raise ValueError("observed snapshot value is invalid") from error
    if not math.isfinite(number) or number < 0:
        raise ValueError("observed snapshot value is not finite and nonnegative")
    return number


def _finite(value: Any) -> float:
    text = _text(value, MAX_SNAPSHOT_ID_LENGTH)
    if not text:
        raise ValueError("observed snapshot value is blank")
    try:
        number = float(text)
    except (TypeError, ValueError) as error:
        raise ValueError("observed snapshot value is invalid") from error
    if not math.isfinite(number):
        raise ValueError("observed snapshot value is not finite")
    return number


def _numerically_equal(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-12, abs_tol=0.0)


def _validate_nondecreasing_depth(numbers: Dict[str, float], sides: Tuple[str, ...]) -> None:
    for side in sides:
        previous = None
        for band in DEPTH_BANDS_BPS:
            current = numbers["{}_depth_{}bps_usd".format(side, band)]
            if (
                previous is not None
                and current < previous
                and not _numerically_equal(current, previous)
            ):
                raise ValueError("snapshot depth decreases across wider bands")
            previous = current


def _validate_depth_totals(
    numbers: Dict[str, float], left_side: str, right_side: str
) -> None:
    for band in DEPTH_BANDS_BPS:
        total = numbers["total_depth_{}bps_usd".format(band)]
        sides = (
            numbers["{}_depth_{}bps_usd".format(left_side, band)]
            + numbers["{}_depth_{}bps_usd".format(right_side, band)]
        )
        if not _numerically_equal(total, sides):
            raise ValueError("snapshot depth total is inconsistent with sides")


def _validate_depth_completeness_status(
    row: Dict[str, str], raw_status: str
) -> Tuple[int, ...]:
    flags = []
    for band in DEPTH_BANDS_BPS:
        text = _text(row.get("depth_{}bps_complete".format(band)), 4)
        if text not in {"0", "1"}:
            raise ValueError("snapshot completeness flag is invalid")
        flags.append(int(text))
    result = tuple(flags)
    if raw_status in {"observed", "complete"} and any(flag != 1 for flag in result):
        raise ValueError("observed snapshot depth is incomplete")
    if raw_status == "partial" and all(flag == 1 for flag in result):
        raise ValueError("partial snapshot depth is complete")
    if any(wider > narrower for narrower, wider in zip(result, result[1:])):
        raise ValueError("snapshot completeness recovers at a wider band")
    return result


def _validate_raw_response_sha256(value: Any, *, required: bool) -> None:
    text = _text(value, 64)
    if not text:
        if required:
            raise ValueError("snapshot source evidence hash is missing")
        return
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError("snapshot source evidence hash is invalid")


def _block_number(value: Any, *, required: bool = False) -> None:
    """Validate optional fixed-block provenance without treating it as a fact."""
    text = _text(value, MAX_SNAPSHOT_ID_LENGTH)
    if not text:
        if required:
            raise ValueError("measured snapshot row has no block identity")
        return
    if not text.isascii() or not text.isdecimal():
        raise ValueError("snapshot block number is not a nonnegative integer")


def _parse_request(request: Dict[str, Any]) -> Tuple[str, str, str, Tuple[str, ...]]:
    token = _text(request.get("token_symbol"), 32).upper()
    fact_type = _text(request.get("fact_type"), 16).lower()
    market_id = _text(request.get("market_id"), MAX_MARKET_ID_LENGTH)
    if not token or fact_type not in {"depth", "tvl"}:
        raise ValueError("snapshot request is invalid")
    parts = market_id.split(":", 2)
    if len(parts) != 3 or not all(parts):
        raise ValueError("market_id is not canonical")
    family, first, final = parts
    if family == "cex":
        if ":" in final or not first or not final:
            raise ValueError("market_id is not canonical")
        instrument_parts = final.split("/")
        if len(instrument_parts) != 2 or not all(instrument_parts):
            raise ValueError("market_id is not canonical")
        base = instrument_parts[0].upper()
        if base != token:
            raise ValueError("market_id token does not match request")
        return token, fact_type, market_id, ("cex", first.lower(), final.upper())
    if family == "dex":
        segments = final.split(":")
        if len(segments) != 3 or not all(segments) or fact_type not in {"depth", "tvl"}:
            raise ValueError("market_id is not canonical")
        dex, address, market_token = segments
        if market_token.upper() != token:
            raise ValueError("market_id token does not match request")
        return token, fact_type, market_id, (
            "dex", first.lower(), dex.lower(), _address(address), token,
        )
    raise ValueError("market_id is not canonical")


def validate_snapshot_request(request: Dict[str, Any]) -> Dict[str, str]:
    """Validate and normalize the canonical request accepted by public actions."""
    token, fact_type, market_id, parsed = _parse_request(request)
    family = parsed[0]
    if fact_type == "tvl" and family != "dex":
        raise ValueError("TVL refresh applies only to DEX markets")
    return {
        "token_symbol": token,
        "market_id": market_id,
        "market_type": family,
        "fact_type": fact_type,
    }


def _publication_path(data_dir: Path, family: str, fact_type: str) -> Path:
    if family == "cex" and fact_type == "depth":
        return data_dir / "cex_depth_latest.csv"
    if family == "dex" and fact_type == "depth":
        return data_dir / "dex_depth_latest.csv"
    if family == "dex" and fact_type == "tvl":
        return data_dir / "dex_pool_tvl_latest.csv"
    raise ValueError("snapshot request has no matching publication")


def _read_publication(path: Path) -> Tuple[str, list, list]:
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(str(path), flags)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise ValueError("publication is not a regular file")
            if before.st_size < 1 or before.st_size > MAX_PUBLICATION_BYTES:
                raise ValueError("publication size is invalid")
            chunks = []
            remaining = MAX_PUBLICATION_BYTES + 1
            while remaining:
                block = os.read(descriptor, min(1024 * 1024, remaining))
                if not block:
                    break
                chunks.append(block)
                remaining -= len(block)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except OSError as error:
        raise ValueError("publication cannot be read") from error
    if (
        len(payload) > MAX_PUBLICATION_BYTES
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
    ):
        raise ValueError("publication changed while being read")
    try:
        decoded = payload.decode("utf-8")
        reader = csv.DictReader(io.StringIO(decoded, newline=""))
        headers = reader.fieldnames
        rows = list(reader)
    except (UnicodeDecodeError, csv.Error) as error:
        raise ValueError("publication is not valid UTF-8 CSV") from error
    if not headers or len(headers) != len(set(headers)) or not rows:
        raise ValueError("publication header or rows are invalid")
    return hashlib.sha256(payload).hexdigest(), headers, rows


def _required_for(family: str, fact_type: str) -> set:
    if family == "cex":
        return _CEX_REQUIRED
    if fact_type == "tvl":
        return _TVL_REQUIRED
    return _DEX_DEPTH_REQUIRED


def _row_identity(row: Dict[str, str], family: str, fact_type: str) -> Tuple[str, ...]:
    token = _text(row.get("token_symbol"), 32).upper()
    if not token:
        raise ValueError("snapshot token is empty")
    if family == "cex":
        exchange = _text(row.get("exchange")).lower()
        symbol = _text(row.get("cex_symbol"), MAX_MARKET_ID_LENGTH).upper()
        symbol_parts = symbol.split("/")
        if not exchange or len(symbol_parts) != 2 or not all(symbol_parts) or symbol_parts[0] != token:
            raise ValueError("snapshot CEX identity is empty")
        return ("cex", exchange, symbol, token)
    chain = _text(row.get("chain")).lower()
    dex = _text(row.get("dex")).lower()
    address = _address(row.get("pool_address"))
    if not chain or not dex:
        raise ValueError("snapshot DEX identity is empty")
    return ("dex", chain, dex, address, token)


def _target_fingerprint(row: Dict[str, str]) -> str:
    payload = {
        field: row.get(field, "")
        for field in sorted(row)
        if field != "snapshot_id"
    }
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_row(row: Dict[str, str], family: str, fact_type: str) -> Tuple[str, ...]:
    snapshot_id = _text(row.get("snapshot_id"), MAX_SNAPSHOT_ID_LENGTH)
    if not snapshot_id:
        raise ValueError("snapshot publication has no identity")
    _timestamp(row.get("observed_at"))
    if family != "dex" or fact_type == "depth":
        _timestamp(row.get("response_received_at"))
    raw_status = _text(row.get("status")).lower()
    if family == "cex":
        if raw_status not in {"observed", "partial", "failed"}:
            raise ValueError("snapshot status is empty")
        normalized_reason = cex_reason_code(row.get("reason_code"), row.get("error"))
        if normalized_reason is not None:
            if normalized_reason == "observed":
                expected_status = "observed"
            elif normalized_reason in {"source_level_limit", "measurement_limit"}:
                expected_status = "partial"
            else:
                expected_status = "failed"
            if raw_status != expected_status:
                raise ValueError("snapshot CEX status and reason conflict")
        if _text(row.get("reason_code")).lower() and normalized_reason is None:
            # A producer-provided unknown reason cannot be upgraded by status.
            status, reason = raw_status, None
        else:
            status, reason = normalize_cex_source_outcome(
                raw_status, row.get("reason_code"), row.get("error")
            )
        if raw_status in {"observed", "partial"}:
            numbers = {}
            for field in _CEX_NUMBERS:
                numbers[field] = _finite_nonnegative(row.get(field))
            best_bid = numbers["best_bid"]
            best_ask = numbers["best_ask"]
            if best_bid <= 0 or best_ask <= 0:
                raise ValueError("snapshot CEX quotes must be positive")
            if best_bid >= best_ask:
                raise ValueError("snapshot CEX book is crossed or locked")
            expected_midpoint = (best_bid + best_ask) / 2
            expected_spread = best_ask - best_bid
            if not (
                _numerically_equal(numbers["midpoint"], expected_midpoint)
                and _numerically_equal(numbers["spread_quote"], expected_spread)
                and _numerically_equal(
                    numbers["spread_bps"],
                    expected_spread / expected_midpoint * 10_000,
                )
            ):
                raise ValueError("snapshot CEX midpoint or spread is inconsistent")
            _validate_nondecreasing_depth(numbers, ("bid", "ask", "total"))
            _validate_depth_totals(numbers, "bid", "ask")
            _validate_depth_completeness_status(row, raw_status)
        else:
            for field in _CEX_NUMBERS:
                if _text(row.get(field)):
                    raise ValueError("unmeasured snapshot row contains a numeric value")
            for field in ("depth_10bps_complete", "depth_25bps_complete", "depth_50bps_complete", "depth_100bps_complete"):
                if _text(row.get(field)):
                    raise ValueError("unmeasured snapshot row contains a completeness flag")
    elif fact_type == "depth":
        if raw_status not in {"observed", "complete", "partial", "unsupported", "failed"}:
            raise ValueError("snapshot status is invalid")
        _block_number(
            row.get("block_number"),
            required=raw_status in {"observed", "complete", "partial"},
        )
        if raw_status in {"observed", "complete", "partial"}:
            numbers = {}
            for field in _DEX_DEPTH_FACT_NUMBERS:
                if field == "price_difference_bps":
                    numbers[field] = _finite(row.get(field))
                else:
                    numbers[field] = _finite_nonnegative(row.get(field))
            _validate_nondecreasing_depth(numbers, ("sell", "buy", "total"))
            _validate_depth_totals(numbers, "sell", "buy")
            _validate_depth_completeness_status(row, raw_status)
        else:
            for field in _DEX_DEPTH_FACT_NUMBERS:
                if _text(row.get(field)):
                    raise ValueError("unmeasured snapshot row contains a numeric value")
            for field in ("depth_10bps_complete", "depth_25bps_complete", "depth_50bps_complete", "depth_100bps_complete"):
                if _text(row.get(field)):
                    raise ValueError("unmeasured snapshot row contains a completeness flag")
        supplied_reason = _text(row.get("reason_code")).lower()
        if "reason_code" in row and not supplied_reason:
            raise ValueError("snapshot DEX depth reason code is missing")
        bounded_reason = dex_depth_reason_code(supplied_reason)
        if supplied_reason and bounded_reason is None:
            raise ValueError("snapshot DEX depth reason code is invalid")
        allowed_reasons = {
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
        }[raw_status]
        if bounded_reason and bounded_reason not in allowed_reasons:
            raise ValueError("snapshot DEX depth status and reason conflict")
        status, reason = normalize_dex_depth_source_outcome(
            raw_status, supplied_reason, row.get("error")
        )
    else:
        if raw_status not in {"observed", "missing", "not_found", "failed"}:
            raise ValueError("snapshot status is invalid")
        if raw_status == "observed":
            _finite_nonnegative(row.get("tvl_usd"))
        elif raw_status in {"missing", "not_found", "failed"}:
            if _text(row.get("tvl_usd")):
                raise ValueError("non-observed TVL must not contain a measured value")
        supplied_reason = _text(row.get("reason_code")).lower()
        if "reason_code" in row and not supplied_reason:
            raise ValueError("snapshot TVL reason code is missing")
        bounded_reason = tvl_reason_code(supplied_reason)
        if supplied_reason and bounded_reason is None:
            raise ValueError("snapshot TVL reason code is invalid")
        allowed_reasons = {
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
        }[raw_status]
        if bounded_reason and bounded_reason not in allowed_reasons:
            raise ValueError("snapshot TVL status and reason conflict")
        status, reason = normalize_tvl_source_outcome(
            raw_status, supplied_reason, row.get("error")
        )
    _text(status)
    if reason is not None:
        _text(reason)
    if family == "cex":
        measured = raw_status in {"observed", "partial"}
    elif fact_type == "depth":
        measured = raw_status in {"observed", "complete", "partial"}
    else:
        measured = raw_status == "observed"
    locally_classified_dex_unsupported = (
        family == "dex"
        and fact_type == "depth"
        and raw_status == "unsupported"
    )
    requires_source_hash = (
        measured
        or (family == "dex" and fact_type == "depth" and raw_status == "failed")
        or (family == "dex" and fact_type == "tvl" and raw_status == "failed")
        or (
            quality_outcome_resolution_state(status, reason)
            == "confirmed_terminal_absence"
            and not locally_classified_dex_unsupported
        )
    )
    _validate_raw_response_sha256(
        row.get("raw_response_sha256"), required=requires_source_hash
    )
    return _row_identity(row, family, fact_type)


def _state_from_row(
    row: Optional[Dict[str, str]],
    *,
    token: str,
    market_id: str,
    fact_type: str,
    family: str,
    snapshot_id: str,
    dataset_sha256: str,
) -> SnapshotFactState:
    generation = "{}:{}".format(dataset_sha256[:16], snapshot_id[:MAX_SNAPSHOT_ID_LENGTH])
    if row is None:
        if fact_type == "tvl":
            status, reason = normalize_tvl_source_outcome(
                "not_cataloged_in_snapshot"
            )
            rule = quality_outcome_rule(status, reason)
            retryable = rule.retryable if rule is not None else False
        else:
            status, reason, retryable = (
                "not_cataloged_in_snapshot",
                None,
                False,
            )
        return SnapshotFactState(
            market_id, fact_type, snapshot_id, dataset_sha256, None,
            status, reason, retryable, generation,
        )
    raw_status = _text(row.get("status")).lower()
    if family == "cex":
        supplied_reason = _text(row.get("reason_code")).lower()
        if supplied_reason and cex_reason_code(row.get("reason_code"), row.get("error")) is None:
            status, reason = raw_status, None
        else:
            status, reason = normalize_cex_source_outcome(raw_status, row.get("reason_code"), row.get("error"))
    elif fact_type == "depth":
        status, reason = normalize_dex_depth_source_outcome(
            raw_status, row.get("reason_code"), row.get("error")
        )
    else:
        status, reason = normalize_tvl_source_outcome(
            raw_status, row.get("reason_code"), row.get("error")
        )
    rule = quality_outcome_rule(status, reason)
    return SnapshotFactState(
        market_id, fact_type, snapshot_id, dataset_sha256,
        _timestamp(row.get("observed_at")), status, reason,
        rule.retryable if rule is not None else False, generation,
        _target_fingerprint(row),
    )


def read_snapshot_fact_state(data_dir: Path, request: Dict[str, Any]) -> SnapshotFactState:
    """Read exactly one validated publication and select the canonical target."""
    token, fact_type, market_id, parsed = _parse_request(request)
    family = parsed[0]
    path = _publication_path(Path(data_dir), family, fact_type)
    dataset_sha256, headers, rows = _read_publication(path)
    missing = _required_for(family, fact_type).difference(headers)
    if missing:
        raise ValueError("publication is missing required columns")
    snapshot_ids = set()
    identities = set()
    tvl_pool_identities = set()
    selected = None
    target = parsed[1:] + (token,) if family == "cex" else parsed[1:]
    for row in rows:
        identity = _validate_row(row, family, fact_type)
        if identity in identities:
            raise ValueError("publication contains duplicate normalized identity")
        identities.add(identity)
        if family == "dex" and fact_type == "tvl":
            pool_identity = (identity[1], identity[3], identity[4])
            if pool_identity in tvl_pool_identities:
                raise ValueError("TVL publication contains conflicting pool identity")
            tvl_pool_identities.add(pool_identity)
        snapshot_ids.add(_text(row.get("snapshot_id"), MAX_SNAPSHOT_ID_LENGTH))
        if identity[1:] == target:
            selected = row
    if len(snapshot_ids) != 1 or not next(iter(snapshot_ids)):
        raise ValueError("publication must contain one nonempty snapshot identity")
    return _state_from_row(
        selected, token=token, market_id=market_id, fact_type=fact_type,
        family=family, snapshot_id=next(iter(snapshot_ids)), dataset_sha256=dataset_sha256,
    )


def evaluate_snapshot_refresh(
    before: SnapshotFactState,
    after: SnapshotFactState,
) -> SnapshotRefreshResult:
    if before.market_id != after.market_id or before.fact_type != after.fact_type:
        return SnapshotRefreshResult.failure("snapshot_target_mismatch", before=before, after=after)
    if not after.snapshot_id:
        return SnapshotRefreshResult.failure(
            "snapshot_publication_identity_invalid", before=before, after=after
        )
    if before.dataset_sha256 == after.dataset_sha256 or before.snapshot_id == after.snapshot_id:
        return SnapshotRefreshResult.failure("snapshot_publication_unchanged", before=before, after=after)
    if (
        before.target_fingerprint is not None
        and before.target_fingerprint == after.target_fingerprint
    ):
        return SnapshotRefreshResult.failure("snapshot_publication_unchanged", before=before, after=after)
    rule = quality_outcome_rule(after.status, after.reason_code)
    resolution_state = quality_outcome_resolution_state(
        after.status, after.reason_code
    )
    if (
        rule is not None
        and rule.terminal
        and not rule.retryable
        and after.retryable is rule.retryable
        and resolution_state in {"observed", "confirmed_terminal_absence"}
    ):
        return SnapshotRefreshResult.success(rule.resolution, before, after)
    return SnapshotRefreshResult.failure(
        "snapshot_target_unresolved", retryable=after.retryable, before=before, after=after
    )
