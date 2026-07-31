"""Bounded public quality outcomes shared by dashboard facts."""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class QualityOutcomeRule:
    retryable: bool
    terminal: bool
    resolution: str


def _rule(retryable, terminal, resolution):
    return QualityOutcomeRule(
        retryable=retryable,
        terminal=terminal,
        resolution=resolution,
    )


_RULES = {
    ("observed", "observed"): _rule(False, True, "observed"),
    ("partial", "source_level_limit"): _rule(False, True, "partial"),
    ("partial", "measurement_limit"): _rule(False, True, "partial"),
    ("collection_failed", "network"): _rule(True, False, "retry_open"),
    ("collection_failed", "rate_limit"): _rule(True, False, "retry_open"),
    ("collection_failed", "source_unavailable"): _rule(True, False, "retry_open"),
    ("collection_failed", "parse"): _rule(True, False, "retry_open"),
    ("collection_failed", "validation"): _rule(True, False, "retry_open"),
    ("source_no_observation", "no_candles"): _rule(
        False, True, "confirmed_absence"
    ),
    ("source_no_observation", "source_no_two_sided_book"): _rule(
        False, True, "confirmed_absence"
    ),
    ("source_no_observation", "source_no_order_book"): _rule(
        False, True, "confirmed_absence"
    ),
    ("unsupported", "source_range_unavailable"): _rule(
        False, True, "confirmed_unsupported"
    ),
    ("unsupported", "unsupported_chain"): _rule(
        False, True, "confirmed_unsupported"
    ),
    ("unsupported", "unsupported_protocol"): _rule(
        False, True, "confirmed_unsupported"
    ),
    ("unsupported", "unsupported_method"): _rule(
        False, True, "confirmed_unsupported"
    ),
    ("unsupported", "unsupported_source"): _rule(
        False, True, "confirmed_unsupported"
    ),
    ("unsupported", "unsupported_protocol_or_chain"): _rule(
        False, True, "confirmed_unsupported"
    ),
    ("needs_review", "not_listed"): _rule(False, False, "manual_review"),
    ("needs_review", "stale_market_lifecycle_unknown"): _rule(
        False, False, "manual_review"
    ),
    ("needs_review", "source_rejected_request"): _rule(
        False, False, "manual_review"
    ),
    ("needs_review", "daily_quality_outcome_invalid"): _rule(
        False, False, "manual_review"
    ),
    ("backfill_pending", "missing_unexplained"): _rule(
        True, False, "retry_open"
    ),
    ("invalid", "invalid_positive_ohlc"): _rule(
        False, False, "blocked_invalid"
    ),
    ("invalid", "inconsistent_ohlc_bounds"): _rule(
        False, False, "blocked_invalid"
    ),
    ("invalid", "invalid_non_negative_volume"): _rule(
        False, False, "blocked_invalid"
    ),
    ("invalid", "invalid_non_negative_pool_tvl"): _rule(
        False, False, "blocked_invalid"
    ),
    ("invalid", "missing_market_identity"): _rule(
        False, False, "blocked_invalid"
    ),
    ("invalid", "invalid_date"): _rule(False, False, "blocked_invalid"),
    ("invalid", "incomplete_or_future_date"): _rule(
        False, False, "blocked_invalid"
    ),
    ("invalid", "duplicate_primary_key"): _rule(
        False, False, "blocked_invalid"
    ),
    ("invalid", "source_invalid_order_book"): _rule(
        False, False, "blocked_invalid"
    ),
}


def quality_outcome_rule(status, reason_code):
    """Return an exact allowlisted rule or fail closed for unknown pairs."""
    pair = (str(status or "").lower(), str(reason_code or "").lower())
    return _RULES.get(pair)


_CEX_LEGACY_ERROR_REASONS = (
    ("empty order-book side", "source_no_two_sided_book"),
    ("returned no order book", "source_no_order_book"),
    ("crossed or locked", "source_invalid_order_book"),
    ("invalid numeric order-book", "source_invalid_order_book"),
)
_CEX_REASON_CODES = {
    "observed",
    "source_level_limit",
    "measurement_limit",
    "network",
    "rate_limit",
    "source_unavailable",
    "parse",
    "validation",
    "source_no_two_sided_book",
    "source_no_order_book",
    "source_invalid_order_book",
    "not_listed",
    "source_rejected_request",
    "unsupported_source",
}
_DEX_UNSUPPORTED_PREFIXES = {
    "source_range_unavailable",
    "unsupported_chain",
    "unsupported_protocol",
    "unsupported_method",
    "unsupported_source",
    "unsupported_protocol_or_chain",
}


def classify_legacy_cex_error(error):
    """Translate known legacy CEX error text without publishing the text."""
    text = str(error or "").lower()
    for phrase, reason_code in _CEX_LEGACY_ERROR_REASONS:
        if phrase in text:
            return reason_code
    return None


def cex_reason_code(reason_code, error):
    """Use a bounded collector code or conservatively classify legacy text."""
    candidate = str(reason_code or "").strip().lower()
    if candidate in _CEX_REASON_CODES:
        return candidate
    return classify_legacy_cex_error(error)


def normalize_cex_source_outcome(status, reason_code, error):
    """Project CEX source results onto public status/reason pairs."""
    raw_status = str(status or "").strip().lower()
    reason = cex_reason_code(reason_code, error)
    if reason in {"source_no_two_sided_book", "source_no_order_book"}:
        return ("source_no_observation", reason)
    if reason == "source_invalid_order_book":
        return ("invalid", reason)
    if reason in {"network", "rate_limit", "source_unavailable", "parse", "validation"}:
        return ("collection_failed", reason)
    if reason == "observed" or raw_status == "observed":
        return ("observed", "observed")
    if reason in {"source_level_limit", "measurement_limit"} or raw_status == "partial":
        return ("partial", reason or "measurement_limit")
    if reason in {"not_listed", "source_rejected_request"}:
        return ("needs_review", reason)
    if reason == "unsupported_source":
        return ("unsupported", reason)
    return (raw_status, None)


def project_dex_unsupported_error(error):
    """Reduce DEX unsupported error prefixes to bounded public reasons."""
    text = str(error or "").strip().lower()
    prefix = text.split(":", 1)[0]
    return prefix if prefix in _DEX_UNSUPPORTED_PREFIXES else None
