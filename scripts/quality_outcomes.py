"""Bounded public quality outcomes shared by dashboard facts."""

import ipaddress
import re
from dataclasses import dataclass
from urllib.parse import urlsplit


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
    ("observed", "target_filled"): _rule(False, True, "observed"),
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
    ("source_no_observation", "source_no_tvl_observation"): _rule(
        False, True, "confirmed_absence"
    ),
    ("source_no_observation", "source_pool_not_found"): _rule(
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
    ("needs_review", "daily_audit_no_matching_issue"): _rule(
        False, False, "manual_review"
    ),
    ("legacy_ohlcv_snapshot", "legacy_ohlcv_snapshot"): _rule(
        False, True, "observed"
    ),
    ("unavailable", "tvl_snapshot_unavailable"): _rule(
        False, False, "unavailable"
    ),
    ("unavailable", "depth_snapshot_unavailable"): _rule(
        False, False, "unavailable"
    ),
    ("unavailable", "execution_snapshot_unavailable"): _rule(
        False, False, "unavailable"
    ),
    (
        "not_cataloged_in_snapshot",
        "tvl_market_not_cataloged_in_snapshot",
    ): _rule(True, False, "retry_open"),
    (
        "not_cataloged_in_snapshot",
        "depth_market_not_cataloged_in_snapshot",
    ): _rule(False, False, "unavailable"),
    (
        "not_cataloged_in_snapshot",
        "execution_market_not_cataloged_in_snapshot",
    ): _rule(True, False, "retry_open"),
    ("not_applicable", "cex_markets_do_not_have_pool_tvl"): _rule(
        False, True, "not_applicable"
    ),
    ("failed", "execution_snapshot_invalid"): _rule(
        True, False, "retry_open"
    ),
    ("failed", "execution_calculation_failed"): _rule(
        True, False, "retry_open"
    ),
    ("failed", "execution_usd_price_time_mismatch"): _rule(
        True, False, "retry_open"
    ),
    ("backfill_pending", "missing_unexplained"): _rule(
        True, False, "retry_open"
    ),
    (
        "backfill_pending",
        "missing_daily_observations_inside_observed_market_lifecycle",
    ): _rule(True, False, "retry_open"),
    (
        "backfill_pending",
        "missing_daily_observations_in_selected_window",
    ): _rule(True, False, "retry_open"),
    (
        "missing_unexplained",
        "no_daily_observations_after_latest_observed_market_date",
    ): _rule(True, False, "retry_open"),
    (
        "missing_unexplained",
        "no_daily_observations_in_selected_window",
    ): _rule(True, False, "retry_open"),
    (
        "not_applicable",
        "selected_window_before_first_market_observation",
    ): _rule(False, True, "not_applicable"),
    ("collection_failed", "multiple_daily_quality_reasons"): _rule(
        True, False, "retry_open"
    ),
    ("needs_review", "multiple_daily_quality_reasons"): _rule(
        False, False, "manual_review"
    ),
    ("backfill_pending", "multiple_daily_quality_reasons"): _rule(
        True, False, "retry_open"
    ),
    ("source_no_observation", "multiple_daily_quality_reasons"): _rule(
        False, True, "confirmed_absence"
    ),
    ("unsupported", "multiple_daily_quality_reasons"): _rule(
        False, True, "confirmed_unsupported"
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


def fail_closed_quality_outcome(status, reason_code):
    """Return one allowlisted pair or the bounded manual-review fallback."""
    pair = (
        str(status or "").strip().lower(),
        str(reason_code or "").strip().lower(),
    )
    if quality_outcome_rule(*pair) is not None:
        return pair
    return ("needs_review", "daily_quality_outcome_invalid")


def quality_outcome_resolution_state(status, reason_code):
    """Classify an exact pair for post-collection resolution decisions.

    Only an allowlisted observed pair or a terminal unsupported/source-absence
    pair can resolve a refresh.  Manual review, partial, invalid, retryable, and
    unknown pairs are deliberately unresolved.
    """
    normalized_status = str(status or "").strip().lower()
    rule = quality_outcome_rule(normalized_status, reason_code)
    if rule is None or not rule.terminal:
        return "unresolved"
    if normalized_status == "observed":
        return "observed"
    if normalized_status in {"source_no_observation", "unsupported"}:
        return "confirmed_terminal_absence"
    return "unresolved"


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
    "collection_failed",
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
    return (
        classify_legacy_cex_error(reason_code)
        or classify_legacy_cex_error(error)
    )


def normalize_cex_source_outcome(status, reason_code, error):
    """Project CEX source results onto public status/reason pairs."""
    raw_status = str(status or "").strip().lower()
    reason = cex_reason_code(reason_code, error)
    if raw_status in {"observed", "complete"}:
        return ("observed", "observed")
    if raw_status == "partial":
        return fail_closed_quality_outcome(
            "partial",
            reason if reason in {"source_level_limit", "measurement_limit"}
            else "source_level_limit",
        )
    if raw_status in {"failed", "error", "collection_failed"}:
        if reason in {"source_no_two_sided_book", "source_no_order_book"}:
            return ("source_no_observation", reason)
        if reason == "source_invalid_order_book":
            return ("invalid", reason)
        if reason in {
            "network",
            "rate_limit",
            "source_unavailable",
            "parse",
            "validation",
        }:
            return ("collection_failed", reason)
        if reason in {"not_listed", "source_rejected_request"}:
            return ("needs_review", reason)
        if reason == "unsupported_source":
            return ("unsupported", reason)
        return ("collection_failed", "source_unavailable")
    if raw_status == "source_no_observation" and reason in {
        "source_no_two_sided_book",
        "source_no_order_book",
    }:
        return (raw_status, reason)
    if raw_status == "unsupported" and reason == "unsupported_source":
        return (raw_status, reason)
    if raw_status == "needs_review" and reason in {
        "not_listed",
        "source_rejected_request",
    }:
        return (raw_status, reason)
    if raw_status == "invalid" and reason == "source_invalid_order_book":
        return (raw_status, reason)
    if raw_status == "unavailable":
        return ("unavailable", "depth_snapshot_unavailable")
    if raw_status == "not_cataloged_in_snapshot":
        return (
            "not_cataloged_in_snapshot",
            "depth_market_not_cataloged_in_snapshot",
        )
    return fail_closed_quality_outcome(raw_status, reason)


def project_dex_unsupported_error(error):
    """Reduce DEX unsupported error prefixes to bounded public reasons."""
    text = str(error or "").strip().lower()
    prefix = text.split(":", 1)[0]
    return prefix if prefix in _DEX_UNSUPPORTED_PREFIXES else None


def normalize_tvl_source_outcome(status, reason_code=None, error=None):
    """Map TVL collector states to bounded public quality outcomes."""
    del error
    raw_status = str(status or "").strip().lower()
    raw_reason = str(reason_code or "").strip().lower()
    if raw_status == "observed":
        pair = ("observed", "observed")
    elif raw_status == "legacy_ohlcv_snapshot":
        pair = (raw_status, "legacy_ohlcv_snapshot")
    elif raw_status == "missing":
        pair = ("source_no_observation", "source_no_tvl_observation")
    elif raw_status == "not_found":
        pair = ("source_no_observation", "source_pool_not_found")
    elif raw_status == "source_no_observation" and raw_reason in {
        "source_no_tvl_observation",
        "source_pool_not_found",
    }:
        pair = (raw_status, raw_reason)
    elif raw_status in {"failed", "error", "collection_failed"}:
        pair = ("collection_failed", "source_unavailable")
    elif raw_status == "unavailable":
        pair = ("unavailable", "tvl_snapshot_unavailable")
    elif raw_status == "not_cataloged_in_snapshot":
        pair = (
            "not_cataloged_in_snapshot",
            "tvl_market_not_cataloged_in_snapshot",
        )
    else:
        pair = (raw_status, None)
    return fail_closed_quality_outcome(*pair)


def normalize_dex_depth_source_outcome(status, reason_code=None, error=None):
    """Map DEX-depth collector states to bounded public quality outcomes."""
    raw_status = str(status or "").strip().lower()
    if raw_status in {"observed", "complete"}:
        pair = ("observed", "observed")
    elif raw_status == "partial":
        pair = ("partial", "measurement_limit")
    elif raw_status in {
        "unsupported",
        "unsupported_chain",
        "unsupported_protocol",
        "unsupported_method",
        "unsupported_source",
        "unsupported_protocol_or_chain",
    }:
        bounded_reason = (
            project_dex_unsupported_error(reason_code)
            or project_dex_unsupported_error(error)
            or (
                raw_status
                if raw_status in _DEX_UNSUPPORTED_PREFIXES
                else None
            )
        )
        pair = ("unsupported", bounded_reason or "unsupported_source")
    elif raw_status in {"failed", "error", "collection_failed"}:
        pair = ("collection_failed", "source_unavailable")
    elif raw_status == "unavailable":
        pair = ("unavailable", "depth_snapshot_unavailable")
    elif raw_status == "not_cataloged_in_snapshot":
        pair = (
            "not_cataloged_in_snapshot",
            "depth_market_not_cataloged_in_snapshot",
        )
    else:
        pair = (raw_status, reason_code)
    return fail_closed_quality_outcome(*pair)


_EXECUTION_FAILED_REASONS = {
    "execution_calculation_failed",
    "execution_snapshot_invalid",
    "execution_usd_price_time_mismatch",
}


def normalize_execution_source_outcome(
    market_type,
    status,
    reason_code=None,
    error=None,
):
    """Map execution rows and aggregate facts to bounded public outcomes."""
    family = str(market_type or "").strip().lower()
    raw_status = str(status or "").strip().lower()
    raw_reason = str(reason_code or "").strip().lower()
    if family == "cex":
        cex_reason = cex_reason_code(reason_code, error)
        if cex_reason in {
            "source_no_two_sided_book",
            "source_no_order_book",
            "source_invalid_order_book",
            "network",
            "rate_limit",
            "source_unavailable",
            "parse",
            "validation",
            "not_listed",
            "source_rejected_request",
            "unsupported_source",
        }:
            return normalize_cex_source_outcome(
                raw_status,
                cex_reason,
                error,
            )
    if raw_status == "observed":
        pair = (
            "observed",
            "target_filled" if raw_reason == "target_filled" else "observed",
        )
    elif raw_status == "partial":
        pair = (
            "partial",
            raw_reason
            if raw_reason in {"source_level_limit", "measurement_limit"}
            else "measurement_limit",
        )
    elif raw_status == "unsupported":
        bounded_reason = (
            project_dex_unsupported_error(raw_reason)
            or project_dex_unsupported_error(error)
            or "unsupported_source"
        )
        pair = ("unsupported", bounded_reason)
    elif raw_status == "failed":
        pair = (
            "failed",
            raw_reason
            if raw_reason in _EXECUTION_FAILED_REASONS
            else "execution_calculation_failed",
        )
    elif raw_status == "unavailable":
        pair = ("unavailable", "execution_snapshot_unavailable")
    elif raw_status == "not_cataloged_in_snapshot":
        pair = (
            "not_cataloged_in_snapshot",
            "execution_market_not_cataloged_in_snapshot",
        )
    else:
        pair = (raw_status, raw_reason)
    return fail_closed_quality_outcome(*pair)


_DAILY_QUALITY_FACT_OUTCOMES = frozenset(
    {
        ("observed", "observed"),
        ("collection_failed", "network"),
        ("collection_failed", "rate_limit"),
        ("collection_failed", "source_unavailable"),
        ("collection_failed", "parse"),
        ("collection_failed", "validation"),
        ("collection_failed", "multiple_daily_quality_reasons"),
        ("needs_review", "multiple_daily_quality_reasons"),
        ("backfill_pending", "multiple_daily_quality_reasons"),
        ("source_no_observation", "multiple_daily_quality_reasons"),
        ("unsupported", "multiple_daily_quality_reasons"),
        ("source_no_observation", "no_candles"),
        ("unsupported", "source_range_unavailable"),
        ("needs_review", "not_listed"),
        ("needs_review", "stale_market_lifecycle_unknown"),
        ("needs_review", "source_rejected_request"),
        ("needs_review", "daily_quality_outcome_invalid"),
        ("needs_review", "daily_audit_no_matching_issue"),
        ("backfill_pending", "missing_unexplained"),
        (
            "backfill_pending",
            "missing_daily_observations_inside_observed_market_lifecycle",
        ),
        (
            "backfill_pending",
            "missing_daily_observations_in_selected_window",
        ),
        (
            "missing_unexplained",
            "no_daily_observations_after_latest_observed_market_date",
        ),
        (
            "missing_unexplained",
            "no_daily_observations_in_selected_window",
        ),
        (
            "not_applicable",
            "selected_window_before_first_market_observation",
        ),
    }
)
_FAIL_CLOSED_FACT_OUTCOME = (
    "needs_review",
    "daily_quality_outcome_invalid",
)
_DEX_TVL_FACT_OUTCOMES = frozenset(
    {
        ("observed", "observed"),
        ("legacy_ohlcv_snapshot", "legacy_ohlcv_snapshot"),
        ("source_no_observation", "source_no_tvl_observation"),
        ("source_no_observation", "source_pool_not_found"),
        ("collection_failed", "source_unavailable"),
        ("unavailable", "tvl_snapshot_unavailable"),
        (
            "not_cataloged_in_snapshot",
            "tvl_market_not_cataloged_in_snapshot",
        ),
        _FAIL_CLOSED_FACT_OUTCOME,
    }
)
_CEX_DEPTH_FACT_OUTCOMES = frozenset(
    {
        ("observed", "observed"),
        ("partial", "source_level_limit"),
        ("partial", "measurement_limit"),
        ("collection_failed", "network"),
        ("collection_failed", "rate_limit"),
        ("collection_failed", "source_unavailable"),
        ("collection_failed", "parse"),
        ("collection_failed", "validation"),
        ("source_no_observation", "source_no_two_sided_book"),
        ("source_no_observation", "source_no_order_book"),
        ("unsupported", "unsupported_source"),
        ("needs_review", "not_listed"),
        ("needs_review", "source_rejected_request"),
        ("invalid", "source_invalid_order_book"),
        ("unavailable", "depth_snapshot_unavailable"),
        (
            "not_cataloged_in_snapshot",
            "depth_market_not_cataloged_in_snapshot",
        ),
        _FAIL_CLOSED_FACT_OUTCOME,
    }
)
_DEX_DEPTH_FACT_OUTCOMES = frozenset(
    {
        ("observed", "observed"),
        ("partial", "measurement_limit"),
        ("collection_failed", "source_unavailable"),
        ("unsupported", "source_range_unavailable"),
        ("unsupported", "unsupported_chain"),
        ("unsupported", "unsupported_protocol"),
        ("unsupported", "unsupported_method"),
        ("unsupported", "unsupported_source"),
        ("unsupported", "unsupported_protocol_or_chain"),
        ("unavailable", "depth_snapshot_unavailable"),
        (
            "not_cataloged_in_snapshot",
            "depth_market_not_cataloged_in_snapshot",
        ),
        _FAIL_CLOSED_FACT_OUTCOME,
    }
)
_EXECUTION_BASE_FACT_OUTCOMES = frozenset(
    {
        ("observed", "observed"),
        ("observed", "target_filled"),
        ("partial", "source_level_limit"),
        ("partial", "measurement_limit"),
        ("failed", "execution_snapshot_invalid"),
        ("failed", "execution_calculation_failed"),
        ("failed", "execution_usd_price_time_mismatch"),
        ("unavailable", "execution_snapshot_unavailable"),
        (
            "not_cataloged_in_snapshot",
            "execution_market_not_cataloged_in_snapshot",
        ),
        _FAIL_CLOSED_FACT_OUTCOME,
    }
)
_CEX_EXECUTION_FACT_OUTCOMES = frozenset(
    set(_EXECUTION_BASE_FACT_OUTCOMES)
    | {
        ("collection_failed", "network"),
        ("collection_failed", "rate_limit"),
        ("collection_failed", "source_unavailable"),
        ("collection_failed", "parse"),
        ("collection_failed", "validation"),
        ("collection_failed", "multiple_daily_quality_reasons"),
        ("source_no_observation", "source_no_two_sided_book"),
        ("source_no_observation", "source_no_order_book"),
        ("unsupported", "unsupported_source"),
        ("needs_review", "not_listed"),
        ("needs_review", "source_rejected_request"),
        ("invalid", "source_invalid_order_book"),
    }
)
_DEX_EXECUTION_FACT_OUTCOMES = frozenset(
    set(_EXECUTION_BASE_FACT_OUTCOMES)
    | {
        ("collection_failed", "network"),
        ("collection_failed", "rate_limit"),
        ("collection_failed", "source_unavailable"),
        ("collection_failed", "parse"),
        ("collection_failed", "validation"),
        ("collection_failed", "multiple_daily_quality_reasons"),
        ("unsupported", "source_range_unavailable"),
        ("unsupported", "unsupported_chain"),
        ("unsupported", "unsupported_protocol"),
        ("unsupported", "unsupported_method"),
        ("unsupported", "unsupported_source"),
        ("unsupported", "unsupported_protocol_or_chain"),
    }
)


def canonical_quality_fact_rule(
    market_type,
    fact_name,
    status,
    reason_code,
):
    """Return a rule only when the pair is possible for this fact family."""
    family = str(market_type or "").strip().lower()
    fact = str(fact_name or "").strip().lower()
    pair = (
        str(status or "").strip().lower(),
        str(reason_code or "").strip().lower(),
    )
    rule = quality_outcome_rule(*pair)
    if family not in {"cex", "dex"} or fact not in {
        "daily",
        "tvl",
        "depth",
        "execution",
    } or rule is None:
        return None
    if fact == "daily":
        allowed = _DAILY_QUALITY_FACT_OUTCOMES
    elif fact == "tvl" and family == "cex":
        allowed = frozenset(
            {
                (
                    "not_applicable",
                    "cex_markets_do_not_have_pool_tvl",
                ),
                _FAIL_CLOSED_FACT_OUTCOME,
            }
        )
    elif fact == "tvl":
        allowed = _DEX_TVL_FACT_OUTCOMES
    elif fact == "depth" and family == "cex":
        allowed = _CEX_DEPTH_FACT_OUTCOMES
    elif fact == "depth":
        allowed = _DEX_DEPTH_FACT_OUTCOMES
    elif family == "cex":
        allowed = _CEX_EXECUTION_FACT_OUTCOMES
    else:
        allowed = _DEX_EXECUTION_FACT_OUTCOMES
    return rule if pair in allowed else None


def canonical_quality_fact_action(
    market_type,
    fact_name,
    status,
    reason_code,
    retryable,
    *,
    daily_evidence_mode=None,
    manual_review_present=False,
):
    """Derive the one public action from the same fact-specific contract."""
    fact = str(fact_name or "").strip().lower()
    normalized_status = str(status or "").strip().lower()
    rule = canonical_quality_fact_rule(
        market_type,
        fact,
        normalized_status,
        reason_code,
    )
    if rule is None or bool(retryable) is not rule.retryable:
        raise ValueError("quality fact outcome is not canonical")
    if fact == "daily":
        if type(manual_review_present) is not bool:
            raise ValueError("daily manual-review evidence is invalid")
        if rule.retryable:
            return (
                "operator_review_retry_and_manual_queues"
                if manual_review_present
                else "operator_review_retry_queue"
            )
        if normalized_status == "needs_review":
            return "operator_manual_review"
        if normalized_status in {"source_no_observation", "unsupported"}:
            return "operator_review_source_outcome"
        return None
    if rule.retryable:
        return "retry_{}_collection".format(fact)
    if normalized_status == "needs_review":
        return "operator_manual_review"
    return None


def sanitize_public_source_endpoint(endpoint):
    """Return only a safe HTTP(S) origin; never retain credentials or paths."""
    if endpoint is None:
        return None
    text = str(endpoint)
    if (
        not text
        or text != text.strip()
        or len(text) > 2048
        or "\\" in text
        or any(character.isspace() or ord(character) < 32 or ord(character) == 127
               for character in text)
    ):
        return None
    try:
        parsed = urlsplit(text)
        scheme = parsed.scheme.lower()
        host = parsed.hostname
        port = parsed.port
    except (TypeError, ValueError):
        return None
    if scheme not in {"http", "https"} or not host:
        return None
    normalized_host = host.lower().rstrip(".")
    try:
        address = ipaddress.ip_address(normalized_host)
    except ValueError:
        try:
            normalized_host = normalized_host.encode("idna").decode("ascii")
        except (UnicodeError, ValueError):
            return None
        if all(
            re.fullmatch(r"(?:0x[0-9a-f]+|[0-9]+)", label)
            for label in normalized_host.split(".")
        ):
            # Legacy IPv4 parsers accept shortened, octal, and hexadecimal
            # literals that ipaddress intentionally rejects.  Never project
            # those ambiguous numeric spellings as if they were DNS names.
            return None
        if (
            len(normalized_host) > 253
            or not re.fullmatch(r"[a-z0-9.-]+", normalized_host)
            or any(
                not label
                or len(label) > 63
                or label.startswith("-")
                or label.endswith("-")
                for label in normalized_host.split(".")
            )
            or "." not in normalized_host
            or normalized_host == "localhost"
            or normalized_host.endswith(".localhost")
            or normalized_host.endswith(".local")
        ):
            return None
    else:
        if (
            not address.is_global
            or address.is_loopback
            or address.is_private
            or address.is_link_local
            or address.is_reserved
            or address.is_unspecified
            or address.is_multicast
        ):
            return None
        if address.version == 6:
            normalized_host = "[{}]".format(address.compressed)
        else:
            normalized_host = address.compressed
    return "{}://{}{}".format(
        scheme,
        normalized_host,
        ":{}".format(port) if port is not None else "",
    )
