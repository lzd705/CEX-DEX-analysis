"""Strict RFC 3339 timestamp normalization shared by collectors and APIs."""

from __future__ import annotations

import re
from datetime import datetime, timezone


RFC3339_TIMESTAMP = re.compile(
    r"(?P<prefix>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d+))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})"
)


def parse_rfc3339_utc(value: str) -> datetime:
    """Parse RFC 3339 on Python 3.8, truncating sub-microsecond precision."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be RFC 3339 text")
    matched = RFC3339_TIMESTAMP.fullmatch(value)
    if matched is None:
        raise ValueError("timestamp must be RFC 3339 with a timezone")
    fraction = matched.group("fraction")
    normalized_fraction = ""
    if fraction:
        normalized_fraction = "." + (fraction + "000000")[:6]
    offset = (
        "+00:00"
        if matched.group("offset") == "Z"
        else matched.group("offset")
    )
    parsed = datetime.fromisoformat(
        matched.group("prefix") + normalized_fraction + offset
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def canonical_rfc3339_utc(value: str) -> str:
    """Return one UTC microsecond-resolution representation of RFC 3339 text."""
    return parse_rfc3339_utc(value).isoformat()
