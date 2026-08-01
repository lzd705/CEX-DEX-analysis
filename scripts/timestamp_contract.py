"""Strict RFC 3339 timestamp normalization shared by collectors and APIs."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from typing import Any, Iterable, Mapping, Optional, Tuple


RFC3339_TIMESTAMP = re.compile(
    r"(?P<prefix>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d+))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})"
)


def exact_rfc3339_epoch_seconds(value: str) -> Decimal:
    """Return an RFC 3339 UTC timestamp as Decimal epoch seconds."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be RFC 3339 text")
    matched = RFC3339_TIMESTAMP.fullmatch(value)
    if matched is None:
        raise ValueError("timestamp must be RFC 3339 with a timezone")
    offset = (
        "+00:00" if matched.group("offset") == "Z" else matched.group("offset")
    )
    try:
        parsed = datetime.fromisoformat(matched.group("prefix") + offset)
        parsed = parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as error:
        raise ValueError("timestamp must represent a valid RFC 3339 instant") from error
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = parsed - epoch
    whole_seconds = delta.days * 86_400 + delta.seconds
    fraction = matched.group("fraction") or ""
    if fraction:
        with localcontext() as context:
            context.prec = len(str(abs(whole_seconds))) + len(fraction) + 1
            return Decimal(whole_seconds) + Decimal("0." + fraction)
    return Decimal(whole_seconds)


def exact_timestamp_skew_seconds(left: str, right: str) -> Decimal:
    """Return the exact absolute RFC 3339 timestamp skew in seconds."""
    left_epoch = exact_rfc3339_epoch_seconds(left)
    right_epoch = exact_rfc3339_epoch_seconds(right)
    with localcontext() as context:
        context.prec = max(
            len(left_epoch.as_tuple().digits),
            len(right_epoch.as_tuple().digits),
        ) + 1
        return abs(left_epoch - right_epoch)


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
    try:
        parsed = datetime.fromisoformat(
            matched.group("prefix") + normalized_fraction + offset
        )
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as error:
        raise ValueError("timestamp must represent a valid RFC 3339 instant") from error


def canonical_rfc3339_utc(value: str) -> str:
    """Return one UTC microsecond-resolution representation of RFC 3339 text."""
    return parse_rfc3339_utc(value).isoformat()


_UNDECLARED = object()


def _strict_canonical_utc(value: Any, *, label: str) -> datetime:
    """Parse one timestamp without silently normalizing publication evidence."""
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(
            "{} must be a non-empty canonical UTC timestamp".format(label)
        )
    try:
        parsed = parse_rfc3339_utc(value)
    except (OverflowError, ValueError) as error:
        raise ValueError(
            "{} must be a timezone-aware canonical UTC timestamp".format(label)
        ) from error
    if parsed.isoformat() != value:
        raise ValueError("{} must use canonical UTC representation".format(label))
    return parsed


def validate_observation_bounds(
    rows: Iterable[Mapping[str, Any]],
    *,
    field: str = "observed_at",
    declared_min: Any = _UNDECLARED,
    declared_max: Any = _UNDECLARED,
) -> Tuple[Optional[str], Optional[str]]:
    """Validate one publication inventory and return its exact UTC bounds.

    A non-empty publication is a bounded sequential-observation cohort: every
    inventory member must carry its own canonical UTC timestamp. Optional
    declared bounds are accepted only when both are present, contain every
    member, and equal the actual minimum and maximum of this exact inventory.
    """
    inventory = list(rows)
    bounds_declared = (
        declared_min is not _UNDECLARED or declared_max is not _UNDECLARED
    )
    if not inventory:
        if not bounds_declared or (declared_min is None and declared_max is None):
            return None, None
        raise ValueError(
            "{} bounds cannot be declared for an empty inventory".format(field)
        )

    observations = [
        _strict_canonical_utc(
            row.get(field),
            label="cohort member {} {}".format(index, field),
        )
        for index, row in enumerate(inventory, start=1)
    ]
    actual_lower = min(observations)
    actual_upper = max(observations)
    actual_bounds = (actual_lower.isoformat(), actual_upper.isoformat())

    if not bounds_declared:
        return actual_bounds
    if declared_min is _UNDECLARED or declared_max is _UNDECLARED:
        raise ValueError("{} declared bounds are incomplete".format(field))
    lower = _strict_canonical_utc(
        declared_min,
        label="{} declared minimum".format(field),
    )
    upper = _strict_canonical_utc(
        declared_max,
        label="{} declared maximum".format(field),
    )
    if lower > upper:
        raise ValueError("{} declared bounds are reversed".format(field))
    if any(
        observation < lower or observation > upper
        for observation in observations
    ):
        raise ValueError(
            "{} cohort member falls outside declared bounds".format(field)
        )
    if (lower, upper) != (actual_lower, actual_upper):
        raise ValueError(
            "{} declared bounds do not exactly match the validated inventory".format(
                field
            )
        )
    return actual_bounds
