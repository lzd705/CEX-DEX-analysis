"""Validate persistent, manually checked market-lifecycle dispositions.

These records may resolve one exact ``stale_market_unknown`` issue into the
informational ``source_no_observation`` state. They are deliberately scoped to
one issue ID, market ID, and UTC date. A past review cannot silently classify a
future missing candle.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REVIEW_PATH = PROJECT_ROOT / "data/curated/market_lifecycle_reviews.json"
REVIEW_SCHEMA = "market_lifecycle_reviews/v1"
MAX_REVIEW_BYTES = 512 * 1024
MAX_REVISION_COUNT = 1_000
MAX_SOURCE_CHECKS = 8
REVIEW_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,95}$")
ISSUE_ID_PATTERN = re.compile(r"^[0-9a-f]{20}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
TOKEN_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
ALLOWED_REVIEW_STATUSES = {"disposed", "withdrawn"}
ALLOWED_MARKET_TYPES = {"cex", "dex"}
ALLOWED_LIFECYCLES = {
    "pool_exists_dormant",
    "listed_quote_market_dormant",
}
ALLOWED_EVIDENCE_STATUSES = {
    "declared_source_confirmed",
    "primary_confirmed",
}
ALLOWED_REVIEW_METHODS = {
    "manual_declared_source_cross_check",
    "manual_primary_source_cross_check",
}
ALLOWED_SOURCE_HOSTS = {
    "api.geckoterminal.com",
    "api.upbit.com",
}


class LifecycleReviewError(ValueError):
    """A curated lifecycle review is invalid or ambiguous."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _bounded_text(
    value: Any,
    *,
    field: str,
    maximum: int,
) -> str:
    if not isinstance(value, str):
        raise LifecycleReviewError("{} must be text".format(field))
    normalized = " ".join(value.split())
    if not normalized or len(normalized) > maximum:
        raise LifecycleReviewError("{} has an invalid length".format(field))
    return normalized


def _iso_day(value: Any, *, field: str) -> str:
    text = _bounded_text(value, field=field, maximum=10)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as error:
        raise LifecycleReviewError("{} is not an ISO date".format(field)) from error
    if parsed.isoformat() != text:
        raise LifecycleReviewError("{} is not canonical".format(field))
    return text


def _iso_timestamp(value: Any, *, field: str) -> str:
    text = _bounded_text(value, field=field, maximum=64)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise LifecycleReviewError(
            "{} is not an ISO timestamp".format(field)
        ) from error
    if parsed.tzinfo is None:
        raise LifecycleReviewError("{} must include a timezone".format(field))
    return text


def _source_check(
    value: Any,
    *,
    review_id: str,
    position: int,
) -> Dict[str, Any]:
    field = "{}.source_checks[{}]".format(review_id, position)
    if not isinstance(value, dict):
        raise LifecycleReviewError("{} must be an object".format(field))
    url = _bounded_text(value.get("url"), field=field + ".url", maximum=2_000)
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in ALLOWED_SOURCE_HOSTS
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise LifecycleReviewError(
            "{} is not an allowed official HTTPS source".format(field)
        )
    http_status = value.get("http_status")
    if (
        not isinstance(http_status, int)
        or isinstance(http_status, bool)
        or http_status < 200
        or http_status > 299
    ):
        raise LifecycleReviewError(
            "{} must contain a successful HTTP status".format(field)
        )
    response_sha256 = value.get("response_sha256")
    if (
        not isinstance(response_sha256, str)
        or not SHA256_PATTERN.fullmatch(response_sha256)
    ):
        raise LifecycleReviewError(
            "{} has an invalid response_sha256".format(field)
        )
    observations = value.get("observations")
    if not isinstance(observations, dict) or not observations:
        raise LifecycleReviewError(
            "{} must contain normalized observations".format(field)
        )
    try:
        encoded_observations = json.dumps(
            observations,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as error:
        raise LifecycleReviewError(
            "{} observations are not JSON serializable".format(field)
        ) from error
    if len(encoded_observations.encode("utf-8")) > 16 * 1024:
        raise LifecycleReviewError(
            "{} observations exceed the size limit".format(field)
        )
    return {
        "source_kind": _bounded_text(
            value.get("source_kind"),
            field=field + ".source_kind",
            maximum=64,
        ),
        "url": url,
        "http_status": http_status,
        "response_sha256": response_sha256,
        "checked_at_utc": _iso_timestamp(
            value.get("checked_at_utc"),
            field=field + ".checked_at_utc",
        ),
        "observations": observations,
    }


def _review_revision(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise LifecycleReviewError("Lifecycle review revision must be an object")
    review_id = _bounded_text(
        value.get("review_id"),
        field="review_id",
        maximum=96,
    )
    if not REVIEW_ID_PATTERN.fullmatch(review_id):
        raise LifecycleReviewError("review_id is not canonical")
    revision = value.get("revision")
    if (
        not isinstance(revision, int)
        or isinstance(revision, bool)
        or revision < 1
    ):
        raise LifecycleReviewError(
            "{} revision must be a positive integer".format(review_id)
        )
    review_status = value.get("review_status")
    if review_status not in ALLOWED_REVIEW_STATUSES:
        raise LifecycleReviewError(
            "{} has an unsupported review_status".format(review_id)
        )
    supersedes_revision = value.get("supersedes_revision")
    if revision == 1:
        if supersedes_revision is not None:
            raise LifecycleReviewError(
                "{} revision 1 cannot supersede another revision".format(review_id)
            )
    elif supersedes_revision != revision - 1:
        raise LifecycleReviewError(
            "{} revision {} must supersede revision {}".format(
                review_id,
                revision,
                revision - 1,
            )
        )

    reviewed_issue_id = value.get("reviewed_issue_id")
    if (
        not isinstance(reviewed_issue_id, str)
        or not ISSUE_ID_PATTERN.fullmatch(reviewed_issue_id)
    ):
        raise LifecycleReviewError(
            "{} has an invalid reviewed_issue_id".format(review_id)
        )
    market_id = _bounded_text(
        value.get("market_id"),
        field=review_id + ".market_id",
        maximum=512,
    )
    market_type = value.get("market_type")
    if market_type not in ALLOWED_MARKET_TYPES or not market_id.startswith(
        "{}:".format(market_type)
    ):
        raise LifecycleReviewError(
            "{} has an inconsistent market_type".format(review_id)
        )
    token_symbol = value.get("token_symbol")
    if (
        not isinstance(token_symbol, str)
        or not TOKEN_PATTERN.fullmatch(token_symbol)
    ):
        raise LifecycleReviewError(
            "{} has an invalid token_symbol".format(review_id)
        )
    issue_date = _iso_day(
        value.get("issue_date"),
        field=review_id + ".issue_date",
    )

    disposition_status = value.get("disposition_status")
    disposition_reason_code = value.get("disposition_reason_code")
    market_lifecycle = value.get("market_lifecycle")
    if review_status == "disposed":
        if (
            disposition_status != "source_no_observation"
            or disposition_reason_code != "no_candles"
            or market_lifecycle not in ALLOWED_LIFECYCLES
        ):
            raise LifecycleReviewError(
                "{} has an unsupported disposition".format(review_id)
            )
    else:
        if (
            disposition_status is not None
            or disposition_reason_code is not None
            or market_lifecycle is not None
        ):
            raise LifecycleReviewError(
                "{} withdrawn revision must not contain a disposition".format(
                    review_id
                )
            )

    raw_checks = value.get("source_checks")
    if (
        not isinstance(raw_checks, list)
        or not raw_checks
        or len(raw_checks) > MAX_SOURCE_CHECKS
    ):
        raise LifecycleReviewError(
            "{} has an invalid source_checks list".format(review_id)
        )
    source_checks = [
        _source_check(check, review_id=review_id, position=position)
        for position, check in enumerate(raw_checks)
    ]
    if len({check["url"] for check in source_checks}) != len(source_checks):
        raise LifecycleReviewError(
            "{} contains duplicate source URLs".format(review_id)
        )

    original_category = _bounded_text(
        value.get("original_category"),
        field=review_id + ".original_category",
        maximum=64,
    )
    original_reason_code = _bounded_text(
        value.get("original_reason_code"),
        field=review_id + ".original_reason_code",
        maximum=64,
    )
    if (
        original_category != "stale_market_unknown"
        or original_reason_code != "stale_market_lifecycle_unknown"
    ):
        raise LifecycleReviewError(
            "{} may resolve only a stale lifecycle issue".format(review_id)
        )
    evidence_status = _bounded_text(
        value.get("evidence_status"),
        field=review_id + ".evidence_status",
        maximum=64,
    )
    if evidence_status not in ALLOWED_EVIDENCE_STATUSES:
        raise LifecycleReviewError(
            "{} has an unsupported evidence_status".format(review_id)
        )
    review_method = _bounded_text(
        value.get("review_method"),
        field=review_id + ".review_method",
        maximum=128,
    )
    if review_method not in ALLOWED_REVIEW_METHODS:
        raise LifecycleReviewError(
            "{} has an unsupported review_method".format(review_id)
        )
    reviewed_at_utc = _iso_timestamp(
        value.get("reviewed_at_utc"),
        field=review_id + ".reviewed_at_utc",
    )
    reviewed_at = datetime.fromisoformat(
        reviewed_at_utc[:-1] + "+00:00"
        if reviewed_at_utc.endswith("Z")
        else reviewed_at_utc
    ).astimezone(timezone.utc)
    if date.fromisoformat(issue_date) > reviewed_at.date():
        raise LifecycleReviewError(
            "{} was reviewed before its issue date".format(review_id)
        )
    expected_host = (
        "api.geckoterminal.com"
        if market_type == "dex"
        else "api.upbit.com"
    )
    if any(
        urlsplit(check["url"]).hostname != expected_host
        for check in source_checks
    ):
        raise LifecycleReviewError(
            "{} source host does not match its market type".format(review_id)
        )
    for check in source_checks:
        checked_text = check["checked_at_utc"]
        checked_at = datetime.fromisoformat(
            checked_text[:-1] + "+00:00"
            if checked_text.endswith("Z")
            else checked_text
        ).astimezone(timezone.utc)
        if checked_at > reviewed_at:
            raise LifecycleReviewError(
                "{} contains evidence checked after review".format(review_id)
            )

    return {
        "review_id": review_id,
        "revision": revision,
        "supersedes_revision": supersedes_revision,
        "review_status": review_status,
        "reviewed_issue_id": reviewed_issue_id,
        "original_category": original_category,
        "original_reason_code": original_reason_code,
        "market_id": market_id,
        "market_type": market_type,
        "token_symbol": token_symbol,
        "issue_date": issue_date,
        "disposition_status": disposition_status,
        "disposition_reason_code": disposition_reason_code,
        "market_lifecycle": market_lifecycle,
        "evidence_status": evidence_status,
        "review_method": review_method,
        "review_actor": _bounded_text(
            value.get("review_actor"),
            field=review_id + ".review_actor",
            maximum=128,
        ),
        "reviewed_at_utc": reviewed_at_utc,
        "disposition_note": _bounded_text(
            value.get("disposition_note"),
            field=review_id + ".disposition_note",
            maximum=2_000,
        ),
        "source_checks": source_checks,
    }


def load_lifecycle_reviews(
    path: Optional[Path] = DEFAULT_REVIEW_PATH,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if path is None:
        return [], {
            "schema": REVIEW_SCHEMA,
            "status": "disabled",
            "source_name": None,
            "sha256": None,
            "revision_count": 0,
            "active_disposition_count": 0,
        }
    path = path.expanduser().resolve()
    if not path.exists():
        return [], {
            "schema": REVIEW_SCHEMA,
            "status": "absent",
            "source_name": path.name,
            "sha256": None,
            "revision_count": 0,
            "active_disposition_count": 0,
        }
    try:
        with path.open("rb") as handle:
            encoded = handle.read(MAX_REVIEW_BYTES + 1)
    except OSError as error:
        raise LifecycleReviewError(
            "Lifecycle review file cannot be read"
        ) from error
    if len(encoded) > MAX_REVIEW_BYTES:
        raise LifecycleReviewError("Lifecycle review file exceeds the size limit")
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LifecycleReviewError(
            "Lifecycle review file is not valid UTF-8 JSON"
        ) from error
    if not isinstance(payload, dict) or payload.get("schema") != REVIEW_SCHEMA:
        raise LifecycleReviewError("Lifecycle review schema is unsupported")
    _iso_timestamp(payload.get("generated_at_utc"), field="generated_at_utc")
    raw_reviews = payload.get("reviews")
    if (
        not isinstance(raw_reviews, list)
        or len(raw_reviews) > MAX_REVISION_COUNT
        or payload.get("review_count") != len(raw_reviews)
    ):
        raise LifecycleReviewError("Lifecycle review count is inconsistent")

    revisions = [_review_revision(item) for item in raw_reviews]
    by_review_id: Dict[str, Dict[int, Dict[str, Any]]] = {}
    for revision in revisions:
        review_revisions = by_review_id.setdefault(revision["review_id"], {})
        if revision["revision"] in review_revisions:
            raise LifecycleReviewError(
                "Lifecycle review contains a duplicate revision"
            )
        review_revisions[revision["revision"]] = revision
    latest = []
    for review_id, review_revisions in sorted(by_review_id.items()):
        expected = list(range(1, max(review_revisions) + 1))
        if sorted(review_revisions) != expected:
            raise LifecycleReviewError(
                "{} revisions are not contiguous".format(review_id)
            )
        latest.append(review_revisions[max(review_revisions)])

    active = [
        item for item in latest if item["review_status"] == "disposed"
    ]
    active_keys = [
        (
            item["reviewed_issue_id"],
            item["market_id"],
            item["issue_date"],
        )
        for item in active
    ]
    if len(active_keys) != len(set(active_keys)):
        raise LifecycleReviewError(
            "Lifecycle review has ambiguous active dispositions"
        )
    metadata = {
        "schema": REVIEW_SCHEMA,
        "status": "accepted",
        "source_name": path.name,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "revision_count": len(revisions),
        "review_id_count": len(by_review_id),
        "active_disposition_count": len(active),
    }
    return active, metadata


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate curated market lifecycle review dispositions"
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_REVIEW_PATH,
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        reviews, metadata = load_lifecycle_reviews(args.path)
    except (LifecycleReviewError, OSError, ValueError) as error:
        print(
            json.dumps(
                {
                    "schema": REVIEW_SCHEMA,
                    "status": "invalid",
                    "error": "{}: {}".format(type(error).__name__, error),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2
    print(
        json.dumps(
            {
                **metadata,
                "review_ids": [item["review_id"] for item in reviews],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
