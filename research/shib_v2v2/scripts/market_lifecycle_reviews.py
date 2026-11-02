"""Validate persistent, manually checked market-lifecycle dispositions.

These records may resolve one exact ``stale_market_unknown`` issue into the
informational ``source_no_observation`` state. They are deliberately scoped to
one issue ID, market ID, and UTC date. A past review cannot silently classify a
future missing candle.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REVIEW_PATH = PROJECT_ROOT / "data/curated/market_lifecycle_reviews.json"
TOKEN_CHAIN_CONFIG_PATH = PROJECT_ROOT / "config/token_chains.csv"
REVIEW_SCHEMA = "market_lifecycle_reviews/v1"
MAX_REVIEW_BYTES = 512 * 1024
MAX_REVISION_COUNT = 1_000
MAX_SOURCE_CHECKS = 8
MAX_INVENTORY_MARKET_CODES = 1_000
REVIEW_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,95}$")
ISSUE_ID_PATTERN = re.compile(r"^[0-9a-f]{20}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
TOKEN_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
UPBIT_MARKET_CODE_PATTERN = re.compile(
    r"^[A-Z0-9][A-Z0-9._]{0,31}-[A-Z0-9][A-Z0-9._]{0,31}$"
)
LOWER_COMPONENT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
EVM_ADDRESS_PATTERN = re.compile(r"^0x[0-9a-fA-F]{40}$")
UTC_TIMESTAMP_PATTERN = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)
PRINTABLE_ASCII_URL_PATTERN = re.compile(r"^[\x21-\x7e]+$")
UTC_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
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
ROOT_FIELDS = {
    "schema",
    "generated_at_utc",
    "review_count",
    "reviews",
}
REVISION_FIELDS = {
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
SOURCE_CHECK_FIELDS = {
    "source_kind",
    "url",
    "http_status",
    "response_sha256",
    "checked_at_utc",
    "observations",
}
UPBIT_TICKER_SOURCE = "official_exchange_ticker"
UPBIT_INVENTORY_SOURCE = "official_exchange_market_inventory"
DEX_POOL_SOURCE = "declared_dex_market_data_api"
DEX_OHLCV_SOURCE = "declared_dex_daily_ohlcv_api"
IMMUTABLE_REVISION_FIELDS = (
    "reviewed_issue_id",
    "original_category",
    "original_reason_code",
    "market_id",
    "market_type",
    "token_symbol",
    "issue_date",
)
TOKEN_CHAIN_CONFIG_FIELDS = (
    "token_symbol",
    "chain",
    "contract_address",
    "notes",
)


class LifecycleReviewError(ValueError):
    """A curated lifecycle review is invalid or ambiguous."""


def _require_exact_keys(
    value: Mapping[str, Any],
    expected: Sequence[str],
    *,
    field: str,
) -> None:
    if set(value) != set(expected):
        raise LifecycleReviewError("{} contains unknown or missing fields".format(field))


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
    if not isinstance(value, str) or not UTC_TIMESTAMP_PATTERN.fullmatch(value):
        raise LifecycleReviewError(
            "{} must use canonical UTC timestamp grammar".format(field)
        )
    try:
        parsed = datetime.strptime(value, UTC_TIMESTAMP_FORMAT)
    except ValueError as error:
        raise LifecycleReviewError(
            "{} is not a valid UTC timestamp".format(field)
        ) from error
    if parsed.strftime(UTC_TIMESTAMP_FORMAT) != value:
        raise LifecycleReviewError("{} is not canonical".format(field))
    return value


def _utc_timestamp(value: str) -> datetime:
    return datetime.strptime(value, UTC_TIMESTAMP_FORMAT).replace(
        tzinfo=timezone.utc
    )


def _bounded_ascii_url(value: Any, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 2_000
        or not PRINTABLE_ASCII_URL_PATTERN.fullmatch(value)
    ):
        raise LifecycleReviewError(
            "{} must be unchanged bounded printable ASCII".format(field)
        )
    return value


def _canonical_token(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not TOKEN_PATTERN.fullmatch(value):
        raise LifecycleReviewError("{} is not a canonical symbol".format(field))
    return value


def _canonical_lower_component(value: str, *, field: str) -> str:
    if not LOWER_COMPONENT_PATTERN.fullmatch(value):
        raise LifecycleReviewError("{} is not canonical".format(field))
    return value


def _normalized_evm_address(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not EVM_ADDRESS_PATTERN.fullmatch(value):
        raise LifecycleReviewError("{} is not an EVM pool address".format(field))
    return value.lower()


def _trusted_token_contracts(path: Path) -> Dict[Tuple[str, str], str]:
    try:
        with Path(path).expanduser().resolve().open(
            "r",
            newline="",
            encoding="utf-8",
        ) as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != TOKEN_CHAIN_CONFIG_FIELDS:
                raise LifecycleReviewError(
                    "Trusted Token contract configuration has invalid columns"
                )
            rows = list(reader)
    except (OSError, TypeError, ValueError) as error:
        if isinstance(error, LifecycleReviewError):
            raise
        raise LifecycleReviewError(
            "Trusted Token contract configuration cannot be read"
        ) from error

    contracts: Dict[Tuple[str, str], str] = {}
    for position, row in enumerate(rows, start=2):
        field = "token_chains.csv row {}".format(position)
        if set(row) != set(TOKEN_CHAIN_CONFIG_FIELDS):
            raise LifecycleReviewError("{} has invalid columns".format(field))
        token = _canonical_token(
            row.get("token_symbol"),
            field=field + ".token_symbol",
        )
        chain_value = row.get("chain")
        if not isinstance(chain_value, str):
            raise LifecycleReviewError("{}.chain must be text".format(field))
        chain = _canonical_lower_component(
            chain_value,
            field=field + ".chain",
        )
        contract = row.get("contract_address")
        if (
            not isinstance(contract, str)
            or not contract
            or contract != contract.strip()
            or len(contract) > 128
        ):
            raise LifecycleReviewError(
                "{}.contract_address is invalid".format(field)
            )
        key = (token, chain)
        if key in contracts:
            raise LifecycleReviewError(
                "Trusted Token contract configuration contains duplicates"
            )
        contracts[key] = contract
    return contracts


def _geckoterminal_evm_token_id(
    value: Any,
    *,
    chain: str,
    field: str,
) -> str:
    if not isinstance(value, str):
        raise LifecycleReviewError(
            "{} must be a GeckoTerminal Token id".format(field)
        )
    source_chain, separator, source_address = value.partition("_")
    if not separator or source_chain != chain:
        raise LifecycleReviewError(
            "{} does not match the reviewed network".format(field)
        )
    address = _normalized_evm_address(source_address, field=field + ".address")
    normalized = "{}_{}".format(chain, address)
    if value != normalized:
        raise LifecycleReviewError("{} is not canonical".format(field))
    return normalized


def _parse_market_identity(
    market_id: str,
    *,
    market_type: str,
    token_symbol: str,
    review_id: str,
) -> Dict[str, str]:
    field = review_id + ".market_id"
    parts = market_id.split(":")
    if market_type == "cex":
        if len(parts) != 3 or parts[0] != "cex":
            raise LifecycleReviewError(
                "{} is not a canonical CEX market identity".format(field)
            )
        exchange = _canonical_lower_component(parts[1], field=field + ".exchange")
        instrument_parts = parts[2].split("/")
        if len(instrument_parts) != 2:
            raise LifecycleReviewError(
                "{} instrument is not BASE/QUOTE".format(field)
            )
        base = _canonical_token(instrument_parts[0], field=field + ".base")
        quote = _canonical_token(instrument_parts[1], field=field + ".quote")
        if base != token_symbol:
            raise LifecycleReviewError(
                "{} base does not match token_symbol".format(field)
            )
        if exchange != "upbit":
            raise LifecycleReviewError(
                "{} has no declared lifecycle evidence adapter".format(field)
            )
        return {
            "exchange": exchange,
            "base": base,
            "quote": quote,
            "source_market": "{}-{}".format(quote, base),
        }

    if market_type == "dex":
        if len(parts) != 5 or parts[0] != "dex":
            raise LifecycleReviewError(
                "{} is not a canonical DEX market identity".format(field)
            )
        chain = _canonical_lower_component(parts[1], field=field + ".chain")
        dex = _canonical_lower_component(parts[2], field=field + ".dex")
        pool = _normalized_evm_address(parts[3], field=field + ".pool")
        if parts[3] != pool:
            raise LifecycleReviewError(
                "{} pool address is not normalized".format(field)
            )
        trailing_token = _canonical_token(parts[4], field=field + ".token")
        if trailing_token != token_symbol:
            raise LifecycleReviewError(
                "{} token does not match token_symbol".format(field)
            )
        return {
            "chain": chain,
            "dex": dex,
            "pool": pool,
            "token": trailing_token,
        }

    raise LifecycleReviewError("{} has an unsupported market type".format(field))


def _safe_official_url(url: str, *, field: str):
    _bounded_ascii_url(url, field=field)
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as error:
        raise LifecycleReviewError("{} has an invalid port".format(field)) from error
    if (
        parsed.scheme != "https"
        or parsed.hostname not in ALLOWED_SOURCE_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.fragment
        or parsed.netloc != parsed.hostname
        or "%" in parsed.path
        or "%" in parsed.query
    ):
        raise LifecycleReviewError(
            "{} is not an allowed official HTTPS source".format(field)
        )
    return parsed


def _exact_query(url: str, *, field: str) -> List[Tuple[str, str]]:
    try:
        return parse_qsl(
            urlsplit(url).query,
            keep_blank_values=True,
            strict_parsing=True,
        )
    except ValueError as error:
        raise LifecycleReviewError("{} has an invalid query".format(field)) from error


def _source_check(
    value: Any,
    *,
    review_id: str,
    position: int,
) -> Dict[str, Any]:
    field = "{}.source_checks[{}]".format(review_id, position)
    if not isinstance(value, dict):
        raise LifecycleReviewError("{} must be an object".format(field))
    _require_exact_keys(value, SOURCE_CHECK_FIELDS, field=field)
    url = _bounded_ascii_url(value.get("url"), field=field + ".url")
    _safe_official_url(url, field=field + ".url")
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
    source_kind = _bounded_text(
        value.get("source_kind"),
        field=field + ".source_kind",
        maximum=64,
    )
    return {
        "source_kind": source_kind,
        "url": url,
        "http_status": http_status,
        "response_sha256": response_sha256,
        "checked_at_utc": _iso_timestamp(
            value.get("checked_at_utc"),
            field=field + ".checked_at_utc",
        ),
        "observations": observations,
    }


def _checks_by_kind(
    source_checks: Sequence[Mapping[str, Any]],
    *,
    review_id: str,
    allowed: Sequence[str],
) -> Dict[str, Mapping[str, Any]]:
    allowed_set = set(allowed)
    by_kind: Dict[str, Mapping[str, Any]] = {}
    for check in source_checks:
        source_kind = str(check["source_kind"])
        if source_kind not in allowed_set:
            raise LifecycleReviewError(
                "{} contains an unsupported source_kind".format(review_id)
            )
        if source_kind in by_kind:
            raise LifecycleReviewError(
                "{} contains a duplicate source_kind".format(review_id)
            )
        by_kind[source_kind] = check
    return by_kind


def _upbit_market_codes(value: Any, *, field: str) -> List[str]:
    if (
        not isinstance(value, list)
        or len(value) > MAX_INVENTORY_MARKET_CODES
        or any(
            not isinstance(code, str)
            or not UPBIT_MARKET_CODE_PATTERN.fullmatch(code)
            for code in value
        )
        or len(value) != len(set(value))
    ):
        raise LifecycleReviewError(
            "{} must contain bounded unique canonical market codes".format(field)
        )
    return list(value)


def _validate_upbit_evidence(
    source_checks: Sequence[Mapping[str, Any]],
    *,
    identity: Mapping[str, str],
    issue_date: str,
    review_id: str,
) -> None:
    checks = _checks_by_kind(
        source_checks,
        review_id=review_id,
        allowed=(UPBIT_TICKER_SOURCE, UPBIT_INVENTORY_SOURCE),
    )
    ticker = checks.get(UPBIT_TICKER_SOURCE)
    if ticker is None:
        raise LifecycleReviewError(
            "{} requires an exact Upbit ticker check".format(review_id)
        )
    expected_market = identity["source_market"]
    ticker_url = str(ticker["url"])
    ticker_parts = _safe_official_url(
        ticker_url,
        field=review_id + ".ticker.url",
    )
    if (
        ticker_parts.hostname != "api.upbit.com"
        or ticker_parts.path != "/v1/ticker"
        or _exact_query(ticker_url, field=review_id + ".ticker.url")
        != [("markets", expected_market)]
    ):
        raise LifecycleReviewError(
            "{} ticker does not query the exact market".format(review_id)
        )
    ticker_observations = ticker["observations"]
    if ticker_observations.get("market") != expected_market:
        raise LifecycleReviewError(
            "{} ticker observation does not match the exact market".format(
                review_id
            )
        )
    last_trade_day = _iso_day(
        ticker_observations.get("last_trade_date_utc"),
        field=review_id + ".ticker.last_trade_date_utc",
    )
    if date.fromisoformat(last_trade_day) >= date.fromisoformat(issue_date):
        raise LifecycleReviewError(
            "{} ticker trade date does not precede issue_date".format(review_id)
        )

    inventory = checks.get(UPBIT_INVENTORY_SOURCE)
    if inventory is None:
        return
    inventory_url = str(inventory["url"])
    inventory_parts = _safe_official_url(
        inventory_url,
        field=review_id + ".inventory.url",
    )
    if (
        inventory_parts.hostname != "api.upbit.com"
        or inventory_parts.path != "/v1/market/all"
        or _exact_query(inventory_url, field=review_id + ".inventory.url")
        != [("is_details", "true")]
    ):
        raise LifecycleReviewError(
            "{} inventory does not use the declared endpoint".format(review_id)
        )
    inventory_observations = inventory["observations"]
    present_codes = _upbit_market_codes(
        inventory_observations.get("present_market_codes"),
        field=review_id + ".inventory.present_market_codes",
    )
    absent_codes: List[str] = []
    if "absent_market_codes" in inventory_observations:
        absent_codes = _upbit_market_codes(
            inventory_observations.get("absent_market_codes"),
            field=review_id + ".inventory.absent_market_codes",
        )
    if (
        expected_market not in present_codes
        or expected_market in absent_codes
        or set(present_codes).intersection(absent_codes)
    ):
        raise LifecycleReviewError(
            "{} inventory has a contradictory exact target state".format(
                review_id
            )
        )


def _validate_dex_evidence(
    source_checks: Sequence[Mapping[str, Any]],
    *,
    identity: Mapping[str, str],
    issue_date: str,
    review_id: str,
    trusted_token_contracts: Mapping[Tuple[str, str], str],
) -> bool:
    checks = _checks_by_kind(
        source_checks,
        review_id=review_id,
        allowed=(DEX_POOL_SOURCE, DEX_OHLCV_SOURCE),
    )
    if set(checks) != {DEX_POOL_SOURCE, DEX_OHLCV_SOURCE}:
        raise LifecycleReviewError(
            "{} requires exact pool and daily OHLCV checks".format(review_id)
        )
    pool = identity["pool"]
    chain = identity["chain"]
    dex = identity["dex"]
    expected_pool_path = "/api/v2/networks/{}/pools/{}".format(chain, pool)

    pool_check = checks[DEX_POOL_SOURCE]
    pool_url = str(pool_check["url"])
    pool_parts = _safe_official_url(
        pool_url,
        field=review_id + ".pool.url",
    )
    if (
        pool_parts.hostname != "api.geckoterminal.com"
        or pool_parts.path != expected_pool_path
        or pool_parts.query
    ):
        raise LifecycleReviewError(
            "{} pool check does not bind the exact network and pool".format(
                review_id
            )
        )
    pool_observations = pool_check["observations"]
    observed_pool = _normalized_evm_address(
        pool_observations.get("pool_address"),
        field=review_id + ".pool.pool_address",
    )
    if observed_pool != pool or pool_observations.get("dex_id") != dex:
        raise LifecycleReviewError(
            "{} pool observation does not bind the exact pool and dex".format(
                review_id
            )
        )
    has_base_token = "base_token_id" in pool_observations
    has_quote_token = "quote_token_id" in pool_observations
    if has_base_token != has_quote_token:
        raise LifecycleReviewError(
            "{} pool Token identity evidence is incomplete".format(review_id)
        )
    token_identity_bound = has_base_token and has_quote_token
    if token_identity_bound:
        base_token_id = _geckoterminal_evm_token_id(
            pool_observations.get("base_token_id"),
            chain=chain,
            field=review_id + ".pool.base_token_id",
        )
        quote_token_id = _geckoterminal_evm_token_id(
            pool_observations.get("quote_token_id"),
            chain=chain,
            field=review_id + ".pool.quote_token_id",
        )
        if base_token_id == quote_token_id:
            raise LifecycleReviewError(
                "{} pool base and quote Token identities are identical".format(
                    review_id
                )
            )
        trusted_contract = trusted_token_contracts.get(
            (identity["token"], chain)
        )
        if trusted_contract is None:
            raise LifecycleReviewError(
                "{} has no trusted Token contract identity for its network".format(
                    review_id
                )
            )
        normalized_trusted_contract = _normalized_evm_address(
            trusted_contract,
            field=review_id + ".trusted_token_contract",
        )
        if trusted_contract != normalized_trusted_contract:
            raise LifecycleReviewError(
                "{} trusted Token contract identity is not canonical".format(
                    review_id
                )
            )
        trusted_token_id = "{}_{}".format(chain, normalized_trusted_contract)
        if trusted_token_id not in {base_token_id, quote_token_id}:
            raise LifecycleReviewError(
                "{} pool evidence does not contain the reviewed Token".format(
                    review_id
                )
            )

    ohlcv_check = checks[DEX_OHLCV_SOURCE]
    ohlcv_url = str(ohlcv_check["url"])
    ohlcv_parts = _safe_official_url(
        ohlcv_url,
        field=review_id + ".ohlcv.url",
    )
    query_pairs = _exact_query(ohlcv_url, field=review_id + ".ohlcv.url")
    query = dict(query_pairs)
    limit = query.get("limit", "")
    if (
        ohlcv_parts.hostname != "api.geckoterminal.com"
        or ohlcv_parts.path != expected_pool_path + "/ohlcv/day"
        or len(query_pairs) != 3
        or len(query) != 3
        or query.get("aggregate") != "1"
        or query.get("currency") != "usd"
        or not limit.isdigit()
        or str(int(limit)) != limit
        or int(limit) < 1
        or int(limit) > 1_000
    ):
        raise LifecycleReviewError(
            "{} OHLCV check is not an exact uncut daily query".format(review_id)
        )
    observations = ohlcv_check["observations"]
    if "pool_address" in observations:
        observed_ohlcv_pool = _normalized_evm_address(
            observations.get("pool_address"),
            field=review_id + ".ohlcv.pool_address",
        )
        if observed_ohlcv_pool != pool:
            raise LifecycleReviewError(
                "{} OHLCV observation names another pool".format(review_id)
            )
    if "dex_id" in observations and observations.get("dex_id") != dex:
        raise LifecycleReviewError(
            "{} OHLCV observation names another dex".format(review_id)
        )
    latest_timestamp = observations.get("latest_candle_timestamp")
    if not isinstance(latest_timestamp, int) or isinstance(latest_timestamp, bool):
        raise LifecycleReviewError(
            "{} OHLCV latest timestamp is invalid".format(review_id)
        )
    try:
        timestamp_day = datetime.fromtimestamp(
            latest_timestamp,
            timezone.utc,
        ).date()
    except (OverflowError, OSError, ValueError) as error:
        raise LifecycleReviewError(
            "{} OHLCV latest timestamp is invalid".format(review_id)
        ) from error
    latest_day = _iso_day(
        observations.get("latest_candle_date_utc"),
        field=review_id + ".ohlcv.latest_candle_date_utc",
    )
    if timestamp_day.isoformat() != latest_day:
        raise LifecycleReviewError(
            "{} OHLCV timestamp and date disagree".format(review_id)
        )
    if timestamp_day >= date.fromisoformat(issue_date):
        raise LifecycleReviewError(
            "{} OHLCV latest candle does not precede issue_date".format(
                review_id
            )
        )
    return bool(token_identity_bound)


def _review_revision(
    value: Any,
    *,
    trusted_token_contracts: Mapping[Tuple[str, str], str],
) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise LifecycleReviewError("Lifecycle review revision must be an object")
    _require_exact_keys(value, REVISION_FIELDS, field="Lifecycle review revision")
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
    else:
        if (
            not isinstance(supersedes_revision, int)
            or isinstance(supersedes_revision, bool)
            or supersedes_revision != revision - 1
        ):
            raise LifecycleReviewError(
                "{} revision {} must supersede integer revision {}".format(
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
    token_symbol = _canonical_token(
        value.get("token_symbol"),
        field=review_id + ".token_symbol",
    )
    issue_date = _iso_day(
        value.get("issue_date"),
        field=review_id + ".issue_date",
    )
    market_identity = _parse_market_identity(
        market_id,
        market_type=market_type,
        token_symbol=token_symbol,
        review_id=review_id,
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
    source_kinds = [check["source_kind"] for check in source_checks]
    if len(source_kinds) != len(set(source_kinds)):
        raise LifecycleReviewError(
            "{} contains a duplicate source_kind".format(review_id)
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
    expected_contract = {
        "cex": (
            "listed_quote_market_dormant",
            "primary_confirmed",
            "manual_primary_source_cross_check",
        ),
        "dex": (
            "pool_exists_dormant",
            "declared_source_confirmed",
            "manual_declared_source_cross_check",
        ),
    }[market_type]
    if review_status == "disposed" and market_lifecycle != expected_contract[0]:
        raise LifecycleReviewError(
            "{} lifecycle does not match market_type".format(review_id)
        )
    if (
        evidence_status != expected_contract[1]
        or review_method != expected_contract[2]
    ):
        raise LifecycleReviewError(
            "{} evidence contract does not match market_type".format(review_id)
        )
    reviewed_at_utc = _iso_timestamp(
        value.get("reviewed_at_utc"),
        field=review_id + ".reviewed_at_utc",
    )
    reviewed_at = _utc_timestamp(reviewed_at_utc)
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
        checked_at = _utc_timestamp(checked_text)
        if checked_at.date() <= date.fromisoformat(issue_date):
            raise LifecycleReviewError(
                "{} contains evidence checked before issue day completed".format(
                    review_id
                )
            )
        if checked_at > reviewed_at:
            raise LifecycleReviewError(
                "{} contains evidence checked after review".format(review_id)
            )

    dex_token_identity_bound = True
    if market_type == "cex":
        _validate_upbit_evidence(
            source_checks,
            identity=market_identity,
            issue_date=issue_date,
            review_id=review_id,
        )
    else:
        dex_token_identity_bound = _validate_dex_evidence(
            source_checks,
            identity=market_identity,
            issue_date=issue_date,
            review_id=review_id,
            trusted_token_contracts=trusted_token_contracts,
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
        "_dex_token_identity_bound": dex_token_identity_bound,
    }


def load_lifecycle_reviews(
    path: Optional[Path] = DEFAULT_REVIEW_PATH,
    *,
    token_chain_path: Path = TOKEN_CHAIN_CONFIG_PATH,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if path is None:
        return [], {
            "schema": REVIEW_SCHEMA,
            "status": "disabled",
            "source_name": None,
            "sha256": None,
            "generated_at_utc": None,
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
            "generated_at_utc": None,
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
    _require_exact_keys(payload, ROOT_FIELDS, field="Lifecycle review file")
    generated_at_utc = _iso_timestamp(
        payload.get("generated_at_utc"),
        field="generated_at_utc",
    )
    generated_at = _utc_timestamp(generated_at_utc)
    raw_reviews = payload.get("reviews")
    review_count = payload.get("review_count")
    if (
        not isinstance(raw_reviews, list)
        or not isinstance(review_count, int)
        or isinstance(review_count, bool)
        or len(raw_reviews) > MAX_REVISION_COUNT
        or review_count != len(raw_reviews)
    ):
        raise LifecycleReviewError("Lifecycle review count is inconsistent")

    trusted_token_contracts: Dict[Tuple[str, str], str] = {}
    if any(
        isinstance(item, dict) and item.get("market_type") == "dex"
        for item in raw_reviews
    ):
        trusted_token_contracts = _trusted_token_contracts(token_chain_path)
    revisions = [
        _review_revision(
            item,
            trusted_token_contracts=trusted_token_contracts,
        )
        for item in raw_reviews
    ]
    if any(
        _utc_timestamp(str(revision["reviewed_at_utc"])) > generated_at
        for revision in revisions
    ):
        raise LifecycleReviewError(
            "Lifecycle review generation precedes a review revision"
        )
    by_review_id: Dict[str, Dict[int, Dict[str, Any]]] = {}
    issue_lineages: Dict[str, str] = {}
    for revision in revisions:
        reviewed_issue_id = str(revision["reviewed_issue_id"])
        existing_lineage = issue_lineages.setdefault(
            reviewed_issue_id,
            str(revision["review_id"]),
        )
        if existing_lineage != revision["review_id"]:
            raise LifecycleReviewError(
                "Lifecycle reviewed_issue_id has forked review lineages"
            )
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
        original = review_revisions[1]
        original_identity = tuple(
            original[field] for field in IMMUTABLE_REVISION_FIELDS
        )
        previous_reviewed_at: Optional[datetime] = None
        for revision_number in expected:
            revision = review_revisions[revision_number]
            revision_identity = tuple(
                revision[field] for field in IMMUTABLE_REVISION_FIELDS
            )
            if revision_identity != original_identity:
                raise LifecycleReviewError(
                    "{} revision identity is immutable".format(review_id)
                )
            reviewed_at = _utc_timestamp(str(revision["reviewed_at_utc"]))
            if (
                previous_reviewed_at is not None
                and reviewed_at <= previous_reviewed_at
            ):
                raise LifecycleReviewError(
                    "{} review timestamps must increase".format(review_id)
                )
            previous_reviewed_at = reviewed_at
        latest_revision = review_revisions[max(review_revisions)]
        if (
            latest_revision["review_status"] == "disposed"
            and latest_revision["market_type"] == "dex"
            and not latest_revision["_dex_token_identity_bound"]
        ):
            raise LifecycleReviewError(
                "{} latest DEX evidence does not bind the reviewed Token".format(
                    review_id
                )
            )
        latest_revision.pop("_dex_token_identity_bound", None)
        latest.append(latest_revision)

    active = [
        item for item in latest if item["review_status"] == "disposed"
    ]
    active_issue_ids = [item["reviewed_issue_id"] for item in active]
    if len(active_issue_ids) != len(set(active_issue_ids)):
        raise LifecycleReviewError(
            "Lifecycle review has ambiguous active dispositions"
        )
    metadata = {
        "schema": REVIEW_SCHEMA,
        "status": "accepted",
        "source_name": path.name,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "generated_at_utc": generated_at_utc,
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
