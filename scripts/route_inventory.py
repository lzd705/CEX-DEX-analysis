"""Validate private inventory evidence and gate strict route modes.

The private CSV is read through one owner-only, no-follow file descriptor.
Public results deliberately retain only an opaque profile hash, evidence
times, and the Token capacity proved for the requested route quantity; raw
balances, source hashes, account identities, wallet identities, and paths are
never projected or logged.
"""

from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

try:
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
except ModuleNotFoundError:
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore


INVENTORY_EVIDENCE_VERSION = "1"

INVENTORY_PROFILE_COLUMNS = (
    "profile_id",
    "market_id",
    "asset",
    "available_quantity",
    "observed_at",
    "valid_until",
    "source_record_sha256",
)

_OPAQUE_HASH = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_ASSET = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,63}\Z", flags=re.ASCII)
_CANONICAL_QUANTITY = re.compile(
    r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z",
    flags=re.ASCII,
)
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
_ROUTE_MODES = {
    "prepositioned_inventory",
    "atomic_onchain",
    "rebalance_required",
}
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_PUBLIC_WEB_ROOT = _PROJECT_ROOT / "dashboard" / "static"

_INVENTORY_REQUEST_FIELDS = (
    "route_id",
    "buy_market_id",
    "sell_market_id",
    "buy_quote_asset",
    "buy_quote_quantity",
    "sell_token_asset",
    "sell_net_token_quantity",
    "target_asset",
    "target_quantity",
)
_ATOMIC_REQUEST_FIELDS = (
    "route_id",
    "buy_market_id",
    "sell_market_id",
    "cohort_state_id",
    "target_asset",
    "target_quantity",
    "composed_call_sha256",
    "route_outcome_sha256",
)
_ATOMIC_EXPECTED_LINEAGE_FIELDS = (
    "atomic_source_record_sha256",
    "atomic_evidence_binding_sha256",
)
_TRANSFER_REQUEST_FIELDS = (
    "transfer_asset",
    "transfer_quantity",
    "transfer_capacity_quantity",
    "transfer_from_market_id",
    "transfer_to_market_id",
    "transfer_from_state_id",
    "transfer_to_state_id",
    "transfer_source_record_sha256",
    "transfer_evidence_binding_sha256",
)
_DEX_BUY_QUOTE_FIELDS = frozenset(
    {
        "evidence_type",
        "market_id",
        "base_asset",
        "quote_asset",
        "quote_debit_asset",
        "quote_debit_quantity",
        "target_base_asset",
        "target_base_quantity",
        "market_rules_sha256",
        "quantity_quote_sha256",
        "observed_at",
        "valid_until",
        "source_record_sha256",
        "evidence_binding_sha256",
    }
)
_ATOMIC_EVIDENCE_FIELDS = frozenset(
    {
        "evidence_type",
        "status",
        *_ATOMIC_REQUEST_FIELDS,
        "observed_at",
        "valid_until",
        "source_record_sha256",
        "evidence_binding_sha256",
    }
)
_TRANSFER_EVIDENCE_FIELDS = frozenset(
    {
        "evidence_type",
        "status",
        "route_id",
        "asset",
        "quantity",
        "capacity_quantity",
        "from_market_id",
        "to_market_id",
        "from_state_id",
        "to_state_id",
        "observed_at",
        "valid_until",
        "source_record_sha256",
        "evidence_binding_sha256",
    }
)


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("{} must be non-empty canonical text".format(field))
    return value


def _opaque_hash(value: Any, field: str) -> str:
    text = _required_text(value, field)
    if _OPAQUE_HASH.fullmatch(text) is None:
        if field == "profile_id":
            raise ValueError("profile_id must be one opaque profile hash")
        raise ValueError("{} must be a lowercase SHA-256 hash".format(field))
    return text


def _asset(value: Any, field: str) -> str:
    text = _required_text(value, field)
    if _ASSET.fullmatch(text) is None:
        raise ValueError("{} must be a canonical asset".format(field))
    return text


def _timestamp(value: Any, field: str) -> Tuple[str, Decimal]:
    text = _required_text(value, field)
    try:
        epoch = exact_rfc3339_epoch_seconds(text)
    except ValueError as error:
        raise ValueError(
            "{} must be timezone-aware RFC 3339 text".format(field)
        ) from error
    return text, epoch


def _validity_window(
    observed_at: Any,
    valid_until: Any,
) -> Tuple[str, Decimal, str, Decimal]:
    observed, observed_epoch = _timestamp(observed_at, "observed_at")
    valid, valid_epoch = _timestamp(valid_until, "valid_until")
    if valid_epoch <= observed_epoch:
        raise ValueError("valid_until must be after observed_at")
    return observed, observed_epoch, valid, valid_epoch


def _decimal_from_exact(
    value: Any,
    field: str,
    *,
    positive: bool,
) -> Decimal:
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, str) and _CANONICAL_QUANTITY.fullmatch(value):
        try:
            number = Decimal(value)
        except InvalidOperation as error:  # pragma: no cover - guarded by regex
            raise ValueError("{} must be an exact Decimal".format(field)) from error
    else:
        raise ValueError("{} must be an exact Decimal".format(field))
    if not number.is_finite():
        raise ValueError("{} must be a finite exact Decimal".format(field))
    if positive and number <= 0:
        raise ValueError("{} must be positive".format(field))
    if not positive and number < 0:
        raise ValueError("{} must be non-negative".format(field))
    return number


def _decimal_text(number: Decimal) -> str:
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"
    return text


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _quantity_text(value: Any, field: str, *, positive: bool = True) -> str:
    return _decimal_text(
        _decimal_from_exact(value, field, positive=positive)
    )


def _market_identity(value: Any) -> Dict[str, Optional[str]]:
    market_id = _required_text(value, "market_id")
    cex_match = _CEX_MARKET_ID.fullmatch(market_id)
    if cex_match is not None:
        venue, token, quote = cex_match.groups()
        return {
            "market_id": market_id,
            "market_type": "cex",
            "source": venue,
            "chain": None,
            "token": token,
            "quote": quote,
        }
    dex_match = _DEX_MARKET_ID.fullmatch(market_id)
    if dex_match is not None:
        chain, dex, pool, token = dex_match.groups()
        if pool.startswith("0x") and pool != pool.lower():
            raise ValueError("market_id must be canonical")
        return {
            "market_id": market_id,
            "market_type": "dex",
            "source": dex,
            "chain": chain,
            "token": token,
            "quote": None,
        }
    raise ValueError("market_id must be canonical")


def _absolute_private_path(value: os.PathLike) -> Path:
    try:
        path = Path(value)
    except (TypeError, ValueError):
        raise ValueError("private inventory profile requires an absolute trusted path") from None
    if not path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise ValueError("private inventory profile requires an absolute trusted path")
    try:
        path.relative_to(_PROJECT_ROOT)
    except ValueError:
        pass
    else:
        raise ValueError(
            "private inventory profile must be outside the repository and web root"
        )
    return path


def _protected_directory_identities() -> frozenset:
    identities = set()
    for path in (_PROJECT_ROOT, _PUBLIC_WEB_ROOT):
        try:
            metadata = os.stat(str(path))
        except OSError:
            continue
        if stat.S_ISDIR(metadata.st_mode):
            identities.add((metadata.st_dev, metadata.st_ino))
    return frozenset(identities)


def _secure_open_flags(*, directory: bool) -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise ValueError("private inventory profile secure open is unavailable")
    flags = os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)
    if directory:
        directory_flag = getattr(os, "O_DIRECTORY", None)
        if directory_flag is None:
            raise ValueError(
                "private inventory profile secure directory open is unavailable"
            )
        flags |= directory_flag
    return flags


def _open_private_parent(
    path: Path,
    *,
    protected: frozenset,
) -> Tuple[int, Tuple[Tuple[int, int], ...]]:
    """Open every parent from root by pinned, no-follow directory descriptors."""
    directory_flags = _secure_open_flags(directory=True)
    descriptor = -1
    identities: List[Tuple[int, int]] = []
    try:
        descriptor = os.open(os.sep, directory_flags)
        for component in path.parts[1:-1]:
            try:
                child = os.open(component, directory_flags, dir_fd=descriptor)
            except OSError:
                raise ValueError(
                    "private inventory profile parent is unavailable, changed, or a symlink"
                ) from None
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                raise ValueError(
                    "private inventory profile parent is unavailable, changed, or a symlink"
                )
            identity = (metadata.st_dev, metadata.st_ino)
            if identity in protected:
                raise ValueError(
                    "private inventory profile must be outside the repository and web root"
                )
            identities.append(identity)
        return descriptor, tuple(identities)
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        raise


def _open_private_file(
    path: Path,
) -> int:
    """Open and reverify one exact profile through a stable dirfd chain."""
    protected = _protected_directory_identities()
    parent_descriptor = -1
    verify_parent_descriptor = -1
    descriptor = -1
    verify_descriptor = -1
    try:
        parent_descriptor, parent_identities = _open_private_parent(
            path,
            protected=protected,
        )
        try:
            descriptor = os.open(
                path.name,
                _secure_open_flags(directory=False),
                dir_fd=parent_descriptor,
            )
        except OSError:
            raise ValueError(
                "private inventory profile is unavailable, changed, or a symlink"
            ) from None
        opened_metadata = os.fstat(descriptor)
        _validate_private_metadata(opened_metadata)

        # A second root-to-leaf walk detects a component replacement between
        # opening the pinned parent and opening the file. Both walks use
        # O_NOFOLLOW at every level; neither ever resolves an attacker path.
        verify_parent_descriptor, verify_parent_identities = _open_private_parent(
            path,
            protected=protected,
        )
        if verify_parent_identities != parent_identities:
            raise ValueError("private inventory profile parent changed during open")
        try:
            verify_descriptor = os.open(
                path.name,
                _secure_open_flags(directory=False),
                dir_fd=verify_parent_descriptor,
            )
        except OSError:
            raise ValueError(
                "private inventory profile is unavailable, changed, or a symlink"
            ) from None
        verify_metadata = os.fstat(verify_descriptor)
        _validate_private_metadata(verify_metadata)
        if (
            opened_metadata.st_dev != verify_metadata.st_dev
            or opened_metadata.st_ino != verify_metadata.st_ino
        ):
            raise ValueError("private inventory profile changed during open")
        result = descriptor
        descriptor = -1
        return result
    finally:
        for current in (
            verify_descriptor,
            verify_parent_descriptor,
            descriptor,
            parent_descriptor,
        ):
            if current >= 0:
                os.close(current)


def _validate_private_metadata(metadata: os.stat_result) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(
            "private inventory profile must be a regular owner-only file"
        )
    if metadata.st_nlink != 1:
        raise ValueError(
            "private inventory profile must be a single-link owner-only file"
        )
    mode = stat.S_IMODE(metadata.st_mode)
    if mode not in {0o400, 0o600}:
        raise ValueError("private inventory profile must be owner-only")
    effective_uid = getattr(os, "geteuid", None)
    if effective_uid is not None and metadata.st_uid != effective_uid():
        raise ValueError(
            "private inventory profile must be owned by the running user"
        )


def _read_private_rows(path: Path) -> List[Dict[str, str]]:
    """Read the validated profile from the exact no-follow descriptor."""
    descriptor = _open_private_file(path)
    try:
        try:
            with os.fdopen(
                descriptor,
                "r",
                encoding="utf-8",
                newline="",
            ) as handle:
                descriptor = -1
                reader = csv.DictReader(handle)
                if tuple(reader.fieldnames or ()) != INVENTORY_PROFILE_COLUMNS:
                    raise ValueError("private inventory profile columns are invalid")
                rows = list(reader)
                if any(set(row) != set(INVENTORY_PROFILE_COLUMNS) for row in rows):
                    raise ValueError("private inventory profile columns are invalid")
                return rows
        except (OSError, UnicodeError, csv.Error):
            raise ValueError("private inventory profile could not be read") from None
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def load_validated_inventory_profile(
    path: os.PathLike,
    *,
    now: str,
) -> List[Dict[str, str]]:
    """Load one fresh owner-only profile without projecting private lineage."""
    profile_path = _absolute_private_path(path)
    _now, now_epoch = _timestamp(now, "now")
    raw_rows = _read_private_rows(profile_path)
    if not raw_rows:
        raise ValueError("private inventory profile must contain at least one row")

    normalized: List[Dict[str, str]] = []
    seen_keys = set()
    profile_hash: Optional[str] = None
    for raw in raw_rows:
        current_profile = _opaque_hash(raw.get("profile_id"), "profile_id")
        if profile_hash is None:
            profile_hash = current_profile
        elif current_profile != profile_hash:
            raise ValueError(
                "private inventory profile must contain one opaque profile"
            )
        market = _market_identity(raw.get("market_id"))["market_id"]
        asset = _asset(raw.get("asset"), "asset")
        available = _decimal_from_exact(
            raw.get("available_quantity"),
            "available_quantity",
            positive=False,
        )
        observed, observed_epoch, valid, valid_epoch = _validity_window(
            raw.get("observed_at"),
            raw.get("valid_until"),
        )
        if observed_epoch > now_epoch:
            raise ValueError("private inventory profile observation is in the future")
        if valid_epoch <= now_epoch:
            raise ValueError("private inventory profile contains a stale record")
        _opaque_hash(raw.get("source_record_sha256"), "source_record_sha256")

        key = (market, asset)
        if key in seen_keys:
            raise ValueError("private inventory profile contains a duplicate key")
        seen_keys.add(key)
        normalized.append({
            "profile_hash": current_profile,
            "market_id": market,
            "asset": asset,
            "available_quantity": _decimal_text(available),
            "observed_at": observed,
            "valid_until": valid,
        })

    return sorted(
        normalized,
        key=lambda row: (row["market_id"], row["asset"]),
    )


def _validated_route(route: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(route, Mapping):
        raise ValueError("route must be an object")
    token = _asset(route.get("token_symbol"), "token_symbol")
    buy = _market_identity(route.get("buy_market_id"))
    sell = _market_identity(route.get("sell_market_id"))
    if buy["market_id"] == sell["market_id"]:
        raise ValueError("route markets must be distinct")
    if buy["token"] != token or sell["token"] != token:
        raise ValueError("route token_symbol does not match market_id")
    mode = _required_text(route.get("route_mode"), "route_mode")
    if mode not in _ROUTE_MODES:
        raise ValueError("route_mode is unsupported")
    route_id = _required_text(route.get("route_id"), "route_id")
    expected_route_id = "route:{}:{}->{}:{}".format(
        token,
        buy["market_id"],
        sell["market_id"],
        mode,
    )
    if route_id != expected_route_id:
        raise ValueError("route_id does not match canonical route identity")
    return {
        "route_id": route_id,
        "token": token,
        "route_mode": mode,
        "buy": buy,
        "sell": sell,
    }


def _inventory_request_binding(
    identity: Mapping[str, Any],
    *,
    buy_quote_asset: Any,
    buy_quote_quantity: Any,
    sell_token_asset: Any,
    sell_net_token_quantity: Any,
) -> Dict[str, str]:
    quote_asset = _asset(buy_quote_asset, "buy_quote_asset")
    sell_asset = _asset(sell_token_asset, "sell_token_asset")
    if (
        identity["buy"]["quote"] is not None
        and quote_asset != identity["buy"]["quote"]
    ):
        raise ValueError("buy_quote_asset does not match buy market_id")
    if sell_asset != identity["token"]:
        raise ValueError("sell_token_asset does not match route token_symbol")
    buy_required = _quantity_text(
        buy_quote_quantity,
        "buy_quote_quantity",
    )
    sell_required = _quantity_text(
        sell_net_token_quantity,
        "sell_net_token_quantity",
    )
    return {
        "route_id": identity["route_id"],
        "buy_market_id": identity["buy"]["market_id"],
        "sell_market_id": identity["sell"]["market_id"],
        "buy_quote_asset": quote_asset,
        "buy_quote_quantity": buy_required,
        "sell_token_asset": sell_asset,
        "sell_net_token_quantity": sell_required,
        "target_asset": sell_asset,
        "target_quantity": sell_required,
    }


def _evidence_binding_matches(
    evidence: Mapping[str, Any],
    fields: frozenset,
) -> bool:
    try:
        provided = _opaque_hash(
            evidence.get("evidence_binding_sha256"),
            "evidence_binding_sha256",
        )
    except (TypeError, ValueError):
        return False
    record = {
        field: evidence.get(field)
        for field in fields
        if field != "evidence_binding_sha256"
    }
    return provided == _canonical_sha256(record)


def _dex_buy_quote_integrity_is_valid(
    evidence: Any,
    *,
    identity: Mapping[str, Any],
    binding: Mapping[str, str],
    now_epoch: Decimal,
) -> bool:
    if not isinstance(evidence, Mapping) or set(evidence) != _DEX_BUY_QUOTE_FIELDS:
        return False
    try:
        if evidence.get("evidence_type") != "market_rules_quantity_quote":
            return False
        exact_fields = {
            "market_id": identity["buy"]["market_id"],
            "base_asset": identity["token"],
            "quote_asset": binding["buy_quote_asset"],
            "quote_debit_asset": binding["buy_quote_asset"],
            "quote_debit_quantity": binding["buy_quote_quantity"],
            "target_base_asset": binding["target_asset"],
            "target_base_quantity": binding["target_quantity"],
        }
        if any(evidence.get(field) != value for field, value in exact_fields.items()):
            return False
        _opaque_hash(evidence.get("market_rules_sha256"), "market_rules_sha256")
        _opaque_hash(evidence.get("quantity_quote_sha256"), "quantity_quote_sha256")
        _opaque_hash(evidence.get("source_record_sha256"), "source_record_sha256")
        _observed, observed_epoch, _valid, valid_epoch = _validity_window(
            evidence.get("observed_at"),
            evidence.get("valid_until"),
        )
        return (
            observed_epoch <= now_epoch < valid_epoch
            and _evidence_binding_matches(evidence, _DEX_BUY_QUOTE_FIELDS)
        )
    except (TypeError, ValueError):
        return False


def _inventory_projection(
    status: str,
    *,
    request_binding: Optional[Mapping[str, str]] = None,
    reason_code: Optional[str] = None,
    profile_hash: Optional[str] = None,
    observed_at: Optional[str] = None,
    valid_until: Optional[str] = None,
    capacity_asset: Optional[str] = None,
    capacity_quantity: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    reason = (
        None
        if status == "inventory_sufficient"
        else reason_code or status
    )
    result = {
        "inventory_evidence_version": INVENTORY_EVIDENCE_VERSION,
        "status": status,
        "reason_code": reason,
        **{
            field: request_binding.get(field) if request_binding else None
            for field in _INVENTORY_REQUEST_FIELDS
        },
        "inventory_request_sha256": (
            _canonical_sha256(request_binding)
            if request_binding is not None
            else None
        ),
        "strict_capacity_asset": capacity_asset,
        "strict_capacity_quantity": capacity_quantity,
        "inventory_profile_hash": profile_hash,
        "observed_at": observed_at,
        "valid_until": valid_until,
    }
    return result


def _private_inventory_index(
    rows: Iterable[Mapping[str, Any]],
) -> Optional[Tuple[str, Dict[Tuple[str, str], Dict[str, Any]]]]:
    try:
        inventory = list(rows)
    except (TypeError, ValueError):
        return None
    if not inventory:
        return None
    profile_hash: Optional[str] = None
    index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    try:
        for raw in inventory:
            if not isinstance(raw, Mapping):
                return None
            current_profile = _opaque_hash(raw.get("profile_hash"), "profile_id")
            if profile_hash is None:
                profile_hash = current_profile
            elif current_profile != profile_hash:
                return None
            market = _market_identity(raw.get("market_id"))["market_id"]
            asset = _asset(raw.get("asset"), "asset")
            available = _decimal_from_exact(
                raw.get("available_quantity"),
                "available_quantity",
                positive=False,
            )
            observed, observed_epoch, valid, valid_epoch = _validity_window(
                raw.get("observed_at"),
                raw.get("valid_until"),
            )
            key = (market, asset)
            if key in index:
                return None
            index[key] = {
                "available": available,
                "observed_at": observed,
                "observed_epoch": observed_epoch,
                "valid_until": valid,
                "valid_epoch": valid_epoch,
            }
    except (TypeError, ValueError):
        return None
    if profile_hash is None:  # pragma: no cover - non-empty input assigns it
        return None
    return profile_hash, index


def inventory_capacity_for_route(
    route: Mapping[str, Any],
    inventory_rows: Iterable[Mapping[str, Any]],
    *,
    buy_quote_asset: str,
    buy_quote_quantity: Decimal,
    sell_token_asset: str,
    sell_net_token_quantity: Decimal,
    now: str,
    dex_buy_quantity_quote: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Optional[str]]:
    """Prove inventory for one exact requested route quantity.

    The numeric capacity is the requested net Token quantity only after both
    its buy-side quote debit and sell-side Token debit are fully covered.  No
    private balance is returned.
    """
    identity = _validated_route(route)
    if identity["route_mode"] not in {
        "prepositioned_inventory",
        "rebalance_required",
    }:
        raise ValueError("inventory capacity is not applicable to route_mode")
    binding = _inventory_request_binding(
        identity,
        buy_quote_asset=buy_quote_asset,
        buy_quote_quantity=buy_quote_quantity,
        sell_token_asset=sell_token_asset,
        sell_net_token_quantity=sell_net_token_quantity,
    )
    _now, now_epoch = _timestamp(now, "now")
    if identity["buy"]["market_type"] == "dex":
        # A canonical checksum proves only that a supplied projection has not
        # changed. It does not prove that Task 5 MarketRules/QuantityQuote
        # produced it. Keep DEX-buy inventory unavailable until those typed,
        # independently verified upstream objects and their immutable hashes
        # are wired into this gate.
        integrity_is_valid = _dex_buy_quote_integrity_is_valid(
            dex_buy_quantity_quote,
            identity=identity,
            binding=binding,
            now_epoch=now_epoch,
        )
        return _inventory_projection(
            "inventory_unavailable",
            request_binding=binding,
            reason_code=(
                "dex_buy_authoritative_upstream_unavailable"
                if integrity_is_valid
                else "dex_buy_quantity_quote_unavailable"
            ),
        )

    quote_asset = binding["buy_quote_asset"]
    sell_asset = binding["sell_token_asset"]
    buy_required = Decimal(binding["buy_quote_quantity"])
    sell_required = Decimal(binding["sell_net_token_quantity"])

    indexed = _private_inventory_index(inventory_rows)
    if indexed is None:
        return _inventory_projection(
            "inventory_unavailable",
            request_binding=binding,
        )
    profile_hash, index = indexed
    buy_row = index.get((identity["buy"]["market_id"], quote_asset))
    sell_row = index.get((identity["sell"]["market_id"], sell_asset))
    if buy_row is None or sell_row is None:
        return _inventory_projection(
            "inventory_unavailable",
            request_binding=binding,
            profile_hash=profile_hash,
        )
    required_rows = (buy_row, sell_row)
    if any(
        row["observed_epoch"] > now_epoch or row["valid_epoch"] <= now_epoch
        for row in required_rows
    ):
        return _inventory_projection(
            "inventory_unavailable",
            request_binding=binding,
            profile_hash=profile_hash,
        )
    observed_row = max(required_rows, key=lambda row: row["observed_epoch"])
    valid_row = min(required_rows, key=lambda row: row["valid_epoch"])
    lineage = {
        "profile_hash": profile_hash,
        "observed_at": observed_row["observed_at"],
        "valid_until": valid_row["valid_until"],
    }
    if buy_row["available"] < buy_required or sell_row["available"] < sell_required:
        return _inventory_projection(
            "inventory_insufficient",
            request_binding=binding,
            **lineage,
        )
    return _inventory_projection(
        "inventory_sufficient",
        request_binding=binding,
        capacity_asset=sell_asset,
        capacity_quantity=_decimal_text(sell_required),
        **lineage,
    )


def _validated_expected_request(
    identity: Mapping[str, Any],
    expected: Any,
) -> Dict[str, str]:
    if not isinstance(expected, Mapping):
        raise ValueError("expected route-mode request must be an object")
    mode = identity["route_mode"]
    if mode == "atomic_onchain":
        required = set(_ATOMIC_REQUEST_FIELDS) | set(
            _ATOMIC_EXPECTED_LINEAGE_FIELDS
        )
    elif mode == "rebalance_required":
        required = set(_INVENTORY_REQUEST_FIELDS) | set(_TRANSFER_REQUEST_FIELDS)
    else:
        required = set(_INVENTORY_REQUEST_FIELDS)
    if set(expected) != required:
        raise ValueError("expected route-mode request fields are invalid")

    if mode == "atomic_onchain":
        normalized = {
            "route_id": identity["route_id"],
            "buy_market_id": identity["buy"]["market_id"],
            "sell_market_id": identity["sell"]["market_id"],
            "cohort_state_id": _required_text(
                expected.get("cohort_state_id"),
                "cohort_state_id",
            ),
            "target_asset": _asset(expected.get("target_asset"), "target_asset"),
            "target_quantity": _quantity_text(
                expected.get("target_quantity"),
                "target_quantity",
            ),
            "composed_call_sha256": _opaque_hash(
                expected.get("composed_call_sha256"),
                "composed_call_sha256",
            ),
            "route_outcome_sha256": _opaque_hash(
                expected.get("route_outcome_sha256"),
                "route_outcome_sha256",
            ),
            "atomic_source_record_sha256": _opaque_hash(
                expected.get("atomic_source_record_sha256"),
                "atomic_source_record_sha256",
            ),
            "atomic_evidence_binding_sha256": _opaque_hash(
                expected.get("atomic_evidence_binding_sha256"),
                "atomic_evidence_binding_sha256",
            ),
        }
        if normalized["target_asset"] != identity["token"]:
            raise ValueError("expected target asset does not match route")
    else:
        normalized = _inventory_request_binding(
            identity,
            buy_quote_asset=expected.get("buy_quote_asset"),
            buy_quote_quantity=expected.get("buy_quote_quantity"),
            sell_token_asset=expected.get("sell_token_asset"),
            sell_net_token_quantity=expected.get("sell_net_token_quantity"),
        )
        if mode == "rebalance_required":
            transfer_asset = _asset(
                expected.get("transfer_asset"),
                "transfer_asset",
            )
            transfer_quantity = _quantity_text(
                expected.get("transfer_quantity"),
                "transfer_quantity",
            )
            transfer_capacity = _quantity_text(
                expected.get("transfer_capacity_quantity"),
                "transfer_capacity_quantity",
            )
            transfer = {
                "transfer_asset": transfer_asset,
                "transfer_quantity": transfer_quantity,
                "transfer_capacity_quantity": transfer_capacity,
                "transfer_from_market_id": _market_identity(
                    expected.get("transfer_from_market_id")
                )["market_id"],
                "transfer_to_market_id": _market_identity(
                    expected.get("transfer_to_market_id")
                )["market_id"],
                "transfer_from_state_id": _required_text(
                    expected.get("transfer_from_state_id"),
                    "transfer_from_state_id",
                ),
                "transfer_to_state_id": _required_text(
                    expected.get("transfer_to_state_id"),
                    "transfer_to_state_id",
                ),
                "transfer_source_record_sha256": _opaque_hash(
                    expected.get("transfer_source_record_sha256"),
                    "transfer_source_record_sha256",
                ),
                "transfer_evidence_binding_sha256": _opaque_hash(
                    expected.get("transfer_evidence_binding_sha256"),
                    "transfer_evidence_binding_sha256",
                ),
            }
            if (
                transfer_asset != identity["token"]
                or transfer_quantity != normalized["target_quantity"]
                or transfer["transfer_from_market_id"]
                != identity["buy"]["market_id"]
                or transfer["transfer_to_market_id"]
                != identity["sell"]["market_id"]
                or Decimal(transfer_capacity) < Decimal(transfer_quantity)
            ):
                raise ValueError("expected transfer does not match route")
            normalized.update(transfer)

    identity_fields = ("route_id", "buy_market_id", "sell_market_id")
    if any(expected.get(field) != normalized[field] for field in identity_fields):
        raise ValueError("expected request does not match route identity")
    if any(expected.get(field) != normalized[field] for field in normalized):
        raise ValueError("expected request is not canonical")
    return normalized


def _current_inventory_reason(
    evidence: Any,
    *,
    expected: Mapping[str, str],
    now_epoch: Decimal,
) -> Optional[str]:
    if not isinstance(evidence, Mapping):
        return "inventory_unavailable"
    try:
        if evidence.get("inventory_evidence_version") != INVENTORY_EVIDENCE_VERSION:
            return "inventory_unavailable"
        if any(
            evidence.get(field) != expected[field]
            for field in _INVENTORY_REQUEST_FIELDS
        ):
            return "inventory_request_mismatch"
        expected_hash = _canonical_sha256({
            field: expected[field]
            for field in _INVENTORY_REQUEST_FIELDS
        })
        if evidence.get("inventory_request_sha256") != expected_hash:
            return "inventory_request_mismatch"
        status = evidence.get("status")
        if status == "inventory_insufficient":
            return "inventory_insufficient"
        if status != "inventory_sufficient":
            reason = evidence.get("reason_code")
            if reason in {
                "dex_buy_quantity_quote_unavailable",
                "dex_buy_authoritative_upstream_unavailable",
            }:
                return reason
            return "inventory_unavailable"
        _opaque_hash(evidence.get("inventory_profile_hash"), "profile_id")
        if (
            _asset(
                evidence.get("strict_capacity_asset"),
                "strict_capacity_asset",
            )
            != expected["target_asset"]
        ):
            return "inventory_unavailable"
        capacity = _quantity_text(
            evidence.get("strict_capacity_quantity"),
            "strict_capacity_quantity",
        )
        if capacity != expected["target_quantity"]:
            return "inventory_request_mismatch"
        _observed, observed_epoch, _valid, valid_epoch = _validity_window(
            evidence.get("observed_at"),
            evidence.get("valid_until"),
        )
        if observed_epoch > now_epoch or valid_epoch <= now_epoch:
            return "inventory_unavailable"
    except (KeyError, TypeError, ValueError):
        return "inventory_unavailable"
    return None


def _current_transfer_evidence(
    evidence: Any,
    *,
    expected: Mapping[str, str],
    now_epoch: Decimal,
) -> bool:
    if not isinstance(evidence, Mapping) or set(evidence) != _TRANSFER_EVIDENCE_FIELDS:
        return False
    try:
        if evidence.get("evidence_type") != "transfer":
            return False
        if evidence.get("status") != "complete":
            return False
        exact = {
            "route_id": expected["route_id"],
            "asset": expected["transfer_asset"],
            "quantity": expected["transfer_quantity"],
            "from_market_id": expected["transfer_from_market_id"],
            "to_market_id": expected["transfer_to_market_id"],
            "from_state_id": expected["transfer_from_state_id"],
            "to_state_id": expected["transfer_to_state_id"],
        }
        if any(evidence.get(field) != value for field, value in exact.items()):
            return False
        capacity_text = _quantity_text(
            evidence.get("capacity_quantity"),
            "capacity_quantity",
        )
        if capacity_text != evidence.get("capacity_quantity"):
            return False
        if capacity_text != expected["transfer_capacity_quantity"]:
            return False
        if Decimal(capacity_text) < Decimal(expected["transfer_quantity"]):
            return False
        source_hash = _opaque_hash(
            evidence.get("source_record_sha256"),
            "source_record_sha256",
        )
        if source_hash != expected["transfer_source_record_sha256"]:
            return False
        if (
            evidence.get("evidence_binding_sha256")
            != expected["transfer_evidence_binding_sha256"]
            or not _evidence_binding_matches(evidence, _TRANSFER_EVIDENCE_FIELDS)
        ):
            return False
        _observed, observed_epoch, _valid, valid_epoch = _validity_window(
            evidence.get("observed_at"),
            evidence.get("valid_until"),
        )
        return observed_epoch <= now_epoch < valid_epoch
    except (TypeError, ValueError):
        return False


def _current_atomic_simulation(
    evidence: Any,
    *,
    expected: Mapping[str, str],
    now_epoch: Decimal,
) -> bool:
    if not isinstance(evidence, Mapping) or set(evidence) != _ATOMIC_EVIDENCE_FIELDS:
        return False
    try:
        if evidence.get("evidence_type") != "composed_route_simulation":
            return False
        if evidence.get("status") != "complete":
            return False
        if any(
            evidence.get(field) != expected[field]
            for field in _ATOMIC_REQUEST_FIELDS
        ):
            return False
        _opaque_hash(evidence.get("composed_call_sha256"), "composed_call_sha256")
        _opaque_hash(evidence.get("route_outcome_sha256"), "route_outcome_sha256")
        source_hash = _opaque_hash(
            evidence.get("source_record_sha256"),
            "source_record_sha256",
        )
        if source_hash != expected["atomic_source_record_sha256"]:
            return False
        if (
            evidence.get("evidence_binding_sha256")
            != expected["atomic_evidence_binding_sha256"]
            or not _evidence_binding_matches(evidence, _ATOMIC_EVIDENCE_FIELDS)
        ):
            return False
        _observed, observed_epoch, _valid, valid_epoch = _validity_window(
            evidence.get("observed_at"),
            evidence.get("valid_until"),
        )
        return observed_epoch <= now_epoch < valid_epoch
    except (TypeError, ValueError):
        return False


def _classification(
    identity: Mapping[str, Any],
    reasons: List[str],
    inventory_evidence: Any,
    maximum_proved_capacity_quantity: Optional[str] = None,
) -> Dict[str, Any]:
    profile_hash = None
    if isinstance(inventory_evidence, Mapping):
        candidate = inventory_evidence.get("inventory_profile_hash")
        try:
            profile_hash = _opaque_hash(candidate, "profile_id")
        except (TypeError, ValueError):
            profile_hash = None
    capacity = None
    if maximum_proved_capacity_quantity is not None:
        capacity = _quantity_text(
            maximum_proved_capacity_quantity,
            "maximum_proved_capacity_quantity",
        )
    return {
        "route_id": identity["route_id"],
        "route_mode": identity["route_mode"],
        "classification": (
            "research_estimate" if reasons else "mode_evidence_eligible"
        ),
        "mode_evidence_eligible": not reasons,
        "reason_code": reasons[0] if reasons else None,
        "reason_codes": list(reasons),
        "inventory_profile_hash": profile_hash,
        "maximum_proved_capacity_quantity": capacity,
    }


def classify_route_mode_evidence(
    route: Mapping[str, Any],
    *,
    expected_request: Optional[Mapping[str, Any]] = None,
    inventory_evidence: Optional[Mapping[str, Any]] = None,
    atomic_route_simulation: Optional[Mapping[str, Any]] = None,
    transfer_evidence: Optional[Mapping[str, Any]] = None,
    now: str,
) -> Dict[str, Any]:
    """Classify strict route-mode evidence without echoing private inputs."""
    identity = _validated_route(route)
    _now, now_epoch = _timestamp(now, "now")
    mode = identity["route_mode"]
    try:
        expected = _validated_expected_request(identity, expected_request)
    except (TypeError, ValueError):
        return _classification(
            identity,
            ["mode_expected_request_unavailable"],
            inventory_evidence,
        )

    if mode == "prepositioned_inventory":
        reason = _current_inventory_reason(
            inventory_evidence,
            expected=expected,
            now_epoch=now_epoch,
        )
        return _classification(
            identity,
            [] if reason is None else [reason],
            inventory_evidence,
            expected["target_quantity"] if reason is None else None,
        )

    if mode == "atomic_onchain":
        if (
            identity["buy"]["market_type"] != "dex"
            or identity["sell"]["market_type"] != "dex"
            or identity["buy"]["chain"] != identity["sell"]["chain"]
        ):
            return _classification(
                identity,
                ["unsupported_cross_chain_settlement"],
                inventory_evidence,
            )
        reasons = []
        if not _current_atomic_simulation(
            atomic_route_simulation,
            expected=expected,
            now_epoch=now_epoch,
        ):
            reasons.append("atomic_route_simulation_unavailable")
        return _classification(
            identity,
            reasons,
            inventory_evidence,
            expected["target_quantity"] if not reasons else None,
        )

    inventory_reason = _current_inventory_reason(
        inventory_evidence,
        expected=expected,
        now_epoch=now_epoch,
    )
    reasons = []
    if inventory_reason is not None:
        reasons.append(inventory_reason)
    if not _current_transfer_evidence(
        transfer_evidence,
        expected=expected,
        now_epoch=now_epoch,
    ):
        reasons.append("rebalance_transfer_evidence_unavailable")
    return _classification(
        identity,
        reasons,
        inventory_evidence,
        expected["target_quantity"] if not reasons else None,
    )
