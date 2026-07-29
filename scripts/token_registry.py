"""Runtime Token identity registry for administrator-managed DEX onboarding.

The reviewed CSV files under ``config/`` remain the version-controlled base
catalog.  This registry stores only runtime additions under ``data/local`` so
the application never needs to edit its Git checkout.

The module deliberately does not infer centralized-exchange instruments from
an on-chain contract address.  New records default to
``requires_manual_review`` for CEX mapping.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_PATH = PROJECT_ROOT / "data/local/admin/token_registry.json"
REGISTRY_SCHEMA_VERSION = 1

CHAIN_ADDRESS_KIND = {
    "eth": "evm",
    "arbitrum": "evm",
    "base": "evm",
    "optimism": "evm",
    "bsc": "evm",
    "avax": "evm",
    "zksync": "evm",
    "starknet": "starknet",
    "solana": "solana",
}
SUPPORTED_CHAINS = tuple(sorted(CHAIN_ADDRESS_KIND))
TOKEN_STATUSES = {"pending", "active", "failed", "needs_review"}
CEX_MAPPING_STATUSES = {"requires_manual_review", "approved", "not_listed"}
TOKEN_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
EVM_ADDRESS_PATTERN = re.compile(r"^0x[0-9a-fA-F]{40}$")
STARKNET_ADDRESS_PATTERN = re.compile(r"^0x[0-9a-fA-F]{1,64}$")
SOLANA_ADDRESS_PATTERN = re.compile(
    r"^[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]{32,44}$"
)
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_INDEX = {character: index for index, character in enumerate(BASE58_ALPHABET)}


class TokenRegistryError(ValueError):
    """A stable, machine-readable runtime registry validation error."""

    def __init__(
        self,
        code: str,
        message: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})
        self.retryable = False

    def as_dict(self) -> Dict[str, Any]:
        payload = {
            "error": self.message,
            "error_code": self.code,
            "retryable": self.retryable,
        }
        if self.details:
            payload["details"] = self.details
        return payload


def utc_now_text() -> str:
    """Return a stable UTC timestamp suitable for registry audit fields."""
    return datetime.now(timezone.utc).isoformat()


def normalize_chain(value: Any) -> str:
    """Normalize and allowlist a GeckoTerminal network id."""
    chain = str(value or "").strip().lower()
    if chain not in CHAIN_ADDRESS_KIND:
        raise TokenRegistryError(
            "invalid_chain",
            "Unsupported chain. Expected one of: %s" % ", ".join(SUPPORTED_CHAINS),
            {"chain": chain},
        )
    return chain


def _decode_base58(value: str) -> bytes:
    number = 0
    for character in value:
        try:
            digit = BASE58_INDEX[character]
        except KeyError:
            raise TokenRegistryError(
                "invalid_contract_address",
                "Solana contract address contains a non-base58 character",
            )
        number = number * 58 + digit
    decoded = (
        number.to_bytes((number.bit_length() + 7) // 8, "big")
        if number
        else b""
    )
    leading_zeroes = len(value) - len(value.lstrip("1"))
    return b"\x00" * leading_zeroes + decoded


def normalize_contract_address(chain: Any, value: Any) -> str:
    """Validate and canonicalize an address for one supported chain."""
    normalized_chain = normalize_chain(chain)
    address = str(value or "").strip()
    kind = CHAIN_ADDRESS_KIND[normalized_chain]

    if kind == "evm":
        if not EVM_ADDRESS_PATTERN.fullmatch(address):
            raise TokenRegistryError(
                "invalid_contract_address",
                "EVM contract address must be 0x followed by exactly 40 hexadecimal characters",
                {"chain": normalized_chain},
            )
        return address.lower()

    if kind == "starknet":
        if not STARKNET_ADDRESS_PATTERN.fullmatch(address):
            raise TokenRegistryError(
                "invalid_contract_address",
                "Starknet contract address must contain 1 to 64 hexadecimal characters after 0x",
                {"chain": normalized_chain},
            )
        # Fixed-width canonical form makes padded and unpadded inputs idempotent.
        return "0x" + address[2:].lower().zfill(64)

    if not SOLANA_ADDRESS_PATTERN.fullmatch(address):
        raise TokenRegistryError(
            "invalid_contract_address",
            "Solana contract address must be a 32-byte base58 public key",
            {"chain": normalized_chain},
        )
    if len(_decode_base58(address)) != 32:
        raise TokenRegistryError(
            "invalid_contract_address",
            "Solana contract address must decode to exactly 32 bytes",
            {"chain": normalized_chain},
        )
    return address


def normalize_token_symbol(value: Any) -> str:
    """Return a bounded catalog-safe Token symbol."""
    symbol = str(value or "").strip().upper()
    if not TOKEN_SYMBOL_PATTERN.fullmatch(symbol):
        raise TokenRegistryError(
            "invalid_token_symbol",
            "Token symbol must contain 1 to 32 letters, numbers, dots, dashes, or underscores",
        )
    return symbol


def token_identity_key(chain: Any, contract_address: Any) -> str:
    """Return the canonical uniqueness key for a runtime Token."""
    normalized_chain = normalize_chain(chain)
    normalized_address = normalize_contract_address(normalized_chain, contract_address)
    return "%s:%s" % (normalized_chain, normalized_address)


def empty_registry() -> Dict[str, Any]:
    return {"schema_version": REGISTRY_SCHEMA_VERSION, "tokens": {}}


def _normalize_optional_text(
    value: Any,
    *,
    field: str,
    maximum_length: int,
) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) > maximum_length:
        raise TokenRegistryError(
            "invalid_registry_record",
            "%s exceeds %s characters" % (field, maximum_length),
        )
    return text


def _normalize_cex_mapping(value: Any) -> Dict[str, Any]:
    if value is None:
        return {
            "status": "requires_manual_review",
            "cex_symbol": None,
            "exchanges": [],
        }
    if not isinstance(value, dict):
        raise TokenRegistryError(
            "invalid_registry_record",
            "cex_mapping must be an object",
        )
    status = str(value.get("status") or "").strip().lower()
    if status not in CEX_MAPPING_STATUSES:
        raise TokenRegistryError(
            "invalid_registry_record",
            "Unsupported cex_mapping status",
            {"status": status},
        )
    cex_symbol = _normalize_optional_text(
        value.get("cex_symbol"),
        field="cex_mapping.cex_symbol",
        maximum_length=64,
    )
    exchanges_value = value.get("exchanges", [])
    if not isinstance(exchanges_value, list):
        raise TokenRegistryError(
            "invalid_registry_record",
            "cex_mapping.exchanges must be a list",
        )
    exchanges = []
    for exchange in exchanges_value:
        normalized = str(exchange or "").strip().lower()
        if not normalized or len(normalized) > 32:
            raise TokenRegistryError(
                "invalid_registry_record",
                "cex_mapping contains an invalid exchange",
            )
        exchanges.append(normalized)
    exchanges = sorted(set(exchanges))
    if status != "approved" and (cex_symbol is not None or exchanges):
        raise TokenRegistryError(
            "invalid_registry_record",
            "Unapproved CEX mapping cannot contain a symbol or exchanges",
        )
    if status == "approved" and (cex_symbol is None or not exchanges):
        raise TokenRegistryError(
            "invalid_registry_record",
            "Approved CEX mapping requires a symbol and at least one exchange",
        )
    return {
        "status": status,
        "cex_symbol": cex_symbol,
        "exchanges": exchanges,
    }


def normalize_token_record(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate one persisted registry record without inventing CEX mapping."""
    if not isinstance(record, Mapping):
        raise TokenRegistryError(
            "invalid_registry_record",
            "Token registry record must be an object",
        )
    chain = normalize_chain(record.get("chain"))
    address = normalize_contract_address(chain, record.get("contract_address"))
    symbol = normalize_token_symbol(record.get("token_symbol"))
    token_name = _normalize_optional_text(
        record.get("token_name"),
        field="token_name",
        maximum_length=160,
    )
    if token_name is None:
        raise TokenRegistryError(
            "invalid_registry_record",
            "token_name is required",
        )
    decimals_value = record.get("decimals")
    if decimals_value is None or decimals_value == "":
        decimals = None
    else:
        if isinstance(decimals_value, bool):
            raise TokenRegistryError(
                "invalid_registry_record",
                "decimals must be an integer between 0 and 255",
            )
        try:
            decimals = int(decimals_value)
        except (TypeError, ValueError):
            raise TokenRegistryError(
                "invalid_registry_record",
                "decimals must be an integer between 0 and 255",
            )
        if str(decimals) != str(decimals_value).strip() or not 0 <= decimals <= 255:
            raise TokenRegistryError(
                "invalid_registry_record",
                "decimals must be an integer between 0 and 255",
            )
    status = str(record.get("status") or "pending").strip().lower()
    if status not in TOKEN_STATUSES:
        raise TokenRegistryError(
            "invalid_registry_record",
            "Unsupported Token registry status",
            {"status": status},
        )
    source = str(record.get("source") or "geckoterminal").strip().lower()
    if source != "geckoterminal":
        raise TokenRegistryError(
            "invalid_registry_record",
            "Runtime onboarding currently supports only GeckoTerminal identities",
        )
    created_at = _normalize_optional_text(
        record.get("created_at"),
        field="created_at",
        maximum_length=64,
    ) or utc_now_text()
    created_by = _normalize_optional_text(
        record.get("created_by"),
        field="created_by",
        maximum_length=80,
    ) or "system"

    return {
        "token_symbol": symbol,
        "token_name": token_name,
        "chain": chain,
        "contract_address": address,
        "decimals": decimals,
        "coingecko_id": _normalize_optional_text(
            record.get("coingecko_id"),
            field="coingecko_id",
            maximum_length=160,
        ),
        "source": source,
        "source_token_id": _normalize_optional_text(
            record.get("source_token_id"),
            field="source_token_id",
            maximum_length=240,
        ),
        "status": status,
        "cex_mapping": _normalize_cex_mapping(record.get("cex_mapping")),
        "created_at": created_at,
        "created_by": created_by,
        "activated_at": _normalize_optional_text(
            record.get("activated_at"),
            field="activated_at",
            maximum_length=64,
        ),
        "last_job_id": _normalize_optional_text(
            record.get("last_job_id"),
            field="last_job_id",
            maximum_length=128,
        ),
    }


def validate_registry_payload(payload: Any) -> Dict[str, Any]:
    """Validate and canonicalize an entire registry document."""
    if not isinstance(payload, dict):
        raise TokenRegistryError(
            "invalid_registry",
            "Token registry root must be an object",
        )
    if payload.get("schema_version") != REGISTRY_SCHEMA_VERSION:
        raise TokenRegistryError(
            "unsupported_registry_schema",
            "Token registry schema_version must be %s" % REGISTRY_SCHEMA_VERSION,
        )
    token_payload = payload.get("tokens")
    if not isinstance(token_payload, dict):
        raise TokenRegistryError(
            "invalid_registry",
            "Token registry tokens must be an object keyed by chain and address",
        )

    tokens = {}
    symbols = {}
    for stored_key, raw_record in token_payload.items():
        record = normalize_token_record(raw_record)
        expected_key = token_identity_key(record["chain"], record["contract_address"])
        if stored_key != expected_key:
            raise TokenRegistryError(
                "invalid_registry",
                "Token registry key does not match its chain and contract address",
                {"stored_key": stored_key, "expected_key": expected_key},
            )
        symbol = record["token_symbol"]
        previous_key = symbols.get(symbol)
        if previous_key is not None and previous_key != expected_key:
            raise TokenRegistryError(
                "symbol_collision",
                "Token symbol is already assigned to another contract",
                {"token_symbol": symbol, "existing_key": previous_key},
            )
        symbols[symbol] = expected_key
        tokens[expected_key] = record
    return {"schema_version": REGISTRY_SCHEMA_VERSION, "tokens": tokens}


def load_registry(path: Path = DEFAULT_REGISTRY_PATH) -> Dict[str, Any]:
    """Load one validated registry; a missing file is an empty registry."""
    registry_path = Path(path)
    if not registry_path.exists():
        return empty_registry()
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise TokenRegistryError(
            "invalid_registry",
            "Token registry is unreadable or is not valid JSON",
            {"path": str(registry_path), "reason": str(error)},
        )
    return validate_registry_payload(payload)


def atomic_write_registry(path: Path, payload: Mapping[str, Any]) -> None:
    """Validate, fsync, and atomically replace a registry in the same directory."""
    registry_path = Path(path)
    normalized = validate_registry_payload(dict(payload))
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".%s." % registry_path.name,
        suffix=".tmp",
        dir=str(registry_path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(normalized, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(temporary_path), str(registry_path))
        directory_descriptor = os.open(
            str(registry_path.parent),
            os.O_RDONLY,
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


class TokenRegistry:
    """Serialized read-modify-write access to the runtime Token registry."""

    def __init__(self, path: Path = DEFAULT_REGISTRY_PATH) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_name(self.path.name + ".lock")

    def load(self) -> Dict[str, Any]:
        return load_registry(self.path)

    def list_records(
        self,
        statuses: Optional[Iterable[str]] = None,
    ) -> List[Dict[str, Any]]:
        payload = self.load()
        allowed: Optional[Set[str]] = None
        if statuses is not None:
            allowed = {str(status).strip().lower() for status in statuses}
            unknown = allowed - TOKEN_STATUSES
            if unknown:
                raise TokenRegistryError(
                    "invalid_registry_status",
                    "Unknown Token registry statuses: %s" % ", ".join(sorted(unknown)),
                )
        records = [
            dict(record)
            for _key, record in sorted(payload["tokens"].items())
            if allowed is None or record["status"] in allowed
        ]
        return records

    def get(self, chain: Any, contract_address: Any) -> Optional[Dict[str, Any]]:
        key = token_identity_key(chain, contract_address)
        record = self.load()["tokens"].get(key)
        return dict(record) if record is not None else None

    def upsert(
        self,
        record: Mapping[str, Any],
        *,
        reserved_symbols: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        """Atomically insert/update one identity while rejecting symbol aliasing."""
        normalized = normalize_token_record(record)
        key = token_identity_key(
            normalized["chain"],
            normalized["contract_address"],
        )
        reserved = {
            normalize_token_symbol(symbol)
            for symbol in (reserved_symbols or [])
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            payload = load_registry(self.path)
            existing = payload["tokens"].get(key)
            if existing is not None and existing["token_symbol"] != normalized["token_symbol"]:
                raise TokenRegistryError(
                    "identity_conflict",
                    "Contract address is already assigned to a different Token symbol",
                    {
                        "identity_key": key,
                        "existing_symbol": existing["token_symbol"],
                        "requested_symbol": normalized["token_symbol"],
                    },
                )
            for existing_key, existing_record in payload["tokens"].items():
                if (
                    existing_key != key
                    and existing_record["token_symbol"] == normalized["token_symbol"]
                ):
                    raise TokenRegistryError(
                        "symbol_collision",
                        "Token symbol is already assigned to another contract",
                        {
                            "token_symbol": normalized["token_symbol"],
                            "existing_key": existing_key,
                        },
                    )
            if existing is None and normalized["token_symbol"] in reserved:
                raise TokenRegistryError(
                    "symbol_collision",
                    "Token symbol is already reserved by the static catalog",
                    {"token_symbol": normalized["token_symbol"]},
                )
            if existing is not None:
                normalized["created_at"] = existing["created_at"]
                normalized["created_by"] = existing["created_by"]
            payload["tokens"][key] = normalized
            atomic_write_registry(self.path, payload)
            return dict(normalized)
