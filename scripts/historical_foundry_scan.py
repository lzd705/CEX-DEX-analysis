"""Pure historical-window planning and semantic projection.

This module accepts only fixture mappings.  It performs no environment,
filesystem, network, subprocess, storage, or publication operation.
"""

from __future__ import annotations

from decimal import Decimal
import contextvars
import hashlib
import json
import platform
import re
import weakref
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from scripts.historical_foundry_contracts import next_historical_base_fee
import scripts.historical_foundry_rpc as _transport_core
from scripts.historical_foundry_rpc import (
    _ArchiveRpcError,
    _validate_historical_anchor_capture,
)


_HISTORICAL_WINDOW_MODULE_GENERATION = object()


_LOOKBACK_SECONDS = 604_800
_MAX_BLOCK_COUNT = 50_401
_MAX_JSON_NODES = 1_048_576
_MAX_SCALAR_BYTES = 8_388_608
_MAX_STRING_BYTES = 262_144
_MAX_DEPTH = 128
_MAX_NUMERIC_TOKEN_BYTES = 4_096
_MAX_RATIO_TOKEN_BYTES = 4_096
_MAX_RATIO_DECIMAL_OBJECT_BYTES = 2_048
_MAX_UINT64 = (1 << 64) - 1
_MAX_UINT80 = (1 << 80) - 1
_MAX_UINT112 = (1 << 112) - 1
_MAX_UINT256 = (1 << 256) - 1
_NONNEGATIVE_JSON_INT_EXCLUSIVE = 10 ** 4096
_NEGATIVE_JSON_INT_MAGNITUDE_EXCLUSIVE = 10 ** 4095

_HASH32 = re.compile(r"0x[0-9a-f]{64}\Z", re.ASCII)
_HASH64 = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z", re.ASCII)
_QUANTITY = re.compile(r"(?:0x0|0x[1-9a-f][0-9a-f]*)\Z", re.ASCII)

_ANCHOR_CAPTURE_DOMAIN = b"historical_foundry_anchor_capture/v1"
_NORMALIZED_HEADER_DOMAIN = b"historical_foundry_normalized_header/v1"
_LOWER_CAPTURE_DOMAIN = b"historical_foundry_lower_bound_capture/v1"
_REQUEST_DOMAIN = b"historical_foundry_scan_request/v1"
_RESULT_DOMAIN = b"historical_foundry_scan_result/v1"
_RESPONSE_DOMAIN = b"historical_foundry_scan_response/v1"
_HEADER_INVENTORY_DOMAIN = b"historical_foundry_header_inventory/v1"
_RESERVE_INVENTORY_DOMAIN = b"historical_foundry_reserve_inventory/v1"
_PRICE_INVENTORY_DOMAIN = b"historical_foundry_price_inventory/v1"
_FEE_INVENTORY_DOMAIN = b"historical_foundry_fee_inventory/v1"
_FINAL_ANCHOR_DOMAIN = b"historical_foundry_final_anchor_inventory/v1"
_CONTINUOUS_IDS_DOMAIN = b"historical_foundry_continuous_request_ids/v1"

_VENUE_ORDER = ("uniswap_v2", "sushiswap_v2")
_ROOT_BATCH_POLICY = {
    "fee_blocks": 1024,
    "header_requests": 40,
    "price_requests": 40,
    "reserve_blocks": 20,
}
_GET_RESERVES_SELECTOR = "0x0902f1ac"
_LATEST_ROUND_SELECTOR = "0xfeaf968c"

_NORMALIZED_HEADER_FIELDS = frozenset((
    "number", "hash", "parent_hash", "state_root", "timestamp",
    "gas_limit", "gas_used", "base_fee_per_gas",
))
_RAW_HEADER_FIELDS = frozenset((
    "number", "hash", "parentHash", "stateRoot", "timestamp",
    "gasLimit", "gasUsed", "baseFeePerGas",
))
_DESCRIPTOR_FIELDS = frozenset((
    "schema", "kind", "root_index", "block_start", "block_stop",
    "request_id_start", "request_id_stop", "request_count", "requests",
    "allow_http_413_bisection",
))
_WIRE_FIELDS = frozenset(("jsonrpc", "id", "method", "params"))
_SUCCESS_FIELDS = frozenset(("jsonrpc", "id", "result"))

_ERROR_PAIRS = frozenset((
    ("authority_mismatch", "anchor_authority_invalid"),
    ("authority_mismatch", "window_plan_invalid"),
    ("authority_mismatch", "request_ledger_invalid"),
    ("authority_mismatch", "fixture_input_invalid"),
    ("anchor_changed", "final_anchor_mismatch"),
    ("block_coverage_incomplete", "lower_bound_invalid"),
    ("block_coverage_incomplete", "lower_bound_witness_invalid"),
    ("block_coverage_incomplete", "window_resource_limit"),
    ("block_coverage_incomplete", "header_invalid"),
    ("block_coverage_incomplete", "header_continuity_invalid"),
    ("block_coverage_incomplete", "header_coverage_invalid"),
    ("reserve_snapshot_incomplete", "reserve_abi_invalid"),
    ("reserve_snapshot_incomplete", "reserve_coverage_invalid"),
    ("price_snapshot_incomplete", "price_abi_invalid"),
    ("price_snapshot_incomplete", "price_round_invalid"),
    ("price_snapshot_incomplete", "price_freshness_invalid"),
    ("price_snapshot_incomplete", "price_coverage_invalid"),
    ("fee_history_incomplete", "fee_shape_invalid"),
    ("fee_history_incomplete", "fee_coverage_invalid"),
    ("fee_history_incomplete", "fee_header_mismatch"),
))


class HistoricalWindowProjectionError(RuntimeError):
    """Closed pure Task-3a failure classification."""

    __slots__ = ("_reason_code", "_failure_kind", "_sealed")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        raise TypeError("HistoricalWindowProjectionError cannot be subclassed")

    def __init__(self, reason_code: str, failure_kind: str) -> None:
        if (
            type(reason_code) is not str
            or type(failure_kind) is not str
            or (reason_code, failure_kind) not in _ERROR_PAIRS
        ):
            raise ValueError("historical window error classification is invalid")
        RuntimeError.__init__(self, "historical window projection failed")
        object.__setattr__(self, "_reason_code", reason_code)
        object.__setattr__(self, "_failure_kind", failure_kind)
        object.__setattr__(self, "_sealed", True)

    @property
    def reason_code(self) -> str:
        return self._reason_code

    @property
    def failure_kind(self) -> str:
        return self._failure_kind

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("HistoricalWindowProjectionError is immutable")

    def __repr__(self) -> str:
        return "HistoricalWindowProjectionError(<redacted>)"

    def __reduce__(self) -> Any:
        raise TypeError("HistoricalWindowProjectionError is not serializable")


def _failure(reason_code: str, failure_kind: str) -> HistoricalWindowProjectionError:
    return HistoricalWindowProjectionError(reason_code, failure_kind)


def _captured_failure_pair(
    error: HistoricalWindowProjectionError,
    fallback: Tuple[str, str],
) -> Tuple[str, str]:
    """Detach one exact closed classification from its exception history."""
    capture_failed = False
    try:
        if type(error) is not HistoricalWindowProjectionError:
            return fallback
        reason_code = object.__getattribute__(error, "_reason_code")
        failure_kind = object.__getattribute__(error, "_failure_kind")
    except Exception:
        capture_failed = True
    if capture_failed:
        return fallback
    if (
        type(reason_code) is str
        and type(failure_kind) is str
        and (reason_code, failure_kind) in _ERROR_PAIRS
    ):
        return reason_code, failure_kind
    return fallback


def _historical_json_int_token_bytes(value: int) -> bytes:
    """Return a bounded ASCII integer token after an allocation-safe gate."""
    if type(value) is not int:
        raise ValueError("historical integer token is invalid")
    try:
        bit_length = int.bit_length(value)
    except Exception:
        bit_length_failed = True
    else:
        bit_length_failed = False
    if bit_length_failed:
        raise ValueError("historical integer token is invalid")
    if value >= 0:
        if bit_length > 13_607 or (
            bit_length == 13_607 and value >= _NONNEGATIVE_JSON_INT_EXCLUSIVE
        ):
            raise ValueError("historical integer token is too large")
    else:
        if bit_length > 13_604 or (
            bit_length == 13_604
            and value <= -_NEGATIVE_JSON_INT_MAGNITUDE_EXCLUSIVE
        ):
            raise ValueError("historical integer token is too large")
    try:
        token = str(value).encode("ascii")
    except Exception:
        token_failed = True
    else:
        token_failed = False
    if token_failed:
        raise ValueError("historical integer token is invalid")
    if len(token) > _MAX_NUMERIC_TOKEN_BYTES:
        raise ValueError("historical integer token is too large")
    return token


_DECIMAL_LAYOUT_VERIFIED = False
_END_OF_INPUT = object()
_ACTIVE_HEADER_VALIDATION = contextvars.ContextVar(
    "historical_foundry_active_header_validation", default=None
)


def _verify_decimal_layout() -> None:
    global _DECIMAL_LAYOUT_VERIFIED
    if _DECIMAL_LAYOUT_VERIFIED:
        return
    if platform.python_implementation() != "CPython":
        raise ValueError("historical Decimal layout is unsupported")
    expected = ((4095, 1832), (4096, 1832), (4097, 1832),
                (4500, 2000), (4617, 2048), (4618, 2056))
    try:
        observed = tuple(
            (digits, Decimal.__sizeof__(Decimal("9" * digits)))
            for digits, _size in expected
        )
    except Exception:
        layout_failed = True
    else:
        layout_failed = False
    if layout_failed:
        raise ValueError("historical Decimal layout is unsupported")
    if observed != expected:
        raise ValueError("historical Decimal layout is unsupported")
    _DECIMAL_LAYOUT_VERIFIED = True


def _preflight_historical_decimal_tuple(
    value: Decimal,
) -> Tuple[int, Tuple[int, ...], int]:
    """Apply the frozen object gate before materializing a Decimal tuple."""
    if type(value) is not Decimal:
        raise ValueError("historical ratio type is invalid")
    _verify_decimal_layout()
    try:
        size = Decimal.__sizeof__(value)
    except Exception:
        size_failed = True
    else:
        size_failed = False
    if size_failed:
        raise ValueError("historical ratio is invalid")
    if type(size) is not int or size > _MAX_RATIO_DECIMAL_OBJECT_BYTES:
        raise ValueError("historical ratio object is too large")
    try:
        finite = Decimal.is_finite(value)
    except Exception:
        finite_failed = True
    else:
        finite_failed = False
    if finite_failed:
        raise ValueError("historical ratio is invalid")
    if not finite:
        raise ValueError("historical ratio must be finite")
    try:
        row = Decimal.as_tuple(value)
    except Exception:
        tuple_failed = True
    else:
        tuple_failed = False
    if tuple_failed:
        raise ValueError("historical ratio is invalid")
    sign, digits, exponent = row
    if type(sign) is not int or type(digits) is not tuple or type(exponent) is not int:
        raise ValueError("historical ratio tuple is invalid")
    if any(type(digit) is not int or digit < 0 or digit > 9 for digit in digits):
        raise ValueError("historical ratio tuple is invalid")
    return sign, digits, exponent


def _coefficient_from_digits(digits: Tuple[int, ...]) -> int:
    coefficient = 0
    for digit in digits:
        coefficient = coefficient * 10 + digit
    return coefficient


def _ratio_decimal_token(
    value: Any,
    *,
    gas_used: Optional[int] = None,
    gas_limit: Optional[int] = None,
    _preflight: Optional[Tuple[int, Tuple[int, ...], int]] = None,
) -> str:
    """Return the frozen bounded scientific ratio projection."""
    if (gas_used is None) != (gas_limit is None):
        raise ValueError("historical ratio header binding is invalid")
    if gas_used is not None:
        if (
            type(gas_used) is not int
            or type(gas_limit) is not int
            or gas_limit <= 0
            or gas_used < 0
            or gas_used > gas_limit
        ):
            raise ValueError("historical ratio header binding is invalid")
    if type(value) is int:
        if value not in (0, 1):
            raise ValueError("historical ratio integer endpoint is invalid")
        token = str(value)
        is_zero = value == 0
        is_one = value == 1
        coefficient = value
        exponent = 0
    elif type(value) is Decimal:
        if _preflight is None:
            sign, digits, exponent = _preflight_historical_decimal_tuple(value)
        else:
            sign, digits, exponent = _preflight
        if exponent < -8190 or exponent > 4095 or len(digits) > 4096:
            raise ValueError("historical ratio tuple is outside bounds")
        coefficient = _coefficient_from_digits(digits)
        if coefficient == 0:
            if sign != 0:
                raise ValueError("historical ratio signed zero is invalid")
            token = "0"
            is_zero = True
            is_one = False
        else:
            if sign != 0:
                raise ValueError("historical ratio is outside [0,1]")
            adjusted = exponent + len(digits) - 1
            if abs(adjusted) > 4095:
                raise ValueError("historical ratio adjusted exponent is outside bounds")
            if exponent >= 0:
                if coefficient != 1 or exponent != 0:
                    raise ValueError("historical ratio is outside [0,1]")
                is_one = True
            else:
                scale = 10 ** (-exponent)
                if coefficient > scale:
                    raise ValueError("historical ratio is outside [0,1]")
                is_one = coefficient == scale
            is_zero = False
            if is_one:
                token = "1"
            else:
                trailing = 0
                for digit in reversed(digits):
                    if digit != 0:
                        break
                    trailing += 1
                compact_digits = digits[:len(digits) - trailing] if trailing else digits
                compact_exponent = exponent + trailing
                count = len(compact_digits)
                scientific_exponent = compact_exponent + count - 1
                mantissa_length = 1 if count == 1 else count + 1
                exponent_length = len(str(abs(scientific_exponent))) + (
                    1 if scientific_exponent < 0 else 0
                )
                token_length = mantissa_length + 1 + exponent_length
                if token_length > _MAX_RATIO_TOKEN_BYTES:
                    raise ValueError("historical ratio token is too large")
                digit_text = "".join(chr(48 + digit) for digit in compact_digits)
                mantissa = digit_text if count == 1 else digit_text[0] + "." + digit_text[1:]
                token = mantissa + "e" + str(scientific_exponent)
        if len(token.encode("ascii")) > _MAX_RATIO_TOKEN_BYTES:
            raise ValueError("historical ratio token is too large")
    else:
        raise ValueError("historical ratio type is invalid")

    if gas_used is not None:
        if (gas_used == 0) != is_zero or (gas_used == gas_limit) != is_one:
            raise ValueError("historical ratio endpoint differs from header")
        if 0 < gas_used < gas_limit:
            if type(value) is not Decimal or is_zero or is_one or exponent >= 0:
                raise ValueError("historical ratio intermediate type is invalid")
            scale = 10 ** (-exponent)
            if abs(gas_used * scale - gas_limit * coefficient) >= gas_limit:
                raise ValueError("historical ratio differs from header")
    return token


def _guard_historical_json_value(value: Any) -> Mapping[int, Tuple[Any, ...]]:
    """Validate one pure value under the frozen iterative JSON accounting."""
    nodes = 0
    scalar_bytes = 0
    decimal_cache = {}
    pending = [(value, 0)]
    try:
        while pending:
            current, depth = pending.pop()
            nodes += 1
            if nodes > _MAX_JSON_NODES or depth > _MAX_DEPTH:
                raise ValueError("historical pure input exceeds resource limits")
            if type(current) is dict:
                for key, nested in current.items():
                    if type(key) is not str:
                        raise ValueError("historical object key is invalid")
                    encoded = key.encode("utf-8")
                    if len(encoded) > _MAX_STRING_BYTES:
                        raise ValueError("historical string exceeds resource limits")
                    scalar_bytes += len(encoded)
                    pending.append((nested, depth + 1))
            elif type(current) in (list, tuple):
                pending.extend((nested, depth + 1) for nested in current)
            elif type(current) is str:
                encoded = current.encode("utf-8")
                if len(encoded) > _MAX_STRING_BYTES:
                    raise ValueError("historical string exceeds resource limits")
                scalar_bytes += len(encoded)
            elif type(current) is int:
                scalar_bytes += len(_historical_json_int_token_bytes(current))
            elif type(current) is bool:
                scalar_bytes += 4 if current else 5
            elif current is None:
                scalar_bytes += 4
            elif type(current) is Decimal:
                key = id(current)
                projection = decimal_cache.get(key)
                if projection is None:
                    preflight = _preflight_historical_decimal_tuple(current)
                    token = _ratio_decimal_token(current, _preflight=preflight)
                    projection = (current, preflight, token)
                    decimal_cache[key] = projection
                elif projection[0] is not current:
                    raise ValueError("historical ratio cache identity differs")
                scalar_bytes += len(projection[2].encode("ascii"))
            else:
                raise ValueError("historical pure input type is invalid")
            if scalar_bytes > _MAX_SCALAR_BYTES:
                raise ValueError("historical pure input exceeds resource limits")
    except ValueError:
        raise
    except Exception:
        guard_failed = True
    else:
        guard_failed = False
    if guard_failed:
        raise ValueError("historical pure input is invalid")
    return MappingProxyType(decimal_cache)


def _require_raw_json_containers(value: Any) -> None:
    """Reject the internal tuple boundary inside decoded JSON values."""
    pending = [value]
    while pending:
        current = pending.pop()
        if type(current) is dict:
            pending.extend(current.values())
        elif type(current) is list:
            pending.extend(current)
        elif type(current) in (str, int, bool, Decimal) or current is None:
            continue
        else:
            raise ValueError("historical decoded JSON container is invalid")


def _cached_decimal_projection(
    decimal_cache: Mapping[int, Tuple[Any, ...]], value: Decimal
) -> Tuple[Any, ...]:
    projection = decimal_cache.get(id(value))
    if projection is None or projection[0] is not value:
        raise ValueError("historical ratio cache identity differs")
    return projection


def _initialize_production_historical_window_authorities():
    provenance = object()
    prefinalization_registry = {}
    reconciliation_registry = {}
    logical_root_registry = {}

    def _consume_scheduler_logical_root(
        *, claim: Any, spool: Any, logical_root: Any
    ) -> Any:
        entry = logical_root_registry.pop(id(claim), None)
        if (
            entry is None
            or entry[0] is not claim
            or entry[1] is not spool
            or entry[2] is not logical_root
        ):
            return False
        try:
            observed = _typed_hash(
                b"historical_foundry_scheduler_logical_root_authority/v1",
                logical_root,
            )
            detached_observed = _typed_hash(
                b"historical_foundry_scheduler_logical_root_authority/v1",
                entry[4],
            )
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            return None
        if observed != entry[3] or detached_observed != entry[3]:
            return None
        return entry[4]

    def register_weak_authority(
        registry: Dict[int, Any], handle: Any, record: Dict[str, Any]
    ) -> None:
        handle_id = id(handle)

        def retire(reference: Any) -> None:
            entry = registry.get(handle_id)
            if entry is not None and entry[0] is reference:
                registry.pop(handle_id, None)

        reference = weakref.ref(handle, retire)
        registry[handle_id] = (reference, record)

    def reconciliation_replay_digests(
        post_root_ledger: Sequence[Mapping[str, Any]],
        post_leaf_ledger: Sequence[Mapping[str, Any]],
        compact_projection: Mapping[str, Any],
    ) -> Tuple[Any, ...]:
        root_digest = _inventory_hasher(
            b"historical_foundry_reconciliation_post_root_ledger/v1"
        )
        root_count = 0
        for row in post_root_ledger:
            _inventory_update(root_digest, row)
            root_count += 1
        leaf_digest = _inventory_hasher(
            b"historical_foundry_reconciliation_post_leaf_ledger/v1"
        )
        leaf_count = 0
        for row in post_leaf_ledger:
            _inventory_update(leaf_digest, row)
            leaf_count += 1
        return (
            "historical_foundry_reconciliation_digest_binding/v1",
            root_count,
            root_digest.hexdigest(),
            leaf_count,
            leaf_digest.hexdigest(),
            _typed_hash(
                b"historical_foundry_prefinalization_compact_projection/v1",
                compact_projection,
            ),
        )

    def prefinalization_digests(
        plan: Mapping[str, Any],
        frozen_pre_ledger: Sequence[Mapping[str, Any]],
        compact_projection: Mapping[str, Any],
        final_anchor: Mapping[str, Any],
    ) -> Tuple[Any, ...]:
        return (
            "historical_foundry_prefinalization_digest_binding/v1",
            _typed_hash(
                b"historical_foundry_prefinalization_plan/v1", plan
            ),
            _typed_hash(
                b"historical_foundry_prefinalization_pre_ledger/v1",
                frozen_pre_ledger,
            ),
            _typed_hash(
                b"historical_foundry_prefinalization_compact_projection/v1",
                compact_projection,
            ),
            _typed_hash(
                b"historical_foundry_prefinalization_final_anchor/v1",
                final_anchor,
            ),
        )

    def detach_pre_ledger_value(value: Any) -> Any:
        if type(value) is dict:
            return {
                key: detach_pre_ledger_value(item)
                for key, item in value.items()
            }
        if type(value) is tuple:
            return tuple(detach_pre_ledger_value(item) for item in value)
        if type(value) is list:
            return [detach_pre_ledger_value(item) for item in value]
        return value

    def reject_authority() -> None:
        raise _failure("authority_mismatch", "fixture_input_invalid")

    def authority_class(name: str, slots: Tuple[str, ...] = ()):
        def new(cls, *, _provenance: object = None):
            if _provenance is not provenance:
                reject_authority()
            return object.__new__(cls)

        def init_subclass(cls, **_kwargs: Any) -> None:
            raise TypeError(name + " is sealed")

        def representation(self) -> str:
            return name + "(<redacted>)"

        def reject_copy(self, *_args: Any) -> Any:
            raise TypeError(name + " is not copyable")

        def reject_reduce(self) -> Any:
            raise TypeError(name + " is not serializable")

        def reject_setattr(self, _name: str, _value: Any) -> None:
            raise AttributeError(name + " is immutable")

        return type(name, (), {
            "__slots__": slots + ("__weakref__",),
            "__new__": new,
            "__init_subclass__": classmethod(init_subclass),
            "__repr__": representation,
            "__setattr__": reject_setattr,
            "__copy__": reject_copy,
            "__deepcopy__": reject_copy,
            "__reduce__": reject_reduce,
            "__module__": __name__,
        })

    _ProductionHistoricalWindowPreFinalization = authority_class(
        "_ProductionHistoricalWindowPreFinalization", ("_digests",)
    )
    _ProductionHistoricalWindowReconciliation = authority_class(
        "_ProductionHistoricalWindowReconciliation", ("_binding_token",)
    )

    def verify_prefinalization(
        *,
        prefinalization: "_ProductionHistoricalWindowPreFinalization",
        expected_claim: Any,
        expected_spool: Any,
    ) -> None:
        if type(prefinalization) is not _ProductionHistoricalWindowPreFinalization:
            reject_authority()
        entry = prefinalization_registry.get(id(prefinalization))
        if entry is None or entry[0]() is not prefinalization:
            reject_authority()
        record = entry[1]
        if (
            record["state"] != "fresh"
            or record["claim"] is not expected_claim
            or record["spool"] is not expected_spool
            or prefinalization._digests is not record["digests"]
        ):
            reject_authority()
        record["state"] = "consumed_by_claimed_finalizer"
        return None

    receipt_keys = (
        "schema", "exchange_index", "logical_batch_index", "attempt_index",
        "request_byte_count", "request_sha256", "request_ids",
        "wire_byte_count", "wire_sha256", "decoded_byte_count",
        "decoded_sha256", "response_ids", "spool_member_index",
        "spool_offset", "spool_length", "spool_member_sha256",
    )
    summary_keys = (
        "schema", "logical_batch_index", "status",
        "logical_request_byte_count", "logical_request_sha256",
        "logical_request_ids", "attempt_count", "recoverable_failures",
        "success_exchange_indices", "wire_byte_count", "decoded_byte_count",
    )
    pre_leaf_keys = (
        "schema", "segment", "segment_local_index", "leaf_index",
        "logical_batch_index", "request_ids", "request_count",
        "canonical_request_sha256", "response_ids",
        "predicted_success_exchange_index",
    )
    pre_root_keys = {
        "historical_foundry_anchor_stage_pre_root_ledger/v1": (
            "schema", "segment", "stage_index", "stage_name",
            "logical_batch_index", "request_ids", "request_count",
            "canonical_request_byte_count", "canonical_request_sha256",
            "response_ids", "predicted_success_exchange_indices",
            "anchor_capture_sha256", "stage_inventory_row_count",
            "stage_inventory_logical_sha256",
        ),
        "historical_foundry_lower_observation_pre_root_ledger/v1": (
            "schema", "segment", "observation_index", "observation_kind",
            "kind_index", "logical_batch_index", "block_number",
            "request_id", "canonical_request_byte_count",
            "canonical_request_sha256", "response_id",
            "predicted_success_exchange_index", "request_sha256",
            "result_sha256", "response_sha256",
            "lower_bound_capture_sha256",
        ),
        "historical_foundry_window_pre_root_ledger/v1": (
            "schema", "segment", "root_index", "kind", "block_start",
            "block_stop", "logical_batch_index", "request_ids",
            "request_count", "canonical_request_byte_count",
            "canonical_request_sha256", "observed_http_413_intervals",
            "predicted_success_exchange_indices", "typed_role",
            "typed_row_count", "typed_logical_sha256",
        ),
    }

    def bounded_reparse(raw: bytes) -> Any:
        if type(raw) is not bytes:
            raise ValueError("historical physical JSON bytes differ")

        def pairs(rows: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
            result = {}
            for key, value in rows:
                if key in result:
                    raise ValueError("historical JSON key repeats")
                result[key] = value
            return result

        def integer(token: str) -> int:
            if type(token) is not str or len(token.encode("ascii")) > 4_096:
                raise ValueError("historical integer token is too large")
            return int(token)

        def ratio(token: str) -> Decimal:
            if type(token) is not str or len(token.encode("ascii")) > 4_096:
                raise ValueError("historical ratio token is too large")
            value = Decimal(token)
            _preflight_historical_decimal_tuple(value)
            return value

        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_int=integer,
            parse_float=ratio,
            parse_constant=lambda _token: (_ for _ in ()).throw(
                ValueError("historical JSON constant is invalid")
            ),
        )
        _guard_historical_json_value(value)
        return value

    def ledger_groups(
        ledger: Tuple[Mapping[str, Any], ...]
    ) -> Iterator[Tuple[Dict[str, Any], Tuple[Dict[str, Any], ...]]]:
        position = 0
        while position < len(ledger):
            root = ledger[position]
            if type(root) is not dict:
                raise ValueError("historical pre-root type differs")
            expected = pre_root_keys.get(root.get("schema"))
            if expected is None or tuple(root) != expected:
                raise ValueError("historical pre-root schema differs")
            position += 1
            leaves = []
            while position < len(ledger):
                leaf = ledger[position]
                if (
                    type(leaf) is not dict
                    or leaf.get("schema")
                    != "historical_foundry_pre_leaf_ledger/v1"
                ):
                    break
                if tuple(leaf) != pre_leaf_keys:
                    raise ValueError("historical pre-leaf schema differs")
                leaves.append(leaf)
                position += 1
            if not leaves:
                raise ValueError("historical logical root has no leaf")
            yield root, tuple(leaves)

    def reconcile(
        *,
        claim: Any,
        prefinalization: Any,
        finalization: Any,
        sealed_spool: Any,
        frozen_pre_ledger: Sequence[Mapping[str, Any]],
        plan: Mapping[str, Any],
        compact_projection: Mapping[str, Any],
    ) -> "_ProductionHistoricalWindowReconciliation":
        if type(prefinalization) is not _ProductionHistoricalWindowPreFinalization:
            reject_authority()
        entry = prefinalization_registry.get(id(prefinalization))
        if entry is None or entry[0]() is not prefinalization:
            reject_authority()
        pre_record = entry[1]
        if (
            pre_record["state"] != "consumed_by_claimed_finalizer"
            or pre_record["claim"] is not claim
            or pre_record["plan"] is not plan
            or pre_record["frozen_pre_ledger"] is not frozen_pre_ledger
            or pre_record["compact_projection"] is not compact_projection
        ):
            reject_authority()
        cursor = None
        try:
            if (
                type(plan) is not dict
                or type(frozen_pre_ledger) is not tuple
                or type(compact_projection) is not dict
            ):
                raise ValueError("historical reconciliation input differs")
            observed_pre_digests = prefinalization_digests(
                plan,
                frozen_pre_ledger,
                compact_projection,
                pre_record["final_anchor"],
            )
            if (
                prefinalization._digests is not pre_record["digests"]
                or observed_pre_digests != pre_record["digests"]
            ):
                raise ValueError(
                    "historical prefinalization digest binding differs"
                )
            final_projection = dict(finalization)
            if (
                final_projection.get("schema")
                != "historical_foundry_archive_rpc_run_finalization/v1"
                or final_projection.get("status") != "finalized"
                or type(final_projection.get("successful_exchanges"))
                is not tuple
            ):
                raise ValueError("historical finalization differs")
            compact_rows = final_projection["successful_exchanges"]
            summaries = final_projection.get("logical_batches")
            if type(summaries) is not tuple:
                raise ValueError("historical logical summaries differ")
            group_iterator = iter(ledger_groups(frozen_pre_ledger))
            summary_iterator = iter(summaries)
            compact_iterator = iter(compact_rows)
            expected_logical_index = 1
            expected_exchange_index = 1
            expected_member_index = 1
            expected_offset = 0
            global_request_ids = []
            global_response_ids = []
            total_wire_bytes = 0
            total_decoded_bytes = 0
            post_roots = []
            post_leaves = []
            anchor_responses = []
            anchor_root_rows = []
            anchor_capture = None
            lower_probe_raw = []
            lower_witness_raw = []
            lower_root_rows = []
            lower_capture = None
            header_descriptors = None
            header_inventory = None
            state_descriptors = None
            state_validation = None
            replay_record = [None]
            def replay_all():
                nonlocal anchor_capture, lower_capture
                nonlocal header_descriptors, header_inventory
                nonlocal state_descriptors, state_validation
                nonlocal expected_logical_index, expected_exchange_index
                nonlocal expected_member_index, expected_offset
                nonlocal total_wire_bytes, total_decoded_bytes
                cursor = sealed_spool._open_reconciliation_cursor_from_bound_scan(
                    claim=claim, finalization=finalization
                )
                with cursor as stream:
                    for root, leaves in group_iterator:
                        summary = next(summary_iterator)
                        if (
                            type(summary) is not dict
                            or tuple(summary) != summary_keys
                            or summary["schema"]
                            != "historical_foundry_archive_rpc_logical_batch_summary/v1"
                            or summary["status"] != "complete"
                            or root["logical_batch_index"]
                            != expected_logical_index
                            or summary["logical_batch_index"]
                            != expected_logical_index
                        ):
                            raise ValueError("historical logical axis differs")
                        expected_logical_index += 1
                        if root["schema"].endswith(
                            "lower_observation_pre_root_ledger/v1"
                        ):
                            root_request_ids = (root["request_id"],)
                            root_response_ids = (root["response_id"],)
                            root_success_indices = (
                                root["predicted_success_exchange_index"],
                            )
                            intervals = ()
                            segment_local_index = root["observation_index"]
                        else:
                            root_request_ids = root["request_ids"]
                            root_response_ids = root.get("response_ids")
                            root_success_indices = root[
                                "predicted_success_exchange_indices"
                            ]
                            intervals = root.get(
                                "observed_http_413_intervals", ()
                            )
                            segment_local_index = (
                                root["stage_index"]
                                if root["segment"] == "anchor_stage"
                                else root["root_index"]
                            )
                        if (
                            type(root_request_ids) is not tuple
                            or not root_request_ids
                            or type(root_success_indices) is not tuple
                            or len(root_success_indices) != len(leaves)
                            or summary["logical_request_ids"]
                            != root_request_ids
                            or summary["success_exchange_indices"]
                            != root_success_indices
                        ):
                            raise ValueError("historical logical coverage differs")
                        expected_failures = []
                        failure_attempts = []
                        previous_failure_attempt = 0
                        for interval in intervals:
                            if (
                                type(interval) is not dict
                                or tuple(interval) != (
                                    "attempt_index", "first_request_id",
                                    "last_request_id", "request_count",
                                )
                                or type(interval["attempt_index"]) is not int
                                or interval["attempt_index"]
                                <= previous_failure_attempt
                                or type(interval["request_count"]) is not int
                                or interval["request_count"] < 2
                            ):
                                raise ValueError("historical 413 interval differs")
                            previous_failure_attempt = interval["attempt_index"]
                            first = root_request_ids.index(
                                interval["first_request_id"]
                            )
                            request_ids = root_request_ids[
                                first:first + interval["request_count"]
                            ]
                            if (
                                len(request_ids) != interval["request_count"]
                                or request_ids[-1] != interval["last_request_id"]
                            ):
                                raise ValueError("historical 413 coverage differs")
                            expected_failures.append({
                                "attempt_index": interval["attempt_index"],
                                "reason_code": "archive_state_unavailable",
                                "failure_kind": "http_413",
                                "request_ids": request_ids,
                            })
                            failure_attempts.append(interval["attempt_index"])
                        if summary["recoverable_failures"] != tuple(
                            expected_failures
                        ):
                            raise ValueError("historical 413 summary differs")
                        root_requests = {}
                        root_responses = {}
                        success_attempts = []
                        root_wire_bytes = 0
                        root_decoded_bytes = 0
                        physical_leaves = []
                        for leaf_index, leaf in enumerate(leaves):
                            compact_row = next(compact_iterator)
                            receipt, request_bytes, decoded_bytes = next(stream)
                            if (
                                type(compact_row) is not dict
                                or type(receipt) is not dict
                                or tuple(compact_row) != receipt_keys
                                or tuple(receipt) != receipt_keys
                                or compact_row["schema"]
                                != "historical_foundry_archive_rpc_spooled_success_exchange/v1"
                                or receipt["schema"]
                                != "historical_foundry_exchange_spool_receipt/v1"
                                or any(
                                    compact_row[key] != receipt[key]
                                    for key in receipt_keys[1:]
                                )
                            ):
                                raise ValueError(
                                    "historical receipt projection differs"
                                )
                            if (
                                leaf["segment"] != root["segment"]
                                or leaf["segment_local_index"]
                                != segment_local_index
                                or leaf["leaf_index"] != leaf_index
                                or leaf["logical_batch_index"]
                                != root["logical_batch_index"]
                                or leaf["predicted_success_exchange_index"]
                                != expected_exchange_index
                                or receipt["exchange_index"]
                                != expected_exchange_index
                                or receipt["logical_batch_index"]
                                != root["logical_batch_index"]
                                or receipt["spool_member_index"]
                                != expected_member_index
                                or receipt["spool_offset"] != expected_offset
                                or leaf["request_ids"] != receipt["request_ids"]
                                or leaf["response_ids"] != receipt["response_ids"]
                                or leaf["request_count"]
                                != len(leaf["request_ids"])
                                or leaf["canonical_request_sha256"]
                                != receipt["request_sha256"]
                            ):
                                raise ValueError("historical leaf axis differs")
                            if (
                                len(request_bytes) != receipt["request_byte_count"]
                                or hashlib.sha256(request_bytes).hexdigest()
                                != receipt["request_sha256"]
                                or len(decoded_bytes)
                                != receipt["decoded_byte_count"]
                                or hashlib.sha256(decoded_bytes).hexdigest()
                                != receipt["decoded_sha256"]
                            ):
                                raise ValueError("historical leaf hash differs")
                            frame_bytes = (
                                len(request_bytes).to_bytes(8, "big")
                                + request_bytes
                                + len(decoded_bytes).to_bytes(8, "big")
                                + decoded_bytes
                            )
                            if (
                                receipt["spool_length"] != len(frame_bytes)
                                or hashlib.sha256(frame_bytes).hexdigest()
                                != receipt["spool_member_sha256"]
                            ):
                                raise ValueError("historical member frame differs")
                            requests = bounded_reparse(request_bytes)
                            responses = bounded_reparse(decoded_bytes)
                            if (
                                type(requests) is not list
                                or type(responses) is not list
                                or _transport_core._archive_canonical_bytes(requests)
                                != request_bytes
                                or tuple(row.get("id") for row in requests)
                                != receipt["request_ids"]
                                or tuple(row.get("id") for row in responses)
                                != receipt["response_ids"]
                                or len(requests) != len(responses)
                            ):
                                raise ValueError(
                                    "historical physical envelope differs"
                                )
                            for request in requests:
                                request_id = request.get("id")
                                if request_id in root_requests:
                                    raise ValueError(
                                        "historical request ID repeats"
                                    )
                                root_requests[request_id] = request
                            for response in responses:
                                response_id = response.get("id")
                                if response_id in root_responses:
                                    raise ValueError(
                                        "historical response ID repeats"
                                    )
                                root_responses[response_id] = response
                            post_leaf = {
                                "schema": "historical_foundry_leaf_ledger/v1",
                                "segment": root["segment"],
                                "segment_local_index": segment_local_index,
                                "leaf_index": leaf_index,
                                "request_ids": receipt["request_ids"],
                                "request_count": len(receipt["request_ids"]),
                                "canonical_request_sha256": receipt[
                                    "request_sha256"
                                ],
                                "response_ids": receipt["response_ids"],
                                "exchange_index": receipt["exchange_index"],
                                "logical_batch_index": receipt[
                                    "logical_batch_index"
                                ],
                                "attempt_index": receipt["attempt_index"],
                                "request_byte_count": len(request_bytes),
                                "decoded_byte_count": len(decoded_bytes),
                                "decoded_sha256": hashlib.sha256(
                                    decoded_bytes
                                ).hexdigest(),
                                "wire_byte_count": receipt["wire_byte_count"],
                                "wire_sha256": receipt["wire_sha256"],
                                "wire_hash_authority": (
                                    "task2b_sealed_not_rehashed"
                                ),
                                "spool_member_index": receipt[
                                    "spool_member_index"
                                ],
                                "spool_offset": receipt["spool_offset"],
                                "spool_length": len(frame_bytes),
                                "spool_member_sha256": hashlib.sha256(
                                    frame_bytes
                                ).hexdigest(),
                            }
                            physical_leaves.append(post_leaf)
                            success_attempts.append(receipt["attempt_index"])
                            root_wire_bytes += receipt["wire_byte_count"]
                            root_decoded_bytes += len(decoded_bytes)
                            expected_exchange_index += 1
                            expected_member_index += 1
                            expected_offset += len(frame_bytes)
                            del request_bytes, decoded_bytes, frame_bytes
                        if (
                            tuple(sorted(failure_attempts + success_attempts))
                            != tuple(range(1, summary["attempt_count"] + 1))
                            or root_wire_bytes != summary["wire_byte_count"]
                            or root_decoded_bytes != summary["decoded_byte_count"]
                            or set(root_requests) != set(root_request_ids)
                            or set(root_responses) != set(root_request_ids)
                        ):
                            raise ValueError("historical root physical summary differs")
                        ordered_requests = tuple(
                            root_requests[request_id]
                            for request_id in root_request_ids
                        )
                        ordered_responses = tuple(
                            root_responses[request_id]
                            for request_id in root_request_ids
                        )
                        canonical_root = _transport_core._archive_canonical_bytes(
                            list(ordered_requests)
                        )
                        if (
                            len(canonical_root)
                            != root["canonical_request_byte_count"]
                            or hashlib.sha256(canonical_root).hexdigest()
                            != root["canonical_request_sha256"]
                            or summary["logical_request_byte_count"]
                            != len(canonical_root)
                            or summary["logical_request_sha256"]
                            != hashlib.sha256(canonical_root).hexdigest()
                            or (
                                root_response_ids is not None
                                and root_response_ids != tuple(root_request_ids)
                            )
                        ):
                            raise ValueError("historical root canonical request differs")
                        if root["segment"] == "anchor_stage":
                            stage_requests = _transport_core._materialize_historical_anchor_stage(
                                pre_record["anchor_plan"],
                                root["stage_index"],
                                tuple(anchor_responses),
                            )
                            if ordered_requests != stage_requests:
                                raise ValueError("historical anchor requests differ")
                            anchor_responses.extend(ordered_responses)
                            anchor_root_rows.append(root)
                            if root["stage_index"] == 2:
                                anchor_capture = _transport_core.project_historical_anchor_capture(
                                    pre_record["anchor_plan"],
                                    tuple(anchor_responses),
                                )
                                capture_sha = _typed_hash(
                                    _ANCHOR_CAPTURE_DOMAIN, anchor_capture
                                )
                                inventory = anchor_capture["request_inventory"]
                                for stage_root, (start, stop) in zip(
                                    anchor_root_rows,
                                    ((0, 2), (2, 39), (39, 48)),
                                ):
                                    digest = _inventory_hasher(
                                        b"historical_foundry_anchor_stage_inventory/v1"
                                    )
                                    for row in inventory[start:stop]:
                                        _inventory_update(digest, row)
                                    if (
                                        stage_root["anchor_capture_sha256"]
                                        != capture_sha
                                        or stage_root[
                                            "stage_inventory_row_count"
                                        ] != stop - start
                                        or stage_root[
                                            "stage_inventory_logical_sha256"
                                        ] != digest.hexdigest()
                                    ):
                                        raise ValueError(
                                            "historical anchor ledger differs"
                                        )
                        elif root["segment"] == "lower_observation":
                            raw = {
                                "request": ordered_requests[0],
                                "response": ordered_responses[0],
                            }
                            if root["observation_kind"] == "search_probe":
                                lower_probe_raw.append(raw)
                            else:
                                lower_witness_raw.append(raw)
                            lower_root_rows.append(root)
                        else:
                            if lower_capture is None:
                                if anchor_capture is None:
                                    raise ValueError("historical anchor replay missing")
                                lower_capture = project_historical_lower_bound_capture(
                                    anchor_capture=anchor_capture,
                                    lookback_seconds=pre_record[
                                        "lookback_seconds"
                                    ],
                                    search_probes=iter(lower_probe_raw),
                                    boundary_witness=iter(lower_witness_raw),
                                )
                                replay_plan = build_historical_window_request_plan(
                                    lower_bound_capture=lower_capture,
                                    anchor_capture=anchor_capture,
                                )
                                if replay_plan != plan:
                                    raise ValueError("historical plan replay differs")
                                compact_lower = tuple(
                                    lower_capture["search_probes"]
                                ) + tuple(lower_capture["boundary_witness"])
                                for lower_root, compact in zip(
                                    lower_root_rows, compact_lower
                                ):
                                    if (
                                        lower_root["request_sha256"]
                                        != compact["request_sha256"]
                                        or lower_root["result_sha256"]
                                        != compact["result_sha256"]
                                        or lower_root["response_sha256"]
                                        != compact["response_sha256"]
                                        or lower_root[
                                            "lower_bound_capture_sha256"
                                        ] != replay_plan[
                                            "lower_bound_capture_sha256"
                                        ]
                                    ):
                                        raise ValueError(
                                            "historical lower ledger differs"
                                        )
                                header_descriptors = iter_historical_header_request_batches(
                                    plan
                                )
                            if root["kind"] == "header":
                                descriptor = next(header_descriptors)
                                if (
                                    root["root_index"]
                                    != descriptor["root_index"]
                                    or root["block_start"]
                                    != descriptor["block_start"]
                                    or root["block_stop"]
                                    != descriptor["block_stop"]
                                    or ordered_requests
                                    != descriptor["requests"]
                                ):
                                    raise ValueError(
                                        "historical header root differs"
                                    )
                                yield ("header", descriptor, ordered_responses)
                                typed = _project_complete_historical_window_root(
                                    plan=plan,
                                    descriptor=descriptor,
                                    responses=ordered_responses,
                                    header_inventory=None,
                                )
                                if (
                                    root["typed_role"] != typed["typed_role"]
                                    or root["typed_row_count"]
                                    != typed["typed_row_count"]
                                    or root["typed_logical_sha256"]
                                    != typed["typed_logical_sha256"]
                                ):
                                    raise ValueError(
                                        "historical header typed ledger differs"
                                    )
                            else:
                                if state_descriptors is None:
                                    state_descriptors = (
                                        iter_historical_state_request_batches(
                                            plan=plan,
                                            header_inventory=header_inventory,
                                        )
                                    )
                                    state_validation = (
                                        state_descriptors
                                        ._validated_header_token
                                    )
                                descriptor = next(state_descriptors)
                                if (
                                    root["root_index"]
                                    != descriptor["root_index"]
                                    or root["kind"] != descriptor["kind"]
                                    or root["block_start"]
                                    != descriptor["block_start"]
                                    or root["block_stop"]
                                    != descriptor["block_stop"]
                                    or ordered_requests
                                    != descriptor["requests"]
                                ):
                                    raise ValueError(
                                        "historical state root differs"
                                    )
                                validation_token = (
                                    _ACTIVE_HEADER_VALIDATION.set(
                                        state_validation
                                    )
                                )
                                try:
                                    yield (
                                        "state", descriptor, ordered_responses
                                    )
                                    typed = (
                                        _project_complete_historical_window_root(
                                            plan=state_validation[1],
                                            descriptor=descriptor,
                                            responses=ordered_responses,
                                            header_inventory=state_validation[0],
                                        )
                                    )
                                finally:
                                    _ACTIVE_HEADER_VALIDATION.reset(
                                        validation_token
                                    )
                                if (
                                    root["typed_role"] != typed["typed_role"]
                                    or root["typed_row_count"]
                                    != typed["typed_row_count"]
                                    or root["typed_logical_sha256"]
                                    != typed["typed_logical_sha256"]
                                ):
                                    raise ValueError(
                                        "historical state typed ledger differs"
                                    )
                        leaf_digest = _inventory_hasher(
                            b"historical_foundry_leaf_ledger/v1"
                        )
                        for post_leaf in physical_leaves:
                            _inventory_update(leaf_digest, post_leaf)
                            post_leaves.append(post_leaf)
                        post_root = dict(root)
                        post_root["schema"] = {
                            "anchor_stage": "historical_foundry_anchor_stage_root_ledger/v1",
                            "lower_observation": "historical_foundry_lower_observation_root_ledger/v1",
                            "window_root": "historical_foundry_window_root_ledger/v1",
                        }[root["segment"]]
                        post_root.update({
                            "attempt_count": summary["attempt_count"],
                            "success_exchange_indices": tuple(
                                root_success_indices
                            ),
                            "wire_byte_count": root_wire_bytes,
                            "decoded_byte_count": root_decoded_bytes,
                            "leaf_count": len(physical_leaves),
                            "leaf_ledger_sha256": leaf_digest.hexdigest(),
                        })
                        post_roots.append(post_root)
                        global_request_ids.extend(root_request_ids)
                        global_response_ids.extend(root_request_ids)
                        total_wire_bytes += root_wire_bytes
                        total_decoded_bytes += root_decoded_bytes
                    try:
                        next(stream)
                    except StopIteration:
                        pass
                    else:
                        raise ValueError("historical receipt inventory has extras")
                for iterator in (summary_iterator, compact_iterator):
                    try:
                        next(iterator)
                    except StopIteration:
                        pass
                    else:
                        raise ValueError("historical finalization has extras")
                if header_descriptors is not None:
                    try:
                        next(header_descriptors)
                    except StopIteration:
                        pass
                    else:
                        raise ValueError("historical header inventory incomplete")
                if state_descriptors is not None:
                    try:
                        next(state_descriptors)
                    except StopIteration:
                        pass
                    else:
                        raise ValueError("historical state inventory incomplete")
                collection = final_projection.get("identity", {}).get(
                    "collection"
                )
                if (
                    global_request_ids
                    != list(range(1, plan["last_request_id"] + 1))
                    or global_response_ids != global_request_ids
                    or type(collection) is not dict
                    or collection.get("logical_batch_count")
                    != expected_logical_index - 1
                    or collection.get("successful_exchange_count")
                    != expected_exchange_index - 1
                    or collection.get("request_count")
                    != len(global_request_ids)
                    or collection.get("response_count")
                    != len(global_response_ids)
                    or collection.get("wire_byte_count") != total_wire_bytes
                    or collection.get("decoded_byte_count")
                    != total_decoded_bytes
                ):
                    raise ValueError("historical global reconciliation differs")
                replay_record[0] = {
                    "post_root_ledger": tuple(post_roots),
                    "post_leaf_ledger": tuple(post_leaves),
                }
                return

            all_results = replay_all()
            first_header_result = next(all_results)

            def replayed_header_results():
                for root_offset in range(_header_root_count(plan)):
                    tagged = (
                        first_header_result
                        if root_offset == 0
                        else next(all_results)
                    )
                    if (
                        type(tagged) is not tuple
                        or len(tagged) != 3
                        or tagged[0] != "header"
                    ):
                        raise ValueError(
                            "historical header replay order differs"
                        )
                    yield tagged[1], tagged[2]
                    del tagged

            header_results = replayed_header_results()
            state_results = None
            try:
                header_inventory = project_historical_header_inventory(
                    plan=plan,
                    anchor_capture=anchor_capture,
                    lower_bound_capture=lower_capture,
                    batch_results=header_results,
                )

                def replayed_state_results():
                    for tagged in all_results:
                        if (
                            type(tagged) is not tuple
                            or len(tagged) != 3
                            or tagged[0] != "state"
                        ):
                            raise ValueError(
                                "historical state replay order differs"
                            )
                        yield tagged[1], tagged[2]

                state_results = replayed_state_results()
                replayed_projection = project_historical_window_projection(
                    plan=plan,
                    anchor_capture=anchor_capture,
                    lower_bound_capture=lower_capture,
                    header_inventory=header_inventory,
                    batch_results=state_results,
                )
            finally:
                if state_results is not None:
                    state_results.close()
                header_results.close()
                all_results.close()
            if replay_record[0] is None:
                raise ValueError("historical replay did not exhaust")
            if replayed_projection != compact_projection:
                raise ValueError("historical compact projection differs")
            post_root_ledger = replay_record[0]["post_root_ledger"]
            post_leaf_ledger = replay_record[0]["post_leaf_ledger"]
            replay_digests = reconciliation_replay_digests(
                post_root_ledger,
                post_leaf_ledger,
                compact_projection,
            )
            binding_token = object()
            binding = (
                claim,
                prefinalization,
                pre_record["digests"],
                finalization,
                sealed_spool,
                frozen_pre_ledger,
                plan,
                compact_projection,
                post_root_ledger,
                post_leaf_ledger,
                replay_digests,
            )
            reconciliation = _ProductionHistoricalWindowReconciliation(
                _provenance=provenance
            )
            object.__setattr__(
                reconciliation, "_binding_token", binding_token
            )
            register_weak_authority(
                reconciliation_registry,
                reconciliation,
                {
                    "state": "live",
                    "claim": claim,
                    "prefinalization": prefinalization,
                    "prefinalization_digests": pre_record["digests"],
                    "finalization": finalization,
                    "sealed_spool": sealed_spool,
                    "frozen_pre_ledger": frozen_pre_ledger,
                    "plan": plan,
                    "compact_projection": compact_projection,
                    "post_root_ledger": post_root_ledger,
                    "post_leaf_ledger": post_leaf_ledger,
                    "binding_token": binding_token,
                    "binding": binding,
                    "replay_digests": replay_digests,
                },
            )
            return reconciliation
        except _ArchiveRpcError:
            raise
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            ) from None

    def verify_reconciliation(
        *,
        reconciliation: "_ProductionHistoricalWindowReconciliation",
        expected_spool_identity: Any,
        expected_finalization_identity: Any,
    ) -> None:
        if type(reconciliation) is not _ProductionHistoricalWindowReconciliation:
            reject_authority()
        entry = reconciliation_registry.get(id(reconciliation))
        if entry is None or entry[0]() is not reconciliation:
            reject_authority()
        record = entry[1]
        if (
            record["state"] != "live"
            or record["sealed_spool"] is not expected_spool_identity
            or record["finalization"] is not expected_finalization_identity
        ):
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            )
        binding = record.get("binding")
        if (
            type(binding) is not tuple
            or len(binding) != 11
            or reconciliation._binding_token
            is not record.get("binding_token")
            or record.get("claim") is not binding[0]
            or record.get("prefinalization") is not binding[1]
            or record.get("prefinalization_digests") is not binding[2]
            or record.get("finalization") is not binding[3]
            or record.get("sealed_spool") is not binding[4]
            or record.get("frozen_pre_ledger") is not binding[5]
            or record.get("plan") is not binding[6]
            or record.get("compact_projection") is not binding[7]
            or record.get("post_root_ledger") is not binding[8]
            or record.get("post_leaf_ledger") is not binding[9]
            or record.get("replay_digests") is not binding[10]
        ):
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            )
        try:
            pre_entry = prefinalization_registry.get(
                id(record["prefinalization"])
            )
            if (
                pre_entry is None
                or pre_entry[0]() is not record["prefinalization"]
                or pre_entry[1]["digests"]
                is not record["prefinalization_digests"]
                or pre_entry[1]["plan"] is not record["plan"]
                or pre_entry[1]["frozen_pre_ledger"]
                is not record["frozen_pre_ledger"]
                or pre_entry[1]["compact_projection"]
                is not record["compact_projection"]
                or prefinalization_digests(
                    record["plan"],
                    record["frozen_pre_ledger"],
                    record["compact_projection"],
                    pre_entry[1]["final_anchor"],
                ) != record["prefinalization_digests"]
            ):
                raise ValueError(
                    "historical prefinalization digest binding differs"
                )
            observed_digests = reconciliation_replay_digests(
                record["post_root_ledger"],
                record["post_leaf_ledger"],
                record["compact_projection"],
            )
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            ) from None
        if observed_digests != record["replay_digests"]:
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            )
        record["state"] = "consumed_by_mint"
        return None

    def _capture_production_historical_window_core(
        *, claim: Any, spool: Any, delivery_guard: List[Any]
    ) -> "_ProductionHistoricalWindowCapability":
        current_owner = None
        delivered = False
        body_error = [None]
        try:
            config = _transport_core._get_claimed_historical_window_config(
                claim=claim
            )
            policy = config.policy.value
            authority = config.authority.value
            lookback_seconds = policy["lookback_seconds"]
            _transport_core._bind_claimed_historical_window_sources_to_spool(
                claim=claim, spool=spool
            ); current_owner = spool

            frozen_rows = []
            expected_success_exchange_index = [1]
            next_logical_batch_index = [1]

            def execute_root(logical_root: Dict[str, Any]) -> Dict[str, Any]:
                requests = logical_root["requests"]
                claim_id = id(claim)
                try:
                    if logical_root_registry.get(claim_id) is not None:
                        raise ValueError(
                            "historical logical root is already pending"
                        )
                    digest = _typed_hash(
                        b"historical_foundry_scheduler_logical_root_authority/v1",
                        logical_root,
                    )
                    detached_root = detach_pre_ledger_value(logical_root)
                    logical_root_registry[claim_id] = (
                        claim, spool, logical_root, digest, detached_root,
                    )
                    del detached_root
                    logical_scope = (
                        _transport_core._open_production_archive_rpc_historical_window_logical_batch(
                            claim=claim,
                            logical_root=logical_root,
                            spool=spool,
                        )
                    )
                finally:
                    entry = logical_root_registry.get(claim_id)
                    if (
                        entry is not None
                        and entry[0] is claim
                        and entry[1] is spool
                        and entry[2] is logical_root
                    ):
                        logical_root_registry.pop(claim_id, None)
                pending = [requests]
                leaves = []
                intervals = []
                responses_by_id = {}
                attempt_index = 0
                with logical_scope:
                    while pending:
                        request_rows = pending.pop(0)
                        attempt_index += 1
                        try:
                            rows, receipt = (
                                _transport_core._production_archive_rpc_historical_window_logical_batch_attempt(
                                    logical_scope=logical_scope,
                                    request_rows=request_rows,
                                )
                            )
                        except _ArchiveRpcError as error:
                            is_recoverable = (
                                type(error) is _ArchiveRpcError
                                and error.reason_code
                                == "archive_state_unavailable"
                                and error.failure_kind == "http_413"
                                and logical_root[
                                    "allow_http_413_bisection"
                                ] is True
                                and logical_root.get("kind")
                                in ("header", "reserve", "price")
                                and len(request_rows) >= 2
                            )
                            if not is_recoverable:
                                raise
                            request_ids = tuple(
                                row["id"] for row in request_rows
                            )
                            intervals.append({
                                "attempt_index": attempt_index,
                                "first_request_id": request_ids[0],
                                "last_request_id": request_ids[-1],
                                "request_count": len(request_ids),
                            })
                            midpoint = len(request_rows) // 2
                            pending[0:0] = [
                                request_rows[:midpoint],
                                request_rows[midpoint:],
                            ]
                            continue
                        receipt_projection = dict(receipt)
                        if (
                            receipt_projection.get("exchange_index")
                            != expected_success_exchange_index[0]
                        ):
                            raise ValueError(
                                "historical success exchange index differs"
                            )
                        expected_success_exchange_index[0] += 1
                        request_ids = tuple(row["id"] for row in request_rows)
                        leaves.append({
                            "request_ids": request_ids,
                            "request_count": len(request_ids),
                            "canonical_request_sha256": receipt_projection[
                                "request_sha256"
                            ],
                            "response_ids": tuple(
                                receipt_projection["response_ids"]
                            ),
                            "predicted_success_exchange_index": (
                                receipt_projection["exchange_index"]
                            ),
                        })
                        for row in rows:
                            response_id = row.get("id")
                            if response_id in responses_by_id:
                                raise ValueError(
                                    "historical root response ID repeats"
                                )
                            responses_by_id[response_id] = row
                request_ids = tuple(row["id"] for row in requests)
                if set(responses_by_id) != set(request_ids):
                    raise ValueError("historical root response coverage differs")
                root_record = {
                    "logical_root": logical_root,
                    "canonical_request_bytes": _transport_core._archive_canonical_bytes(
                        list(requests)
                    ),
                    "response_ids": request_ids,
                    "observed_http_413_intervals": tuple(intervals),
                    "leaves": leaves,
                    "attempt_count": attempt_index,
                    "typed": None,
                }
                return {
                    "record": root_record,
                    "responses": tuple(
                        responses_by_id[request_id]
                        for request_id in request_ids
                    ),
                }

            def freeze_root(record: Dict[str, Any]) -> None:
                root = record.pop("logical_root")
                requests = root.pop("requests")
                request_ids = tuple(row["id"] for row in requests)
                request_bytes = record.pop("canonical_request_bytes")
                leaves = record.pop("leaves")
                segment = root["segment"]
                typed = record.pop("typed")
                if segment == "anchor_stage":
                    root_row = {
                        "schema": "historical_foundry_anchor_stage_pre_root_ledger/v1",
                        "segment": segment,
                        "stage_index": root["stage_index"],
                        "stage_name": root["stage_name"],
                        "logical_batch_index": root["logical_batch_index"],
                        "request_ids": request_ids,
                        "request_count": len(request_ids),
                        "canonical_request_byte_count": len(request_bytes),
                        "canonical_request_sha256": hashlib.sha256(
                            request_bytes
                        ).hexdigest(),
                        "response_ids": record["response_ids"],
                        "predicted_success_exchange_indices": tuple(
                            row["predicted_success_exchange_index"]
                            for row in leaves
                        ),
                        "anchor_capture_sha256": typed[
                            "anchor_capture_sha256"
                        ],
                        "stage_inventory_row_count": typed[
                            "stage_inventory_row_count"
                        ],
                        "stage_inventory_logical_sha256": typed[
                            "stage_inventory_logical_sha256"
                        ],
                    }
                    segment_local_index = root["stage_index"]
                elif segment == "lower_observation":
                    root_row = {
                        "schema": "historical_foundry_lower_observation_pre_root_ledger/v1",
                        "segment": segment,
                        "observation_index": root["observation_index"],
                        "observation_kind": root["observation_kind"],
                        "kind_index": root["kind_index"],
                        "logical_batch_index": root["logical_batch_index"],
                        "block_number": root["block_number"],
                        "request_id": request_ids[0],
                        "canonical_request_byte_count": len(request_bytes),
                        "canonical_request_sha256": hashlib.sha256(
                            request_bytes
                        ).hexdigest(),
                        "response_id": record["response_ids"][0],
                        "predicted_success_exchange_index": leaves[0][
                            "predicted_success_exchange_index"
                        ],
                        "request_sha256": typed["request_sha256"],
                        "result_sha256": typed["result_sha256"],
                        "response_sha256": typed["response_sha256"],
                        "lower_bound_capture_sha256": typed[
                            "lower_bound_capture_sha256"
                        ],
                    }
                    segment_local_index = root["observation_index"]
                else:
                    if type(typed) is not dict:
                        raise ValueError("historical typed root was not accepted")
                    root_row = {
                        "schema": "historical_foundry_window_pre_root_ledger/v1",
                        "segment": segment,
                        "root_index": root["root_index"],
                        "kind": root["kind"],
                        "block_start": root["block_start"],
                        "block_stop": root["block_stop"],
                        "logical_batch_index": root["logical_batch_index"],
                        "request_ids": request_ids,
                        "request_count": len(request_ids),
                        "canonical_request_byte_count": len(request_bytes),
                        "canonical_request_sha256": hashlib.sha256(
                            request_bytes
                        ).hexdigest(),
                        "observed_http_413_intervals": record[
                            "observed_http_413_intervals"
                        ],
                        "predicted_success_exchange_indices": tuple(
                            row["predicted_success_exchange_index"]
                            for row in leaves
                        ),
                        "typed_role": typed["typed_role"],
                        "typed_row_count": typed["typed_row_count"],
                        "typed_logical_sha256": typed[
                            "typed_logical_sha256"
                        ],
                    }
                    segment_local_index = root["root_index"]
                frozen_rows.append(root_row)
                for leaf_index, leaf in enumerate(leaves):
                    frozen_rows.append({
                        "schema": "historical_foundry_pre_leaf_ledger/v1",
                        "segment": segment,
                        "segment_local_index": segment_local_index,
                        "leaf_index": leaf_index,
                        "logical_batch_index": root["logical_batch_index"],
                        "request_ids": leaf["request_ids"],
                        "request_count": leaf["request_count"],
                        "canonical_request_sha256": leaf[
                            "canonical_request_sha256"
                        ],
                        "response_ids": leaf["response_ids"],
                        "predicted_success_exchange_index": leaf[
                            "predicted_success_exchange_index"
                        ],
                    })
                record.clear()
                root.clear()
                del requests, request_bytes, leaves

            anchor_plan = _transport_core.build_historical_anchor_request_plan(
                policy, authority
            )
            anchor_successes = []
            anchor_roots = []
            for stage_index, stage_name in enumerate((
                "anchor", "fixed_authority", "derived_authority"
            )):
                requests = _transport_core._materialize_historical_anchor_stage(
                    anchor_plan, stage_index, tuple(anchor_successes)
                )
                logical_index = next_logical_batch_index[0]
                next_logical_batch_index[0] += 1
                executed = execute_root({
                    "schema": "historical_foundry_anchor_stage_logical_root/v1",
                    "segment": "anchor_stage",
                    "stage_index": stage_index,
                    "stage_name": stage_name,
                    "logical_batch_index": logical_index,
                    "requests": requests,
                    "allow_http_413_bisection": False,
                })
                anchor_successes.extend(executed["responses"])
                anchor_roots.append(executed["record"])
            try:
                anchor_capture = _transport_core.project_historical_anchor_capture(
                    anchor_plan, tuple(anchor_successes)
                )
            except Exception:
                raise _failure(
                    "authority_mismatch", "anchor_authority_invalid"
                ) from None
            normalized_anchor = _normalized_anchor_from_capture(anchor_capture)
            anchor_capture_sha256 = _typed_hash(
                _ANCHOR_CAPTURE_DOMAIN, anchor_capture
            )
            inventory = anchor_capture["request_inventory"]
            stage_offsets = ((0, 2), (2, 39), (39, 48))
            for record, (start, stop) in zip(anchor_roots, stage_offsets):
                digest = _inventory_hasher(
                    b"historical_foundry_anchor_stage_inventory/v1"
                )
                for row in inventory[start:stop]:
                    _inventory_update(digest, row)
                record["typed"] = {
                    "anchor_capture_sha256": anchor_capture_sha256,
                    "stage_inventory_row_count": stop - start,
                    "stage_inventory_logical_sha256": digest.hexdigest(),
                }
                freeze_root(record)
            anchor_roots.clear()
            anchor_successes.clear()

            probe_raw = []
            probe_roots = []
            witness_raw = []
            witness_roots = []
            next_request_id = [49]
            observation_index = [0]

            def observe(block_number: int, observation_kind: str,
                        kind_index: int) -> Mapping[str, Any]:
                request_id = next_request_id[0]
                next_request_id[0] += 1
                request = _build_historical_block_header_request(
                    block_number=block_number, request_id=request_id
                )
                logical_index = next_logical_batch_index[0]
                next_logical_batch_index[0] += 1
                executed = execute_root({
                    "schema": (
                        "historical_foundry_lower_observation_logical_root/v1"
                    ),
                    "segment": "lower_observation",
                    "observation_index": observation_index[0],
                    "observation_kind": observation_kind,
                    "kind_index": kind_index,
                    "logical_batch_index": logical_index,
                    "block_number": block_number,
                    "requests": (request,),
                    "allow_http_413_bisection": False,
                })
                observation_index[0] += 1
                response = executed["responses"][0]
                raw = {"request": request, "response": response}
                projected = _project_historical_block_header_success(
                    request=request, response=response
                )
                if observation_kind == "search_probe":
                    probe_raw.append(raw)
                    probe_roots.append(executed["record"])
                else:
                    witness_raw.append(raw)
                    witness_roots.append(executed["record"])
                return projected["header"]

            probe_index = [0]

            def header_at_number(block_number: int) -> Mapping[str, Any]:
                result = observe(block_number, "search_probe", probe_index[0])
                probe_index[0] += 1
                return result

            lower_number = locate_inclusive_lower_bound(
                anchor=normalized_anchor,
                header_at_number=header_at_number,
                lookback_seconds=lookback_seconds,
            )
            witness_numbers = (
                (lower_number - 1, lower_number)
                if lower_number > 0 else (0,)
            )
            for kind_index, block_number in enumerate(witness_numbers):
                observe(block_number, "boundary_witness", kind_index)
            lower_capture = project_historical_lower_bound_capture(
                anchor_capture=anchor_capture,
                lookback_seconds=lookback_seconds,
                search_probes=iter(probe_raw),
                boundary_witness=iter(witness_raw),
            )
            plan = build_historical_window_request_plan(
                lower_bound_capture=lower_capture,
                anchor_capture=anchor_capture,
            )
            compact_lower = tuple(lower_capture["search_probes"]) + tuple(
                lower_capture["boundary_witness"]
            )
            for record, compact in zip(
                probe_roots + witness_roots, compact_lower
            ):
                record["typed"] = {
                    "request_sha256": compact["request_sha256"],
                    "result_sha256": compact["result_sha256"],
                    "response_sha256": compact["response_sha256"],
                    "lower_bound_capture_sha256": plan[
                        "lower_bound_capture_sha256"
                    ],
                }
                freeze_root(record)
            probe_roots.clear()
            witness_roots.clear()
            del probe_raw, witness_raw

            header_rpc_error = [None]

            def header_results() -> Iterator[
                Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
            ]:
                try:
                    for descriptor in iter_historical_header_request_batches(plan):
                        logical_index = next_logical_batch_index[0]
                        next_logical_batch_index[0] += 1
                        executed = execute_root({
                            "schema": "historical_foundry_window_logical_root/v1",
                            "segment": "window_root",
                            "root_index": descriptor["root_index"],
                            "kind": descriptor["kind"],
                            "block_start": descriptor["block_start"],
                            "block_stop": descriptor["block_stop"],
                            "logical_batch_index": logical_index,
                            "requests": descriptor["requests"],
                            "allow_http_413_bisection": descriptor[
                                "allow_http_413_bisection"
                            ],
                        })
                        record = executed["record"]
                        responses = executed["responses"]
                        yield descriptor, responses
                        typed = _project_complete_historical_window_root(
                            plan=plan,
                            descriptor=descriptor,
                            responses=responses,
                            header_inventory=None,
                        )
                        record["typed"] = {
                            "typed_role": typed["typed_role"],
                            "typed_row_count": typed["typed_row_count"],
                            "typed_logical_sha256": typed[
                                "typed_logical_sha256"
                            ],
                        }
                        freeze_root(record)
                        del responses, typed, executed
                except _ArchiveRpcError as error:
                    if type(error) is _ArchiveRpcError:
                        header_rpc_error[0] = error
                        return
                    raise

            header_iterator = header_results()
            try:
                header_inventory = project_historical_header_inventory(
                    plan=plan,
                    anchor_capture=anchor_capture,
                    lower_bound_capture=lower_capture,
                    batch_results=header_iterator,
                )
            except HistoricalWindowProjectionError:
                if header_rpc_error[0] is not None:
                    raise header_rpc_error[0]
                raise
            finally:
                header_iterator.close()

            state_rpc_error = [None]

            def state_results() -> Iterator[
                Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
            ]:
                try:
                    descriptors = iter_historical_state_request_batches(
                        plan=plan, header_inventory=header_inventory
                    )
                    state_validation = descriptors._validated_header_token
                    for descriptor in descriptors:
                        logical_index = next_logical_batch_index[0]
                        next_logical_batch_index[0] += 1
                        executed = execute_root({
                            "schema": "historical_foundry_window_logical_root/v1",
                            "segment": "window_root",
                            "root_index": descriptor["root_index"],
                            "kind": descriptor["kind"],
                            "block_start": descriptor["block_start"],
                            "block_stop": descriptor["block_stop"],
                            "logical_batch_index": logical_index,
                            "requests": descriptor["requests"],
                            "allow_http_413_bisection": descriptor[
                                "allow_http_413_bisection"
                            ],
                        })
                        record = executed["record"]
                        responses = executed["responses"]
                        validation_token = _ACTIVE_HEADER_VALIDATION.set(
                            state_validation
                        )
                        try:
                            yield descriptor, responses
                            typed = _project_complete_historical_window_root(
                                plan=state_validation[1],
                                descriptor=descriptor,
                                responses=responses,
                                header_inventory=state_validation[0],
                            )
                        finally:
                            _ACTIVE_HEADER_VALIDATION.reset(validation_token)
                        record["typed"] = {
                            "typed_role": typed["typed_role"],
                            "typed_row_count": typed["typed_row_count"],
                            "typed_logical_sha256": typed[
                                "typed_logical_sha256"
                            ],
                        }
                        freeze_root(record)
                        del responses, typed, executed
                except _ArchiveRpcError as error:
                    if type(error) is _ArchiveRpcError:
                        state_rpc_error[0] = error
                        return
                    raise

            state_iterator = state_results()
            try:
                compact_projection = project_historical_window_projection(
                    plan=plan,
                    anchor_capture=anchor_capture,
                    lower_bound_capture=lower_capture,
                    header_inventory=header_inventory,
                    batch_results=state_iterator,
                )
            except HistoricalWindowProjectionError:
                if state_rpc_error[0] is not None:
                    raise state_rpc_error[0]
                raise
            finally:
                state_iterator.close()

            if (
                compact_projection["boundaries"]["final_anchor_header"]
                != normalized_anchor
            ):
                raise _failure("anchor_changed", "final_anchor_mismatch")

            frozen_pre_ledger = tuple(
                detach_pre_ledger_value(row) for row in frozen_rows
            )
            final_anchor = _validate_normalized_header(
                compact_projection["boundaries"]["final_anchor_header"]
            )
            digests = prefinalization_digests(
                plan,
                frozen_pre_ledger,
                compact_projection,
                final_anchor,
            )
            prefinalization = _ProductionHistoricalWindowPreFinalization(
                _provenance=provenance
            )
            object.__setattr__(prefinalization, "_digests", digests)
            register_weak_authority(
                prefinalization_registry,
                prefinalization,
                {
                    "state": "fresh",
                    "claim": claim,
                    "spool": spool,
                    "plan": plan,
                    "frozen_pre_ledger": frozen_pre_ledger,
                    "compact_projection": compact_projection,
                    "final_anchor": final_anchor,
                    "anchor_plan": anchor_plan,
                    "lookback_seconds": lookback_seconds,
                    "digests": digests,
                },
            )
            finalization = (
                _transport_core._finalize_claimed_production_archive_rpc_run_for_historical_window(
                    claim=claim, prefinalization=prefinalization
                )
            )
            current_owner = sealed_spool = spool.seal()
            reconciliation = reconcile(
                claim=claim,
                prefinalization=prefinalization,
                finalization=finalization,
                sealed_spool=sealed_spool,
                frozen_pre_ledger=frozen_pre_ledger,
                plan=plan,
                compact_projection=compact_projection,
            )
            current_owner = capability = sealed_spool.mint_production_historical_window_capability(
                claim=claim,
                finalization=finalization,
                reconciliation=reconciliation,
            )
            delivery_guard[0] = capability; delivered = True; return capability
        except BaseException as error:
            body_error[0] = error
            raise
        finally:
            cleanup_error = None
            try:
                claim.close()
            except BaseException as error:
                cleanup_error = error
            if not delivered and current_owner is not None:
                try:
                    current_owner.close()
                except BaseException as error:
                    if cleanup_error is None:
                        cleanup_error = error
            if cleanup_error is not None:
                if body_error[0] is None or (
                    isinstance(body_error[0], Exception)
                    and not isinstance(cleanup_error, Exception)
                ):
                    raise cleanup_error

    def capture(
        *, claim: Any, spool: Any
    ) -> "_ProductionHistoricalWindowCapability":
        scheduler_logical_root_consumer = _consume_scheduler_logical_root
        del scheduler_logical_root_consumer
        delivery_guard = [None]
        try:
            result = _capture_production_historical_window_core(
                claim=claim, spool=spool, delivery_guard=delivery_guard
            )
            return result
        except BaseException as error:
            cleanup_error = None
            owner = delivery_guard[0]
            if owner is not None:
                try:
                    owner.close()
                except BaseException as observed:
                    cleanup_error = observed
            if not isinstance(error, Exception):
                raise error
            if cleanup_error is not None and not isinstance(
                cleanup_error, Exception
            ):
                raise cleanup_error
            raise
        raise RuntimeError("unreachable historical window delivery state")

    return (
        _ProductionHistoricalWindowPreFinalization,
        _ProductionHistoricalWindowReconciliation,
        verify_prefinalization,
        reconcile,
        verify_reconciliation,
        capture,
    )


(
    _ProductionHistoricalWindowPreFinalization,
    _ProductionHistoricalWindowReconciliation,
    _verify_production_historical_window_prefinalization,
    _reconcile_production_historical_window,
    _verify_production_historical_window_reconciliation,
    _capture_production_historical_window,
) = _initialize_production_historical_window_authorities()
del _initialize_production_historical_window_authorities


def _canonical_hash_value(
    value: Any,
    decimal_cache: Optional[Mapping[int, Tuple[Any, ...]]] = None,
) -> Any:
    if type(value) is dict:
        return {
            key: _canonical_hash_value(nested, decimal_cache)
            for key, nested in value.items()
        }
    if type(value) in (list, tuple):
        return [_canonical_hash_value(nested, decimal_cache) for nested in value]
    if type(value) is Decimal:
        if decimal_cache is not None:
            return _cached_decimal_projection(decimal_cache, value)[2]
        return _ratio_decimal_token(value)
    return value


def _canonical_json_bytes(
    value: Any,
    decimal_cache: Optional[Mapping[int, Tuple[Any, ...]]] = None,
) -> bytes:
    try:
        return json.dumps(
            _canonical_hash_value(value, decimal_cache),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except Exception:
        canonical_failed = True
    if canonical_failed:
        raise ValueError("historical canonical JSON is invalid")


def _frame(
    value: Any,
    decimal_cache: Optional[Mapping[int, Tuple[Any, ...]]] = None,
) -> bytes:
    payload = _canonical_json_bytes(value, decimal_cache)
    return len(payload).to_bytes(8, "big") + payload


def _typed_hash(
    domain: bytes,
    value: Any,
    decimal_cache: Optional[Mapping[int, Tuple[Any, ...]]] = None,
) -> str:
    return hashlib.sha256(
        domain + b"\0" + _frame(value, decimal_cache)
    ).hexdigest()


def _inventory_hasher(domain: bytes) -> Any:
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(b"\0")
    return digest


def _inventory_update(digest: Any, value: Any) -> None:
    digest.update(_frame(value))


def _require_hash32(value: Any) -> str:
    if type(value) is not str or _HASH32.fullmatch(value) is None:
        raise ValueError("historical header hash is invalid")
    return value


def _require_hash64(value: Any) -> str:
    if type(value) is not str or _HASH64.fullmatch(value) is None:
        raise ValueError("historical digest is invalid")
    return value


def _require_uint(value: Any, maximum: int) -> int:
    if type(value) is not int or value < 0 or value > maximum:
        raise ValueError("historical unsigned integer is invalid")
    return value


def _parse_quantity(value: Any, maximum: int) -> int:
    if type(value) is not str or _QUANTITY.fullmatch(value) is None:
        raise ValueError("historical quantity is invalid")
    maximum_digits = 16 if maximum == _MAX_UINT64 else 64
    if len(value) - 2 > maximum_digits:
        raise ValueError("historical quantity is outside bounds")
    parsed = int(value, 16)
    if parsed > maximum:
        raise ValueError("historical quantity is outside bounds")
    return parsed


def _validate_normalized_header(value: Any) -> Dict[str, Any]:
    if type(value) is not dict or set(value) != _NORMALIZED_HEADER_FIELDS:
        raise ValueError("historical normalized header schema is invalid")
    result = {
        "number": _require_uint(value["number"], _MAX_UINT64),
        "hash": _require_hash32(value["hash"]),
        "parent_hash": _require_hash32(value["parent_hash"]),
        "state_root": _require_hash32(value["state_root"]),
        "timestamp": _require_uint(value["timestamp"], _MAX_UINT64),
        "gas_limit": _require_uint(value["gas_limit"], _MAX_UINT64),
        "gas_used": _require_uint(value["gas_used"], _MAX_UINT64),
        "base_fee_per_gas": _require_uint(value["base_fee_per_gas"], _MAX_UINT256),
    }
    if result["gas_limit"] <= 0 or result["gas_used"] > result["gas_limit"]:
        raise ValueError("historical normalized header gas values are invalid")
    return result


def _normalized_from_raw(value: Any) -> Dict[str, Any]:
    if type(value) is not dict or not _RAW_HEADER_FIELDS.issubset(value):
        raise ValueError("historical raw header schema is invalid")
    return _validate_normalized_header({
        "number": _parse_quantity(value["number"], _MAX_UINT64),
        "hash": value["hash"],
        "parent_hash": value["parentHash"],
        "state_root": value["stateRoot"],
        "timestamp": _parse_quantity(value["timestamp"], _MAX_UINT64),
        "gas_limit": _parse_quantity(value["gasLimit"], _MAX_UINT64),
        "gas_used": _parse_quantity(value["gasUsed"], _MAX_UINT64),
        "base_fee_per_gas": _parse_quantity(value["baseFeePerGas"], _MAX_UINT256),
    })


def _normalized_anchor_from_capture(capture: Mapping[str, Any]) -> Dict[str, Any]:
    anchor = capture["anchor"]
    if type(anchor) is not dict or set(anchor) != _NORMALIZED_HEADER_FIELDS:
        raise ValueError("historical anchor projection schema is invalid")
    return _validate_normalized_header({
        "number": _parse_quantity(anchor["number"], _MAX_UINT64),
        "hash": anchor["hash"],
        "parent_hash": anchor["parent_hash"],
        "state_root": anchor["state_root"],
        "timestamp": _parse_quantity(anchor["timestamp"], _MAX_UINT64),
        "gas_limit": _parse_quantity(anchor["gas_limit"], _MAX_UINT64),
        "gas_used": _parse_quantity(anchor["gas_used"], _MAX_UINT64),
        "base_fee_per_gas": _parse_quantity(anchor["base_fee_per_gas"], _MAX_UINT256),
    })


def _validate_anchor_capture(capture: Mapping[str, Any]) -> Tuple[Dict[str, Any], str, str]:
    failure_pair = ("authority_mismatch", "anchor_authority_invalid")
    try:
        _guard_historical_json_value(capture)
        _validate_historical_anchor_capture(capture)
        anchor = _normalized_anchor_from_capture(capture)
        capture_hash = _typed_hash(_ANCHOR_CAPTURE_DOMAIN, capture)
        header_hash = _typed_hash(_NORMALIZED_HEADER_DOMAIN, anchor)
        return anchor, capture_hash, header_hash
    except _ArchiveRpcError:
        raise
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


def _build_historical_block_header_request(
    *,
    block_number: int,
    request_id: int,
) -> Mapping[str, Any]:
    try:
        _require_uint(block_number, _MAX_UINT64)
        if type(request_id) is not int or request_id <= 0:
            raise ValueError("historical request ID is invalid")
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "eth_getBlockByNumber",
            "params": [hex(block_number), False],
        }
    except Exception:
        build_failed = True
    if build_failed:
        raise _failure("block_coverage_incomplete", "header_invalid")


def _project_historical_block_header_success(
    *,
    request: Mapping[str, Any],
    response: Mapping[str, Any],
) -> Mapping[str, Any]:
    failure_pair = ("block_coverage_incomplete", "header_invalid")
    try:
        _guard_historical_json_value((request, response))
        _require_raw_json_containers(response)
        if type(request) is not dict or set(request) != _WIRE_FIELDS:
            raise ValueError("historical header request is invalid")
        request_id = request["id"]
        if type(request_id) is not int or request_id <= 0:
            raise ValueError("historical header request ID is invalid")
        if (
            request["jsonrpc"] != "2.0"
            or request["method"] != "eth_getBlockByNumber"
            or type(request["params"]) is not list
            or len(request["params"]) != 2
            or request["params"][1] is not False
        ):
            raise ValueError("historical header request differs")
        number = _parse_quantity(request["params"][0], _MAX_UINT64)
        if request != _build_historical_block_header_request(
            block_number=number, request_id=request_id
        ):
            raise ValueError("historical header request differs")
        if (
            type(response) is not dict
            or set(response) != _SUCCESS_FIELDS
            or response["jsonrpc"] != "2.0"
            or type(response["id"]) is not int
            or response["id"] != request_id
        ):
            raise ValueError("historical header response is invalid")
        header = _normalized_from_raw(response["result"])
        if header["number"] != number:
            raise ValueError("historical header number differs")
        return {
            "header": header,
            "result_sha256": _typed_hash(_RESULT_DOMAIN, response["result"]),
            "response_sha256": _typed_hash(_RESPONSE_DOMAIN, response),
        }
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


def locate_inclusive_lower_bound(
    *,
    anchor: Mapping[str, Any],
    header_at_number: Callable[[int], Mapping[str, Any]],
    lookback_seconds: int,
) -> int:
    failure_pair = ("block_coverage_incomplete", "lower_bound_invalid")
    try:
        normalized_anchor = _validate_normalized_header(anchor)
        if type(lookback_seconds) is not int or lookback_seconds <= 0:
            raise ValueError("historical lookback is invalid")
        if not callable(header_at_number):
            raise ValueError("historical header callback is invalid")
        cutoff = normalized_anchor["timestamp"] - lookback_seconds
        lo = 0
        hi = normalized_anchor["number"]
        while lo < hi:
            mid = (lo + hi) // 2
            current = _validate_normalized_header(header_at_number(mid))
            if current["number"] != mid:
                raise ValueError("historical callback header number differs")
            if current["timestamp"] >= cutoff:
                hi = mid
            else:
                lo = mid + 1
        return lo
    except _ArchiveRpcError:
        raise
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


def _next_input(iterator: Any, pair: Tuple[str, str], *, allow_end: bool = False) -> Any:
    input_failed = False
    try:
        return next(iterator)
    except StopIteration:
        if allow_end:
            return _END_OF_INPUT
        input_failed = True
    except Exception:
        input_failed = True
    if input_failed:
        raise _failure(*pair)


def _iterator_once(value: Any, pair: Tuple[str, str]) -> Any:
    try:
        return iter(value)
    except Exception:
        iteration_failed = True
    if iteration_failed:
        raise _failure(*pair)


def _project_lower_observation(
    raw: Any,
    *,
    block_number: int,
    request_id: int,
    pair: Tuple[str, str],
) -> Dict[str, Any]:
    try:
        _guard_historical_json_value(raw)
        if type(raw) is not dict or set(raw) != {"request", "response"}:
            raise ValueError("historical lower observation schema is invalid")
        expected_request = _build_historical_block_header_request(
            block_number=block_number, request_id=request_id
        )
        if raw["request"] != expected_request:
            raise ValueError("historical lower request differs")
        projected = _project_historical_block_header_success(
            request=raw["request"], response=raw["response"]
        )
        return {
            "request_id": request_id,
            "block_number": block_number,
            "header": projected["header"],
            "request_sha256": _typed_hash(_REQUEST_DOMAIN, expected_request),
            "result_sha256": projected["result_sha256"],
            "response_sha256": projected["response_sha256"],
        }
    except HistoricalWindowProjectionError:
        projection_failed = True
    except Exception:
        projection_failed = True
    if projection_failed:
        raise _failure(*pair)


def project_historical_lower_bound_capture(
    *,
    anchor_capture: Mapping[str, Any],
    lookback_seconds: int,
    search_probes: Iterable[Mapping[str, Any]],
    boundary_witness: Iterable[Mapping[str, Any]],
) -> Mapping[str, Any]:
    anchor, anchor_capture_hash, anchor_header_hash = _validate_anchor_capture(
        anchor_capture
    )
    if type(lookback_seconds) is not int or lookback_seconds != _LOOKBACK_SECONDS:
        raise _failure("block_coverage_incomplete", "lower_bound_invalid")
    cutoff = anchor["timestamp"] - lookback_seconds
    probe_iterator = _iterator_once(
        search_probes, ("block_coverage_incomplete", "lower_bound_invalid")
    )
    compact_probes = []
    seen = set()
    lo = 0
    hi = anchor["number"]
    request_id = 49
    while lo < hi:
        mid = (lo + hi) // 2
        if mid in seen:
            raise _failure("block_coverage_incomplete", "lower_bound_invalid")
        seen.add(mid)
        raw = _next_input(
            probe_iterator, ("block_coverage_incomplete", "lower_bound_invalid")
        )
        compact = _project_lower_observation(
            raw, block_number=mid, request_id=request_id,
            pair=("block_coverage_incomplete", "lower_bound_invalid"),
        )
        compact_probes.append(compact)
        del raw
        if compact["header"]["timestamp"] >= cutoff:
            hi = mid
        else:
            lo = mid + 1
        request_id += 1
    if _next_input(
        probe_iterator,
        ("block_coverage_incomplete", "lower_bound_invalid"),
        allow_end=True,
    ) is not _END_OF_INPUT:
        raise _failure("block_coverage_incomplete", "lower_bound_invalid")

    lower = lo
    witness_numbers = (lower - 1, lower) if lower > 0 else (0,)
    witness_iterator = _iterator_once(
        boundary_witness,
        ("block_coverage_incomplete", "lower_bound_witness_invalid"),
    )
    compact_witness = []
    for number in witness_numbers:
        raw = _next_input(
            witness_iterator,
            ("block_coverage_incomplete", "lower_bound_witness_invalid"),
        )
        compact_witness.append(_project_lower_observation(
            raw, block_number=number, request_id=request_id,
            pair=("block_coverage_incomplete", "lower_bound_witness_invalid"),
        ))
        del raw
        request_id += 1
    if _next_input(
        witness_iterator,
        ("block_coverage_incomplete", "lower_bound_witness_invalid"),
        allow_end=True,
    ) is not _END_OF_INPUT:
        raise _failure("block_coverage_incomplete", "lower_bound_witness_invalid")
    if lower == 0:
        if compact_witness[0]["header"]["timestamp"] < cutoff:
            raise _failure("block_coverage_incomplete", "lower_bound_witness_invalid")
    else:
        predecessor = compact_witness[0]["header"]
        lower_header = compact_witness[1]["header"]
        if (
            predecessor["timestamp"] >= cutoff
            or lower_header["timestamp"] < cutoff
            or predecessor["hash"] != lower_header["parent_hash"]
        ):
            raise _failure("block_coverage_incomplete", "lower_bound_witness_invalid")

    request_ids = tuple(range(49, request_id))
    result = {
        "schema": "historical_foundry_lower_bound_capture/v1",
        "chain_id": 1,
        "lookback_seconds": lookback_seconds,
        "cutoff_timestamp": cutoff,
        "anchor_capture_sha256": anchor_capture_hash,
        "anchor_header_sha256": anchor_header_hash,
        "anchor_header": anchor,
        "anchor_number": anchor["number"],
        "anchor_hash": anchor["hash"],
        "lower_bound_number": lower,
        "search_probes": tuple(compact_probes),
        "boundary_witness": tuple(compact_witness),
        "request_ids": request_ids,
        "next_request_id": request_id,
    }
    validation_failed = False
    failure_pair = ("block_coverage_incomplete", "lower_bound_invalid")
    try:
        _guard_historical_json_value(result)
        _validate_lower_capture(result, anchor_capture, anchor)
    except HistoricalWindowProjectionError as error:
        validation_failed = True
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        validation_failed = True
    if validation_failed:
        raise _failure(*failure_pair)
    return result


_LOWER_FIELDS = frozenset((
    "schema", "chain_id", "lookback_seconds", "cutoff_timestamp",
    "anchor_capture_sha256", "anchor_header_sha256", "anchor_header",
    "anchor_number", "anchor_hash", "lower_bound_number", "search_probes",
    "boundary_witness", "request_ids", "next_request_id",
))
_OBSERVATION_FIELDS = frozenset((
    "request_id", "block_number", "header", "request_sha256",
    "result_sha256", "response_sha256",
))


def _validate_compact_observation(
    value: Any, block_number: int, request_id: int
) -> Dict[str, Any]:
    if type(value) is not dict or set(value) != _OBSERVATION_FIELDS:
        raise ValueError("historical compact observation schema is invalid")
    if value["request_id"] != request_id or type(value["request_id"]) is not int:
        raise ValueError("historical compact observation ID differs")
    if value["block_number"] != block_number or type(value["block_number"]) is not int:
        raise ValueError("historical compact observation block differs")
    header = _validate_normalized_header(value["header"])
    if header["number"] != block_number:
        raise ValueError("historical compact observation header differs")
    expected_request = _build_historical_block_header_request(
        block_number=block_number, request_id=request_id
    )
    if value["request_sha256"] != _typed_hash(_REQUEST_DOMAIN, expected_request):
        raise ValueError("historical compact request hash differs")
    _require_hash64(value["result_sha256"])
    _require_hash64(value["response_sha256"])
    return dict(value, header=header)


def _validate_lower_capture(
    value: Mapping[str, Any],
    anchor_capture: Mapping[str, Any],
    anchor: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], str]:
    failure_pair = ("block_coverage_incomplete", "lower_bound_invalid")
    try:
        _guard_historical_json_value(value)
        if type(value) is not dict or set(value) != _LOWER_FIELDS:
            raise ValueError("historical lower capture schema is invalid")
        validated_anchor, capture_hash, anchor_header_hash = _validate_anchor_capture(
            anchor_capture
        )
        if anchor is not None and anchor != validated_anchor:
            raise ValueError("historical lower anchor differs")
        if (
            value["schema"] != "historical_foundry_lower_bound_capture/v1"
            or value["chain_id"] != 1
            or type(value["chain_id"]) is not int
            or value["lookback_seconds"] != _LOOKBACK_SECONDS
            or type(value["lookback_seconds"]) is not int
            or value["anchor_capture_sha256"] != capture_hash
            or value["anchor_header_sha256"] != anchor_header_hash
            or value["anchor_header"] != validated_anchor
            or value["anchor_number"] != validated_anchor["number"]
            or value["anchor_hash"] != validated_anchor["hash"]
            or value["cutoff_timestamp"]
            != validated_anchor["timestamp"] - _LOOKBACK_SECONDS
        ):
            raise ValueError("historical lower capture authority differs")
        if type(value["search_probes"]) is not tuple or type(value["boundary_witness"]) is not tuple:
            raise ValueError("historical lower observations are invalid")
        lo = 0
        hi = validated_anchor["number"]
        request_id = 49
        seen = set()
        probes = []
        probe_index = 0
        cutoff = value["cutoff_timestamp"]
        while lo < hi:
            if probe_index >= len(value["search_probes"]):
                raise ValueError("historical lower probe is missing")
            mid = (lo + hi) // 2
            if mid in seen:
                raise ValueError("historical lower probe repeats")
            seen.add(mid)
            row = _validate_compact_observation(
                value["search_probes"][probe_index], mid, request_id
            )
            probes.append(row)
            if row["header"]["timestamp"] >= cutoff:
                hi = mid
            else:
                lo = mid + 1
            request_id += 1
            probe_index += 1
        if probe_index != len(value["search_probes"]):
            raise ValueError("historical lower probe is extra")
        lower = lo
        if value["lower_bound_number"] != lower or type(value["lower_bound_number"]) is not int:
            raise ValueError("historical lower result differs")
        witness_numbers = (lower - 1, lower) if lower > 0 else (0,)
        if len(value["boundary_witness"]) != len(witness_numbers):
            raise ValueError("historical lower witness count differs")
        witnesses = []
        for index, number in enumerate(witness_numbers):
            witnesses.append(_validate_compact_observation(
                value["boundary_witness"][index], number, request_id
            ))
            request_id += 1
        if lower == 0:
            if witnesses[0]["header"]["timestamp"] < cutoff:
                raise ValueError("historical genesis witness differs")
        elif (
            witnesses[0]["header"]["timestamp"] >= cutoff
            or witnesses[1]["header"]["timestamp"] < cutoff
            or witnesses[0]["header"]["hash"] != witnesses[1]["header"]["parent_hash"]
        ):
            raise ValueError("historical lower witness differs")
        expected_ids = tuple(range(49, request_id))
        if (
            value["request_ids"] != expected_ids
            or type(value["request_ids"]) is not tuple
            or value["next_request_id"] != request_id
            or type(value["next_request_id"]) is not int
        ):
            raise ValueError("historical lower request ledger differs")
        detached = dict(value)
        detached["anchor_header"] = validated_anchor
        detached["search_probes"] = tuple(probes)
        detached["boundary_witness"] = tuple(witnesses)
        detached["request_ids"] = expected_ids
        return detached, _typed_hash(_LOWER_CAPTURE_DOMAIN, value)
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


_PLAN_FIELDS = frozenset((
    "schema", "chain_id", "anchor_capture_sha256",
    "lower_bound_capture_sha256", "anchor_number", "lower_bound_number",
    "block_count", "venue_order", "pair_addresses", "price_feed_proxy",
    "first_request_id", "last_request_id", "request_count",
    "fee_chunk_count", "root_batch_policy",
))


def _anchor_state_authority(capture: Mapping[str, Any]) -> Tuple[Dict[str, str], str]:
    venues = capture["venues"]
    price_feed = capture["price_feed"]
    if type(venues) is not list or len(venues) != 2 or type(price_feed) is not dict:
        raise ValueError("historical anchor state authority is invalid")
    pair_addresses = {}
    for index, venue_id in enumerate(_VENUE_ORDER):
        venue = venues[index]
        if (
            type(venue) is not dict
            or venue.get("venue_id") != venue_id
            or type(venue.get("pair")) is not dict
        ):
            raise ValueError("historical venue authority differs")
        address = venue["pair"].get("address")
        if type(address) is not str or _ADDRESS.fullmatch(address) is None:
            raise ValueError("historical pair authority is invalid")
        pair_addresses[venue_id] = address
    proxy = price_feed.get("proxy")
    if type(proxy) is not dict:
        raise ValueError("historical price proxy authority is invalid")
    proxy_address = proxy.get("address")
    if type(proxy_address) is not str or _ADDRESS.fullmatch(proxy_address) is None:
        raise ValueError("historical price proxy authority is invalid")
    return pair_addresses, proxy_address


def build_historical_window_request_plan(
    *,
    lower_bound_capture: Mapping[str, Any],
    anchor_capture: Mapping[str, Any],
) -> Mapping[str, Any]:
    anchor, anchor_capture_hash, _anchor_header_hash = _validate_anchor_capture(
        anchor_capture
    )
    lower, lower_hash = _validate_lower_capture(
        lower_bound_capture, anchor_capture, anchor
    )
    block_count = anchor["number"] - lower["lower_bound_number"] + 1
    if block_count < 1 or block_count > _MAX_BLOCK_COUNT:
        raise _failure("block_coverage_incomplete", "window_resource_limit")
    failure_pair = ("authority_mismatch", "window_plan_invalid")
    try:
        pair_addresses, proxy_address = _anchor_state_authority(anchor_capture)
        fee_chunk_count = (block_count + 1023) // 1024
        first_request_id = lower["next_request_id"]
        request_count = 4 * block_count + fee_chunk_count + 1
        result = {
            "schema": "historical_foundry_window_request_plan/v1",
            "chain_id": 1,
            "anchor_capture_sha256": anchor_capture_hash,
            "lower_bound_capture_sha256": lower_hash,
            "anchor_number": anchor["number"],
            "lower_bound_number": lower["lower_bound_number"],
            "block_count": block_count,
            "venue_order": _VENUE_ORDER,
            "pair_addresses": pair_addresses,
            "price_feed_proxy": proxy_address,
            "first_request_id": first_request_id,
            "last_request_id": first_request_id + request_count - 1,
            "request_count": request_count,
            "fee_chunk_count": fee_chunk_count,
            "root_batch_policy": dict(_ROOT_BATCH_POLICY),
        }
        _validate_plan_shape(result)
        return result
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


def _validate_plan_shape(value: Mapping[str, Any]) -> Dict[str, Any]:
    failure_pair = ("authority_mismatch", "window_plan_invalid")
    try:
        _guard_historical_json_value(value)
        if type(value) is not dict or set(value) != _PLAN_FIELDS:
            raise ValueError("historical plan schema is invalid")
        if (
            value["schema"] != "historical_foundry_window_request_plan/v1"
            or value["chain_id"] != 1
            or type(value["chain_id"]) is not int
        ):
            raise ValueError("historical plan identity is invalid")
        _require_hash64(value["anchor_capture_sha256"])
        _require_hash64(value["lower_bound_capture_sha256"])
        anchor_number = _require_uint(value["anchor_number"], _MAX_UINT64)
        lower_number = _require_uint(value["lower_bound_number"], _MAX_UINT64)
        block_count = anchor_number - lower_number + 1
        if (
            block_count < 1
            or block_count > _MAX_BLOCK_COUNT
            or value["block_count"] != block_count
            or type(value["block_count"]) is not int
            or value["venue_order"] != _VENUE_ORDER
            or type(value["venue_order"]) is not tuple
            or value["root_batch_policy"] != _ROOT_BATCH_POLICY
            or type(value["root_batch_policy"]) is not dict
        ):
            raise ValueError("historical plan range differs")
        pairs = value["pair_addresses"]
        if type(pairs) is not dict or set(pairs) != set(_VENUE_ORDER):
            raise ValueError("historical plan pair authority is invalid")
        for venue_id in _VENUE_ORDER:
            if type(pairs[venue_id]) is not str or _ADDRESS.fullmatch(pairs[venue_id]) is None:
                raise ValueError("historical plan pair authority is invalid")
        if type(value["price_feed_proxy"]) is not str or _ADDRESS.fullmatch(value["price_feed_proxy"]) is None:
            raise ValueError("historical plan price authority is invalid")
        fee_count = (block_count + 1023) // 1024
        request_count = 4 * block_count + fee_count + 1
        if (
            type(value["first_request_id"]) is not int
            or value["first_request_id"] < 50
            or value["fee_chunk_count"] != fee_count
            or type(value["fee_chunk_count"]) is not int
            or value["request_count"] != request_count
            or type(value["request_count"]) is not int
            or value["last_request_id"] != value["first_request_id"] + request_count - 1
            or type(value["last_request_id"]) is not int
        ):
            raise ValueError("historical plan request ledger differs")
        return {
            "schema": value["schema"],
            "chain_id": value["chain_id"],
            "anchor_capture_sha256": value["anchor_capture_sha256"],
            "lower_bound_capture_sha256": value["lower_bound_capture_sha256"],
            "anchor_number": value["anchor_number"],
            "lower_bound_number": value["lower_bound_number"],
            "block_count": value["block_count"],
            "venue_order": tuple(value["venue_order"]),
            "pair_addresses": dict(value["pair_addresses"]),
            "price_feed_proxy": value["price_feed_proxy"],
            "first_request_id": value["first_request_id"],
            "last_request_id": value["last_request_id"],
            "request_count": value["request_count"],
            "fee_chunk_count": value["fee_chunk_count"],
            "root_batch_policy": dict(value["root_batch_policy"]),
        }
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


class _OneShotCursor(Iterator[Mapping[str, Any]]):
    __slots__ = ("_owner",)

    def __init__(self, owner: "_OneShotIterator") -> None:
        self._owner = owner

    def __iter__(self) -> "_OneShotCursor":
        return self

    def __next__(self) -> Mapping[str, Any]:
        return self._owner._advance()


class _OneShotIterator(Iterator[Mapping[str, Any]]):
    __slots__ = (
        "_iterator", "_mode", "_exhausted", "_validated_header_token"
    )

    def __init__(
        self,
        iterator: Iterator[Mapping[str, Any]],
        validated_header_token: Any = None,
    ) -> None:
        self._iterator = iterator
        self._mode = None
        self._exhausted = False
        self._validated_header_token = validated_header_token

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        if self._mode is not None:
            raise _failure("authority_mismatch", "window_plan_invalid")
        self._mode = "cursor"
        return _OneShotCursor(self)

    def __next__(self) -> Mapping[str, Any]:
        if self._mode is None:
            self._mode = "direct"
        elif self._mode != "direct":
            raise _failure("authority_mismatch", "window_plan_invalid")
        return self._advance()

    def _advance(self) -> Mapping[str, Any]:
        if self._exhausted:
            raise StopIteration
        failure_pair = ("authority_mismatch", "window_plan_invalid")
        try:
            return next(self._iterator)
        except StopIteration:
            self._exhausted = True
            raise
        except HistoricalWindowProjectionError as error:
            self._exhausted = True
            failure_pair = _captured_failure_pair(error, failure_pair)
        except Exception:
            self._exhausted = True
        raise _failure(*failure_pair)


def _make_descriptor(
    *,
    kind: str,
    root_index: int,
    block_start: int,
    block_stop: int,
    requests: Tuple[Mapping[str, Any], ...],
    allow_http_413_bisection: bool,
) -> Dict[str, Any]:
    return {
        "schema": "historical_foundry_window_batch/v1",
        "kind": kind,
        "root_index": root_index,
        "block_start": block_start,
        "block_stop": block_stop,
        "request_id_start": requests[0]["id"],
        "request_id_stop": requests[-1]["id"],
        "request_count": len(requests),
        "requests": requests,
        "allow_http_413_bisection": allow_http_413_bisection,
    }


def _header_descriptor_rows(plan: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    lower = plan["lower_bound_number"]
    anchor = plan["anchor_number"]
    request_id = plan["first_request_id"]
    root_index = 0
    start = lower
    while start <= anchor:
        stop = min(start + 39, anchor)
        requests = tuple(
            _build_historical_block_header_request(
                block_number=number,
                request_id=request_id + number - start,
            )
            for number in range(start, stop + 1)
        )
        yield _make_descriptor(
            kind="header",
            root_index=root_index,
            block_start=start,
            block_stop=stop,
            requests=requests,
            allow_http_413_bisection=len(requests) >= 2,
        )
        request_id += len(requests)
        root_index += 1
        start = stop + 1


def iter_historical_header_request_batches(
    plan: Mapping[str, Any],
) -> Iterator[Mapping[str, Any]]:
    validated = _validate_plan_shape(plan)
    return _OneShotIterator(_header_descriptor_rows(validated))


def _header_root_count(plan: Mapping[str, Any]) -> int:
    return (plan["block_count"] + 39) // 40


def _header_hash_at(
    plan: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], number: int
) -> str:
    offset = number - plan["lower_bound_number"]
    if offset < 0 or offset >= len(rows):
        raise ValueError("historical header hash lookup is outside range")
    row = rows[offset]
    if row["number"] != number:
        raise ValueError("historical header hash lookup differs")
    return row["hash"]


def _state_descriptor_rows(
    plan: Mapping[str, Any], header_rows: Sequence[Mapping[str, Any]]
) -> Iterator[Mapping[str, Any]]:
    lower = plan["lower_bound_number"]
    anchor = plan["anchor_number"]
    block_count = plan["block_count"]
    request_id = plan["first_request_id"] + block_count
    root_index = _header_root_count(plan)

    start = lower
    while start <= anchor:
        stop = min(start + 19, anchor)
        requests = []
        for number in range(start, stop + 1):
            block_hash = _header_hash_at(plan, header_rows, number)
            for venue_id in _VENUE_ORDER:
                requests.append({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "eth_call",
                    "params": [
                        {
                            "to": plan["pair_addresses"][venue_id],
                            "data": _GET_RESERVES_SELECTOR,
                        },
                        {"blockHash": block_hash, "requireCanonical": True},
                    ],
                })
                request_id += 1
        request_tuple = tuple(requests)
        yield _make_descriptor(
            kind="reserve", root_index=root_index,
            block_start=start, block_stop=stop, requests=request_tuple,
            allow_http_413_bisection=len(request_tuple) >= 2,
        )
        root_index += 1
        start = stop + 1

    start = lower
    while start <= anchor:
        stop = min(start + 39, anchor)
        requests = []
        for number in range(start, stop + 1):
            block_hash = _header_hash_at(plan, header_rows, number)
            requests.append({
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "eth_call",
                "params": [
                    {"to": plan["price_feed_proxy"], "data": _LATEST_ROUND_SELECTOR},
                    {"blockHash": block_hash, "requireCanonical": True},
                ],
            })
            request_id += 1
        request_tuple = tuple(requests)
        yield _make_descriptor(
            kind="price", root_index=root_index,
            block_start=start, block_stop=stop, requests=request_tuple,
            allow_http_413_bisection=len(request_tuple) >= 2,
        )
        root_index += 1
        start = stop + 1

    start = lower
    while start <= anchor:
        stop = min(start + 1023, anchor)
        count = stop - start + 1
        requests = ({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "eth_feeHistory",
            "params": [hex(count), hex(stop), [50, 90]],
        },)
        yield _make_descriptor(
            kind="fee_history", root_index=root_index,
            block_start=start, block_stop=stop, requests=requests,
            allow_http_413_bisection=False,
        )
        request_id += 1
        root_index += 1
        start = stop + 1

    final_request = _build_historical_block_header_request(
        block_number=anchor, request_id=request_id
    )
    yield _make_descriptor(
        kind="final_anchor", root_index=root_index,
        block_start=anchor, block_stop=anchor, requests=(final_request,),
        allow_http_413_bisection=False,
    )


def _expected_descriptor(
    plan: Mapping[str, Any],
    *,
    kind: str,
    root_index: int,
    block_start: int,
    block_stop: int,
    header_rows: Optional[Sequence[Mapping[str, Any]]],
) -> Mapping[str, Any]:
    lower = plan["lower_bound_number"]
    anchor = plan["anchor_number"]
    block_count = plan["block_count"]
    header_roots = (block_count + 39) // 40
    reserve_roots = (block_count + 19) // 20
    price_roots = header_roots
    fee_roots = plan["fee_chunk_count"]
    if kind == "header":
        local = root_index
        if local < 0 or local >= header_roots:
            raise ValueError("historical header root is outside plan")
        start = lower + local * 40
        stop = min(start + 39, anchor)
        request_start = plan["first_request_id"] + (start - lower)
        requests = tuple(
            _build_historical_block_header_request(
                block_number=number,
                request_id=request_start + number - start,
            )
            for number in range(start, stop + 1)
        )
        expected = _make_descriptor(
            kind=kind, root_index=root_index, block_start=start, block_stop=stop,
            requests=requests, allow_http_413_bisection=len(requests) >= 2,
        )
    elif kind == "reserve":
        if header_rows is None:
            raise ValueError("historical reserve header inventory is absent")
        local = root_index - header_roots
        if local < 0 or local >= reserve_roots:
            raise ValueError("historical reserve root is outside plan")
        start = lower + local * 20
        stop = min(start + 19, anchor)
        request_id = plan["first_request_id"] + block_count + local * 40
        requests_list = []
        for number in range(start, stop + 1):
            block_hash = _header_hash_at(plan, header_rows, number)
            for venue_id in _VENUE_ORDER:
                requests_list.append({
                    "jsonrpc": "2.0", "id": request_id, "method": "eth_call",
                    "params": [
                        {"to": plan["pair_addresses"][venue_id],
                         "data": _GET_RESERVES_SELECTOR},
                        {"blockHash": block_hash, "requireCanonical": True},
                    ],
                })
                request_id += 1
        requests = tuple(requests_list)
        expected = _make_descriptor(
            kind=kind, root_index=root_index, block_start=start, block_stop=stop,
            requests=requests, allow_http_413_bisection=len(requests) >= 2,
        )
    elif kind == "price":
        if header_rows is None:
            raise ValueError("historical price header inventory is absent")
        local = root_index - header_roots - reserve_roots
        if local < 0 or local >= price_roots:
            raise ValueError("historical price root is outside plan")
        start = lower + local * 40
        stop = min(start + 39, anchor)
        request_id = plan["first_request_id"] + 3 * block_count + local * 40
        requests_list = []
        for number in range(start, stop + 1):
            block_hash = _header_hash_at(plan, header_rows, number)
            requests_list.append({
                "jsonrpc": "2.0", "id": request_id, "method": "eth_call",
                "params": [
                    {"to": plan["price_feed_proxy"], "data": _LATEST_ROUND_SELECTOR},
                    {"blockHash": block_hash, "requireCanonical": True},
                ],
            })
            request_id += 1
        requests = tuple(requests_list)
        expected = _make_descriptor(
            kind=kind, root_index=root_index, block_start=start, block_stop=stop,
            requests=requests, allow_http_413_bisection=len(requests) >= 2,
        )
    elif kind == "fee_history":
        local = root_index - header_roots - reserve_roots - price_roots
        if local < 0 or local >= fee_roots:
            raise ValueError("historical fee root is outside plan")
        start = lower + local * 1024
        stop = min(start + 1023, anchor)
        count = stop - start + 1
        request_id = plan["first_request_id"] + 4 * block_count + local
        requests = ({
            "jsonrpc": "2.0", "id": request_id, "method": "eth_feeHistory",
            "params": [hex(count), hex(stop), [50, 90]],
        },)
        expected = _make_descriptor(
            kind=kind, root_index=root_index, block_start=start, block_stop=stop,
            requests=requests, allow_http_413_bisection=False,
        )
    elif kind == "final_anchor":
        expected_root = header_roots + reserve_roots + price_roots + fee_roots
        if root_index != expected_root:
            raise ValueError("historical final root is outside plan")
        request = _build_historical_block_header_request(
            block_number=anchor, request_id=plan["last_request_id"]
        )
        expected = _make_descriptor(
            kind=kind, root_index=root_index, block_start=anchor,
            block_stop=anchor, requests=(request,),
            allow_http_413_bisection=False,
        )
    else:
        raise ValueError("historical descriptor kind is invalid")
    if expected["block_start"] != block_start or expected["block_stop"] != block_stop:
        raise ValueError("historical descriptor block range differs")
    return expected


def _validate_descriptor(
    plan: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    *,
    expected: Mapping[str, Any],
) -> Dict[str, Any]:
    if type(descriptor) is not dict or set(descriptor) != _DESCRIPTOR_FIELDS:
        raise ValueError("historical descriptor schema is invalid")
    if type(descriptor["requests"]) is not tuple:
        raise ValueError("historical descriptor requests are not a tuple")
    if descriptor != expected:
        raise ValueError("historical descriptor differs from plan")
    count = descriptor["request_count"]
    if (
        type(count) is not int
        or count != len(descriptor["requests"])
        or descriptor["request_id_stop"] - descriptor["request_id_start"] + 1 != count
        or tuple(request["id"] for request in descriptor["requests"])
        != tuple(range(descriptor["request_id_start"], descriptor["request_id_stop"] + 1))
    ):
        raise ValueError("historical descriptor request ledger differs")
    for request in descriptor["requests"]:
        if type(request) is not dict or set(request) != _WIRE_FIELDS:
            raise ValueError("historical descriptor request schema is invalid")
    return descriptor


_HEADER_ROW_FIELDS = frozenset((
    "request_id", "number", "hash", "parent_hash", "state_root", "timestamp",
    "gas_limit", "gas_used", "base_fee_per_gas", "result_sha256",
    "response_sha256",
))
_HEADER_INVENTORY_FIELDS = frozenset((
    "schema", "anchor_capture_sha256", "lower_bound_capture_sha256",
    "anchor_header_sha256", "lower_header_sha256", "lower_bound_number",
    "anchor_number", "row_count", "rows", "logical_sha256",
))


def _validate_bound_inputs(
    plan: Mapping[str, Any],
    anchor_capture: Mapping[str, Any],
    lower_bound_capture: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    validated_plan = _validate_plan_shape(plan)
    anchor, anchor_hash, _anchor_header_hash = _validate_anchor_capture(anchor_capture)
    lower, lower_hash = _validate_lower_capture(lower_bound_capture, anchor_capture, anchor)
    failure_pair = ("authority_mismatch", "window_plan_invalid")
    try:
        pairs, proxy = _anchor_state_authority(anchor_capture)
        if (
            validated_plan["anchor_capture_sha256"] != anchor_hash
            or validated_plan["lower_bound_capture_sha256"] != lower_hash
            or validated_plan["anchor_number"] != anchor["number"]
            or validated_plan["lower_bound_number"] != lower["lower_bound_number"]
            or validated_plan["first_request_id"] != lower["next_request_id"]
            or validated_plan["pair_addresses"] != pairs
            or validated_plan["price_feed_proxy"] != proxy
        ):
            raise ValueError("historical plan authority differs")
        return validated_plan, anchor, lower
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


def _header_row_from_projection(
    request_id: int, projected: Mapping[str, Any]
) -> Dict[str, Any]:
    header = projected["header"]
    return {
        "request_id": request_id,
        "number": header["number"],
        "hash": header["hash"],
        "parent_hash": header["parent_hash"],
        "state_root": header["state_root"],
        "timestamp": header["timestamp"],
        "gas_limit": header["gas_limit"],
        "gas_used": header["gas_used"],
        "base_fee_per_gas": header["base_fee_per_gas"],
        "result_sha256": projected["result_sha256"],
        "response_sha256": projected["response_sha256"],
    }


def _header_from_inventory_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {key: row[key] for key in _NORMALIZED_HEADER_FIELDS}


def _validate_header_inventory(
    *,
    plan: Mapping[str, Any],
    header_inventory: Mapping[str, Any],
    anchor: Optional[Mapping[str, Any]] = None,
    lower: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], Tuple[Mapping[str, Any], ...]]:
    failure_pair = ("block_coverage_incomplete", "header_invalid")
    try:
        if (
            type(header_inventory) is not dict
            or set(header_inventory) != _HEADER_INVENTORY_FIELDS
            or header_inventory["schema"] != "historical_foundry_header_inventory/v1"
            or header_inventory["anchor_capture_sha256"]
            != plan["anchor_capture_sha256"]
            or header_inventory["lower_bound_capture_sha256"]
            != plan["lower_bound_capture_sha256"]
            or header_inventory["lower_bound_number"] != plan["lower_bound_number"]
            or header_inventory["anchor_number"] != plan["anchor_number"]
            or type(header_inventory["rows"]) is not tuple
            or header_inventory["row_count"] != plan["block_count"]
            or type(header_inventory["row_count"]) is not int
            or len(header_inventory["rows"]) != plan["block_count"]
        ):
            raise ValueError("historical header inventory schema differs")
        rows = []
        digest = _inventory_hasher(_HEADER_INVENTORY_DOMAIN)
        previous = None
        for offset, raw_row in enumerate(header_inventory["rows"]):
            if type(raw_row) is not dict or set(raw_row) != _HEADER_ROW_FIELDS:
                raise ValueError("historical header inventory row is invalid")
            number = plan["lower_bound_number"] + offset
            expected_request_id = plan["first_request_id"] + offset
            header = _validate_normalized_header(_header_from_inventory_row(raw_row))
            if (
                raw_row["request_id"] != expected_request_id
                or type(raw_row["request_id"]) is not int
                or header["number"] != number
            ):
                raise ValueError("historical header inventory coverage differs")
            _require_hash64(raw_row["result_sha256"])
            _require_hash64(raw_row["response_sha256"])
            if previous is not None and (
                header["parent_hash"] != previous["hash"]
                or header["timestamp"] <= previous["timestamp"]
            ):
                raise ValueError("historical header inventory continuity differs")
            row = dict(raw_row)
            rows.append(row)
            _inventory_update(digest, row)
            previous = header
        first_header = _header_from_inventory_row(rows[0])
        last_header = _header_from_inventory_row(rows[-1])
        if (
            header_inventory["anchor_header_sha256"]
            != _typed_hash(_NORMALIZED_HEADER_DOMAIN, last_header)
            or header_inventory["lower_header_sha256"]
            != _typed_hash(_NORMALIZED_HEADER_DOMAIN, first_header)
            or header_inventory["logical_sha256"] != digest.hexdigest()
        ):
            raise ValueError("historical header inventory digest differs")
        if anchor is not None and last_header != anchor:
            raise ValueError("historical header inventory anchor differs")
        if lower is not None:
            witness_rows = lower["boundary_witness"]
            lower_header = witness_rows[-1]["header"]
            if first_header != lower_header:
                raise ValueError("historical header inventory lower boundary differs")
            for observation in lower["search_probes"]:
                number = observation["block_number"]
                if plan["lower_bound_number"] <= number <= plan["anchor_number"]:
                    full = _header_from_inventory_row(
                        rows[number - plan["lower_bound_number"]]
                    )
                    if full != observation["header"]:
                        raise ValueError("historical probe and full header differ")
        detached_rows = tuple(rows)
        detached_inventory = dict(header_inventory)
        detached_inventory["rows"] = detached_rows
        return detached_inventory, detached_rows
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


def _project_header_root(
    *,
    plan: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    failure_pair = ("block_coverage_incomplete", "header_invalid")
    try:
        expected = _expected_descriptor(
            plan, kind="header", root_index=descriptor["root_index"],
            block_start=descriptor["block_start"], block_stop=descriptor["block_stop"],
            header_rows=None,
        )
        validated = _validate_descriptor(plan, descriptor, expected=expected)
        if type(responses) not in (list, tuple) or len(responses) != validated["request_count"]:
            raise ValueError("historical header response count differs")
        rows = []
        for request, response in zip(validated["requests"], responses):
            projected = _project_historical_block_header_success(
                request=request, response=response
            )
            rows.append(_header_row_from_projection(request["id"], projected))
        digest = _inventory_hasher(_HEADER_INVENTORY_DOMAIN)
        for row in rows:
            _inventory_update(digest, row)
        return {
            "kind": "header",
            "root_index": validated["root_index"],
            "block_start": validated["block_start"],
            "block_stop": validated["block_stop"],
            "request_ids": tuple(request["id"] for request in validated["requests"]),
            "typed_role": "headers",
            "typed_row_count": len(rows),
            "typed_logical_sha256": digest.hexdigest(),
            "rows": tuple(rows),
        }
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


def project_historical_header_inventory(
    *,
    plan: Mapping[str, Any],
    anchor_capture: Mapping[str, Any],
    lower_bound_capture: Mapping[str, Any],
    batch_results: Iterable[
        Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
    ],
) -> Mapping[str, Any]:
    validated_plan, anchor, lower = _validate_bound_inputs(
        plan, anchor_capture, lower_bound_capture
    )
    expected_iterator = _header_descriptor_rows(validated_plan)
    result_iterator = _iterator_once(
        batch_results, ("block_coverage_incomplete", "header_coverage_invalid")
    )
    rows = []
    digest = _inventory_hasher(_HEADER_INVENTORY_DOMAIN)
    previous = None
    for expected in expected_iterator:
        raw_pair = _next_input(
            result_iterator,
            ("block_coverage_incomplete", "header_coverage_invalid"),
        )
        root_failed = False
        failure_pair = (
            "block_coverage_incomplete", "header_coverage_invalid"
        )
        try:
            if type(raw_pair) is not tuple or len(raw_pair) != 2:
                raise ValueError("historical header batch result is invalid")
            descriptor, responses = raw_pair
            if descriptor != expected:
                raise ValueError("historical header descriptor order differs")
            root = _project_complete_historical_window_root(
                plan=validated_plan,
                descriptor=descriptor,
                responses=responses,
                header_inventory=None,
            )
        except HistoricalWindowProjectionError as error:
            root_failed = True
            failure_pair = _captured_failure_pair(error, failure_pair)
        except Exception:
            root_failed = True
        if root_failed:
            raise _failure(*failure_pair)
        for row in root["rows"]:
            header = _header_from_inventory_row(row)
            if previous is not None and (
                header["number"] != previous["number"] + 1
                or header["parent_hash"] != previous["hash"]
                or header["timestamp"] <= previous["timestamp"]
            ):
                raise _failure("block_coverage_incomplete", "header_continuity_invalid")
            rows.append(row)
            _inventory_update(digest, row)
            previous = header
        del raw_pair, root
    if _next_input(
        result_iterator,
        ("block_coverage_incomplete", "header_coverage_invalid"),
        allow_end=True,
    ) is not _END_OF_INPUT:
        raise _failure("block_coverage_incomplete", "header_coverage_invalid")
    lower_header = lower["boundary_witness"][-1]["header"]
    if not rows or _header_from_inventory_row(rows[0]) != lower_header:
        raise _failure("block_coverage_incomplete", "header_coverage_invalid")
    if _header_from_inventory_row(rows[-1]) != anchor:
        raise _failure("block_coverage_incomplete", "header_coverage_invalid")
    for observation in lower["search_probes"]:
        number = observation["block_number"]
        if validated_plan["lower_bound_number"] <= number <= validated_plan["anchor_number"]:
            if (
                _header_from_inventory_row(
                    rows[number - validated_plan["lower_bound_number"]]
                )
                != observation["header"]
            ):
                raise _failure("block_coverage_incomplete", "header_coverage_invalid")
    inventory = {
        "schema": "historical_foundry_header_inventory/v1",
        "anchor_capture_sha256": validated_plan["anchor_capture_sha256"],
        "lower_bound_capture_sha256": validated_plan["lower_bound_capture_sha256"],
        "anchor_header_sha256": _typed_hash(_NORMALIZED_HEADER_DOMAIN, anchor),
        "lower_header_sha256": _typed_hash(_NORMALIZED_HEADER_DOMAIN, lower_header),
        "lower_bound_number": validated_plan["lower_bound_number"],
        "anchor_number": validated_plan["anchor_number"],
        "row_count": len(rows),
        "rows": tuple(rows),
        "logical_sha256": digest.hexdigest(),
    }
    _validate_header_inventory(
        plan=validated_plan, header_inventory=inventory, anchor=anchor, lower=lower
    )
    return inventory


def iter_historical_state_request_batches(
    *,
    plan: Mapping[str, Any],
    header_inventory: Mapping[str, Any],
) -> Iterator[Mapping[str, Any]]:
    validated_plan = _validate_plan_shape(plan)
    inventory, rows = _validate_header_inventory(
        plan=validated_plan, header_inventory=header_inventory
    )
    return _OneShotIterator(
        _state_descriptor_rows(validated_plan, rows),
        validated_header_token=(inventory, validated_plan),
    )


def _hex_payload(value: Any, size: int) -> bytes:
    if type(value) is not str or len(value) != 2 + 2 * size or not value.startswith("0x"):
        raise ValueError("historical ABI payload length is invalid")
    try:
        payload = bytes.fromhex(value[2:])
    except Exception:
        payload_failed = True
    else:
        payload_failed = False
    if payload_failed:
        raise ValueError("historical ABI payload encoding is invalid")
    if len(payload) != size:
        raise ValueError("historical ABI payload length is invalid")
    return payload


def _response_result(
    request: Mapping[str, Any],
    response: Mapping[str, Any],
    decimal_cache: Mapping[int, Tuple[Any, ...]],
) -> Tuple[Any, str, str]:
    _require_raw_json_containers(response)
    if (
        type(response) is not dict
        or set(response) != _SUCCESS_FIELDS
        or response["jsonrpc"] != "2.0"
        or type(response["id"]) is not int
        or response["id"] != request["id"]
    ):
        raise ValueError("historical response identity differs")
    return (
        response["result"],
        _typed_hash(_RESULT_DOMAIN, response["result"], decimal_cache),
        _typed_hash(_RESPONSE_DOMAIN, response, decimal_cache),
    )


def _root_header_rows(
    plan: Mapping[str, Any],
    header_inventory: Mapping[str, Any],
    start: int,
    stop: int,
) -> Tuple[Mapping[str, Any], ...]:
    if (
        type(header_inventory) is not dict
        or set(header_inventory) != _HEADER_INVENTORY_FIELDS
        or header_inventory["schema"] != "historical_foundry_header_inventory/v1"
        or header_inventory["anchor_capture_sha256"] != plan["anchor_capture_sha256"]
        or header_inventory["lower_bound_capture_sha256"] != plan["lower_bound_capture_sha256"]
        or header_inventory["lower_bound_number"] != plan["lower_bound_number"]
        or header_inventory["anchor_number"] != plan["anchor_number"]
        or type(header_inventory["rows"]) is not tuple
        or len(header_inventory["rows"]) != plan["block_count"]
    ):
        raise ValueError("historical root header inventory differs")
    rows = []
    for number in range(start, stop + 1):
        raw = header_inventory["rows"][number - plan["lower_bound_number"]]
        if type(raw) is not dict or set(raw) != _HEADER_ROW_FIELDS:
            raise ValueError("historical root header row is invalid")
        header = _validate_normalized_header(_header_from_inventory_row(raw))
        if header["number"] != number:
            raise ValueError("historical root header coverage differs")
        _require_hash64(raw["result_sha256"])
        _require_hash64(raw["response_sha256"])
        rows.append(raw)
    return tuple(rows)


def _project_reserve_root(
    *,
    plan: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
    header_inventory: Mapping[str, Any],
    decimal_cache: Mapping[int, Tuple[Any, ...]],
) -> Dict[str, Any]:
    header_rows = header_inventory["rows"]
    expected = _expected_descriptor(
        plan, kind="reserve", root_index=descriptor["root_index"],
        block_start=descriptor["block_start"], block_stop=descriptor["block_stop"],
        header_rows=header_rows,
    )
    validated = _validate_descriptor(plan, descriptor, expected=expected)
    if type(responses) not in (list, tuple) or len(responses) != validated["request_count"]:
        raise ValueError("historical reserve response count differs")
    bounded_headers = _root_header_rows(
        plan, header_inventory, validated["block_start"], validated["block_stop"]
    )
    rows = []
    for offset, (request, response) in enumerate(zip(validated["requests"], responses)):
        result, result_hash, response_hash = _response_result(
            request, response, decimal_cache
        )
        payload = _hex_payload(result, 96)
        reserve0 = int.from_bytes(payload[0:32], "big")
        reserve1 = int.from_bytes(payload[32:64], "big")
        pair_timestamp = int.from_bytes(payload[64:96], "big")
        if reserve0 > _MAX_UINT112 or reserve1 > _MAX_UINT112 or pair_timestamp >= (1 << 32):
            raise ValueError("historical reserve ABI padding is invalid")
        block_offset = offset // 2
        venue_offset = offset % 2
        header = bounded_headers[block_offset]
        venue_id = _VENUE_ORDER[venue_offset]
        rows.append({
            "request_id": request["id"],
            "block_number": header["number"],
            "block_hash": header["hash"],
            "venue_id": venue_id,
            "pair_address": plan["pair_addresses"][venue_id],
            "reserve0": reserve0,
            "reserve1": reserve1,
            "pair_timestamp": pair_timestamp,
            "result_sha256": result_hash,
            "response_sha256": response_hash,
        })
    digest = _inventory_hasher(_RESERVE_INVENTORY_DOMAIN)
    for row in rows:
        _inventory_update(digest, row)
    return {
        "kind": "reserve", "root_index": validated["root_index"],
        "block_start": validated["block_start"], "block_stop": validated["block_stop"],
        "request_ids": tuple(request["id"] for request in validated["requests"]),
        "typed_role": "reserves", "typed_row_count": len(rows),
        "typed_logical_sha256": digest.hexdigest(), "rows": tuple(rows),
    }


def _decode_price_result(value: Any, header_timestamp: int) -> Dict[str, int]:
    payload = _hex_payload(value, 160)
    words = [int.from_bytes(payload[index:index + 32], "big") for index in range(0, 160, 32)]
    round_id, answer_unsigned, started_at, updated_at, answered_in_round = words
    answer = answer_unsigned - (1 << 256) if answer_unsigned >= (1 << 255) else answer_unsigned
    phase_id = round_id >> 64
    round_in_phase = round_id & ((1 << 64) - 1)
    answered_phase = answered_in_round >> 64
    answered_low = answered_in_round & ((1 << 64) - 1)
    if (
        round_id > _MAX_UINT80
        or answered_in_round > _MAX_UINT80
        or phase_id <= 0
        or round_in_phase <= 0
        or answered_phase != phase_id
        or answered_low <= 0
        or answered_in_round < round_id
        or answer <= 0
        or started_at <= 0
        or started_at > updated_at
        or updated_at > header_timestamp
    ):
        raise ValueError("historical price round is invalid")
    if header_timestamp - updated_at > 3600:
        raise _failure("price_snapshot_incomplete", "price_freshness_invalid")
    return {
        "round_id": round_id,
        "phase_id": phase_id,
        "round_in_phase": round_in_phase,
        "answer": answer,
        "started_at": started_at,
        "updated_at": updated_at,
        "answered_in_round": answered_in_round,
        "valid_until": updated_at + 3601,
    }


def _project_price_root(
    *,
    plan: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
    header_inventory: Mapping[str, Any],
    decimal_cache: Mapping[int, Tuple[Any, ...]],
) -> Dict[str, Any]:
    header_rows = header_inventory["rows"]
    expected = _expected_descriptor(
        plan, kind="price", root_index=descriptor["root_index"],
        block_start=descriptor["block_start"], block_stop=descriptor["block_stop"],
        header_rows=header_rows,
    )
    validated = _validate_descriptor(plan, descriptor, expected=expected)
    if type(responses) not in (list, tuple) or len(responses) != validated["request_count"]:
        raise ValueError("historical price response count differs")
    bounded_headers = _root_header_rows(
        plan, header_inventory, validated["block_start"], validated["block_stop"]
    )
    rows = []
    for request, response, header in zip(validated["requests"], responses, bounded_headers):
        result, result_hash, response_hash = _response_result(
            request, response, decimal_cache
        )
        round_row = _decode_price_result(result, header["timestamp"])
        rows.append({
            "request_id": request["id"],
            "block_number": header["number"],
            "block_hash": header["hash"],
            "proxy_address": plan["price_feed_proxy"],
            "round_id": round_row["round_id"],
            "phase_id": round_row["phase_id"],
            "round_in_phase": round_row["round_in_phase"],
            "answer": round_row["answer"],
            "started_at": round_row["started_at"],
            "updated_at": round_row["updated_at"],
            "answered_in_round": round_row["answered_in_round"],
            "valid_until": round_row["valid_until"],
            "result_sha256": result_hash,
            "response_sha256": response_hash,
        })
    digest = _inventory_hasher(_PRICE_INVENTORY_DOMAIN)
    for row in rows:
        _inventory_update(digest, row)
    return {
        "kind": "price", "root_index": validated["root_index"],
        "block_start": validated["block_start"], "block_stop": validated["block_stop"],
        "request_ids": tuple(request["id"] for request in validated["requests"]),
        "typed_role": "prices", "typed_row_count": len(rows),
        "typed_logical_sha256": digest.hexdigest(), "rows": tuple(rows),
    }


def _fee_quantity_list(value: Any, count: int) -> List[int]:
    if type(value) is not list or len(value) != count:
        raise ValueError("historical fee quantity list shape is invalid")
    return [_parse_quantity(item, _MAX_UINT256) for item in value]


def _project_fee_root(
    *,
    plan: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
    header_inventory: Mapping[str, Any],
    decimal_cache: Mapping[int, Tuple[Any, ...]],
) -> Dict[str, Any]:
    expected = _expected_descriptor(
        plan, kind="fee_history", root_index=descriptor["root_index"],
        block_start=descriptor["block_start"], block_stop=descriptor["block_stop"],
        header_rows=header_inventory["rows"],
    )
    validated = _validate_descriptor(plan, descriptor, expected=expected)
    if type(responses) not in (list, tuple) or len(responses) != 1:
        raise ValueError("historical fee response count differs")
    request = validated["requests"][0]
    result, result_hash, response_hash = _response_result(
        request, responses[0], decimal_cache
    )
    if type(result) is not dict:
        raise ValueError("historical fee result is invalid")
    base_fields = {"oldestBlock", "baseFeePerGas", "gasUsedRatio", "reward"}
    blob_fields = {"baseFeePerBlobGas", "blobGasUsedRatio"}
    present_blob = blob_fields.intersection(result)
    if set(result) not in (base_fields, base_fields | blob_fields) or present_blob not in (set(), blob_fields):
        raise ValueError("historical fee result fields are invalid")
    start = validated["block_start"]
    stop = validated["block_stop"]
    count = stop - start + 1
    if result["oldestBlock"] != hex(start) or type(result["oldestBlock"]) is not str:
        raise ValueError("historical fee oldest block differs")
    base_fees = _fee_quantity_list(result["baseFeePerGas"], count + 1)
    ratios = result["gasUsedRatio"]
    rewards = result["reward"]
    if type(ratios) is not list or len(ratios) != count or type(rewards) is not list or len(rewards) != count:
        raise ValueError("historical fee row shape differs")
    if present_blob:
        _fee_quantity_list(result["baseFeePerBlobGas"], count + 1)
        blob_ratios = result["blobGasUsedRatio"]
        if type(blob_ratios) is not list or len(blob_ratios) != count:
            raise ValueError("historical blob ratio shape differs")
        for ratio in blob_ratios:
            if type(ratio) is Decimal:
                projection = _cached_decimal_projection(decimal_cache, ratio)
                _ratio_decimal_token(ratio, _preflight=projection[1])
            else:
                _ratio_decimal_token(ratio)
    bounded_headers = _root_header_rows(plan, header_inventory, start, stop)
    rows = []
    for offset, header in enumerate(bounded_headers):
        if base_fees[offset] != header["base_fee_per_gas"]:
            raise _failure("fee_history_incomplete", "fee_header_mismatch")
        child_failed = False
        try:
            expected_child = next_historical_base_fee(
                parent_base_fee=header["base_fee_per_gas"],
                parent_gas_used=header["gas_used"],
                parent_gas_limit=header["gas_limit"],
            )
        except Exception:
            child_failed = True
        if child_failed:
            raise _failure("fee_history_incomplete", "fee_header_mismatch")
        if expected_child > _MAX_UINT256 or base_fees[offset + 1] != expected_child:
            raise _failure("fee_history_incomplete", "fee_header_mismatch")
        ratio = ratios[offset]
        if type(ratio) is Decimal:
            projection = _cached_decimal_projection(decimal_cache, ratio)
            ratio_token = _ratio_decimal_token(
                ratio,
                gas_used=header["gas_used"],
                gas_limit=header["gas_limit"],
                _preflight=projection[1],
            )
        else:
            ratio_token = _ratio_decimal_token(
                ratio,
                gas_used=header["gas_used"],
                gas_limit=header["gas_limit"],
            )
        reward = rewards[offset]
        if type(reward) is not list or len(reward) != 2:
            raise ValueError("historical fee reward shape differs")
        p50 = _parse_quantity(reward[0], _MAX_UINT256)
        p90 = _parse_quantity(reward[1], _MAX_UINT256)
        if p50 > p90:
            raise ValueError("historical fee reward order differs")
        rows.append({
            "request_id": request["id"],
            "row_offset": offset,
            "block_number": header["number"],
            "base_fee_per_gas": base_fees[offset],
            "next_base_fee_per_gas": base_fees[offset + 1],
            "gas_used_ratio_decimal": ratio_token,
            "p50_priority_fee_per_gas": p50,
            "p90_priority_fee_per_gas": p90,
            "result_sha256": result_hash,
            "response_sha256": response_hash,
        })
    digest = _inventory_hasher(_FEE_INVENTORY_DOMAIN)
    for row in rows:
        _inventory_update(digest, row)
    return {
        "kind": "fee_history", "root_index": validated["root_index"],
        "block_start": start, "block_stop": stop,
        "request_ids": (request["id"],), "typed_role": "fees",
        "typed_row_count": len(rows), "typed_logical_sha256": digest.hexdigest(),
        "rows": tuple(rows),
    }


def _project_final_anchor_root(
    *,
    plan: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
    header_inventory: Mapping[str, Any],
) -> Dict[str, Any]:
    expected = _expected_descriptor(
        plan, kind="final_anchor", root_index=descriptor["root_index"],
        block_start=descriptor["block_start"], block_stop=descriptor["block_stop"],
        header_rows=header_inventory["rows"],
    )
    validated = _validate_descriptor(plan, descriptor, expected=expected)
    if type(responses) not in (list, tuple) or len(responses) != 1:
        raise ValueError("historical final anchor response count differs")
    anchor_projection_failed = False
    try:
        projected = _project_historical_block_header_success(
            request=validated["requests"][0], response=responses[0]
        )
    except HistoricalWindowProjectionError:
        anchor_projection_failed = True
    if anchor_projection_failed:
        raise _failure("anchor_changed", "final_anchor_mismatch")
    anchor_header = _header_from_inventory_row(header_inventory["rows"][-1])
    if projected["header"] != anchor_header:
        raise _failure("anchor_changed", "final_anchor_mismatch")
    row = {
        "request_id": validated["requests"][0]["id"],
        "header": projected["header"],
        "result_sha256": projected["result_sha256"],
        "response_sha256": projected["response_sha256"],
    }
    digest = _inventory_hasher(_FINAL_ANCHOR_DOMAIN)
    _inventory_update(digest, row)
    return {
        "kind": "final_anchor", "root_index": validated["root_index"],
        "block_start": validated["block_start"], "block_stop": validated["block_stop"],
        "request_ids": (validated["requests"][0]["id"],),
        "typed_role": "final_anchor", "typed_row_count": 1,
        "typed_logical_sha256": digest.hexdigest(), "rows": (row,),
    }


def _descriptor_root_failure_pair(value: Any) -> Tuple[str, str]:
    fallback = ("block_coverage_incomplete", "header_invalid")
    selection_failed = False
    try:
        if type(value) is not dict or len(value) != len(_DESCRIPTOR_FIELDS):
            return fallback
        kind = None
        for key, candidate in dict.items(value):
            if type(key) is str and key == "kind":
                kind = candidate if type(candidate) is str else None
                break
        if kind == "reserve":
            return ("reserve_snapshot_incomplete", "reserve_abi_invalid")
        if kind == "price":
            return ("price_snapshot_incomplete", "price_abi_invalid")
        if kind == "fee_history":
            return ("fee_history_incomplete", "fee_shape_invalid")
        if kind == "final_anchor":
            return ("anchor_changed", "final_anchor_mismatch")
        return fallback
    except Exception:
        selection_failed = True
    if selection_failed:
        return fallback


def _project_complete_historical_window_root(
    *,
    plan: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
    header_inventory: Optional[Mapping[str, Any]],
) -> Mapping[str, Any]:
    failure_pair = _descriptor_root_failure_pair(descriptor)
    try:
        decimal_cache = _guard_historical_json_value((descriptor, responses))
        validated_plan = _validate_plan_shape(plan)
        if type(descriptor) is not dict or set(descriptor) != _DESCRIPTOR_FIELDS:
            raise ValueError("historical descriptor schema is invalid")
        kind = descriptor["kind"]
        if type(kind) is not str:
            raise ValueError("historical descriptor kind is invalid")
        if kind == "header":
            if header_inventory is not None:
                raise ValueError("historical header root received state inventory")
            return _project_header_root(
                plan=validated_plan, descriptor=descriptor, responses=responses
            )
        if header_inventory is None:
            raise ValueError("historical state root lacks header inventory")
        active_validation = _ACTIVE_HEADER_VALIDATION.get()
        if not (
            active_validation is not None
            and active_validation[0] is header_inventory
            and active_validation[1] is plan
        ):
            header_inventory, _validated_rows = _validate_header_inventory(
                plan=validated_plan, header_inventory=header_inventory
            )
        if kind == "reserve":
            return _project_reserve_root(
                plan=validated_plan, descriptor=descriptor, responses=responses,
                header_inventory=header_inventory, decimal_cache=decimal_cache,
            )
        if kind == "price":
            return _project_price_root(
                plan=validated_plan, descriptor=descriptor, responses=responses,
                header_inventory=header_inventory, decimal_cache=decimal_cache,
            )
        if kind == "fee_history":
            return _project_fee_root(
                plan=validated_plan, descriptor=descriptor, responses=responses,
                header_inventory=header_inventory, decimal_cache=decimal_cache,
            )
        if kind == "final_anchor":
            return _project_final_anchor_root(
                plan=validated_plan, descriptor=descriptor, responses=responses,
                header_inventory=header_inventory,
            )
        raise ValueError("historical descriptor kind is invalid")
    except HistoricalWindowProjectionError as error:
        failure_pair = _captured_failure_pair(error, failure_pair)
    except Exception:
        pass
    raise _failure(*failure_pair)


def _coverage_pair(kind: str) -> Tuple[str, str]:
    if kind == "reserve":
        return ("reserve_snapshot_incomplete", "reserve_coverage_invalid")
    if kind == "price":
        return ("price_snapshot_incomplete", "price_coverage_invalid")
    if kind == "fee_history":
        return ("fee_history_incomplete", "fee_coverage_invalid")
    if kind == "final_anchor":
        return ("anchor_changed", "final_anchor_mismatch")
    return ("authority_mismatch", "request_ledger_invalid")


def _validate_header_base_fee_chain(rows: Sequence[Mapping[str, Any]]) -> None:
    validation_failed = False
    try:
        for parent, child in zip(rows, rows[1:]):
            expected = next_historical_base_fee(
                parent_base_fee=parent["base_fee_per_gas"],
                parent_gas_used=parent["gas_used"],
                parent_gas_limit=parent["gas_limit"],
            )
            if expected > _MAX_UINT256 or child["base_fee_per_gas"] != expected:
                raise ValueError("historical header base fee chain differs")
    except Exception:
        validation_failed = True
    if validation_failed:
        raise _failure("fee_history_incomplete", "fee_header_mismatch")


def _anchor_price_authority(anchor_capture: Mapping[str, Any]) -> Dict[str, int]:
    try:
        price_feed = anchor_capture["price_feed"]
        latest = price_feed["latest_round"]
        phase_id = price_feed["phase_id"]
        if type(price_feed) is not dict or type(latest) is not dict:
            raise ValueError("historical anchor price authority is invalid")
        fields = {"round_id", "answer", "started_at", "updated_at", "answered_in_round"}
        if set(latest) != fields or type(phase_id) is not int:
            raise ValueError("historical anchor price authority is invalid")
        result = dict(latest)
        result["phase_id"] = phase_id
        for key, value in result.items():
            if type(value) is not int:
                raise ValueError("historical anchor price authority is invalid")
        return result
    except Exception:
        validation_failed = True
    if validation_failed:
        raise _failure("authority_mismatch", "anchor_authority_invalid")


def _stage_range(role: str, first_id: int, last_id: int) -> Dict[str, Any]:
    count = last_id - first_id + 1
    if count <= 0:
        raise ValueError("historical stage range is empty")
    return {"role": role, "first_id": first_id, "last_id": last_id, "count": count}


def _request_stage_ranges(
    plan: Mapping[str, Any], lower: Mapping[str, Any]
) -> Tuple[Mapping[str, Any], ...]:
    count = plan["block_count"]
    first = plan["first_request_id"]
    header_first = first
    reserve_first = header_first + count
    price_first = reserve_first + 2 * count
    fee_first = price_first + count
    final_id = plan["last_request_id"]
    return (
        _stage_range("anchor", 1, 48),
        _stage_range("lower_bound", 49, lower["next_request_id"] - 1),
        _stage_range("headers", header_first, reserve_first - 1),
        _stage_range("reserves", reserve_first, price_first - 1),
        _stage_range("prices", price_first, fee_first - 1),
        _stage_range("fee_history", fee_first, final_id - 1),
        _stage_range("final_anchor", final_id, final_id),
    )


def project_historical_window_projection(
    *,
    plan: Mapping[str, Any],
    anchor_capture: Mapping[str, Any],
    lower_bound_capture: Mapping[str, Any],
    header_inventory: Mapping[str, Any],
    batch_results: Iterable[
        Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
    ],
) -> Mapping[str, Any]:
    validated_plan, anchor, lower = _validate_bound_inputs(
        plan, anchor_capture, lower_bound_capture
    )
    validated_inventory, header_rows = _validate_header_inventory(
        plan=validated_plan, header_inventory=header_inventory,
        anchor=anchor, lower=lower,
    )
    _validate_header_base_fee_chain(header_rows)
    anchor_price = _anchor_price_authority(anchor_capture)
    result_iterator = _iterator_once(
        batch_results, ("authority_mismatch", "request_ledger_invalid")
    )
    expected_iterator = _state_descriptor_rows(validated_plan, header_rows)
    digests = {
        "reserves": _inventory_hasher(_RESERVE_INVENTORY_DOMAIN),
        "prices": _inventory_hasher(_PRICE_INVENTORY_DOMAIN),
        "fees": _inventory_hasher(_FEE_INVENTORY_DOMAIN),
    }
    counts = {"reserves": 0, "prices": 0, "fees": 0}
    final_header = None
    previous_request_id = validated_plan["first_request_id"] + validated_plan["block_count"] - 1
    for expected in expected_iterator:
        pair = _coverage_pair(expected["kind"])
        raw_pair = _next_input(result_iterator, pair)
        root_failed = False
        failure_pair = pair
        try:
            if type(raw_pair) is not tuple or len(raw_pair) != 2:
                raise ValueError("historical state batch result is invalid")
            descriptor, responses = raw_pair
            if descriptor != expected:
                raise ValueError("historical state descriptor order differs")
            validation_token = _ACTIVE_HEADER_VALIDATION.set(
                (validated_inventory, validated_plan)
            )
            try:
                root = _project_complete_historical_window_root(
                    plan=validated_plan,
                    descriptor=descriptor,
                    responses=responses,
                    header_inventory=validated_inventory,
                )
            finally:
                _ACTIVE_HEADER_VALIDATION.reset(validation_token)
            request_ids = root["request_ids"]
            if (
                not request_ids
                or request_ids[0] != previous_request_id + 1
                or request_ids != tuple(range(request_ids[0], request_ids[-1] + 1))
            ):
                raise ValueError("historical state request ledger differs")
            previous_request_id = request_ids[-1]
            role = root["typed_role"]
            if role in digests:
                for row in root["rows"]:
                    _inventory_update(digests[role], row)
                    counts[role] += 1
                    if role == "prices" and row["block_number"] == anchor["number"]:
                        if any(row[key] != anchor_price[key] for key in (
                            "round_id", "phase_id", "answer", "started_at",
                            "updated_at", "answered_in_round",
                        )):
                            raise _failure(
                                "price_snapshot_incomplete", "price_round_invalid"
                            )
            elif role == "final_anchor":
                if root["typed_row_count"] != 1:
                    raise ValueError("historical final anchor row count differs")
                final_header = root["rows"][0]["header"]
            else:
                raise ValueError("historical typed role is invalid")
        except HistoricalWindowProjectionError as error:
            root_failed = True
            failure_pair = _captured_failure_pair(error, failure_pair)
        except Exception:
            root_failed = True
        if root_failed:
            raise _failure(*failure_pair)
        del raw_pair, root
    if _next_input(
        result_iterator,
        ("authority_mismatch", "request_ledger_invalid"),
        allow_end=True,
    ) is not _END_OF_INPUT:
        raise _failure("authority_mismatch", "request_ledger_invalid")
    if previous_request_id != validated_plan["last_request_id"]:
        raise _failure("authority_mismatch", "request_ledger_invalid")
    if final_header != lower["anchor_header"]:
        raise _failure("anchor_changed", "final_anchor_mismatch")
    if (
        counts["reserves"] != 2 * validated_plan["block_count"]
        or counts["prices"] != validated_plan["block_count"]
        or counts["fees"] != validated_plan["block_count"]
    ):
        raise _failure("authority_mismatch", "request_ledger_invalid")
    # Detect caller mutation after the initial detached validation pass.
    _validate_header_inventory(
        plan=validated_plan, header_inventory=header_inventory,
        anchor=anchor, lower=lower,
    )

    lower_number = validated_plan["lower_bound_number"]
    anchor_number = validated_plan["anchor_number"]
    predecessor = (
        lower["boundary_witness"][0]["header"] if lower_number > 0 else None
    )
    lower_header = lower["boundary_witness"][-1]["header"]
    stage_ranges = _request_stage_ranges(validated_plan, lower)
    continuous_value = {
        "first_id": 1,
        "last_id": validated_plan["last_request_id"],
        "count": validated_plan["last_request_id"],
    }
    projection = {
        "schema": "historical_foundry_window_projection/v1",
        "authority": "fixture_only_nonauthorizing",
        "chain_id": 1,
        "anchor_capture_sha256": validated_plan["anchor_capture_sha256"],
        "lower_bound_capture_sha256": validated_plan["lower_bound_capture_sha256"],
        "range": {
            "lower_bound_number": lower_number,
            "anchor_number": anchor_number,
            "cutoff_timestamp": lower["cutoff_timestamp"],
            "block_count": validated_plan["block_count"],
        },
        "role_inventories": {
            "headers": {
                "row_count": validated_inventory["row_count"],
                "first_block": lower_number,
                "last_block": anchor_number,
                "logical_sha256": validated_inventory["logical_sha256"],
            },
            "reserves": {
                "row_count": counts["reserves"],
                "first_block": lower_number,
                "last_block": anchor_number,
                "logical_sha256": digests["reserves"].hexdigest(),
            },
            "prices": {
                "row_count": counts["prices"],
                "first_block": lower_number,
                "last_block": anchor_number,
                "logical_sha256": digests["prices"].hexdigest(),
            },
            "fees": {
                "row_count": counts["fees"],
                "first_block": lower_number,
                "last_block": anchor_number,
                "logical_sha256": digests["fees"].hexdigest(),
            },
        },
        "boundaries": {
            "predecessor_header": predecessor,
            "lower_header": lower_header,
            "anchor_header": anchor,
            "final_anchor_header": final_header,
        },
        "request_ledger": {
            "first_request_id": 1,
            "last_request_id": validated_plan["last_request_id"],
            "request_count": validated_plan["last_request_id"],
            "stage_ranges": stage_ranges,
            "continuous_ids_sha256": _typed_hash(
                _CONTINUOUS_IDS_DOMAIN, continuous_value
            ),
        },
        "coverage": {
            "header_count": validated_inventory["row_count"],
            "reserve_count": counts["reserves"],
            "price_count": counts["prices"],
            "fee_count": counts["fees"],
        },
    }
    projection_failed = False
    try:
        _guard_historical_json_value(projection)
    except Exception:
        projection_failed = True
    if projection_failed:
        raise _failure("authority_mismatch", "fixture_input_invalid")
    return projection
