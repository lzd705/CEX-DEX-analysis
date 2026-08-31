"""Historical-window planning, semantic projection, and capture ingress.

Existing planners and projectors remain pure.  The sole authenticated Task-4b
ingress delegates held offline materialization to the exact canonical storage
module.  Scan adds no environment, network, subprocess, or publication
controller.
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from decimal import Decimal
import contextvars
from fractions import Fraction
import gzip
import hashlib
import json
import platform
import re
import sys
import weakref
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from scripts.historical_foundry_contracts import (
    HistoricalFoundryConfigSet,
    next_historical_base_fee,
    project_historical_prefilter_math,
)
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
    ("authority_mismatch", "final_identity_drift"),
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
    capture_replay_event_registry = {}
    capture_materialization_registry = {}
    active_capture_materialization = contextvars.ContextVar(
        "historical_foundry_active_capture_materialization",
        default=None,
    )
    task4b_weakref_module = weakref
    task4b_weakref_ref = weakref.ref
    task4b_scan_local_names = (
        "_materialize_historical_window_staging_snapshot",
        "_ProductionHistoricalWindowCaptureReplayEvent",
        "_bind_production_historical_window_capture_replay_source_from_bound_storage",
        "_replay_production_historical_window_capture_from_bound_storage",
        "_consume_production_historical_window_capture_replay_event_for_storage",
    )
    task4b_scan_local_originals = None
    task4b_attestation_provenance = object()
    task4b_runtime_sys = sys
    task4b_scripts_package = sys.modules.get("scripts")
    from scripts import historical_foundry_storage as task4b_storage_module
    task4b_storage_consume = (
        task4b_storage_module
        .consume_production_historical_window_capability
    )
    task4b_storage_consume_code = task4b_storage_consume.__code__
    task4b_storage_capability_class = (
        task4b_storage_module._ProductionHistoricalWindowCapability
    )
    task4b_storage_consumed_view_class = (
        task4b_storage_module
        ._ConsumedProductionHistoricalWindowCapabilityView
    )
    task4b_storage_source_class = (
        task4b_storage_module._HistoricalWindowCaptureReplaySource
    )
    task4b_storage_snapshot_class = (
        task4b_storage_module.HistoricalRunStagingSnapshot
    )
    task4b_storage_view_materialize = (
        task4b_storage_consumed_view_class
        ._materialize_staging_snapshot_from_bound_scan
    )
    task4b_storage_view_materialize_code = (
        task4b_storage_view_materialize.__code__
    )
    task4b_storage_view_close = task4b_storage_consumed_view_class.close
    task4b_storage_view_close_code = task4b_storage_view_close.__code__
    task4b_storage_snapshot_projection = (
        task4b_storage_snapshot_class.frozen_identity_projection
    )
    task4b_storage_snapshot_projection_code = (
        task4b_storage_snapshot_projection.__code__
    )
    task4b_storage_snapshot_close = task4b_storage_snapshot_class.close
    task4b_storage_snapshot_close_code = (
        task4b_storage_snapshot_close.__code__
    )

    task4b_storage_function_type = type(task4b_storage_consume)
    task4b_storage_function_clones = {}
    task4b_storage_cell_clones = {}

    def task4b_new_storage_cell(value):
        def cell_value():
            return value

        return cell_value.__closure__[0]

    def task4b_clone_storage_function(function):
        function_entry = task4b_storage_function_clones.get(id(function))
        if function_entry is not None and function_entry[0] is function:
            return function_entry[1]
        original_closure = function.__closure__ or ()
        cloned_cells = []
        for original_cell in original_closure:
            cell_entry = task4b_storage_cell_clones.get(id(original_cell))
            if cell_entry is None or cell_entry[0] is not original_cell:
                cell_entry = [
                    original_cell,
                    task4b_new_storage_cell(None),
                    False,
                ]
                task4b_storage_cell_clones[id(original_cell)] = cell_entry
            cloned_cells.append(cell_entry[1])
        cloned = task4b_storage_function_type(
            function.__code__,
            function.__globals__,
            function.__name__,
            function.__defaults__,
            tuple(cloned_cells) if cloned_cells else None,
        )
        cloned.__kwdefaults__ = (
            dict(function.__kwdefaults__)
            if type(function.__kwdefaults__) is dict else None
        )
        task4b_storage_function_clones[id(function)] = (function, cloned)
        for original_cell in original_closure:
            cell_entry = task4b_storage_cell_clones[id(original_cell)]
            if not cell_entry[2]:
                cell_entry[2] = True
                original_value = original_cell.cell_contents
                cloned_value = (
                    task4b_clone_storage_function(original_value)
                    if type(original_value) is task4b_storage_function_type
                    else original_value
                )
                cell_entry[1].cell_contents = cloned_value
        return cloned

    task4b_storage_consume_runner = task4b_clone_storage_function(
        task4b_storage_consume
    )
    task4b_storage_view_materialize_runner = (
        task4b_clone_storage_function(task4b_storage_view_materialize)
    )
    task4b_storage_view_close_runner = task4b_clone_storage_function(
        task4b_storage_view_close
    )
    task4b_storage_snapshot_projection_runner = (
        task4b_clone_storage_function(task4b_storage_snapshot_projection)
    )
    task4b_storage_snapshot_close_runner = (
        task4b_clone_storage_function(task4b_storage_snapshot_close)
    )
    task4b_storage_function_graph = tuple(
        (
            original,
            original.__code__,
            original.__globals__,
            original.__defaults__,
            original.__kwdefaults__,
            (
                tuple(sorted(original.__kwdefaults__.items()))
                if type(original.__kwdefaults__) is dict else None
            ),
            original.__closure__ or (),
            tuple(
                cell.cell_contents
                for cell in (original.__closure__ or ())
            ),
        )
        for original, _cloned in task4b_storage_function_clones.values()
    )
    task4b_storage_runner_graph = tuple(
        (
            cloned,
            cloned.__code__,
            cloned.__globals__,
            cloned.__defaults__,
            cloned.__kwdefaults__,
            (
                tuple(sorted(cloned.__kwdefaults__.items()))
                if type(cloned.__kwdefaults__) is dict else None
            ),
            cloned.__closure__ or (),
            tuple(
                cell.cell_contents
                for cell in (cloned.__closure__ or ())
            ),
        )
        for _original, cloned in task4b_storage_function_clones.values()
    )
    task4b_storage_function_clones = None
    task4b_storage_cell_clones = None

    def task4b_storage_function_graph_is_current(graph):
        try:
            for (
                function,
                code,
                function_globals,
                defaults,
                kwdefaults,
                kwdefault_items,
                closure,
                closure_values,
            ) in graph:
                current_closure = function.__closure__ or ()
                if (
                    function.__code__ is not code
                    or function.__globals__ is not function_globals
                    or function.__defaults__ is not defaults
                    or function.__kwdefaults__ is not kwdefaults
                    or (
                        kwdefault_items is not None
                        and (
                            type(function.__kwdefaults__) is not dict
                            or len(function.__kwdefaults__)
                            != len(kwdefault_items)
                            or any(
                                key not in function.__kwdefaults__
                                or function.__kwdefaults__[key] is not value
                                for key, value in kwdefault_items
                            )
                        )
                    )
                    or len(current_closure) != len(closure)
                    or any(
                        current is not expected
                        for current, expected in zip(
                            current_closure, closure
                        )
                    )
                    or len(closure) != len(closure_values)
                    or any(
                        cell.cell_contents is not expected
                        for cell, expected in zip(
                            closure, closure_values
                        )
                    )
                ):
                    return False
        except BaseException:
            return False
        return True

    def task4b_storage_original_graph_is_current():
        return task4b_storage_function_graph_is_current(
            task4b_storage_function_graph
        )

    def task4b_storage_runner_graph_is_current():
        return task4b_storage_function_graph_is_current(
            task4b_storage_runner_graph
        )

    def task4b_storage_identity_is_current(runtime_sys):
        return (
            task4b_storage_original_graph_is_current()
            and task4b_storage_runner_graph_is_current()
            and
            sys is task4b_runtime_sys
            and runtime_sys is task4b_runtime_sys
            and runtime_sys.modules.get(
                "scripts.historical_foundry_storage"
            ) is task4b_storage_module
            and getattr(
                task4b_scripts_package,
                "historical_foundry_storage",
                None,
            ) is task4b_storage_module
            and getattr(
                task4b_storage_module,
                "consume_production_historical_window_capability",
                None,
            ) is task4b_storage_consume
            and task4b_storage_consume.__code__
            is task4b_storage_consume_code
            and getattr(
                task4b_storage_module,
                "_ProductionHistoricalWindowCapability",
                None,
            ) is task4b_storage_capability_class
            and getattr(
                task4b_storage_module,
                "_ConsumedProductionHistoricalWindowCapabilityView",
                None,
            ) is task4b_storage_consumed_view_class
            and getattr(
                task4b_storage_module,
                "_HistoricalWindowCaptureReplaySource",
                None,
            ) is task4b_storage_source_class
            and getattr(
                task4b_storage_module,
                "HistoricalRunStagingSnapshot",
                None,
            ) is task4b_storage_snapshot_class
            and getattr(
                task4b_storage_consumed_view_class,
                "_materialize_staging_snapshot_from_bound_scan",
                None,
            ) is task4b_storage_view_materialize
            and task4b_storage_view_materialize.__code__
            is task4b_storage_view_materialize_code
            and getattr(
                task4b_storage_consumed_view_class, "close", None
            ) is task4b_storage_view_close
            and task4b_storage_view_close.__code__
            is task4b_storage_view_close_code
            and getattr(
                task4b_storage_snapshot_class,
                "frozen_identity_projection",
                None,
            ) is task4b_storage_snapshot_projection
            and task4b_storage_snapshot_projection.__code__
            is task4b_storage_snapshot_projection_code
            and getattr(
                task4b_storage_snapshot_class, "close", None
            ) is task4b_storage_snapshot_close
            and task4b_storage_snapshot_close.__code__
            is task4b_storage_snapshot_close_code
        )
    if (
        task4b_scripts_package is None
        or task4b_runtime_sys.modules.get(
            "scripts.historical_foundry_storage"
        ) is not task4b_storage_module
        or getattr(
            task4b_scripts_package, "historical_foundry_storage", None
        ) is not task4b_storage_module
    ):
        raise RuntimeError("historical storage module identity is unavailable")
    _verify_decimal_layout()
    if _DECIMAL_LAYOUT_VERIFIED is not True:
        raise RuntimeError("historical Decimal layout did not stabilize")

    scan_module = sys.modules.get(__name__)
    if scan_module is None:
        main_module = sys.modules.get("__main__")
        if (
            getattr(getattr(main_module, "__spec__", None), "name", None)
            == "scripts.historical_foundry_scan"
        ):
            scan_module = main_module
    contracts_module = sys.modules.get(
        "scripts.historical_foundry_contracts"
    )
    route_module = sys.modules.get("scripts.route_cost_evidence")
    task4b_canonical_modules = (
        ("scan", "scripts.historical_foundry_scan", scan_module),
        ("rpc", "scripts.historical_foundry_rpc", _transport_core),
        (
            "contracts", "scripts.historical_foundry_contracts",
            contracts_module,
        ),
        ("route", "scripts.route_cost_evidence", route_module),
    )
    if any(
        module is None or module.__name__ != canonical_name
        for _role, canonical_name, module in task4b_canonical_modules
    ):
        raise RuntimeError("historical semantic module identity is unavailable")

    def _task4b_module(role: str) -> Any:
        for candidate_role, _canonical_name, module in task4b_canonical_modules:
            if candidate_role == role:
                return module
        raise RuntimeError("historical semantic module role is invalid")

    def _task4b_resolve(module: Any, qualified_name: str) -> Any:
        value = module
        for component in qualified_name.split("."):
            value = getattr(value, component)
        return value

    contracts_callable_names = (
        "_next_base_fee", "_nonnegative_int", "_positive_int",
        "next_historical_base_fee",
    )
    rpc_callable_names = (
        "_abi_string", "_address_argument", "_address_word",
        "_allowance_calldata", "_anchor_bindings", "_anchor_projection",
        "_balance_calldata", "_binding", "_build_closed_plan", "_call",
        "_canonical_bytes", "_copy_json", "_derived_bindings",
        "_derived_templates", "_feed_projection", "_fixed_templates",
        "_guard_exact_json", "_hash32", "_hex_bytes", "_inventory",
        "_latest_round_projection", "_materialize_historical_anchor_stage",
        "_project_capture", "_quantity",
        "_require_derived_authority_addresses", "_resolve_template",
        "_resource_error", "_runtime_projection", "_stage_identity",
        "_template", "_token_projection", "_typed_hash", "_uint_word",
        "_validate_closed_plan", "_validate_historical_anchor_capture",
        "_validate_success_rows", "_venue_projection", "_zero_word",
        "build_factory_get_pair_calldata", "keccak256",
        "project_historical_anchor_capture", "solidity_allowance_storage_key",
        "solidity_balance_storage_key",
    )
    scan_callable_names = (
        "_anchor_state_authority", "_build_historical_block_header_request",
        "_cached_decimal_projection", "_canonical_hash_value",
        "_canonical_json_bytes", "_captured_failure_pair",
        "_coefficient_from_digits", "_decode_price_result",
        "_descriptor_root_failure_pair", "_expected_descriptor", "_failure",
        "_fee_quantity_list", "_frame", "_guard_historical_json_value",
        "_header_descriptor_rows", "_header_from_inventory_row",
        "_header_hash_at", "_header_root_count",
        "_header_row_from_projection", "_hex_payload",
        "_historical_json_int_token_bytes", "_inventory_hasher",
        "_inventory_update", "_iterator_once", "_make_descriptor",
        "_next_input", "_normalized_anchor_from_capture",
        "_normalized_from_raw", "_parse_quantity",
        "_preflight_historical_decimal_tuple",
        "_project_complete_historical_window_root", "_project_fee_root",
        "_project_final_anchor_root", "_project_header_root",
        "_project_historical_block_header_success",
        "_project_lower_observation", "_project_price_root",
        "_project_reserve_root", "_ratio_decimal_token", "_require_hash32",
        "_require_hash64", "_require_raw_json_containers", "_require_uint",
        "_response_result", "_root_header_rows", "_state_descriptor_rows",
        "_typed_hash", "_validate_anchor_capture",
        "_validate_compact_observation", "_validate_descriptor",
        "_validate_header_inventory", "_validate_historical_anchor_capture",
        "_validate_lower_capture", "_validate_normalized_header",
        "_validate_plan_shape", "_verify_decimal_layout",
        "build_historical_window_request_plan",
        "iter_historical_header_request_batches",
        "iter_historical_state_request_batches", "next_historical_base_fee",
        "project_historical_lower_bound_capture",
    )
    route_callable_names = (
        "_abi_address_word", "_address", "_exact_int", "_keccak_f1600",
        "_pad_address", "_pad_slot", "_rotate_left_64", "_uint256",
        "build_factory_get_pair_calldata", "keccak256",
        "solidity_allowance_storage_key", "solidity_balance_storage_key",
    )
    class_callable_bindings = (
        ("scan", "Decimal"),
        ("scan", "HistoricalWindowProjectionError"),
        ("scan", "MappingProxyType"),
        ("scan", "_ArchiveRpcError"),
        ("scan", "_OneShotIterator"),
        ("route", "RouteCostEvidenceError"),
    )
    module_attribute_callable_names = (
        ("rpc", "hashlib", "sha256"),
        ("rpc", "json", "dumps"),
        ("rpc", "json", "loads"),
        ("scan", "hashlib", "sha256"),
        ("scan", "json", "dumps"),
        ("scan", "platform", "python_implementation"),
    )
    rpc_constant_names = (
        "_ADDRESS", "_AGGREGATOR", "_ALLOWANCE", "_ANCHOR_RESULT_FIELDS",
        "_BALANCE_OF", "_CAPTURE_FIELDS", "_CAPTURE_SCHEMA", "_DECIMALS",
        "_DESCRIPTION", "_EXECUTOR", "_FACTORY", "_FEED_PROXY",
        "_FIXED_AUTHORITY_ADDRESSES", "_HASH32", "_HEX_BYTES",
        "_INVENTORY_FIELDS", "_LATEST_ROUND", "_MAX_ABI_BYTES",
        "_MAX_JSON_NODES", "_MAX_NESTING_DEPTH",
        "_MAX_ORDINARY_STRING_BYTES", "_MAX_RUNTIME_BYTES",
        "_MAX_SCALAR_BYTES", "_PARAMS_HASH_DOMAIN", "_PHASE",
        "_PLAN_FIELDS", "_PLAN_SCHEMA", "_QUANTITY", "_REQUEST_HASH_DOMAIN",
        "_RESPONSE_FIELDS", "_RESPONSE_HASH_DOMAIN", "_RESULT_HASH_DOMAIN",
        "_SENDER", "_STAGE_FIELDS", "_TEMPLATE_FIELDS", "_TOKEN0",
        "_TOKEN1", "_UNI", "_VENUES", "_WETH", "_WETH_GETTER",
        "_WIRE_FIELDS",
    )
    scan_constant_names = (
        "_ACTIVE_HEADER_VALIDATION", "_ADDRESS", "_ANCHOR_CAPTURE_DOMAIN",
        "_DECIMAL_LAYOUT_VERIFIED", "_DESCRIPTOR_FIELDS", "_END_OF_INPUT",
        "_ERROR_PAIRS", "_FEE_INVENTORY_DOMAIN", "_FINAL_ANCHOR_DOMAIN",
        "_GET_RESERVES_SELECTOR", "_HASH32", "_HASH64",
        "_HEADER_INVENTORY_DOMAIN", "_HEADER_INVENTORY_FIELDS",
        "_HEADER_ROW_FIELDS", "_LATEST_ROUND_SELECTOR", "_LOOKBACK_SECONDS",
        "_LOWER_CAPTURE_DOMAIN", "_LOWER_FIELDS", "_MAX_BLOCK_COUNT",
        "_MAX_DEPTH", "_MAX_JSON_NODES", "_MAX_NUMERIC_TOKEN_BYTES",
        "_MAX_RATIO_DECIMAL_OBJECT_BYTES", "_MAX_RATIO_TOKEN_BYTES",
        "_MAX_SCALAR_BYTES", "_MAX_STRING_BYTES", "_MAX_UINT112",
        "_MAX_UINT256", "_MAX_UINT64", "_MAX_UINT80",
        "_NEGATIVE_JSON_INT_MAGNITUDE_EXCLUSIVE",
        "_NONNEGATIVE_JSON_INT_EXCLUSIVE", "_NORMALIZED_HEADER_DOMAIN",
        "_NORMALIZED_HEADER_FIELDS", "_OBSERVATION_FIELDS", "_PLAN_FIELDS",
        "_PRICE_INVENTORY_DOMAIN", "_QUANTITY", "_RAW_HEADER_FIELDS",
        "_REQUEST_DOMAIN", "_RESERVE_INVENTORY_DOMAIN", "_RESPONSE_DOMAIN",
        "_RESULT_DOMAIN", "_ROOT_BATCH_POLICY", "_SUCCESS_FIELDS",
        "_VENUE_ORDER", "_WIRE_FIELDS",
    )
    route_constant_names = (
        "_ADDRESS", "_KECCAK_ROTATION", "_KECCAK_ROUND_CONSTANTS", "_MASK64",
    )
    regex_constant_names = frozenset((
        ("rpc", "_ADDRESS"), ("rpc", "_HASH32"),
        ("rpc", "_HEX_BYTES"), ("rpc", "_QUANTITY"),
        ("scan", "_ADDRESS"), ("scan", "_HASH32"),
        ("scan", "_HASH64"), ("scan", "_QUANTITY"),
        ("route", "_ADDRESS"),
    ))

    def _task4b_constant_projection(role: str, name: str, value: Any) -> Any:
        if (role, name) in regex_constant_names:
            return ("regex", type(value), value.pattern, value.flags)
        if (role, name) == ("scan", "_ROOT_BATCH_POLICY"):
            if type(value) is not dict:
                raise RuntimeError("historical semantic table is invalid")
            return ("dict", dict, tuple(sorted(dict.items(value))))
        if (role, name) == ("scan", "_ACTIVE_HEADER_VALIDATION"):
            return ("contextvar", type(value))
        if (role, name) == ("scan", "_END_OF_INPUT"):
            return ("identity_only", type(value))
        return ("value", type(value), value)

    task4b_semantic_roots = (
        ("rpc", "_materialize_historical_anchor_stage",
         _transport_core._materialize_historical_anchor_stage),
        ("rpc", "project_historical_anchor_capture",
         _transport_core.project_historical_anchor_capture),
        ("scan", "project_historical_lower_bound_capture",
         project_historical_lower_bound_capture),
        ("scan", "build_historical_window_request_plan",
         build_historical_window_request_plan),
        ("scan", "iter_historical_header_request_batches",
         iter_historical_header_request_batches),
        ("scan", "iter_historical_state_request_batches",
         iter_historical_state_request_batches),
        ("scan", "_project_complete_historical_window_root",
         _project_complete_historical_window_root),
    )
    task4b_semantic_callables = tuple(
        (role, name, _task4b_resolve(_task4b_module(role), name))
        for role, names in (
            ("contracts", contracts_callable_names),
            ("rpc", rpc_callable_names),
            ("scan", scan_callable_names),
            ("route", route_callable_names),
        )
        for name in names
    )
    task4b_semantic_classes = tuple(
        (role, name, _task4b_resolve(_task4b_module(role), name))
        for role, name in class_callable_bindings
    )
    task4b_semantic_module_callables = tuple(
        (
            role, module_name, attribute_name,
            _task4b_resolve(_task4b_module(role), module_name),
            getattr(
                _task4b_resolve(_task4b_module(role), module_name),
                attribute_name,
            ),
        )
        for role, module_name, attribute_name
        in module_attribute_callable_names
    )
    task4b_semantic_constants = tuple(
        (
            role, name, _task4b_resolve(_task4b_module(role), name),
            _task4b_constant_projection(
                role, name, _task4b_resolve(_task4b_module(role), name)
            ),
        )
        for role, names in (
            ("rpc", rpc_constant_names),
            ("scan", scan_constant_names),
            ("route", route_constant_names),
        )
        for name in names
    )
    task4b_semantic_dependency_manifest = (
        task4b_semantic_roots,
        task4b_semantic_callables,
        task4b_semantic_classes,
        task4b_semantic_module_callables,
        task4b_semantic_constants,
    )
    class_surface_names = (
        ("scan", "_OneShotCursor"),
        ("scan", "_OneShotCursor.__init__"),
        ("scan", "_OneShotCursor.__iter__"),
        ("scan", "_OneShotCursor.__next__"),
        ("scan", "_OneShotIterator.__init__"),
        ("scan", "_OneShotIterator.__iter__"),
        ("scan", "_OneShotIterator.__next__"),
        ("scan", "_OneShotIterator._advance"),
        ("scan", "HistoricalWindowProjectionError.__init__"),
    )
    task4b_class_surfaces = tuple(
        (role, name, _task4b_resolve(_task4b_module(role), name))
        for role, name in class_surface_names
    )
    driver_only_callable_names = (
        "project_historical_header_inventory",
        "project_historical_window_projection",
        "_validate_bound_inputs", "_coverage_pair",
        "_validate_header_base_fee_chain", "_anchor_price_authority",
        "_stage_range", "_request_stage_ranges",
    )
    task4b_driver_callable_anchors = tuple(
        ("scan", name, _task4b_resolve(scan_module, name))
        for name in driver_only_callable_names
    )
    task4b_driver_constant_anchors = (
        (
            "scan", "_CONTINUOUS_IDS_DOMAIN", _CONTINUOUS_IDS_DOMAIN,
            _task4b_constant_projection(
                "scan", "_CONTINUOUS_IDS_DOMAIN", _CONTINUOUS_IDS_DOMAIN
            ),
        ),
    )
    projection_error_original = HistoricalWindowProjectionError
    archive_error_original = _ArchiveRpcError

    def _task4b_final_identity_projection_error(
    ) -> HistoricalWindowProjectionError:
        error = RuntimeError.__new__(
            projection_error_original,
            "historical window projection failed",
        )
        object.__setattr__(error, "_reason_code", "authority_mismatch")
        object.__setattr__(error, "_failure_kind", "final_identity_drift")
        object.__setattr__(error, "_sealed", True)
        return error

    def _verify_task4b_semantic_dependency_manifest(
        *, expected_header_validation: Any = None
    ) -> None:
        import sys as runtime_sys

        closed_manifest = task4b_semantic_dependency_manifest
        del closed_manifest
        drifted = False
        try:
            for _role, canonical_name, original_module in task4b_canonical_modules:
                if runtime_sys.modules.get(canonical_name) is not original_module:
                    raise ValueError("historical semantic module identity drift")
            for group in (
                task4b_semantic_roots,
                task4b_semantic_callables,
                task4b_semantic_classes,
                task4b_class_surfaces,
                task4b_driver_callable_anchors[2:],
            ):
                for role, name, original in group:
                    if _task4b_resolve(_task4b_module(role), name) is not original:
                        raise ValueError("historical semantic callable drift")
            for (
                role, module_name, attribute_name,
                original_module, original_attribute,
            ) in task4b_semantic_module_callables:
                current_module = _task4b_resolve(
                    _task4b_module(role), module_name
                )
                if (
                    current_module is not original_module
                    or getattr(current_module, attribute_name)
                    is not original_attribute
                ):
                    raise ValueError("historical semantic module callable drift")
            for role, name, original, projection in (
                task4b_semantic_constants + task4b_driver_constant_anchors
            ):
                current = _task4b_resolve(_task4b_module(role), name)
                if (
                    current is not original
                    or _task4b_constant_projection(role, name, current)
                    != projection
                ):
                    raise ValueError("historical semantic constant drift")
            active_header_validation = _ACTIVE_HEADER_VALIDATION.get()
            if expected_header_validation is None:
                if active_header_validation is not None:
                    raise ValueError("historical semantic context is active")
            elif (
                type(expected_header_validation) is not tuple
                or len(expected_header_validation) != 2
                or type(active_header_validation) is not tuple
                or len(active_header_validation) != 2
                or active_header_validation[0]
                is not expected_header_validation[0]
                or active_header_validation[1]
                is not expected_header_validation[1]
            ):
                raise ValueError("historical semantic context differs")
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            drifted = True
        if drifted:
            raise _task4b_final_identity_projection_error()
        return None

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
    post_leaf_keys = (
        "schema", "segment", "segment_local_index", "leaf_index",
        "request_ids", "request_count", "canonical_request_sha256",
        "response_ids", "exchange_index", "logical_batch_index",
        "attempt_index", "request_byte_count", "decoded_byte_count",
        "decoded_sha256", "wire_byte_count", "wire_sha256",
        "wire_hash_authority", "spool_member_index", "spool_offset",
        "spool_length", "spool_member_sha256",
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
                                lower_raw_end = object()
                                lower_probe_raw.append(lower_raw_end)
                                lower_witness_raw.append(lower_raw_end)
                                lower_probe_raw.reverse()
                                lower_witness_raw.reverse()
                                lower_capture = project_historical_lower_bound_capture(
                                    anchor_capture=anchor_capture,
                                    lookback_seconds=pre_record[
                                        "lookback_seconds"
                                    ],
                                    search_probes=iter(
                                        lower_probe_raw.pop, lower_raw_end
                                    ),
                                    boundary_witness=iter(
                                        lower_witness_raw.pop, lower_raw_end
                                    ),
                                )
                                if lower_probe_raw or lower_witness_raw:
                                    raise ValueError(
                                        "historical lower replay did not drain"
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
        record["state"] = "consumed_failed"
        surface_drifted = False
        current_scan_objects = ()
        exported_scan_names = None
        exported_scan_objects = None
        try:
            current_scan_objects = tuple(
                _task4b_resolve(scan_module, name)
                for name in task4b_scan_local_names
            )
            exported_scan_names = getattr(
                scan_module, "_TASK4B_SCAN_LOCAL_SURFACE_NAMES"
            )
            exported_scan_objects = getattr(
                scan_module, "_TASK4B_SCAN_LOCAL_SURFACE_OBJECTS"
            )
        except (AttributeError, TypeError):
            surface_drifted = True
        if (
            surface_drifted
            or sys.modules.get("scripts.historical_foundry_scan")
            is not scan_module
            or type(task4b_scan_local_originals) is not tuple
            or len(task4b_scan_local_originals) != 5
            or exported_scan_names != task4b_scan_local_names
            or type(exported_scan_objects) is not tuple
            or len(exported_scan_objects) != 5
            or not all(
                current is original is exported
                for current, original, exported in zip(
                    current_scan_objects,
                    task4b_scan_local_originals,
                    exported_scan_objects,
                )
            )
        ):
            raise _ArchiveRpcError(
                "authority_mismatch", "final_identity_drift"
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
        return (
            task4b_attestation_provenance,
            task4b_scan_local_originals,
        )

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
            lower_raw_end = object()
            probe_raw.append(lower_raw_end)
            witness_raw.append(lower_raw_end)
            probe_raw.reverse()
            witness_raw.reverse()
            lower_capture = project_historical_lower_bound_capture(
                anchor_capture=anchor_capture,
                lookback_seconds=lookback_seconds,
                search_probes=iter(probe_raw.pop, lower_raw_end),
                boundary_witness=iter(witness_raw.pop, lower_raw_end),
            )
            if probe_raw or witness_raw:
                raise ValueError("historical lower capture did not drain")
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

    header_declaration = project_historical_header_inventory
    window_declaration = project_historical_window_projection
    driver_ack = object()
    driver_expect_eof = object()
    driver_eof = object()
    task4b_driver_provenance = object()
    task4b_active_driver = contextvars.ContextVar(
        "historical_foundry_active_task4b_semantic_driver",
        default=None,
    )
    task4b_materialize_anchor_stage = task4b_semantic_roots[0][2]
    task4b_project_anchor_capture = task4b_semantic_roots[1][2]
    task4b_project_lower_capture = task4b_semantic_roots[2][2]
    task4b_build_window_plan = task4b_semantic_roots[3][2]
    task4b_iter_header_batches = task4b_semantic_roots[4][2]
    task4b_iter_state_batches = task4b_semantic_roots[5][2]
    task4b_project_complete_root = task4b_semantic_roots[6][2]

    def _task4b_driver_transaction(kind: str) -> Any:
        transaction = task4b_active_driver.get()
        if transaction is None:
            return None
        if (
            type(transaction) is not dict
            or transaction.get("provenance") is not task4b_driver_provenance
            or transaction.get("state") != "constructing_" + kind
            or transaction.get("kind") != kind
            or type(transaction.get("record")) is not dict
            or transaction["record"].get("state")
            != "capture_replay_bound"
        ):
            _reject_task4b_capability()
        transaction["state"] = "active_" + kind
        return transaction

    def _verify_task4b_driver_transaction(
        transaction: Dict[str, Any], kind: str
    ) -> None:
        if (
            type(transaction) is not dict
            or task4b_active_driver.get() is not transaction
            or transaction.get("provenance") is not task4b_driver_provenance
            or transaction.get("state") != "active_" + kind
            or transaction.get("kind") != kind
            or type(transaction.get("record")) is not dict
            or transaction["record"].get("state")
            != "capture_replay_bound"
        ):
            _reject_task4b_capability()
        return None

    def _call_task4b_semantic_root(
        function: Callable[..., Any],
        *args: Any,
        expected_header_validation: Any = None,
        **kwargs: Any
    ) -> Any:
        _verify_task4b_semantic_dependency_manifest(
            expected_header_validation=expected_header_validation
        )
        body_value = None
        body_error = None
        body_traceback = None
        try:
            body_value = function(*args, **kwargs)
        except BaseException as error:
            body_error = error
            body_traceback = error.__traceback__
        post_error = None
        post_traceback = None
        try:
            _verify_task4b_semantic_dependency_manifest(
                expected_header_validation=expected_header_validation
            )
        except BaseException as error:
            post_error = error
            post_traceback = error.__traceback__
        if body_error is not None and not isinstance(body_error, Exception):
            body_error.__context__ = None
            body_error.__cause__ = None
            raise body_error.with_traceback(body_traceback) from None
        if post_error is not None and not isinstance(post_error, Exception):
            post_error.__context__ = None
            post_error.__cause__ = None
            raise post_error.with_traceback(post_traceback) from None
        if post_error is not None:
            raise post_error.with_traceback(post_traceback) from None
        if body_error is not None:
            raise body_error.with_traceback(body_traceback) from None
        _verify_task4b_semantic_dependency_manifest(
            expected_header_validation=expected_header_validation
        )
        return body_value

    def _new_task4b_exchange_replay(
        *, source: Any
    ) -> Iterator["_ProductionHistoricalWindowCaptureReplayEvent"]:
        def verify_replay_ledgers(record: Dict[str, Any]) -> None:
            replay_invalid = False
            observed = None
            try:
                observed = reconciliation_replay_digests(
                    record["post_root_ledger"],
                    record["post_leaf_ledger"],
                    record["compact_projection"],
                )
            except BaseException as error:
                if not isinstance(error, Exception):
                    raise
                replay_invalid = True
            if replay_invalid:
                raise _ArchiveRpcError(
                    "authority_mismatch",
                    "historical_window_reconciliation_mismatch",
                )
            if observed != record.get("replay_digests"):
                raise _ArchiveRpcError(
                    "authority_mismatch",
                    "historical_window_reconciliation_mismatch",
                )
            return None

        matches = []
        for reconciliation_reference, candidate in tuple(
            reconciliation_registry.values()
        ):
            source_reference = candidate.get("capture_source_ref")
            if (
                callable(source_reference)
                and source_reference() is source
            ):
                matches.append((reconciliation_reference, candidate))
        if len(matches) != 1:
            _reject_task4b_capability()
        reconciliation_reference, record = matches[0]
        reconciliation = reconciliation_reference()
        if (
            type(reconciliation)
            is not _ProductionHistoricalWindowReconciliation
            or record.get("capture_reconciliation_ref")() is not reconciliation
            or record.get("capture_view_ref")() is None
        ):
            _reject_task4b_capability()
        _verify_task4b_scan_association_currentness(
            reconciliation=reconciliation,
            record=record,
            expected_state="capture_replay_bound",
        )
        final_identity_error = False
        try:
            _verify_task4b_semantic_dependency_manifest()
        except projection_error_original as error:
            if (
                error.reason_code == "authority_mismatch"
                and error.failure_kind == "final_identity_drift"
            ):
                final_identity_error = True
            else:
                raise
        if final_identity_error:
            raise archive_error_original(
                "authority_mismatch", "final_identity_drift"
            ) from None
        verify_replay_ledgers(record)
        projection_invalid = False
        try:
            finalization = dict(record["finalization"])
            compact_rows = finalization["successful_exchanges"]
            post_leaves = record["post_leaf_ledger"]
            post_roots = record["post_root_ledger"]
        except (KeyError, TypeError, ValueError):
            projection_invalid = True
        if projection_invalid:
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            )
        if (
            type(compact_rows) is not tuple
            or type(post_leaves) is not tuple
            or type(post_roots) is not tuple
            or not compact_rows
            or len(compact_rows) != len(post_leaves)
        ):
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            )
        leaves_by_logical_batch = {}
        for leaf in post_leaves:
            logical_batch_index = (
                leaf.get("logical_batch_index")
                if type(leaf) is dict else None
            )
            if type(logical_batch_index) is not int:
                raise _ArchiveRpcError(
                    "authority_mismatch",
                    "historical_window_reconciliation_mismatch",
                )
            leaves_by_logical_batch.setdefault(
                logical_batch_index, []
            ).append(leaf)
        root_by_logical_batch = {}
        for expected_logical_index, root in enumerate(post_roots, 1):
            logical_batch_index = (
                root.get("logical_batch_index")
                if type(root) is dict else None
            )
            leaves = leaves_by_logical_batch.get(logical_batch_index)
            intervals = (
                root.get("observed_http_413_intervals", ())
                if type(root) is dict else None
            )
            success_indices = (
                root.get("success_exchange_indices")
                if type(root) is dict else None
            )
            attempt_count = (
                root.get("attempt_count")
                if type(root) is dict else None
            )
            if (
                type(logical_batch_index) is not int
                or logical_batch_index <= 0
                or logical_batch_index != expected_logical_index
                or logical_batch_index in root_by_logical_batch
                or type(leaves) is not list
                or not leaves
                or type(intervals) is not tuple
                or type(success_indices) is not tuple
                or type(attempt_count) is not int
                or attempt_count <= 0
                or root.get("leaf_count") != len(leaves)
                or success_indices
                != tuple(leaf.get("exchange_index") for leaf in leaves)
                or tuple(leaf.get("leaf_index") for leaf in leaves)
                != tuple(range(len(leaves)))
                or any(
                    type(interval) is not dict
                    or tuple(interval) != (
                        "attempt_index", "first_request_id",
                        "last_request_id", "request_count",
                    )
                    for interval in intervals
                )
                or tuple(sorted(
                    tuple(
                        interval["attempt_index"]
                        for interval in intervals
                    )
                    + tuple(
                        leaf.get("attempt_index") for leaf in leaves
                    )
                )) != tuple(range(1, attempt_count + 1))
            ):
                raise _ArchiveRpcError(
                    "authority_mismatch",
                    "historical_window_reconciliation_mismatch",
                )
            root_by_logical_batch[logical_batch_index] = root
        if len(leaves_by_logical_batch) != len(root_by_logical_batch):
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            )

        def drive() -> Iterator[
            "_ProductionHistoricalWindowCaptureReplayEvent"
        ]:
            completed = False
            ordinary_failure = False
            identity_drift = False
            header_driver = None
            window_driver = None
            driver_token = None
            transaction = {
                "provenance": task4b_driver_provenance,
                "state": "installing",
                "kind": None,
                "record": record,
                "source": source,
            }
            compact_position = [0]

            def root_role(root: Dict[str, Any]) -> str:
                segment = root.get("segment")
                if segment == "anchor_stage":
                    return "anchor_stage"
                if segment == "lower_observation":
                    return "lower_observation"
                if segment != "window_root":
                    raise ValueError("historical capture root segment differs")
                role = {
                    "header": "headers",
                    "reserve": "reserves",
                    "price": "prices",
                    "fee_history": "fees",
                    "final_anchor": "final_anchor",
                }.get(root.get("kind"))
                if role is None:
                    raise ValueError("historical capture root role differs")
                return role

            def emit(payload: Tuple[Any, ...]) -> Iterator[
                "_ProductionHistoricalWindowCaptureReplayEvent"
            ]:
                _verify_task4b_scan_association_currentness(
                    reconciliation=reconciliation,
                    record=record,
                    expected_state="capture_replay_bound",
                )
                _verify_task4b_semantic_dependency_manifest()
                expected_event_index = record.get("next_event_index")
                event = _issue_task4b_capture_replay_event(
                    record=record, source=source, payload=payload
                )
                _verify_task4b_semantic_dependency_manifest()
                yield event
                if (
                    capture_replay_event_registry.get(id(event)) is not None
                    or record.get("live_event") is not None
                    or record.get("event_issuer_state") != "ready"
                    or record.get("next_event_index")
                    != expected_event_index + 1
                ):
                    _reject_task4b_capability()
                return None

            def read_root(
                stream: Iterator[Any], root: Dict[str, Any]
            ) -> Iterator[Any]:
                logical_batch_index = root["logical_batch_index"]
                leaves = leaves_by_logical_batch.get(logical_batch_index)
                success_indices = root.get("success_exchange_indices")
                if (
                    type(leaves) is not list
                    or type(success_indices) is not tuple
                    or success_indices
                    != tuple(leaf.get("exchange_index") for leaf in leaves)
                ):
                    raise ValueError("historical capture root leaves differ")
                if root["segment"] == "lower_observation":
                    request_ids = (root.get("request_id"),)
                    response_ids = (root.get("response_id"),)
                    local_index = root.get("observation_index")
                else:
                    request_ids = root.get("request_ids")
                    response_ids = root.get("response_ids", request_ids)
                    local_index = (
                        root.get("stage_index")
                        if root["segment"] == "anchor_stage"
                        else root.get("root_index")
                    )
                if (
                    type(request_ids) is not tuple
                    or not request_ids
                    or type(response_ids) is not tuple
                    or response_ids != request_ids
                ):
                    raise ValueError("historical capture root IDs differ")
                requests_by_id = {}
                responses_by_id = {}
                for leaf_index, post_leaf in enumerate(leaves):
                    position = compact_position[0]
                    if position >= len(compact_rows) or position >= len(post_leaves):
                        raise ValueError("historical capture exchange is missing")
                    expected_compact = compact_rows[position]
                    expected_leaf = post_leaves[position]
                    try:
                        supplied = next(stream)
                    except StopIteration:
                        raise ValueError(
                            "historical capture source ended early"
                        )
                    if (
                        type(supplied) is not tuple
                        or len(supplied) != 3
                        or type(supplied[0]) is not dict
                        or type(supplied[1]) is not bytes
                        or type(supplied[2]) is not bytes
                        or type(expected_compact) is not dict
                        or tuple(expected_compact) != receipt_keys
                        or supplied[0] != expected_compact
                        or tuple(supplied[0]) != receipt_keys
                        or supplied[0].get("exchange_index") != position + 1
                        or expected_leaf is not post_leaf
                        or type(post_leaf) is not dict
                        or tuple(post_leaf) != post_leaf_keys
                        or post_leaf.get("schema")
                        != "historical_foundry_leaf_ledger/v1"
                        or post_leaf.get("logical_batch_index")
                        != logical_batch_index
                        or post_leaf.get("leaf_index") != leaf_index
                        or post_leaf.get("segment") != root["segment"]
                        or post_leaf.get("segment_local_index") != local_index
                    ):
                        raise ValueError(
                            "historical capture exchange join differs"
                        )
                    compact, request_bytes, decoded_bytes = supplied
                    for compact_key, leaf_key in (
                        ("exchange_index", "exchange_index"),
                        ("logical_batch_index", "logical_batch_index"),
                        ("attempt_index", "attempt_index"),
                        ("request_ids", "request_ids"),
                        ("response_ids", "response_ids"),
                        ("request_sha256", "canonical_request_sha256"),
                        ("request_byte_count", "request_byte_count"),
                        ("decoded_byte_count", "decoded_byte_count"),
                        ("decoded_sha256", "decoded_sha256"),
                        ("wire_byte_count", "wire_byte_count"),
                        ("wire_sha256", "wire_sha256"),
                        ("spool_member_index", "spool_member_index"),
                        ("spool_offset", "spool_offset"),
                        ("spool_length", "spool_length"),
                        ("spool_member_sha256", "spool_member_sha256"),
                    ):
                        if compact[compact_key] != post_leaf[leaf_key]:
                            raise ValueError(
                                "historical capture leaf join differs"
                            )
                    if (
                        success_indices[leaf_index]
                        != compact["exchange_index"]
                        or post_leaf["request_count"]
                        != len(compact["request_ids"])
                        or post_leaf["wire_hash_authority"]
                        != "task2b_sealed_not_rehashed"
                        or len(request_bytes)
                        != compact["request_byte_count"]
                        or hashlib.sha256(request_bytes).hexdigest()
                        != compact["request_sha256"]
                        or len(decoded_bytes)
                        != compact["decoded_byte_count"]
                        or hashlib.sha256(decoded_bytes).hexdigest()
                        != compact["decoded_sha256"]
                    ):
                        raise ValueError(
                            "historical capture physical leaf differs"
                        )
                    requests = bounded_reparse(request_bytes)
                    responses = bounded_reparse(decoded_bytes)
                    if (
                        type(requests) is not list
                        or type(responses) is not list
                        or _transport_core._archive_canonical_bytes(requests)
                        != request_bytes
                        or tuple(row.get("id") for row in requests)
                        != compact["request_ids"]
                        or tuple(row.get("id") for row in responses)
                        != compact["response_ids"]
                        or len(requests) != len(responses)
                        or set(compact["request_ids"])
                        != set(compact["response_ids"])
                    ):
                        raise ValueError(
                            "historical capture envelope differs"
                        )
                    for request in requests:
                        request_id = request.get("id")
                        if request_id in requests_by_id:
                            raise ValueError(
                                "historical capture request repeats"
                            )
                        requests_by_id[request_id] = request
                    for response in responses:
                        response_id = response.get("id")
                        if response_id in responses_by_id:
                            raise ValueError(
                                "historical capture response repeats"
                            )
                        responses_by_id[response_id] = response
                    payload = (
                        "exchange", dict(compact), dict(post_leaf)
                    )
                    yield from emit(payload)
                    compact_position[0] += 1
                    del supplied, compact, request_bytes, decoded_bytes
                    del requests, responses, payload
                if (
                    set(requests_by_id) != set(request_ids)
                    or set(responses_by_id) != set(request_ids)
                ):
                    raise ValueError(
                        "historical capture root response coverage differs"
                    )
                ordered_requests = tuple(
                    requests_by_id[request_id] for request_id in request_ids
                )
                ordered_responses = tuple(
                    responses_by_id[request_id] for request_id in request_ids
                )
                canonical_root = _transport_core._archive_canonical_bytes(
                    list(ordered_requests)
                )
                if (
                    len(canonical_root)
                    != root.get("canonical_request_byte_count")
                    or hashlib.sha256(canonical_root).hexdigest()
                    != root.get("canonical_request_sha256")
                ):
                    raise ValueError(
                        "historical capture root canonical request differs"
                    )
                return ordered_requests, ordered_responses

            def root_payload(
                post_root: Dict[str, Any], semantic_root: Any = None
            ) -> Tuple[Any, ...]:
                role = root_role(post_root)
                if role in ("headers", "reserves", "prices", "fees"):
                    if (
                        type(semantic_root) is not dict
                        or semantic_root.get("typed_role") != role
                        or semantic_root.get("typed_row_count")
                        != post_root.get("typed_row_count")
                        or semantic_root.get("typed_logical_sha256")
                        != post_root.get("typed_logical_sha256")
                        or type(semantic_root.get("rows")) is not tuple
                        or len(semantic_root["rows"])
                        != semantic_root["typed_row_count"]
                    ):
                        raise ValueError(
                            "historical capture semantic root differs"
                        )
                    canonical = json.dumps(
                        list(semantic_root["rows"]),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ).encode("utf-8")
                    return (
                        "root", dict(post_root), role, canonical,
                        semantic_root["typed_row_count"],
                        semantic_root["typed_logical_sha256"],
                    )
                if role == "final_anchor":
                    if (
                        type(semantic_root) is not dict
                        or semantic_root.get("typed_role") != role
                        or semantic_root.get("typed_row_count")
                        != post_root.get("typed_row_count")
                        or semantic_root.get("typed_logical_sha256")
                        != post_root.get("typed_logical_sha256")
                    ):
                        raise ValueError(
                            "historical capture final anchor differs"
                        )
                elif semantic_root is not None:
                    raise ValueError(
                        "historical capture bootstrap typed root differs"
                    )
                return ("root", dict(post_root), role, None, 0, None)

            try:
                if task4b_active_driver.get() is not None:
                    _reject_task4b_capability()
                driver_token = task4b_active_driver.set(transaction)
                transaction["state"] = "active_bootstrap"
                _verify_task4b_semantic_dependency_manifest()
                verify_replay_ledgers(record)
                _verify_task4b_scan_association_currentness(
                    reconciliation=reconciliation,
                    record=record,
                    expected_state="capture_replay_bound",
                )
                pre_entry = prefinalization_registry.get(
                    id(record.get("prefinalization"))
                )
                if (
                    pre_entry is None
                    or pre_entry[0]() is not record.get("prefinalization")
                    or type(pre_entry[1]) is not dict
                    or pre_entry[1].get("plan") is not record.get("plan")
                ):
                    raise ValueError(
                        "historical capture prefinalization differs"
                    )
                pre_record = pre_entry[1]
                anchor_plan = pre_record.get("anchor_plan")
                plan = record.get("plan")
                if type(anchor_plan) is not dict or type(plan) is not dict:
                    raise ValueError("historical capture plan differs")
                with source as entered_source:
                    if entered_source is not source:
                        _reject_task4b_capability()
                    stream = iter(entered_source)
                    anchor_responses = []
                    anchor_roots = []
                    root_position = 0
                    for stage_index in range(3):
                        if root_position >= len(post_roots):
                            raise ValueError(
                                "historical capture anchor root is missing"
                            )
                        post_root = post_roots[root_position]
                        if (
                            type(post_root) is not dict
                            or post_root.get("segment") != "anchor_stage"
                            or post_root.get("stage_index") != stage_index
                        ):
                            raise ValueError(
                                "historical capture anchor root differs"
                            )
                        expected_requests = _call_task4b_semantic_root(
                            task4b_materialize_anchor_stage,
                            anchor_plan,
                            stage_index,
                            tuple(anchor_responses),
                        )
                        requests, responses = yield from read_root(
                            stream, post_root
                        )
                        if requests != expected_requests:
                            raise ValueError(
                                "historical capture anchor requests differ"
                            )
                        anchor_responses.extend(responses)
                        anchor_roots.append(post_root)
                        if stage_index == 2:
                            anchor_capture = _call_task4b_semantic_root(
                                task4b_project_anchor_capture,
                                anchor_plan,
                                tuple(anchor_responses),
                            )
                            capture_digest = _typed_hash(
                                _ANCHOR_CAPTURE_DOMAIN, anchor_capture
                            )
                            inventory = anchor_capture.get(
                                "request_inventory"
                            )
                            if type(inventory) is not list or len(inventory) != 48:
                                raise ValueError(
                                    "historical capture anchor inventory differs"
                                )
                            for root_row, offsets in zip(
                                anchor_roots,
                                ((0, 2), (2, 39), (39, 48)),
                            ):
                                digest = _inventory_hasher(
                                    b"historical_foundry_anchor_stage_inventory/v1"
                                )
                                for row in inventory[offsets[0]:offsets[1]]:
                                    _inventory_update(digest, row)
                                if (
                                    root_row.get("anchor_capture_sha256")
                                    != capture_digest
                                    or root_row.get("stage_inventory_row_count")
                                    != offsets[1] - offsets[0]
                                    or root_row.get(
                                        "stage_inventory_logical_sha256"
                                    ) != digest.hexdigest()
                                ):
                                    raise ValueError(
                                        "historical capture anchor ledger differs"
                                    )
                        yield from emit(root_payload(post_root))
                        root_position += 1
                        del requests, responses, expected_requests
                    anchor_responses.clear()
                    del anchor_responses, anchor_roots

                    lower_probe_raw = []
                    lower_witness_raw = []
                    lower_roots = []
                    while (
                        root_position < len(post_roots)
                        and post_roots[root_position].get("segment")
                        == "lower_observation"
                    ):
                        post_root = post_roots[root_position]
                        requests, responses = yield from read_root(
                            stream, post_root
                        )
                        if len(requests) != 1 or len(responses) != 1:
                            raise ValueError(
                                "historical capture lower root differs"
                            )
                        raw = {
                            "request": requests[0],
                            "response": responses[0],
                        }
                        if post_root.get("observation_kind") == "search_probe":
                            lower_probe_raw.append(raw)
                        elif post_root.get("observation_kind") == "boundary_witness":
                            lower_witness_raw.append(raw)
                        else:
                            raise ValueError(
                                "historical capture lower kind differs"
                            )
                        lower_roots.append(post_root)
                        yield from emit(root_payload(post_root))
                        root_position += 1
                        del requests, responses, raw
                    if not lower_roots:
                        raise ValueError(
                            "historical capture lower proof is missing"
                        )
                    lower_raw_end = object()
                    lower_probe_raw.append(lower_raw_end)
                    lower_witness_raw.append(lower_raw_end)
                    lower_probe_raw.reverse()
                    lower_witness_raw.reverse()
                    lower_capture = _call_task4b_semantic_root(
                        task4b_project_lower_capture,
                        anchor_capture=anchor_capture,
                        lookback_seconds=pre_record.get("lookback_seconds"),
                        search_probes=iter(
                            lower_probe_raw.pop, lower_raw_end
                        ),
                        boundary_witness=iter(
                            lower_witness_raw.pop, lower_raw_end
                        ),
                    )
                    if lower_probe_raw or lower_witness_raw:
                        raise ValueError(
                            "historical capture lower proof did not drain"
                        )
                    replay_plan = _call_task4b_semantic_root(
                        task4b_build_window_plan,
                        lower_bound_capture=lower_capture,
                        anchor_capture=anchor_capture,
                    )
                    if replay_plan != plan:
                        raise ValueError(
                            "historical capture replay plan differs"
                        )
                    compact_lower = tuple(
                        lower_capture["search_probes"]
                    ) + tuple(lower_capture["boundary_witness"])
                    if len(compact_lower) != len(lower_roots):
                        raise ValueError(
                            "historical capture lower coverage differs"
                        )
                    for lower_root, compact in zip(
                        lower_roots, compact_lower
                    ):
                        if (
                            lower_root.get("request_sha256")
                            != compact.get("request_sha256")
                            or lower_root.get("result_sha256")
                            != compact.get("result_sha256")
                            or lower_root.get("response_sha256")
                            != compact.get("response_sha256")
                            or lower_root.get("lower_bound_capture_sha256")
                            != replay_plan.get(
                                "lower_bound_capture_sha256"
                            )
                        ):
                            raise ValueError(
                                "historical capture lower ledger differs"
                            )
                    lower_probe_raw.clear()
                    lower_witness_raw.clear()
                    del lower_probe_raw, lower_witness_raw, lower_roots
                    del compact_lower

                    transaction["kind"] = "header"
                    transaction["state"] = "constructing_header"
                    header_driver = new_header_driver(
                        plan=replay_plan,
                        anchor_capture=anchor_capture,
                        lower_bound_capture=lower_capture,
                    )
                    if transaction.get("state") != "active_header":
                        _reject_task4b_capability()
                    request = next(header_driver)
                    while (
                        root_position < len(post_roots)
                        and post_roots[root_position].get("segment")
                        == "window_root"
                        and post_roots[root_position].get("kind") == "header"
                    ):
                        post_root = post_roots[root_position]
                        requests, responses = yield from read_root(
                            stream, post_root
                        )
                        if (
                            type(request) is not dict
                            or request.get("requests") != requests
                            or request.get("root_index")
                            != post_root.get("root_index")
                            or request.get("block_start")
                            != post_root.get("block_start")
                            or request.get("block_stop")
                            != post_root.get("block_stop")
                        ):
                            raise ValueError(
                                "historical capture header descriptor differs"
                            )
                        semantic_root = header_driver.send(
                            (request, responses)
                        )
                        yield from emit(
                            root_payload(post_root, semantic_root)
                        )
                        request = header_driver.send(driver_ack)
                        root_position += 1
                        del requests, responses, semantic_root
                    if request is not driver_expect_eof:
                        raise ValueError(
                            "historical capture header coverage differs"
                        )
                    try:
                        header_driver.send(driver_eof)
                    except StopIteration as stopped:
                        header_inventory = stopped.value
                    else:
                        raise ValueError(
                            "historical capture header EOF differs"
                        )

                    transaction["kind"] = "window"
                    transaction["state"] = "constructing_window"
                    window_driver = new_window_driver(
                        plan=replay_plan,
                        anchor_capture=anchor_capture,
                        lower_bound_capture=lower_capture,
                        header_inventory=header_inventory,
                    )
                    if transaction.get("state") != "active_window":
                        _reject_task4b_capability()
                    request = next(window_driver)
                    while root_position < len(post_roots):
                        post_root = post_roots[root_position]
                        if (
                            post_root.get("segment") != "window_root"
                            or post_root.get("kind") == "header"
                        ):
                            raise ValueError(
                                "historical capture state root order differs"
                            )
                        requests, responses = yield from read_root(
                            stream, post_root
                        )
                        if (
                            type(request) is not dict
                            or request.get("requests") != requests
                            or request.get("root_index")
                            != post_root.get("root_index")
                            or request.get("kind") != post_root.get("kind")
                            or request.get("block_start")
                            != post_root.get("block_start")
                            or request.get("block_stop")
                            != post_root.get("block_stop")
                        ):
                            raise ValueError(
                                "historical capture state descriptor differs"
                            )
                        semantic_root = window_driver.send(
                            (request, responses)
                        )
                        yield from emit(
                            root_payload(post_root, semantic_root)
                        )
                        request = window_driver.send(driver_ack)
                        root_position += 1
                        del requests, responses, semantic_root
                    if request is not driver_expect_eof:
                        raise ValueError(
                            "historical capture state coverage differs"
                        )
                    try:
                        window_driver.send(driver_eof)
                    except StopIteration as stopped:
                        compact_projection = stopped.value
                    else:
                        raise ValueError(
                            "historical capture window EOF differs"
                        )
                    if compact_projection != record.get("compact_projection"):
                        raise ValueError(
                            "historical capture compact projection differs"
                        )
                    if compact_position[0] != len(compact_rows):
                        raise ValueError(
                            "historical capture exchange coverage differs"
                        )
                    try:
                        next(stream)
                    except StopIteration:
                        pass
                    else:
                        raise ValueError(
                            "historical capture source has extras"
                        )
                    _verify_task4b_scan_association_currentness(
                        reconciliation=reconciliation,
                        record=record,
                        expected_state="capture_replay_bound",
                    )
                    verify_replay_ledgers(record)
                    _verify_task4b_semantic_dependency_manifest()
                    height = header_inventory.get("row_count")
                    counts = (
                        ("headers", height),
                        ("reserves", 2 * height),
                        ("prices", height),
                        ("fees", height),
                    )
                    finish = (
                        "finish", len(compact_rows), counts,
                        record.get("prefinalization_digests"),
                        record.get("replay_digests"),
                    )
                    yield from emit(finish)
                transaction["state"] = "complete"
                transaction["kind"] = None
                record["event_issuer_state"] = "raw_complete"
                record["raw_exchange_replay_complete"] = True
                completed = True
                return
            except archive_error_original:
                raise
            except projection_error_original as error:
                if (
                    error.reason_code == "authority_mismatch"
                    and error.failure_kind == "final_identity_drift"
                ):
                    identity_drift = True
                else:
                    ordinary_failure = True
            except BaseException as error:
                if not isinstance(error, Exception):
                    raise
                ordinary_failure = True
            finally:
                for active_driver in (window_driver, header_driver):
                    if active_driver is not None:
                        active_driver.close()
                if driver_token is not None:
                    task4b_active_driver.reset(driver_token)
                pending_event = record.get("live_event")
                if pending_event is not None:
                    event_entry = capture_replay_event_registry.get(
                        id(pending_event)
                    )
                    if (
                        event_entry is not None
                        and event_entry[0] is pending_event
                    ):
                        event_entry[1]["state"] = "revoked"
                        event_entry[1]["source"] = None
                        event_entry[1]["association"] = None
                        event_entry[1]["payload"] = None
                        capture_replay_event_registry.pop(
                            id(pending_event), None
                        )
                    if record.get("live_event") is pending_event:
                        record["live_event"] = None
                transaction["source"] = None
                transaction["record"] = None
                transaction["kind"] = None
                if not completed:
                    transaction["state"] = "failed"
                    record["event_issuer_state"] = "failed"
                    record["state"] = "consumed_failed"
            if identity_drift:
                raise archive_error_original(
                    "authority_mismatch", "final_identity_drift"
                ) from None
            if ordinary_failure:
                raise archive_error_original(
                    "authority_mismatch",
                    "historical_window_reconciliation_mismatch",
                ) from None

        return drive()

    def new_header_driver(
        *,
        plan: Optional[Mapping[str, Any]] = None,
        anchor_capture: Optional[Mapping[str, Any]] = None,
        lower_bound_capture: Optional[Mapping[str, Any]] = None,
        source: Any = None,
    ) -> Iterator[Any]:
        if source is not None:
            if (
                plan is not None
                or anchor_capture is not None
                or lower_bound_capture is not None
            ):
                _reject_task4b_capability()
            return _new_task4b_exchange_replay(source=source)
        authenticated_transaction = _task4b_driver_transaction("header")
        declaration_anchor = header_declaration
        del declaration_anchor
        if authenticated_transaction is not None:
            _verify_task4b_driver_transaction(
                authenticated_transaction, "header"
            )
            _verify_task4b_semantic_dependency_manifest()
        validated_plan, anchor, lower = _validate_bound_inputs(
            plan, anchor_capture, lower_bound_capture
        )
        if authenticated_transaction is None:
            expected_iterator = _header_descriptor_rows(validated_plan)
        else:
            expected_iterator = _call_task4b_semantic_root(
                task4b_iter_header_batches, validated_plan
            )
        rows = []
        digest = _inventory_hasher(_HEADER_INVENTORY_DOMAIN)
        previous = None
        if authenticated_transaction is not None:
            _verify_task4b_semantic_dependency_manifest()

        def drive() -> Iterator[Any]:
            nonlocal previous
            for expected in expected_iterator:
                if authenticated_transaction is not None:
                    _verify_task4b_driver_transaction(
                        authenticated_transaction, "header"
                    )
                    _verify_task4b_semantic_dependency_manifest()
                raw_pair = yield expected
                root_failed = False
                failure_pair = (
                    "block_coverage_incomplete", "header_coverage_invalid"
                )
                try:
                    if type(raw_pair) is not tuple or len(raw_pair) != 2:
                        raise ValueError(
                            "historical header batch result is invalid"
                        )
                    descriptor, responses = raw_pair
                    if descriptor != expected:
                        raise ValueError(
                            "historical header descriptor order differs"
                        )
                    if authenticated_transaction is None:
                        root = _project_complete_historical_window_root(
                            plan=validated_plan,
                            descriptor=descriptor,
                            responses=responses,
                            header_inventory=None,
                        )
                    else:
                        root = _call_task4b_semantic_root(
                            task4b_project_complete_root,
                            plan=validated_plan,
                            descriptor=descriptor,
                            responses=responses,
                            header_inventory=None,
                        )
                except projection_error_original as error:
                    if (
                        authenticated_transaction is not None
                        and error.reason_code == "authority_mismatch"
                        and error.failure_kind == "final_identity_drift"
                    ):
                        raise
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
                        raise _failure(
                            "block_coverage_incomplete",
                            "header_continuity_invalid",
                        )
                    rows.append(row)
                    _inventory_update(digest, row)
                    previous = header
                if authenticated_transaction is not None:
                    _verify_task4b_driver_transaction(
                        authenticated_transaction, "header"
                    )
                    _verify_task4b_semantic_dependency_manifest()
                acknowledgement = yield root
                if acknowledgement is not driver_ack:
                    raise _failure(*failure_pair)
                del raw_pair, root

            if authenticated_transaction is not None:
                _verify_task4b_driver_transaction(
                    authenticated_transaction, "header"
                )
                _verify_task4b_semantic_dependency_manifest()
            eof = yield driver_expect_eof
            if eof is not driver_eof:
                raise _failure(
                    "block_coverage_incomplete", "header_coverage_invalid"
                )
            lower_header = lower["boundary_witness"][-1]["header"]
            if not rows or _header_from_inventory_row(rows[0]) != lower_header:
                raise _failure(
                    "block_coverage_incomplete", "header_coverage_invalid"
                )
            if _header_from_inventory_row(rows[-1]) != anchor:
                raise _failure(
                    "block_coverage_incomplete", "header_coverage_invalid"
                )
            for observation in lower["search_probes"]:
                number = observation["block_number"]
                if (
                    validated_plan["lower_bound_number"]
                    <= number
                    <= validated_plan["anchor_number"]
                    and _header_from_inventory_row(
                        rows[number - validated_plan["lower_bound_number"]]
                    )
                    != observation["header"]
                ):
                    raise _failure(
                        "block_coverage_incomplete", "header_coverage_invalid"
                    )
            inventory = {
                "schema": "historical_foundry_header_inventory/v1",
                "anchor_capture_sha256": validated_plan[
                    "anchor_capture_sha256"
                ],
                "lower_bound_capture_sha256": validated_plan[
                    "lower_bound_capture_sha256"
                ],
                "anchor_header_sha256": _typed_hash(
                    _NORMALIZED_HEADER_DOMAIN, anchor
                ),
                "lower_header_sha256": _typed_hash(
                    _NORMALIZED_HEADER_DOMAIN, lower_header
                ),
                "lower_bound_number": validated_plan["lower_bound_number"],
                "anchor_number": validated_plan["anchor_number"],
                "row_count": len(rows),
                "rows": tuple(rows),
                "logical_sha256": digest.hexdigest(),
            }
            _validate_header_inventory(
                plan=validated_plan,
                header_inventory=inventory,
                anchor=anchor,
                lower=lower,
            )
            if authenticated_transaction is not None:
                _verify_task4b_driver_transaction(
                    authenticated_transaction, "header"
                )
                _verify_task4b_semantic_dependency_manifest()
            return inventory

        return drive()

    def new_window_driver(
        *,
        plan: Mapping[str, Any],
        anchor_capture: Mapping[str, Any],
        lower_bound_capture: Mapping[str, Any],
        header_inventory: Mapping[str, Any],
    ) -> Iterator[Any]:
        authenticated_transaction = _task4b_driver_transaction("window")
        declaration_anchor = window_declaration
        del declaration_anchor
        if authenticated_transaction is not None:
            _verify_task4b_driver_transaction(
                authenticated_transaction, "window"
            )
            _verify_task4b_semantic_dependency_manifest()
        validated_plan, anchor, lower = _validate_bound_inputs(
            plan, anchor_capture, lower_bound_capture
        )
        validated_inventory, header_rows = _validate_header_inventory(
            plan=validated_plan,
            header_inventory=header_inventory,
            anchor=anchor,
            lower=lower,
        )
        _validate_header_base_fee_chain(header_rows)
        anchor_price = _anchor_price_authority(anchor_capture)
        digests = {
            "reserves": _inventory_hasher(_RESERVE_INVENTORY_DOMAIN),
            "prices": _inventory_hasher(_PRICE_INVENTORY_DOMAIN),
            "fees": _inventory_hasher(_FEE_INVENTORY_DOMAIN),
        }
        counts = {"reserves": 0, "prices": 0, "fees": 0}
        final_header = None
        previous_request_id = (
            validated_plan["first_request_id"]
            + validated_plan["block_count"]
            - 1
        )
        if authenticated_transaction is not None:
            _verify_task4b_semantic_dependency_manifest()

        def drive() -> Iterator[Any]:
            nonlocal final_header, previous_request_id
            if authenticated_transaction is None:
                expected_iterator = _state_descriptor_rows(
                    validated_plan, header_rows
                )
                authenticated_validation = None
            else:
                _verify_task4b_driver_transaction(
                    authenticated_transaction, "window"
                )
                expected_iterator = _call_task4b_semantic_root(
                    task4b_iter_state_batches,
                    plan=validated_plan,
                    header_inventory=validated_inventory,
                )
                authenticated_validation = (
                    expected_iterator._validated_header_token
                )
            for expected in expected_iterator:
                pair = _coverage_pair(expected["kind"])
                if authenticated_transaction is not None:
                    _verify_task4b_driver_transaction(
                        authenticated_transaction, "window"
                    )
                    _verify_task4b_semantic_dependency_manifest()
                raw_pair = yield expected
                root_failed = False
                failure_pair = pair
                try:
                    if type(raw_pair) is not tuple or len(raw_pair) != 2:
                        raise ValueError(
                            "historical state batch result is invalid"
                        )
                    descriptor, responses = raw_pair
                    if descriptor != expected:
                        raise ValueError(
                            "historical state descriptor order differs"
                        )
                    active_validation = (
                        (validated_inventory, validated_plan)
                        if authenticated_validation is None
                        else authenticated_validation
                    )
                    validation_token = _ACTIVE_HEADER_VALIDATION.set(
                        active_validation
                    )
                    try:
                        if authenticated_transaction is not None:
                            _verify_task4b_semantic_dependency_manifest(
                                expected_header_validation=active_validation
                            )
                            root = _call_task4b_semantic_root(
                                task4b_project_complete_root,
                                plan=active_validation[1],
                                descriptor=descriptor,
                                responses=responses,
                                header_inventory=active_validation[0],
                                expected_header_validation=active_validation,
                            )
                        else:
                            root = _project_complete_historical_window_root(
                                plan=validated_plan,
                                descriptor=descriptor,
                                responses=responses,
                                header_inventory=validated_inventory,
                            )
                        if authenticated_transaction is not None:
                            _verify_task4b_semantic_dependency_manifest(
                                expected_header_validation=active_validation
                            )
                    finally:
                        _ACTIVE_HEADER_VALIDATION.reset(validation_token)
                    if authenticated_transaction is not None:
                        _verify_task4b_semantic_dependency_manifest()
                    request_ids = root["request_ids"]
                    if (
                        not request_ids
                        or request_ids[0] != previous_request_id + 1
                        or request_ids
                        != tuple(range(request_ids[0], request_ids[-1] + 1))
                    ):
                        raise ValueError(
                            "historical state request ledger differs"
                        )
                    previous_request_id = request_ids[-1]
                    role = root["typed_role"]
                    if role in digests:
                        for row in root["rows"]:
                            _inventory_update(digests[role], row)
                            counts[role] += 1
                            if (
                                role == "prices"
                                and row["block_number"] == anchor["number"]
                                and any(
                                    row[key] != anchor_price[key]
                                    for key in (
                                        "round_id", "phase_id", "answer",
                                        "started_at", "updated_at",
                                        "answered_in_round",
                                    )
                                )
                            ):
                                raise _failure(
                                    "price_snapshot_incomplete",
                                    "price_round_invalid",
                                )
                    elif role == "final_anchor":
                        if root["typed_row_count"] != 1:
                            raise ValueError(
                                "historical final anchor row count differs"
                            )
                        final_header = root["rows"][0]["header"]
                    else:
                        raise ValueError("historical typed role is invalid")
                except projection_error_original as error:
                    if (
                        authenticated_transaction is not None
                        and error.reason_code == "authority_mismatch"
                        and error.failure_kind == "final_identity_drift"
                    ):
                        raise
                    root_failed = True
                    failure_pair = _captured_failure_pair(error, failure_pair)
                except Exception:
                    root_failed = True
                if root_failed:
                    raise _failure(*failure_pair)
                if authenticated_transaction is not None:
                    _verify_task4b_driver_transaction(
                        authenticated_transaction, "window"
                    )
                    _verify_task4b_semantic_dependency_manifest()
                acknowledgement = yield root
                if acknowledgement is not driver_ack:
                    raise _failure(*pair)
                del raw_pair, root

            if authenticated_transaction is not None:
                _verify_task4b_driver_transaction(
                    authenticated_transaction, "window"
                )
                _verify_task4b_semantic_dependency_manifest()
            eof = yield driver_expect_eof
            if eof is not driver_eof:
                raise _failure(
                    "authority_mismatch", "request_ledger_invalid"
                )
            if previous_request_id != validated_plan["last_request_id"]:
                raise _failure(
                    "authority_mismatch", "request_ledger_invalid"
                )
            if final_header != lower["anchor_header"]:
                raise _failure("anchor_changed", "final_anchor_mismatch")
            if (
                counts["reserves"] != 2 * validated_plan["block_count"]
                or counts["prices"] != validated_plan["block_count"]
                or counts["fees"] != validated_plan["block_count"]
            ):
                raise _failure(
                    "authority_mismatch", "request_ledger_invalid"
                )
            _validate_header_inventory(
                plan=validated_plan,
                header_inventory=header_inventory,
                anchor=anchor,
                lower=lower,
            )
            lower_number = validated_plan["lower_bound_number"]
            anchor_number = validated_plan["anchor_number"]
            predecessor = (
                lower["boundary_witness"][0]["header"]
                if lower_number > 0 else None
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
                "anchor_capture_sha256": validated_plan[
                    "anchor_capture_sha256"
                ],
                "lower_bound_capture_sha256": validated_plan[
                    "lower_bound_capture_sha256"
                ],
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
                        "logical_sha256": validated_inventory[
                            "logical_sha256"
                        ],
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
                raise _failure(
                    "authority_mismatch", "fixture_input_invalid"
                )
            if authenticated_transaction is not None:
                _verify_task4b_driver_transaction(
                    authenticated_transaction, "window"
                )
                _verify_task4b_semantic_dependency_manifest()
            return projection

        return drive()

    def header_drain(
        *,
        plan: Mapping[str, Any],
        anchor_capture: Mapping[str, Any],
        lower_bound_capture: Mapping[str, Any],
        batch_results: Iterable[
            Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
        ],
    ) -> Mapping[str, Any]:
        driver = new_header_driver(
            plan=plan,
            anchor_capture=anchor_capture,
            lower_bound_capture=lower_bound_capture,
        )
        result_iterator = _iterator_once(
            batch_results,
            ("block_coverage_incomplete", "header_coverage_invalid"),
        )
        try:
            request = next(driver)
            while request is not driver_expect_eof:
                raw_pair = _next_input(
                    result_iterator,
                    ("block_coverage_incomplete", "header_coverage_invalid"),
                )
                root = driver.send(raw_pair)
                del raw_pair, root
                request = driver.send(driver_ack)
            if _next_input(
                result_iterator,
                ("block_coverage_incomplete", "header_coverage_invalid"),
                allow_end=True,
            ) is not _END_OF_INPUT:
                raise _failure(
                    "block_coverage_incomplete", "header_coverage_invalid"
                )
            try:
                driver.send(driver_eof)
            except StopIteration as completed:
                return completed.value
            raise _failure(
                "block_coverage_incomplete", "header_coverage_invalid"
            )
        finally:
            driver.close()

    def window_drain(
        *,
        plan: Mapping[str, Any],
        anchor_capture: Mapping[str, Any],
        lower_bound_capture: Mapping[str, Any],
        header_inventory: Mapping[str, Any],
        batch_results: Iterable[
            Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
        ],
    ) -> Mapping[str, Any]:
        driver = new_window_driver(
            plan=plan,
            anchor_capture=anchor_capture,
            lower_bound_capture=lower_bound_capture,
            header_inventory=header_inventory,
        )
        result_iterator = _iterator_once(
            batch_results,
            ("authority_mismatch", "request_ledger_invalid"),
        )
        try:
            request = next(driver)
            while request is not driver_expect_eof:
                raw_pair = _next_input(
                    result_iterator,
                    _coverage_pair(request["kind"]),
                )
                root = driver.send(raw_pair)
                del raw_pair, root
                request = driver.send(driver_ack)
            if _next_input(
                result_iterator,
                ("authority_mismatch", "request_ledger_invalid"),
                allow_end=True,
            ) is not _END_OF_INPUT:
                raise _failure(
                    "authority_mismatch", "request_ledger_invalid"
                )
            try:
                driver.send(driver_eof)
            except StopIteration as completed:
                return completed.value
            raise _failure("authority_mismatch", "request_ledger_invalid")
        finally:
            driver.close()

    def project_historical_header_inventory_wrapper(
        *,
        plan: Mapping[str, Any],
        anchor_capture: Mapping[str, Any],
        lower_bound_capture: Mapping[str, Any],
        batch_results: Iterable[
            Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
        ],
    ) -> Mapping[str, Any]:
        return header_drain(
            plan=plan,
            anchor_capture=anchor_capture,
            lower_bound_capture=lower_bound_capture,
            batch_results=batch_results,
        )

    def project_historical_window_projection_wrapper(
        *,
        plan: Mapping[str, Any],
        anchor_capture: Mapping[str, Any],
        lower_bound_capture: Mapping[str, Any],
        header_inventory: Mapping[str, Any],
        batch_results: Iterable[
            Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
        ],
    ) -> Mapping[str, Any]:
        return window_drain(
            plan=plan,
            anchor_capture=anchor_capture,
            lower_bound_capture=lower_bound_capture,
            header_inventory=header_inventory,
            batch_results=batch_results,
        )

    for wrapper, declaration, name in (
        (
            project_historical_header_inventory_wrapper,
            header_declaration,
            "project_historical_header_inventory",
        ),
        (
            project_historical_window_projection_wrapper,
            window_declaration,
            "project_historical_window_projection",
        ),
    ):
        wrapper.__name__ = name
        wrapper.__qualname__ = name
        wrapper.__module__ = __name__
        wrapper.__annotations__ = dict(declaration.__annotations__)

    def replay_surface(
        *,
        source: Any,
    ) -> Iterator["_ProductionHistoricalWindowCaptureReplayEvent"]:
        constructors = (new_header_driver, new_window_driver)
        del constructors
        return new_header_driver(source=source)

    replay_surface.__name__ = (
        "_replay_production_historical_window_capture_from_bound_storage"
    )
    replay_surface.__qualname__ = replay_surface.__name__
    replay_surface.__module__ = __name__

    def _reject_task4b_capability() -> None:
        raise _ArchiveRpcError(
            "authority_mismatch",
            "historical_window_capability_invalid",
        )

    def _verify_task4b_scan_association_currentness(
        *,
        reconciliation: Any,
        record: Dict[str, Any],
        expected_state: str,
    ) -> None:
        drifted = False
        try:
            binding = record["binding"]
            current_scan_objects = tuple(
                _task4b_resolve(scan_module, name)
                for name in task4b_scan_local_names
            )
            exported_scan_names = getattr(
                scan_module, "_TASK4B_SCAN_LOCAL_SURFACE_NAMES"
            )
            exported_scan_objects = getattr(
                scan_module, "_TASK4B_SCAN_LOCAL_SURFACE_OBJECTS"
            )
        except (AttributeError, KeyError, TypeError):
            drifted = True
            binding = None
            current_scan_objects = ()
            exported_scan_names = None
            exported_scan_objects = None
        if (
            drifted
            or sys.modules.get("scripts.historical_foundry_scan")
            is not scan_module
            or type(record) is not dict
            or expected_state not in (
                "consumed_by_mint", "capture_replay_bound"
            )
            or record.get("state") != expected_state
            or type(binding) is not tuple
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
            or type(task4b_scan_local_originals) is not tuple
            or len(task4b_scan_local_originals) != 5
            or exported_scan_names != task4b_scan_local_names
            or type(exported_scan_objects) is not tuple
            or len(exported_scan_objects) != 5
            or not all(
                current is original is exported
                for current, original, exported in zip(
                    current_scan_objects,
                    task4b_scan_local_originals,
                    exported_scan_objects,
                )
            )
        ):
            raise _ArchiveRpcError(
                "authority_mismatch", "final_identity_drift"
            )
        return None

    def _validate_task4b_capture_replay_payload(
        payload: Any
    ) -> bool:
        if type(payload) is not tuple or not payload or type(payload[0]) is not str:
            return False
        tag = payload[0]
        if tag == "exchange":
            return (
                len(payload) == 3
                and type(payload[1]) is dict
                and type(payload[2]) is dict
                and tuple(payload[1]) == receipt_keys
                and tuple(payload[2]) == post_leaf_keys
            )
        if tag == "root":
            if (
                len(payload) != 6
                or type(payload[1]) is not dict
                or type(payload[2]) is not str
            ):
                return False
            root = payload[1]
            role = payload[2]
            segment = root.get("segment")
            expected_role = None
            if segment == "anchor_stage":
                expected_role = "anchor_stage"
            elif segment == "lower_observation":
                expected_role = "lower_observation"
            elif segment == "window_root":
                expected_role = {
                    "header": "headers",
                    "reserve": "reserves",
                    "price": "prices",
                    "fee_history": "fees",
                    "final_anchor": "final_anchor",
                }.get(root.get("kind"))
            if role != expected_role:
                return False
            success_indices = root.get("success_exchange_indices")
            if (
                type(root.get("logical_batch_index")) is not int
                or root["logical_batch_index"] <= 0
                or type(success_indices) is not tuple
                or not success_indices
                or any(type(index) is not int or index <= 0 for index in success_indices)
                or root.get("leaf_count") != len(success_indices)
            ):
                return False
            if role in ("headers", "reserves", "prices", "fees"):
                return (
                    type(payload[3]) is bytes
                    and type(payload[4]) is int
                    and payload[4] > 0
                    and type(payload[5]) is str
                    and _HASH64.fullmatch(payload[5]) is not None
                    and root.get("typed_role") == role
                    and root.get("typed_row_count") == payload[4]
                    and root.get("typed_logical_sha256") == payload[5]
                )
            return (
                payload[3] is None
                and type(payload[4]) is int
                and payload[4] == 0
                and payload[5] is None
            )
        if tag != "finish" or len(payload) != 5:
            return False
        exchange_count, counts, pre_digests, replay_digests = payload[1:]
        expected_roles = ("headers", "reserves", "prices", "fees")
        if (
            type(exchange_count) is not int
            or exchange_count <= 0
            or type(counts) is not tuple
            or len(counts) != 4
            or any(
                type(pair) is not tuple
                or len(pair) != 2
                or type(pair[0]) is not str
                or type(pair[1]) is not int
                or pair[1] <= 0
                for pair in counts
            )
            or tuple(pair[0] for pair in counts) != expected_roles
            or counts[1][1] != 2 * counts[0][1]
            or counts[2][1] != counts[0][1]
            or counts[3][1] != counts[0][1]
            or type(pre_digests) is not tuple
            or len(pre_digests) != 5
            or pre_digests[0]
            != "historical_foundry_prefinalization_digest_binding/v1"
            or any(
                type(value) is not str or _HASH64.fullmatch(value) is None
                for value in pre_digests[1:]
            )
            or type(replay_digests) is not tuple
            or len(replay_digests) != 6
            or replay_digests[0]
            != "historical_foundry_reconciliation_digest_binding/v1"
            or type(replay_digests[1]) is not int
            or replay_digests[1] <= 0
            or type(replay_digests[3]) is not int
            or replay_digests[3] != exchange_count
            or any(
                type(replay_digests[index]) is not str
                or _HASH64.fullmatch(replay_digests[index]) is None
                for index in (2, 4, 5)
            )
        ):
            return False
        return True

    def _issue_task4b_capture_replay_event(
        *,
        record: Dict[str, Any],
        source: Any,
        payload: Tuple[Any, ...],
    ) -> "_ProductionHistoricalWindowCaptureReplayEvent":
        event_index = record.get("next_event_index")
        source_ref = record.get("capture_source_ref")
        if (
            record.get("state") != "capture_replay_bound"
            or record.get("event_issuer")
            is not _issue_task4b_capture_replay_event
            or record.get("event_issuer_state") != "ready"
            or type(event_index) is not int
            or event_index < 0
            or not callable(source_ref)
            or source_ref() is not source
            or not _validate_task4b_capture_replay_payload(payload)
            or record.get("live_event") is not None
            or capture_replay_event_registry
        ):
            _reject_task4b_capability()
        event = object.__new__(
            _ProductionHistoricalWindowCaptureReplayEvent
        )
        record["live_event"] = event
        capture_replay_event_registry[id(event)] = (
            event,
            {
                "state": "live",
                "source": source,
                "association": record,
                "event_index": event_index,
                "payload": payload,
            },
        )
        record["event_issuer_state"] = "awaiting_consume"
        return event

    def _install_task4b_capture_replay_association(
        *,
        record: Dict[str, Any],
        reconciliation: Any,
        source: Any,
        view: Any,
    ) -> None:
        if record.get("state") != "consumed_by_mint":
            _reject_task4b_capability()
        if (
            weakref is not task4b_weakref_module
            or getattr(task4b_weakref_module, "ref", None)
            is not task4b_weakref_ref
        ):
            raise _ArchiveRpcError(
                "authority_mismatch", "final_identity_drift"
            )
        association_names = (
            "capture_source_ref",
            "capture_view_ref",
            "capture_reconciliation_ref",
            "event_issuer",
            "event_issuer_state",
            "next_event_index",
            "live_event",
        )
        try:
            record["state"] = "capture_replay_binding"
            record["capture_source_ref"] = task4b_weakref_ref(source)
            record["capture_view_ref"] = task4b_weakref_ref(view)
            record["capture_reconciliation_ref"] = task4b_weakref_ref(
                reconciliation
            )
            record["event_issuer"] = _issue_task4b_capture_replay_event
            record["event_issuer_state"] = "ready"
            record["next_event_index"] = 0
            record["live_event"] = None
            record["state"] = "capture_replay_bound"
        except BaseException:
            for name in association_names:
                record.pop(name, None)
            record["state"] = "consumed_by_mint"
            raise

    def _bind_production_historical_window_capture_replay_source_from_bound_storage(
        *,
        reconciliation: Any,
        source: Any,
    ) -> None:
        transaction_nonce = active_capture_materialization.get()
        transaction_entry = capture_materialization_registry.get(
            id(transaction_nonce)
        )
        if (
            transaction_entry is None
            or transaction_entry[0] is not transaction_nonce
            or type(transaction_entry[1]) is not dict
        ):
            _reject_task4b_capability()
        transaction = transaction_entry[1]
        if (
            transaction.get("provenance")
            is not capture_materialization_registry
            or transaction.get("nonce") is not transaction_nonce
            or transaction.get("state") != "active"
        ):
            _reject_task4b_capability()
        origin_token = transaction.get("token")
        try:
            active_capture_materialization.reset(origin_token)
        except (TypeError, ValueError):
            _reject_task4b_capability()
        transaction["state"] = "rotating"
        transaction["token"] = None
        try:
            rotated_token = active_capture_materialization.set(
                transaction_nonce
            )
        except BaseException:
            transaction["state"] = "failed"
            raise
        transaction["token"] = rotated_token
        transaction["state"] = "binding"
        view = transaction.get("view")
        storage_module = transaction.get("storage_module")
        source_class = transaction.get("source_class")
        try:
            storage_surface = getattr(
                storage_module, "_TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS"
            )
            bind_method = getattr(
                source_class,
                "_bind_reconciliation_from_bound_scan",
            )
        except (AttributeError, TypeError):
            _reject_task4b_capability()
        if (
            sys.modules.get("scripts.historical_foundry_storage")
            is not storage_module
            or type(storage_surface) is not tuple
            or len(storage_surface) != 21
            or storage_surface[0] is not source_class
            or storage_surface[2] is not bind_method
            or type(source) is not source_class
        ):
            _reject_task4b_capability()
        ordinary_bind_failure = False
        try:
            bind_method(
                source,
                expected_view=view,
                expected_reconciliation=reconciliation,
            )
            if (
                getattr(
                    storage_module,
                    "_TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS",
                    None,
                )
                is not storage_surface
                or getattr(
                    source_class,
                    "_bind_reconciliation_from_bound_scan",
                    None,
                )
                is not bind_method
                or getattr(
                    storage_module,
                    "_HistoricalWindowCaptureReplaySource",
                    None,
                )
                is not source_class
            ):
                raise _ArchiveRpcError(
                    "authority_mismatch", "final_identity_drift"
                )
            if type(reconciliation) is not _ProductionHistoricalWindowReconciliation:
                _reject_task4b_capability()
            entry = reconciliation_registry.get(id(reconciliation))
            if entry is None or entry[0]() is not reconciliation:
                _reject_task4b_capability()
            record = entry[1]
            if record.get("state") != "consumed_by_mint":
                _reject_task4b_capability()
            _verify_task4b_scan_association_currentness(
                reconciliation=reconciliation,
                record=record,
                expected_state="consumed_by_mint",
            )
            event_registry = capture_replay_event_registry
            if event_registry:
                _reject_task4b_capability()
            _install_task4b_capture_replay_association(
                record=record,
                reconciliation=reconciliation,
                source=source,
                view=view,
            )
            transaction["state"] = "bound"
            return None
        except _ArchiveRpcError:
            transaction["state"] = "failed"
            raise
        except BaseException as error:
            transaction["state"] = "failed"
            if not isinstance(error, Exception):
                raise
            ordinary_bind_failure = True
            del error
        if ordinary_bind_failure:
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_spool_handoff_failed",
            ) from None

    def _consume_production_historical_window_capture_replay_event_for_storage(
        *,
        event: Any,
        expected_source: Any,
        expected_event_index: int,
    ) -> Tuple[Any, ...]:
        if (
            type(event) is not _ProductionHistoricalWindowCaptureReplayEvent
            or type(expected_event_index) is not int
            or expected_event_index < 0
        ):
            _reject_task4b_capability()
        entry = capture_replay_event_registry.get(id(event))
        if (
            entry is None
            or entry[0] is not event
            or entry[1].get("state") != "live"
            or entry[1].get("source") is not expected_source
            or entry[1].get("event_index") != expected_event_index
        ):
            _reject_task4b_capability()
        record = entry[1]
        payload = record.get("payload")
        association = record.get("association")
        source_reference = (
            association.get("capture_source_ref")
            if type(association) is dict else None
        )
        if (
            type(association) is not dict
            or association.get("state") != "capture_replay_bound"
            or association.get("event_issuer_state") != "awaiting_consume"
            or association.get("next_event_index") != expected_event_index
            or association.get("live_event") is not event
            or not callable(source_reference)
            or source_reference() is not expected_source
        ):
            _reject_task4b_capability()
        if not _validate_task4b_capture_replay_payload(payload):
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            )
        record["state"] = "consumed"
        record["source"] = None
        record["association"] = None
        record["payload"] = None
        capture_replay_event_registry.pop(id(event), None)
        association["next_event_index"] = expected_event_index + 1
        association["event_issuer_state"] = "ready"
        association["live_event"] = None
        return payload

    def _materialize_historical_window_staging_snapshot(
        *,
        capability: "_ProductionHistoricalWindowCapability",
    ) -> "HistoricalRunStagingSnapshot":
        final_identity_error = False
        manifest_control = None
        manifest_control_traceback = None
        try:
            _verify_task4b_semantic_dependency_manifest()
        except BaseException as error:
            if not isinstance(error, Exception):
                manifest_control = error
                manifest_control_traceback = error.__traceback__
            elif (
                type(error) is projection_error_original
                and error.reason_code == "authority_mismatch"
                and error.failure_kind == "final_identity_drift"
            ):
                final_identity_error = True
            else:
                raise
            del error
        import sys as runtime_sys
        storage_identity_error = not task4b_storage_identity_is_current(
            runtime_sys
        )
        if (
            storage_identity_error
            and type(capability) is not task4b_storage_capability_class
        ):
            raise _failure("authority_mismatch", "fixture_input_invalid")
        if storage_identity_error:
            final_identity_error = True
        view = None
        snapshot = None
        body_error = None
        body_traceback = None
        transaction_nonce = None
        transaction = None
        try:
            consume_authority = (
                task4b_storage_consume_runner
                if task4b_storage_runner_graph_is_current()
                else (
                    task4b_storage_consume
                    if task4b_storage_original_graph_is_current()
                    else None
                )
            )
            if consume_authority is None:
                raise archive_error_original(
                    "authority_mismatch", "final_identity_drift"
                ) from None
            view = consume_authority(
                capability=capability
            )
            consume_authority = None
            if type(view) is not task4b_storage_consumed_view_class:
                raise archive_error_original(
                    "authority_mismatch", "final_identity_drift"
                ) from None
            if not task4b_storage_identity_is_current(runtime_sys):
                final_identity_error = True
            if manifest_control is not None:
                raise manifest_control.with_traceback(
                    manifest_control_traceback
                )
            if final_identity_error:
                raise archive_error_original(
                    "authority_mismatch", "final_identity_drift"
                ) from None
            if active_capture_materialization.get() is not None:
                _reject_task4b_capability()
            source_class = task4b_storage_source_class
            transaction_nonce = object()
            transaction = {
                "provenance": capture_materialization_registry,
                "nonce": transaction_nonce,
                "state": "installing",
                "token": None,
                "view": view,
                "storage_module": task4b_storage_module,
                "source_class": source_class,
            }
            capture_materialization_registry[id(transaction_nonce)] = (
                transaction_nonce, transaction
            )
            transaction["token"] = active_capture_materialization.set(
                transaction_nonce
            )
            transaction["state"] = "active"
            try:
                materialize_authority = (
                    task4b_storage_view_materialize_runner
                    if task4b_storage_runner_graph_is_current()
                    else (
                        task4b_storage_view_materialize
                        if task4b_storage_original_graph_is_current()
                        else None
                    )
                )
                if materialize_authority is None:
                    raise archive_error_original(
                        "authority_mismatch", "final_identity_drift"
                    ) from None
                materialized_snapshot = (
                    materialize_authority(view)
                )
                materialize_authority = None
                if type(materialized_snapshot) is not task4b_storage_snapshot_class:
                    raise archive_error_original(
                        "authority_mismatch", "final_identity_drift"
                    ) from None
                snapshot = materialized_snapshot
                projection_authority = (
                    task4b_storage_snapshot_projection_runner
                    if task4b_storage_runner_graph_is_current()
                    else (
                        task4b_storage_snapshot_projection
                        if task4b_storage_original_graph_is_current()
                        else None
                    )
                )
                if projection_authority is None:
                    raise archive_error_original(
                        "authority_mismatch", "final_identity_drift"
                    ) from None
                projection_authority(snapshot)
                projection_authority = None
                if not task4b_storage_identity_is_current(runtime_sys):
                    raise archive_error_original(
                        "authority_mismatch", "final_identity_drift"
                    ) from None
            finally:
                entry = capture_materialization_registry.pop(
                    id(transaction_nonce), None
                )
                transaction["state"] = "scrubbing"
                transaction_token = transaction.get("token")
                transaction["token"] = None
                transaction["view"] = None
                transaction["storage_module"] = None
                transaction["source_class"] = None
                transaction["nonce"] = None
                transaction["provenance"] = None
                if transaction_token is not None:
                    active_capture_materialization.reset(transaction_token)
                transaction["state"] = "closed"
                del entry, transaction_token
                transaction_nonce = None
                transaction = None
            return snapshot
        except BaseException as observed_body_error:
            body_error = observed_body_error
            body_traceback = observed_body_error.__traceback__
            del observed_body_error

        if manifest_control is not None and body_error is not manifest_control:
            body_error = manifest_control
            body_traceback = manifest_control_traceback

        if (
            final_identity_error
            and type(body_error) is _ArchiveRpcError
            and body_error.reason_code == "authority_mismatch"
            and body_error.failure_kind
            == "historical_window_capability_invalid"
        ):
            body_error.__traceback__ = None
            body_error = archive_error_original(
                "authority_mismatch", "final_identity_drift"
            )
            body_traceback = None

        if transaction is not None:
            capture_materialization_registry.pop(
                id(transaction_nonce), None
            )
            transaction["state"] = "scrubbing"
            transaction_token = transaction.get("token")
            transaction["token"] = None
            transaction["view"] = None
            transaction["storage_module"] = None
            transaction["source_class"] = None
            transaction["nonce"] = None
            transaction["provenance"] = None
            if transaction_token is not None:
                active_capture_materialization.reset(transaction_token)
            transaction["state"] = "closed"
        if (
            view is None
            and isinstance(body_error, Exception)
            and type(body_error) is not _ArchiveRpcError
        ):
            raise _ArchiveRpcError(
                "authority_mismatch",
                "historical_window_capability_invalid",
            ) from None
        close_control = None
        close_traceback = None
        if snapshot is not None or view is not None:
            try:
                if snapshot is not None:
                    close_authority = (
                        task4b_storage_snapshot_close_runner
                        if task4b_storage_runner_graph_is_current()
                        else (
                            task4b_storage_snapshot_close
                            if task4b_storage_original_graph_is_current()
                            else None
                        )
                    )
                else:
                    close_authority = (
                        task4b_storage_view_close_runner
                        if task4b_storage_runner_graph_is_current()
                        else (
                            task4b_storage_view_close
                            if task4b_storage_original_graph_is_current()
                            else None
                        )
                    )
                if close_authority is None:
                    raise archive_error_original(
                        "authority_mismatch", "final_identity_drift"
                    ) from None
                close_authority(
                    snapshot if snapshot is not None else view
                )
                close_authority = None
            except BaseException as observed_close_error:
                if not isinstance(observed_close_error, Exception):
                    close_control = observed_close_error
                    close_traceback = observed_close_error.__traceback__
                del observed_close_error
        if isinstance(body_error, Exception) and close_control is not None:
            raise close_control.with_traceback(close_traceback)
        raise body_error.with_traceback(body_traceback)

    for task4b_surface in (
        _materialize_historical_window_staging_snapshot,
        _bind_production_historical_window_capture_replay_source_from_bound_storage,
        _consume_production_historical_window_capture_replay_event_for_storage,
    ):
        task4b_surface.__qualname__ = task4b_surface.__name__
        task4b_surface.__module__ = __name__

    task4b_scan_local_originals = (
        _materialize_historical_window_staging_snapshot,
        _ProductionHistoricalWindowCaptureReplayEvent,
        _bind_production_historical_window_capture_replay_source_from_bound_storage,
        replay_surface,
        _consume_production_historical_window_capture_replay_event_for_storage,
    )

    return (
        _ProductionHistoricalWindowPreFinalization,
        _ProductionHistoricalWindowReconciliation,
        verify_prefinalization,
        reconcile,
        verify_reconciliation,
        capture,
        project_historical_header_inventory_wrapper,
        project_historical_window_projection_wrapper,
        _materialize_historical_window_staging_snapshot,
        _bind_production_historical_window_capture_replay_source_from_bound_storage,
        replay_surface,
        _consume_production_historical_window_capture_replay_event_for_storage,
    )


def _canonical_hash_value(
    value: Any,
    decimal_cache: Optional[Mapping[int, Tuple[Any, ...]]] = None,
) -> Any:
    if type(value) in (dict, MappingProxyType):
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
        del raw
        compact_probes.append(compact)
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
        compact = _project_lower_observation(
            raw, block_number=number, request_id=request_id,
            pair=("block_coverage_incomplete", "lower_bound_witness_invalid"),
        )
        del raw
        compact_witness.append(compact)
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


class _ProductionHistoricalWindowCaptureReplayEvent:
    __slots__ = ()

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        del cls, args, kwargs
        raise _failure("authority_mismatch", "fixture_input_invalid")

    def __init_subclass__(cls, **_kwargs: Any) -> None:
        del cls
        raise TypeError("historical replay event is sealed")

    def __repr__(self) -> str:
        return "_ProductionHistoricalWindowCaptureReplayEvent(<redacted>)"

    def __copy__(self) -> Any:
        raise TypeError("historical replay event is not copyable")

    def __deepcopy__(self, _memo: Any) -> Any:
        raise TypeError("historical replay event is not copyable")

    def __reduce__(self) -> Any:
        raise TypeError("historical replay event is not serializable")

    def __reduce_ex__(self, _protocol: int) -> Any:
        raise TypeError("historical replay event is not serializable")


(
    _ProductionHistoricalWindowPreFinalization,
    _ProductionHistoricalWindowReconciliation,
    _verify_production_historical_window_prefinalization,
    _reconcile_production_historical_window,
    _verify_production_historical_window_reconciliation,
    _capture_production_historical_window,
    project_historical_header_inventory,
    project_historical_window_projection,
    _materialize_historical_window_staging_snapshot,
    _bind_production_historical_window_capture_replay_source_from_bound_storage,
    _replay_production_historical_window_capture_from_bound_storage,
    _consume_production_historical_window_capture_replay_event_for_storage,
) = _initialize_production_historical_window_authorities()
del _initialize_production_historical_window_authorities


_TASK4B_SCAN_LOCAL_SURFACE_NAMES = (
    "_materialize_historical_window_staging_snapshot",
    "_ProductionHistoricalWindowCaptureReplayEvent",
    "_bind_production_historical_window_capture_replay_source_from_bound_storage",
    "_replay_production_historical_window_capture_from_bound_storage",
    "_consume_production_historical_window_capture_replay_event_for_storage",
)
_TASK4B_SCAN_LOCAL_SURFACE_OBJECTS = (
    _materialize_historical_window_staging_snapshot,
    _ProductionHistoricalWindowCaptureReplayEvent,
    _bind_production_historical_window_capture_replay_source_from_bound_storage,
    _replay_production_historical_window_capture_from_bound_storage,
    _consume_production_historical_window_capture_replay_event_for_storage,
)


_PREFILTER_GRID_DOMAIN = b"historical_foundry_prefilter_grid/v1"
_PREFILTER_COVERAGE_DOMAIN = b"historical_foundry_window_coverage/v1"
_PREFILTER_DIRECTIONS = (
    "uniswap_to_sushiswap", "sushiswap_to_uniswap",
)
_PREFILTER_VENUES = ("uniswap_v2", "sushiswap_v2")


def _detach_prefilter_value(value: Any) -> Any:
    if type(value) in (dict, MappingProxyType):
        return {
            key: _detach_prefilter_value(nested)
            for key, nested in value.items()
        }
    if type(value) in (list, tuple):
        return [_detach_prefilter_value(nested) for nested in value]
    return value


def _freeze_prefilter_value(value: Any) -> Any:
    if type(value) is dict:
        return MappingProxyType({
            key: _freeze_prefilter_value(nested)
            for key, nested in value.items()
        })
    if type(value) in (list, tuple):
        return tuple(_freeze_prefilter_value(nested) for nested in value)
    return value


def _prefilter_grid_digest(rows: Sequence[Mapping[str, Any]]) -> str:
    digest = _inventory_hasher(_PREFILTER_GRID_DOMAIN)
    for row in rows:
        _inventory_update(digest, row)
    return digest.hexdigest()


def _exact_fraction_projection(display: str) -> Mapping[str, Any]:
    if type(display) is not str or not display:
        raise ValueError("historical prefilter fraction is invalid")
    try:
        exact = Fraction(display)
    except (ValueError, ZeroDivisionError):
        raise ValueError("historical prefilter fraction is invalid") from None
    return MappingProxyType({
        "numerator": exact.numerator,
        "denominator": exact.denominator,
        "display": display,
    })


def _initialize_historical_prefilter_capabilities():
    issuer = object()
    window_registry: Dict[int, Tuple[Any, Dict[str, Any]]] = {}
    grid_registry: Dict[int, Tuple[Any, Dict[str, Any]]] = {}
    scenario_registry: Dict[int, Tuple[Any, Dict[str, Any]]] = {}

    def register_capability(
        value: Any,
        record: Dict[str, Any],
        registry: Dict[int, Tuple[Any, Dict[str, Any]]],
    ) -> None:
        key = id(value)

        def retire(reference: Any) -> None:
            entry = registry.get(key)
            if entry is not None and entry[0] is reference:
                registry.pop(key, None)

        reference = weakref.ref(value, retire)
        registry[key] = (reference, record)

    def capability_record(
        value: Any,
        expected_type: type,
        registry: Dict[int, Tuple[Any, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        entry = registry.get(id(value))
        if (
            type(value) is not expected_type
            or entry is None
            or entry[0]() is not value
            or entry[1].get("issuer") is not issuer
        ):
            raise ValueError("historical prefilter capability is invalid")
        return entry[1]

    class ValidatedHistoricalWindow:
        """Opaque descriptor-held exact historical coverage capability."""

        __slots__ = ("__weakref__",)

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise ValueError("historical window capability is private")

        def __init_subclass__(cls, **_kwargs: Any) -> None:
            del cls
            raise TypeError("ValidatedHistoricalWindow is sealed")

        @property
        def scan_inventory_sha256(self) -> str:
            return capability_record(
                self, ValidatedHistoricalWindow, window_registry
            )["scan_inventory_sha256"]

        @property
        def lower_bound_number(self) -> int:
            return capability_record(
                self, ValidatedHistoricalWindow, window_registry
            )["lower_bound_number"]

        @property
        def anchor_number(self) -> int:
            return capability_record(
                self, ValidatedHistoricalWindow, window_registry
            )["anchor_number"]

        @property
        def block_count(self) -> int:
            return capability_record(
                self, ValidatedHistoricalWindow, window_registry
            )["block_count"]

        @property
        def coverage_digest(self) -> str:
            return capability_record(
                self, ValidatedHistoricalWindow, window_registry
            )["coverage_digest"]

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("ValidatedHistoricalWindow is immutable")

        def __repr__(self) -> str:
            return "ValidatedHistoricalWindow(<redacted>)"

        def __copy__(self) -> Any:
            raise TypeError("ValidatedHistoricalWindow is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("ValidatedHistoricalWindow is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("ValidatedHistoricalWindow is not serializable")

        def __reduce_ex__(self, _protocol: int) -> Any:
            raise TypeError("ValidatedHistoricalWindow is not serializable")

    class ValidatedHistoricalPrefilterGrid:
        """Opaque descriptor-held recomputed prefilter-grid capability."""

        __slots__ = ("__weakref__",)

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise ValueError("historical prefilter grid capability is private")

        def __init_subclass__(cls, **_kwargs: Any) -> None:
            del cls
            raise TypeError("ValidatedHistoricalPrefilterGrid is sealed")

        @property
        def scan_inventory_sha256(self) -> str:
            return capability_record(
                self, ValidatedHistoricalPrefilterGrid, grid_registry
            )["scan_inventory_sha256"]

        @property
        def row_count(self) -> int:
            return capability_record(
                self, ValidatedHistoricalPrefilterGrid, grid_registry
            )["row_count"]

        @property
        def safe_excluded_count(self) -> int:
            return capability_record(
                self, ValidatedHistoricalPrefilterGrid, grid_registry
            )["safe_excluded_count"]

        @property
        def replay_required_count(self) -> int:
            return capability_record(
                self, ValidatedHistoricalPrefilterGrid, grid_registry
            )["replay_required_count"]

        @property
        def grid_digest(self) -> str:
            return capability_record(
                self, ValidatedHistoricalPrefilterGrid, grid_registry
            )["grid_digest"]

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError(
                "ValidatedHistoricalPrefilterGrid is immutable"
            )

        def __repr__(self) -> str:
            return "ValidatedHistoricalPrefilterGrid(<redacted>)"

        def __copy__(self) -> Any:
            raise TypeError(
                "ValidatedHistoricalPrefilterGrid is not copyable"
            )

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError(
                "ValidatedHistoricalPrefilterGrid is not copyable"
            )

        def __reduce__(self) -> Any:
            raise TypeError(
                "ValidatedHistoricalPrefilterGrid is not serializable"
            )

        def __reduce_ex__(self, _protocol: int) -> Any:
            raise TypeError(
                "ValidatedHistoricalPrefilterGrid is not serializable"
            )

    class ValidatedReplayScenario:
        """Opaque exact-lineage scenario issued from one validated grid row."""

        __slots__ = ("__weakref__",)

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise ValueError("historical replay scenario is private")

        def __init_subclass__(cls, **_kwargs: Any) -> None:
            del cls
            raise TypeError("ValidatedReplayScenario is sealed")

        @property
        def scenario_key(self) -> str:
            return capability_record(
                self, ValidatedReplayScenario, scenario_registry
            )["row"]["scenario_key"]

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("ValidatedReplayScenario is immutable")

        def __repr__(self) -> str:
            return "ValidatedReplayScenario(<redacted>)"

        def __copy__(self) -> Any:
            raise TypeError("ValidatedReplayScenario is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("ValidatedReplayScenario is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("ValidatedReplayScenario is not serializable")

        def __reduce_ex__(self, _protocol: int) -> Any:
            raise TypeError("ValidatedReplayScenario is not serializable")

    def require_config(config: Any) -> HistoricalFoundryConfigSet:
        if type(config) is not HistoricalFoundryConfigSet:
            raise ValueError("historical config capability is invalid")
        if (
            tuple(config.policy.value["directions"]) != _PREFILTER_DIRECTIONS
            or tuple(int(value) for value in config.policy.value[
                "requested_notionals_usd"
            ]) != (1000, 5000, 10000, 50000, 100000)
            or tuple(row["venue_id"] for row in config.authority.value[
                "venues"
            ]) != _PREFILTER_VENUES
        ):
            raise ValueError("historical prefilter config differs")
        return config

    def read_typed_rows(
        staging: Any,
        inventory: Mapping[str, Any],
    ) -> Dict[str, List[Dict[str, Any]]]:
        typed_chunks = inventory.get("typed_chunks")
        if type(typed_chunks) is not list or not typed_chunks:
            raise ValueError("historical capture inventory is invalid")
        rows_by_role = {role: [] for role in (
            "headers", "reserves", "prices", "fees",
        )}
        next_index = {role: 1 for role in rows_by_role}
        for descriptor in typed_chunks:
            role = descriptor.get("role") if type(descriptor) is dict else None
            if (
                role not in rows_by_role
                or descriptor.get("chunk_index") != next_index[role]
                or descriptor.get("path")
                != role + "/{:08d}.json.gz".format(next_index[role])
                or type(descriptor.get("gzip_sha256")) is not str
                or _HASH64.fullmatch(descriptor["gzip_sha256"]) is None
                or type(descriptor.get("gzip_byte_count")) is not int
                or type(descriptor.get("decoded_byte_count")) is not int
                or type(descriptor.get("decoded_sha256")) is not str
                or type(descriptor.get("row_count")) is not int
            ):
                raise ValueError("historical typed descriptor is invalid")
            physical = staging.read_frozen_member(
                descriptor["path"],
                expected_sha256=descriptor["gzip_sha256"],
                max_bytes=16_842_752,
            )
            try:
                decoded = gzip.decompress(physical)
                chunk_rows = json.loads(decoded.decode("utf-8"))
            except Exception:
                raise ValueError("historical typed member is invalid") from None
            if (
                type(chunk_rows) is not list
                or len(chunk_rows) != descriptor["row_count"]
                or len(decoded) != descriptor["decoded_byte_count"]
                or hashlib.sha256(decoded).hexdigest()
                != descriptor["decoded_sha256"]
                or _canonical_json_bytes(chunk_rows) != decoded
            ):
                raise ValueError("historical typed member differs")
            rows_by_role[role].extend(chunk_rows)
            next_index[role] += 1
        return rows_by_role

    def validate_coverage(
        *,
        config: HistoricalFoundryConfigSet,
        staging: Any,
    ) -> Dict[str, Any]:
        import scripts.historical_foundry_storage as storage

        if type(staging) is not storage.HistoricalRunStagingSnapshot:
            raise ValueError("historical staging capability is invalid")
        staging.reread_frozen_members_unchanged()
        identity = staging.frozen_identity_projection()
        if (
            type(identity) is not dict
            or identity.get("schema")
            != "historical_foundry_staging_snapshot_identity/v1"
            or identity.get("stage") not in (
                "capture_frozen", "prefilter_frozen",
            )
            or identity.get("generation") not in (1, 2)
            or type(identity.get("capture_inventory_sha256")) is not str
            or _HASH64.fullmatch(identity["capture_inventory_sha256"]) is None
        ):
            raise ValueError("historical staging identity is invalid")
        capture_bytes = staging.read_frozen_member(
            "scan/capture_inventory.json",
            expected_sha256=identity["capture_inventory_sha256"],
            max_bytes=16_777_216,
        )
        try:
            inventory = json.loads(capture_bytes.decode("utf-8"))
        except Exception:
            raise ValueError("historical capture inventory is invalid") from None
        if (
            type(inventory) is not dict
            or inventory.get("schema")
            != "historical_foundry_capture_inventory/v1"
            or _canonical_json_bytes(inventory) != capture_bytes
        ):
            raise ValueError("historical capture inventory differs")
        expected_configs = (
            ("policy", config.policy),
            ("authority", config.authority),
            ("toolchain", config.toolchain),
        )
        config_rows = inventory.get("configs")
        if (
            type(config_rows) is not list
            or len(config_rows) != 3
            or any(
                config_rows[index].get("role") != role
                or config_rows[index].get("sha256")
                != loaded.physical_sha256
                or staging.read_frozen_member(
                    config_rows[index]["path"],
                    expected_sha256=loaded.physical_sha256,
                    max_bytes=1_048_576,
                ) != loaded.physical_bytes
                for index, (role, loaded) in enumerate(expected_configs)
            )
        ):
            raise ValueError("historical capture config binding differs")
        range_row = inventory.get("range")
        if type(range_row) is not dict or tuple(range_row) != (
            "anchor_number", "block_count", "cutoff_timestamp",
            "lower_bound_number",
        ):
            raise ValueError("historical capture range is invalid")
        lower = range_row.get("lower_bound_number")
        anchor = range_row.get("anchor_number")
        block_count = range_row.get("block_count")
        if (
            type(lower) is not int
            or type(anchor) is not int
            or type(block_count) is not int
            or lower < 0
            or anchor < lower
            or block_count != anchor - lower + 1
        ):
            raise ValueError("historical capture denominator differs")
        rows = read_typed_rows(staging, inventory)
        headers = rows["headers"]
        reserves = rows["reserves"]
        prices = rows["prices"]
        fees = rows["fees"]
        expected_blocks = list(range(lower, anchor + 1))
        if (
            [row.get("number") for row in headers] != expected_blocks
            or [row.get("block_number") for row in prices] != expected_blocks
            or [row.get("block_number") for row in fees] != expected_blocks
            or [row.get("block_number") for row in reserves]
            != [number for number in expected_blocks for _venue in range(2)]
        ):
            raise ValueError("historical capture coverage has gaps")
        records = []
        prior_header = None
        authority = config.authority.value
        price_feed = authority["price_feed"]
        for index, block_number in enumerate(expected_blocks):
            header = headers[index]
            block_reserves = reserves[index * 2:index * 2 + 2]
            price = prices[index]
            fee = fees[index]
            if (
                tuple(row.get("venue_id") for row in block_reserves)
                != _PREFILTER_VENUES
                or any(row.get("block_hash") != header.get("hash")
                       for row in block_reserves)
                or price.get("block_hash") != header.get("hash")
                or price.get("proxy_address")
                != price_feed["proxy_address"]
                or price.get("answer", 0) <= 0
                or price.get("updated_at", -1) > header.get("timestamp", -1)
                or header.get("timestamp", -1) - price.get("updated_at", -1)
                > config.policy.value["max_eth_usd_age_seconds"]
                or fee.get("base_fee_per_gas")
                != header.get("base_fee_per_gas")
                or fee.get("next_base_fee_per_gas")
                != next_historical_base_fee(
                    parent_base_fee=header.get("base_fee_per_gas"),
                    parent_gas_used=header.get("gas_used"),
                    parent_gas_limit=header.get("gas_limit"),
                )
                or (
                    prior_header is not None
                    and (
                        header.get("number") != prior_header.get("number") + 1
                        or header.get("parent_hash") != prior_header.get("hash")
                    )
                )
            ):
                raise ValueError("historical capture record binding differs")
            records.append(_freeze_prefilter_value({
                "header": header,
                "reserves": block_reserves,
                "price": price,
                "fee": fee,
            }))
            prior_header = header
        coverage_value = {
            "capture_inventory_sha256": identity[
                "capture_inventory_sha256"
            ],
            "range": range_row,
            "headers": headers,
            "reserves": reserves,
            "prices": prices,
            "fees": fees,
        }
        return {
            "snapshot": staging,
            "scan_inventory_sha256": identity.get(
                "scan_inventory_sha256",
                identity["capture_inventory_sha256"],
            ),
            "capture_inventory_sha256": identity[
                "capture_inventory_sha256"
            ],
            "lower_bound_number": lower,
            "anchor_number": anchor,
            "block_count": block_count,
            "coverage_digest": _typed_hash(
                _PREFILTER_COVERAGE_DOMAIN, coverage_value
            ),
            "records": tuple(records),
        }

    def issue_window(record: Dict[str, Any]) -> ValidatedHistoricalWindow:
        window = object.__new__(ValidatedHistoricalWindow)
        record["issuer"] = issuer
        register_capability(window, record, window_registry)
        return window

    def open_validated_historical_window(
        *,
        config: HistoricalFoundryConfigSet,
        staging: Any,
    ) -> ValidatedHistoricalWindow:
        import scripts.historical_foundry_storage as storage

        record = validate_coverage(
            config=require_config(config), staging=staging
        )
        record["lineage_token"] = (
            storage._bind_historical_prefilter_staging_transition(
                staging=staging
            )
        )
        return issue_window(record)

    def _iter_validated_historical_window_records(
        *, window: ValidatedHistoricalWindow
    ):
        record = capability_record(
            window, ValidatedHistoricalWindow, window_registry
        )
        record["snapshot"].reread_frozen_members_unchanged()
        records = record["records"]
        if type(records) is not tuple or len(records) != record["block_count"]:
            raise ValueError("historical window capability differs")
        return iter(records)

    def build_historical_prefilter_grid(
        *,
        config: HistoricalFoundryConfigSet,
        window: ValidatedHistoricalWindow,
    ) -> Tuple[Mapping[str, Any], ...]:
        checked_config = require_config(config)
        record = capability_record(
            window, ValidatedHistoricalWindow, window_registry
        )
        record["snapshot"].reread_frozen_members_unchanged()
        policy = checked_config.policy.value
        authority = checked_config.authority.value
        feed_decimals = authority["price_feed"]["decimals"]
        notionals = tuple(int(value) for value in policy[
            "requested_notionals_usd"
        ])
        window_projection = {
            "lower_bound_number": record["lower_bound_number"],
            "anchor_number": record["anchor_number"],
            "block_count": record["block_count"],
            "scenario_denominator": record["block_count"] * 10,
        }
        result = []
        records = tuple(_iter_validated_historical_window_records(
            window=window
        ))
        for captured in reversed(records):
            header = captured["header"]
            reserves_by_venue = {
                row["venue_id"]: row for row in captured["reserves"]
            }
            price = captured["price"]
            fee = captured["fee"]
            reserve_projection = {
                venue: {
                    "pair_address": reserves_by_venue[venue]["pair_address"],
                    "reserve_uni_raw": reserves_by_venue[venue]["reserve0"],
                    "reserve_weth_raw": reserves_by_venue[venue]["reserve1"],
                    "pair_timestamp": reserves_by_venue[venue]["pair_timestamp"],
                }
                for venue in _PREFILTER_VENUES
            }
            for direction in _PREFILTER_DIRECTIONS:
                first_venue, second_venue = (
                    _PREFILTER_VENUES
                    if direction == "uniswap_to_sushiswap"
                    else tuple(reversed(_PREFILTER_VENUES))
                )
                first_reserves = reserves_by_venue[first_venue]
                second_reserves = reserves_by_venue[second_venue]
                for notional in notionals:
                    projected = project_historical_prefilter_math(
                        requested_notional_usd=notional,
                        direction=direction,
                        first_reserves=(
                            first_reserves["reserve0"],
                            first_reserves["reserve1"],
                        ),
                        second_reserves=(
                            second_reserves["reserve0"],
                            second_reserves["reserve1"],
                        ),
                        eth_usd_answer=price["answer"],
                        feed_decimals=feed_decimals,
                        parent_base_fee=header["base_fee_per_gas"],
                        parent_gas_used=header["gas_used"],
                        parent_gas_limit=header["gas_limit"],
                        acceptance_mev_bps=policy["fees"][
                            "acceptance_mev_bps"
                        ],
                    )
                    if projected["child_base_fee_wei"] != fee[
                        "next_base_fee_per_gas"
                    ]:
                        raise ValueError(
                            "historical fee projection differs"
                        )
                    row = {
                        "schema": "historical_foundry_prefilter_row/v1",
                        "scenario_key": "{}:{}:{}".format(
                            header["number"], direction, notional
                        ),
                        "route_key": first_venue + ":" + second_venue,
                        "coverage_digest": record["coverage_digest"],
                        "window": window_projection,
                        "block_number": header["number"],
                        "block_hash": header["hash"],
                        "direction": direction,
                        "requested_notional_usd": notional,
                        "header": {
                            key: header[key] for key in (
                                "number", "hash", "parent_hash", "state_root",
                                "timestamp", "gas_limit", "gas_used",
                                "base_fee_per_gas",
                            )
                        },
                        "reserves": reserve_projection,
                        "price": {
                            **{
                                key: price[key] for key in (
                                    "proxy_address", "round_id", "phase_id",
                                    "answer", "started_at", "updated_at",
                                    "answered_in_round", "valid_until",
                                )
                            },
                            "feed_decimals": feed_decimals,
                        },
                        "fee": {
                            key: fee[key] for key in (
                                "base_fee_per_gas", "next_base_fee_per_gas",
                                "p50_priority_fee_per_gas",
                                "p90_priority_fee_per_gas",
                            )
                        },
                        "amount_weth_in_wei": projected["amount_weth_in_wei"],
                        "first_amount_out_raw": projected["first_amount_out_raw"],
                        "second_amount_out_raw": projected["second_amount_out_raw"],
                        "gross_profit_weth_wei": projected[
                            "gross_profit_weth_wei"
                        ],
                        "gross_edge_usd": _exact_fraction_projection(
                            projected["gross_edge_usd"]
                        ),
                        "child_base_fee_wei": projected["child_base_fee_wei"],
                        "prefilter_gas_cost_usd": _exact_fraction_projection(
                            projected["prefilter_gas_cost_usd"]
                        ),
                        "prefilter_mev_buffer_usd": _exact_fraction_projection(
                            projected["prefilter_mev_buffer_usd"]
                        ),
                        "prefilter_policy_net_upper_bound_usd": (
                            _exact_fraction_projection(projected[
                                "prefilter_policy_net_upper_bound_usd"
                            ])
                        ),
                        "decision": projected["decision"],
                        "reason": projected["reason"],
                    }
                    result.append(_freeze_prefilter_value(row))
        if len(result) != record["block_count"] * 10:
            raise ValueError("historical prefilter denominator differs")
        return tuple(result)

    def validate_grid_core(
        *, checked_config: HistoricalFoundryConfigSet,
        window: ValidatedHistoricalWindow, staging: Any,
        original: Dict[str, Any], verify_lineage: bool,
    ) -> ValidatedHistoricalPrefilterGrid:
        if verify_lineage:
            import scripts.historical_foundry_storage as storage

            try:
                storage._verify_historical_prefilter_staging_transition(
                    lineage_token=original["lineage_token"],
                    staging=staging,
                )
            except storage.HistoricalFoundryStorageError:
                raise ValueError(
                    "historical staging lineage differs"
                ) from None
        fresh = validate_coverage(config=checked_config, staging=staging)
        for key in (
            "capture_inventory_sha256", "lower_bound_number",
            "anchor_number", "block_count", "coverage_digest",
        ):
            if original[key] != fresh[key]:
                raise ValueError("historical window capability differs")
        identity = staging.frozen_identity_projection()
        if (
            identity.get("stage") != "prefilter_frozen"
            or identity.get("generation") != 2
            or type(identity.get("scan_inventory_sha256")) is not str
            or _HASH64.fullmatch(identity["scan_inventory_sha256"]) is None
        ):
            raise ValueError("historical prefilter staging is invalid")
        inventory_bytes = staging.read_frozen_member(
            "scan/prefilter_inventory.json",
            expected_sha256=identity["scan_inventory_sha256"],
            max_bytes=16_777_216,
        )
        try:
            inventory = json.loads(inventory_bytes.decode("utf-8"))
        except Exception:
            raise ValueError("historical scan inventory is invalid") from None
        expected_window = {
            "lower_bound_number": fresh["lower_bound_number"],
            "anchor_number": fresh["anchor_number"],
            "block_count": fresh["block_count"],
            "scenario_denominator": fresh["block_count"] * 10,
        }
        denominator = fresh["block_count"] * 10
        chunks = inventory.get("prefilter_chunks")
        if (
            type(inventory) is not dict
            or _canonical_json_bytes(inventory) != inventory_bytes
            or inventory.get("schema")
            != "historical_foundry_scan_inventory/v1"
            or inventory.get("capture_inventory_sha256")
            != fresh["capture_inventory_sha256"]
            or inventory.get("range") != expected_window
            or inventory.get("scenario_denominator") != denominator
            or inventory.get("row_count") != denominator
            or type(chunks) is not list
            or not chunks
        ):
            raise ValueError("historical scan inventory differs")
        stored_rows = []
        for expected_index, descriptor in enumerate(chunks, 1):
            if (
                type(descriptor) is not dict
                or descriptor.get("chunk_index") != expected_index
                or descriptor.get("path")
                != "scan/prefilter/{:08d}.json.gz".format(expected_index)
            ):
                raise ValueError("historical prefilter chunk differs")
            physical = staging.read_frozen_member(
                descriptor["path"],
                expected_sha256=descriptor["gzip_sha256"],
                max_bytes=16_842_752,
            )
            try:
                decoded = gzip.decompress(physical)
                chunk_rows = json.loads(decoded.decode("utf-8"))
            except Exception:
                raise ValueError("historical prefilter chunk is invalid") from None
            if (
                type(chunk_rows) is not list
                or len(chunk_rows) != descriptor.get("row_count")
                or len(decoded) != descriptor.get("decoded_byte_count")
                or hashlib.sha256(decoded).hexdigest()
                != descriptor.get("decoded_sha256")
                or len(physical) != descriptor.get("gzip_byte_count")
                or hashlib.sha256(physical).hexdigest()
                != descriptor.get("gzip_sha256")
                or _canonical_json_bytes(chunk_rows) != decoded
            ):
                raise ValueError("historical prefilter chunk differs")
            stored_rows.extend(chunk_rows)
        fresh_window = issue_window(fresh)
        expected_rows = build_historical_prefilter_grid(
            config=checked_config, window=fresh_window
        )
        detached_expected = tuple(
            _detach_prefilter_value(row) for row in expected_rows
        )
        if (
            stored_rows != list(detached_expected)
            or len(stored_rows) != denominator
            or inventory.get("grid_digest")
            != _prefilter_grid_digest(stored_rows)
            or inventory.get("grid_digest")
            != _prefilter_grid_digest(expected_rows)
        ):
            raise ValueError("historical prefilter recomputation differs")
        safe_count = sum(
            row["decision"] == "safe_excluded" for row in stored_rows
        )
        replay_count = sum(
            row["decision"] == "replay_required" for row in stored_rows
        )
        if (
            safe_count + replay_count != denominator
            or inventory.get("safe_excluded_count") != safe_count
            or inventory.get("replay_required_count") != replay_count
            or identity.get("prefilter_row_count") != denominator
            or identity.get("prefilter_grid_digest")
            != inventory["grid_digest"]
        ):
            raise ValueError("historical prefilter decision counts differ")
        original.update({
            "snapshot": staging,
            "scan_inventory_sha256": identity["scan_inventory_sha256"],
            "records": fresh["records"],
        })
        grid = object.__new__(ValidatedHistoricalPrefilterGrid)
        grid_record = {
            "issuer": issuer,
            "snapshot": staging,
            "window": window,
            "rows": expected_rows,
            "scan_inventory_sha256": identity["scan_inventory_sha256"],
            "row_count": denominator,
            "safe_excluded_count": safe_count,
            "replay_required_count": replay_count,
            "grid_digest": inventory["grid_digest"],
        }
        register_capability(grid, grid_record, grid_registry)
        return grid

    def validate_historical_prefilter_grid(
        *,
        config: HistoricalFoundryConfigSet,
        window: ValidatedHistoricalWindow,
        staging: Any,
    ) -> ValidatedHistoricalPrefilterGrid:
        checked_config = require_config(config)
        original = capability_record(
            window, ValidatedHistoricalWindow, window_registry
        )
        return validate_grid_core(
            checked_config=checked_config, window=window, staging=staging,
            original=original, verify_lineage=True,
        )

    def _open_validated_historical_scan_authorities(
        *, config: HistoricalFoundryConfigSet, staging: Any,
    ) -> tuple:
        checked_config = require_config(config)
        fresh = validate_coverage(config=checked_config, staging=staging)
        window = issue_window(fresh)
        grid = validate_grid_core(
            checked_config=checked_config, window=window, staging=staging,
            original=fresh, verify_lineage=False,
        )
        return window, grid

    def _iter_validated_historical_prefilter_rows(
        *, grid: ValidatedHistoricalPrefilterGrid
    ):
        record = capability_record(
            grid, ValidatedHistoricalPrefilterGrid, grid_registry
        )
        record["snapshot"].reread_frozen_members_unchanged()
        rows = record["rows"]
        if (
            type(rows) is not tuple
            or len(rows) != record["row_count"]
            or _prefilter_grid_digest(rows) != record["grid_digest"]
        ):
            raise ValueError("historical prefilter capability differs")
        return iter(rows)

    def _issue_validated_replay_scenario(
        *,
        staging: Any,
        window: ValidatedHistoricalWindow,
        grid: ValidatedHistoricalPrefilterGrid,
        scenario_key: str,
    ) -> ValidatedReplayScenario:
        window_record = capability_record(
            window, ValidatedHistoricalWindow, window_registry
        )
        grid_record = capability_record(
            grid, ValidatedHistoricalPrefilterGrid, grid_registry
        )
        if (
            type(scenario_key) is not str
            or not scenario_key
            or grid_record.get("window") is not window
            or grid_record.get("snapshot") is not staging
            or window_record.get("snapshot") is not staging
            or grid_record.get("scan_inventory_sha256")
            != window_record.get("scan_inventory_sha256")
        ):
            raise ValueError("historical replay scenario lineage differs")
        staging.reread_frozen_members_unchanged()
        matched = tuple(
            row for row in grid_record["rows"]
            if row.get("scenario_key") == scenario_key
        )
        if len(matched) != 1:
            raise ValueError("historical replay scenario key is invalid")
        row = _freeze_prefilter_value(
            _detach_prefilter_value(matched[0])
        )
        import scripts.historical_foundry_storage as storage

        storage_token = storage._bind_historical_replay_scenario_transition(
            staging=staging, scenario_key=scenario_key
        )
        scenario = object.__new__(ValidatedReplayScenario)
        register_capability(scenario, {
            "issuer": issuer,
            "snapshot": staging,
            "window": window,
            "grid": grid,
            "row": row,
            "scan_inventory_sha256": grid_record["scan_inventory_sha256"],
            "grid_digest": grid_record["grid_digest"],
            "storage_token": storage_token,
        }, scenario_registry)
        return scenario

    def _validated_replay_scenario_projection(
        *, scenario: ValidatedReplayScenario
    ) -> Mapping[str, Any]:
        record = capability_record(
            scenario, ValidatedReplayScenario, scenario_registry
        )
        record["snapshot"].reread_frozen_members_unchanged()
        return _detach_prefilter_value(record["row"])

    def _consume_replay_scenario_storage_token(
        *, scenario: ValidatedReplayScenario
    ) -> object:
        record = capability_record(
            scenario, ValidatedReplayScenario, scenario_registry
        )
        token = record.get("storage_token")
        if token is None or record.get("storage_token_consumed") is True:
            raise ValueError("historical replay scenario storage authority is invalid")
        record["storage_token_consumed"] = True
        return token

    def _validate_replay_scenario_for_context(
        *,
        scenario: ValidatedReplayScenario,
        staging: Any,
        window: ValidatedHistoricalWindow,
        grid: ValidatedHistoricalPrefilterGrid,
    ) -> Mapping[str, Any]:
        record = capability_record(
            scenario, ValidatedReplayScenario, scenario_registry
        )
        if (
            record.get("snapshot") is not staging
            or record.get("window") is not window
            or record.get("grid") is not grid
        ):
            raise ValueError("historical replay scenario lineage differs")
        staging.reread_frozen_members_unchanged()
        return _detach_prefilter_value(record["row"])

    def _advance_validated_replay_authorities(
        *, ledger: Any, window: ValidatedHistoricalWindow,
        grid: ValidatedHistoricalPrefilterGrid,
    ) -> tuple:
        window_record = capability_record(
            window, ValidatedHistoricalWindow, window_registry
        )
        grid_record = capability_record(
            grid, ValidatedHistoricalPrefilterGrid, grid_registry
        )
        previous = window_record.get("snapshot")
        if (
            grid_record.get("snapshot") is not previous
            or grid_record.get("window") is not window
        ):
            raise ValueError("historical replay successor lineage differs")
        import scripts.historical_foundry_storage as storage

        successor = storage._consume_historical_replay_successor(
            ledger=ledger, previous_staging=previous
        )
        next_window = object.__new__(ValidatedHistoricalWindow)
        next_window_record = dict(window_record)
        next_window_record["snapshot"] = successor
        register_capability(next_window, next_window_record, window_registry)
        next_grid = object.__new__(ValidatedHistoricalPrefilterGrid)
        next_grid_record = dict(grid_record)
        next_grid_record["snapshot"] = successor
        next_grid_record["window"] = next_window
        register_capability(next_grid, next_grid_record, grid_registry)
        return successor, next_window, next_grid

    return (
        ValidatedHistoricalWindow,
        ValidatedHistoricalPrefilterGrid,
        ValidatedReplayScenario,
        open_validated_historical_window,
        _iter_validated_historical_window_records,
        build_historical_prefilter_grid,
        validate_historical_prefilter_grid,
        _iter_validated_historical_prefilter_rows,
        _issue_validated_replay_scenario,
        _validated_replay_scenario_projection,
        _consume_replay_scenario_storage_token,
        _validate_replay_scenario_for_context,
        _advance_validated_replay_authorities,
        _open_validated_historical_scan_authorities,
    )


(
    ValidatedHistoricalWindow,
    ValidatedHistoricalPrefilterGrid,
    ValidatedReplayScenario,
    open_validated_historical_window,
    _iter_validated_historical_window_records,
    build_historical_prefilter_grid,
    validate_historical_prefilter_grid,
    _iter_validated_historical_prefilter_rows,
    _issue_validated_replay_scenario,
    _validated_replay_scenario_projection,
    _consume_replay_scenario_storage_token,
    _validate_replay_scenario_for_context,
    _advance_validated_replay_authorities,
    _open_validated_historical_scan_authorities,
) = _initialize_historical_prefilter_capabilities()
del _initialize_historical_prefilter_capabilities


def _initialize_historical_selection_capabilities():
    issuer = object()
    snapshot_registry: Dict[
        int, Tuple[weakref.ReferenceType, Dict[str, Any]]
    ] = {}
    action_registry: Dict[
        int, Tuple[weakref.ReferenceType, Dict[str, Any]]
    ] = {}
    selection_registry: Dict[
        int, Tuple[weakref.ReferenceType, Dict[str, Any]]
    ] = {}

    def snapshot_record(value: Any) -> Dict[str, Any]:
        entry = snapshot_registry.get(id(value))
        if (
            type(value) is not ValidatedHistoricalScanSnapshot
            or entry is None
            or entry[0]() is not value
            or entry[1].get("issuer") is not issuer
        ):
            raise ValueError("historical scan snapshot is invalid")
        return entry[1]

    class ValidatedHistoricalScanSnapshot:
        """Opaque capability over a descriptor-reread frozen Task-5 grid."""

        __slots__ = ("__weakref__",)

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise ValueError("historical scan snapshot is private")

        def __init_subclass__(cls, **_kwargs: Any) -> None:
            del cls
            raise TypeError("ValidatedHistoricalScanSnapshot is sealed")

        @property
        def staging_inventory_sha256(self) -> str:
            return snapshot_record(self)["staging_inventory_sha256"]

        @property
        def validated_window(self) -> ValidatedHistoricalWindow:
            return snapshot_record(self)["window"]

        @property
        def validated_grid(self) -> ValidatedHistoricalPrefilterGrid:
            return snapshot_record(self)["grid"]

        @property
        def candidate_block_count(self) -> int:
            return snapshot_record(self)["candidate_block_count"]

        @property
        def candidate_scenario_denominator(self) -> int:
            return snapshot_record(self)["candidate_scenario_denominator"]

        @property
        def initial_replay_required_count(self) -> int:
            return snapshot_record(self)["initial_replay_required_count"]

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("ValidatedHistoricalScanSnapshot is immutable")

        def __repr__(self) -> str:
            return "ValidatedHistoricalScanSnapshot(<redacted>)"

        def __copy__(self) -> Any:
            raise TypeError("ValidatedHistoricalScanSnapshot is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("ValidatedHistoricalScanSnapshot is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("ValidatedHistoricalScanSnapshot is not serializable")

        def __reduce_ex__(self, _protocol: int) -> Any:
            raise TypeError("ValidatedHistoricalScanSnapshot is not serializable")

    class _HistoricalSelectionAction:
        __slots__ = ("__weakref__",)

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise ValueError("historical selection action is private")

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("historical selection action is immutable")

        def __repr__(self) -> str:
            return "_HistoricalSelectionAction(<redacted>)"

    class _ValidatedHistoricalSelection(MappingABC):
        __slots__ = ("__weakref__",)

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise ValueError("historical selection is private")

        def __getitem__(self, key: str) -> Any:
            return selection_record(self)["projection"][key]

        def __iter__(self):
            return iter(selection_record(self)["projection"])

        def __len__(self) -> int:
            return len(selection_record(self)["projection"])

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("historical selection is immutable")

        def __repr__(self) -> str:
            return "ValidatedHistoricalSelection(<redacted>)"

    def selection_record(value: Any) -> Dict[str, Any]:
        entry = selection_registry.get(id(value))
        if (
            type(value) is not _ValidatedHistoricalSelection
            or entry is None
            or entry[0]() is not value
            or entry[1].get("issuer") is not issuer
        ):
            raise ValueError("historical replay selection is invalid")
        return entry[1]

    def register_weak(
        value: Any, record: Dict[str, Any],
        registry: Dict[int, Tuple[weakref.ReferenceType, Dict[str, Any]]],
    ) -> None:
        value_id = id(value)

        def retire(reference: weakref.ReferenceType) -> None:
            current = registry.get(value_id)
            if current is not None and current[0] is reference:
                registry.pop(value_id, None)

        registry[value_id] = (weakref.ref(value, retire), record)

    def exact_fraction_projection(value: Fraction) -> Dict[str, Any]:
        numerator = value.numerator
        denominator = value.denominator
        sign = "-" if numerator < 0 else ""
        integer, remainder = divmod(abs(numerator), denominator)
        digits = []
        while remainder and len(digits) <= 4_096:
            remainder *= 10
            digit, remainder = divmod(remainder, denominator)
            digits.append(str(digit))
        if remainder:
            raise ValueError("historical replay economics is invalid")
        display = sign + str(integer)
        if digits:
            display += "." + "".join(digits).rstrip("0")
        return {
            "numerator": numerator,
            "denominator": denominator,
            "display": display,
        }

    def scenario_economics(
        record: Dict[str, Any], row: Mapping[str, Any],
        fact: Mapping[str, Any],
    ) -> Dict[str, Any]:
        policy = record["config"].policy.value
        mev_bps = policy.get("fees", {}).get("acceptance_mev_bps")
        if type(mev_bps) is not str or not mev_bps.isdigit():
            raise ValueError("historical replay economics is invalid")
        price = row["price"]
        denominator = 10 ** (18 + price["feed_decimals"])
        gross = Fraction(
            fact["weth_delta_raw"] * price["answer"], denominator
        )
        gas = Fraction(
            fact["gas_used"] * fact["effective_gas_price"]
            * price["answer"], denominator,
        )
        mev = Fraction(
            row["requested_notional_usd"] * int(mev_bps), 10_000
        )
        policy_net = gross - gas - mev
        return {
            "gross_edge_usd": exact_fraction_projection(gross),
            "gas_cost_usd": exact_fraction_projection(gas),
            "mev_buffer_usd": exact_fraction_projection(mev),
            "policy_net_edge_usd": exact_fraction_projection(policy_net),
        }

    def ledger_facts(
        record: Dict[str, Any], replay_ledger: Any,
    ) -> Tuple[Mapping[str, Any], ...]:
        if replay_ledger is None:
            return ()
        import scripts.historical_foundry_storage as storage

        try:
            projection = storage._validated_historical_replay_ledger_projection(
                ledger=replay_ledger,
                selection_transition=record["storage_transition"],
            )
        except storage.HistoricalFoundryStorageError:
            raise ValueError("historical replay ledger is invalid") from None
        if (
            projection.get("scan_inventory_sha256")
            != record["staging_inventory_sha256"]
            or projection.get("scenario_count")
            != len(projection.get("scenarios", ()))
        ):
            raise ValueError("historical replay ledger differs")
        return tuple(projection["scenarios"])

    def compute_controller(
        record: Dict[str, Any], replay_ledger: Any,
    ) -> Tuple[str, Mapping[str, Any]]:
        facts = ledger_facts(record, replay_ledger)
        rows = record["rows"]
        rows_by_key = {row["scenario_key"]: row for row in rows}
        if (
            len(rows_by_key) != len(rows)
            or any(fact["scenario_key"] not in rows_by_key for fact in facts)
        ):
            raise ValueError("historical replay ledger differs")
        issued_keys = record["issued_keys"]
        observed_keys = tuple(fact["scenario_key"] for fact in facts)
        failure = record.get("replay_failure")
        failure_key = (
            failure.get("scenario_key")
            if isinstance(failure, MappingABC) else None
        )
        expected_observed_count = len(issued_keys) - (
            1 if failure_key is not None else 0
        )
        if (
            len(set(observed_keys)) != len(observed_keys)
            or len(observed_keys) != expected_observed_count
            or observed_keys != tuple(issued_keys[:expected_observed_count])
            or (
                failure_key is not None
                and (
                    not issued_keys
                    or issued_keys[-1] != failure_key
                    or failure.get("category") not in (
                        "fork_hardfork_unsupported", "fork_window_mixed",
                        "foundry_replay_failed", "candidate_unresolved",
                        "authority", "archive",
                    )
                )
            )
        ):
            raise ValueError("historical replay ledger order differs")
        record["last_validated_facts"] = tuple(facts)

        position = 0
        candidate_states = []
        selected_block = None
        selected_facts = ()
        for block_number in record["block_numbers"]:
            block_rows = tuple(
                row for row in rows if row["block_number"] == block_number
            )
            required = tuple(
                row for row in block_rows
                if row["decision"] == "replay_required"
            )
            if not required:
                candidate_states.append({
                    "block_number": block_number,
                    "state": "resolved_nonpositive",
                    "transitions": [
                        "prefilter_non_candidate", "resolved_nonpositive",
                    ],
                    "scenario_count": 0,
                })
                continue
            transitions = ["candidate", "replaying_required"]
            block_facts = []
            for row in required:
                if position == len(facts):
                    if failure_key == row["scenario_key"]:
                        transitions.append("unresolved")
                        candidate_states.append({
                            "block_number": block_number,
                            "state": "unresolved",
                            "transitions": transitions,
                            "scenario_count": len(block_facts),
                        })
                        return "final", _freeze_prefilter_value({
                            "schema": "historical_foundry_selection/v1",
                            "status": "candidate_unresolved",
                            "staging_inventory_sha256": record[
                                "staging_inventory_sha256"
                            ],
                            "prefilter_grid_digest": record["grid_digest"],
                            "candidate_block_count": record[
                                "candidate_block_count"
                            ],
                            "scenario_denominator": record[
                                "candidate_scenario_denominator"
                            ],
                            "initial_replay_required_count": record[
                                "initial_replay_required_count"
                            ],
                            "selected_block": None,
                            "selected_scenario_count": 0,
                            "selected_scenarios": [],
                            "candidate_states": candidate_states,
                            "unresolved_candidate_count": 1,
                            "closed_reason": failure["category"],
                        })
                    return "action", MappingProxyType({
                        "state": "replaying_required",
                        "block_number": block_number,
                        "scenario_key": row["scenario_key"],
                    })
                fact = facts[position]
                if fact["scenario_key"] != row["scenario_key"]:
                    raise ValueError("historical replay ledger order differs")
                block_facts.append(fact)
                position += 1
            positive = any(
                fact["status"] == 1
                and scenario_economics(
                    record, rows_by_key[fact["scenario_key"]], fact
                )["policy_net_edge_usd"]["numerator"] > 0
                for fact in block_facts
            )
            if not positive:
                transitions.append("resolved_nonpositive")
                candidate_states.append({
                    "block_number": block_number,
                    "state": "resolved_nonpositive",
                    "transitions": transitions,
                    "scenario_count": len(block_facts),
                })
                continue
            transitions.extend(("tentative_positive", "completing_full_ten"))
            completed = {fact["scenario_key"] for fact in block_facts}
            for row in block_rows:
                if row["scenario_key"] in completed:
                    continue
                if position == len(facts):
                    if failure_key == row["scenario_key"]:
                        transitions.append("unresolved")
                        candidate_states.append({
                            "block_number": block_number,
                            "state": "unresolved",
                            "transitions": transitions,
                            "scenario_count": len(block_facts),
                        })
                        return "final", _freeze_prefilter_value({
                            "schema": "historical_foundry_selection/v1",
                            "status": "candidate_unresolved",
                            "staging_inventory_sha256": record[
                                "staging_inventory_sha256"
                            ],
                            "prefilter_grid_digest": record["grid_digest"],
                            "candidate_block_count": record[
                                "candidate_block_count"
                            ],
                            "scenario_denominator": record[
                                "candidate_scenario_denominator"
                            ],
                            "initial_replay_required_count": record[
                                "initial_replay_required_count"
                            ],
                            "selected_block": None,
                            "selected_scenario_count": 0,
                            "selected_scenarios": [],
                            "candidate_states": candidate_states,
                            "unresolved_candidate_count": 1,
                            "closed_reason": failure["category"],
                        })
                    return "action", MappingProxyType({
                        "state": "completing_full_ten",
                        "block_number": block_number,
                        "scenario_key": row["scenario_key"],
                    })
                fact = facts[position]
                if fact["scenario_key"] != row["scenario_key"]:
                    raise ValueError("historical replay ledger order differs")
                block_facts.append(fact)
                position += 1
            if len(block_facts) != 10:
                raise ValueError("historical replay denominator differs")
            if any(fact["status"] == 0 for fact in block_facts):
                transitions.append("nonpublishable_positive")
                candidate_states.append({
                    "block_number": block_number,
                    "state": "nonpublishable_positive",
                    "transitions": transitions,
                    "scenario_count": 10,
                })
                continue
            all_positive = any(
                scenario_economics(
                    record, rows_by_key[fact["scenario_key"]], fact
                )["policy_net_edge_usd"]["numerator"] > 0
                for fact in block_facts
            )
            if not all(fact["status"] == 1 for fact in block_facts):
                raise ValueError("historical replay result is unresolved")
            if not all_positive:
                transitions.append("resolved_nonpositive")
                candidate_states.append({
                    "block_number": block_number,
                    "state": "resolved_nonpositive",
                    "transitions": transitions,
                    "scenario_count": 10,
                })
                continue
            transitions.append("selected")
            candidate_states.append({
                "block_number": block_number,
                "state": "selected",
                "transitions": transitions,
                "scenario_count": 10,
            })
            selected_block = block_rows[0]["header"]
            selected_facts = tuple(block_facts)
            break

        if selected_block is not None:
            selected_number = selected_block["number"]
            seen_blocks = {row["block_number"] for row in candidate_states}
            for block_number in record["block_numbers"]:
                if block_number >= selected_number or block_number in seen_blocks:
                    continue
                candidate_states.append({
                    "block_number": block_number,
                    "state": "not_needed_older_than_selected",
                    "transitions": ["not_needed_older_than_selected"],
                    "scenario_count": 0,
                })
            if position != len(facts):
                raise ValueError("historical replay ledger has extra scenarios")
            selected_scenarios = []
            for fact in selected_facts:
                row = rows_by_key[fact["scenario_key"]]
                selected_scenarios.append({
                    **dict(fact),
                    "direction": row["direction"],
                    "requested_notional_usd": row[
                        "requested_notional_usd"
                    ],
                    "economics": scenario_economics(record, row, fact),
                })
            projection = {
                "schema": "historical_foundry_selection/v1",
                "status": "found_publishable_profitable_block",
                "staging_inventory_sha256": record[
                    "staging_inventory_sha256"
                ],
                "prefilter_grid_digest": record["grid_digest"],
                "candidate_block_count": record["candidate_block_count"],
                "scenario_denominator": record[
                    "candidate_scenario_denominator"
                ],
                "initial_replay_required_count": record[
                    "initial_replay_required_count"
                ],
                "selected_block": _detach_prefilter_value(selected_block),
                "selected_scenario_count": 10,
                "selected_scenarios": selected_scenarios,
                "candidate_states": candidate_states,
                "unresolved_candidate_count": 0,
            }
            return "final", _freeze_prefilter_value(projection)
        if position != len(facts):
            raise ValueError("historical replay ledger has extra scenarios")
        projection = {
            "schema": "historical_foundry_selection/v1",
            "status": "no_publishable_profitable_block",
            "staging_inventory_sha256": record[
                "staging_inventory_sha256"
            ],
            "prefilter_grid_digest": record["grid_digest"],
            "candidate_block_count": record["candidate_block_count"],
            "scenario_denominator": record[
                "candidate_scenario_denominator"
            ],
            "initial_replay_required_count": record[
                "initial_replay_required_count"
            ],
            "selected_block": None,
            "selected_scenario_count": 0,
            "selected_scenarios": [],
            "candidate_states": candidate_states,
            "unresolved_candidate_count": 0,
            "closed_reason": "no_publishable_profitable_block",
        }
        return "final", _freeze_prefilter_value(projection)

    def issue_selection(
        snapshot: ValidatedHistoricalScanSnapshot,
        record: Dict[str, Any], projection: Mapping[str, Any],
    ) -> _ValidatedHistoricalSelection:
        selection = object.__new__(_ValidatedHistoricalSelection)
        selection_record_value = {
            "issuer": issuer,
            "snapshot": snapshot,
            "projection": projection,
            "storage_transition": record["storage_transition"],
            "validated_facts": tuple(record.get("last_validated_facts", ())),
        }
        register_weak(selection, selection_record_value, selection_registry)
        return selection

    def open_validated_historical_scan_snapshot(
        *, config: HistoricalFoundryConfigSet, staging: Any,
    ) -> ValidatedHistoricalScanSnapshot:
        window, grid = _open_validated_historical_scan_authorities(
            config=config, staging=staging
        )
        rows = tuple(_iter_validated_historical_prefilter_rows(grid=grid))
        block_numbers = tuple(dict.fromkeys(
            row["block_number"] for row in rows
        ))
        candidate_blocks = tuple(
            block_number for block_number in block_numbers
            if any(
                row["block_number"] == block_number
                and row["decision"] == "replay_required"
                for row in rows
            )
        )
        if (
            len(rows) != window.block_count * 10
            or len(block_numbers) != window.block_count
            or tuple(sorted(block_numbers, reverse=True)) != block_numbers
            or any(
                sum(row["block_number"] == block for row in rows) != 10
                for block in block_numbers
            )
        ):
            raise ValueError("historical scan denominator differs")
        import scripts.historical_foundry_storage as storage

        transition = storage._bind_historical_selection_transition(
            staging=staging
        )
        snapshot = object.__new__(ValidatedHistoricalScanSnapshot)
        record = {
            "issuer": issuer,
            "config": config,
            "staging": staging,
            "window": window,
            "grid": grid,
            "rows": rows,
            "block_numbers": block_numbers,
            "candidate_blocks": candidate_blocks,
            "candidate_block_count": len(candidate_blocks),
            "candidate_scenario_denominator": len(candidate_blocks) * 10,
            "initial_replay_required_count": sum(
                row["decision"] == "replay_required" for row in rows
            ),
            "staging_inventory_sha256": grid.scan_inventory_sha256,
            "grid_digest": grid.grid_digest,
            "storage_transition": transition,
            "issued_keys": [],
            "active_action": None,
            "transition_history": [],
        }
        snapshot_id = id(snapshot)

        def retire(reference: weakref.ReferenceType) -> None:
            current = snapshot_registry.get(snapshot_id)
            if current is not None and current[0] is reference:
                snapshot_registry.pop(snapshot_id, None)

        snapshot_registry[snapshot_id] = (weakref.ref(snapshot, retire), record)
        return snapshot

    def select_historical_replay_block(
        *, snapshot: ValidatedHistoricalScanSnapshot, replay_ledger: Any,
    ) -> Mapping[str, Any]:
        record = snapshot_record(snapshot)
        if replay_ledger is None:
            raise ValueError("historical replay ledger is invalid")
        kind, projection = compute_controller(record, replay_ledger)
        if kind == "action":
            return projection
        terminal = record.get("terminal_selection")
        if terminal is not None:
            return terminal
        terminal = issue_selection(snapshot, record, projection)
        record["terminal_selection"] = terminal
        return terminal

    def _advance_historical_selection_controller(
        *, snapshot: ValidatedHistoricalScanSnapshot, replay_ledger: Any,
    ) -> Any:
        record = snapshot_record(snapshot)
        if record.get("terminal_selection") is not None:
            raise ValueError("historical replay selection is terminal")
        active = record.get("active_action")
        if active is not None:
            active_entry = action_registry.get(id(active))
            if (
                active_entry is not None
                and active_entry[0]() is active
                and active_entry[1].get("consumed") is not True
            ):
                raise ValueError("historical selection action is pending")
        kind, projection = compute_controller(record, replay_ledger)
        if kind == "final":
            if (
                replay_ledger is None
                and record["initial_replay_required_count"]
                and record.get("replay_failure") is None
            ):
                raise ValueError("historical replay ledger is invalid")
            terminal = issue_selection(snapshot, record, projection)
            record["terminal_selection"] = terminal
            record["transition_history"].append(
                _freeze_prefilter_value({
                    "index": len(record["transition_history"]),
                    "state": projection["status"],
                    "scenario_prefix": tuple(record["issued_keys"]),
                })
            )
            return terminal
        action = object.__new__(_HistoricalSelectionAction)
        action_record = {
            "issuer": issuer,
            "snapshot": snapshot,
            "projection": projection,
            "scenario_key": projection["scenario_key"],
            "consumed": False,
        }
        register_weak(action, action_record, action_registry)
        record["active_action"] = action
        record["transition_history"].append(
            _freeze_prefilter_value({
                "index": len(record["transition_history"]),
                "state": projection["state"],
                "scenario_key": projection["scenario_key"],
                "scenario_prefix": tuple(record["issued_keys"]),
            })
        )
        return action

    def _historical_selection_action_projection(
        *, action: Any,
    ) -> Mapping[str, Any]:
        entry = action_registry.get(id(action))
        if (
            type(action) is not _HistoricalSelectionAction
            or entry is None
            or entry[0]() is not action
            or entry[1].get("issuer") is not issuer
            or entry[1].get("consumed") is True
        ):
            raise ValueError("historical selection action is invalid")
        return entry[1]["projection"]

    def _consume_historical_selection_action(
        *, action: Any, context: Any,
    ) -> ValidatedReplayScenario:
        entry = action_registry.get(id(action))
        if (
            type(action) is not _HistoricalSelectionAction
            or entry is None
            or entry[0]() is not action
            or entry[1].get("issuer") is not issuer
            or entry[1].get("consumed") is True
        ):
            raise ValueError("historical selection action is invalid")
        action_record = entry[1]
        record = snapshot_record(action_record["snapshot"])
        if record.get("active_action") is not action:
            raise ValueError("historical selection action is invalid")
        import scripts.historical_foundry_anvil as anvil

        scenario = anvil._issue_next_historical_replay_scenario(
            context=context, scenario_key=action_record["scenario_key"]
        )
        if (
            type(scenario) is not ValidatedReplayScenario
            or scenario.scenario_key != action_record["scenario_key"]
        ):
            raise ValueError("historical selection scenario differs")
        action_record["consumed"] = True
        record["issued_keys"].append(action_record["scenario_key"])
        record["active_action"] = None
        return scenario

    def _record_historical_selection_failure(
        *, action: Any, error: Any,
    ) -> None:
        import scripts.historical_foundry_anvil as anvil

        entry = action_registry.get(id(action))
        if (
            type(action) is not _HistoricalSelectionAction
            or entry is None
            or entry[0]() is not action
            or entry[1].get("issuer") is not issuer
            or entry[1].get("consumed") is not True
            or type(error) is not anvil.HistoricalReplayError
        ):
            raise ValueError("historical replay failure is invalid")
        action_record = entry[1]
        record = snapshot_record(action_record["snapshot"])
        if (
            record.get("replay_failure") is not None
            or not record["issued_keys"]
            or record["issued_keys"][-1] != action_record["scenario_key"]
        ):
            raise ValueError("historical replay failure is invalid")
        record["replay_failure"] = MappingProxyType({
            "scenario_key": action_record["scenario_key"],
            "category": error.category,
        })
        return None

    def build_selected_historical_typed_members(
        *, config: HistoricalFoundryConfigSet,
        snapshot: ValidatedHistoricalScanSnapshot,
        selection: Mapping[str, Any],
    ) -> Mapping[str, bytes]:
        record = snapshot_record(snapshot)
        selected = selection_record(selection)
        bound_config = record["config"]
        if (
            selected.get("snapshot") is not snapshot
            or type(config) is not HistoricalFoundryConfigSet
            or any(
                getattr(config, role).physical_sha256
                != getattr(bound_config, role).physical_sha256
                for role in ("policy", "authority", "toolchain")
            )
        ):
            raise ValueError("historical replay selection lineage differs")
        projection = selected["projection"]
        if projection["status"] == "no_publishable_profitable_block":
            selected["typed_members"] = MappingProxyType({})
            return MappingProxyType({})
        if projection["status"] != "found_publishable_profitable_block":
            raise ValueError("historical replay selection is unresolved")
        selected_number = projection["selected_block"]["number"]
        block_rows = tuple(
            row for row in record["rows"]
            if row["block_number"] == selected_number
        )
        if len(block_rows) != 10:
            raise ValueError("historical selected block denominator differs")
        representative = block_rows[0]
        import scripts.historical_foundry_storage as storage

        try:
            reserve_source = storage._historical_selected_block_source_projection(
                selection_transition=record["storage_transition"],
                block_number=selected_number,
            )
            factory_pairs = storage._historical_factory_pair_projection(
                selection_transition=record["storage_transition"],
            )
        except storage.HistoricalFoundryStorageError:
            raise ValueError(
                "historical selected source authority differs"
            ) from None
        authority = config.authority.value
        tokens = {row["role"]: row for row in authority["tokens"]}
        venues = {row["venue_id"]: row for row in authority["venues"]}
        formula = authority["v2_formula"]
        if (
            set(tokens) != {"uni", "weth"}
            or set(venues) != {"uniswap_v2", "sushiswap_v2"}
            or formula.get("fee_numerator") != 997
            or formula.get("fee_denominator") != 1000
        ):
            raise ValueError("historical selected market authority differs")
        from datetime import datetime, timezone
        from scripts.route_quantity import V2PoolState, V2_FEE_FORMULA

        observed_at = datetime.fromtimestamp(
            representative["header"]["timestamp"], timezone.utc
        ).isoformat(timespec="seconds").replace("+00:00", "Z")
        header_sha = hashlib.sha256(_canonical_json_bytes(
            representative["header"]
        )).hexdigest()
        typed_members: Dict[str, bytes] = {}
        market_rows = []
        for venue_id in ("uniswap_v2", "sushiswap_v2"):
            reserve = representative["reserves"][venue_id]
            pair_authority = factory_pairs.get(venue_id)
            if (
                type(pair_authority) is not MappingProxyType
                or pair_authority.get("factory_pair_forward")
                != reserve["pair_address"]
                or pair_authority.get("factory_pair_reverse")
                != reserve["pair_address"]
            ):
                raise ValueError(
                    "historical selected pair authority differs"
                )
            market_id = "dex:eth:{}:{}:UNI".format(
                venue_id, reserve["pair_address"]
            )
            market_key = hashlib.sha256(
                b"historical_foundry_market_key/v1\0"
                + _canonical_json_bytes({"market_id": market_id})
            ).hexdigest()
            fee_identity = {
                "schema": "historical_foundry_v2_fee_identity/v1",
                "authority_sha256": config.authority.physical_sha256,
                "venue_id": venue_id,
                "fee_numerator": 997,
                "fee_denominator": 1000,
                "fee_bps": 30,
            }
            fee_proof = hashlib.sha256(
                b"historical_foundry_v2_fee_identity/v1\0"
                + _canonical_json_bytes(fee_identity)
            ).hexdigest()
            state = V2PoolState(
                chain="eth", chain_id=1, dex=venue_id,
                pool_address=reserve["pair_address"],
                token0_address=tokens["uni"]["address"],
                token1_address=tokens["weth"]["address"],
                token0_decimals=tokens["uni"]["decimals"],
                token1_decimals=tokens["weth"]["decimals"],
                reserve0_raw=reserve["reserve_uni_raw"],
                reserve1_raw=reserve["reserve_weth_raw"],
                reserve_timestamp_last_raw=reserve["pair_timestamp"],
                fee_bps=30, fee_numerator=997, fee_denominator=1000,
                fee_formula=V2_FEE_FORMULA,
                fee_proof_sha256=fee_proof,
                block_number=selected_number,
                block_hash=representative["block_hash"],
                block_header_sha256=header_sha,
                observed_at=observed_at,
                raw_response_sha256=reserve_source["sha256"],
            )
            integer_fields = {
                "chain_id", "token0_decimals", "token1_decimals",
                "reserve0_raw", "reserve1_raw",
                "reserve_timestamp_last_raw", "fee_bps",
                "fee_numerator", "fee_denominator", "block_number",
            }
            state_payload = {
                "schema": "route_v2_pool_state/v1",
                **{
                    name: (
                        str(getattr(state, name))
                        if name in integer_fields else getattr(state, name)
                    )
                    for name in (
                        "chain", "chain_id", "dex", "pool_address",
                        "token0_address", "token1_address",
                        "token0_decimals", "token1_decimals",
                        "reserve0_raw", "reserve1_raw",
                        "reserve_timestamp_last_raw", "fee_bps",
                        "fee_numerator", "fee_denominator", "fee_formula",
                        "fee_proof_sha256", "block_number", "block_hash",
                        "block_header_sha256", "observed_at",
                        "raw_response_sha256", "state_id",
                    )
                },
            }
            price = representative["price"]
            price_payload = {
                "schema": "route_dex_usd_price_context/v1",
                "market_id": market_id,
                "venue_id": venue_id,
                "chain_id": "1",
                "block_number": str(selected_number),
                "block_hash": representative["block_hash"],
                "proxy_address": price["proxy_address"],
                "round_id": str(price["round_id"]),
                "phase_id": str(price["phase_id"]),
                "answer": str(price["answer"]),
                "decimals": str(price["feed_decimals"]),
                "started_at": str(price["started_at"]),
                "updated_at": str(price["updated_at"]),
                "answered_in_round": str(price["answered_in_round"]),
                "valid_until": str(price["valid_until"]),
                "scan_inventory_sha256": record[
                    "staging_inventory_sha256"
                ],
            }
            state_bytes = _canonical_json_bytes(state_payload)
            price_bytes = _canonical_json_bytes(price_payload)
            state_path = "typed/{}/dex_pool_state.json".format(market_key)
            price_path = "typed/{}/dex_usd_price_context.json".format(
                market_key
            )
            typed_members[state_path] = state_bytes
            typed_members[price_path] = price_bytes
            market_rows.append({
                "market_id": market_id,
                "market_key": market_key,
                "venue_id": venue_id,
                "pair_address": reserve["pair_address"],
                "factory_pair_forward": pair_authority[
                    "factory_pair_forward"
                ],
                "factory_pair_reverse": pair_authority[
                    "factory_pair_reverse"
                ],
                "members": [
                    {
                        "role": "dex_pool_state", "path": state_path,
                        "byte_count": len(state_bytes),
                        "sha256": hashlib.sha256(state_bytes).hexdigest(),
                    },
                    {
                        "role": "dex_usd_price_context", "path": price_path,
                        "byte_count": len(price_bytes),
                        "sha256": hashlib.sha256(price_bytes).hexdigest(),
                    },
                ],
            })
        selected["typed_members"] = MappingProxyType(dict(typed_members))
        selected["typed_markets"] = _freeze_prefilter_value(market_rows)
        return MappingProxyType(dict(typed_members))

    def _finalize_historical_replay_run(
        *, config: HistoricalFoundryConfigSet,
        snapshot: ValidatedHistoricalScanSnapshot,
        selection: Mapping[str, Any],
    ) -> Any:
        record = snapshot_record(snapshot)
        selected = selection_record(selection)
        projection = selected["projection"]
        if (
            selected.get("snapshot") is not snapshot
            or selected.get("finalized") is True
            or projection["status"] not in (
                "found_publishable_profitable_block",
                "no_publishable_profitable_block",
            )
        ):
            raise ValueError("historical replay selection is not finalizable")
        typed_members = selected.get("typed_members")
        if typed_members is None:
            typed_members = build_selected_historical_typed_members(
                config=config, snapshot=snapshot, selection=selection
            )
        elif type(typed_members) is not MappingProxyType:
            raise ValueError("historical selected typed inventory differs")
        facts = selected.get("validated_facts")
        if type(facts) is not tuple:
            raise ValueError("historical candidate inventory differs")
        candidate_rows = []
        rows_by_key = {
            row["scenario_key"]: row for row in record["rows"]
        }
        for fact in facts:
            candidate_row = {
                name: fact[name] for name in (
                    "scenario_key", "block_number", "status",
                    "classification", "gas_used", "effective_gas_price",
                    "weth_delta_raw", "proof_inputs_hash",
                    "overlay_sha256", "receipt_sha256", "trace_sha256",
                    "result_sha256",
                )
            }
            candidate_row["economics"] = (
                scenario_economics(
                    record, rows_by_key[fact["scenario_key"]], fact
                ) if fact["status"] == 1 else None
            )
            candidate_rows.append(candidate_row)
        candidate_manifest = {
            "schema": "historical_foundry_candidate_manifest/v1",
            "staging_inventory_sha256": record[
                "staging_inventory_sha256"
            ],
            "prefilter_grid_digest": record["grid_digest"],
            "candidate_block_count": record["candidate_block_count"],
            "scenario_denominator": record[
                "candidate_scenario_denominator"
            ],
            "initial_replay_required_count": record[
                "initial_replay_required_count"
            ],
            "attempted_scenario_count": len(candidate_rows),
            "candidate_states": _detach_prefilter_value(
                projection["candidate_states"]
            ),
            "scenarios": candidate_rows,
        }
        typed_markets = selected.get("typed_markets", ())
        typed_manifest = {
            "schema": "historical_foundry_typed_manifest/v1",
            "selection_status": projection["status"],
            "selected_block": _detach_prefilter_value(
                projection["selected_block"]
            ),
            "market_count": len(typed_markets),
            "markets": _detach_prefilter_value(typed_markets),
            "member_count": len(typed_members),
            "members": [{
                "path": path,
                "byte_count": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            } for path, payload in sorted(typed_members.items())],
        }
        candidate_bytes = _canonical_json_bytes(candidate_manifest)
        typed_bytes = _canonical_json_bytes(typed_manifest)
        selection_bytes = _canonical_json_bytes(
            _detach_prefilter_value(projection)
        )
        import scripts.historical_foundry_storage as storage

        try:
            token = selected.get("finalization_token")
            if token is None:
                token = storage._seal_historical_run_finalization(
                    selection_transition=selected["storage_transition"],
                    candidate_manifest=candidate_bytes,
                    typed_manifest=typed_bytes,
                    selection=selection_bytes,
                    typed_members=typed_members,
                )
                selected["finalization_token"] = token
            result = storage._commit_historical_run_finalization(token=token)
        except Exception:
            if not storage._historical_run_finalization_is_retryable(
                token=token
            ):
                selected.pop("finalization_token", None)
            raise ValueError("historical replay finalization failed") from None
        selected.pop("finalization_token", None)
        selected["finalized"] = True
        selected["run_snapshot"] = result
        return result

    return (
        ValidatedHistoricalScanSnapshot,
        open_validated_historical_scan_snapshot,
        select_historical_replay_block,
        build_selected_historical_typed_members,
        _advance_historical_selection_controller,
        _historical_selection_action_projection,
        _consume_historical_selection_action,
        _record_historical_selection_failure,
        _finalize_historical_replay_run,
    )


(
    ValidatedHistoricalScanSnapshot,
    open_validated_historical_scan_snapshot,
    select_historical_replay_block,
    build_selected_historical_typed_members,
    _advance_historical_selection_controller,
    _historical_selection_action_projection,
    _consume_historical_selection_action,
    _record_historical_selection_failure,
    _finalize_historical_replay_run,
) = _initialize_historical_selection_capabilities()
del _initialize_historical_selection_capabilities
