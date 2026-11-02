"""Sealed production I/O boundary for Shadow route-cost evidence.

This module owns private profile capture and bounded wire clients.  Logical
evidence construction/replay remains in :mod:`route_cost_evidence`, so secrets,
URLs, filesystem paths, and live client objects cannot enter the sidecar.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple
import urllib.error
import urllib.request
import zlib

try:
    from scripts.route_cost_evidence import (
        MAX_NATIVE_PRICE_JSON_NODES,
        MAX_NATIVE_PRICE_JSON_SCALAR_BYTES,
        MAX_NATIVE_PRICE_JSON_STRING_BYTES,
        MAX_NATIVE_PRICE_MARKET_RULES_RAW_BYTES,
        MAX_NATIVE_PRICE_RAW_BYTES,
        MAX_PROFILE_BYTES,
        RouteCostEvidenceError,
        _validate_state_overrides,
        bind_native_price_to_phase_a_capture,
        build_route_cost_evidence_manifest_from_captured,
        build_fixed_block_phase_a_request_plan,
        build_native_price_evidence_from_captured,
        build_phase_b_scenario_request_plan,
        build_phase_b_trace_request_plan,
        build_selected_markets,
        build_submission_policy_scope,
        build_terminal_submission_policy_snapshot,
        build_terminal_transcript_inventory,
        canonical_json_bytes,
        decode_v2_swap_calldata,
        load_route_cost_adapter_registry,
        load_route_cost_connector_key_registry,
        physical_sha256,
        project_fixed_block_phase_a_capture,
        project_native_price_terminal_phase_a_capture,
        project_phase_b_capture,
        solidity_allowance_storage_key,
        solidity_balance_storage_key,
        submission_connector_profile_identity,
        trace_profile_identity,
        validate_retained_v2_pool_state_member,
        validate_route_cost_evidence_manifest_for_publication,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from route_cost_evidence import (  # type: ignore
        MAX_NATIVE_PRICE_JSON_NODES,
        MAX_NATIVE_PRICE_JSON_SCALAR_BYTES,
        MAX_NATIVE_PRICE_JSON_STRING_BYTES,
        MAX_NATIVE_PRICE_MARKET_RULES_RAW_BYTES,
        MAX_NATIVE_PRICE_RAW_BYTES,
        MAX_PROFILE_BYTES,
        RouteCostEvidenceError,
        _validate_state_overrides,
        bind_native_price_to_phase_a_capture,
        build_route_cost_evidence_manifest_from_captured,
        build_fixed_block_phase_a_request_plan,
        build_native_price_evidence_from_captured,
        build_phase_b_scenario_request_plan,
        build_phase_b_trace_request_plan,
        build_selected_markets,
        build_submission_policy_scope,
        build_terminal_submission_policy_snapshot,
        build_terminal_transcript_inventory,
        canonical_json_bytes,
        decode_v2_swap_calldata,
        load_route_cost_adapter_registry,
        load_route_cost_connector_key_registry,
        physical_sha256,
        project_fixed_block_phase_a_capture,
        project_native_price_terminal_phase_a_capture,
        project_phase_b_capture,
        solidity_allowance_storage_key,
        solidity_balance_storage_key,
        submission_connector_profile_identity,
        trace_profile_identity,
        validate_retained_v2_pool_state_member,
        validate_route_cost_evidence_manifest_for_publication,
    )


TRACE_PROFILE_ENV = "MARKET_ROUTE_TRACE_RPC_PROFILE"
CONNECTOR_PROFILE_ENV = "MARKET_ROUTE_SUBMISSION_CONNECTOR_PROFILE"
_JSON_NUMBER = re.compile(
    r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z",
    flags=re.ASCII,
)
_JSON_HEX4 = re.compile(br"[0-9A-Fa-f]{4}\Z", flags=re.ASCII)
_HTTP_FIELD_NAME = re.compile(
    r"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z", flags=re.ASCII
)
_MAX_EXACT_JSON_NUMBER_TOKEN_BYTES = 4 * 1024
_MAX_JSON_NESTING_DEPTH = 128


class RouteCostCollectorError(ValueError):
    """Fail-closed production capture error without secret-bearing text."""


class _RpcUnavailableError(Exception):
    """Secret-free internal classification for a failed RPC interaction."""


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Make every redirect terminal; private authorities are never portable."""

    def redirect_request(
        self, request: Any, file_pointer: Any, code: int, message: str,
        headers: Any, new_url: str,
    ) -> None:
        return None


_NATIVE_PRICE_TIMEOUT_SECONDS = 5
_NATIVE_PRICE_REQUESTS = (
    (
        "book",
        "https://data-api.binance.vision/api/v3/depth?symbol=ETHUSDT&limit=100",
        MAX_NATIVE_PRICE_RAW_BYTES,
    ),
    (
        "market_rules",
        "https://api.binance.com/api/v3/exchangeInfo?symbol=ETHUSDT",
        MAX_NATIVE_PRICE_MARKET_RULES_RAW_BYTES,
    ),
)


@dataclass(frozen=True)
class _NativePriceCaptureResult:
    """Secret-free private Phase-A outcome; raw bytes stay inside evidence."""

    status: str
    reason_code: Optional[str]
    evidence: Optional[Mapping[str, Any]] = field(repr=False)


def _native_capture_timestamp(anchor: str, elapsed_seconds: float) -> str:
    try:
        parsed = datetime.fromisoformat(anchor.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        value = (
            parsed + timedelta(seconds=elapsed_seconds)
        ).astimezone(__import__("datetime").timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
    except (AttributeError, TypeError, ValueError, OverflowError):
        raise RouteCostCollectorError(
            "route-cost time capability is invalid"
        ) from None
    return value.replace(".000000Z", "Z")


def _capture_native_price_evidence(
    *,
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    capture_utc_anchor: str,
) -> _NativePriceCaptureResult:
    """Capture and seal the two fixed Binance native-price responses once."""
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), _NoRedirectHandler()
    )
    opener.addheaders = []

    def monotonic(previous: Optional[float] = None) -> float:
        try:
            value = time.monotonic()
        except (TypeError, ValueError, OverflowError):
            raise RouteCostCollectorError(
                "route-cost native price monotonic capability is invalid"
            ) from None
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not __import__("math").isfinite(value)
            or (previous is not None and value < previous)
        ):
            raise RouteCostCollectorError(
                "route-cost native price monotonic capability is invalid"
            )
        return float(value)

    started = monotonic()
    deadline = started + _NATIVE_PRICE_TIMEOUT_SECONDS
    previous = started
    bodies: Dict[str, bytes] = {}
    observed_at: Dict[str, str] = {}
    outcomes: List[str] = []
    for role, url, byte_limit in _NATIVE_PRICE_REQUESTS:
        current = monotonic(previous)
        previous = current
        remaining = deadline - current
        if remaining <= 0:
            outcomes.append("unavailable")
            continue
        request = urllib.request.Request(
            url,
            method="GET",
            headers={"Accept": "application/json"},
        )
        try:
            with opener.open(request, timeout=remaining) as response:
                if getattr(response, "status", None) != 200:
                    raise RouteCostCollectorError(
                        "route-cost native price response is invalid"
                    )
                body = _decode_bounded_json_response(
                    response,
                    wire_limit=byte_limit,
                    decoded_limit=byte_limit,
                    scalar_limit=min(
                        MAX_NATIVE_PRICE_JSON_SCALAR_BYTES, byte_limit
                    ),
                    node_limit=MAX_NATIVE_PRICE_JSON_NODES,
                    ordinary_string_limit=MAX_NATIVE_PRICE_JSON_STRING_BYTES,
                    absolute_deadline=deadline,
                    return_decoded_bytes=True,
                )
        except urllib.error.HTTPError as error:
            outcomes.append(
                "unavailable"
                if error.code == 429 or 500 <= error.code <= 599
                else "invalid"
            )
            continue
        except (TimeoutError, OSError, urllib.error.URLError):
            outcomes.append("unavailable")
            continue
        except RouteCostCollectorError as error:
            message = str(error)
            if message == str(_resource_limit()):
                raise
            outcomes.append(
                "unavailable"
                if message in {
                    "route-cost response deadline exceeded",
                    "route-cost response stream is unavailable",
                }
                else "invalid"
            )
            continue
        finished = monotonic(previous)
        previous = finished
        if finished > deadline:
            outcomes.append("unavailable")
            continue
        bodies[role] = body
        observed_at[role] = _native_capture_timestamp(
            capture_utc_anchor, finished - started
        )
        outcomes.append("observed")

    if outcomes != ["observed", "observed"]:
        failed = "invalid" in outcomes
        return _NativePriceCaptureResult(
            status="failed" if failed else "unavailable",
            reason_code=(
                "native_price_invalid" if failed
                else "native_price_unavailable"
            ),
            evidence=None,
        )
    try:
        evidence = build_native_price_evidence_from_captured(
            run_id=run_id,
            route_cohort_id=route_cohort_id,
            candidate_source_generation=candidate_source_generation,
            book_raw_response=bodies["book"],
            book_observed_at=observed_at["book"],
            market_rules_raw_response=bodies["market_rules"],
            market_rules_observed_at=observed_at["market_rules"],
        )
    except RouteCostEvidenceError:
        return _NativePriceCaptureResult(
            status="failed",
            reason_code="native_price_invalid",
            evidence=None,
        )
    return _NativePriceCaptureResult(
        status="observed", reason_code=None, evidence=evidence
    )


def _resource_limit() -> RouteCostCollectorError:
    return RouteCostCollectorError("route-cost wire resource limit exceeded")


def _response_header_rows(response: Any) -> Tuple[Tuple[str, str], ...]:
    try:
        headers = getattr(response, "headers", None)
        raw_items = getattr(headers, "raw_items", None)
    except Exception:
        raise RouteCostCollectorError(
            "route-cost response headers are invalid"
        ) from None
    if not callable(raw_items):
        raise RouteCostCollectorError("route-cost response headers are invalid")
    try:
        rows = tuple(raw_items())
    except Exception:
        raise RouteCostCollectorError(
            "route-cost response headers are invalid"
        ) from None
    if len(rows) > 64:
        raise _resource_limit()
    total = 0
    normalized = []
    for name, value in rows:
        if not isinstance(name, str) or not isinstance(value, str):
            raise RouteCostCollectorError(
                "route-cost response headers are invalid"
            )
        try:
            name_bytes = name.encode("ascii")
            value_bytes = value.encode("latin-1")
        except UnicodeEncodeError:
            raise RouteCostCollectorError(
                "route-cost response headers are invalid"
            ) from None
        if (
            not name_bytes
            or len(name_bytes) > 128
            or len(value_bytes) > 8 * 1024
            or _HTTP_FIELD_NAME.fullmatch(name) is None
            or any(
                (byte < 32 and byte != 9) or byte == 127
                for byte in value_bytes
            )
        ):
            if len(name_bytes) > 128 or len(value_bytes) > 8 * 1024:
                raise _resource_limit()
            raise RouteCostCollectorError(
                "route-cost response headers are invalid"
            )
        total += len(name_bytes) + len(value_bytes)
        if total > 32 * 1024:
            raise _resource_limit()
        normalized.append((name.lower(), value))
    return tuple(normalized)


def _one_header(
    rows: Tuple[Tuple[str, str], ...], name: str
) -> Optional[str]:
    values = [value for key, value in rows if key == name]
    if len(values) > 1:
        raise RouteCostCollectorError(
            "route-cost response headers are ambiguous"
        )
    return values[0] if values else None


def _bounded_json_shape(
    value: Any, *, node_limit: int, ordinary_string_limit: int,
    permit_binary_float: bool = False,
) -> None:
    nodes = 0
    pending = [value]
    while pending:
        current = pending.pop()
        nodes += 1
        if nodes > node_limit:
            raise _resource_limit()
        if isinstance(current, dict):
            for key, nested in current.items():
                if not isinstance(key, str):
                    raise RouteCostCollectorError(
                        "route-cost JSON object key is invalid"
                    )
                try:
                    encoded = key.encode("utf-8")
                except UnicodeEncodeError:
                    raise RouteCostCollectorError(
                        "route-cost JSON string is invalid"
                    ) from None
                if len(encoded) > ordinary_string_limit:
                    raise _resource_limit()
                pending.append(nested)
        elif isinstance(current, list):
            pending.extend(current)
        elif isinstance(current, str):
            try:
                encoded = current.encode("utf-8")
            except UnicodeEncodeError:
                raise RouteCostCollectorError(
                    "route-cost JSON string is invalid"
                ) from None
            if len(encoded) > ordinary_string_limit:
                raise _resource_limit()
        elif current is None or current is True or current is False:
            pass
        elif type(current) is int:
            pass
        elif isinstance(current, Decimal):
            token = str(current)
            if (
                not current.is_finite()
                or (current.is_zero() and current.is_signed())
                or _JSON_NUMBER.fullmatch(token) is None
            ):
                raise RouteCostCollectorError(
                    "route-cost JSON number is invalid"
                )
        elif type(current) is float and permit_binary_float:
            if (
                not __import__("math").isfinite(current)
                or (current == 0.0 and __import__("math").copysign(1.0, current) < 0)
            ):
                raise RouteCostCollectorError(
                    "route-cost JSON number is invalid"
                )
        else:
            raise RouteCostCollectorError("route-cost JSON value is invalid")


class _JSONPreflightState:
    """Lexical limits enforced before ``json.loads`` builds a Python tree."""

    def __init__(self, *, node_limit: int, scalar_limit: int) -> None:
        self.node_limit = node_limit
        self.scalar_limit = scalar_limit
        self.nodes = 0
        self.scalars = 0

    def add_node(self) -> None:
        self.nodes += 1
        if self.nodes > self.node_limit:
            raise _resource_limit()

    def add_scalar(self, amount: int) -> None:
        self.scalars += amount
        if self.scalars > self.scalar_limit:
            raise _resource_limit()


def _json_invalid() -> RouteCostCollectorError:
    return RouteCostCollectorError("route-cost JSON response is invalid")


def _scan_json_string(
    data: bytes,
    index: int,
    *,
    maximum_bytes: int,
    state: _JSONPreflightState,
    capture: bool,
) -> Tuple[int, Optional[str]]:
    if index >= len(data) or data[index] != 0x22:
        raise _json_invalid()
    index += 1
    decoded_bytes = 0
    characters = [] if capture else None

    def add_character(character: str, width: int) -> None:
        nonlocal decoded_bytes
        decoded_bytes += width
        if decoded_bytes > maximum_bytes:
            raise _resource_limit()
        state.add_scalar(width)
        if characters is not None:
            characters.append(character)

    while index < len(data):
        byte = data[index]
        if byte == 0x22:
            return index + 1, (
                None if characters is None else "".join(characters)
            )
        if byte < 0x20:
            raise _json_invalid()
        if byte == 0x5C:
            if index + 1 >= len(data):
                raise _json_invalid()
            escaped = data[index + 1]
            simple = {
                0x22: '"',
                0x5C: "\\",
                0x2F: "/",
                0x62: "\b",
                0x66: "\f",
                0x6E: "\n",
                0x72: "\r",
                0x74: "\t",
            }
            if escaped in simple:
                add_character(simple[escaped], 1)
                index += 2
                continue
            if escaped != 0x75 or index + 6 > len(data):
                raise _json_invalid()
            unit_token = data[index + 2:index + 6]
            if _JSON_HEX4.fullmatch(unit_token) is None:
                raise _json_invalid()
            unit = int(unit_token, 16)
            if 0xD800 <= unit <= 0xDBFF:
                if (
                    index + 12 > len(data)
                    or data[index + 6:index + 8] != b"\\u"
                ):
                    raise _json_invalid()
                low_token = data[index + 8:index + 12]
                if _JSON_HEX4.fullmatch(low_token) is None:
                    raise _json_invalid()
                low = int(low_token, 16)
                if not 0xDC00 <= low <= 0xDFFF:
                    raise _json_invalid()
                codepoint = (
                    0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00)
                )
                add_character(chr(codepoint), 4)
                index += 12
                continue
            if 0xDC00 <= unit <= 0xDFFF:
                raise _json_invalid()
            width = 1 if unit <= 0x7F else 2 if unit <= 0x7FF else 3
            add_character(chr(unit), width)
            index += 6
            continue
        if byte < 0x80:
            add_character(chr(byte), 1)
            index += 1
            continue

        if 0xC2 <= byte <= 0xDF:
            width = 2
        elif 0xE0 <= byte <= 0xEF:
            width = 3
        elif 0xF0 <= byte <= 0xF4:
            width = 4
        else:
            raise _json_invalid()
        end = index + width
        if end > len(data):
            raise _json_invalid()
        encoded = data[index:end]
        try:
            character = encoded.decode("utf-8")
        except UnicodeDecodeError:
            raise _json_invalid() from None
        add_character(character, width)
        index = end
    raise _json_invalid()


def _preflight_json_bytes(
    data: bytes,
    *,
    node_limit: int,
    ordinary_string_limit: int,
    scalar_limit: int,
) -> None:
    """Parse JSON lexically and enforce resource limits before materializing."""
    state = _JSONPreflightState(
        node_limit=node_limit, scalar_limit=scalar_limit
    )
    whitespace = {0x09, 0x0A, 0x0D, 0x20}

    def skip(index: int) -> int:
        while index < len(data) and data[index] in whitespace:
            index += 1
        return index

    def parse_number(index: int) -> int:
        start = index
        if index < len(data) and data[index] == 0x2D:
            index += 1
        if index >= len(data):
            raise _json_invalid()
        if data[index] == 0x30:
            index += 1
        elif 0x31 <= data[index] <= 0x39:
            index += 1
            while index < len(data) and 0x30 <= data[index] <= 0x39:
                index += 1
        else:
            raise _json_invalid()
        if index < len(data) and data[index] == 0x2E:
            index += 1
            fraction_start = index
            while index < len(data) and 0x30 <= data[index] <= 0x39:
                index += 1
            if index == fraction_start:
                raise _json_invalid()
        if index < len(data) and data[index] in {0x45, 0x65}:
            index += 1
            if index < len(data) and data[index] in {0x2B, 0x2D}:
                index += 1
            exponent_start = index
            while index < len(data) and 0x30 <= data[index] <= 0x39:
                index += 1
            if index == exponent_start:
                raise _json_invalid()
        width = index - start
        if width > _MAX_EXACT_JSON_NUMBER_TOKEN_BYTES:
            raise _resource_limit()
        state.add_scalar(width)
        return index

    def parse_value(index: int, depth: int) -> int:
        index = skip(index)
        if index >= len(data):
            raise _json_invalid()
        state.add_node()
        byte = data[index]
        if byte == 0x22:
            return _scan_json_string(
                data,
                index,
                maximum_bytes=ordinary_string_limit,
                state=state,
                capture=False,
            )[0]
        if byte == 0x7B:
            if depth + 1 > _MAX_JSON_NESTING_DEPTH:
                raise _resource_limit()
            index = skip(index + 1)
            if index < len(data) and data[index] == 0x7D:
                return index + 1
            while True:
                index, _key = _scan_json_string(
                    data,
                    index,
                    maximum_bytes=ordinary_string_limit,
                    state=state,
                    capture=False,
                )
                index = skip(index)
                if index >= len(data) or data[index] != 0x3A:
                    raise _json_invalid()
                index = parse_value(index + 1, depth + 1)
                index = skip(index)
                if index < len(data) and data[index] == 0x7D:
                    return index + 1
                if index >= len(data) or data[index] != 0x2C:
                    raise _json_invalid()
                index = skip(index + 1)
        if byte == 0x5B:
            if depth + 1 > _MAX_JSON_NESTING_DEPTH:
                raise _resource_limit()
            index = skip(index + 1)
            if index < len(data) and data[index] == 0x5D:
                return index + 1
            while True:
                index = parse_value(index, depth + 1)
                index = skip(index)
                if index < len(data) and data[index] == 0x5D:
                    return index + 1
                if index >= len(data) or data[index] != 0x2C:
                    raise _json_invalid()
                index = skip(index + 1)
        for literal, scalar_width in (
            (b"true", 4), (b"false", 5), (b"null", 4)
        ):
            if data.startswith(literal, index):
                state.add_scalar(scalar_width)
                return index + len(literal)
        if byte == 0x2D or 0x30 <= byte <= 0x39:
            return parse_number(index)
        raise _json_invalid()

    final = skip(parse_value(0, 0))
    if final != len(data):
        raise _json_invalid()


def _decode_bounded_json_response(
    response: Any, *, wire_limit: int, decoded_limit: int,
    scalar_limit: int, node_limit: int, ordinary_string_limit: int,
    require_canonical: bool = False,
    materialize_exact_floats: bool = False,
    absolute_deadline: Optional[float] = None,
    return_decoded_bytes: bool = False,
) -> Any:
    """Stream, bound, decode, and shape-check one JSON HTTP response."""
    for value in (
        wire_limit, decoded_limit, scalar_limit, node_limit,
        ordinary_string_limit,
    ):
        if type(value) is not int or value <= 0:
            raise RouteCostCollectorError("route-cost wire limit is invalid")
    if (
        type(require_canonical) is not bool
        or type(materialize_exact_floats) is not bool
        or type(return_decoded_bytes) is not bool
        or (
            absolute_deadline is not None
            and (
                isinstance(absolute_deadline, bool)
                or not isinstance(absolute_deadline, (int, float))
                or not __import__("math").isfinite(absolute_deadline)
            )
        )
    ):
        raise RouteCostCollectorError("route-cost wire limit is invalid")
    rows = _response_header_rows(response)
    raw_length = _one_header(rows, "content-length")
    declared_length = None
    if raw_length is not None:
        if (
            not raw_length
            or not raw_length.isascii()
            or not raw_length.isdecimal()
            or (len(raw_length) > 1 and raw_length.startswith("0"))
        ):
            raise RouteCostCollectorError(
                "route-cost Content-Length is invalid"
            )
        wire_limit_text = str(wire_limit)
        if len(raw_length) > len(wire_limit_text) or (
            len(raw_length) == len(wire_limit_text)
            and raw_length > wire_limit_text
        ):
            raise _resource_limit()
        declared_length = int(raw_length)
    encoding = _one_header(rows, "content-encoding")
    if encoding not in {None, "identity", "gzip"}:
        raise RouteCostCollectorError(
            "route-cost response encoding is unsupported"
        )
    wire_bytes = 0
    decoded = bytearray()
    decoder = (
        zlib.decompressobj(16 + zlib.MAX_WBITS)
        if encoding == "gzip" else None
    )

    def deadline_stream() -> Tuple[Any, Any]:
        """Return a one-recv reader and the production socket timeout setter."""
        reader = getattr(response, "read1", None)
        try:
            socket_value = response.fp.raw._sock
            setter = getattr(socket_value, "settimeout", None)
        except (AttributeError, OSError):
            setter = None
        if not callable(reader) or not callable(setter):
            raise RouteCostCollectorError(
                "route-cost deadline-bound stream is invalid"
            )
        return reader, setter

    deadline_reader = None
    deadline_setter = None
    if absolute_deadline is not None:
        deadline_reader, deadline_setter = deadline_stream()
    try:
        while True:
            if absolute_deadline is not None:
                remaining = float(absolute_deadline) - time.monotonic()
                if remaining <= 0:
                    raise RouteCostCollectorError(
                        "route-cost response deadline exceeded"
                    )
                try:
                    deadline_setter(remaining)
                except (OSError, TimeoutError):
                    raise RouteCostCollectorError(
                        "route-cost response stream is unavailable"
                    ) from None
            reader = deadline_reader if deadline_reader is not None else response.read
            chunk = reader(min(64 * 1024, wire_limit + 1 - wire_bytes))
            if not isinstance(chunk, bytes):
                raise RouteCostCollectorError(
                    "route-cost response stream is invalid"
                )
            if not chunk:
                break
            wire_bytes += len(chunk)
            if wire_bytes > wire_limit:
                raise _resource_limit()
            if decoder is None:
                decoded.extend(chunk)
                if len(decoded) > decoded_limit:
                    raise _resource_limit()
                continue
            pending = chunk
            while pending:
                piece = decoder.decompress(
                    pending, decoded_limit + 1 - len(decoded)
                )
                decoded.extend(piece)
                if len(decoded) > decoded_limit:
                    raise _resource_limit()
                pending = decoder.unconsumed_tail
        if declared_length is not None and wire_bytes != declared_length:
            raise RouteCostCollectorError(
                "route-cost Content-Length differs from wire bytes"
            )
        if decoder is not None:
            decoded.extend(
                decoder.flush(decoded_limit + 1 - len(decoded))
            )
            if (
                len(decoded) > decoded_limit
                or not decoder.eof
                or decoder.unused_data
                or decoder.unconsumed_tail
            ):
                if len(decoded) > decoded_limit:
                    raise _resource_limit()
                raise RouteCostCollectorError(
                    "route-cost gzip response is invalid"
                )
    except zlib.error:
        raise RouteCostCollectorError(
            "route-cost gzip response is invalid"
        ) from None
    except (OSError, TimeoutError):
        raise RouteCostCollectorError(
            "route-cost response stream is unavailable"
        ) from None
    try:
        text_value = bytes(decoded).decode("utf-8")
    except UnicodeDecodeError:
        raise RouteCostCollectorError(
            "route-cost JSON response is invalid"
        ) from None
    _preflight_json_bytes(
        bytes(decoded),
        node_limit=node_limit,
        ordinary_string_limit=ordinary_string_limit,
        scalar_limit=scalar_limit,
    )

    def reject_duplicate(pairs: Any) -> Dict[str, Any]:
        result = {}
        for key, nested in pairs:
            if key in result:
                raise RouteCostCollectorError(
                    "route-cost JSON has duplicate keys"
                )
            result[key] = nested
        return result

    def reject_constant(_value: str) -> None:
        raise RouteCostCollectorError("route-cost JSON number is invalid")

    def exact_float(token: str) -> Any:
        if len(token.encode("ascii")) > min(
            scalar_limit, _MAX_EXACT_JSON_NUMBER_TOKEN_BYTES
        ):
            raise _resource_limit()
        try:
            value = Decimal(token)
        except (InvalidOperation, ValueError):
            raise RouteCostCollectorError(
                "route-cost JSON number is invalid"
            ) from None
        if (
            not value.is_finite()
            or (value.is_zero() and value.is_signed())
            or (not value.is_zero() and abs(value.adjusted()) > 4095)
        ):
            raise RouteCostCollectorError(
                "route-cost JSON number is invalid"
            )
        if materialize_exact_floats:
            try:
                binary_value = float(value)
            except (OverflowError, ValueError):
                raise RouteCostCollectorError(
                    "route-cost JSON number is invalid"
                ) from None
            if (
                not __import__("math").isfinite(binary_value)
                or Decimal(str(binary_value)) != value
            ):
                raise RouteCostCollectorError(
                    "route-cost JSON number is invalid"
                )
            return binary_value
        return value

    def exact_int(token: str) -> int:
        encoded = token.encode("ascii")
        if len(encoded) > min(
            scalar_limit, _MAX_EXACT_JSON_NUMBER_TOKEN_BYTES
        ):
            raise _resource_limit()
        if (
            not token
            or (token.startswith("-") and token == "-0")
            or not re.fullmatch(r"-?(?:0|[1-9][0-9]*)", token)
        ):
            raise RouteCostCollectorError(
                "route-cost JSON number is invalid"
            )
        try:
            return int(token)
        except (ValueError, OverflowError):
            raise RouteCostCollectorError(
                "route-cost JSON number is invalid"
            ) from None

    try:
        value = json.loads(
            text_value,
            object_pairs_hook=reject_duplicate,
            parse_constant=reject_constant,
            parse_float=exact_float,
            parse_int=exact_int,
        )
    except RouteCostCollectorError:
        raise
    except RecursionError:
        raise _resource_limit() from None
    except (TypeError, ValueError, json.JSONDecodeError):
        raise RouteCostCollectorError(
            "route-cost JSON response is invalid"
        ) from None
    _bounded_json_shape(
        value,
        node_limit=node_limit,
        ordinary_string_limit=ordinary_string_limit,
        permit_binary_float=materialize_exact_floats,
    )
    if require_canonical:
        try:
            canonical = canonical_json_bytes(value)
        except RouteCostEvidenceError:
            raise RouteCostCollectorError(
                "route-cost JSON response is noncanonical"
            ) from None
        if bytes(decoded) != canonical:
            raise RouteCostCollectorError(
                "route-cost JSON response is noncanonical"
            )
    if return_decoded_bytes:
        return bytes(decoded)
    return value


@dataclass(frozen=True)
class RouteCostProfileCapture:
    """One-run private profiles plus their safe public identity projections."""

    trace_profile: Optional[Mapping[str, Any]] = field(repr=False)
    connector_profile: Optional[Mapping[str, Any]] = field(repr=False)
    trace_profile_identity: Mapping[str, Any]
    trace_profile_generation: str
    submission_connector_profile_identity: Mapping[str, Any]
    submission_connector_profile_generation: str

    def public_projection(self) -> Dict[str, Any]:
        return {
            "trace_profile_identity": dict(self.trace_profile_identity),
            "trace_profile_generation": self.trace_profile_generation,
            "submission_connector_profile_identity": dict(
                self.submission_connector_profile_identity
            ),
            "submission_connector_profile_generation": (
                self.submission_connector_profile_generation
            ),
        }


def _private_profile_path(raw: str, label: str) -> Path:
    try:
        path = Path(raw)
    except (TypeError, ValueError):
        raise RouteCostCollectorError(
            "configured {} profile path is invalid".format(label)
        ) from None
    if (
        not path.is_absolute()
        or any(component in {"", ".", ".."} for component in path.parts[1:])
    ):
        raise RouteCostCollectorError(
            "configured {} profile path is invalid".format(label)
        )
    if sys.platform == "darwin" and len(path.parts) > 1:
        if path.parts[1] in {"tmp", "var"}:
            alias = Path("/") / path.parts[1]
            expected = Path("/private") / path.parts[1]
            if (
                alias.is_symlink()
                and Path(os.path.realpath(str(alias))) == expected
            ):
                path = expected.joinpath(*path.parts[2:])
    return path


def _directory_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise RouteCostCollectorError(
            "private profile descriptor-safe open is unavailable"
        )
    return (
        os.O_RDONLY
        | nofollow
        | directory
        | getattr(os, "O_CLOEXEC", 0)
    )


def _profile_metadata(metadata: os.stat_result, label: str) -> None:
    getuid = getattr(os, "geteuid", None)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (getuid is not None and metadata.st_uid != getuid())
        or metadata.st_size <= 0
        or metadata.st_size > MAX_PROFILE_BYTES
    ):
        raise RouteCostCollectorError(
            "configured {} profile is not one bounded owner-only file".format(
                label
            )
        )


def _read_private_profile(path: Path, label: str) -> Mapping[str, Any]:
    directory_flags = _directory_flags()
    file_flags = (
        os.O_RDONLY
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    held = []
    ancestry = []
    descriptor = -1
    try:
        parent = os.open(os.sep, directory_flags)
        held.append(parent)
        for component in path.parts[1:-1]:
            child = os.open(component, directory_flags, dir_fd=parent)
            # Ownership begins at successful open, before any metadata call
            # that may fail.  The unified finally block must close this FD on
            # every unsafe/error path.
            held.append(child)
            metadata = os.fstat(child)
            by_path = os.stat(component, dir_fd=parent, follow_symlinks=False)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino)
                != (by_path.st_dev, by_path.st_ino)
            ):
                raise RouteCostCollectorError(
                    "configured {} profile ancestry is unsafe".format(label)
                )
            snapshot = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_ctime_ns,
                metadata.st_mtime_ns,
            )
            ancestry.append((parent, component, child, snapshot))
            parent = child
        descriptor = os.open(path.name, file_flags, dir_fd=parent)
        metadata = os.fstat(descriptor)
        by_path = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        _profile_metadata(metadata, label)
        if (metadata.st_dev, metadata.st_ino) != (
            by_path.st_dev,
            by_path.st_ino,
        ):
            raise RouteCostCollectorError(
                "configured {} profile identity changed".format(label)
            )
        data = bytearray()
        while True:
            remaining = MAX_PROFILE_BYTES + 1 - len(data)
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > MAX_PROFILE_BYTES:
                raise RouteCostCollectorError(
                    "configured {} profile exceeds its byte limit".format(label)
                )
        final = os.fstat(descriptor)
        if (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_nlink,
            final.st_ctime_ns,
            final.st_mtime_ns,
        ) != (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_nlink,
            metadata.st_ctime_ns,
            metadata.st_mtime_ns,
        ):
            raise RouteCostCollectorError(
                "configured {} profile changed while reading".format(label)
            )
        final_by_path = os.stat(
            path.name, dir_fd=parent, follow_symlinks=False
        )
        if (
            not stat.S_ISREG(final_by_path.st_mode)
            or (final_by_path.st_dev, final_by_path.st_ino)
            != (metadata.st_dev, metadata.st_ino)
        ):
            raise RouteCostCollectorError(
                "configured {} profile changed while reading".format(label)
            )
        for ancestor_parent, component, ancestor, snapshot in ancestry:
            current = os.fstat(ancestor)
            current_by_path = os.stat(
                component,
                dir_fd=ancestor_parent,
                follow_symlinks=False,
            )
            if (
                current.st_dev,
                current.st_ino,
                current.st_ctime_ns,
                current.st_mtime_ns,
            ) != snapshot or (
                current_by_path.st_dev,
                current_by_path.st_ino,
            ) != (current.st_dev, current.st_ino):
                raise RouteCostCollectorError(
                    "configured {} profile ancestry changed".format(label)
                )
    except RouteCostCollectorError:
        raise
    except OSError:
        raise RouteCostCollectorError(
            "configured {} profile is unavailable or unsafe".format(label)
        ) from None
    finally:
        active_error = sys.exc_info()[0] is not None
        cleanup_failed = False
        close_inventory = (
            ([descriptor] if descriptor >= 0 else []) + list(reversed(held))
        )
        for current in close_inventory:
            try:
                os.close(current)
            except OSError:
                # close(2) failure leaves descriptor state ambiguous.  Never
                # retry that FD, but do attempt every other owned descriptor.
                cleanup_failed = True
        if cleanup_failed and not active_error:
            raise RouteCostCollectorError(
                "configured {} profile cleanup failed".format(label)
            ) from None
    try:
        value = json.loads(bytes(data).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RouteCostCollectorError(
            "configured {} profile JSON is invalid".format(label)
        ) from None
    try:
        canonical_profile = canonical_json_bytes(value)
    except RouteCostEvidenceError:
        raise RouteCostCollectorError(
            "configured {} profile bytes are noncanonical".format(label)
        ) from None
    if not isinstance(value, Mapping) or bytes(data) != canonical_profile + b"\n":
        raise RouteCostCollectorError(
            "configured {} profile bytes are noncanonical".format(label)
        )
    return dict(value)


def _profile_from_environment(name: str, label: str) -> Optional[Mapping[str, Any]]:
    if name not in os.environ:
        return None
    path = _private_profile_path(os.environ[name], label)
    return _read_private_profile(path, label)


def load_route_cost_profile_capture() -> RouteCostProfileCapture:
    """Capture both fixed private profiles exactly once for one cost run."""
    trace = _profile_from_environment(TRACE_PROFILE_ENV, "trace RPC")
    connector = _profile_from_environment(
        CONNECTOR_PROFILE_ENV, "submission connector"
    )
    try:
        trace_identity, trace_generation = trace_profile_identity(trace)
        connector_identity, connector_generation = (
            submission_connector_profile_identity(connector)
        )
    except (TypeError, ValueError):
        raise RouteCostCollectorError(
            "configured route-cost profile contract is invalid"
        ) from None
    return RouteCostProfileCapture(
        trace_profile=(
            None if trace is None else MappingProxyType(dict(trace))
        ),
        connector_profile=(
            None if connector is None else MappingProxyType(dict(connector))
        ),
        trace_profile_identity=MappingProxyType(dict(trace_identity)),
        trace_profile_generation=trace_generation,
        submission_connector_profile_identity=MappingProxyType(
            dict(connector_identity)
        ),
        submission_connector_profile_generation=connector_generation,
    )


def _load_retained_route_cost_typed_members(
    data_dir: Path,
    cohort: Mapping[str, Any],
    *,
    retain_keys: Optional[frozenset] = None,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Descriptor-reread the exact retained typed inventory for cost replay.

    The raw run is derived solely from ``cohort.raw_evidence_run_id``.  Paths
    and the manifest are treated as untrusted indexes: every descriptor is
    reconstructed from validated core lineage and every byte is reread before
    any result escapes.
    """
    import hashlib

    try:
        from scripts.route_cost_evidence import (
            validate_retained_v2_pool_state_member,
        )
        from scripts.route_shadow_inputs import (
            TYPED_SOURCE_MANIFEST_FIELDS,
            TYPED_SOURCE_MANIFEST_MEMBER_FIELDS,
            TYPED_SOURCE_MANIFEST_SCHEMA,
            TYPED_SOURCE_ROLE_CONTRACTS,
            typed_source_lineage_observed_members,
            validate_typed_source_lineage,
        )
    except ModuleNotFoundError:  # pragma: no cover - direct script execution
        from route_cost_evidence import (  # type: ignore
            validate_retained_v2_pool_state_member,
        )
        from route_shadow_inputs import (  # type: ignore
            TYPED_SOURCE_MANIFEST_FIELDS,
            TYPED_SOURCE_MANIFEST_MEMBER_FIELDS,
            TYPED_SOURCE_MANIFEST_SCHEMA,
            TYPED_SOURCE_ROLE_CONTRACTS,
            typed_source_lineage_observed_members,
            validate_typed_source_lineage,
        )

    failure = "retained route-cost typed evidence is invalid"
    basename_pattern = re.compile(
        r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", flags=re.ASCII
    )
    hash_pattern = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
    manifest_limit = 16 * 1024 * 1024

    def fail() -> None:
        raise RouteCostCollectorError(failure)

    if retain_keys is not None and (
        not isinstance(retain_keys, frozenset)
        or any(
            not isinstance(key, tuple)
            or len(key) != 2
            or not all(isinstance(value, str) for value in key)
            for key in retain_keys
        )
    ):
        fail()

    def basename(value: Any) -> str:
        if (
            not isinstance(value, str)
            or basename_pattern.fullmatch(value) is None
            or value in {".", ".."}
            or os.path.basename(value) != value
            or len(value.encode("ascii")) > 128
        ):
            fail()
        return value

    def stable(details: os.stat_result) -> Tuple[Any, ...]:
        return (
            details.st_dev,
            details.st_ino,
            details.st_mode,
            details.st_nlink,
            details.st_uid,
            details.st_gid,
            details.st_size,
            getattr(details, "st_mtime_ns", None),
            getattr(details, "st_ctime_ns", None),
            getattr(details, "st_birthtime_ns", None),
            getattr(details, "st_flags", None),
        )

    def open_directory_at(
        parent_fd: int, name: str
    ) -> Tuple[int, os.stat_result]:
        if (
            not isinstance(name, str)
            or not name
            or name in {".", ".."}
            or os.path.basename(name) != name
            or os.sep in name
            or (os.altsep is not None and os.altsep in name)
        ):
            fail()
        child_name = name
        flags = _directory_flags()
        descriptor = os.open(child_name, flags, dir_fd=parent_fd)
        try:
            details = os.fstat(descriptor)
            by_path = os.stat(
                child_name, dir_fd=parent_fd, follow_symlinks=False
            )
            if (
                not stat.S_ISDIR(details.st_mode)
                or stable(details) != stable(by_path)
            ):
                fail()
        except BaseException:
            try:
                os.close(descriptor)
            except BaseException:
                # The descriptor state is ambiguous after close(2) fails.  Do
                # not retry it, and never mask the primary validation/process
                # control exception.
                pass
            raise
        return descriptor, details

    def verify_directory_at(
        parent_fd: int, name: str, descriptor: int,
        original: os.stat_result,
    ) -> None:
        current = os.fstat(descriptor)
        by_path = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISDIR(current.st_mode)
            or stable(current) != stable(original)
            or stable(by_path) != stable(original)
        ):
            fail()

    def read_regular_at(
        parent_fd: int, name: str, limit: int
    ) -> Tuple[bytes, os.stat_result]:
        filename = basename(name)
        descriptor = os.open(
            filename,
            os.O_RDONLY
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        try:
            details = os.fstat(descriptor)
            by_path = os.stat(
                filename, dir_fd=parent_fd, follow_symlinks=False
            )
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_nlink != 1
                or details.st_size <= 0
                or details.st_size > limit
                or stable(details) != stable(by_path)
            ):
                fail()
            chunks = bytearray()
            while True:
                remaining = limit + 1 - len(chunks)
                chunk = os.read(descriptor, min(65536, remaining))
                if not chunk:
                    break
                chunks.extend(chunk)
                if len(chunks) > limit:
                    fail()
            final = os.fstat(descriptor)
            final_by_path = os.stat(
                filename, dir_fd=parent_fd, follow_symlinks=False
            )
            if (
                len(chunks) != details.st_size
                or stable(final) != stable(details)
                or stable(final_by_path) != stable(details)
            ):
                fail()
            result = (bytes(chunks), details)
        except BaseException:
            try:
                os.close(descriptor)
            except BaseException:
                pass
            raise
        try:
            os.close(descriptor)
        except BaseException:
            raise RouteCostCollectorError(failure) from None
        return result

    if not isinstance(cohort, Mapping) or set(cohort).isdisjoint({"legs"}):
        fail()
    run_id = basename(cohort.get("raw_evidence_run_id"))
    legs = cohort.get("legs")
    if not isinstance(legs, list):
        fail()
    expected = []
    try:
        try:
            from scripts.route_publication import _canonical_market_token
        except ModuleNotFoundError:  # pragma: no cover - direct script execution
            from route_publication import _canonical_market_token  # type: ignore
        for leg in legs:
            if not isinstance(leg, Mapping):
                fail()
            market_id = leg.get("market_id")
            market_type = leg.get("market_type")
            expected_prefix = {
                "cex": "cex:",
                "dex": "dex:",
            }.get(market_type)
            if (
                not isinstance(market_id, str)
                or expected_prefix is None
                or not market_id.startswith(expected_prefix)
            ):
                fail()
            _canonical_market_token(market_id)
            lineage = validate_typed_source_lineage(
                leg.get("typed_source_lineage"), market_type=market_type
            )
            observed = typed_source_lineage_observed_members(
                lineage, market_type=market_type
            )
            expected.extend({"market_id": market_id, **item} for item in observed)
    except (TypeError, ValueError, UnicodeError):
        fail()
    expected.sort(key=lambda item: (item["market_id"], item["role"]))
    keys = [(item["market_id"], item["role"]) for item in expected]
    filenames = [item["filename"] for item in expected]
    if len(keys) != len(set(keys)) or len(filenames) != len(set(filenames)):
        fail()

    try:
        raw_path = os.path.abspath(str(Path(data_dir)))
    except (TypeError, ValueError, OSError):
        fail()
    if sys.platform == "darwin":
        if raw_path == "/tmp" or raw_path.startswith("/tmp/"):
            raw_path = "/private" + raw_path
        elif raw_path == "/var" or raw_path.startswith("/var/"):
            raw_path = "/private" + raw_path
    root_parts = Path(raw_path).parts
    if not root_parts or root_parts[0] != os.sep:
        fail()
    components = list(root_parts[1:]) + ["raw", "route-cohort", run_id]
    held = []
    ancestry = []
    typed_fd = None
    try:
        root_fd = os.open(os.sep, _directory_flags())
        held.append(root_fd)
        parent = root_fd
        for component in components:
            child, details = open_directory_at(parent, component)
            held.append(child)
            ancestry.append((parent, component, child, details))
            parent = child
        run_fd = parent

        manifest_bytes, manifest_details = read_regular_at(
            run_fd, "typed-manifest.json", manifest_limit
        )
        try:
            manifest = json.loads(manifest_bytes.decode("utf-8"))
            canonical_manifest = canonical_json_bytes(manifest)
        except (UnicodeDecodeError, json.JSONDecodeError, RouteCostEvidenceError):
            fail()
        if (
            not isinstance(manifest, Mapping)
            or set(manifest) != TYPED_SOURCE_MANIFEST_FIELDS
            or manifest.get("schema") != TYPED_SOURCE_MANIFEST_SCHEMA
            or manifest.get("raw_evidence_run_id") != run_id
            or isinstance(manifest.get("member_count"), bool)
            or not isinstance(manifest.get("member_count"), int)
            or not isinstance(manifest.get("members"), list)
            or manifest["member_count"] != len(manifest["members"])
            or manifest["member_count"] != len(expected)
            or manifest_bytes != canonical_manifest
            or hashlib.sha256(manifest_bytes).hexdigest()
            != hashlib.sha256(canonical_manifest).hexdigest()
            or any(
                not isinstance(item, Mapping)
                or set(item) != TYPED_SOURCE_MANIFEST_MEMBER_FIELDS
                for item in manifest["members"]
            )
            or manifest["members"] != expected
        ):
            fail()

        typed_fd, typed_details = open_directory_at(run_fd, "typed")
        try:
            actual_filenames = sorted(os.listdir(typed_fd))
        except OSError:
            fail()
        if actual_filenames != sorted(filenames):
            fail()

        snapshots = []
        pending: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for item in expected:
            contract = TYPED_SOURCE_ROLE_CONTRACTS.get(item["role"])
            if contract is None:
                fail()
            payload, details = read_regular_at(
                typed_fd, item["filename"], contract["max_bytes"]
            )
            if (
                len(payload) != item["size"]
                or hashlib.sha256(payload).hexdigest() != item["sha256"]
                or hash_pattern.fullmatch(item["logical_generation"]) is None
                or (
                    item["role"] != "dex_pool_state"
                    and item["logical_generation"]
                    != hashlib.sha256(payload).hexdigest()
                )
            ):
                fail()
            descriptor_copy = json.loads(canonical_json_bytes(item).decode("utf-8"))
            if item["role"] == "dex_pool_state":
                validate_retained_v2_pool_state_member(
                    payload, descriptor=descriptor_copy
                )
            key = (item["market_id"], item["role"])
            keep = retain_keys is None or key in retain_keys
            snapshots.append((
                item["filename"],
                payload if keep else None,
                item["sha256"],
                item["size"],
                details,
                contract["max_bytes"],
            ))
            if keep:
                pending[key] = {
                    "descriptor": descriptor_copy,
                    "payload": payload,
                }

        for filename, payload, expected_sha, expected_size, details, limit in snapshots:
            reread_payload, reread_details = read_regular_at(
                typed_fd, filename, limit
            )
            if (
                len(reread_payload) != expected_size
                or hashlib.sha256(reread_payload).hexdigest() != expected_sha
                or (payload is not None and reread_payload != payload)
                or stable(reread_details) != stable(details)
            ):
                fail()
        reread_manifest, reread_manifest_details = read_regular_at(
            run_fd, "typed-manifest.json", manifest_limit
        )
        if (
            reread_manifest != manifest_bytes
            or stable(reread_manifest_details) != stable(manifest_details)
        ):
            fail()
        if sorted(os.listdir(typed_fd)) != sorted(filenames):
            fail()
        verify_directory_at(run_fd, "typed", typed_fd, typed_details)
        for ancestor_parent, component, descriptor, details in ancestry:
            verify_directory_at(
                ancestor_parent, component, descriptor, details
            )
        return pending
    except RouteCostCollectorError:
        raise
    except (OSError, TypeError, ValueError, UnicodeError, RouteCostEvidenceError):
        raise RouteCostCollectorError(failure) from None
    finally:
        active_error = sys.exc_info()[0] is not None
        cleanup_failed = False
        if typed_fd is not None:
            try:
                os.close(typed_fd)
            except OSError:
                cleanup_failed = True
        for descriptor in reversed(held):
            try:
                os.close(descriptor)
            except OSError:
                cleanup_failed = True
        if cleanup_failed and not active_error:
            raise RouteCostCollectorError(failure) from None


def _normalize_route_cost_cohort(
    cohort: Mapping[str, Any],
) -> Dict[str, Any]:
    """Reuse the one canonical cohort validator without leaking its errors."""
    try:
        try:
            from scripts.route_publication import (
                RoutePublicationError,
                _normalize_and_validate_cohort,
            )
        except ModuleNotFoundError:  # pragma: no cover - direct execution
            from route_publication import (  # type: ignore
                RoutePublicationError,
                _normalize_and_validate_cohort,
            )
        return _normalize_and_validate_cohort(cohort)
    except (RoutePublicationError, TypeError, ValueError, UnicodeError):
        raise RouteCostCollectorError(
            "route-cost cohort lineage is invalid"
        ) from None


def _validate_route_cost_universe(
    universe: Mapping[str, Any],
) -> Dict[str, Any]:
    """Replay the complete canonical route-universe contract."""
    try:
        try:
            from scripts.route_publication import (
                RoutePublicationError,
                _validate_route_universe_payload,
            )
        except ModuleNotFoundError:  # pragma: no cover - direct execution
            from route_publication import (  # type: ignore
                RoutePublicationError,
                _validate_route_universe_payload,
            )
        return _validate_route_universe_payload(universe)
    except (RoutePublicationError, TypeError, ValueError, UnicodeError):
        raise RouteCostCollectorError(
            "route-cost universe lineage is invalid"
        ) from None


def _evaluated_at_from_capability(capability: Any) -> str:
    """Take the runner's sole narrow monotonic-derived UTC sample."""
    if not callable(capability):
        raise RouteCostCollectorError("route-cost time capability is invalid")
    try:
        value = capability()
    except (TypeError, ValueError, OverflowError):
        raise RouteCostCollectorError(
            "route-cost time capability is invalid"
        ) from None
    if isinstance(value, str):
        text = value
    else:
        try:
            if (
                value.tzinfo is None
                or value.utcoffset() is None
            ):
                raise ValueError
            text = value.astimezone(__import__("datetime").timezone.utc).isoformat(
                timespec="microseconds"
            ).replace("+00:00", "Z")
            text = text.replace(".000000Z", "Z")
        except (AttributeError, TypeError, ValueError, OverflowError):
            raise RouteCostCollectorError(
                "route-cost time capability is invalid"
            ) from None
    # The evidence assembler remains the timestamp grammar authority.  This
    # local bound prevents arbitrary objects or secret-bearing exception text.
    if not isinstance(text, str) or not text or len(text) > 64:
        raise RouteCostCollectorError("route-cost time capability is invalid")
    return text


_RPC_BATCH_CALL_LIMIT = 40
_RPC_REQUEST_BYTES_LIMIT = 4 * 1024 * 1024
_RPC_RESPONSE_BYTES_LIMIT = 8 * 1024 * 1024
_FIXED_PHASE_A_TIMEOUT_SECONDS = 10
_PHASE_B_TIMEOUT_SECONDS = 35
_RPC_ERROR_MESSAGE_BYTES_LIMIT = 4 * 1024
_RPC_ERROR_DATA_BYTES_LIMIT = 256 * 1024


def _decode_rpc_batch_bytes(
    data: Any,
    planned_requests: List[Mapping[str, Any]],
    planned_roles: List[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Decode one exact Phase-A batch and project only evidence-bound results."""
    if not isinstance(data, bytes) or not data:
        raise RouteCostCollectorError("route-cost RPC response is invalid")
    if len(data) > _RPC_RESPONSE_BYTES_LIMIT:
        raise _resource_limit()
    def exact_float(token: str) -> Decimal:
        try:
            value = Decimal(token)
        except (InvalidOperation, ValueError):
            raise ValueError("number") from None
        if (
            not value.is_finite()
            or (value.is_zero() and value.is_signed())
        ):
            raise ValueError("number")
        return value

    def reject_constant(_token: str) -> None:
        raise ValueError("number")

    try:
        value = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_float=exact_float,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise RouteCostCollectorError("route-cost RPC response is invalid") from None
    if (
        not isinstance(planned_requests, list)
        or not isinstance(planned_roles, list)
        or len(planned_requests) != len(planned_roles)
        or not planned_requests
    ):
        raise RouteCostCollectorError("route-cost RPC plan is invalid")
    request_by_id: Dict[int, Mapping[str, Any]] = {}
    role_by_id: Dict[int, Mapping[str, Any]] = {}
    for request, role in zip(planned_requests, planned_roles):
        identifier = request.get("id") if isinstance(request, Mapping) else None
        if (
            type(identifier) is not int
            or identifier in request_by_id
            or not isinstance(role, Mapping)
            or set(role) != {"id", "role", "market_id"}
            or role.get("id") != identifier
            or not isinstance(role.get("role"), str)
            or (
                role.get("market_id") is not None
                and not isinstance(role.get("market_id"), str)
            )
        ):
            raise RouteCostCollectorError("route-cost RPC plan is invalid")
        request_by_id[identifier] = request
        role_by_id[identifier] = role
    if not isinstance(value, list) or len(value) != len(request_by_id):
        raise RouteCostCollectorError("route-cost RPC response is invalid")
    ids = []
    has_error = False
    for row in value:
        if (
            not isinstance(row, Mapping)
            or set(row) not in ({"jsonrpc", "id", "result"}, {"jsonrpc", "id", "error"})
            or row.get("jsonrpc") != "2.0"
            or type(row.get("id")) is not int
        ):
            raise RouteCostCollectorError("route-cost RPC response is invalid")
        ids.append(row["id"])
        if "error" in row:
            _validate_rpc_error_object(row["error"])
            has_error = True
    if set(ids) != set(request_by_id) or len(ids) != len(set(ids)):
        raise RouteCostCollectorError("route-cost RPC response is invalid")
    if has_error:
        raise RouteCostCollectorError("route-cost RPC unavailable")
    projected = []
    block_fields = (
        "number", "hash", "parentHash", "timestamp", "baseFeePerGas",
        "gasUsed", "gasLimit",
    )
    fee_fields = ("oldestBlock", "baseFeePerGas", "reward", "gasUsedRatio")
    for row in value:
        role = role_by_id[row["id"]]["role"]
        result = row["result"]
        if role == "block_header":
            if not isinstance(result, Mapping) or any(
                field not in result for field in block_fields
            ):
                raise RouteCostCollectorError("route-cost RPC response is invalid")
            result = {field: result[field] for field in block_fields}
        elif role == "fee_history":
            if not isinstance(result, Mapping) or any(
                field not in result for field in fee_fields
            ):
                raise RouteCostCollectorError("route-cost RPC response is invalid")
            result = {field: result[field] for field in fee_fields}
            ratios = result.get("gasUsedRatio")
            if not isinstance(ratios, list):
                raise RouteCostCollectorError(
                    "route-cost RPC response is invalid"
                )
            if any(
                isinstance(item, bool)
                or not isinstance(item, (int, float, Decimal))
                for item in ratios
            ):
                raise RouteCostCollectorError(
                    "route-cost RPC response is invalid"
                )
        projected.append({
            "jsonrpc": "2.0",
            "id": row["id"],
            "result": result,
        })
    return projected


def _validate_rpc_error_object(value: Any) -> None:
    if not isinstance(value, Mapping) or set(value) not in (
        {"code", "message"}, {"code", "message", "data"},
    ):
        raise RouteCostCollectorError("route-cost RPC response is invalid")
    code = value.get("code")
    message = value.get("message")
    if (
        type(code) is not int
        or not isinstance(message, str)
        or not message
    ):
        raise RouteCostCollectorError("route-cost RPC response is invalid")
    try:
        message_bytes = message.encode("utf-8")
    except UnicodeEncodeError:
        raise RouteCostCollectorError("route-cost RPC response is invalid") from None
    if len(message_bytes) > _RPC_ERROR_MESSAGE_BYTES_LIMIT:
        raise RouteCostCollectorError("route-cost RPC response is invalid")
    if "data" in value:
        try:
            data_bytes = canonical_json_bytes(value["data"])
        except RouteCostEvidenceError:
            raise RouteCostCollectorError(
                "route-cost RPC response is invalid"
            ) from None
        if len(data_bytes) > _RPC_ERROR_DATA_BYTES_LIMIT:
            raise RouteCostCollectorError("route-cost RPC response is invalid")


def _reject_duplicate_json_keys(pairs: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate")
        result[key] = value
    return result


_TRACE_ADDRESS = re.compile(r"0x[0-9A-Fa-f]{40}\Z", flags=re.ASCII)
_TRACE_WORD = re.compile(r"0x[0-9a-f]{64}\Z", flags=re.ASCII)
_TRACE_QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z", flags=re.ASCII)
_TRACE_ACCOUNT_FIELDS = frozenset(("balance", "nonce", "storage", "codeHash"))
_PHASE_B_SCENARIO_SPEC_FIELDS = frozenset((
    "schema", "market_id", "direction", "requested_notional_usd",
    "simulation_target_token_address", "simulation_target_unit_decimals",
    "simulation_target_raw_quantity", "simulation_target_lattice_raw",
    "simulation_target_sha256", "core_pool_state_id",
    "core_pool_state_sha256", "chain_evidence_sha256",
    "market_evidence_sha256", "quoted_amount_in_raw",
    "quoted_amount_out_raw", "submission_loss_bound_bps", "calldata_hex",
    "state_overrides", "estimate_request_id", "trace_request_id",
))
_ZERO_WORD = "0x" + "0" * 64


def _trace_invalid() -> RouteCostCollectorError:
    return RouteCostCollectorError("route-cost Phase B trace response is invalid")


def _trace_address(value: Any) -> str:
    if not isinstance(value, str) or _TRACE_ADDRESS.fullmatch(value) is None:
        raise _trace_invalid()
    return value.lower()


def _trace_account_map(value: Any) -> Dict[str, Dict[str, Any]]:
    """Validate one Geth pre/post account map and normalize EIP-55 keys."""
    if not isinstance(value, Mapping):
        raise _trace_invalid()
    normalized: Dict[str, Dict[str, Any]] = {}
    for raw_address, raw_account in value.items():
        address = _trace_address(raw_address)
        if address in normalized or not isinstance(raw_account, Mapping):
            raise _trace_invalid()
        if not set(raw_account).issubset(_TRACE_ACCOUNT_FIELDS):
            # ``disableCode=true`` forbids bytecode, while current Geth may
            # still return the fixed-width public ``codeHash`` metadata.
            raise _trace_invalid()
        if "balance" in raw_account and (
            not isinstance(raw_account["balance"], str)
            or _TRACE_QUANTITY.fullmatch(raw_account["balance"]) is None
        ):
            raise _trace_invalid()
        if "nonce" in raw_account and (
            type(raw_account["nonce"]) is not int or raw_account["nonce"] < 0
            or raw_account["nonce"] >= 2 ** 64
        ):
            raise _trace_invalid()
        if "codeHash" in raw_account and (
            not isinstance(raw_account["codeHash"], str)
            or _TRACE_WORD.fullmatch(raw_account["codeHash"]) is None
        ):
            raise _trace_invalid()
        storage = raw_account.get("storage", {})
        if not isinstance(storage, Mapping):
            raise _trace_invalid()
        checked_storage: Dict[str, str] = {}
        for key, word in storage.items():
            if (
                not isinstance(key, str) or _TRACE_WORD.fullmatch(key) is None
                or not isinstance(word, str) or _TRACE_WORD.fullmatch(word) is None
                or key in checked_storage
            ):
                raise _trace_invalid()
            checked_storage[key] = word
        normalized[address] = {"storage": checked_storage}
    return normalized


def _decode_phase_b_trace_batch(
    response_bytes: Any,
    *,
    trace_requests: Any,
    scenario_specs_by_trace_id: Any,
    adapter: Any,
    market_evidence_by_id: Any,
    fixed_block_tag: Any,
) -> List[Dict[str, Any]]:
    """Project a bounded real Geth prestateTracer diff-mode response batch."""
    if not isinstance(response_bytes, bytes) or not response_bytes:
        raise _trace_invalid()
    if len(response_bytes) > _RPC_RESPONSE_BYTES_LIMIT:
        raise _resource_limit()
    _preflight_json_bytes(
        response_bytes,
        node_limit=1_048_576,
        ordinary_string_limit=256 * 1024,
        scalar_limit=64 * 1024 * 1024,
    )
    try:
        decoded_batch = json.loads(
            response_bytes.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=lambda _token: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise _trace_invalid() from None
    _bounded_json_shape(
        decoded_batch, node_limit=1_048_576,
        ordinary_string_limit=256 * 1024,
    )

    if (
        not isinstance(trace_requests, list) or not trace_requests
        or not isinstance(scenario_specs_by_trace_id, Mapping)
        or not isinstance(adapter, Mapping)
        or not isinstance(market_evidence_by_id, Mapping)
        or not isinstance(fixed_block_tag, str)
        or _TRACE_QUANTITY.fullmatch(fixed_block_tag) is None
        or fixed_block_tag == "0x0"
    ):
        raise _trace_invalid()
    if any(
        type(identifier) is not int or identifier <= 0
        for identifier in scenario_specs_by_trace_id
    ):
        raise _trace_invalid()
    sender = _trace_address(adapter.get("simulation_sender_address"))
    router = _trace_address(adapter.get("router_address"))
    descriptor_by_token: Dict[str, Mapping[str, Any]] = {}
    descriptors = adapter.get("token_funding_descriptors")
    if not isinstance(descriptors, list):
        raise _trace_invalid()
    for descriptor in descriptors:
        if not isinstance(descriptor, Mapping):
            raise _trace_invalid()
        token = _trace_address(descriptor.get("token_address"))
        if token in descriptor_by_token:
            raise _trace_invalid()
        for field in ("balance_mapping_slot", "allowance_mapping_slot"):
            raw_slot = descriptor.get(field)
            if (
                not isinstance(raw_slot, str)
                or re.fullmatch(r"(?:0|[1-9][0-9]*)", raw_slot) is None
                or int(raw_slot) >= 2 ** 256
            ):
                raise _trace_invalid()
        descriptor_by_token[token] = descriptor

    plans: Dict[int, Tuple[Mapping[str, Any], List[Tuple[str, str, str]]]] = {}
    order: List[int] = []
    for request in trace_requests:
        if (
            not isinstance(request, Mapping)
            or set(request) != {"schema", "jsonrpc", "id", "method", "params"}
            or request.get("schema") != "route_cost_trace_request/v1"
            or request.get("jsonrpc") != "2.0"
            or request.get("method") != "debug_traceCall"
            or type(request.get("id")) is not int or request["id"] <= 0
            or request["id"] in plans
        ):
            raise _trace_invalid()
        identifier = request["id"]
        spec = scenario_specs_by_trace_id.get(identifier)
        if (
            not isinstance(spec, Mapping)
            or set(spec) != _PHASE_B_SCENARIO_SPEC_FIELDS
            or spec.get("schema") != "route_cost_phase_b_scenario_spec/v1"
            or spec.get("trace_request_id") != identifier
            or not isinstance(spec.get("market_id"), str)
            or spec.get("direction") not in {"buy", "sell"}
            or spec.get("requested_notional_usd") not in {
                "1000", "5000", "10000", "50000", "100000",
            }
            or not isinstance(spec.get("simulation_target_token_address"), str)
            or _TRACE_ADDRESS.fullmatch(
                spec["simulation_target_token_address"]
            ) is None
            or not isinstance(spec.get("simulation_target_unit_decimals"), str)
            or re.fullmatch(r"(?:0|[1-9][0-9]*)", spec[
                "simulation_target_unit_decimals"
            ]) is None
            or any(
                not isinstance(spec.get(field), str)
                or re.fullmatch(r"[1-9][0-9]*", spec[field]) is None
                for field in (
                    "simulation_target_raw_quantity",
                    "simulation_target_lattice_raw",
                    "quoted_amount_in_raw", "quoted_amount_out_raw",
                )
            )
            or not isinstance(spec.get("submission_loss_bound_bps"), str)
            or re.fullmatch(r"(?:0|[1-9][0-9]*)", spec[
                "submission_loss_bound_bps"
            ]) is None
            or any(
                not isinstance(spec.get(field), str)
                or re.fullmatch(r"[0-9a-f]{64}", spec[field]) is None
                for field in (
                    "simulation_target_sha256", "core_pool_state_sha256",
                    "chain_evidence_sha256", "market_evidence_sha256",
                )
            )
            or not isinstance(spec.get("core_pool_state_id"), str)
            or not spec["core_pool_state_id"]
            or type(spec.get("estimate_request_id")) is not int
            or spec["estimate_request_id"] <= 0
            or not isinstance(spec.get("state_overrides"), Mapping)
        ):
            raise _trace_invalid()
        try:
            _validate_state_overrides(
                spec["state_overrides"],
                calldata=spec["calldata_hex"],
                adapter=adapter,
            )
        except RouteCostEvidenceError:
            raise _trace_invalid() from None
        params = request.get("params")
        if not isinstance(params, list) or len(params) != 3:
            raise _trace_invalid()
        call, block_tag, options = params
        if (
            not isinstance(call, Mapping)
            or set(call) != {"from", "to", "gas", "data", "value"}
            or _trace_address(call.get("from")) != sender
            or _trace_address(call.get("to")) != router
            or not isinstance(call.get("gas"), str)
            or _TRACE_QUANTITY.fullmatch(call["gas"]) is None
            or call["gas"] == "0x0"
            or call.get("data") != spec.get("calldata_hex")
            or not isinstance(call.get("data"), str)
            or call.get("value") != "0x0"
            or not isinstance(block_tag, str)
            or _TRACE_QUANTITY.fullmatch(block_tag) is None
            or block_tag == "0x0"
            or block_tag != fixed_block_tag
            or not isinstance(options, Mapping)
            or set(options) != {"tracer", "tracerConfig", "stateOverrides"}
            or options.get("tracer") != "prestateTracer"
            or options.get("tracerConfig") != {
                "diffMode": True, "disableCode": True,
                "disableStorage": False,
            }
            or options.get("stateOverrides") != spec["state_overrides"]
        ):
            raise _trace_invalid()
        try:
            calldata = decode_v2_swap_calldata(spec.get("calldata_hex"))
        except RouteCostEvidenceError:
            raise _trace_invalid() from None
        path = calldata.get("path")
        quoted_in = int(spec["quoted_amount_in_raw"])
        quoted_out = int(spec["quoted_amount_out_raw"])
        submission_bound = int(spec["submission_loss_bound_bps"])
        if submission_bound > 10000:
            raise _trace_invalid()
        if (
            not isinstance(path, list) or len(path) != 2
            or calldata.get("recipient") != sender
            or calldata.get("direction") != spec["direction"]
            or spec["simulation_target_token_address"]
            != (path[1] if spec["direction"] == "buy" else path[0])
            or (
                spec["direction"] == "buy"
                and (
                    calldata.get("amount_out_raw") != quoted_out
                    or calldata.get("amount_in_max_raw")
                    != (
                        quoted_in * (10000 + submission_bound) + 9999
                    ) // 10000
                )
            )
            or (
                spec["direction"] == "sell"
                and (
                    calldata.get("amount_in_raw") != quoted_in
                    or calldata.get("amount_out_min_raw")
                    != quoted_out * (10000 - submission_bound) // 10000
                )
            )
        ):
            raise _trace_invalid()
        token_in, token_out = (_trace_address(path[0]), _trace_address(path[1]))
        if token_in not in descriptor_by_token or token_out not in descriptor_by_token:
            raise _trace_invalid()
        market = market_evidence_by_id.get(spec["market_id"])
        if not isinstance(market, Mapping):
            raise _trace_invalid()
        pair = _trace_address(market.get("pair_address"))
        pair_descriptors = adapter.get("pair_descriptors")
        if not isinstance(pair_descriptors, list):
            raise _trace_invalid()
        matching_pairs = [row for row in pair_descriptors if (
            isinstance(row, Mapping)
            and isinstance(row.get("pair_address"), str)
            and _TRACE_ADDRESS.fullmatch(row["pair_address"]) is not None
            and row["pair_address"].lower() == pair
        )]
        if len(matching_pairs) != 1:
            raise _trace_invalid()
        pair_tokens = {
            _trace_address(matching_pairs[0].get("token0_address")),
            _trace_address(matching_pairs[0].get("token1_address")),
        }
        if set((token_in, token_out)) != pair_tokens:
            raise _trace_invalid()
        descriptor_in = descriptor_by_token[token_in]
        descriptor_out = descriptor_by_token[token_out]
        planned = [
            (token_in, "sender", solidity_balance_storage_key(
                sender, int(descriptor_in["balance_mapping_slot"]))),
            (token_in, "sender", solidity_allowance_storage_key(
                sender, router, int(descriptor_in["allowance_mapping_slot"]))),
            (token_in, "pair", solidity_balance_storage_key(
                pair, int(descriptor_in["balance_mapping_slot"]))),
            (token_out, "pair", solidity_balance_storage_key(
                pair, int(descriptor_out["balance_mapping_slot"]))),
            (token_out, "recipient", solidity_balance_storage_key(
                sender, int(descriptor_out["balance_mapping_slot"]))),
        ]
        if len(set(planned)) != 5:
            raise _trace_invalid()
        plans[identifier] = (spec, planned)
        order.append(identifier)
    if set(scenario_specs_by_trace_id) != set(order):
        raise _trace_invalid()
    if not isinstance(decoded_batch, list) or len(decoded_batch) != len(order):
        raise _trace_invalid()
    response_by_id: Dict[int, Mapping[str, Any]] = {}
    has_error = False
    for response in decoded_batch:
        if (
            not isinstance(response, Mapping)
            or set(response) not in (
                {"jsonrpc", "id", "result"}, {"jsonrpc", "id", "error"}
            )
            or response.get("jsonrpc") != "2.0"
            or type(response.get("id")) is not int
            or response["id"] in response_by_id
        ):
            raise _trace_invalid()
        if "error" in response:
            try:
                _validate_rpc_error_object(response["error"])
            except RouteCostCollectorError:
                raise _trace_invalid() from None
            has_error = True
        response_by_id[response["id"]] = response
    if set(response_by_id) != set(order):
        raise _trace_invalid()
    if has_error:
        raise RouteCostCollectorError("route-cost RPC unavailable")

    projected: List[Dict[str, Any]] = []
    for identifier in order:
        result = response_by_id[identifier]["result"]
        if not isinstance(result, Mapping) or set(result) != {"pre", "post"}:
            raise _trace_invalid()
        pre = _trace_account_map(result["pre"])
        post = _trace_account_map(result["post"])
        _spec, planned = plans[identifier]
        planned_by_token = {
            token: {key for candidate, _role, key in planned if candidate == token}
            for token, _role, _key in planned
        }
        for token, planned_keys in planned_by_token.items():
            observed_keys = set(pre.get(token, {}).get("storage", {})) | set(
                post.get(token, {}).get("storage", {})
            )
            if observed_keys - planned_keys:
                raise _trace_invalid()
        diffs = []
        for token, role, key in planned:
            pre_storage = pre.get(token, {}).get("storage", {})
            post_storage = post.get(token, {}).get("storage", {})
            pre_present, post_present = key in pre_storage, key in post_storage
            if not pre_present and not post_present:
                raise _trace_invalid()
            pre_word = pre_storage.get(key, _ZERO_WORD)
            post_word = post_storage.get(key, _ZERO_WORD)
            if pre_present and post_present and pre_word == post_word:
                raise _trace_invalid()
            diffs.append({
                "token_address": token, "account_role": role,
                "storage_key": key,
                "pre_present": pre_present, "pre_value": pre_word,
                "post_present": post_present, "post_value": post_word,
            })
        diffs.sort(key=lambda row: (
            row["token_address"], row["account_role"], row["storage_key"]
        ))
        projected.append({
            "schema": "route_cost_trace_response/v1", "jsonrpc": "2.0",
            "id": identifier, "storage_diffs": diffs,
        })
    return projected


def _production_rpc_batch(
    profile: Mapping[str, Any], request_bytes: bytes, timeout_seconds: float,
) -> bytes:
    if len(request_bytes) > _RPC_REQUEST_BYTES_LIMIT:
        raise _resource_limit()
    request = urllib.request.Request(
        profile["rpc_url"],
        data=request_bytes,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": profile["authorization"],
        },
    )
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), _NoRedirectHandler()
    )
    opener.addheaders = []
    deadline = time.monotonic() + timeout_seconds
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            value = _decode_bounded_json_response(
                response,
                wire_limit=_RPC_RESPONSE_BYTES_LIMIT,
                decoded_limit=_RPC_RESPONSE_BYTES_LIMIT,
                scalar_limit=64 * 1024 * 1024,
                node_limit=1_048_576,
                ordinary_string_limit=256 * 1024,
                absolute_deadline=deadline,
                return_decoded_bytes=True,
            )
    except urllib.error.HTTPError as error:
        if error.code == 429 or 500 <= error.code <= 599:
            raise _RpcUnavailableError() from None
        raise RouteCostCollectorError("route-cost RPC response is invalid") from None
    except (TimeoutError, OSError, urllib.error.URLError):
        raise _RpcUnavailableError() from None
    except RouteCostCollectorError as error:
        if str(error) == "route-cost response stream is unavailable":
            raise _RpcUnavailableError() from None
        raise
    # Preserve the validated physical response bytes.  The downstream batch
    # decoder retains exact Decimal tokens until it projects the only numeric
    # field that enters Phase-A evidence.
    return value


@dataclass(frozen=True)
class _PhaseACaptureResult:
    terminal_reasons: Mapping[str, str]
    phase_a_capture: Optional[Mapping[str, Any]] = field(
        default=None, repr=False
    )
    native_capture: Optional[_NativePriceCaptureResult] = field(
        default=None, repr=False
    )
    captured_finished_at: Optional[str] = None
    fixed_block_tag: Optional[str] = None


def _capture_phase_a_result(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    capture_utc_anchor: str,
    supported: Tuple[str, ...],
    retained_pool_members: Mapping[str, Mapping[str, Any]],
    adapter_registry: Mapping[str, Any],
    profiles: RouteCostProfileCapture,
    capability: Any,
) -> _PhaseACaptureResult:
    reasons = {
        market_id: "core_pool_state_unavailable"
        for market_id in supported
        if market_id not in retained_pool_members
    }
    retained_ids = tuple(
        market_id for market_id in supported
        if market_id in retained_pool_members
    )
    if not retained_ids:
        return _PhaseACaptureResult(terminal_reasons=reasons)
    if profiles.trace_profile is None:
        reasons.update({market_id: "trace_profile_missing" for market_id in retained_ids})
        return _PhaseACaptureResult(terminal_reasons=reasons)
    retained_subset = {
        market_id: retained_pool_members[market_id] for market_id in retained_ids
    }
    try:
        plan = build_fixed_block_phase_a_request_plan(
            universe=universe,
            adapter_registry=adapter_registry,
            retained_typed_pool_state_members=retained_subset,
        )
    except RouteCostEvidenceError as error:
        if str(error) == "fixed-block Phase A retained anchor differs":
            reasons.update({
                market_id: "fixed_block_mismatch" for market_id in retained_ids
            })
            return _PhaseACaptureResult(terminal_reasons=reasons)
        raise
    requests = plan["requests"]
    request_roles = plan["request_roles"]
    rpc_batch = getattr(capability, "rpc_batch", None)
    monotonic = getattr(capability, "monotonic", None)
    if not callable(rpc_batch):
        rpc_batch = lambda body, timeout_seconds: _production_rpc_batch(
            profiles.trace_profile, body, timeout_seconds
        )
    if not callable(monotonic):
        monotonic = time.monotonic

    def sample_monotonic(previous: Optional[float] = None) -> float:
        try:
            sample = monotonic()
        except (TypeError, ValueError, OverflowError):
            raise RouteCostCollectorError(
                "route-cost monotonic capability is invalid"
            ) from None
        if (
            isinstance(sample, bool)
            or not isinstance(sample, (int, float))
            or not __import__("math").isfinite(sample)
            or (previous is not None and sample < previous)
        ):
            raise RouteCostCollectorError(
                "route-cost monotonic capability is invalid"
            )
        return float(sample)

    started = sample_monotonic()
    deadline = started + _FIXED_PHASE_A_TIMEOUT_SECONDS
    previous_sample = started
    terminal_reason: Optional[str] = None
    captured_rows: List[Dict[str, Any]] = []
    for offset in range(0, len(requests), _RPC_BATCH_CALL_LIMIT):
        batch = requests[offset:offset + _RPC_BATCH_CALL_LIMIT]
        batch_ids = {row["id"] for row in batch}
        role_batch = [
            row for row in request_roles if row["id"] in batch_ids
        ]
        request_bytes = canonical_json_bytes(batch)
        if len(request_bytes) > _RPC_REQUEST_BYTES_LIMIT:
            raise _resource_limit()
        current = sample_monotonic(previous_sample)
        previous_sample = current
        remaining = deadline - current
        if remaining <= 0:
            terminal_reason = "rpc_unavailable"
            break
        try:
            response_bytes = rpc_batch(
                request_bytes, timeout_seconds=remaining
            )
        except (
            _RpcUnavailableError, TimeoutError, OSError,
            urllib.error.URLError, urllib.error.HTTPError,
        ):
            terminal_reason = "rpc_unavailable"
            break
        except RouteCostCollectorError as error:
            if str(error) == str(_resource_limit()):
                raise
            terminal_reason = "rpc_invalid"
            break
        try:
            decoded_rows = _decode_rpc_batch_bytes(
                response_bytes, batch, role_batch
            )
        except RouteCostCollectorError as error:
            if str(error) == "route-cost RPC unavailable":
                terminal_reason = "rpc_unavailable"
                break
            terminal_reason = "rpc_invalid"
            break
        captured_rows.extend(decoded_rows)
    if terminal_reason is None:
        finished = sample_monotonic(previous_sample)
        if finished > deadline:
            terminal_reason = "rpc_unavailable"
    if terminal_reason is None:
        elapsed = finished - started
        try:
            anchor = datetime.fromisoformat(
                capture_utc_anchor.replace("Z", "+00:00")
            )
            captured_finished_at = (
                anchor + timedelta(seconds=elapsed)
            ).isoformat(timespec="microseconds").replace("+00:00", "Z")
            captured_finished_at = captured_finished_at.replace(".000000Z", "Z")
        except (AttributeError, TypeError, ValueError, OverflowError):
            raise RouteCostCollectorError(
                "route-cost time capability is invalid"
            ) from None
        try:
            phase_a_capture = project_fixed_block_phase_a_capture(
                universe=universe,
                plan=plan,
                responses=captured_rows,
                run_id=run_id,
                route_cohort_id=route_cohort_id,
                candidate_source_generation=candidate_source_generation,
                route_universe_sha256=route_universe_sha256,
                trace_profile_identity=dict(profiles.trace_profile_identity),
                adapter_registry=adapter_registry,
                retained_typed_pool_state_members=retained_subset,
                captured_started_at=capture_utc_anchor,
                captured_finished_at=captured_finished_at,
            )
        except RouteCostEvidenceError:
            raise RouteCostCollectorError(
                "route-cost fixed-block Phase A projection is invalid"
            ) from None
        native_capture = _capture_native_price_evidence(
            run_id=run_id,
            route_cohort_id=route_cohort_id,
            candidate_source_generation=candidate_source_generation,
            capture_utc_anchor=captured_finished_at,
        )
        return _PhaseACaptureResult(
            terminal_reasons=reasons,
            phase_a_capture=phase_a_capture,
            native_capture=native_capture,
            captured_finished_at=captured_finished_at,
            fixed_block_tag=plan["block_tag"],
        )
    reasons.update({market_id: terminal_reason for market_id in retained_ids})
    return _PhaseACaptureResult(terminal_reasons=reasons)


def _phase_a_terminal_reasons(**arguments: Any) -> Dict[str, str]:
    """Compatibility wrapper for the closed terminal Phase-A scheduler."""
    result = _capture_phase_a_result(**arguments)
    if result.phase_a_capture is not None:
        raise RouteCostCollectorError(
            "configured trace Phase B projection is not implemented"
        )
    return dict(result.terminal_reasons)


def _phase_b_invalid() -> RouteCostCollectorError:
    return RouteCostCollectorError("route-cost Phase B RPC response is invalid")


def _decode_phase_b_estimate_batch(
    response_bytes: Any, planned_requests: List[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Decode one exact estimate batch without releasing partial evidence."""
    if not isinstance(response_bytes, bytes) or not response_bytes:
        raise _phase_b_invalid()
    if len(response_bytes) > _RPC_RESPONSE_BYTES_LIMIT:
        raise _resource_limit()
    _preflight_json_bytes(
        response_bytes,
        node_limit=1_048_576,
        ordinary_string_limit=256 * 1024,
        scalar_limit=64 * 1024 * 1024,
    )
    try:
        rows = json.loads(
            response_bytes.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=lambda _token: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise _phase_b_invalid() from None
    if not isinstance(planned_requests, list) or not planned_requests:
        raise _phase_b_invalid()
    expected = []
    for request in planned_requests:
        if (
            not isinstance(request, Mapping)
            or request.get("schema") != "route_cost_estimate_gas_request/v1"
            or request.get("jsonrpc") != "2.0"
            or request.get("method") != "eth_estimateGas"
            or type(request.get("id")) is not int
            or request["id"] <= 0
            or request["id"] in expected
        ):
            raise _phase_b_invalid()
        expected.append(request["id"])
    if not isinstance(rows, list) or len(rows) != len(expected):
        raise _phase_b_invalid()
    by_id: Dict[int, Dict[str, Any]] = {}
    has_error = False
    for row in rows:
        if (
            not isinstance(row, Mapping)
            or set(row) not in (
                {"jsonrpc", "id", "result"},
                {"jsonrpc", "id", "error"},
            )
            or row.get("jsonrpc") != "2.0"
            or type(row.get("id")) is not int
            or row["id"] in by_id
        ):
            raise _phase_b_invalid()
        if "error" in row:
            try:
                _validate_rpc_error_object(row["error"])
            except RouteCostCollectorError:
                raise _phase_b_invalid() from None
            has_error = True
        by_id[row["id"]] = dict(row)
    if set(by_id) != set(expected):
        raise _phase_b_invalid()
    if has_error:
        raise RouteCostCollectorError("route-cost RPC unavailable")
    return [by_id[identifier] for identifier in expected]


def _wire_rpc_request(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: row[key]
        for key in ("jsonrpc", "id", "method", "params")
    }


def _phase_b_rpc_batch(
    requests: List[Mapping[str, Any]],
    *,
    rpc_batch: Any,
    sample_monotonic: Any,
    deadline: float,
    previous_sample: float,
) -> Tuple[bytes, float]:
    current = sample_monotonic(previous_sample)
    remaining = deadline - current
    if remaining <= 0:
        raise RouteCostCollectorError("route-cost Phase B RPC unavailable")
    request_bytes = canonical_json_bytes([
        _wire_rpc_request(row) for row in requests
    ])
    if len(request_bytes) > _RPC_REQUEST_BYTES_LIMIT:
        raise _resource_limit()
    try:
        response_bytes = rpc_batch(request_bytes, timeout_seconds=remaining)
    except (
        _RpcUnavailableError, TimeoutError, OSError,
        urllib.error.URLError, urllib.error.HTTPError,
    ):
        raise RouteCostCollectorError(
            "route-cost Phase B RPC unavailable"
        ) from None
    return response_bytes, current


def _capture_phase_b(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    phase: str,
    adapter_registry: Mapping[str, Any],
    connector_key_registry: Mapping[str, Any],
    profiles: RouteCostProfileCapture,
    retained_pool_members: Mapping[str, Mapping[str, Any]],
    phase_a_result: _PhaseACaptureResult,
    policy_snapshot: Mapping[str, Any],
    capability: Any,
) -> Dict[str, Any]:
    native_capture = phase_a_result.native_capture
    if (
        phase_a_result.phase_a_capture is None
        or phase_a_result.captured_finished_at is None
        or phase_a_result.fixed_block_tag is None
        or native_capture is None
    ):
        raise RouteCostCollectorError("route-cost Phase B inputs are invalid")
    if native_capture.status != "observed" or native_capture.evidence is None:
        raise RouteCostCollectorError(
            "route-cost Phase B native price is unavailable"
        )
    native = native_capture.evidence
    terminal_core_reasons = dict(phase_a_result.terminal_reasons)
    try:
        bound = bind_native_price_to_phase_a_capture(
            universe=universe,
            phase_a_capture=phase_a_result.phase_a_capture,
            native_price_evidence=native,
            run_id=run_id,
            route_cohort_id=route_cohort_id,
            candidate_source_generation=candidate_source_generation,
            route_universe_sha256=route_universe_sha256,
            trace_profile_identity=dict(profiles.trace_profile_identity),
            adapter_registry=adapter_registry,
            retained_typed_pool_state_members=retained_pool_members,
        )
        scenario_plan = build_phase_b_scenario_request_plan(
            universe=universe,
            run_id=run_id,
            route_cohort_id=route_cohort_id,
            candidate_source_generation=candidate_source_generation,
            route_universe_sha256=route_universe_sha256,
            adapter_registry=adapter_registry,
            trace_profile_identity=dict(profiles.trace_profile_identity),
            submission_connector_profile_identity=dict(
                profiles.submission_connector_profile_identity
            ),
            retained_typed_pool_state_members=retained_pool_members,
            native_price_evidence=native,
            submission_policy_snapshot=policy_snapshot,
            native_bound_phase_a_capture=bound,
            terminal_reason_by_market=terminal_core_reasons,
        )
    except RouteCostEvidenceError:
        raise RouteCostCollectorError(
            "route-cost Phase B planning is invalid"
        ) from None

    rpc_batch = getattr(capability, "rpc_batch", None)
    monotonic = getattr(capability, "monotonic", None)
    if not callable(rpc_batch):
        rpc_batch = lambda body, timeout_seconds: _production_rpc_batch(
            profiles.trace_profile, body, timeout_seconds
        )
    if not callable(monotonic):
        monotonic = time.monotonic

    def sample(previous: Optional[float] = None) -> float:
        try:
            value = monotonic()
        except (TypeError, ValueError, OverflowError):
            raise RouteCostCollectorError(
                "route-cost monotonic capability is invalid"
            ) from None
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not __import__("math").isfinite(value)
            or (previous is not None and value < previous)
        ):
            raise RouteCostCollectorError(
                "route-cost monotonic capability is invalid"
            )
        return float(value)

    started = sample()
    deadline = started + _PHASE_B_TIMEOUT_SECONDS
    previous = started
    estimate_responses: List[Dict[str, Any]] = []
    estimates = scenario_plan["estimate_requests"]
    for offset in range(0, len(estimates), _RPC_BATCH_CALL_LIMIT):
        batch = estimates[offset:offset + _RPC_BATCH_CALL_LIMIT]
        response_bytes, previous = _phase_b_rpc_batch(
            batch, rpc_batch=rpc_batch, sample_monotonic=sample,
            deadline=deadline, previous_sample=previous,
        )
        estimate_responses.extend(
            _decode_phase_b_estimate_batch(response_bytes, batch)
        )
    try:
        trace_plan = build_phase_b_trace_request_plan(
            scenario_plan=scenario_plan,
            estimate_responses=estimate_responses,
        )
    except RouteCostEvidenceError:
        raise _phase_b_invalid() from None

    specs_by_id = {
        row["trace_request_id"]: row
        for row in scenario_plan["scenario_specs"]
    }
    markets_by_id = {
        row["market_id"]: row for row in bound["market_evidence"]
    }
    adapter_rows = adapter_registry.get("adapters", [])
    if not isinstance(adapter_rows, list) or len(adapter_rows) != 1:
        raise RouteCostCollectorError("route-cost Phase B adapter is invalid")
    trace_responses: List[Dict[str, Any]] = []
    traces = trace_plan["trace_requests"]
    for offset in range(0, len(traces), _RPC_BATCH_CALL_LIMIT):
        batch = traces[offset:offset + _RPC_BATCH_CALL_LIMIT]
        response_bytes, previous = _phase_b_rpc_batch(
            batch, rpc_batch=rpc_batch, sample_monotonic=sample,
            deadline=deadline, previous_sample=previous,
        )
        batch_specs = {row["id"]: specs_by_id[row["id"]] for row in batch}
        trace_responses.extend(_decode_phase_b_trace_batch(
            response_bytes,
            trace_requests=batch,
            scenario_specs_by_trace_id=batch_specs,
            adapter=adapter_rows[0],
            market_evidence_by_id=markets_by_id,
            fixed_block_tag=phase_a_result.fixed_block_tag,
        ))
    finished = sample(previous)
    if finished > deadline:
        raise RouteCostCollectorError("route-cost Phase B RPC unavailable")
    elapsed = finished - started
    captured_started_at = native["observed_at"]
    captured_finished_at = _native_capture_timestamp(
        captured_started_at, elapsed
    )
    try:
        transcripts = project_phase_b_capture(
            universe=universe,
            run_id=run_id,
            route_cohort_id=route_cohort_id,
            candidate_source_generation=candidate_source_generation,
            route_universe_sha256=route_universe_sha256,
            adapter_registry=adapter_registry,
            trace_profile_identity=dict(profiles.trace_profile_identity),
            submission_connector_profile_identity=dict(
                profiles.submission_connector_profile_identity
            ),
            retained_typed_pool_state_members=retained_pool_members,
            native_price_evidence=native,
            submission_policy_snapshot=policy_snapshot,
            native_bound_phase_a_capture=bound,
            scenario_plan=scenario_plan,
            trace_plan=trace_plan,
            trace_responses=trace_responses,
            captured_started_at=captured_started_at,
            captured_finished_at=captured_finished_at,
            terminal_reason_by_market=terminal_core_reasons,
        )
        selected = build_selected_markets(universe, adapter_registry)
        eligible_ids = set(retained_pool_members)
        selected_ids = {row["market_id"] for row in selected}
        supported_ids = {
            row["market_id"] for row in selected
            if row["structural_support_status"] == "supported"
        }
        if selected_ids - eligible_ids:
            terminal_rows = build_terminal_transcript_inventory(
                universe=universe,
                run_id=run_id,
                route_cohort_id=route_cohort_id,
                candidate_source_generation=candidate_source_generation,
                route_universe_sha256=route_universe_sha256,
                adapter_registry=adapter_registry,
                trace_profile_identity=dict(profiles.trace_profile_identity),
                submission_connector_profile_identity=dict(
                    profiles.submission_connector_profile_identity
                ),
                retained_typed_pool_state_members=retained_pool_members,
                terminal_reason_by_market={
                    market_id: terminal_core_reasons.get(
                        market_id, "rpc_unavailable"
                    )
                    for market_id in supported_ids
                },
            )
            transcripts.extend(
                row for row in terminal_rows
                if row["market_id"] not in eligible_ids
            )
        evaluated_at = _evaluated_at_from_capability(capability)
        return build_route_cost_evidence_manifest_from_captured(
            universe=universe,
            run_id=run_id,
            route_cohort_id=route_cohort_id,
            phase=phase,
            candidate_source_generation=candidate_source_generation,
            route_universe_sha256=route_universe_sha256,
            evaluated_at=evaluated_at,
            adapter_registry=adapter_registry,
            connector_key_registry=connector_key_registry,
            trace_profile_identity=dict(profiles.trace_profile_identity),
            submission_connector_profile_identity=dict(
                profiles.submission_connector_profile_identity
            ),
            native_price_evidence=native,
            chain_evidence=bound["chain_evidence"],
            market_evidence=bound["market_evidence"],
            transcripts=transcripts,
            submission_policy_snapshot=policy_snapshot,
        )
    except RouteCostEvidenceError:
        raise RouteCostCollectorError(
            "route-cost Phase B projection is invalid"
        ) from None


def _terminal_manifest(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    phase: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    adapter_registry: Mapping[str, Any],
    connector_key_registry: Mapping[str, Any],
    profiles: RouteCostProfileCapture,
    retained_pool_members: Mapping[str, Mapping[str, Any]],
    supported: Tuple[str, ...],
    capability: Any,
) -> Dict[str, Any]:
    evaluated_at = _evaluated_at_from_capability(capability)
    policy_scope_nonempty = bool(build_submission_policy_scope(
        universe=universe, adapter_registry=adapter_registry
    ))
    if policy_scope_nonempty and profiles.connector_profile is not None:
        # A configured connector is a distinct production authority.  Until
        # its single bounded policy-batch path is implemented, fail before
        # releasing any Phase-A RPC or native-price network request.
        raise RouteCostCollectorError(
            "configured submission connector collection is not implemented"
        )
    if not supported:
        reasons: Dict[str, str] = {}
        phase_a_result: Optional[_PhaseACaptureResult] = None
    else:
        phase_a_result = _capture_phase_a_result(
            universe=universe,
            run_id=run_id,
            route_cohort_id=route_cohort_id,
            candidate_source_generation=candidate_source_generation,
            route_universe_sha256=route_universe_sha256,
            capture_utc_anchor=evaluated_at,
            supported=supported,
            retained_pool_members=retained_pool_members,
            adapter_registry=adapter_registry,
            profiles=profiles,
            capability=capability,
        )
        reasons = dict(phase_a_result.terminal_reasons)
    if not policy_scope_nonempty:
        policy_reason = "scope_empty"
    else:
        policy_reason = "submission_connector_missing"
    snapshot = build_terminal_submission_policy_snapshot(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        connector_key_registry=connector_key_registry,
        trace_profile_identity=dict(profiles.trace_profile_identity),
        submission_connector_profile_identity=(
            dict(profiles.submission_connector_profile_identity)
        ),
        reason_code=policy_reason,
    )
    if phase_a_result is not None and phase_a_result.phase_a_capture is not None:
        native_capture = phase_a_result.native_capture
        if (
            native_capture is not None
            and native_capture.status in {"unavailable", "failed"}
            and native_capture.evidence is None
            and native_capture.reason_code in {
                "native_price_unavailable", "native_price_invalid",
            }
        ):
            try:
                projected = project_native_price_terminal_phase_a_capture(
                    universe=universe,
                    phase_a_capture=phase_a_result.phase_a_capture,
                    run_id=run_id,
                    route_cohort_id=route_cohort_id,
                    candidate_source_generation=candidate_source_generation,
                    route_universe_sha256=route_universe_sha256,
                    trace_profile_identity=dict(
                        profiles.trace_profile_identity
                    ),
                    submission_connector_profile_identity=dict(
                        profiles.submission_connector_profile_identity
                    ),
                    adapter_registry=adapter_registry,
                    retained_typed_pool_state_members=retained_pool_members,
                    submission_policy_snapshot=snapshot,
                    reason_code=native_capture.reason_code,
                    terminal_reason_by_market=(
                        phase_a_result.terminal_reasons
                    ),
                )
                terminal_evaluated_at = _evaluated_at_from_capability(
                    capability
                )
                return build_route_cost_evidence_manifest_from_captured(
                    universe=universe,
                    run_id=run_id,
                    route_cohort_id=route_cohort_id,
                    phase=phase,
                    candidate_source_generation=candidate_source_generation,
                    route_universe_sha256=route_universe_sha256,
                    evaluated_at=terminal_evaluated_at,
                    adapter_registry=adapter_registry,
                    connector_key_registry=connector_key_registry,
                    trace_profile_identity=dict(
                        profiles.trace_profile_identity
                    ),
                    submission_connector_profile_identity=dict(
                        profiles.submission_connector_profile_identity
                    ),
                    native_price_evidence=None,
                    chain_evidence=projected["chain_evidence"],
                    market_evidence=projected["market_evidence"],
                    transcripts=projected["transcripts"],
                    submission_policy_snapshot=snapshot,
                )
            except RouteCostEvidenceError:
                raise RouteCostCollectorError(
                    "route-cost native-price terminal projection is invalid"
                ) from None
        return _capture_phase_b(
            universe=universe,
            run_id=run_id,
            route_cohort_id=route_cohort_id,
            candidate_source_generation=candidate_source_generation,
            route_universe_sha256=route_universe_sha256,
            phase=phase,
            adapter_registry=adapter_registry,
            connector_key_registry=connector_key_registry,
            profiles=profiles,
            retained_pool_members=retained_pool_members,
            phase_a_result=phase_a_result,
            policy_snapshot=snapshot,
            capability=capability,
        )
    transcripts = build_terminal_transcript_inventory(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        trace_profile_identity=dict(profiles.trace_profile_identity),
        submission_connector_profile_identity=(
            dict(profiles.submission_connector_profile_identity)
        ),
        retained_typed_pool_state_members=retained_pool_members,
        terminal_reason_by_market=reasons,
    )
    return build_route_cost_evidence_manifest_from_captured(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        phase=phase,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        evaluated_at=evaluated_at,
        adapter_registry=adapter_registry,
        connector_key_registry=connector_key_registry,
        trace_profile_identity=dict(profiles.trace_profile_identity),
        submission_connector_profile_identity=(
            dict(profiles.submission_connector_profile_identity)
        ),
        native_price_evidence=None,
        chain_evidence=[],
        market_evidence=[],
        transcripts=transcripts,
        submission_policy_snapshot=snapshot,
    )


def _collect_route_cost_evidence_manifest_with_capability(
    data_dir: Path,
    *,
    universe: Mapping[str, Any],
    cohort: Mapping[str, Any],
    run_id: str,
    phase: str,
    route_universe_sha256: str,
    capability: Any,
) -> Dict[str, Any]:
    """Sealed terminal collector used only by the runner's time capability."""
    try:
        normalized_cohort = _normalize_route_cost_cohort(cohort)
        candidate_generation = universe.get("candidate_source_generation")
        normalized_universe = _validate_route_cost_universe(universe)
        if (
            not isinstance(universe, Mapping)
            or not isinstance(candidate_generation, str)
            or normalized_universe != universe
            or normalized_cohort.get("candidate_source_generation")
            != candidate_generation
            or physical_sha256(universe) != route_universe_sha256
        ):
            raise RouteCostCollectorError(
                "route-cost outer lineage is invalid"
            )
        adapter_registry = load_route_cost_adapter_registry()
        connector_registry = load_route_cost_connector_key_registry()
        selected = build_selected_markets(universe, adapter_registry)
        selected_ids = {row["market_id"] for row in selected}
        cohort_ids = {
            row.get("market_id")
            for row in normalized_cohort.get("legs", [])
            if isinstance(row, Mapping)
        }
        universe_market_ids = {
            row.get("market_id")
            for row in universe.get("selected_legs", [])
            if isinstance(row, Mapping)
        }
        universe_route_ids = {
            row.get("route_id")
            for row in universe.get("routes", [])
            if isinstance(row, Mapping)
        }
        cohort_route_ids = {
            row.get("route_id")
            for row in normalized_cohort.get("routes", [])
            if isinstance(row, Mapping)
        }
        universe_leg_lineage = sorted((
            {
                "market_id": row.get("market_id"),
                "market_type": row.get("market_type"),
                "token_symbol": row.get("token_symbol"),
                **(
                    {"collector_context": row.get("collector_context")}
                    if row.get("market_type") == "dex" else {}
                ),
            }
            for row in normalized_universe.get("selected_legs", [])
            if isinstance(row, Mapping)
        ), key=lambda row: str(row.get("market_id")))
        cohort_leg_lineage = sorted((
            {
                "market_id": row.get("market_id"),
                "market_type": row.get("market_type"),
                "token_symbol": row.get("token_symbol"),
                **(
                    {"collector_context": row.get("collector_context")}
                    if row.get("market_type") == "dex" else {}
                ),
            }
            for row in normalized_cohort.get("legs", [])
            if isinstance(row, Mapping)
        ), key=lambda row: str(row.get("market_id")))
        if (
            not selected_ids.issubset(cohort_ids)
            or cohort_ids != universe_market_ids
            or cohort_route_ids != universe_route_ids
            or normalized_cohort.get("routes")
            != normalized_universe.get("routes")
            or cohort_leg_lineage != universe_leg_lineage
        ):
            raise RouteCostCollectorError(
                "route-cost cohort market inventory differs"
            )
        supported = tuple(sorted(
            row["market_id"] for row in selected
            if row["structural_support_status"] == "supported"
        ))
        profiles = load_route_cost_profile_capture()
        typed_inventory = _load_retained_route_cost_typed_members(
            Path(data_dir), normalized_cohort,
            retain_keys=frozenset(
                (market_id, "dex_pool_state") for market_id in supported
            ),
        )
        retained = {
            market_id: typed_inventory[(market_id, "dex_pool_state")]
            for market_id in supported
            if (market_id, "dex_pool_state") in typed_inventory
        }
        manifest = _terminal_manifest(
            universe=universe,
            run_id=run_id,
            route_cohort_id=normalized_cohort["route_cohort_id"],
            phase=phase,
            candidate_source_generation=candidate_generation,
            route_universe_sha256=route_universe_sha256,
            adapter_registry=adapter_registry,
            connector_key_registry=connector_registry,
            profiles=profiles,
            retained_pool_members=retained,
            supported=supported,
            capability=capability,
        )
        validated = validate_route_cost_evidence_manifest_for_publication(
            manifest,
            universe=universe,
            expected_run_id=run_id,
            expected_route_cohort_id=normalized_cohort["route_cohort_id"],
            expected_phase=phase,
            expected_candidate_source_generation=candidate_generation,
            expected_route_universe_sha256=route_universe_sha256,
            retained_typed_pool_state_members=retained,
        )
        if validated != manifest:
            raise RouteCostCollectorError(
                "route-cost publication replay is noncanonical"
            )
        return validated
    except RouteCostCollectorError:
        raise
    except (RouteCostEvidenceError, TypeError, ValueError, UnicodeError):
        raise RouteCostCollectorError(
            "route-cost terminal collection is invalid"
        ) from None


def collect_route_cost_evidence_manifest(
    data_dir: Path,
    *,
    universe: Mapping[str, Any],
    cohort: Mapping[str, Any],
    run_id: str,
    phase: str,
    route_universe_sha256: str,
) -> Dict[str, Any]:
    """Collect one production manifest with no caller-selected I/O seams."""
    from datetime import datetime, timezone

    return _collect_route_cost_evidence_manifest_with_capability(
        Path(data_dir),
        universe=universe,
        cohort=cohort,
        run_id=run_id,
        phase=phase,
        route_universe_sha256=route_universe_sha256,
        capability=lambda: datetime.now(timezone.utc),
    )
