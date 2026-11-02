"""Reusable bounded HTTP/JSON response decoding.

The decoder owns transport-byte, decompression, lexical, shape, header, and
deadline limits.  It exposes only closed error classifications so callers can
map failures into their own secret-free error contracts.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import json
import math
import re
import time
from typing import Any, Callable, Dict, Optional, Tuple
import zlib


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
_REASON_CODES = frozenset({
    "invalid",
    "resource_limit",
    "deadline",
    "unavailable",
    "encoding_unsupported",
    "noncanonical",
})
_DETAIL_MESSAGES = {
    "limit_invalid": "bounded JSON limit is invalid",
    "headers_invalid": "bounded JSON response headers are invalid",
    "headers_ambiguous": "bounded JSON response headers are ambiguous",
    "resource_limit": "bounded JSON resource limit exceeded",
    "content_length_invalid": "bounded JSON Content-Length is invalid",
    "content_length_mismatch": (
        "bounded JSON Content-Length differs from wire bytes"
    ),
    "encoding_unsupported": "bounded JSON response encoding is unsupported",
    "deadline_stream_invalid": "bounded JSON deadline-bound stream is invalid",
    "monotonic_invalid": "bounded JSON deadline monotonic capability is invalid",
    "deadline": "bounded JSON response deadline exceeded",
    "stream_unavailable": "bounded JSON response stream is unavailable",
    "stream_invalid": "bounded JSON response stream is invalid",
    "gzip_invalid": "bounded JSON gzip response is invalid",
    "json_invalid": "bounded JSON response is invalid",
    "duplicate_keys": "bounded JSON has duplicate keys",
    "number_invalid": "bounded JSON number is invalid",
    "object_key_invalid": "bounded JSON object key is invalid",
    "string_invalid": "bounded JSON string is invalid",
    "value_invalid": "bounded JSON value is invalid",
    "noncanonical": "bounded JSON response is noncanonical",
}


class BoundedJsonError(ValueError):
    """A secret-free bounded-decoder failure with one closed reason code."""

    def __init__(self, reason_code: str, detail_code: str = "json_invalid") -> None:
        if reason_code not in _REASON_CODES or detail_code not in _DETAIL_MESSAGES:
            raise ValueError("bounded JSON error classification is invalid")
        self.reason_code = reason_code
        self.detail_code = detail_code
        super().__init__(_DETAIL_MESSAGES[detail_code])


def _error(reason_code: str, detail_code: str) -> BoundedJsonError:
    return BoundedJsonError(reason_code, detail_code)


def _resource_limit() -> BoundedJsonError:
    return _error("resource_limit", "resource_limit")


def _response_header_rows(
    response: Any, *, header_limit: int
) -> Tuple[Tuple[str, str], ...]:
    try:
        headers = getattr(response, "headers", None)
        raw_items = getattr(headers, "raw_items", None)
    except Exception:
        raise _error("invalid", "headers_invalid") from None
    if not callable(raw_items):
        raise _error("invalid", "headers_invalid")
    try:
        rows = iter(raw_items())
    except BoundedJsonError:
        raise
    except Exception:
        raise _error("invalid", "headers_invalid") from None
    total = 0
    normalized = []
    while True:
        try:
            row = next(rows)
        except StopIteration:
            break
        except BoundedJsonError:
            raise
        except Exception:
            raise _error("invalid", "headers_invalid") from None
        if len(normalized) == 64:
            raise _resource_limit()
        try:
            name, value = row
            if type(name) is not str or type(value) is not str:
                raise _error("invalid", "headers_invalid")
            name_bytes = name.encode("ascii")
            value_bytes = value.encode("latin-1")
            normalized_name = name.lower()
            if type(normalized_name) is not str:
                raise _error("invalid", "headers_invalid")
        except BoundedJsonError:
            raise
        except Exception:
            raise _error("invalid", "headers_invalid") from None
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
            raise _error("invalid", "headers_invalid")
        total += len(name_bytes) + len(value_bytes)
        if total > header_limit:
            raise _resource_limit()
        normalized.append((normalized_name, value))
    return tuple(normalized)


def validate_bounded_json_response_headers(
    response: Any, *, header_limit: int
) -> Tuple[Tuple[str, str], ...]:
    """Return the bounded, normalized header projection without reading a body."""
    if type(header_limit) is not int or header_limit <= 0:
        raise _error("invalid", "limit_invalid")
    return _response_header_rows(response, header_limit=header_limit)


def _one_header(
    rows: Tuple[Tuple[str, str], ...], name: str
) -> Optional[str]:
    values = [value for key, value in rows if key == name]
    if len(values) > 1:
        raise _error("invalid", "headers_ambiguous")
    return values[0] if values else None


def _bounded_json_shape(
    value: Any,
    *,
    node_limit: int,
    ordinary_string_limit: int,
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
                    raise _error("invalid", "object_key_invalid")
                try:
                    encoded = key.encode("utf-8")
                except UnicodeEncodeError:
                    raise _error("invalid", "string_invalid") from None
                if len(encoded) > ordinary_string_limit:
                    raise _resource_limit()
                pending.append(nested)
        elif isinstance(current, list):
            pending.extend(current)
        elif isinstance(current, str):
            try:
                encoded = current.encode("utf-8")
            except UnicodeEncodeError:
                raise _error("invalid", "string_invalid") from None
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
                raise _error("invalid", "number_invalid")
        elif type(current) is float and permit_binary_float:
            if (
                not math.isfinite(current)
                or (current == 0.0 and math.copysign(1.0, current) < 0)
            ):
                raise _error("invalid", "number_invalid")
        else:
            raise _error("invalid", "value_invalid")


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


def _json_invalid() -> BoundedJsonError:
    return _error("invalid", "json_invalid")


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


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError):
        raise _error("noncanonical", "noncanonical") from None


def decode_bounded_json_response(
    response: Any,
    *,
    header_limit: int,
    wire_limit: int,
    decoded_limit: int,
    scalar_limit: int,
    node_limit: int,
    ordinary_string_limit: int,
    require_canonical: bool,
    materialize_exact_floats: bool = False,
    absolute_deadline: Optional[float] = None,
    monotonic: Callable[[], float] = time.monotonic,
    return_decoded_bytes: bool = False,
) -> Any:
    """Stream, bound, decode, and shape-check one JSON HTTP response."""
    for value in (
        header_limit,
        wire_limit,
        decoded_limit,
        scalar_limit,
        node_limit,
        ordinary_string_limit,
    ):
        if type(value) is not int or value <= 0:
            raise _error("invalid", "limit_invalid")
    if (
        type(require_canonical) is not bool
        or type(materialize_exact_floats) is not bool
        or type(return_decoded_bytes) is not bool
        or not callable(monotonic)
        or (
            absolute_deadline is not None
            and (
                isinstance(absolute_deadline, bool)
                or not isinstance(absolute_deadline, (int, float))
                or not math.isfinite(absolute_deadline)
            )
        )
    ):
        raise _error("invalid", "limit_invalid")
    rows = validate_bounded_json_response_headers(
        response, header_limit=header_limit
    )
    raw_length = _one_header(rows, "content-length")
    declared_length = None
    if raw_length is not None:
        if (
            not raw_length
            or not raw_length.isascii()
            or not raw_length.isdecimal()
            or (len(raw_length) > 1 and raw_length.startswith("0"))
        ):
            raise _error("invalid", "content_length_invalid")
        wire_limit_text = str(wire_limit)
        if len(raw_length) > len(wire_limit_text) or (
            len(raw_length) == len(wire_limit_text)
            and raw_length > wire_limit_text
        ):
            raise _resource_limit()
        declared_length = int(raw_length)
    encoding = _one_header(rows, "content-encoding")
    if encoding not in {None, "identity", "gzip"}:
        raise _error("encoding_unsupported", "encoding_unsupported")
    wire_bytes = 0
    decoded = bytearray()
    decoder = (
        zlib.decompressobj(16 + zlib.MAX_WBITS)
        if encoding == "gzip" else None
    )

    def deadline_stream() -> Tuple[Any, Any]:
        try:
            reader = getattr(response, "read1", None)
            socket_value = response.fp.raw._sock
            setter = getattr(socket_value, "settimeout", None)
        except BoundedJsonError:
            raise
        except Exception:
            raise _error("invalid", "deadline_stream_invalid") from None
        if not callable(reader) or not callable(setter):
            raise _error("invalid", "deadline_stream_invalid")
        return reader, setter

    previous_monotonic = None

    def sample_monotonic() -> float:
        nonlocal previous_monotonic
        try:
            sample = monotonic()
        except BoundedJsonError:
            raise
        except Exception:
            raise _error("unavailable", "stream_unavailable") from None
        if type(sample) not in {int, float}:
            raise _error("invalid", "monotonic_invalid")
        try:
            normalized = float(sample)
        except Exception:
            raise _error("invalid", "monotonic_invalid") from None
        if (
            not math.isfinite(normalized)
            or (
                previous_monotonic is not None
                and normalized < previous_monotonic
            )
        ):
            raise _error("invalid", "monotonic_invalid")
        previous_monotonic = normalized
        return normalized

    deadline_reader = None
    deadline_setter = None
    if absolute_deadline is not None:
        deadline_reader, deadline_setter = deadline_stream()
    try:
        while True:
            if absolute_deadline is not None:
                remaining = float(absolute_deadline) - sample_monotonic()
                if remaining <= 0:
                    raise _error("deadline", "deadline")
                try:
                    deadline_setter(remaining)
                except BoundedJsonError:
                    raise
                except Exception:
                    raise _error("unavailable", "stream_unavailable") from None
            if deadline_reader is not None:
                reader = deadline_reader
            else:
                try:
                    reader = response.read
                except BoundedJsonError:
                    raise
                except Exception:
                    raise _error("invalid", "stream_invalid") from None
                if not callable(reader):
                    raise _error("invalid", "stream_invalid")
            try:
                chunk = reader(min(64 * 1024, wire_limit + 1 - wire_bytes))
            except BoundedJsonError:
                raise
            except Exception:
                raise _error("unavailable", "stream_unavailable") from None
            if not isinstance(chunk, bytes):
                raise _error("invalid", "stream_invalid")
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
            raise _error("invalid", "content_length_mismatch")
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
                raise _error("invalid", "gzip_invalid")
    except zlib.error:
        raise _error("invalid", "gzip_invalid") from None
    except (OSError, TimeoutError):
        raise _error("unavailable", "stream_unavailable") from None
    try:
        text_value = bytes(decoded).decode("utf-8")
    except UnicodeDecodeError:
        raise _json_invalid() from None
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
                raise _error("invalid", "duplicate_keys")
            result[key] = nested
        return result

    def reject_constant(_value: str) -> None:
        raise _error("invalid", "number_invalid")

    def exact_float(token: str) -> Any:
        if len(token.encode("ascii")) > min(
            scalar_limit, _MAX_EXACT_JSON_NUMBER_TOKEN_BYTES
        ):
            raise _resource_limit()
        try:
            value = Decimal(token)
        except (InvalidOperation, ValueError):
            raise _error("invalid", "number_invalid") from None
        if (
            not value.is_finite()
            or (value.is_zero() and value.is_signed())
            or (not value.is_zero() and abs(value.adjusted()) > 4095)
        ):
            raise _error("invalid", "number_invalid")
        if materialize_exact_floats:
            try:
                binary_value = float(value)
            except (OverflowError, ValueError):
                raise _error("invalid", "number_invalid") from None
            if (
                not math.isfinite(binary_value)
                or Decimal(str(binary_value)) != value
            ):
                raise _error("invalid", "number_invalid")
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
            raise _error("invalid", "number_invalid")
        try:
            return int(token)
        except (ValueError, OverflowError):
            raise _error("invalid", "number_invalid") from None

    try:
        value = json.loads(
            text_value,
            object_pairs_hook=reject_duplicate,
            parse_constant=reject_constant,
            parse_float=exact_float,
            parse_int=exact_int,
        )
    except BoundedJsonError:
        raise
    except RecursionError:
        raise _resource_limit() from None
    except (TypeError, ValueError, json.JSONDecodeError):
        raise _json_invalid() from None
    _bounded_json_shape(
        value,
        node_limit=node_limit,
        ordinary_string_limit=ordinary_string_limit,
        permit_binary_float=materialize_exact_floats,
    )
    if require_canonical and bytes(decoded) != _canonical_json_bytes(value):
        raise _error("noncanonical", "noncanonical")
    if return_decoded_bytes:
        return bytes(decoded)
    return value
