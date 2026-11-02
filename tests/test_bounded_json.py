from decimal import Decimal
import traceback
import unittest
from unittest.mock import patch
import zlib

import scripts.bounded_json as bounded_json
from scripts.bounded_json import BoundedJsonError, decode_bounded_json_response
from scripts.route_cost_collector import (
    RouteCostCollectorError,
    _decode_bounded_json_response,
)


_CLOSED_REASON_CODES = {
    "invalid",
    "resource_limit",
    "deadline",
    "unavailable",
    "encoding_unsupported",
    "noncanonical",
}


class _Headers:
    def __init__(self, rows):
        self._rows = tuple(rows)

    def raw_items(self):
        return iter(self._rows)


class _LowerRaisingName(str):
    def __new__(cls, value, exception, calls=None):
        instance = super().__new__(cls, value)
        instance.exception = exception
        instance.calls = calls
        return instance

    def lower(self):
        if self.calls is not None:
            self.calls.append("lower")
        raise self.exception


class _BadEncode(str):
    def __new__(cls, value, outcome, calls):
        instance = super().__new__(cls, value)
        instance.outcome = outcome
        instance.calls = calls
        return instance

    def encode(self, encoding="utf-8", errors="strict"):
        self.calls.append((encoding, errors))
        return self.outcome


class _Response:
    def __init__(self, body, *, headers=(), chunk_size=7):
        self._body = body
        self._offset = 0
        self._chunk_size = chunk_size
        self.headers = _Headers(headers)
        self.read_calls = 0

    def read(self, maximum):
        self.read_calls += 1
        amount = min(maximum, self._chunk_size)
        result = self._body[self._offset:self._offset + amount]
        self._offset += len(result)
        return result


class _DeadlineResponse(_Response):
    class _Socket:
        def __init__(self):
            self.timeouts = []

        def settimeout(self, value):
            self.timeouts.append(value)

    def __init__(self, body, *, chunk_size=1):
        super().__init__(body, chunk_size=chunk_size)
        self.socket = self._Socket()
        self.fp = type("Buffered", (), {
            "raw": type("Raw", (), {"_sock": self.socket})(),
        })()

    def read1(self, maximum):
        return super().read(maximum)

    def read(self, _maximum):
        raise AssertionError("deadline-bound stream must use read1")


def _gzip(body):
    encoder = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
    return encoder.compress(body) + encoder.flush()


class BoundedJsonParityTests(unittest.TestCase):
    def _options(self, **overrides):
        options = {
            "wire_limit": 4096,
            "decoded_limit": 4096,
            "scalar_limit": 4096,
            "node_limit": 256,
            "ordinary_string_limit": 256,
            "require_canonical": False,
        }
        options.update(overrides)
        return options

    def _decode(self, implementation, response, *, monotonic=None, **overrides):
        options = self._options(**overrides)
        if implementation == "shared":
            options["header_limit"] = options.pop("header_limit", 32 * 1024)
            if monotonic is not None:
                options["monotonic"] = monotonic
            return decode_bounded_json_response(response, **options)
        self.assertEqual(implementation, "route")
        options.pop("header_limit", None)
        if monotonic is None:
            return _decode_bounded_json_response(response, **options)
        with patch(
            "scripts.route_cost_collector.time.monotonic", monotonic
        ):
            return _decode_bounded_json_response(response, **options)

    def _assert_success(self, response_factory, expected, **options):
        for implementation in ("shared", "route"):
            with self.subTest(implementation=implementation):
                self.assertEqual(
                    self._decode(
                        implementation, response_factory(), **options
                    ),
                    expected,
                )

    def _assert_error(
        self, response_factory, reason_code, route_message, **options
    ):
        for implementation in ("shared", "route"):
            with self.subTest(implementation=implementation):
                error_type = (
                    BoundedJsonError
                    if implementation == "shared"
                    else RouteCostCollectorError
                )
                with self.assertRaises(error_type) as raised:
                    self._decode(
                        implementation, response_factory(), **options
                    )
                if implementation == "shared":
                    self.assertEqual(raised.exception.reason_code, reason_code)
                    self.assertIn(
                        raised.exception.reason_code, _CLOSED_REASON_CODES
                    )
                else:
                    self.assertEqual(str(raised.exception), route_message)

    def _assert_closed_sanitized_error(
        self,
        response_factory,
        reason_code,
        route_message,
        *,
        monotonic_factory=None,
        **options
    ):
        for implementation in ("shared", "route"):
            monotonic = (
                None if monotonic_factory is None else monotonic_factory()
            )
            with self.subTest(implementation=implementation):
                error_type = (
                    BoundedJsonError
                    if implementation == "shared"
                    else RouteCostCollectorError
                )
                caught = None
                try:
                    self._decode(
                        implementation,
                        response_factory(),
                        monotonic=monotonic,
                        **options
                    )
                except error_type as error:
                    rendered = "".join(traceback.format_exception(
                        type(error), error, error.__traceback__
                    ))
                    caught = error
                else:
                    self.fail("capability failure unexpectedly decoded")
                if implementation == "shared":
                    self.assertEqual(caught.reason_code, reason_code)
                else:
                    self.assertEqual(str(caught), route_message)
                self.assertNotIn("SECRET", rendered)
                self.assertIsNone(caught.__cause__)

    def test_identity_wire_and_decoded_exact_limits_and_plus_one(self):
        body = b'{"ok":true}'
        self._assert_success(
            lambda: _Response(body, headers=(
                ("Content-Length", str(len(body))),
            )),
            {"ok": True},
            wire_limit=len(body),
            decoded_limit=len(body),
        )
        self._assert_error(
            lambda: _Response(body),
            "resource_limit",
            "route-cost wire resource limit exceeded",
            wire_limit=len(body) - 1,
            decoded_limit=len(body),
        )
        self._assert_error(
            lambda: _Response(body),
            "resource_limit",
            "route-cost wire resource limit exceeded",
            wire_limit=len(body),
            decoded_limit=len(body) - 1,
        )

    def test_gzip_wire_and_decoded_exact_limits_and_plus_one(self):
        body = b'{"value":"' + (b"a" * 200) + b'"}'
        compressed = _gzip(body)
        headers = (
            ("Content-Encoding", "gzip"),
            ("Content-Length", str(len(compressed))),
        )
        self._assert_success(
            lambda: _Response(compressed, headers=headers),
            {"value": "a" * 200},
            wire_limit=len(compressed),
            decoded_limit=len(body),
        )
        self._assert_error(
            lambda: _Response(compressed, headers=headers),
            "resource_limit",
            "route-cost wire resource limit exceeded",
            wire_limit=len(compressed) - 1,
            decoded_limit=len(body),
        )
        self._assert_error(
            lambda: _Response(compressed, headers=headers),
            "resource_limit",
            "route-cost wire resource limit exceeded",
            wire_limit=len(compressed),
            decoded_limit=len(body) - 1,
        )

    def test_header_profiles_accept_exact_limit_and_reject_plus_one(self):
        body = b'{"ok":true}'
        name_32 = "X" * 3
        exact_32 = tuple((name_32, "a" * 8189) for _ in range(4))
        plus_one_32 = exact_32[:-1] + ((name_32, "a" * 8190),)
        self._assert_success(
            lambda: _Response(body, headers=exact_32), {"ok": True}
        )
        self._assert_error(
            lambda: _Response(body, headers=plus_one_32),
            "resource_limit",
            "route-cost wire resource limit exceeded",
        )

        name_64 = "X" * 128
        exact_64 = tuple((name_64, "a" * 8064) for _ in range(8))
        plus_one_64 = exact_64[:-1] + ((name_64, "a" * 8065),)
        self.assertEqual(
            self._decode(
                "shared",
                _Response(body, headers=exact_64),
                header_limit=64 * 1024,
            ),
            {"ok": True},
        )
        with self.assertRaises(BoundedJsonError) as raised:
            self._decode(
                "shared",
                _Response(body, headers=plus_one_64),
                header_limit=64 * 1024,
            )
        self.assertEqual(raised.exception.reason_code, "resource_limit")
        with self.assertRaisesRegex(
            RouteCostCollectorError,
            "^route-cost wire resource limit exceeded$",
        ):
            self._decode("route", _Response(body, headers=exact_64))

    def test_content_length_is_an_early_bound_and_must_match_wire(self):
        body = b'{"ok":true}'
        for declared, reason, route_message in (
            (
                str(len(body) + 1),
                "invalid",
                "route-cost Content-Length differs from wire bytes",
            ),
            ("01", "invalid", "route-cost Content-Length is invalid"),
            ("x", "invalid", "route-cost Content-Length is invalid"),
        ):
            with self.subTest(declared=declared):
                self._assert_error(
                    lambda declared=declared: _Response(
                        body, headers=(("Content-Length", declared),)
                    ),
                    reason,
                    route_message,
                )
        for implementation in ("shared", "route"):
            response = _Response(
                body, headers=(("Content-Length", "4097"),)
            )
            with self.subTest(implementation=implementation):
                error_type = (
                    BoundedJsonError
                    if implementation == "shared"
                    else RouteCostCollectorError
                )
                with self.assertRaises(error_type):
                    self._decode(implementation, response)
                self.assertEqual(response.read_calls, 0)

    def test_canonical_and_noncanonical_bytes(self):
        body = b'{"a":1,"b":[true,null]}'
        self._assert_success(
            lambda: _Response(body),
            {"a": 1, "b": [True, None]},
            require_canonical=True,
        )
        for body in (b'{ "a":1}', b'{"b":1,"a":2}', b"0.1"):
            with self.subTest(body=body):
                self._assert_error(
                    lambda body=body: _Response(body),
                    "noncanonical",
                    "route-cost JSON response is noncanonical",
                    require_canonical=True,
                )

    def test_duplicate_keys_exact_numbers_and_nonfinite_numbers(self):
        exact_integer = b"9" * 4096
        self._assert_success(
            lambda: _Response(exact_integer, chunk_size=len(exact_integer)),
            int(exact_integer),
            wire_limit=len(exact_integer),
            decoded_limit=len(exact_integer),
            scalar_limit=len(exact_integer),
            node_limit=1,
        )
        self._assert_success(
            lambda: _Response(b"1.234567890123456789"),
            Decimal("1.234567890123456789"),
        )
        for body in (
            b'{"a":1,"a":2}',
            b"NaN",
            b"Infinity",
            b"-Infinity",
            b"1e1000000",
            b"1e-9999999",
            b"-0",
            b"-0.0",
            b"9" * 4097,
        ):
            with self.subTest(body=body[:32]):
                self._assert_error(
                    lambda body=body: _Response(body, chunk_size=len(body)),
                    (
                        "resource_limit"
                        if len(body) > 4096
                        else "invalid"
                    ),
                    (
                        "route-cost wire resource limit exceeded"
                        if len(body) > 4096
                        else (
                            "route-cost JSON has duplicate keys"
                            if body == b'{"a":1,"a":2}'
                            else "route-cost JSON response is invalid"
                            if body in {b"NaN", b"Infinity", b"-Infinity"}
                            else "route-cost JSON number is invalid"
                        )
                    ),
                    wire_limit=max(4096, len(body)),
                    decoded_limit=max(4096, len(body)),
                    scalar_limit=max(4096, len(body)),
                    node_limit=1 if not body.startswith(b"{") else 8,
                )

    def test_exact_float_materialization_is_opt_in_and_exact(self):
        self._assert_success(
            lambda: _Response(b"0.5"),
            0.5,
            materialize_exact_floats=True,
        )
        self._assert_error(
            lambda: _Response(b"1.234567890123456789"),
            "invalid",
            "route-cost JSON number is invalid",
            materialize_exact_floats=True,
        )

    def test_valid_surrogate_pair_and_malformed_or_lone_surrogates(self):
        self._assert_success(
            lambda: _Response(b'"\\ud83d\\ude00"'), chr(0x1F600)
        )
        for body in (
            b'"\\ud800"',
            b'"\\udfff"',
            b'"\\ud800\\u0041"',
            b'"\\u-001"',
            b'"\\u0_01"',
            b'"\\ud800\\u-001"',
        ):
            with self.subTest(body=body):
                self._assert_error(
                    lambda body=body: _Response(body),
                    "invalid",
                    "route-cost JSON response is invalid",
                )

    def test_nesting_depth_128_passes_and_129_rejects(self):
        allowed = b"[" * 128 + b"0" + b"]" * 128
        rejected = b"[" * 129 + b"0" + b"]" * 129
        self._assert_success(
            lambda: _Response(allowed, chunk_size=len(allowed)),
            self._nested_list(128),
            wire_limit=len(allowed),
            decoded_limit=len(allowed),
            node_limit=256,
        )
        self._assert_error(
            lambda: _Response(rejected, chunk_size=len(rejected)),
            "resource_limit",
            "route-cost wire resource limit exceeded",
            wire_limit=len(rejected),
            decoded_limit=len(rejected),
            node_limit=256,
        )

    @staticmethod
    def _nested_list(depth):
        value = 0
        for _ in range(depth):
            value = [value]
        return value

    def test_node_scalar_and_string_exact_limits_and_plus_one(self):
        body = b'["ab",12,true]'
        self._assert_success(
            lambda: _Response(body),
            ["ab", 12, True],
            node_limit=4,
            scalar_limit=8,
            ordinary_string_limit=2,
        )
        for options in (
            {"node_limit": 3, "scalar_limit": 8, "ordinary_string_limit": 2},
            {"node_limit": 4, "scalar_limit": 7, "ordinary_string_limit": 2},
            {"node_limit": 4, "scalar_limit": 8, "ordinary_string_limit": 1},
        ):
            with self.subTest(options=options):
                self._assert_error(
                    lambda: _Response(body),
                    "resource_limit",
                    "route-cost wire resource limit exceeded",
                    **options
                )

    def test_deadline_equality_and_plus_one_tick_fail_closed(self):
        for ticks in ((1.0, 2.0), (1.0, 2.000001)):
            for implementation in ("shared", "route"):
                response = _DeadlineResponse(b"0")
                clock = iter(ticks).__next__
                error_type = (
                    BoundedJsonError
                    if implementation == "shared"
                    else RouteCostCollectorError
                )
                with self.subTest(
                    implementation=implementation, final_tick=ticks[-1]
                ), self.assertRaises(error_type) as raised:
                    self._decode(
                        implementation,
                        response,
                        monotonic=clock,
                        absolute_deadline=2.0,
                        node_limit=1,
                    )
                if implementation == "shared":
                    self.assertEqual(raised.exception.reason_code, "deadline")
                else:
                    self.assertEqual(
                        str(raised.exception),
                        "route-cost response deadline exceeded",
                    )
                self.assertEqual(response.read_calls, 1)
                self.assertEqual(response.socket.timeouts, [1.0])

    def test_header_accessor_and_stream_failures_are_sanitized(self):
        class BrokenResponse:
            @property
            def headers(self):
                raise OSError("/private/SECRET-HEADER")

        class BrokenHeaders:
            def raw_items(self):
                raise OSError("https://user:SECRET@example.invalid/path")

        class BrokenStream(_Response):
            def read(self, maximum):
                raise OSError("/private/SECRET-WIRE-PATH")

        factories = (
            BrokenResponse,
            lambda: self._with_headers(BrokenHeaders()),
            lambda: BrokenStream(b""),
        )
        for factory in factories:
            for implementation in ("shared", "route"):
                with self.subTest(
                    factory=getattr(factory, "__name__", "headers"),
                    implementation=implementation,
                ):
                    error_type = (
                        BoundedJsonError
                        if implementation == "shared"
                        else RouteCostCollectorError
                    )
                    try:
                        self._decode(implementation, factory())
                    except error_type as error:
                        rendered = "".join(traceback.format_exception(
                            type(error), error, error.__traceback__
                        ))
                    else:
                        self.fail("broken response unexpectedly decoded")
                    self.assertNotIn("SECRET", rendered)
                    self.assertNotIn("example.invalid", rendered)

    def test_malformed_header_rows_are_closed_sanitized_invalid_errors(self):
        class BrokenRow:
            def __iter__(self):
                raise RuntimeError("SECRET-HEADER-ROW")

        for row in (("X-Only",), BrokenRow()):
            with self.subTest(row=type(row).__name__):
                self._assert_closed_sanitized_error(
                    lambda row=row: _Response(
                        b'{"ok":true}', headers=(row,)
                    ),
                    "invalid",
                    "route-cost response headers are invalid",
                )

    def test_header_normalization_ordinary_errors_are_closed_and_sanitized(self):
        self._assert_closed_sanitized_error(
            lambda: _Response(
                b'{"ok":true}',
                headers=((
                    _LowerRaisingName(
                        "X-Test", RuntimeError("/private/SECRET-LOWER")
                    ),
                    "value",
                ),),
            ),
            "invalid",
            "route-cost response headers are invalid",
        )

    def test_header_lower_base_exception_overrides_are_not_invoked(self):
        for exception_type in (KeyboardInterrupt, SystemExit):
            for implementation in ("shared", "route"):
                calls = []
                marker = exception_type("HEADER-NORMALIZATION")
                response = _Response(
                    b'{"ok":true}',
                    headers=((
                        _LowerRaisingName("X-Test", marker, calls), "value"
                    ),),
                )
                error_type = (
                    BoundedJsonError
                    if implementation == "shared"
                    else RouteCostCollectorError
                )
                with self.subTest(
                    exception=exception_type.__name__,
                    implementation=implementation,
                ):
                    try:
                        self._decode(implementation, response)
                    except exception_type:
                        self.fail("untrusted lower override was invoked")
                    except error_type as error:
                        if implementation == "shared":
                            self.assertEqual(error.reason_code, "invalid")
                        else:
                            self.assertEqual(
                                str(error),
                                "route-cost response headers are invalid",
                            )
                    else:
                        self.fail("header subclass unexpectedly decoded")
                    self.assertEqual(calls, [])

    def test_header_encode_subclass_capabilities_are_not_invoked(self):
        class LenRaising:
            def __len__(self):
                raise RuntimeError("/private/SECRET-ENCODE-LEN")

        for position in ("name", "value"):
            for outcome in (7, LenRaising()):
                calls = []

                def response_factory():
                    name = "X-Test"
                    value = "value"
                    if position == "name":
                        name = _BadEncode(name, outcome, calls)
                    else:
                        value = _BadEncode(value, outcome, calls)
                    return _Response(
                        b'{"ok":true}', headers=((name, value),)
                    )

                with self.subTest(
                    position=position, outcome=type(outcome).__name__
                ):
                    self._assert_closed_sanitized_error(
                        response_factory,
                        "invalid",
                        "route-cost response headers are invalid",
                    )
                    self.assertEqual(calls, [])

    def test_header_normalization_rejects_non_string_lower_result(self):
        class NonStringLowerName(str):
            def lower(self):
                return 7

        self._assert_error(
            lambda: _Response(
                b'{"ok":true}',
                headers=((NonStringLowerName("X-Test"), "value"),),
            ),
            "invalid",
            "route-cost response headers are invalid",
        )

    def test_header_iteration_stops_at_row_65_without_eager_consumption(self):
        class CountingHeaders:
            def __init__(self, count, *, fail_after=False):
                self.count = count
                self.fail_after = fail_after
                self.consumed = 0

            def raw_items(self):
                for index in range(self.count):
                    self.consumed += 1
                    yield ("X-{}".format(index), "a")
                if self.fail_after:
                    raise RuntimeError("SECRET-HEADER-ITERATION-PAST-65")

        for implementation in ("shared", "route"):
            headers = CountingHeaders(64)
            response = self._with_headers(headers)
            with self.subTest(implementation=implementation, rows=64):
                self.assertEqual(
                    self._decode(implementation, response), {"ok": True}
                )
                self.assertEqual(headers.consumed, 64)

        for total, fail_after in ((100, False), (65, True)):
            for implementation in ("shared", "route"):
                headers = CountingHeaders(total, fail_after=fail_after)
                response = self._with_headers(headers)
                error_type = (
                    BoundedJsonError
                    if implementation == "shared"
                    else RouteCostCollectorError
                )
                with self.subTest(
                    implementation=implementation,
                    total=total,
                    fail_after=fail_after,
                ), self.assertRaises(error_type) as raised:
                    self._decode(implementation, response)
                if implementation == "shared":
                    self.assertEqual(
                        raised.exception.reason_code, "resource_limit"
                    )
                else:
                    self.assertEqual(
                        str(raised.exception),
                        "route-cost wire resource limit exceeded",
                    )
                self.assertEqual(headers.consumed, 65)

    def test_header_iterator_error_before_limit_is_closed_sanitized_invalid(self):
        class BrokenIteratorHeaders:
            def __init__(self):
                self.consumed = 0

            def raw_items(self):
                for index in range(2):
                    self.consumed += 1
                    yield ("X-{}".format(index), "a")
                raise RuntimeError("SECRET-HEADER-ITERATION")

        for implementation in ("shared", "route"):
            headers = BrokenIteratorHeaders()
            response = self._with_headers(headers)
            error_type = (
                BoundedJsonError
                if implementation == "shared"
                else RouteCostCollectorError
            )
            caught = None
            with self.subTest(implementation=implementation):
                try:
                    self._decode(implementation, response)
                except error_type as error:
                    rendered = "".join(traceback.format_exception(
                        type(error), error, error.__traceback__
                    ))
                    caught = error
                else:
                    self.fail("broken header iterator unexpectedly decoded")
                if implementation == "shared":
                    self.assertEqual(caught.reason_code, "invalid")
                else:
                    self.assertEqual(
                        str(caught),
                        "route-cost response headers are invalid",
                    )
                self.assertEqual(headers.consumed, 2)
                self.assertNotIn("SECRET", rendered)

    def test_response_capability_errors_are_closed_and_sanitized(self):
        class BrokenReadAccessor(_Response):
            @property
            def read(self):
                raise RuntimeError("SECRET-READ-ACCESSOR")

        class BrokenReadCall(_Response):
            def read(self, maximum):
                raise ValueError("SECRET-READ-CALL")

        class BrokenRead1Accessor(_Response):
            @property
            def read1(self):
                raise RuntimeError("SECRET-READ1-ACCESSOR")

        class BrokenRead1Call(_DeadlineResponse):
            def read1(self, maximum):
                raise ValueError("SECRET-READ1-CALL")

        class BrokenFpAccessor(_Response):
            @property
            def fp(self):
                raise RuntimeError("SECRET-FP-ACCESSOR")

            def read1(self, maximum):
                return super().read(maximum)

        def deadline_response_with(raw):
            response = _Response(b"0")
            response.read1 = response.read
            response.fp = type("Buffered", (), {"raw": raw})()
            return response

        def deadline_response_with_socket(socket):
            raw = type("Raw", (), {"_sock": socket})()
            return deadline_response_with(raw)

        class BrokenRaw:
            @property
            def _sock(self):
                raise ValueError("SECRET-SOCK-ACCESSOR")

        class BrokenBuffered:
            @property
            def raw(self):
                raise RuntimeError("SECRET-RAW-ACCESSOR")

        class BrokenSetterAccessor:
            @property
            def settimeout(self):
                raise RuntimeError("SECRET-SETTIMEOUT-ACCESSOR")

        class BrokenSetterCall:
            def settimeout(self, value):
                raise ValueError("SECRET-SETTIMEOUT-CALL")

        invalid_cases = (
            (lambda: BrokenReadAccessor(b"0"), False),
            (lambda: BrokenRead1Accessor(b"0"), True),
            (lambda: BrokenFpAccessor(b"0"), True),
            (
                lambda: self._deadline_response_with_buffer(
                    BrokenBuffered()
                ),
                True,
            ),
            (lambda: deadline_response_with(BrokenRaw()), True),
            (
                lambda: deadline_response_with_socket(
                    BrokenSetterAccessor()
                ),
                True,
            ),
        )
        for factory, deadline_bound in invalid_cases:
            with self.subTest(factory=factory, invalid=True):
                self._assert_closed_sanitized_error(
                    factory,
                    "invalid",
                    (
                        "route-cost deadline-bound stream is invalid"
                        if deadline_bound
                        else "route-cost response stream is invalid"
                    ),
                    monotonic_factory=(lambda: lambda: 1.0)
                    if deadline_bound else None,
                    absolute_deadline=2.0 if deadline_bound else None,
                    node_limit=1,
                )

        unavailable_cases = (
            (lambda: BrokenReadCall(b"0"), False),
            (lambda: BrokenRead1Call(b"0"), True),
            (
                lambda: deadline_response_with_socket(BrokenSetterCall()),
                True,
            ),
        )
        for factory, deadline_bound in unavailable_cases:
            with self.subTest(factory=factory, unavailable=True):
                self._assert_closed_sanitized_error(
                    factory,
                    "unavailable",
                    "route-cost response stream is unavailable",
                    monotonic_factory=(lambda: lambda: 1.0)
                    if deadline_bound else None,
                    absolute_deadline=2.0 if deadline_bound else None,
                    node_limit=1,
                )

    @staticmethod
    def _deadline_response_with_buffer(buffered):
        response = _Response(b"0")
        response.read1 = response.read
        response.fp = buffered
        return response

    def test_monotonic_errors_are_closed_exact_finite_and_nondecreasing(self):
        invalid_clocks = (
            lambda: iter((True, True)).__next__,
            lambda: iter((Decimal("1"), Decimal("1"))).__next__,
            lambda: iter((float("nan"),)).__next__,
            lambda: iter((float("inf"),)).__next__,
            lambda: iter((1.5, 1.0)).__next__,
        )
        for clock_factory in invalid_clocks:
            with self.subTest(clock_factory=clock_factory):
                self._assert_closed_sanitized_error(
                    lambda: _DeadlineResponse(b"0"),
                    "invalid",
                    "route-cost deadline monotonic capability is invalid",
                    monotonic_factory=clock_factory,
                    absolute_deadline=2.0,
                    node_limit=1,
                )

        def raising_clock():
            def monotonic():
                raise RuntimeError("SECRET-MONOTONIC")

            return monotonic

        self._assert_closed_sanitized_error(
            lambda: _DeadlineResponse(b"0"),
            "unavailable",
            "route-cost response stream is unavailable",
            monotonic_factory=raising_clock,
            absolute_deadline=2.0,
            node_limit=1,
        )

    @staticmethod
    def _with_headers(headers):
        response = _Response(b'{"ok":true}')
        response.headers = headers
        return response

    def test_keyboard_interrupt_and_system_exit_propagate(self):
        class RaisingHeaders:
            def __init__(self, exception):
                self.exception = exception

            def raw_items(self):
                raise self.exception

        class RaisingStream(_Response):
            def __init__(self, exception):
                super().__init__(b"")
                self.exception = exception

            def read(self, maximum):
                raise self.exception

        for exception_type in (KeyboardInterrupt, SystemExit):
            for implementation in ("shared", "route"):
                responses = (
                    self._with_headers(RaisingHeaders(exception_type())),
                    RaisingStream(exception_type()),
                )
                for response in responses:
                    with self.subTest(
                        exception=exception_type.__name__,
                        implementation=implementation,
                        response=type(response).__name__,
                    ), self.assertRaises(exception_type):
                        self._decode(implementation, response)

    def test_closed_reason_codes_and_unsupported_encoding(self):
        cases = (
            (lambda: _Response(b"{"), {}, "invalid"),
            (
                lambda: _Response(b"00"),
                {"wire_limit": 1, "decoded_limit": 2},
                "resource_limit",
            ),
            (
                lambda: _Response(
                    b'{"ok":true}', headers=(("Content-Encoding", "br"),)
                ),
                {},
                "encoding_unsupported",
            ),
            (
                lambda: _Response(b'{ "a":1}'),
                {"require_canonical": True},
                "noncanonical",
            ),
        )
        for factory, options, reason_code in cases:
            with self.subTest(reason_code=reason_code):
                with self.assertRaises(BoundedJsonError) as raised:
                    self._decode("shared", factory(), **options)
                self.assertEqual(raised.exception.reason_code, reason_code)
        self.assertEqual(_CLOSED_REASON_CODES, {
            "invalid",
            "resource_limit",
            "deadline",
            "unavailable",
            "encoding_unsupported",
            "noncanonical",
        })

    def test_return_decoded_bytes_preserves_validated_physical_body(self):
        body = b'{"amount":1.25}'
        self._assert_success(
            lambda: _Response(body), body, return_decoded_bytes=True
        )

    def test_public_header_projection_reuses_decoder_authority_without_body_read(self):
        response = _Response(
            b"SECRET-UNREAD",
            headers=(("X-Test", "value"), ("CONTENT-ENCODING", "gzip")),
        )
        self.assertEqual(
            bounded_json.validate_bounded_json_response_headers(
                response, header_limit=64 * 1024
            ),
            (("x-test", "value"), ("content-encoding", "gzip")),
        )
        self.assertEqual(response.read_calls, 0)


if __name__ == "__main__":
    unittest.main()
