import hashlib
import json
from datetime import datetime, timedelta, timezone
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock, Thread as TestThread, enumerate as enumerate_threads
import time
from unittest.mock import patch

from scripts.collect_route_cohort import (
    _safe_leg_projection,
    collect_route_cohort as _collect_route_cohort,
    collect_unique_route_legs,
    main,
    materialize_route_leg_rows,
)


_TEST_RAW_DIRECTORY = tempfile.TemporaryDirectory(prefix="route-cohort-tests-")


def _complete_test_routes(universe):
    normalized = dict(universe)
    normalized["routes"] = []
    for source in universe.get("routes", []):
        route = dict(source)
        if not route.get("route_id") and all(
            isinstance(route.get(key), str)
            for key in (
                "token_symbol", "buy_market_id", "sell_market_id", "route_mode"
            )
        ):
            route["route_id"] = "route:{}:{}->{}:{}".format(
                route["token_symbol"],
                route["buy_market_id"],
                route["sell_market_id"],
                route["route_mode"],
            )
        normalized["routes"].append(route)
    return normalized


def _raw_writing_fake(collector):
    def wrapped(*args, **kwargs):
        value = collector(*args, **kwargs)
        row = value[0] if isinstance(value, tuple) and len(value) == 2 else value
        raw_path = kwargs.get("raw_path")
        if (
            isinstance(row, dict)
            and row.get("status") in {"observed", "partial"}
            and isinstance(raw_path, Path)
            and not raw_path.exists()
        ):
            raw_path.write_bytes(b"test raw evidence")
        return value

    return wrapped


def collect_route_cohort(universe, *args, **kwargs):
    """Keep stateful unit fakes in-process and give every test isolated raw."""
    kwargs.setdefault("raw_root", Path(_TEST_RAW_DIRECTORY.name))
    kwargs.setdefault("executor_factory", ThreadPoolExecutor)
    if "cex_collector" in kwargs:
        kwargs["cex_collector"] = _raw_writing_fake(kwargs["cex_collector"])
    if "dex_collector" in kwargs:
        kwargs["dex_collector"] = _raw_writing_fake(kwargs["dex_collector"])
    return _collect_route_cohort(_complete_test_routes(universe), *args, **kwargs)


def _strict_route(token, buy_market_id, sell_market_id, route_mode):
    return {
        "route_id": "route:{}:{}->{}:{}".format(
            token, buy_market_id, sell_market_id, route_mode
        ),
        "token_symbol": token,
        "buy_market_id": buy_market_id,
        "sell_market_id": sell_market_id,
        "route_mode": route_mode,
    }


def _strict_cex_universe():
    alpha = "cex:alpha:UNI/USDT"
    beta = "cex:beta:UNI/USDT"
    return {
        "candidate_source_generation": "generation-a",
        "selected_legs": [
            {"market_id": alpha, "market_type": "cex", "exchange": "alpha"},
            {"market_id": beta, "market_type": "cex", "exchange": "beta"},
        ],
        "routes": [
            _strict_route("UNI", alpha, beta, "prepositioned_inventory")
        ],
    }


def _write_observed_raw(_leg, *, raw_path, **_kwargs):
    raw_path.write_bytes(b"observed raw")
    return {
        "status": "observed",
        "state_observed_at": "2026-08-01T12:00:00Z",
    }


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.payload


class CollectionDeadlineTest(unittest.TestCase):
    def test_expired_deadline_uses_one_stable_exception_without_http_request(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_cex_depth import request_json
        from scripts.fetch_dex_depth import http_json_rpc

        clock = FakeClock()
        deadline = CollectionDeadline.for_duration(
            0,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        with patch("urllib.request.urlopen") as urlopen:
            with self.assertRaises(CollectionDeadlineExceeded) as cex_error:
                request_json("https://example.test/book", deadline=deadline)
            with self.assertRaises(CollectionDeadlineExceeded) as dex_error:
                http_json_rpc(
                    "https://example.test/rpc",
                    {"jsonrpc": "2.0", "id": 1, "method": "test", "params": []},
                    deadline=deadline,
                )

        urlopen.assert_not_called()
        self.assertEqual(str(cex_error.exception), "collection deadline exceeded")
        self.assertEqual(type(cex_error.exception), type(dex_error.exception))
        self.assertEqual(str(cex_error.exception), str(dex_error.exception))

    def test_request_timeout_never_exceeds_remaining_deadline(self):
        from scripts.collection_deadline import CollectionDeadline
        from scripts.fetch_cex_depth import request_json

        clock = FakeClock()
        deadline = CollectionDeadline.for_duration(
            2.5,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        raw = json.dumps({"ok": True}).encode("utf-8")
        with patch(
            "urllib.request.urlopen",
            return_value=FakeResponse(raw),
        ) as urlopen:
            payload, returned_raw = request_json(
                "https://example.test/book",
                deadline=deadline,
                timeout_seconds=30,
                max_retries=1,
            )

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(returned_raw, raw)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 2.5)

    def test_retry_sleep_is_clamped_and_exhaustion_raises_stable_exception(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )

        clock = FakeClock(now=10.0)
        deadline = CollectionDeadline.for_duration(
            1.25,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        with self.assertRaisesRegex(
            CollectionDeadlineExceeded,
            "^collection deadline exceeded$",
        ):
            deadline.sleep_before_retry(30)

        self.assertEqual(clock.sleeps, [1.25])
        self.assertEqual(deadline.remaining_seconds(), 0.0)

    def test_final_transport_failure_is_replaced_by_deadline_exhaustion(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_cex_depth import request_json
        from scripts.fetch_dex_depth import http_json_rpc

        for request_call in (
            lambda deadline: request_json(
                "https://example.test/book",
                deadline=deadline,
                max_retries=1,
            ),
            lambda deadline: http_json_rpc(
                "https://example.test/rpc",
                {"jsonrpc": "2.0", "id": 1, "method": "test", "params": []},
                deadline=deadline,
                max_retries=1,
            ),
        ):
            clock = FakeClock()
            deadline = CollectionDeadline.for_duration(
                1,
                clock=clock.monotonic,
                sleeper=clock.sleep,
            )

            def expire_then_fail(*_args, **_kwargs):
                clock.now = 2
                raise urllib.error.URLError("transport timed out")

            with patch("urllib.request.urlopen", side_effect=expire_then_fail):
                with self.assertRaisesRegex(
                    CollectionDeadlineExceeded,
                    "^collection deadline exceeded$",
                ):
                    request_call(deadline)


class RpcClientIsolationTest(unittest.TestCase):
    def test_production_clients_start_at_one_and_keep_independent_records(self):
        from scripts.fetch_dex_depth import RpcClient

        def transport(_url, payload):
            response = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": "0x1",
            }
            return response, json.dumps(response, sort_keys=True).encode("utf-8")

        first = RpcClient("eth", "https://rpc.example.test", request=transport)
        second = RpcClient("eth", "https://rpc.example.test", request=transport)

        self.assertEqual(first.method("test_first", []), "0x1")
        self.assertEqual(second.method("test_second", []), "0x1")
        self.assertEqual(first.records[0]["request"]["id"], 1)
        self.assertEqual(second.records[0]["request"]["id"], 1)
        self.assertIsNot(first.records, second.records)

        self.assertEqual(first.method("test_first_again", []), "0x1")
        self.assertEqual(first.records[1]["request"]["id"], 2)
        self.assertEqual(len(second.records), 1)


class RouteLegCollectionTests(unittest.TestCase):
    def test_completion_time_rejects_future_observations_and_is_retained(self):
        universe = _strict_cex_universe()
        wall_times = iter([
            datetime(2026, 8, 1, 12, tzinfo=timezone.utc),
            datetime(2026, 8, 1, 12, 0, 1, tzinfo=timezone.utc),
        ])

        def future_observation(_leg, *, raw_path, **_kwargs):
            raw_path.write_bytes(b"future")
            return {
                "status": "observed",
                "state_observed_at": "2099-01-01T00:00:00Z",
            }

        with tempfile.TemporaryDirectory() as directory_name:
            result = _collect_route_cohort(
                universe,
                cex_collector=future_observation,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=Path(directory_name),
                executor_factory=ThreadPoolExecutor,
                wall_clock=lambda: next(wall_times),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        self.assertEqual(
            result["collection_completed_at"], "2026-08-01T12:00:01Z"
        )
        route = result["route_rows"][0]
        self.assertEqual(route["validated_at"], result["collection_completed_at"])
        self.assertEqual(route["timing_status"], "unavailable")
        self.assertEqual(route["reason_code"], "invalid_state_timestamp")

    def test_collection_completion_time_is_bound_into_both_hashes(self):
        from scripts.collection_deadline import CollectionDeadline

        universe = _strict_cex_universe()
        start = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)

        def collect_once(raw_root, completed_at):
            wall_times = iter([start, completed_at])
            monotonic = FakeClock()
            return _collect_route_cohort(
                universe,
                cex_collector=_write_observed_raw,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=raw_root,
                snapshot_id="same-run",
                executor_factory=ThreadPoolExecutor,
                deadline=CollectionDeadline.for_duration(
                    1, clock=monotonic.monotonic, sleeper=monotonic.sleep
                ),
                wall_clock=lambda: next(wall_times),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            first = collect_once(
                root / "first",
                datetime(2026, 8, 1, 12, 0, 1, tzinfo=timezone.utc),
            )
            second = collect_once(
                root / "second",
                datetime(2026, 8, 1, 12, 0, 2, tzinfo=timezone.utc),
            )

        self.assertNotEqual(first["route_cohort_id"], second["route_cohort_id"])
        self.assertNotEqual(first["fingerprint"], second["fingerprint"])

    def test_exact_route_id_is_required_before_source_reads_or_raw_work(self):
        canonical = _strict_cex_universe()
        invalid_routes = [
            {key: value for key, value in canonical["routes"][0].items() if key != "route_id"},
            {**canonical["routes"][0], "route_id": ""},
            {**canonical["routes"][0], "route_id": "route:not-canonical"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for index, route in enumerate(invalid_routes):
                source_reads = []
                raw_root = root / str(index)
                with self.subTest(route_id=route.get("route_id")):
                    with self.assertRaisesRegex(
                        ValueError, "route_id must be canonical"
                    ):
                        _collect_route_cohort(
                            {**canonical, "routes": [route]},
                            cex_collector=_write_observed_raw,
                            dex_collector=lambda *_args, **_kwargs: None,
                            raw_root=raw_root,
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: (
                                source_reads.append("read") or "input-a"
                            ),
                            expected_source_generation="input-a",
                        )
                    self.assertEqual(source_reads, [])
                    self.assertFalse(raw_root.exists())

    def test_selected_market_type_must_match_market_id_before_source_read(self):
        universe = _strict_cex_universe()
        universe["selected_legs"][0]["market_type"] = "dex"
        source_reads = []
        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name) / "raw"
            with self.assertRaisesRegex(ValueError, "market type.*market_id"):
                _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    dex_block_resolver=lambda *_args, **_kwargs: {
                        "block_number": 1,
                        "block_timestamp": "2026-08-01T11:59:59Z",
                    },
                    raw_root=raw_root,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: (
                        source_reads.append("read") or "input-a"
                    ),
                    expected_source_generation="input-a",
                )
            self.assertEqual(source_reads, [])
            self.assertFalse(raw_root.exists())

    def test_malformed_market_ids_fail_before_source_read_or_raw_creation(self):
        valid_cex = "cex:beta:UNI/USDT"
        invalid_ids = (
            "cex::UNI/USDT",
            "cex:alpha:UNI",
            "cex:alpha:/USDT",
            "cex:alpha:UNI/",
            "cex:alpha:UNI//USDT",
            "cex:alpha:UNI/US DT",
            "cex:alpha:../USDT",
            "dex::swap:0xpool:UNI",
            "dex:eth::0xpool:UNI",
            "dex:eth:swap::UNI",
            "dex:eth:swap:0xpool:",
            "dex:eth:swap:0xpool",
            "dex:eth:swap:..:UNI",
            "dex:eth:swap:0xpool:UNI/USDT",
            "dex:eth:swap:0xAbC:UNI",
        )
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for index, market_id in enumerate(invalid_ids):
                source_reads = []
                raw_root = root / str(index)
                market_type = "dex" if market_id.startswith("dex:") else "cex"
                selected_leg = {
                    "market_id": market_id,
                    "market_type": market_type,
                }
                universe = {
                    "candidate_source_generation": "generation-a",
                    "selected_legs": [
                        selected_leg,
                        {
                            "market_id": valid_cex,
                            "market_type": "cex",
                        },
                    ],
                    "routes": [
                        _strict_route(
                            "UNI",
                            market_id,
                            valid_cex,
                            "prepositioned_inventory",
                        )
                    ],
                }
                with self.subTest(market_id=market_id):
                    with self.assertRaisesRegex(
                        ValueError, "route leg identity is invalid"
                    ):
                        _collect_route_cohort(
                            universe,
                            cex_collector=lambda *_args, **_kwargs: self.fail(
                                "invalid identity must not collect"
                            ),
                            dex_collector=lambda *_args, **_kwargs: self.fail(
                                "invalid identity must not collect"
                            ),
                            dex_block_resolver=lambda *_args, **_kwargs: self.fail(
                                "invalid identity must not resolve"
                            ),
                            raw_root=raw_root,
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: (
                                source_reads.append("read") or "input-a"
                            ),
                            expected_source_generation="input-a",
                        )
                    self.assertEqual(source_reads, [])
                    self.assertFalse(raw_root.exists())

    def test_selected_identity_fields_must_match_canonical_market_id(self):
        left = "cex:alpha:UNI/USDT"
        right = "dex:eth:swap:0xpool:UNI"
        cases = (
            (left, "cex", {"exchange": "beta"}),
            (left, "cex", {"exchange": " alpha "}),
            (left, "cex", {"cex_symbol": "AAVE/USDT"}),
            (left, "cex", {"cex_symbol": " UNI/USDT "}),
            (left, "cex", {"token_symbol": "AAVE"}),
            (left, "cex", {"token_symbol": " UNI "}),
            (right, "dex", {"chain": "arb"}),
            (right, "dex", {"chain": " eth "}),
            (right, "dex", {"dex": "other"}),
            (right, "dex", {"dex": " swap "}),
            (right, "dex", {"pool_address": "0xother"}),
            (right, "dex", {"pool_address": " 0xpool "}),
            (right, "dex", {"token_symbol": "AAVE"}),
            (right, "dex", {"token_symbol": " UNI "}),
        )
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for index, (market_id, market_type, conflicting) in enumerate(cases):
                source_reads = []
                raw_root = root / str(index)
                universe = {
                    "candidate_source_generation": "generation-a",
                    "selected_legs": [
                        {
                            "market_id": market_id,
                            "market_type": market_type,
                            **conflicting,
                        },
                        {
                            "market_id": "cex:beta:UNI/USDT",
                            "market_type": "cex",
                        },
                    ],
                    "routes": [
                        _strict_route(
                            "UNI",
                            market_id,
                            "cex:beta:UNI/USDT",
                            "prepositioned_inventory",
                        )
                    ],
                }
                with self.subTest(market_id=market_id, conflicting=conflicting):
                    with self.assertRaisesRegex(
                        ValueError, "route leg identity is invalid"
                    ):
                        _collect_route_cohort(
                            universe,
                            cex_collector=lambda *_args, **_kwargs: self.fail(
                                "identity conflict must not collect"
                            ),
                            dex_collector=lambda *_args, **_kwargs: self.fail(
                                "identity conflict must not collect"
                            ),
                            dex_block_resolver=lambda *_args, **_kwargs: self.fail(
                                "identity conflict must not resolve"
                            ),
                            raw_root=raw_root,
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: (
                                source_reads.append("read") or "input-a"
                            ),
                            expected_source_generation="input-a",
                        )
                    self.assertEqual(source_reads, [])
                    self.assertFalse(raw_root.exists())

    def test_dex_pool_identity_accepts_publication_maximum_length(self):
        pool = "P" + ("a" * 255)
        dex_market = "dex:eth:swap:{}:UNI".format(pool)
        cex_market = "cex:beta:UNI/USDT"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {
                    "market_id": dex_market,
                    "market_type": "dex",
                    "chain": "eth",
                    "dex": "swap",
                    "pool_address": pool,
                    "token_symbol": "UNI",
                },
                {"market_id": cex_market, "market_type": "cex"},
            ],
            "routes": [
                _strict_route(
                    "UNI", dex_market, cex_market, "prepositioned_inventory"
                )
            ],
        }

        def dex_observation(_leg, *, raw_path, fixed_block_number,
                            fixed_block_timestamp, **_kwargs):
            raw_path.write_bytes(b"long pool raw")
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "block_number": str(fixed_block_number),
                "block_timestamp": fixed_block_timestamp,
            }

        with tempfile.TemporaryDirectory() as directory_name:
            try:
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=dex_observation,
                    dex_block_resolver=lambda *_args, **_kwargs: {
                        "block_number": 123,
                        "block_timestamp": "2026-08-01T12:00:00Z",
                    },
                    raw_root=Path(directory_name),
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )
            except ValueError as error:
                self.fail("maximum-length canonical pool was rejected: {}".format(error))

        self.assertTrue(all(row["status"] == "observed" for row in result["legs"]))

    def test_fixed_block_lineage_is_strict_and_future_safe(self):
        left = "dex:eth:swap:0xone:UNI"
        right = "dex:eth:swap:0xtwo:UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": left, "market_type": "dex", "chain": "eth"},
                {"market_id": right, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [_strict_route("UNI", left, right, "atomic_onchain")],
        }
        invalid = [
            {"block_number": 0, "block_timestamp": "2026-08-01T11:59:59Z"},
            {"block_number": -1, "block_timestamp": "2026-08-01T11:59:59Z"},
            {"block_number": True, "block_timestamp": "2026-08-01T11:59:59Z"},
            {"block_number": 1, "block_timestamp": ""},
            {"block_number": 1, "block_timestamp": "not-a-time"},
            {"block_number": 1, "block_timestamp": "2099-01-01T00:00:00Z"},
        ]

        def dex_observation(_leg, *, raw_path, fixed_block_number,
                            fixed_block_timestamp, **_kwargs):
            raw_path.write_bytes(b"dex raw")
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "block_number": str(fixed_block_number),
                "block_timestamp": fixed_block_timestamp,
            }

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for index, lineage in enumerate(invalid):
                calls = []
                with self.subTest(lineage=lineage):
                    result = _collect_route_cohort(
                        universe,
                        cex_collector=lambda *_args, **_kwargs: None,
                        dex_collector=lambda *args, **kwargs: (
                            calls.append("called")
                            or dex_observation(*args, **kwargs)
                        ),
                        dex_block_resolver=lambda *_args, value=lineage, **_kwargs: value,
                        raw_root=root / str(index),
                        executor_factory=ThreadPoolExecutor,
                        wall_clock=lambda: datetime(
                            2026, 8, 1, 12, tzinfo=timezone.utc
                        ),
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    )
                    self.assertEqual(calls, [])
                    self.assertTrue(all(
                        row["reason_code"] == "fixed_block_unavailable"
                        for row in result["legs"]
                    ))

    def test_terminal_dex_leg_retains_normalized_resolved_block_lineage(self):
        left = "dex:eth:swap:0xone:UNI"
        right = "dex:eth:swap:0xtwo:UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": left, "market_type": "dex", "chain": "eth"},
                {"market_id": right, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [_strict_route("UNI", left, right, "atomic_onchain")],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            result = _collect_route_cohort(
                universe,
                cex_collector=lambda *_args, **_kwargs: None,
                dex_collector=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    ValueError("collection failed")
                ),
                dex_block_resolver=lambda *_args, **_kwargs: {
                    "block_number": 123,
                    "block_timestamp": "2026-08-01T20:00:00+08:00",
                },
                raw_root=Path(directory_name),
                executor_factory=ThreadPoolExecutor,
                wall_clock=lambda: datetime(
                    2026, 8, 1, 12, 0, 1, tzinfo=timezone.utc
                ),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        self.assertTrue(all(row["status"] == "failed" for row in result["legs"]))
        self.assertTrue(all(
            row["fixed_block_number"] == "123"
            and row["fixed_block_timestamp"] == "2026-08-01T12:00:00Z"
            for row in result["legs"]
        ))

    def test_hung_resolvers_cannot_consume_reserved_cex_capacity(self):
        cex_one = "cex:alpha:UNI/USDT"
        cex_two = "cex:alpha:UNI/USDC"
        dex_one = "dex:arb:swap:0xone:UNI"
        dex_two = "dex:eth:swap:0xtwo:UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": cex_one, "market_type": "cex", "exchange": "alpha"},
                {"market_id": cex_two, "market_type": "cex", "exchange": "alpha"},
                {"market_id": dex_one, "market_type": "dex", "chain": "arb"},
                {"market_id": dex_two, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                _strict_route(
                    "UNI", cex_one, cex_two, "prepositioned_inventory"
                ),
                _strict_route("UNI", dex_one, dex_two, "research_only"),
            ],
        }
        gates = {"arb": Event(), "eth": Event()}
        started = []

        def hung_resolver(chain, **_kwargs):
            gates[chain].wait()

        def cex_observation(leg, **_kwargs):
            started.append(leg["market_id"])
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        result = collect_route_cohort(
            universe,
            cex_collector=cex_observation,
            dex_collector=lambda *_args, **_kwargs: None,
            dex_block_resolver=hung_resolver,
            max_workers=2,
            cex_workers_per_venue=1,
            deadline_seconds=0.05,
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )
        for gate in gates.values():
            gate.set()

        self.assertEqual(len(started), 2)
        self.assertEqual(set(started), {cex_one, cex_two})
        self.assertTrue(all(
            row["status"] == "observed"
            for row in result["legs"] if row["market_id"].startswith("cex:")
        ))

    def test_one_worker_finishes_all_cex_work_before_any_resolver(self):
        cex_one = "cex:alpha:UNI/USDT"
        cex_two = "cex:alpha:UNI/USDC"
        dex_one = "dex:eth:swap:0xone:UNI"
        dex_two = "dex:eth:swap:0xtwo:UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": cex_one, "market_type": "cex", "exchange": "alpha"},
                {"market_id": cex_two, "market_type": "cex", "exchange": "alpha"},
                {"market_id": dex_one, "market_type": "dex", "chain": "eth"},
                {"market_id": dex_two, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                _strict_route(
                    "UNI", cex_one, cex_two, "prepositioned_inventory"
                ),
                _strict_route("UNI", dex_one, dex_two, "atomic_onchain"),
            ],
        }
        gate = Event()
        starts = []

        def cex_observation(leg, **_kwargs):
            starts.append(leg["market_id"])
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        result = collect_route_cohort(
            universe,
            cex_collector=cex_observation,
            dex_collector=lambda *_args, **_kwargs: None,
            dex_block_resolver=lambda *_args, **_kwargs: gate.wait(),
            max_workers=1,
            cex_workers_per_venue=1,
            deadline_seconds=0.05,
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )
        gate.set()

        self.assertEqual(len(starts), 2)
        self.assertEqual(set(starts), {cex_one, cex_two})
        self.assertTrue(all(
            row["status"] == "observed"
            for row in result["legs"] if row["market_id"].startswith("cex:")
        ))

    def test_repeated_blocked_default_calls_leave_no_workers_or_processes(self):
        universe = _strict_cex_universe()
        baseline_processes = {process.pid for process in multiprocessing.active_children()}
        baseline_threads = {
            thread.ident
            for thread in enumerate_threads()
            if thread.name.startswith("route-cohort-")
        }
        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name)
            for _index in range(3):
                gate = multiprocessing.get_context("fork").Event()

                def blocked_collector(*_args, **_kwargs):
                    gate.wait()

                result = _collect_route_cohort(
                    universe,
                    cex_collector=blocked_collector,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    max_workers=1,
                    deadline_seconds=0.03,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )
                self.assertTrue(all(
                    row["status"] == "deadline_exceeded"
                    for row in result["legs"]
                ))

        time.sleep(0.05)
        self.assertEqual(
            {process.pid for process in multiprocessing.active_children()},
            baseline_processes,
        )
        self.assertEqual(
            {
                thread.ident
                for thread in enumerate_threads()
                if thread.name.startswith("route-cohort-")
            },
            baseline_threads,
        )

    def test_default_process_executor_creates_no_monitor_threads(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            with patch(
                "scripts.collect_route_cohort.Thread",
                side_effect=AssertionError("process executor created a thread"),
            ):
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=Path(directory_name),
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )

        self.assertTrue(all(row["status"] == "observed" for row in result["legs"]))

    def test_multithreaded_caller_fails_before_source_read_raw_or_fork(self):
        universe = _strict_cex_universe()
        gate = Event()
        caller_thread = TestThread(
            target=gate.wait,
            name="unrelated-caller-thread",
        )
        caller_thread.start()
        source_reads = []
        baseline_processes = {
            process.pid for process in multiprocessing.active_children()
        }
        try:
            with tempfile.TemporaryDirectory() as directory_name:
                raw_root = Path(directory_name) / "raw"
                with self.assertRaisesRegex(RuntimeError, "single-threaded"):
                    _collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=raw_root,
                        source_generation_reader=lambda: (
                            source_reads.append("read") or "input-a"
                        ),
                        expected_source_generation="input-a",
                    )
                self.assertFalse(raw_root.exists())
        finally:
            gate.set()
            caller_thread.join(timeout=1)

        self.assertEqual(source_reads, [])
        self.assertEqual(
            {process.pid for process in multiprocessing.active_children()},
            baseline_processes,
        )

    def test_default_fork_path_is_clean_under_deprecation_warnings_as_errors(self):
        code = textwrap.dedent(
            """
            from pathlib import Path
            import tempfile
            import time
            from scripts.collect_route_cohort import collect_route_cohort

            left = 'cex:alpha:UNI/USDT'
            right = 'cex:beta:UNI/USDT'
            universe = {
                'candidate_source_generation': 'generation-a',
                'selected_legs': [
                    {'market_id': left, 'market_type': 'cex'},
                    {'market_id': right, 'market_type': 'cex'},
                ],
                'routes': [{
                    'route_id': 'route:UNI:{}->{}:prepositioned_inventory'.format(left, right),
                    'token_symbol': 'UNI',
                    'buy_market_id': left,
                    'sell_market_id': right,
                    'route_mode': 'prepositioned_inventory',
                }],
            }

            def collect(_leg, *, raw_path, **_kwargs):
                time.sleep(0.1)
                raw_path.write_bytes(b'raw')
                return {
                    'status': 'observed',
                    'state_observed_at': '2026-08-01T12:00:00Z',
                }

            result = collect_route_cohort(
                universe,
                cex_collector=collect,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=Path(tempfile.mkdtemp()),
                source_generation_reader=lambda: 'input-a',
                expected_source_generation='input-a',
            )
            assert all(row['status'] == 'observed' for row in result['legs'])
            print('ok')
            """
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-W",
                "error::DeprecationWarning",
                "-c",
                code,
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            text=True,
            capture_output=True,
            timeout=2,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "ok")

    def test_direct_collection_requires_explicit_raw_root_without_artifacts(self):
        universe = _strict_cex_universe()
        source_reads = []
        previous_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as directory_name:
            temporary_cwd = Path(directory_name)
            os.chdir(str(temporary_cwd))
            try:
                with self.assertRaisesRegex(ValueError, "raw_root is required"):
                    _collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: (
                            source_reads.append("read") or "input-a"
                        ),
                        expected_source_generation="input-a",
                    )
            finally:
                os.chdir(str(previous_cwd))
            self.assertFalse((temporary_cwd / "data").exists())
        self.assertEqual(source_reads, [])

    def test_raw_root_rejects_existing_and_broken_symlinks(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            target = root / "target"
            target.mkdir()
            existing_link = root / "existing-link"
            broken_link = root / "broken-link"
            existing_link.symlink_to(target, target_is_directory=True)
            broken_link.symlink_to(root / "missing", target_is_directory=True)
            for raw_root in (existing_link, broken_link):
                with self.subTest(raw_root=raw_root.name):
                    with self.assertRaisesRegex(ValueError, "raw_root.*symlink"):
                        _collect_route_cohort(
                            universe,
                            cex_collector=_write_observed_raw,
                            dex_collector=lambda *_args, **_kwargs: None,
                            raw_root=raw_root,
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: "input-a",
                            expected_source_generation="input-a",
                        )
            self.assertEqual(list(target.iterdir()), [])

    def test_raw_root_rejects_caller_controlled_symlink_ancestor(self):
        universe = _strict_cex_universe()
        source_reads = []
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            target = root / "target"
            target.mkdir()
            alias = root / "caller-alias"
            alias.symlink_to(target, target_is_directory=True)
            raw_root = alias / "raw"
            with self.assertRaisesRegex(ValueError, "raw_root.*symlink"):
                _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: (
                        source_reads.append("read") or "input-a"
                    ),
                    expected_source_generation="input-a",
                )
            self.assertFalse((target / "raw").exists())
        self.assertEqual(source_reads, [])

    def test_symlinked_stage_directory_cannot_import_external_evidence(self):
        universe = _strict_cex_universe()
        external_raw = b"EXTERNAL_STAGE_EVIDENCE_SENTINEL"
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            external = root / "external"
            external.mkdir()
            (external / "response.json").write_bytes(external_raw)
            displaced = root / "displaced-original-stage"

            def collector(leg, *, raw_path, **_kwargs):
                if leg["market_id"].startswith("cex:alpha:"):
                    raw_path.parent.rename(displaced)
                    raw_path.parent.symlink_to(
                        external, target_is_directory=True
                    )
                    return {
                        "status": "observed",
                        "state_observed_at": "2026-08-01T12:00:00Z",
                        "raw_response_sha256": hashlib.sha256(
                            external_raw
                        ).hexdigest(),
                    }
                return _write_observed_raw(leg, raw_path=raw_path)

            result = _collect_route_cohort(
                universe,
                cex_collector=collector,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=root / "raw",
                max_workers=1,
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

            alpha = next(
                row for row in result["legs"]
                if row["market_id"].startswith("cex:alpha:")
            )
            self.assertEqual(alpha["status"], "failed")
            self.assertEqual(
                alpha["reason_code"], "raw_evidence_path_unsafe"
            )
            self.assertEqual(
                (external / "response.json").read_bytes(), external_raw
            )
            accepted = root / "raw" / result["raw_evidence_run_id"] / "accepted"
            self.assertEqual(len(list(accepted.iterdir())), 1)
            self.assertTrue(all(not path.is_symlink() for path in accepted.iterdir()))

    def test_swapped_real_stage_directory_is_not_promoted(self):
        universe = _strict_cex_universe()
        replacement_raw = b"REAL_DIRECTORY_SWAP_SENTINEL"
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            replacement = root / "replacement-stage"
            replacement.mkdir()
            (replacement / "response.json").write_bytes(replacement_raw)
            displaced = root / "original-stage"

            def collector(leg, *, raw_path, **_kwargs):
                if leg["market_id"].startswith("cex:alpha:"):
                    raw_path.parent.rename(displaced)
                    replacement.rename(raw_path.parent)
                    return {
                        "status": "observed",
                        "state_observed_at": "2026-08-01T12:00:00Z",
                        "raw_response_sha256": hashlib.sha256(
                            replacement_raw
                        ).hexdigest(),
                    }
                return _write_observed_raw(leg, raw_path=raw_path)

            result = _collect_route_cohort(
                universe,
                cex_collector=collector,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=root / "raw",
                max_workers=1,
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

            alpha = next(
                row for row in result["legs"]
                if row["market_id"].startswith("cex:alpha:")
            )
            self.assertEqual(alpha["status"], "failed")
            self.assertEqual(
                alpha["reason_code"], "raw_evidence_path_unsafe"
            )
            accepted = root / "raw" / result["raw_evidence_run_id"] / "accepted"
            self.assertEqual(len(list(accepted.iterdir())), 1)

    def test_swapped_accepted_root_cannot_export_staged_evidence(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            external = root / "external-accepted"
            external.mkdir()
            displaced = root / "original-accepted"

            def collector(leg, *, raw_path, **_kwargs):
                if leg["market_id"].startswith("cex:alpha:"):
                    accepted = raw_path.parents[2] / "accepted"
                    accepted.rename(displaced)
                    accepted.symlink_to(external, target_is_directory=True)
                return _write_observed_raw(leg, raw_path=raw_path)

            result = _collect_route_cohort(
                universe,
                cex_collector=collector,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=root / "raw",
                max_workers=1,
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

            self.assertTrue(all(
                row["status"] == "failed"
                and row["reason_code"] == "raw_evidence_path_unsafe"
                for row in result["legs"]
            ))
            self.assertEqual(list(external.iterdir()), [])

    def test_accepted_root_swap_between_guard_and_rename_cannot_export_evidence(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            raw_root = root / "raw"
            external = root / "external-accepted"
            external.mkdir()
            displaced = root / "original-accepted"
            swapped = []

            def swap_then_rename(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                if not swapped:
                    accepted = raw_root / "stable-run" / "accepted"
                    accepted.rename(displaced)
                    accepted.symlink_to(external, target_is_directory=True)
                    swapped.append(True)
                return os.rename(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory_fd,
                    dst_dir_fd=destination_directory_fd,
                )

            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=swap_then_rename,
            ):
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    snapshot_id="stable-run",
                    max_workers=1,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )

            self.assertTrue(swapped)
            self.assertEqual(list(external.iterdir()), [])
            self.assertFalse(
                (raw_root / "stable-run" / "accepted").is_symlink()
            )
            self.assertTrue(all(
                row["status"] == "failed"
                and row["reason_code"] == "raw_evidence_path_unsafe"
                for row in result["legs"]
            ))

    def test_rollback_exchanges_staging_collision_before_clearing_swapped_accepted(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            raw_root = root / "raw"
            external_accepted = root / "external-accepted"
            attacked = []

            def promote_then_swap_and_collide(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                result = os.rename(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory_fd,
                    dst_dir_fd=destination_directory_fd,
                )
                if not attacked:
                    accepted = raw_root / "stable-run" / "accepted"
                    accepted.rename(external_accepted)
                    accepted.symlink_to(
                        external_accepted,
                        target_is_directory=True,
                    )
                    os.symlink(
                        str(root / "attacker-controlled"),
                        source_name,
                        dir_fd=source_directory_fd,
                    )
                    attacked.append(source_name)
                return result

            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=promote_then_swap_and_collide,
            ):
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    snapshot_id="stable-run",
                    max_workers=1,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )

            self.assertTrue(attacked)
            self.assertEqual(list(external_accepted.iterdir()), [])
            run_dir = raw_root / "stable-run"
            self.assertFalse((run_dir / "accepted").is_symlink())
            recovered = run_dir / "staging" / attacked[0]
            self.assertTrue(recovered.is_dir())
            self.assertFalse(recovered.is_symlink())
            self.assertEqual(
                (recovered / "response.json").read_bytes(),
                b"observed raw",
            )
            self.assertTrue(all(
                row["status"] == "failed"
                and row["reason_code"] == "raw_evidence_path_unsafe"
                for row in result["legs"]
            ))

    def test_rollback_exchange_failure_hard_fails_after_descriptor_cleanup(self):
        universe = _strict_cex_universe()
        returned = []
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            raw_root = root / "raw"
            external_accepted = root / "external-accepted"
            attacked = []

            def promote_then_swap_and_collide(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                result = os.rename(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory_fd,
                    dst_dir_fd=destination_directory_fd,
                )
                if not attacked:
                    accepted = raw_root / "stable-run" / "accepted"
                    accepted.rename(external_accepted)
                    accepted.symlink_to(
                        external_accepted,
                        target_is_directory=True,
                    )
                    os.symlink(
                        str(root / "attacker-controlled"),
                        source_name,
                        dir_fd=source_directory_fd,
                    )
                    attacked.append(source_name)
                return result

            descriptors_before = len(os.listdir("/dev/fd"))
            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=promote_then_swap_and_collide,
            ), patch(
                "scripts.collect_route_cohort._exchange_directory_entries",
                side_effect=OSError("injected exchange failure"),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "^raw evidence rollback could not be verified$",
                ):
                    returned.append(_collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=raw_root,
                        snapshot_id="stable-run",
                        max_workers=1,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    ))
            self.assertEqual(len(os.listdir("/dev/fd")), descriptors_before)
            self.assertEqual(returned, [])

    def test_rollback_quarantine_failure_hard_fails_after_descriptor_cleanup(self):
        universe = _strict_cex_universe()
        returned = []
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            raw_root = root / "raw"
            external_accepted = root / "external-accepted"
            attacked = []

            def fail_quarantine_after_collision(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                if destination_name.startswith(".rejected-"):
                    raise OSError("injected quarantine failure")
                result = os.rename(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory_fd,
                    dst_dir_fd=destination_directory_fd,
                )
                if not attacked:
                    accepted = raw_root / "stable-run" / "accepted"
                    accepted.rename(external_accepted)
                    accepted.symlink_to(
                        external_accepted,
                        target_is_directory=True,
                    )
                    os.symlink(
                        str(root / "attacker-controlled"),
                        source_name,
                        dir_fd=source_directory_fd,
                    )
                    attacked.append(source_name)
                return result

            descriptors_before = len(os.listdir("/dev/fd"))
            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=fail_quarantine_after_collision,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "^raw evidence rollback could not be verified$",
                ):
                    returned.append(_collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=raw_root,
                        snapshot_id="stable-run",
                        max_workers=1,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    ))
            self.assertEqual(len(os.listdir("/dev/fd")), descriptors_before)
            self.assertEqual(returned, [])

    def test_rollback_state_verification_failure_hard_fails_after_descriptor_cleanup(self):
        universe = _strict_cex_universe()
        returned = []
        with tempfile.TemporaryDirectory() as directory_name:
            descriptors_before = len(os.listdir("/dev/fd"))
            with patch(
                "scripts.collect_route_cohort._post_promotion_failure",
                return_value="raw_evidence_hash_mismatch",
            ), patch(
                "scripts.collect_route_cohort._rollback_state_is_safe",
                return_value=False,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "^raw evidence rollback could not be verified$",
                ):
                    returned.append(_collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=Path(directory_name),
                        snapshot_id="stable-run",
                        max_workers=1,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    ))
            self.assertEqual(len(os.listdir("/dev/fd")), descriptors_before)
            self.assertEqual(returned, [])

    def test_post_promotion_raw_tamper_is_rejected_and_rolled_back(self):
        universe = _strict_cex_universe()

        def tampering_rename(
            source_name,
            destination_name,
            *,
            source_directory_fd,
            destination_directory_fd,
        ):
            result = os.rename(
                source_name,
                destination_name,
                src_dir_fd=source_directory_fd,
                dst_dir_fd=destination_directory_fd,
            )
            promoted_descriptor = os.open(
                destination_name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=destination_directory_fd,
            )
            try:
                response_descriptor = os.open(
                    "response.json",
                    os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW,
                    dir_fd=promoted_descriptor,
                )
                try:
                    os.write(
                        response_descriptor,
                        b"POST_PROMOTION_TAMPER_SENTINEL",
                    )
                finally:
                    os.close(response_descriptor)
            finally:
                os.close(promoted_descriptor)
            return result

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=tampering_rename,
            ):
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=root,
                    max_workers=1,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )

            self.assertTrue(all(
                row["status"] == "failed"
                and row["reason_code"] == "raw_evidence_hash_mismatch"
                for row in result["legs"]
            ))
            run_dir = root / result["raw_evidence_run_id"]
            self.assertEqual(list((run_dir / "accepted").iterdir()), [])
            self.assertEqual(len(list((run_dir / "staging").iterdir())), 2)

    def test_missing_or_mismatched_raw_evidence_cannot_be_accepted(self):
        universe = _strict_cex_universe()

        def missing_raw(_leg, **_kwargs):
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        def mismatched_raw(_leg, *, raw_path, **_kwargs):
            raw_path.write_bytes(b"actual")
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "raw_response_sha256": "0" * 64,
            }

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for name, collector, reason in (
                ("missing", missing_raw, "raw_evidence_missing"),
                ("mismatch", mismatched_raw, "raw_evidence_hash_mismatch"),
            ):
                with self.subTest(name=name):
                    result = _collect_route_cohort(
                        universe,
                        cex_collector=collector,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=root / name,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    )
                    self.assertTrue(all(
                        row["status"] == "failed" and row["reason_code"] == reason
                        for row in result["legs"]
                    ))
                    run_dir = root / name / result["raw_evidence_run_id"]
                    self.assertEqual(list((run_dir / "accepted").iterdir()), [])

    def test_live_main_returns_only_the_fingerprint_bound_cohort(self):
        universe = _strict_cex_universe()
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]

        def cex_observation(_market, *, raw_path, **_kwargs):
            raw = b"cli raw"
            raw_path.write_bytes(raw)
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
            }, []

        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(universe), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ):
                result = main(
                    ["--data-dir", str(data_dir), "--deadline-seconds", "1"],
                    cex_collector=cex_observation,
                    executor_factory=ThreadPoolExecutor,
                )

        self.assertNotIn("dry_run", result)
        self.assertNotIn("universe_path", result)
        without_hashes = {
            key: value
            for key, value in result.items()
            if key not in {"route_cohort_id", "fingerprint"}
        }
        expected_id = "cohort:" + hashlib.sha256(json.dumps(
            without_hashes,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        self.assertEqual(result["route_cohort_id"], expected_id)
        expected_fingerprint = hashlib.sha256(json.dumps(
            {**without_hashes, "route_cohort_id": expected_id},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        self.assertEqual(result["fingerprint"], expected_fingerprint)

    def test_cli_recomputes_full_input_generation_and_rejects_inventory_mutation(self):
        universe = {
            "candidate_source_generation": "candidate-a",
            "selection_window": {"start": "2026-08-01", "end": "2026-08-01"},
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }
        first = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT", "observed_at": "2026-08-01T00:00:00Z"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT", "observed_at": "2026-08-01T00:00:00Z"},
        ]
        mutated = [{**row, "observed_at": "2026-08-01T00:00:01Z"} for row in first]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                side_effect=[first, mutated],
            ):
                with self.assertRaisesRegex(ValueError, "collection input generation changed"):
                    main(
                        ["--data-dir", str(data_dir), "--start", "2026-08-01", "--end", "2026-08-01", "--deadline-seconds", "1"],
                        cex_collector=lambda *_args, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}, []),
                    )

    def test_cli_generation_includes_unfiltered_route_universe(self):
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:alpha:AAVE/USDT", "market_type": "cex", "token_symbol": "AAVE"},
                {"market_id": "cex:beta:AAVE/USDT", "market_type": "cex", "token_symbol": "AAVE"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "AAVE", "buy_market_id": "cex:alpha:AAVE/USDT", "sell_market_id": "cex:beta:AAVE/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        mutated = {
            **universe,
            "routes": [universe["routes"][0], {
                **universe["routes"][1],
                "route_class": "mutated-unselected-input",
            }],
        }
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            with patch(
                "scripts.collect_route_cohort._load_universe_for_cli",
                side_effect=[
                    _complete_test_routes(universe),
                    _complete_test_routes(mutated),
                ],
            ), patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ):
                with self.assertRaisesRegex(
                    ValueError, "collection input generation changed"
                ):
                    main(
                        ["--data-dir", str(data_dir), "--tokens", "UNI"],
                        cex_collector=lambda *_args, **_kwargs: self.fail(
                            "must reject before collection"
                        ),
                    )
            self.assertFalse((data_dir / "raw").exists())

    def test_selected_identity_conflict_and_returned_identity_mismatch_fail_closed(self):
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "AAVE"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ) as load_catalog:
                with self.assertRaisesRegex(ValueError, "route leg identity is invalid"):
                    main(["--data-dir", str(data_dir)], cex_collector=lambda *_args, **_kwargs: self.fail("must not collect"))
                load_catalog.assert_not_called()

        direct = {**universe, "selected_legs": [{**row, "token_symbol": "UNI"} for row in universe["selected_legs"]]}
        result = collect_route_cohort(
            direct,
            cex_collector=lambda leg, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "token_symbol": "AAVE"}, []),
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX"),
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )
        self.assertTrue(all(row["reason_code"] == "collector_identity_mismatch" for row in result["legs"]))

    def test_partial_dex_collector_pool_identity_is_case_sensitive(self):
        cex_market = "cex:alpha:UNI/USDT"
        dex_market = "dex:sol:orca:PoolCase:UNI"
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {
                    "market_id": cex_market,
                    "market_type": "cex",
                    "exchange": "alpha",
                },
                {
                    "market_id": dex_market,
                    "market_type": "dex",
                    "chain": "sol",
                    "dex": "orca",
                    "pool_address": "PoolCase",
                    "token_symbol": "UNI",
                },
            ],
            "routes": [
                _strict_route(
                    "UNI",
                    cex_market,
                    dex_market,
                    "prepositioned_inventory",
                )
            ],
        }
        fixed_timestamp = "2026-08-01T12:00:00Z"

        result = collect_route_cohort(
            universe,
            cex_collector=_write_observed_raw,
            dex_collector=lambda _leg, **_kwargs: {
                "status": "partial",
                "state_observed_at": fixed_timestamp,
                "block_number": 123,
                "block_timestamp": fixed_timestamp,
                "pool_address": "poolcase",
            },
            dex_block_resolver=lambda *_args, **_kwargs: {
                "block_number": 123,
                "block_timestamp": fixed_timestamp,
            },
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )

        dex_leg = next(
            row for row in result["legs"] if row["market_id"] == dex_market
        )
        self.assertEqual(dex_leg["status"], "failed")
        self.assertEqual(
            dex_leg["reason_code"],
            "collector_identity_mismatch",
        )

    def test_blocked_worker_does_not_keep_subprocess_alive_past_deadline(self):
        code = textwrap.dedent(
            """
            from pathlib import Path
            from threading import Event
            import tempfile
            from scripts.collect_route_cohort import collect_route_cohort
            universe = {
                'candidate_source_generation': 'candidate-a',
                'selected_legs': [
                    {'market_id': 'cex:alpha:UNI/USDT', 'market_type': 'cex'},
                    {'market_id': 'cex:beta:UNI/USDT', 'market_type': 'cex'},
                ],
                'routes': [{'route_id': 'route:UNI:cex:alpha:UNI/USDT->cex:beta:UNI/USDT:prepositioned_inventory', 'token_symbol': 'UNI', 'buy_market_id': 'cex:alpha:UNI/USDT', 'sell_market_id': 'cex:beta:UNI/USDT', 'route_mode': 'prepositioned_inventory'}],
            }
            gate = Event()
            result = collect_route_cohort(
                universe, deadline_seconds=0.05,
                cex_collector=lambda *_args, **_kwargs: gate.wait(),
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=Path(tempfile.mkdtemp()),
                source_generation_reader=lambda: 'input-a',
                expected_source_generation='input-a',
            )
            print([row['status'] for row in result['legs']])
            """
        )
        completed = subprocess.run(
            [sys.executable, "-c", code], cwd=str(Path(__file__).resolve().parents[1]),
            text=True, capture_output=True, timeout=1,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("deadline_exceeded", completed.stdout)

    def test_late_raw_write_remains_staging_and_never_becomes_accepted(self):
        started = Event()
        release = Event()
        finished = Event()
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{
                "token_symbol": "UNI",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "cex:beta:UNI/USDT",
                "route_mode": "prepositioned_inventory",
            }],
        }

        def late_collector(_leg, *, raw_path, **_kwargs):
            started.set()
            release.wait()
            raw_path.write_text("late", encoding="utf-8")
            finished.set()
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            result = collect_route_cohort(
                universe,
                cex_collector=late_collector,
                dex_collector=lambda *_args, **_kwargs: None,
                deadline_seconds=0.05,
                max_workers=1,
                raw_root=root,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )
            self.assertTrue(started.is_set())
            release.set()
            self.assertTrue(finished.wait(timeout=0.5))
            run_dir = root / result["raw_evidence_run_id"]
            self.assertEqual(list((run_dir / "accepted").iterdir()), [])
            self.assertEqual(
                [path.read_text(encoding="utf-8") for path in (run_dir / "staging").glob("*/response.json")],
                ["late"],
            )

    def test_resolver_is_scheduled_fairly_with_cex_collection(self):
        cex_started = Event()
        resolver_started = Event()
        resolver_gate = Event()
        cex_saw_resolver = []
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "dex:eth:swap:0xone:UNI", "route_mode": "prepositioned_inventory"},
            ],
        }

        def resolver(_chain, **_kwargs):
            resolver_started.set()
            resolver_gate.wait()

        def collect_cex(*_args, **_kwargs):
            cex_started.set()
            cex_saw_resolver.append(resolver_started.wait(timeout=0.2))
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        result = collect_route_cohort(
            universe,
            cex_collector=collect_cex,
            dex_collector=lambda *_args, **_kwargs: self.fail("unresolved DEX must not collect"),
            dex_block_resolver=resolver, max_workers=2, deadline_seconds=0.05,
            source_generation_reader=lambda: "input-a", expected_source_generation="input-a",
        )
        self.assertTrue(cex_started.is_set())
        self.assertTrue(all(cex_saw_resolver))
        self.assertTrue(all(
            row["status"] == "observed"
            for row in result["legs"] if row["market_id"].startswith("cex:")
        ))
        self.assertEqual(
            next(row for row in result["legs"] if row["market_id"].startswith("dex:"))["status"],
            "deadline_exceeded",
        )

    def test_snapshot_id_traversal_and_raw_run_collision_are_rejected(self):
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }
        kwargs = {
            "cex_collector": lambda *_args, **_kwargs: {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"},
            "dex_collector": lambda *_args, **_kwargs: None,
            "source_generation_reader": lambda: "input-a",
            "expected_source_generation": "input-a",
        }
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            with self.assertRaisesRegex(ValueError, "snapshot_id"):
                collect_route_cohort(universe, raw_root=root, snapshot_id="../escape", **kwargs)
            explicit = collect_route_cohort(
                universe, raw_root=root, snapshot_id="same-run", **kwargs
            )
            with self.assertRaisesRegex(FileExistsError, "same-run"):
                collect_route_cohort(universe, raw_root=root, snapshot_id="same-run", **kwargs)
            wall_time = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
            first = collect_route_cohort(
                universe, raw_root=root, wall_clock=lambda: wall_time, **kwargs
            )
            second = collect_route_cohort(
                universe, raw_root=root, wall_clock=lambda: wall_time, **kwargs
            )

            self.assertNotEqual(
                first["raw_evidence_run_id"], second["raw_evidence_run_id"]
            )
            accepted_names = [
                path.name
                for path in (root / explicit["raw_evidence_run_id"] / "accepted").iterdir()
            ]
            self.assertEqual(len(accepted_names), 2)
            self.assertTrue(all(
                len(name) == 64 and set(name) <= set("0123456789abcdef")
                for name in accepted_names
            ))

    def test_task3_cex_adapter_receives_only_its_declared_arguments(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {
                    "market_id": "cex:alpha:UNI/USDT",
                    "market_type": "cex",
                    "token_symbol": "UNI",
                    "exchange": "alpha",
                    "cex_symbol": "UNI/USDT",
                },
                {
                    "market_id": "cex:beta:UNI/USDT",
                    "market_type": "cex",
                    "token_symbol": "UNI",
                    "exchange": "beta",
                    "cex_symbol": "UNI/USDT",
                },
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        calls = []

        def cex_primitive(market, *, snapshot_id, raw_path, deadline):
            calls.append((market["market_id"], snapshot_id, raw_path.name, deadline))
            return ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "source_endpoint": "https://user:pass@example.test/depth?api_key=private", "credential": "private"}, [])

        with tempfile.TemporaryDirectory() as directory_name:
            result = collect_route_cohort(
                universe,
                cex_collector=cex_primitive,
                dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
                raw_root=Path(directory_name),
                snapshot_id="cohort-test",
                source_generation_reader=lambda: "generation-a",
                expected_source_generation="generation-a",
            )

        self.assertCountEqual(
            [call[0] for call in calls],
            ["cex:alpha:UNI/USDT", "cex:beta:UNI/USDT"],
        )
        self.assertTrue(all(call[1] == "cohort-test" for call in calls))
        self.assertEqual([row["status"] for row in result["legs"]], ["observed", "observed"])
        self.assertEqual(result["legs"][0]["source_endpoint"], "https://example.test/depth")
        self.assertNotIn("credential", result["legs"][0])

    def test_safe_leg_projection_recursively_drops_non_json_objects(self):
        value = {
            "safe": {
                "items": [
                    "keep",
                    Path("/PRIVATE_PATH_SENTINEL"),
                    ValueError("PRIVATE_EXCEPTION_SENTINEL"),
                    object(),
                    float("nan"),
                    ("tuple-value",),
                ],
                "url": (
                    "https://user:pass@example.test/path?"
                    "api_key=PRIVATE_QUERY_SENTINEL#fragment"
                ),
            }
        }

        projected = _safe_leg_projection(value)

        self.assertEqual(projected, {
            "safe": {
                "items": ["keep", ["tuple-value"]],
                "url": "https://example.test/path",
            }
        })
        json.dumps(projected, allow_nan=False)

    def test_safe_leg_projection_drops_local_paths_and_path_like_keys(self):
        value = {
            "safe": "retained",
            "absolute": "/private/tmp/PRIVATE_ABSOLUTE_PATH_SENTINEL.json",
            "home": "~/PRIVATE_HOME_PATH_SENTINEL.json",
            "unc": r"\\server\share\PRIVATE_UNC_PATH_SENTINEL.json",
            "drive": r"C:\Users\name\PRIVATE_DRIVE_PATH_SENTINEL.json",
            "file_uri": "file:///private/tmp/PRIVATE_FILE_URI_SENTINEL.json",
            "artifact_path": "relative/PRIVATE_PATH_KEY_SENTINEL.json",
            "nested": [{"cachePath": "PRIVATE_PATH_KEY_SENTINEL"}],
        }

        projected = _safe_leg_projection(value)

        self.assertEqual(projected, {"safe": "retained", "nested": [{}]})
        encoded = json.dumps(projected, allow_nan=False)
        for sentinel in (
            "PRIVATE_ABSOLUTE_PATH_SENTINEL",
            "PRIVATE_HOME_PATH_SENTINEL",
            "PRIVATE_UNC_PATH_SENTINEL",
            "PRIVATE_DRIVE_PATH_SENTINEL",
            "PRIVATE_FILE_URI_SENTINEL",
            "PRIVATE_PATH_KEY_SENTINEL",
        ):
            self.assertNotIn(sentinel, encoded)

    def test_safe_leg_projection_rejects_non_http_credentials_and_parent_paths(self):
        projected = _safe_leg_projection({
            "endpoint": (
                "wss://user:PRIVATE_PASS@example.test/ws?"
                "token=PRIVATE_TOKEN#PRIVATE_FRAGMENT"
            ),
            "database": (
                "postgres://user:PRIVATE_DB_PASS@example.test/market?"
                "sslkey=PRIVATE_SSL_KEY"
            ),
            "private_key": "PRIVATE_KEY_SENTINEL",
            "provenance": "../PRIVATE_PARENT/secret.json",
            "windows_parent": r"..\PRIVATE_WINDOWS_PARENT\secret.json",
            "middle_parent": "safe/../PRIVATE_MIDDLE_PARENT/secret.json",
            "middle_dot": r"safe\.\PRIVATE_MIDDLE_DOT\secret.json",
            "opaque_wss": (
                "wss:user:PRIVATE_OPAQUE_WSS@example.test/ws?"
                "token=PRIVATE_OPAQUE_WSS_TOKEN"
            ),
            "opaque_postgres": (
                "postgres:user:PRIVATE_OPAQUE_DB@example.test/market?"
                "sslkey=PRIVATE_OPAQUE_DB_KEY"
            ),
            "opaque_https": (
                "https:user:PRIVATE_OPAQUE_HTTPS@example.test/depth?"
                "token=PRIVATE_OPAQUE_HTTPS_TOKEN"
            ),
            "market_symbol": "UNI/USDT",
            "market_id": "dex:sol:orca:PoolCase:UNI",
        })

        self.assertEqual(projected, {
            "market_symbol": "UNI/USDT",
            "market_id": "dex:sol:orca:PoolCase:UNI",
        })
        encoded = json.dumps(projected, allow_nan=False)
        for sentinel in (
            "PRIVATE_PASS",
            "PRIVATE_TOKEN",
            "PRIVATE_FRAGMENT",
            "PRIVATE_DB_PASS",
            "PRIVATE_SSL_KEY",
            "PRIVATE_KEY_SENTINEL",
            "PRIVATE_PARENT",
            "PRIVATE_WINDOWS_PARENT",
            "PRIVATE_MIDDLE_PARENT",
            "PRIVATE_MIDDLE_DOT",
            "PRIVATE_OPAQUE_WSS",
            "PRIVATE_OPAQUE_WSS_TOKEN",
            "PRIVATE_OPAQUE_DB",
            "PRIVATE_OPAQUE_DB_KEY",
            "PRIVATE_OPAQUE_HTTPS",
            "PRIVATE_OPAQUE_HTTPS_TOKEN",
        ):
            self.assertNotIn(sentinel, encoded)

    def test_nested_leg_provenance_is_secret_free_and_json_safe(self):
        universe = _strict_cex_universe()

        def collector(_leg, **_kwargs):
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "provenance": {
                    "api_key": "PRIVATE_API_KEY_SENTINEL",
                    "Authorization": "Bearer PRIVATE_AUTH_SENTINEL",
                    "safe_label": "retained",
                    "cache_path": "relative/PRIVATE_PATH_KEY_SENTINEL.json",
                    "local_source": "/private/tmp/PRIVATE_PATH_VALUE_SENTINEL.json",
                    "nested": [
                        {
                            "endpoint": (
                                "https://user:pass@example.test/depth?"
                                "token=PRIVATE_QUERY_SENTINEL"
                            ),
                            "token": "PRIVATE_TOKEN_SENTINEL",
                        },
                        (
                            "https://user:pass@example.test/tuple?"
                            "api_key=PRIVATE_TUPLE_SENTINEL",
                            {"safe": "value", "secret": "PRIVATE_SECRET_SENTINEL"},
                        ),
                    ],
                },
            }

        with tempfile.TemporaryDirectory() as directory_name:
            result = collect_route_cohort(
                universe,
                cex_collector=collector,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=Path(directory_name),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        self.assertEqual(result["legs"][0]["provenance"], {
            "safe_label": "retained",
            "nested": [
                {"endpoint": "https://example.test/depth"},
                [
                    "https://example.test/tuple",
                    {"safe": "value"},
                ],
            ],
        })
        encoded = json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
        for sentinel in (
            "PRIVATE_API_KEY_SENTINEL",
            "PRIVATE_AUTH_SENTINEL",
            "PRIVATE_QUERY_SENTINEL",
            "PRIVATE_TOKEN_SENTINEL",
            "PRIVATE_TUPLE_SENTINEL",
            "PRIVATE_SECRET_SENTINEL",
            "PRIVATE_PATH_KEY_SENTINEL",
            "PRIVATE_PATH_VALUE_SENTINEL",
            "user:pass@",
        ):
            self.assertNotIn(sentinel, encoded)

    def test_live_cli_resolves_exact_cex_inventory_and_runs_without_publish(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            calls = []

            def cex_primitive(market, *, snapshot_id, raw_path, deadline):
                calls.append(market)
                raw_path.write_bytes(b"cli inventory raw")
                return ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}, [])

            with patch("scripts.collect_route_cohort.load_cataloged_markets", return_value=inventory):
                result = main(
                    ["--data-dir", str(data_dir), "--deadline-seconds", "1"],
                    cex_collector=cex_primitive,
                    executor_factory=ThreadPoolExecutor,
                )

        self.assertNotIn("dry_run", result)
        self.assertEqual(
            sorted(
                ({key: row[key] for key in ("token_symbol", "exchange", "cex_symbol")} for row in calls),
                key=lambda row: row["exchange"],
            ),
            sorted(inventory, key=lambda row: row["exchange"]),
        )

    def test_dry_run_applies_tokens_and_rejects_invalid_worker_values(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:alpha:AAVE/USDT", "market_type": "cex", "token_symbol": "AAVE"},
                {"market_id": "cex:beta:AAVE/USDT", "market_type": "cex", "token_symbol": "AAVE"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "AAVE", "buy_market_id": "cex:alpha:AAVE/USDT", "sell_market_id": "cex:beta:AAVE/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            inventory = [
                {
                    "token_symbol": leg["token_symbol"],
                    "exchange": leg["market_id"].split(":", 2)[1],
                    "cex_symbol": leg["market_id"].split(":", 2)[2],
                }
                for leg in universe["selected_legs"]
            ]
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ):
                result = main(["--data-dir", str(data_dir), "--tokens", "UNI", "--dry-run"])
            self.assertEqual((result["selected_leg_count"], result["route_count"]), (2, 1))
            self.assertEqual(len(result["collection_input_generation"]), 64)
            with self.assertRaisesRegex(ValueError, "worker limits"):
                main(["--data-dir", str(data_dir), "--max-workers", "0", "--dry-run"])

    def test_dry_run_binds_authoritative_inventory_and_fails_closed(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "AAVE"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            universe_path = data_dir / "route_universe.json"
            universe_path.write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ) as load_catalog:
                with self.assertRaisesRegex(
                    ValueError, "route leg identity is invalid"
                ):
                    main(["--data-dir", str(data_dir), "--dry-run"])
                load_catalog.assert_not_called()

            universe["selected_legs"][0]["token_symbol"] = "UNI"
            universe_path.write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory[:1],
            ):
                with self.assertRaisesRegex(
                    ValueError, "absent from authoritative inventory"
                ):
                    main(["--data-dir", str(data_dir), "--dry-run"])
            self.assertFalse((data_dir / "raw").exists())

    def test_cli_dates_routes_and_nonfinite_deadlines_fail_closed(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selection_window": {"start": "2026-08-01", "end": "2026-08-02"},
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            universe_path = data_dir / "route_universe.json"
            universe_path.write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "selection_window"):
                main([
                    "--data-dir", str(data_dir), "--start", "2026-07-31",
                    "--end", "2026-08-02", "--dry-run",
                ])
            malformed = {**universe, "routes": [{
                "token_symbol": "UNI",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "cex:beta:UNI/USDT",
            }]}
            universe_path.write_text(json.dumps(malformed), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "route candidate is invalid"):
                main(["--data-dir", str(data_dir), "--dry-run"])
            for value in ("nan", "inf", "-inf"):
                with self.subTest(deadline_seconds=value):
                    with self.assertRaisesRegex(ValueError, "must be positive"):
                        main([
                            "--data-dir", str(data_dir),
                            "--deadline-seconds={}".format(value), "--dry-run",
                        ])
        valid = {
            **universe,
            "routes": [{
                "token_symbol": "UNI",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "cex:beta:UNI/USDT",
                "route_mode": "prepositioned_inventory",
            }],
        }
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(direct_deadline_seconds=value):
                with self.assertRaisesRegex(ValueError, "finite and positive"):
                    collect_route_cohort(
                        valid,
                        deadline_seconds=value,
                        cex_collector=lambda *_args, **_kwargs: self.fail(
                            "must reject before collection"
                        ),
                        dex_collector=lambda *_args, **_kwargs: None,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    )

    def test_direct_collection_rejects_malformed_route_before_raw_or_work(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{
                "token_symbol": "UNI",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "cex:beta:UNI/USDT",
            }],
        }
        calls = []
        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name) / "raw"
            with self.assertRaisesRegex(ValueError, "route candidate is invalid"):
                collect_route_cohort(
                    universe,
                    cex_collector=lambda *_args, **_kwargs: calls.append("called"),
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )
            self.assertEqual(calls, [])
            self.assertFalse(raw_root.exists())

    def test_cohort_metadata_and_late_success_are_stable_and_deadline_terminal(self):
        from scripts.collection_deadline import CollectionDeadline

        clock = FakeClock()
        deadline = CollectionDeadline.for_duration(1, clock=clock.monotonic, sleeper=clock.sleep)
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }

        def cex_collector(leg, **_kwargs):
            if leg["market_id"] == "cex:beta:UNI/USDT":
                clock.now = 2
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        result = collect_route_cohort(
            universe, cex_collector=cex_collector,
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
            deadline=deadline, target_observed_at="2026-08-01T12:00:00+08:00",
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
            wall_clock=lambda: datetime(
                2026, 8, 1, 12, tzinfo=timezone(timedelta(hours=8))
            ),
        )

        self.assertTrue(result["route_cohort_id"].startswith("cohort:"))
        self.assertEqual(result["target_observed_at"], "2026-08-01T04:00:00Z")
        self.assertEqual(result["collection_started_at"], "2026-08-01T04:00:00Z")
        self.assertEqual(result["collection_deadline_at"], "2026-08-01T04:00:01Z")
        terminal = next(row for row in result["legs"] if row["market_id"] == "cex:beta:UNI/USDT")
        self.assertEqual(terminal["status"], "deadline_exceeded")
        self.assertEqual(result["skew_sla_seconds"], "60")
        self.assertEqual(result["route_age_sla_seconds"], "120")
        self.assertEqual(result["candidate_source_generation"], "generation-a")
        self.assertEqual(result["collection_input_generation"], "generation-a")
        self.assertEqual(result["source_state"], {
            "candidate_source_generation": "generation-a",
            "collection_input_generation": "generation-a",
        })

    def test_resolver_timeout_and_fixed_block_mismatch_are_isolated(self):
        from scripts.collection_deadline import CollectionDeadlineExceeded

        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "dex:eth:swap:0xone:UNI", "route_mode": "prepositioned_inventory"},
            ],
        }
        cex = lambda *_args, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}, [])
        timed_out = collect_route_cohort(
            universe, cex_collector=cex,
            dex_collector=lambda *_args, **_kwargs: self.fail("unresolved chain must not collect"),
            dex_block_resolver=lambda *_args, **_kwargs: (_ for _ in ()).throw(CollectionDeadlineExceeded("collection deadline exceeded")),
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
        )
        timed_leg = next(row for row in timed_out["legs"] if row["market_id"].startswith("dex:"))
        self.assertEqual(timed_leg["status"], "deadline_exceeded")
        self.assertEqual(timed_leg["reason_code"], "route_deadline_exceeded")
        self.assertEqual(
            next(row for row in timed_out["route_rows"] if row["sell_market_id"].startswith("dex:"))["reason_code"],
            "route_deadline_exceeded",
        )
        self.assertEqual(
            next(row for row in timed_out["route_rows"] if row["sell_market_id"] == "cex:beta:UNI/USDT")["timing_status"],
            "within_sla",
        )

        mismatch = collect_route_cohort(
            universe, cex_collector=cex,
            dex_collector=lambda *_args, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "block_number": "999", "block_timestamp": "wrong"}, []),
            dex_block_resolver=lambda *_args, **_kwargs: {"block_number": 123, "block_timestamp": "2026-08-01T12:00:00Z"},
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
        )
        mismatch_leg = next(row for row in mismatch["legs"] if row["market_id"].startswith("dex:"))
        self.assertEqual(mismatch_leg["reason_code"], "fixed_block_lineage_mismatch")

    def test_generation_reader_is_mandatory(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }
        for extra in ({}, {"source_generation_reader": lambda: "generation-a"}):
            with self.subTest(extra=sorted(extra)):
                with self.assertRaisesRegex(ValueError, "generation reader is required"):
                    collect_route_cohort(
                        universe,
                        cex_collector=lambda *_args, **_kwargs: self.fail("must not collect"),
                        dex_collector=lambda *_args, **_kwargs: self.fail("must not collect"),
                        **extra,
                    )

    def test_fair_scheduler_respects_global_venue_and_chain_caps(self):
        from threading import Barrier

        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "exchange": "alpha"},
                {"market_id": "cex:alpha:UNI/USDC", "market_type": "cex", "exchange": "alpha"},
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex", "chain": "eth"},
                {"market_id": "dex:eth:swap:0xtwo:UNI", "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:alpha:UNI/USDC", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "dex:eth:swap:0xone:UNI", "sell_market_id": "dex:eth:swap:0xtwo:UNI", "route_mode": "atomic_onchain"},
            ],
        }
        barrier = Barrier(2)
        lock = Lock()
        active = {"all": 0, "cex": 0, "dex": 0}
        maximum = dict(active)
        starts = []

        def observe(kind, leg, **kwargs):
            with lock:
                active["all"] += 1
                active[kind] += 1
                maximum["all"] = max(maximum["all"], active["all"])
                maximum[kind] = max(maximum[kind], active[kind])
                starts.append((kind, leg["market_id"]))
                first_pair = len(starts) <= 2
            if first_pair:
                barrier.wait(timeout=1)
            with lock:
                active["all"] -= 1
                active[kind] -= 1
            if kind == "dex":
                return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "block_number": str(kwargs["fixed_block_number"]), "block_timestamp": kwargs["fixed_block_timestamp"]}
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        collect_route_cohort(
            universe,
            cex_collector=lambda leg, **kwargs: observe("cex", leg, **kwargs),
            dex_collector=lambda leg, **kwargs: observe("dex", leg, **kwargs),
            dex_block_resolver=lambda *_args, **_kwargs: {"block_number": 123, "block_timestamp": "2026-08-01T12:00:00Z"},
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
            max_workers=2, cex_workers_per_venue=1, dex_workers_per_chain=1,
        )
        self.assertEqual(maximum, {"all": 2, "cex": 1, "dex": 1})
        self.assertEqual({kind for kind, _market_id in starts[:2]}, {"cex", "dex"})

    def test_cli_dry_run_reads_and_validates_universe_without_collection_or_publication(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [],
            "routes": [],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(json.dumps(universe), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "selected legs and routes"):
                main([
                    "--data-dir", str(data_dir), "--start", "2026-08-01",
                    "--end", "2026-08-01", "--tokens", "UNI", "--dry-run",
                ])
            self.assertEqual(sorted(path.name for path in data_dir.iterdir()), ["route_universe.json"])
            with self.assertRaisesRegex(RuntimeError, "Task 5"):
                main(["--data-dir", str(data_dir), "--publish"])

    def test_unique_route_legs_deduplicates_directional_route_references(self):
        routes = [
            {
                "route_id": "route:UNI:cex:alpha:UNI/USDT->dex:eth:swap:0xpool:UNI:prepositioned_inventory",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "dex:eth:swap:0xpool:UNI",
            },
            {
                "route_id": "route:UNI:dex:eth:swap:0xpool:UNI->cex:alpha:UNI/USDT:prepositioned_inventory",
                "buy_market_id": "dex:eth:swap:0xpool:UNI",
                "sell_market_id": "cex:alpha:UNI/USDT",
            },
        ]

        self.assertEqual(
            collect_unique_route_legs(routes),
            ["cex:alpha:UNI/USDT", "dex:eth:swap:0xpool:UNI"],
        )

    def test_materialize_route_leg_rows_retains_terminal_deadline_leg(self):
        rows = materialize_route_leg_rows(
            ["cex:alpha:UNI/USDT"],
            {},
            deadline_exceeded={"cex:alpha:UNI/USDT"},
        )

        self.assertEqual(
            rows,
            [{
                "leg_id": "cex:alpha:UNI/USDT",
                "market_id": "cex:alpha:UNI/USDT",
                "status": "deadline_exceeded",
                "available": False,
                "reason_code": "route_deadline_exceeded",
            }],
        )

    def test_collects_each_shared_leg_once_with_per_venue_limit(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "exchange": "alpha"},
                {"market_id": "cex:alpha:UNI/USDC", "market_type": "cex", "exchange": "alpha"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "exchange": "beta"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:alpha:UNI/USDC", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDC", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        active = {"alpha": 0, "beta": 0}
        maximum = {"alpha": 0, "beta": 0}
        started = []
        lock = Lock()

        def cex_collector(leg, **_kwargs):
            venue = leg["exchange"]
            with lock:
                active[venue] += 1
                maximum[venue] = max(maximum[venue], active[venue])
                started.append(leg["market_id"])
            try:
                return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}
            finally:
                with lock:
                    active[venue] -= 1

        result = collect_route_cohort(
            universe,
            cex_collector=cex_collector,
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
            max_workers=3,
            cex_workers_per_venue=1,
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
            executor_factory=ThreadPoolExecutor,
        )

        self.assertCountEqual(
            started,
            ["cex:alpha:UNI/USDC", "cex:alpha:UNI/USDT", "cex:beta:UNI/USDT"],
        )
        self.assertEqual(maximum, {"alpha": 1, "beta": 1})
        self.assertEqual(
            [row["market_id"] for row in result["legs"]],
            ["cex:alpha:UNI/USDC", "cex:alpha:UNI/USDT", "cex:beta:UNI/USDT"],
        )

    def test_same_chain_dex_legs_receive_one_resolved_fixed_block(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex", "chain": "eth"},
                {"market_id": "dex:eth:swap:0xtwo:UNI", "market_type": "dex", "chain": "eth"},
                {"market_id": "dex:arb:swap:0xthree:UNI", "market_type": "dex", "chain": "arb"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "dex:eth:swap:0xone:UNI", "sell_market_id": "dex:eth:swap:0xtwo:UNI", "route_mode": "atomic_onchain"},
            ],
        }
        resolved = []
        received = {}

        def resolve_block(chain, **_kwargs):
            resolved.append(chain)
            return {"block_number": 101 if chain == "eth" else 202, "block_timestamp": "2026-08-01T12:00:00Z"}

        def dex_collector(leg, **kwargs):
            received[leg["market_id"]] = (
                kwargs["fixed_block_number"], kwargs["fixed_block_timestamp"],
            )
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        collect_route_cohort(
            universe,
            cex_collector=lambda *_args, **_kwargs: self.fail("unexpected CEX collection"),
            dex_collector=dex_collector,
            dex_block_resolver=resolve_block,
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
            max_workers=3,
        )

        self.assertEqual(resolved, ["eth"])
        self.assertEqual(received["dex:eth:swap:0xone:UNI"], (101, "2026-08-01T12:00:00Z"))
        self.assertEqual(received["dex:eth:swap:0xtwo:UNI"], (101, "2026-08-01T12:00:00Z"))
        self.assertNotIn("dex:arb:swap:0xthree:UNI", received)

    def test_dex_collection_fails_closed_without_fixed_block_resolver(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex"},
                {"market_id": "dex:eth:swap:0xtwo:UNI", "market_type": "dex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "dex:eth:swap:0xone:UNI", "sell_market_id": "dex:eth:swap:0xtwo:UNI", "route_mode": "atomic_onchain"},
            ],
        }

        with self.assertRaisesRegex(ValueError, "fixed block resolver"):
            collect_route_cohort(
                universe,
                cex_collector=lambda *_args, **_kwargs: self.fail("unexpected CEX collection"),
                dex_collector=lambda *_args, **_kwargs: self.fail("must not collect without a fixed block"),
                source_generation_reader=lambda: "generation-a",
                expected_source_generation="generation-a",
            )

    def test_deadline_terminal_leg_only_makes_its_routes_unavailable(self):
        from scripts.collection_deadline import CollectionDeadlineExceeded

        good = "2026-08-01T12:00:00Z"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:gamma:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:gamma:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }

        def cex_collector(leg, **_kwargs):
            if leg["market_id"] == "cex:beta:UNI/USDT":
                raise CollectionDeadlineExceeded("collection deadline exceeded")
            return {"status": "observed", "state_observed_at": good}

        result = collect_route_cohort(
            universe,
            cex_collector=cex_collector,
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
        )

        timing = {row["route_id"]: row for row in result["route_rows"]}
        self.assertEqual(
            timing["route:UNI:cex:alpha:UNI/USDT->cex:beta:UNI/USDT:prepositioned_inventory"]["reason_code"],
            "route_deadline_exceeded",
        )
        self.assertEqual(
            timing["route:UNI:cex:alpha:UNI/USDT->cex:gamma:UNI/USDT:prepositioned_inventory"]["timing_status"],
            "within_sla",
        )

    def test_reverse_completion_orders_produce_identical_normalized_cohort(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }

        def collect_with_delays(delays):
            def cex_collector(leg, **_kwargs):
                time.sleep(delays[leg["market_id"]])
                return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

            with tempfile.TemporaryDirectory() as directory_name:
                return collect_route_cohort(
                    universe,
                    cex_collector=cex_collector,
                    dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
                    max_workers=2,
                    target_observed_at="2026-08-01T12:00:00Z",
                    source_generation_reader=lambda: "generation-a",
                    expected_source_generation="generation-a",
                    raw_root=Path(directory_name), snapshot_id="stable-run",
                    wall_clock=lambda: datetime(
                        2026, 8, 1, 12, tzinfo=timezone.utc
                    ),
                )

        alpha_first = collect_with_delays({"cex:alpha:UNI/USDT": 0, "cex:beta:UNI/USDT": 0.02})
        beta_first = collect_with_delays({"cex:alpha:UNI/USDT": 0.02, "cex:beta:UNI/USDT": 0})

        self.assertEqual(alpha_first["legs"], beta_first["legs"])
        self.assertEqual(alpha_first["route_rows"], beta_first["route_rows"])
        self.assertEqual(alpha_first["fingerprint"], beta_first["fingerprint"])

    def test_source_generation_change_during_collection_fails_closed(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        current_generation = ["generation-a"]

        def cex_collector(_leg, **_kwargs):
            current_generation[0] = "generation-b"
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name)
            with self.assertRaisesRegex(ValueError, "collection input generation changed"):
                collect_route_cohort(
                    universe,
                    cex_collector=cex_collector,
                    dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
                    source_generation_reader=lambda: current_generation[0],
                    expected_source_generation="generation-a",
                    raw_root=raw_root,
                )
            accepted = list(raw_root.glob("*/accepted/*"))
            self.assertEqual(accepted, [])


if __name__ == "__main__":
    unittest.main()
