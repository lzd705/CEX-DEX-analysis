import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time
from unittest.mock import patch

from scripts.collect_route_cohort import (
    collect_route_cohort,
    collect_unique_route_legs,
    main,
    materialize_route_leg_rows,
)


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
    def test_cli_dry_run_reads_and_validates_universe_without_collection_or_publication(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [],
            "routes": [],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(json.dumps(universe), encoding="utf-8")
            result = main([
                "--data-dir", str(data_dir), "--start", "2026-08-01",
                "--end", "2026-08-01", "--tokens", "UNI", "--dry-run",
            ])
            self.assertEqual(result["candidate_source_generation"], "generation-a")
            self.assertTrue(result["dry_run"])
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

            return collect_route_cohort(
                universe,
                cex_collector=cex_collector,
                dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
                max_workers=2,
                target_observed_at="2026-08-01T12:00:00Z",
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

        with self.assertRaisesRegex(ValueError, "candidate source generation changed"):
            collect_route_cohort(
                universe,
                cex_collector=cex_collector,
                dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
                source_generation_reader=lambda: current_generation[0],
            )


if __name__ == "__main__":
    unittest.main()
