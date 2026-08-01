import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
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
            (data_dir / "route_universe.json").write_text(json.dumps(universe), encoding="utf-8")
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
                side_effect=[universe, mutated],
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

    def test_authoritative_identity_conflict_and_returned_identity_mismatch_fail_closed(self):
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
            (data_dir / "route_universe.json").write_text(json.dumps(universe), encoding="utf-8")
            with patch("scripts.collect_route_cohort.load_cataloged_markets", return_value=inventory):
                with self.assertRaisesRegex(ValueError, "conflicts with authoritative inventory"):
                    main(["--data-dir", str(data_dir)], cex_collector=lambda *_args, **_kwargs: self.fail("must not collect"))

        direct = {**universe, "selected_legs": [{**row, "token_symbol": "UNI"} for row in universe["selected_legs"]]}
        result = collect_route_cohort(
            direct,
            cex_collector=lambda leg, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "token_symbol": "AAVE"}, []),
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX"),
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )
        self.assertTrue(all(row["reason_code"] == "collector_identity_mismatch" for row in result["legs"]))

    def test_blocked_worker_does_not_keep_subprocess_alive_past_deadline(self):
        code = textwrap.dedent(
            """
            from threading import Event
            from scripts.collect_route_cohort import collect_route_cohort
            universe = {
                'candidate_source_generation': 'candidate-a',
                'selected_legs': [
                    {'market_id': 'cex:alpha:UNI/USDT', 'market_type': 'cex'},
                    {'market_id': 'cex:beta:UNI/USDT', 'market_type': 'cex'},
                ],
                'routes': [{'token_symbol': 'UNI', 'buy_market_id': 'cex:alpha:UNI/USDT', 'sell_market_id': 'cex:beta:UNI/USDT', 'route_mode': 'prepositioned_inventory'}],
            }
            gate = Event()
            result = collect_route_cohort(
                universe, deadline_seconds=0.05,
                cex_collector=lambda *_args, **_kwargs: gate.wait(),
                dex_collector=lambda *_args, **_kwargs: None,
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
            (data_dir / "route_universe.json").write_text(json.dumps(universe), encoding="utf-8")
            calls = []

            def cex_primitive(market, *, snapshot_id, raw_path, deadline):
                calls.append(market)
                return ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}, [])

            with patch("scripts.collect_route_cohort.load_cataloged_markets", return_value=inventory):
                result = main(["--data-dir", str(data_dir), "--deadline-seconds", "1"], cex_collector=cex_primitive)

        self.assertFalse(result["dry_run"])
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
            (data_dir / "route_universe.json").write_text(json.dumps(universe), encoding="utf-8")
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
            universe_path.write_text(json.dumps(universe), encoding="utf-8")
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ):
                with self.assertRaisesRegex(
                    ValueError, "conflicts with authoritative inventory"
                ):
                    main(["--data-dir", str(data_dir), "--dry-run"])

            universe["selected_legs"][0]["token_symbol"] = "UNI"
            universe_path.write_text(json.dumps(universe), encoding="utf-8")
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
            universe_path.write_text(json.dumps(universe), encoding="utf-8")
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
