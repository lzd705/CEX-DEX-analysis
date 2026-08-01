import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from unittest.mock import MagicMock, patch

from dashboard import server
from dashboard.event_facts import EventBundleError, resolve_event_bundle
from scripts.event_facts import (
    DEFAULT_INPUT,
    DEFAULT_RECORD_ROOT,
    DEFAULT_TOKEN_CONFIG,
    build_event_bundle,
)


def write_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class EventApiIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.event_root = self.root / "events"
        build_event_bundle(
            DEFAULT_INPUT,
            record_root=DEFAULT_RECORD_ROOT,
            output_root=self.event_root,
            token_config=DEFAULT_TOKEN_CONFIG,
        )
        self.cex_path = self.root / server.CEX_FILENAME
        self.dex_path = self.root / server.DEX_FILENAME
        write_csv(
            self.cex_path,
            [
                "date",
                "token_symbol",
                "exchange",
                "cex_symbol",
                "open",
                "high",
                "low",
                "close",
                "base_volume",
                "quote_volume_usd",
            ],
            [
                {
                    "date": "2026-07-28",
                    "token_symbol": "AAVE",
                    "exchange": "binance",
                    "cex_symbol": "AAVE/USDT",
                    "close": "100",
                    "quote_volume_usd": "1000",
                }
            ],
        )
        write_csv(
            self.dex_path,
            [
                "date",
                "token_symbol",
                "chain",
                "dex",
                "pool_address",
                "pool_name",
                "open",
                "high",
                "low",
                "close",
                "dex_volume_usd",
                "pool_tvl_usd",
            ],
            [
                {
                    "date": "2026-07-28",
                    "token_symbol": "AAVE",
                    "chain": "eth",
                    "dex": "uniswap_v3",
                    "pool_address": "0xpool",
                    "pool_name": "AAVE / USDC",
                    "close": "101",
                    "dex_volume_usd": "500",
                    "pool_tvl_usd": "",
                }
            ],
        )
        self.environment = {
            "MARKET_CEX_DATA": str(self.cex_path),
            "MARKET_DEX_DATA": str(self.dex_path),
            "MARKET_EVENT_DATA_DIR": str(self.event_root),
        }
        server.clear_runtime_caches()

    def tearDown(self):
        server.clear_runtime_caches()
        self.temporary_directory.cleanup()

    def test_available_bundle_honors_token_date_and_lifecycle_scope(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_event_facts(
                token=" strk ",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="SCHEDULED",
            )

        self.assertEqual(payload["schema"], "event_facts_api/v2")
        self.assertEqual(
            payload["availability"],
            {"status": "available", "reason": None},
        )
        self.assertEqual(
            payload["query"],
            {
                "token": "STRK",
                "start": "2026-08-15",
                "end": "2026-08-15",
                "lifecycle": "scheduled",
                "clock_state": None,
            },
        )
        self.assertRegex(
            payload["clock_as_of_utc"],
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
        )
        self.assertIn(event_clock := payload["events"][0]["clock"]["state"], {
            "past", "future", "current_window",
        })
        self.assertEqual(payload["clock_state_counts"][event_clock], 1)
        self.assertTrue(
            all(
                event["clock"]["as_of_utc"] == payload["clock_as_of_utc"]
                for event in payload["events"]
            )
        )
        self.assertEqual(payload["event_count"], 1)
        self.assertEqual(payload["coverage"]["configured_token_count"], 30)
        self.assertEqual(payload["coverage"]["covered_token_count"], 30)
        self.assertEqual(payload["coverage"]["uncovered_tokens"], [])
        self.assertTrue(payload["coverage"]["query_token_has_published_fact"])
        event = payload["events"][0]
        self.assertEqual(event["token_symbol"], "STRK")
        self.assertEqual(event["lifecycle"], "scheduled")
        self.assertEqual(event["revision"], 1)
        self.assertEqual(event["evidence_status"], "primary_confirmed")
        self.assertTrue(event["source"]["url"].startswith("https://"))
        self.assertEqual(len(event["source"]["record_sha256"]), 64)
        self.assertTrue(event["source"]["record_locator"])
        self.assertTrue(event["revision_lineage"]["reason"])

    def test_available_empty_scope_is_distinct_from_missing_publication(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            no_matching_event = server.build_event_facts(
                token="AAVE",
                start="1900-01-01",
                end="1900-01-01",
            )
        self.assertEqual(
            no_matching_event["availability"]["status"],
            "available",
        )
        self.assertEqual(no_matching_event["event_count"], 0)
        self.assertTrue(
            no_matching_event["coverage"]["query_token_has_published_fact"]
        )

        missing_environment = {
            **self.environment,
            "MARKET_EVENT_DATA_DIR": str(self.root / "not-published"),
        }
        with patch.dict(server.os.environ, missing_environment, clear=True):
            unavailable = server.build_event_facts(token="AAVE")
        self.assertEqual(
            unavailable["availability"],
            {
                "status": "unavailable",
                "reason": "event_bundle_not_published",
            },
        )
        self.assertEqual(unavailable["event_count"], 0)
        self.assertEqual(unavailable["events"], [])
        self.assertIsNone(unavailable["bundle_id"])

    def test_missing_publication_still_validates_query_contract(self):
        environment = {
            **self.environment,
            "MARKET_EVENT_DATA_DIR": str(self.root / "not-published"),
        }
        with patch.dict(server.os.environ, environment, clear=True):
            with self.assertRaisesRegex(ValueError, "lifecycle must be one of"):
                server.build_event_facts(lifecycle="predicted")
            with self.assertRaisesRegex(ValueError, "end must be on or after start"):
                server.build_event_facts(
                    start="2026-08-16",
                    end="2026-08-15",
                )
            with self.assertRaisesRegex(ValueError, "clock_state must be one of"):
                server.build_event_facts(clock_state="predicted")

    def test_event_response_bypasses_minute_cache_for_exact_transitions(self):
        clocks = [
            datetime(2026, 8, 15, 12, 0, 29, tzinfo=timezone.utc),
            datetime(2026, 8, 15, 12, 0, 31, tzinfo=timezone.utc),
        ]
        payloads = []

        def fake_payload(route, query_items, source_signature=None):
            self.assertEqual(route, "events")
            current = server.event_response_clock()
            payloads.append(current)
            return {
                "clock_as_of_utc": current.isoformat(timespec="seconds").replace(
                    "+00:00",
                    "Z",
                )
            }

        signature = (("events", 1),)
        with patch.object(
            server,
            "event_response_clock",
            side_effect=clocks,
        ), patch.object(
            server,
            "_build_public_api_payload",
            side_effect=fake_payload,
        ), patch.object(
            server,
            "api_source_signature",
            return_value=signature,
        ):
            first, first_compressed = server.build_public_api_response(
                "events",
                (),
                True,
            )
            second, second_compressed = server.build_public_api_response(
                "events",
                (),
                True,
            )

        self.assertEqual(len(payloads), 2)
        self.assertEqual(first_compressed, second_compressed)
        self.assertNotEqual(first, second)

    def test_event_files_participate_in_source_generation(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            before = server.api_source_signature()
            pointer_path = self.event_root / "latest.json"
            pointer_path.write_text(
                pointer_path.read_text(encoding="utf-8") + " ",
                encoding="utf-8",
            )
            after = server.api_source_signature()

        self.assertNotEqual(before, after)
        normalized = server.public_api_query_items(
            "events",
            {
                "token": [" strk "],
                "start": [" 2026-08-15 "],
                "end": ["2026-08-15"],
                "lifecycle": [" SCHEDULED "],
                "clock_state": [" FUTURE "],
                "ignored": ["does-not-enter-cache-key"],
            },
        )
        self.assertEqual(
            normalized,
            (
                ("token", "STRK"),
                ("start", "2026-08-15"),
                ("end", "2026-08-15"),
                ("lifecycle", "scheduled"),
                ("clock_state", "future"),
            ),
        )

    def test_corrupt_event_bundle_fails_closed_without_breaking_market_api(self):
        bundle = resolve_event_bundle(self.event_root)
        with bundle["database_path"].open("ab") as handle:
            handle.write(b"tampered")

        with patch.dict(server.os.environ, self.environment, clear=True):
            summary_body, compressed = server.build_public_api_response(
                "summary",
                (),
                False,
            )
            self.assertFalse(compressed)
            summary = json.loads(summary_body)
            self.assertEqual(summary["metadata"]["response_scope"], "screener_summary")
            with self.assertRaisesRegex(EventBundleError, "checksum"):
                server.build_public_api_response("events", (), False)

    def test_corrupt_event_route_maps_validation_failure_to_503(self):
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/api/markets/events?token=STRK"
        handler.send_public_api = MagicMock(
            side_effect=EventBundleError("database checksum mismatch")
        )
        handler.send_json = MagicMock()

        handler.do_GET()

        handler.send_json.assert_called_once()
        payload, status = handler.send_json.call_args.args
        self.assertEqual(status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertEqual(payload["error"], "Event Fact bundle failed validation")
        self.assertEqual(payload["reason"], "event_bundle_validation_failed")
        self.assertNotIn("database checksum mismatch", json.dumps(payload))

    def test_events_route_and_spa_route_are_declared(self):
        self.assertTrue(server.is_spa_shell_path("/tokens/STRK/events"))
        self.assertFalse(server.is_spa_shell_path("/tokens/STRK/event-study"))
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = (
            "/api/markets/events?token=strk&lifecycle=scheduled"
            "&clock_state=future"
        )
        handler.send_public_api = MagicMock()

        handler.do_GET()

        handler.send_public_api.assert_called_once_with(
            "events",
            {
                "token": ["strk"],
                "lifecycle": ["scheduled"],
                "clock_state": ["future"],
            },
        )


if __name__ == "__main__":
    unittest.main()
