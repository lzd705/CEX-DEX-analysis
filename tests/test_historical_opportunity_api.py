"""Historical opportunity facts and HTTP contract tests."""

from __future__ import annotations

import hashlib
import gzip
import json
import shutil
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock


class HistoricalOpportunityFactsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from dashboard import opportunity_facts
        from tests.historical_replay_fixture import (
            PublishedHistoricalReplayFixture,
        )

        cls.facts = opportunity_facts
        cls.fixture = PublishedHistoricalReplayFixture()
        cls.loaded = opportunity_facts.load_latest_historical_opportunities(
            cls.fixture.historical_root.parent,
            cls.fixture.raw_root,
        )

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "loaded", None) is not None:
            cls.loaded["validated_view"].close()
        cls.fixture.close()

    def test_loader_returns_only_a_fully_verified_historical_generation(self):
        loaded = self.loaded

        self.assertEqual(len(loaded["routes"]), 2)
        self.assertEqual(len(loaded["opportunities"]), 10)
        self.assertEqual(len(loaded["cost_components"]), 90)
        self.assertEqual(loaded["verification_report"]["status"], "verified")
        self.assertEqual(
            loaded["verification_report"]["evidence_mode"],
            "production_connected",
        )
        loaded["validated_view"].reread_unchanged()

    def test_data_generation_is_path_clock_filter_and_inode_free(self):
        loaded = self.loaded
        members = [
            [role, physical_sha256, size]
            for role, physical_sha256, size, *_physical_metadata
            in loaded["publication_signature"]
            if (
                (role.startswith("complete:")
                 and role != "complete:manifest.json")
                or role.startswith("raw:")
            )
        ]
        expected_projection = {
            "contract_version": (
                self.facts.HISTORICAL_OPPORTUNITY_SUMMARY_CONTRACT
            ),
            "pointer_sha256": loaded["pointer_sha256"],
            "verification_report_sha256": (
                loaded["verification_report_sha256"]
            ),
            "manifest_sha256": loaded["manifest_sha256"],
            "historical_core_manifest_sha256": loaded["manifest"][
                "historical_core_manifest_sha256"
            ],
            "historical_core_pointer_sha256": loaded["manifest"][
                "historical_core_pointer_sha256"
            ],
            "members": members,
        }
        expected = hashlib.sha256(json.dumps(
            expected_projection,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")).hexdigest()

        generation = self.facts.historical_opportunity_data_generation(
            loaded
        )
        self.assertEqual(generation, expected)
        self.assertRegex(generation, r"^[0-9a-f]{64}$")
        self.assertNotIn(str(self.fixture.data_dir), json.dumps(
            expected_projection, sort_keys=True
        ))

    def test_unfiltered_payload_projects_exact_closed_replay_grid(self):
        payload = self.facts.build_historical_opportunity_payload(self.loaded)
        metadata = payload["metadata"]
        coverage = metadata["coverage"]
        manifest = self.loaded["manifest"]
        evidence = self.loaded["replay_evidence"]

        self.assertEqual(
            metadata["contract_version"],
            "opportunity_historical_summary/v1",
        )
        self.assertEqual(metadata["temporal_scope"], "historical_replay")
        self.assertEqual(
            metadata["execution_claim"],
            "historical_counterfactual_state_override_next_block",
        )
        for field in (
            "replay_id", "route_cohort_id", "policy_sha256", "run_id",
            "run_manifest_sha256", "selection_sha256",
        ):
            self.assertEqual(metadata[field], manifest[field])
        self.assertEqual(metadata["manifest_sha256"], self.loaded["manifest_sha256"])
        self.assertEqual(
            metadata["scenario_set_sha256"], evidence["scenario_set_sha256"]
        )
        self.assertEqual(metadata["selected_block_number"], 1)
        self.assertEqual(
            metadata["simulation_basis"],
            "hash_bound_state_override_next_block",
        )
        self.assertEqual(payload["freshness"], {
            "applicable": False,
            "reason_code": "historical_replay",
            "next_deadline": None,
        })
        self.assertEqual(coverage["route_count"], 2)
        self.assertEqual(coverage["scenario_count"], 10)
        self.assertEqual(coverage["returned_count"], 10)
        self.assertEqual(coverage["foundry_verified_count"], 10)
        self.assertEqual(coverage["research_estimate_count"], 10)
        self.assertEqual(coverage["strict_count"], 0)
        self.assertEqual(coverage["executable_count"], 0)
        self.assertEqual(coverage["attested_count"], 0)
        self.assertEqual(coverage["unavailable_count"], 0)
        self.assertEqual(
            coverage["positive_count"],
            sum(
                Decimal(row["research_net_edge_usd"]) > 0
                for row in payload["routes"]
            ),
        )
        self.assertEqual(
            {row["opportunity_id"] for row in payload["routes"]},
            {row["opportunity_id"] for row in self.loaded["opportunities"]},
        )
        self.assertEqual(
            {
                (row["direction"], row["requested_notional_usd"])
                for row in payload["routes"]
            },
            {
                (direction, notional)
                for direction in (
                    "sushiswap_to_uniswap", "uniswap_to_sushiswap"
                )
                for notional in ("1000", "5000", "10000", "50000", "100000")
            },
        )

    def test_each_row_exposes_only_historical_research_semantics(self):
        payload = self.facts.build_historical_opportunity_payload(self.loaded)
        selected = self.loaded["manifest"]["selected_block"]
        expected_age = (
            selected["synthetic_child_timestamp"] - selected["timestamp"]
        )
        scenarios = {
            row["opportunity_id"]: row
            for row in self.loaded["replay_evidence"]["scenarios"]
        }

        for row in payload["routes"]:
            scenario = scenarios[row["opportunity_id"]]
            self.assertEqual(row["opportunity_class"], "research_estimate")
            self.assertEqual(
                row["availability"], {"status": "available", "reason": None}
            )
            self.assertEqual(
                row["route_mode"],
                "historical_counterfactual_state_override_next_block",
            )
            self.assertEqual(row["selected_block_number"], selected["number"])
            self.assertEqual(row["selected_block_hash"], selected["hash"])
            self.assertEqual(row["state_age_seconds"], expected_age)
            self.assertTrue(row["foundry_verified"])
            self.assertEqual(row["gas_used"], scenario["receipt"]["gas_used"])
            self.assertEqual(row["receipt_sha256"], scenario["receipt_sha256"])
            self.assertEqual(row["trace_sha256"], scenario["trace_sha256"])
            self.assertEqual(
                row["executor_model"], "prefunded_predeployed_preapproved"
            )
            self.assertEqual(
                row["policy_net_edge_usd"],
                scenario["baseline"]["research_net_edge_usd"],
            )
            self.assertEqual(
                row["baseline_net_edge_usd"],
                scenario["baseline"]["research_net_edge_usd"],
            )
            self.assertEqual(
                row["stress_25_net_edge_usd"],
                scenario["stress_25"]["research_net_edge_usd"],
            )
            self.assertEqual(
                row["stress_50_net_edge_usd"],
                scenario["stress_50"]["research_net_edge_usd"],
            )

    def test_filters_share_one_generation_and_strict_is_empty(self):
        generation = self.facts.historical_opportunity_data_generation(
            self.loaded
        )
        filtered = self.facts.build_historical_opportunity_payload(
            self.loaded,
            notional_usd="1000",
            opportunity_class="estimate",
            route_type="dex_dex",
            availability="available",
            sort="requested_notional_usd",
            direction="asc",
        )
        strict = self.facts.build_historical_opportunity_payload(
            self.loaded, opportunity_class="strict"
        )

        self.assertEqual(len(filtered["routes"]), 2)
        self.assertEqual(filtered["metadata"]["data_generation"], generation)
        self.assertEqual(strict["routes"], [])
        self.assertEqual(strict["metadata"]["coverage"]["returned_count"], 0)
        self.assertEqual(strict["metadata"]["data_generation"], generation)

    def test_projection_never_calls_live_freshness_helpers(self):
        with mock.patch.object(
            self.facts, "_timing", side_effect=AssertionError("live timing")
        ), mock.patch.object(
            self.facts,
            "_cost_component_deadline",
            side_effect=AssertionError("live cost deadline"),
        ), mock.patch.object(
            self.facts,
            "_next_freshness_deadline_at",
            side_effect=AssertionError("live response deadline"),
        ):
            first = self.facts.build_historical_opportunity_payload(self.loaded)
            second = self.facts.build_historical_opportunity_payload(self.loaded)
        self.assertEqual(first, second)

    def test_missing_pointer_has_dedicated_unavailable_payload(self):
        with tempfile.TemporaryDirectory() as name:
            data = Path(name)
            routes = data / "routes"
            raw = data / "raw" / "historical-foundry-replay"
            (routes / "historical").mkdir(parents=True)
            raw.mkdir(parents=True)
            with self.assertRaises(self.facts.OpportunityBundleUnavailable) as caught:
                self.facts.load_latest_historical_opportunities(routes, raw)
            self.assertEqual(
                caught.exception.reason, "historical_replay_pointer_absent"
            )

        payload = self.facts.build_unavailable_historical_opportunity_payload(
            opportunity_class="strict"
        )
        self.assertEqual(payload["availability"], {
            "status": "unavailable",
            "reason": "historical_replay_pointer_absent",
        })
        self.assertIsNone(payload["metadata"]["data_generation"])
        self.assertTrue(all(
            value == 0
            for value in payload["metadata"]["coverage"].values()
        ))
        self.assertEqual(payload["routes"], [])

    def test_corrupt_reader_result_is_never_projected(self):
        import scripts.historical_route_publication as publication

        with mock.patch.object(
            publication,
            "load_latest_historical_replay_bundle",
            side_effect=publication.HistoricalRoutePublicationError(
                "tampered proof input"
            ),
        ):
            with self.assertRaises(self.facts.OpportunityBundleInvalid):
                self.facts.load_latest_historical_opportunities(
                    self.fixture.historical_root.parent,
                    self.fixture.raw_root,
                )


class HistoricalOpportunityServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from dashboard import server
        from tests.historical_replay_fixture import (
            PublishedHistoricalReplayFixture,
        )

        cls.server = server
        cls.fixture = PublishedHistoricalReplayFixture()

    @classmethod
    def tearDownClass(cls):
        cls.fixture.close()

    def setUp(self):
        self.server.clear_runtime_caches()
        self.server._reset_historical_pointer_publication_identities_for_tests()

    def tearDown(self):
        self.server.clear_runtime_caches()
        self.server._reset_historical_pointer_publication_identities_for_tests()

    def _environment(self, routes_root=None):
        return mock.patch.dict(
            self.server.os.environ,
            {
                "MARKET_ROUTE_DATA_DIR": str(
                    routes_root or self.fixture.historical_root.parent
                )
            },
            clear=True,
        )

    def _response(self, *, routes_root=None, query=(), gzip_response=False):
        with self._environment(routes_root):
            body, compressed = self.server.build_public_api_response(
                "opportunities_historical",
                tuple(query),
                gzip_response,
            )
        if compressed:
            body = gzip.decompress(body)
        return json.loads(body), compressed

    def _copy_publication(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        copied = Path(temporary.name) / "data"
        shutil.copytree(self.fixture.data_dir, copied)
        return copied, copied / "routes"

    def test_historical_route_normalizes_the_same_eight_filters(self):
        query = {
            "token": [" uni "],
            "venue": [" UNISWAP_V2 "],
            "notional": ["1000.0"],
            "class": [" ESTIMATE "],
            "route_type": [" DEX_DEX "],
            "availability": [" AVAILABLE "],
            "sort": [" NET_EDGE_USD "],
            "dir": [" DESC "],
            "ignored": ["not-a-cache-key"],
        }

        self.assertEqual(
            self.server.public_api_query_items(
                "opportunities_historical", query
            ),
            (
                ("token", "UNI"),
                ("venue", "uniswap_v2"),
                ("notional", "1000"),
                ("class", "estimate"),
                ("route_type", "dex_dex"),
                ("availability", "available"),
                ("sort", "net_edge_usd"),
                ("dir", "desc"),
            ),
        )

    def test_cold_warm_identity_and_gzip_share_one_generation(self):
        cache = self.server._build_historical_opportunity_response_cached
        before = cache.cache_info()

        first, first_compressed = self._response()
        warm, warm_compressed = self._response()
        zipped, zipped_compressed = self._response(gzip_response=True)
        filtered_query = self.server.public_api_query_items(
            "opportunities_historical",
            {
                "notional": ["1000"],
                "class": ["estimate"],
                "availability": ["available"],
            },
        )
        filtered, _filtered_compressed = self._response(
            query=filtered_query
        )

        self.assertFalse(first_compressed)
        self.assertFalse(warm_compressed)
        self.assertTrue(zipped_compressed)
        self.assertEqual(first, warm)
        self.assertEqual(first, zipped)
        self.assertEqual(len(first["routes"]), 10)
        self.assertEqual(len(filtered["routes"]), 2)
        self.assertEqual(
            filtered["metadata"]["data_generation"],
            first["metadata"]["data_generation"],
        )
        self.assertRegex(
            first["metadata"]["data_generation"], r"^[0-9a-f]{64}$"
        )
        after = cache.cache_info()
        self.assertGreaterEqual(after.misses - before.misses, 2)
        self.assertGreaterEqual(after.hits - before.hits, 1)

    def test_historical_response_never_uses_live_source_or_minute_clock(self):
        with self._environment(), mock.patch.object(
            self.server,
            "api_source_signature",
            side_effect=AssertionError("live source signature"),
        ), mock.patch.object(
            self.server,
            "api_freshness_bucket",
            side_effect=AssertionError("live minute bucket"),
        ), mock.patch.object(
            self.server,
            "opportunity_response_clock",
            side_effect=AssertionError("live deadline clock"),
        ):
            body, compressed = self.server.build_public_api_response(
                "opportunities_historical", (), False
            )

        self.assertFalse(compressed)
        self.assertEqual(len(json.loads(body)["routes"]), 10)

    def test_missing_pointer_is_http_200_style_unavailable_payload(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        data = Path(temporary.name)
        routes = data / "routes"
        (routes / "historical").mkdir(parents=True)
        (data / "raw" / "historical-foundry-replay").mkdir(parents=True)

        payload, compressed = self._response(routes_root=routes)

        self.assertFalse(compressed)
        self.assertEqual(payload["availability"], {
            "status": "unavailable",
            "reason": "historical_replay_pointer_absent",
        })
        self.assertIsNone(payload["metadata"]["data_generation"])
        self.assertEqual(payload["routes"], [])

    def test_warm_cache_never_survives_deleted_pointer_bound_report(self):
        copied, routes = self._copy_publication()
        first, _compressed = self._response(routes_root=routes)
        pointer = json.loads(
            (routes / "historical" / "latest.json").read_bytes()
        )
        report = (
            routes / "historical" / "verifications" / "by-sha256"
            / (pointer["verification_report_sha256"] + ".json")
        )
        report.unlink()

        with self.assertRaises(self.server.OpportunityBundleInvalid):
            self._response(routes_root=routes)
        self.assertEqual(len(first["routes"]), 10)
        self.assertTrue(copied.is_dir())

    def test_warm_cache_rejects_same_size_mutation_and_same_byte_inode_swap(self):
        for mutation in ("same_size", "same_bytes_new_inode"):
            with self.subTest(mutation=mutation):
                self.server.clear_runtime_caches()
                self.server._reset_historical_pointer_publication_identities_for_tests()
                _copied, routes = self._copy_publication()
                first, _compressed = self._response(routes_root=routes)
                replay_id = first["metadata"]["replay_id"]
                member = (
                    routes / "historical" / "bundles" / replay_id
                    / "route_legs.csv"
                )
                payload = member.read_bytes()
                member.unlink()
                if mutation == "same_size":
                    replacement = bytes([payload[0] ^ 1]) + payload[1:]
                else:
                    replacement = payload
                member.write_bytes(replacement)

                with self.assertRaises(self.server.OpportunityBundleInvalid):
                    self._response(routes_root=routes)

    def test_pointer_guard_covers_every_publication_layer_and_survives_cache_clear(self):
        from dashboard.opportunity_facts import (
            load_latest_historical_opportunities,
        )

        loaded = load_latest_historical_opportunities(
            self.fixture.historical_root.parent,
            self.fixture.raw_root,
        )
        try:
            pointer = loaded["pointer_sha256"]
            signature = loaded["publication_signature"]
        finally:
            loaded["validated_view"].close()
        roles = [
            "pointer:latest.json",
            "verification:report.json",
            "complete:manifest.json",
            next(
                row[0] for row in signature
                if row[0].startswith("complete:route_legs")
            ),
            next(
                row[0] for row in signature
                if row[0].startswith("core:route_legs")
            ),
            next(
                row[0] for row in signature
                if row[0].startswith("raw:")
                and row[0] != "raw:run_manifest.json"
            ),
        ]
        for role in roles:
            with self.subTest(role=role):
                self.server._reset_historical_pointer_publication_identities_for_tests()
                self.server.require_stable_historical_pointer_publication_identity(
                    pointer_sha256=pointer,
                    publication_signature=signature,
                )
                self.server.clear_runtime_caches()
                changed = list(signature)
                index = next(
                    offset for offset, row in enumerate(changed)
                    if row[0] == role
                )
                row = list(changed[index])
                row[4] = row[4] + 1
                changed[index] = tuple(row)
                with self.assertRaises(self.server.OpportunityBundleInvalid):
                    self.server.require_stable_historical_pointer_publication_identity(
                        pointer_sha256=pointer,
                        publication_signature=tuple(changed),
                    )

    def test_new_pointer_can_register_but_old_pointer_cannot_be_rebound(self):
        original = (("pointer:latest.json", "b" * 64, 1, 1, 1),)
        new_pointer = (("pointer:latest.json", "c" * 64, 1, 1, 2),)
        changed = (("pointer:latest.json", "b" * 64, 1, 1, 2),)
        self.server.require_stable_historical_pointer_publication_identity(
            pointer_sha256="b" * 64,
            publication_signature=original,
        )
        self.server.require_stable_historical_pointer_publication_identity(
            pointer_sha256="c" * 64,
            publication_signature=new_pointer,
        )
        with self.assertRaises(self.server.OpportunityBundleInvalid):
            self.server.require_stable_historical_pointer_publication_identity(
                pointer_sha256="b" * 64,
                publication_signature=changed,
            )

    def test_handler_routes_historical_endpoint_and_maps_corruption_to_503(self):
        handler = object.__new__(self.server.MarketMonitorHandler)
        handler.path = "/api/markets/opportunities/historical?class=all"
        handler.headers = {}
        with mock.patch.object(
            self.server.MarketMonitorHandler,
            "send_public_api",
            side_effect=self.server.OpportunityBundleInvalid(),
        ) as send_public, mock.patch.object(
            self.server.MarketMonitorHandler, "send_json"
        ) as send_json:
            handler.do_GET()

        send_public.assert_called_once_with(
            "opportunities_historical", {"class": ["all"]}
        )
        send_json.assert_called_once_with(
            {
                "code": "opportunity_bundle_validation_failed",
                "message": (
                    "Published route opportunity data failed validation. "
                    "Retry after the next complete publication."
                ),
            },
            503,
        )


if __name__ == "__main__":
    unittest.main()
