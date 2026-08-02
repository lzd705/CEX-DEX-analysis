import argparse
import copy
import gzip
import hashlib
import json
import shutil
import tempfile
import inspect
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from dashboard import server
from dashboard.opportunity_facts import (
    build_opportunity_payload,
    build_unavailable_opportunity_payload,
)
import scripts.check_dashboard_release as release_checker
from scripts.check_dashboard_release import (
    DAILY_FACT_EVIDENCE_FIELDS,
    ReleaseCheckError,
    ResponseMetrics,
    STATIC_ASSET_FILENAMES,
    _validate_daily_fact_evidence,
    fetch_static_asset_bundle,
    release_check,
    validate_comparison,
    validate_events,
    validate_execution,
    validate_exact_cex_market_identity,
    validate_quality,
    validate_screening_quality_parity,
    validate_summary,
    validate_token_catalog,
)
from scripts.cex_instrument_lifecycle import configured_market_ids_sha256
from scripts.static_asset_contract import PUBLIC_STATIC_ASSET_SOURCES
import scripts.route_publication as route_publication
import scripts.route_opportunity as route_opportunity
from tests import test_opportunity_api as opportunity_fixture


def _opportunity_inventory_sha256(rows):
    members = [
        {
            "opportunity_id": str(row.get("opportunity_id")),
            "route_id": str(row.get("route_id")),
            "token_symbol": str(row.get("token_symbol")),
            "requested_notional_usd": str(row.get("requested_notional_usd")),
            "opportunity_class": str(row.get("opportunity_class")),
        }
        for row in rows
    ]
    members.sort(key=lambda row: row["opportunity_id"])
    return hashlib.sha256(json.dumps(
        members,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _route_candidates_for_rows(rows):
    route_ids = sorted({str(row["route_id"]) for row in rows})
    volumes = [
        ("500", "700", "500"),
        ("100", "300", "100"),
        ("100", None, None),
    ]
    return [
        {
            "route_id": route_id,
            "buy_reference_volume_usd": values[0],
            "sell_reference_volume_usd": values[1],
            "route_volume_usd": values[2],
            "route_volume_basis": "minimum_leg_source_horizon_usd",
        }
        for route_id, values in zip(route_ids, volumes)
    ]


def _opportunity_metrics(
    path,
    payload,
    *,
    raw_bytes=None,
    wire_bytes=None,
    compressed=True,
    request_started_at=None,
    response_completed_at=None,
    elapsed_ms=7.5,
):
    raw = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    wire = gzip.compress(raw) if compressed else raw
    checked_at = (payload.get("metadata") or {}).get("checked_at")
    if checked_at and (
        request_started_at is None or response_completed_at is None
    ):
        try:
            checked_at_datetime = datetime.fromisoformat(
                str(checked_at).replace("Z", "+00:00")
            )
        except ValueError:
            checked_at_datetime = None
        if checked_at_datetime is not None:
            if request_started_at is None:
                request_started_at = checked_at_datetime
            if response_completed_at is None:
                response_completed_at = checked_at_datetime
    return ResponseMetrics(
        path=path,
        elapsed_ms=elapsed_ms,
        wire_bytes=len(wire) if wire_bytes is None else wire_bytes,
        raw_bytes=len(raw) if raw_bytes is None else raw_bytes,
        compressed=compressed,
        request_started_at=request_started_at,
        response_completed_at=response_completed_at,
    )


def _add_public_cost_provenance(payload, source_costs, source_rows):
    """Mirror cost provenance, then apply the real public server envelope."""

    costs_by_key = {
        (
            str(item["opportunity_id"]),
            str(item["leg"]),
            str(item["component_type"]),
        ): item
        for item in source_costs
    }
    rows_by_id = {
        str(item["opportunity_id"]): item for item in source_rows
    }
    for route in payload.get("routes", []):
        opportunity_id = str(route["opportunity_id"])
        for component in route.get("cost_components", []):
            source = costs_by_key[
                (
                    opportunity_id,
                    str(component["leg"]),
                    str(component["component_type"]),
                )
            ]
            component["strict_eligible"] = (
                False
                if component.get("value_status") == "stale"
                else source["strict_eligible"]
            )
            component["embedded_in_leg_quote"] = source[
                "embedded_in_leg_quote"
            ]
            reflected = set(
                rows_by_id[opportunity_id].get(
                    "reflected_or_embedded_component_keys", []
                )
            )
            component["reflected_or_embedded"] = bool(
                source["embedded_in_leg_quote"]
                or "{}:{}".format(
                    component["leg"], component["component_type"]
                ) in reflected
            )
    payload.update(server.attach_public_action_capabilities(payload))
    return payload


def _opportunity_binding(row):
    payload = dict(row)
    payload.pop("evidence_binding_sha256", None)
    return hashlib.sha256(json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _reseal_complete_opportunity(bundle, opportunity_id):
    row = next(
        item for item in bundle["opportunities"]
        if item["opportunity_id"] == opportunity_id
    )
    components = [
        item for item in bundle["cost_components"]
        if item["opportunity_id"] == opportunity_id
    ]
    row["cost_component_set_sha256"] = (
        route_publication._canonical_cost_set_sha256(components)
    )
    if row["strict_eligible"]:
        row["publication_attestation_sha256"] = (
            route_publication._publication_binding_sha256(
                cohort_id=row["cohort_id"],
                opportunity_id=row["opportunity_id"],
                route_id=row["route_id"],
                target_token_quantity=row["target_token_quantity"],
                buy_state_id=row["buy_state_id"],
                sell_state_id=row["sell_state_id"],
                buy_usd_projection_sha256=row["buy_usd_projection_sha256"],
                sell_usd_projection_sha256=row["sell_usd_projection_sha256"],
                cost_component_set_sha256=row["cost_component_set_sha256"],
                mode_evidence_sha256=row["mode_evidence_sha256"],
                core_manifest_sha256=bundle["core_manifest_sha256"],
            )
        )
    row["evidence_binding_sha256"] = _opportunity_binding(row)
    bundle["cost_components"].sort(key=lambda item: (
        item["opportunity_id"], item["leg"], item["component_type"]
    ))
    bundle["input_generations"]["cost_component_generation"] = (
        route_publication._canonical_input_sha256(bundle["cost_components"])
    )


class RouteOpportunityReleaseGateTest(unittest.TestCase):
    """Release checks consume only Task 7's complete public generation."""

    @classmethod
    def setUpClass(cls):
        from tests.test_route_publication import _task7_cex_inputs

        cls.base_temporary = tempfile.TemporaryDirectory()
        base = Path(cls.base_temporary.name)
        cls.base_routes = base / "data/local/routes"
        fixture = _task7_cex_inputs(
            cls.base_routes / "core",
            base / "data/raw/route-cohort",
            base / "data/local/route-sources",
            base / "private",
        )
        route_publication.publish_complete_route_bundle(
            core_root=cls.base_routes / "core",
            routes_root=cls.base_routes,
            raw_root=base / "data/raw/route-cohort",
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        cls.validated_at = "2026-08-01T12:02:00Z"

    @classmethod
    def tearDownClass(cls):
        cls.base_temporary.cleanup()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.routes_root = Path(self.temporary.name) / "routes"
        shutil.copytree(self.base_routes, self.routes_root)

    def _loaded(self):
        return route_publication.load_latest_complete_route_bundle(
            self.routes_root
        )

    def _rewrite(self, bundle):
        artifacts, _manifest = route_publication._complete_artifact_bytes(
            bundle
        )
        bundle_path = (
            self.routes_root / "bundles" / bundle["route_cohort_id"]
        )
        for filename, value in artifacts.items():
            (bundle_path / filename).write_bytes(value)
        manifest_sha = hashlib.sha256(artifacts["manifest.json"]).hexdigest()
        pointer = {
            "schema": route_publication.ROUTE_OPPORTUNITY_POINTER_SCHEMA,
            "bundle_stage": route_publication.ROUTE_OPPORTUNITY_BUNDLE_STAGE,
            "route_cohort_id": bundle["route_cohort_id"],
            "manifest_sha256": manifest_sha,
            "core_manifest_sha256": bundle["core_manifest_sha256"],
            "core_pointer_sha256": bundle["core_pointer_sha256"],
        }
        (self.routes_root / "latest.json").write_text(
            json.dumps(pointer, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _validate(self, *, required=True, now=None):
        return release_checker.validate_route_opportunity_release(
            self.routes_root,
            required=required,
            now=now or self.validated_at,
        )

    def _public_payload_for_path(self, loaded, path):
        query = {
            key: values[-1]
            for key, values in parse_qs(urlsplit(path).query).items()
        }
        payload = build_opportunity_payload(
            loaded["bundle"]["opportunities"],
            manifest=loaded["manifest"],
            legs=loaded["legs"],
            cost_components=loaded["cost_components"],
            route_candidates=loaded["bundle"]["routes"],
            token=query.get("token"),
            venue=query.get("venue"),
            notional_usd=query.get("notional"),
            opportunity_class=query.get("class"),
            route_type=query.get("route_type"),
            availability=query.get("availability"),
            sort=query.get("sort"),
            direction=query.get("dir"),
            now=datetime.fromisoformat(
                self.validated_at.replace("Z", "+00:00")
            ),
            manifest_sha256=loaded["manifest_sha256"],
        )
        return _add_public_cost_provenance(
            payload,
            loaded["cost_components"],
            loaded["bundle"]["opportunities"],
        )

    def test_valid_complete_public_bundle_passes_and_reports_strict_counts(self):
        result = self._validate()
        loaded = self._loaded()

        self.assertEqual(result["status"], "validated")
        self.assertEqual(result["bundle_stage"], "route_opportunity/v1")
        self.assertEqual(result["strict_opportunity_count"], 5)
        self.assertEqual(result["research_opportunity_count"], 0)
        self.assertEqual(
            result["opportunity_inventory_sha256"],
            _opportunity_inventory_sha256(
                loaded["bundle"]["opportunities"]
            ),
        )

    def test_checker_reason_registry_covers_every_canonical_mode_reason(self):
        canonical_mode_reasons = set().union(
            *route_opportunity._MODE_REASON_CODES_BY_MODE.values()
        )

        self.assertTrue(
            canonical_mode_reasons
            <= release_checker._OPPORTUNITY_REASON_CODES
        )

    def test_checker_accepts_canonical_hyphenated_dex_venue(self):
        self.assertEqual(
            release_checker._route_public_leg_venue(
                "dex:ethereum:uniswap-v3:0xPool:AAVE"
            ),
            "uniswap-v3",
        )

    def test_missing_pointer_api_is_200_unavailable_with_no_route_values(self):
        (self.routes_root / "latest.json").unlink()
        route_release = self._validate(required=False)
        payload = server.attach_public_action_capabilities(
            build_unavailable_opportunity_payload(
                sort="route_id",
                direction="asc",
            )
        )
        requested = []

        def fake_fetch(_base_url, path, *, timeout):
            self.assertEqual(timeout, 1.0)
            requested.append(path)
            return payload, _opportunity_metrics(
                path, payload, compressed=False
            )

        with patch(
            "scripts.check_dashboard_release.fetch_json",
            side_effect=fake_fetch,
        ):
            result, metrics = release_checker.validate_opportunity_api_release(
                "https://dashboard.test",
                timeout=1.0,
                route_release=route_release,
                raw_max=2_000_000,
                gzip_max=300_000,
            )

        self.assertEqual(result, {
            "status": "unavailable",
            "reason": "complete_pointer_absent",
            "cold_elapsed_ms": 7.5,
            "warm_elapsed_ms": 7.5,
            "request_count": 2,
        })
        self.assertEqual(len(metrics), 2)
        self.assertEqual(requested[0], requested[1])
        self.assertTrue(
            requested[0].startswith("/api/markets/opportunities?")
        )

        invalid_cases = {
            "fixed reason": {
                **copy.deepcopy(payload),
                "availability": {
                    "status": "unavailable",
                    "reason": "network_failed",
                },
            },
            "route values": {
                **copy.deepcopy(payload),
                "routes": [{"route_id": "route:x", "net_edge_usd": "0"}],
            },
        }
        for label, invalid in invalid_cases.items():
            with self.subTest(label=label), self.assertRaisesRegex(
                ReleaseCheckError,
                "complete_pointer_absent|route rows",
            ):
                release_checker._validate_opportunity_api_payload(
                    invalid,
                    _opportunity_metrics(
                        "/api/markets/opportunities", invalid
                    ),
                    route_release=route_release,
                    expected_filters=payload["filters"],
                    raw_max=2_000_000,
                    gzip_max=300_000,
                    require_complete_inventory=True,
                )

    def test_checker_accepts_server_public_action_capability_metadata(self):
        (self.routes_root / "latest.json").unlink()
        route_release = self._validate(required=False)
        with patch.dict(
            server.os.environ,
            {"MARKET_ROUTE_DATA_DIR": str(self.routes_root)},
            clear=True,
        ):
            payload = server._build_public_api_payload(
                "opportunities",
                (("sort", "route_id"), ("dir", "asc")),
            )

        validated = release_checker._validate_opportunity_api_payload(
            payload,
            _opportunity_metrics(
                "/api/markets/opportunities", payload
            ),
            route_release=route_release,
            expected_filters=payload["filters"],
            raw_max=2_000_000,
            gzip_max=300_000,
            require_complete_inventory=True,
        )

        self.assertEqual(validated["status"], "unavailable")

    def test_checker_rejects_missing_server_public_action_capability_metadata(self):
        (self.routes_root / "latest.json").unlink()
        route_release = self._validate(required=False)
        payload = build_unavailable_opportunity_payload(
            sort="route_id",
            direction="asc",
        )

        with self.assertRaisesRegex(
            ReleaseCheckError,
            "metadata fields differ",
        ):
            release_checker._validate_opportunity_api_payload(
                payload,
                _opportunity_metrics(
                    "/api/markets/opportunities", payload
                ),
                route_release=route_release,
                expected_filters=payload["filters"],
                raw_max=2_000_000,
                gzip_max=300_000,
                require_complete_inventory=True,
            )

    def test_checker_rejects_capability_change_between_cold_and_warm(self):
        (self.routes_root / "latest.json").unlink()
        route_release = self._validate(required=False)
        enabled = server.attach_public_action_capabilities(
            build_unavailable_opportunity_payload(
                sort="route_id",
                direction="asc",
            )
        )
        disabled = copy.deepcopy(enabled)
        disabled["metadata"]["public_actions"] = {
            "fact_refresh_enabled": not enabled["metadata"][
                "public_actions"
            ]["fact_refresh_enabled"],
        }
        responses = [enabled, disabled]

        def fake_fetch(_base_url, path, *, timeout):
            self.assertEqual(timeout, 1.0)
            payload = responses.pop(0)
            return payload, _opportunity_metrics(path, payload)

        with patch(
            "scripts.check_dashboard_release.fetch_json",
            side_effect=fake_fetch,
        ), self.assertRaisesRegex(
            ReleaseCheckError,
            "public action capability changed",
        ):
            release_checker.validate_opportunity_api_release(
                "https://dashboard.test",
                timeout=1.0,
                route_release=route_release,
                raw_max=2_000_000,
                gzip_max=300_000,
            )

    def test_checker_rejects_malformed_public_action_capability_metadata(self):
        (self.routes_root / "latest.json").unlink()
        route_release = self._validate(required=False)
        base_payload = build_unavailable_opportunity_payload(
            sort="route_id",
            direction="asc",
        )
        invalid_cases = {
            "non-boolean capability": (
                {"fact_refresh_enabled": "true"},
                "not boolean",
            ),
            "unexpected capability": (
                {
                    "fact_refresh_enabled": True,
                    "route_refresh_enabled": True,
                },
                "fields differ",
            ),
        }

        for label, (public_actions, error) in invalid_cases.items():
            payload = copy.deepcopy(base_payload)
            payload["metadata"]["public_actions"] = public_actions
            with self.subTest(label=label), self.assertRaisesRegex(
                ReleaseCheckError,
                error,
            ):
                release_checker._validate_opportunity_api_payload(
                    payload,
                    _opportunity_metrics(
                        "/api/markets/opportunities", payload
                    ),
                    route_release=route_release,
                    expected_filters=payload["filters"],
                    raw_max=2_000_000,
                    gzip_max=300_000,
                    require_complete_inventory=True,
                )

    def test_complete_bundle_api_cross_checks_filters_inventory_and_lineage(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        for row, opportunity_class, reason in (
            (
                bundle["opportunities"][-2],
                "research_estimate",
                "cost_component_estimated",
            ),
            (
                bundle["opportunities"][-1],
                "unavailable",
                "buy_leg_unavailable",
            ),
        ):
            row.update({
                "strict_eligible": False,
                "strict_ready_for_publication": False,
                "publication_attestation_sha256": None,
                "opportunity_class": opportunity_class,
                "primary_reason": reason,
                "reason_codes": [reason],
            })
            _reseal_complete_opportunity(bundle, row["opportunity_id"])
        self._rewrite(bundle)
        loaded = self._loaded()
        route_release = self._validate()
        requested = []

        def fake_fetch(_base_url, path, *, timeout):
            self.assertEqual(timeout, 1.0)
            requested.append(path)
            payload = self._public_payload_for_path(loaded, path)
            return payload, _opportunity_metrics(
                path, payload, elapsed_ms=0
            )

        with patch(
            "scripts.check_dashboard_release.fetch_json",
            side_effect=fake_fetch,
        ):
            result, metrics = release_checker.validate_opportunity_api_release(
                "https://dashboard.test",
                timeout=1.0,
                route_release=route_release,
                raw_max=2_000_000,
                gzip_max=300_000,
            )

        self.assertEqual(result["status"], "validated")
        self.assertEqual(
            result["route_cohort_id"], route_release["route_cohort_id"]
        )
        self.assertEqual(
            result["manifest_sha256"], route_release["manifest_sha256"]
        )
        self.assertEqual(result["class_counts"], {
            "executable_candidate": 3,
            "research_estimate": 1,
            "unavailable": 1,
        })
        self.assertEqual(result["filter_check_count"], 6)
        self.assertEqual(result["request_count"], 8)
        self.assertEqual(len(metrics), 8)
        self.assertEqual(requested[0], requested[1])
        self.assertTrue(any("class=strict" in path for path in requested))
        self.assertTrue(any("class=estimate" in path for path in requested))
        self.assertTrue(any(
            "sort=volume" in path and "dir=asc" in path
            for path in requested
        ))
        self.assertTrue(any(
            "sort=volume" in path and "dir=desc" in path
            for path in requested
        ))
        self.assertTrue(
            any("availability=unavailable" in path for path in requested)
        )
        self.assertTrue(any("token=AAVE" in path for path in requested))
        self.assertTrue(any("venue=binance" in path for path in requested))
        self.assertEqual(
            result["opportunity_inventory_sha256"],
            route_release["opportunity_inventory_sha256"],
        )

    def test_cross_request_age_boundary_is_not_a_generation_change(self):
        loaded = self._loaded()
        route_release = self._validate()
        boundary = datetime.fromisoformat("2026-08-01T12:03:00+00:00")
        after_boundary = datetime.fromisoformat(
            "2026-08-01T12:03:00.000001+00:00"
        )
        request_times = [boundary] + [after_boundary] * 7

        def fake_fetch(_base_url, path, *, timeout):
            self.assertEqual(timeout, 1.0)
            now = request_times.pop(0)
            query = {
                key: values[-1]
                for key, values in parse_qs(urlsplit(path).query).items()
            }
            payload = build_opportunity_payload(
                loaded["bundle"]["opportunities"],
                manifest=loaded["manifest"],
                legs=loaded["legs"],
                cost_components=loaded["cost_components"],
                route_candidates=loaded["bundle"]["routes"],
                token=query.get("token"),
                venue=query.get("venue"),
                notional_usd=query.get("notional"),
                opportunity_class=query.get("class"),
                route_type=query.get("route_type"),
                availability=query.get("availability"),
                sort=query.get("sort"),
                direction=query.get("dir"),
                now=now,
                manifest_sha256=loaded["manifest_sha256"],
            )
            _add_public_cost_provenance(
                payload,
                loaded["cost_components"],
                loaded["bundle"]["opportunities"],
            )
            return payload, _opportunity_metrics(
                path, payload, elapsed_ms=0
            )

        with patch(
            "scripts.check_dashboard_release.fetch_json",
            side_effect=fake_fetch,
        ):
            result, metrics = release_checker.validate_opportunity_api_release(
                "https://dashboard.test",
                timeout=1.0,
                route_release=route_release,
                raw_max=2_000_000,
                gzip_max=300_000,
            )

        self.assertEqual(result["status"], "validated")
        self.assertEqual(len(metrics), 8)
        self.assertEqual(request_times, [])

    def test_public_routes_are_exactly_bound_to_the_complete_bundle(self):
        loaded = self._loaded()
        route_release = self._validate()

        def wrong_allowlisted_source(payload):
            route = next(
                row for row in payload["routes"]
                if row["availability"]["status"] == "available"
            )
            link = route["source_links"][0]
            link["url"] = (
                "https://api.kraken.com"
                if link["url"] != "https://api.kraken.com"
                else "https://api.binance.com"
            )

        def forged_economics(payload):
            route = next(
                row for row in payload["routes"]
                if row["availability"]["status"] == "available"
            )
            route["gross_edge_usd"] = str(
                Decimal(route["gross_edge_usd"]) + Decimal("100")
            )
            route["net_edge_usd"] = str(
                Decimal(route["net_edge_usd"]) + Decimal("100")
            )

        def forged_cost_provenance(payload):
            route = next(
                row for row in payload["routes"]
                if row["availability"]["status"] == "available"
            )
            component = next(
                item for item in route["cost_components"]
                if item["amount_usd"] is not None
                and item["reflected_or_embedded"] is True
            )
            component["reflected_or_embedded"] = False
            amount = Decimal(component["amount_usd"])
            strict_cost = Decimal(
                route["cost_breakdown"]["strict_nonembedded_usd"]
            ) + amount
            route["cost_breakdown"]["strict_nonembedded_usd"] = str(
                strict_cost
            )
            route["net_edge_usd"] = str(
                Decimal(route["gross_edge_usd"]) - strict_cost
            )

        for label, mutate in (
            ("wrong allowlisted source host", wrong_allowlisted_source),
            ("coherently forged economics", forged_economics),
            ("coherently forged cost provenance", forged_cost_provenance),
        ):
            with self.subTest(label=label):
                def fake_fetch(_base_url, path, *, timeout):
                    self.assertEqual(timeout, 1.0)
                    payload = self._public_payload_for_path(loaded, path)
                    if any(
                        row["availability"]["status"] == "available"
                        for row in payload["routes"]
                    ):
                        mutate(payload)
                    return payload, _opportunity_metrics(path, payload)

                with patch(
                    "scripts.check_dashboard_release.fetch_json",
                    side_effect=fake_fetch,
                ), self.assertRaisesRegex(
                    ReleaseCheckError,
                    "public row differs from the complete bundle",
                ):
                    release_checker.validate_opportunity_api_release(
                        "https://dashboard.test",
                        timeout=1.0,
                        route_release=route_release,
                        raw_max=2_000_000,
                        gzip_max=300_000,
                    )

    def test_complete_route_volume_cannot_diverge_from_pinned_core(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        route = bundle["routes"][0]
        route["buy_reference_volume_usd"] = "8000"
        route["sell_reference_volume_usd"] = "6000"
        route["route_volume_usd"] = "6000"
        self._rewrite(bundle)

        with self.assertRaisesRegex(
            ReleaseCheckError,
            "complete.*core|core.*complete|lineage",
        ):
            self._validate()

    def test_complete_core_pointer_hash_must_match_canonical_core_pointer(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        bundle["core_pointer_sha256"] = "f" * 64
        self._rewrite(bundle)

        with self.assertRaisesRegex(
            ReleaseCheckError,
            "core pointer.*lineage|lineage.*core pointer",
        ):
            self._validate()

    def test_complete_core_context_must_match_pinned_core(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        bundle["core_context"]["collection_input_generation"] = (
            "forged-generation"
        )
        bundle["input_generations"]["collection_input_generation"] = (
            "forged-generation"
        )
        self._rewrite(bundle)

        with self.assertRaisesRegex(
            ReleaseCheckError,
            "core context.*lineage|lineage.*core context",
        ):
            self._validate()

    def test_complete_route_legs_must_match_pinned_core(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        bundle["legs"][0]["source_endpoint"] = (
            "https://api.bybit.com/v5/market/orderbook"
        )
        self._rewrite(bundle)

        with self.assertRaisesRegex(
            ReleaseCheckError,
            "route legs.*lineage|lineage.*route legs",
        ):
            self._validate()

    def test_filtered_public_route_body_is_bound_to_the_complete_bundle(self):
        loaded = self._loaded()
        route_release = self._validate()
        request_count = {"value": 0}

        def fake_fetch(_base_url, path, *, timeout):
            self.assertEqual(timeout, 1.0)
            request_count["value"] += 1
            payload = self._public_payload_for_path(loaded, path)
            if request_count["value"] > 2:
                available = next(
                    (
                        row for row in payload["routes"]
                        if row["availability"]["status"] == "available"
                    ),
                    None,
                )
                if available is not None:
                    available["gross_edge_usd"] = str(
                        Decimal(available["gross_edge_usd"])
                        + Decimal("100")
                    )
                    available["net_edge_usd"] = str(
                        Decimal(available["net_edge_usd"])
                        + Decimal("100")
                    )
            return payload, _opportunity_metrics(path, payload)

        with patch(
            "scripts.check_dashboard_release.fetch_json",
            side_effect=fake_fetch,
        ), self.assertRaisesRegex(
            ReleaseCheckError,
            "public row differs from the complete bundle",
        ):
            release_checker.validate_opportunity_api_release(
                "https://dashboard.test",
                timeout=1.0,
                route_release=route_release,
                raw_max=2_000_000,
                gzip_max=300_000,
            )

    def test_filtered_api_views_cannot_diverge_from_full_inventory_metadata(self):
        loaded = self._loaded()
        route_release = self._validate()
        mutations = {
            "route count": lambda payload: payload["metadata"]["coverage"].update(
                route_count=payload["metadata"]["coverage"]["route_count"] + 1
            ),
            "availability counts": lambda payload: payload["metadata"][
                "coverage"
            ]["availability_counts"].update(
                available=payload["metadata"]["coverage"][
                    "availability_counts"
                ].get("available", 0) + 1
            ),
            "freshness deadline": lambda payload: payload["metadata"].update(
                next_freshness_deadline_at=None
            ),
            "public action capability": lambda payload: payload["metadata"][
                "public_actions"
            ].update(
                fact_refresh_enabled=not payload["metadata"][
                    "public_actions"
                ]["fact_refresh_enabled"]
            ),
        }

        for label, mutate in mutations.items():
            with self.subTest(label=label):
                request_count = 0

                def fake_fetch(_base_url, path, *, timeout):
                    nonlocal request_count
                    self.assertEqual(timeout, 1.0)
                    request_count += 1
                    payload = self._public_payload_for_path(loaded, path)
                    if request_count > 2 and "availability=unavailable" in path:
                        mutate(payload)
                    return payload, _opportunity_metrics(path, payload)

                with patch(
                    "scripts.check_dashboard_release.fetch_json",
                    side_effect=fake_fetch,
                ), self.assertRaisesRegex(
                    ReleaseCheckError,
                (
                    "full view|generation|complete bundle|checked_at|"
                    "boundary mode|public action capability"
                ),
                ):
                    release_checker.validate_opportunity_api_release(
                        "https://dashboard.test",
                        timeout=1.0,
                        route_release=route_release,
                        raw_max=2_000_000,
                        gzip_max=300_000,
                    )

    def test_api_payload_rejects_release_counterexamples(self):
        rows = opportunity_fixture.OpportunityPayloadTests()._rows()
        manifest = opportunity_fixture._manifest(rows)
        manifest_sha256 = "b" * 64
        public_costs = [
            component
            for row in rows
            for component in opportunity_fixture._route_costs(row)
        ]
        route_candidates = _route_candidates_for_rows(rows)
        payload = build_opportunity_payload(
            rows,
            manifest=manifest,
            legs=opportunity_fixture.LEGS,
            cost_components=public_costs,
            route_candidates=route_candidates,
            opportunity_class="all",
            availability="all",
            sort="route_id",
            direction="asc",
            now=opportunity_fixture.NOW,
            manifest_sha256=manifest_sha256,
        )
        _add_public_cost_provenance(payload, public_costs, rows)
        route_release = {
            "status": "validated",
            "reason": None,
            "route_cohort_id": opportunity_fixture.COHORT_ID,
            "manifest_sha256": manifest_sha256,
            "strict_opportunity_count": 1,
            "research_opportunity_count": 1,
            "unavailable_opportunity_count": 1,
            "opportunity_inventory_sha256": _opportunity_inventory_sha256(
                rows
            ),
            release_checker._OPPORTUNITY_PUBLIC_BINDING_ROWS: (
                release_checker._route_public_binding_inventory(
                    rows,
                    opportunity_fixture.LEGS,
                    public_costs,
                    route_candidates,
                )
            ),
        }
        expected_filters = payload["filters"]

        null_source = copy.deepcopy(payload)
        null_source["routes"][0]["source_links"][0]["url"] = None
        invalid = {"missing exact source origin": null_source}
        mixed = copy.deepcopy(payload)
        mixed["filters"]["opportunity_class"] = "strict"
        invalid["mixed strict and estimate rows"] = mixed

        invalid_counts = copy.deepcopy(payload)
        invalid_counts["metadata"]["coverage"]["class_counts"][
            "executable_candidate"
        ] = 2
        invalid["invalid class counts"] = invalid_counts

        forged_volume = copy.deepcopy(payload)
        forged_volume["routes"][0]["route_volume_usd"] = "999999"
        invalid["forged route reference volume"] = forged_volume

        unknown_reason = copy.deepcopy(payload)
        unavailable = next(
            row for row in unknown_reason["routes"]
            if row["opportunity_class"] == "unavailable"
        )
        unavailable["availability"]["reason"] = "mystery_reason"
        unavailable["primary_reason"] = "mystery_reason"
        unavailable["reason_codes"] = ["mystery_reason"]
        invalid["unknown reason"] = unknown_reason

        stale_numeric = copy.deepcopy(payload)
        strict = next(
            row for row in stale_numeric["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["availability"] = {
            "status": "unavailable",
            "reason": "cohort_stale",
        }
        invalid["stale strict numeric"] = stale_numeric

        unavailable_residue = copy.deepcopy(payload)
        unavailable = next(
            row for row in unavailable_residue["routes"]
            if row["opportunity_class"] == "unavailable"
        )
        unavailable.update({
            "gross_edge_usd": "100",
            "gross_edge_bps": "100",
            "target_token_quantity": "5",
            "capacity_quantity": "10",
        })
        unavailable["cost_breakdown"] = {
            "strict_nonembedded_usd": "1",
            "research_bounded_usd": "2",
            "research_assumed_usd": "3",
        }
        for component in unavailable["cost_components"]:
            component["amount_usd"] = "1"
            component["rate_bps"] = "1"
        invalid["unavailable economic residue"] = unavailable_residue

        generation = copy.deepcopy(payload)
        generation["metadata"]["route_cohort_id"] = "cohort:" + "c" * 64
        invalid["generation mismatch"] = generation

        manifest_mismatch = copy.deepcopy(payload)
        manifest_mismatch["metadata"]["manifest_sha256"] = "d" * 64
        invalid["manifest mismatch"] = manifest_mismatch

        inventory = copy.deepcopy(payload)
        inventory["routes"].pop()
        inventory["metadata"]["coverage"]["returned_count"] -= 1
        invalid["route inventory divergence"] = inventory

        wrong_age = copy.deepcopy(payload)
        research = next(
            row for row in wrong_age["routes"]
            if row["opportunity_class"] == "research_estimate"
        )
        research["route_age_seconds"] = 999
        invalid["recomputed route age"] = wrong_age

        wrong_skew = copy.deepcopy(payload)
        strict = next(
            row for row in wrong_skew["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["skew_seconds"] = 29.999999
        invalid["recomputed route skew"] = wrong_skew

        stale_wrong_reason = copy.deepcopy(payload)
        research = next(
            row for row in stale_wrong_reason["routes"]
            if row["opportunity_class"] == "research_estimate"
        )
        research["leg_timestamps"] = {
            "buy": "2026-08-01T11:58:30Z",
            "sell": "2026-08-01T11:58:00Z",
        }
        research["skew_seconds"] = 30
        research["route_age_seconds"] = 180
        research["availability"] = {
            "status": "unavailable",
            "reason": "snapshot_skew_exceeded",
        }
        research["target_token_quantity"] = None
        research["gross_edge_usd"] = None
        research["gross_edge_bps"] = None
        research["net_edge_usd"] = None
        research["net_edge_bps"] = None
        research["capacity_quantity"] = None
        research["cost_breakdown"] = {
            "strict_nonembedded_usd": None,
            "research_bounded_usd": None,
            "research_assumed_usd": None,
        }
        for component in research["cost_components"]:
            component["amount_usd"] = None
            component["rate_bps"] = None
        invalid["stale route wrong reason"] = stale_wrong_reason

        skew_wrong_reason = copy.deepcopy(payload)
        research = next(
            row for row in skew_wrong_reason["routes"]
            if row["opportunity_class"] == "research_estimate"
        )
        research["leg_timestamps"] = {
            "buy": "2026-08-01T12:01:00Z",
            "sell": "2026-08-01T11:59:30Z",
        }
        research["skew_seconds"] = 90
        research["route_age_seconds"] = 30
        research["availability"] = {
            "status": "unavailable",
            "reason": "cohort_stale",
        }
        research["target_token_quantity"] = None
        research["gross_edge_usd"] = None
        research["gross_edge_bps"] = None
        research["net_edge_usd"] = None
        research["net_edge_bps"] = None
        research["capacity_quantity"] = None
        research["cost_breakdown"] = {
            "strict_nonembedded_usd": None,
            "research_bounded_usd": None,
            "research_assumed_usd": None,
        }
        for component in research["cost_components"]:
            component["amount_usd"] = None
            component["rate_bps"] = None
        invalid["skewed route wrong reason"] = skew_wrong_reason

        wrong_source_market = copy.deepcopy(payload)
        wrong_source_market["routes"][0]["source_links"][0][
            "market_id"
        ] = "cex:kraken:AAVE/USD"
        invalid["source link leg mismatch"] = wrong_source_market

        missing_cost = copy.deepcopy(payload)
        strict = next(
            row for row in missing_cost["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["cost_components"].pop()
        invalid["missing strict cost topology"] = missing_cost

        wrong_cost_market = copy.deepcopy(payload)
        strict = next(
            row for row in wrong_cost_market["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["cost_components"][0]["market_id"] = strict["sell_market_id"]
        invalid["strict cost leg market mismatch"] = wrong_cost_market

        duplicate_cost = copy.deepcopy(payload)
        strict = next(
            row for row in duplicate_cost["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["cost_components"].append(
            copy.deepcopy(strict["cost_components"][0])
        )
        invalid["duplicate strict cost topology"] = duplicate_cost

        wrong_breakdown = copy.deepcopy(payload)
        strict = next(
            row for row in wrong_breakdown["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["cost_breakdown"]["strict_nonembedded_usd"] = "19"
        strict["net_edge_usd"] = "181"
        invalid["strict cost breakdown mismatch"] = wrong_breakdown

        false_embedded = copy.deepcopy(payload)
        strict = next(
            row for row in false_embedded["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["cost_components"][0]["reflected_or_embedded"] = True
        invalid["strict embedded breakdown mismatch"] = false_embedded

        invalid_cost_flags = copy.deepcopy(payload)
        strict = next(
            row for row in invalid_cost_flags["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["cost_components"][0]["strict_eligible"] = "true"
        invalid["invalid strict cost provenance flags"] = invalid_cost_flags

        inconsistent_embedded = copy.deepcopy(payload)
        strict = next(
            row for row in inconsistent_embedded["routes"]
            if row["opportunity_class"] == "executable_candidate"
        )
        strict["cost_components"][0]["embedded_in_leg_quote"] = True
        strict["cost_components"][0]["reflected_or_embedded"] = False
        invalid["inconsistent embedded cost marker"] = inconsistent_embedded

        missing_reason = copy.deepcopy(payload)
        unavailable = next(
            row for row in missing_reason["routes"]
            if row["opportunity_class"] == "unavailable"
        )
        unavailable["availability"]["reason"] = None
        unavailable["primary_reason"] = None
        unavailable["reason_codes"] = []
        invalid["missing N/A reason"] = missing_reason

        secret = copy.deepcopy(payload)
        secret["routes"][0]["source_links"][0]["url"] = (
            "https://example.test?api_key=SECRET_SENTINEL"
        )
        invalid["secret material"] = secret

        private_path = copy.deepcopy(payload)
        private_path["routes"][0]["source_links"][0]["url"] = (
            "/private/runtime/routes/manifest.json"
        )
        invalid["absolute path"] = private_path

        unsafe_origins = {
            "http source origin": "http://api.binance.com",
            "loopback source origin": "https://127.0.0.1",
            "private source origin": "https://10.0.0.5",
            "internal source origin": "https://metadata.google.internal",
            "unapproved source origin": "https://api.evil.example.org",
        }
        for label, url in unsafe_origins.items():
            candidate = copy.deepcopy(payload)
            candidate["routes"][0]["source_links"][0]["url"] = url
            invalid[label] = candidate

        for label, candidate in invalid.items():
            filters = (
                candidate["filters"] if label.startswith("mixed")
                else expected_filters
            )
            with self.subTest(label=label), self.assertRaises(ReleaseCheckError):
                release_checker._validate_opportunity_api_payload(
                    candidate,
                    _opportunity_metrics(
                        "/api/markets/opportunities", candidate
                    ),
                    route_release=route_release,
                    expected_filters=filters,
                    raw_max=2_000_000,
                    gzip_max=300_000,
                    require_complete_inventory=(
                        not label.startswith("mixed")
                    ),
                )

        with self.assertRaisesRegex(ReleaseCheckError, "raw payload"):
            release_checker._validate_opportunity_api_payload(
                payload,
                _opportunity_metrics(
                    "/api/markets/opportunities",
                    payload,
                    raw_bytes=2_000_001,
                ),
                route_release=route_release,
                expected_filters=expected_filters,
                raw_max=2_000_000,
                gzip_max=300_000,
                require_complete_inventory=True,
            )

    def test_api_timing_sla_accepts_exact_boundary_and_fails_closed_after(self):
        rows = opportunity_fixture.OpportunityPayloadTests()._rows()
        manifest = opportunity_fixture._manifest(rows)
        manifest_sha256 = "b" * 64
        public_costs = [
            component
            for row in rows
            for component in opportunity_fixture._route_costs(row)
        ]
        route_candidates = _route_candidates_for_rows(rows)
        route_release = {
            "status": "validated",
            "reason": None,
            "route_cohort_id": opportunity_fixture.COHORT_ID,
            "manifest_sha256": manifest_sha256,
            "strict_opportunity_count": 1,
            "research_opportunity_count": 1,
            "unavailable_opportunity_count": 1,
            "opportunity_inventory_sha256": _opportunity_inventory_sha256(
                rows
            ),
            release_checker._OPPORTUNITY_PUBLIC_BINDING_ROWS: (
                release_checker._route_public_binding_inventory(
                    rows,
                    opportunity_fixture.LEGS,
                    public_costs,
                    route_candidates,
                )
            ),
        }

        for checked_at, expected_status, expected_reason in (
            (
                datetime.fromisoformat("2026-08-01T12:03:00+00:00"),
                "available",
                None,
            ),
            (
                datetime.fromisoformat("2026-08-01T12:03:00.000001+00:00"),
                "unavailable",
                "cohort_stale",
            ),
        ):
            with self.subTest(checked_at=checked_at.isoformat()):
                payload = build_opportunity_payload(
                    rows,
                    manifest=manifest,
                    legs=opportunity_fixture.LEGS,
                    cost_components=public_costs,
                    route_candidates=route_candidates,
                    opportunity_class="all",
                    availability="all",
                    sort="route_id",
                    direction="asc",
                    now=checked_at,
                    manifest_sha256=manifest_sha256,
                )
                _add_public_cost_provenance(payload, public_costs, rows)
                validated = release_checker._validate_opportunity_api_payload(
                    payload,
                    _opportunity_metrics(
                        "/api/markets/opportunities",
                        payload,
                        elapsed_ms=0,
                    ),
                    route_release=route_release,
                    expected_filters=payload["filters"],
                    raw_max=2_000_000,
                    gzip_max=300_000,
                    require_complete_inventory=True,
                )
                strict = next(
                    row for row in validated["rows"]
                    if row["opportunity_class"] == "executable_candidate"
                )
                self.assertEqual(
                    strict["availability"],
                    {"status": expected_status, "reason": expected_reason},
                )

    def test_api_rejects_cached_checked_at_outside_request_wall_clock(self):
        loaded = self._loaded()
        route_release = self._validate()
        path = release_checker._opportunity_api_path(
            release_checker._opportunity_filters()
        )
        payload = self._public_payload_for_path(loaded, path)
        request_started_at = datetime.fromisoformat(
            "2026-08-01T12:05:01+00:00"
        )

        with self.assertRaisesRegex(
            ReleaseCheckError,
            "checked_at.*request wall clock",
        ):
            release_checker._validate_opportunity_api_payload(
                payload,
                _opportunity_metrics(
                    path,
                    payload,
                    request_started_at=request_started_at,
                    response_completed_at=request_started_at,
                ),
                route_release=route_release,
                expected_filters=payload["filters"],
                raw_max=2_000_000,
                gzip_max=300_000,
                require_complete_inventory=True,
            )

    def test_api_rejects_response_that_crosses_freshness_after_projection(self):
        loaded = self._loaded()
        route_release = self._validate()
        path = release_checker._opportunity_api_path(
            release_checker._opportunity_filters()
        )
        checked_at = datetime.fromisoformat("2026-08-01T12:02:56+00:00")
        request_started_at = datetime.fromisoformat(
            "2026-08-01T12:03:00+00:00"
        )
        response_completed_at = datetime.fromisoformat(
            "2026-08-01T12:03:01+00:00"
        )
        payload = build_opportunity_payload(
            loaded["bundle"]["opportunities"],
            manifest=loaded["manifest"],
            legs=loaded["legs"],
            cost_components=loaded["cost_components"],
            route_candidates=loaded["bundle"]["routes"],
            now=checked_at,
            manifest_sha256=loaded["manifest_sha256"],
            sort="route_id",
            direction="asc",
        )
        _add_public_cost_provenance(
            payload,
            loaded["cost_components"],
            loaded["bundle"]["opportunities"],
        )

        with self.assertRaisesRegex(
            ReleaseCheckError,
            "response completion.*freshness",
        ):
            release_checker._validate_opportunity_api_payload(
                payload,
                _opportunity_metrics(
                    path,
                    payload,
                    request_started_at=request_started_at,
                    response_completed_at=response_completed_at,
                    elapsed_ms=1_000,
                ),
                route_release=route_release,
                expected_filters=payload["filters"],
                raw_max=2_000_000,
                gzip_max=300_000,
                require_complete_inventory=True,
            )

    def test_api_rechecks_sealed_cost_expiry_at_response_time(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        affected = set()
        for component in bundle["cost_components"]:
            if component["component_type"] == "venue_taker_fee":
                component["valid_until"] = (
                    "2026-08-01T12:02:00.500000Z"
                )
                affected.add(component["opportunity_id"])
        for opportunity_id in sorted(affected):
            _reseal_complete_opportunity(bundle, opportunity_id)
        self._rewrite(bundle)
        loaded = self._loaded()
        route_release = self._validate(now="2026-08-01T12:02:00Z")
        checked_at = datetime.fromisoformat("2026-08-01T12:02:01+00:00")
        path = release_checker._opportunity_api_path(
            release_checker._opportunity_filters()
        )
        correct = build_opportunity_payload(
            loaded["bundle"]["opportunities"],
            manifest=loaded["manifest"],
            legs=loaded["legs"],
            cost_components=loaded["cost_components"],
            route_candidates=loaded["bundle"]["routes"],
            now=checked_at,
            manifest_sha256=loaded["manifest_sha256"],
            sort="route_id",
            direction="asc",
        )
        _add_public_cost_provenance(
            correct,
            loaded["cost_components"],
            loaded["bundle"]["opportunities"],
        )
        validated = release_checker._validate_opportunity_api_payload(
            correct,
            _opportunity_metrics(path, correct),
            route_release=route_release,
            expected_filters=correct["filters"],
            raw_max=2_000_000,
            gzip_max=300_000,
            require_complete_inventory=True,
        )
        self.assertTrue(all(
            row["availability"]["reason"] == "cost_component_stale"
            for row in validated["rows"]
        ))

        forged_costs = copy.deepcopy(loaded["cost_components"])
        for component in forged_costs:
            if component["component_type"] == "venue_taker_fee":
                component["valid_until"] = "2026-08-01T13:00:00Z"
        forged = build_opportunity_payload(
            loaded["bundle"]["opportunities"],
            manifest=loaded["manifest"],
            legs=loaded["legs"],
            cost_components=forged_costs,
            route_candidates=loaded["bundle"]["routes"],
            now=checked_at,
            manifest_sha256=loaded["manifest_sha256"],
            sort="route_id",
            direction="asc",
        )
        _add_public_cost_provenance(
            forged,
            forged_costs,
            loaded["bundle"]["opportunities"],
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "complete bundle|checked_at|cost",
        ):
            release_checker._validate_opportunity_api_payload(
                forged,
                _opportunity_metrics(path, forged),
                route_release=route_release,
                expected_filters=forged["filters"],
                raw_max=2_000_000,
                gzip_max=300_000,
                require_complete_inventory=True,
            )

    def test_valid_non_ascii_cost_evidence_uses_task7_canonical_encoding(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        opportunity_id = bundle["opportunities"][0]["opportunity_id"]
        component = next(
            item for item in bundle["cost_components"]
            if item["opportunity_id"] == opportunity_id
            and item["component_type"] == "venue_taker_fee"
            and item["leg"] == "buy"
        )
        component["source"] = "已验证的 CEX 费率证据"
        component["basis"] += "；账户等级已验证"
        _reseal_complete_opportunity(bundle, opportunity_id)
        self._rewrite(bundle)

        result = self._validate()

        self.assertEqual(result["status"], "validated")
        self.assertEqual(result["strict_opportunity_count"], 5)

    def test_prepublication_strict_ready_row_is_not_a_public_candidate(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        row = bundle["opportunities"][0]
        row.update({
            "strict_eligible": False,
            "opportunity_class": "research_estimate",
            "primary_reason": "publication_evidence_unverified",
            "reason_codes": ["publication_evidence_unverified"],
            "publication_attestation_sha256": None,
        })
        row["evidence_binding_sha256"] = _opportunity_binding(row)
        self._rewrite(bundle)

        with self.assertRaisesRegex(ReleaseCheckError, "prepublication"):
            self._validate()

    def test_missing_cost_stays_null_and_cannot_be_promoted_to_zero(self):
        loaded = self._loaded()
        bundle = copy.deepcopy(loaded["bundle"])
        row = bundle["opportunities"][0]
        component = next(
            item for item in bundle["cost_components"]
            if item["opportunity_id"] == row["opportunity_id"]
            and item["leg"] == "buy"
        )
        component.update({
            "value_status": "unsupported",
            "amount_usd": "0",
            "rate_bps": "0",
            "strict_eligible": False,
            "observed_at": None,
            "valid_until": None,
            "source_record_sha256": None,
            "reason_code": "cost_source_unsupported",
        })
        candidate = dict(loaded)
        candidate["bundle"] = bundle
        candidate["cost_components"] = bundle["cost_components"]

        with self.assertRaisesRegex(ReleaseCheckError, "cannot contain numeric"):
            release_checker._validate_loaded_route_opportunity_release(
                candidate, now=self.validated_at
            )

    def test_absent_complete_pointer_is_optional_but_core_pointer_is_never_public(self):
        (self.routes_root / "latest.json").unlink()
        self.assertEqual(
            self._validate(required=False),
            {"status": "unavailable", "reason": "complete_pointer_absent"},
        )
        with self.assertRaisesRegex(
            ReleaseCheckError, "required complete route opportunity.*unavailable"
        ):
            self._validate(required=True)

        core_pointer = json.loads(
            (self.routes_root / "core/latest.json").read_text(encoding="utf-8")
        )
        (self.routes_root / "latest.json").write_text(
            json.dumps(core_pointer) + "\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(
            ReleaseCheckError, "complete route opportunity validation failed"
        ):
            self._validate(required=False)

    def test_partial_or_divergent_bundle_never_counts_as_public(self):
        loaded = self._loaded()
        bundle_path = loaded["path"]
        (bundle_path / "cost_components.csv").unlink()
        with self.assertRaisesRegex(ReleaseCheckError, "validation failed"):
            self._validate()

        shutil.rmtree(self.routes_root)
        shutil.copytree(self.base_routes, self.routes_root)
        loaded = self._loaded()
        with (loaded["path"] / "route_opportunities.csv").open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaisesRegex(ReleaseCheckError, "validation failed"):
            self._validate()

    def test_coordinated_row_and_binding_rehash_cannot_change_common_quantity(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        row = bundle["opportunities"][0]
        row["target_token_quantity"] = "11"
        row["target_base_raw"] = "1100"
        for component in bundle["cost_components"]:
            if component["opportunity_id"] == row["opportunity_id"]:
                component["target_token_quantity"] = "11"
        _reseal_complete_opportunity(bundle, row["opportunity_id"])
        self._rewrite(bundle)

        with self.assertRaisesRegex(
            ReleaseCheckError, "inventory capacity|quantity"
        ):
            self._validate()

    def test_exact_opportunity_arithmetic_is_rebuilt_not_trusted(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        row = bundle["opportunities"][0]
        row["gross_edge_usd"] = "7.96"
        row["evidence_binding_sha256"] = _opportunity_binding(row)
        self._rewrite(bundle)

        with self.assertRaisesRegex(ReleaseCheckError, "gross edge"):
            self._validate()

    def test_state_quantity_skew_and_age_counterexamples_fail_closed(self):
        mutations = {
            "quantity lattice": lambda row: row.update(target_base_raw="1001"),
            "state lineage": lambda row: row.update(
                buy_state_observed_at="2026-08-01T12:00:01Z"
            ),
            "skew": lambda row: row.update(skew_seconds="61"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                shutil.rmtree(self.routes_root)
                shutil.copytree(self.base_routes, self.routes_root)
                bundle = copy.deepcopy(self._loaded()["bundle"])
                row = bundle["opportunities"][0]
                mutate(row)
                row["evidence_binding_sha256"] = _opportunity_binding(row)
                self._rewrite(bundle)
                with self.assertRaisesRegex(ReleaseCheckError, label.split()[0]):
                    self._validate()

        shutil.rmtree(self.routes_root)
        shutil.copytree(self.base_routes, self.routes_root)
        with self.assertRaisesRegex(ReleaseCheckError, "stale"):
            self._validate(now="2026-08-01T12:03:01Z")

    def test_stale_fee_evidence_cannot_remain_strict(self):
        bundle = copy.deepcopy(self._loaded()["bundle"])
        row = bundle["opportunities"][0]
        fee = next(
            item for item in bundle["cost_components"]
            if item["opportunity_id"] == row["opportunity_id"]
            and item["component_type"] == "venue_taker_fee"
        )
        fee["valid_until"] = "2026-08-01T12:01:59Z"
        _reseal_complete_opportunity(bundle, row["opportunity_id"])
        self._rewrite(bundle)

        with self.assertRaisesRegex(ReleaseCheckError, "stale cost"):
            self._validate()

    def test_missing_duplicate_or_orphan_costs_fail_closed(self):
        loaded = self._loaded()
        base = loaded["bundle"]
        opportunity_id = base["opportunities"][0]["opportunity_id"]
        variants = {}
        missing = copy.deepcopy(base)
        missing["cost_components"] = [
            item for item in missing["cost_components"]
            if not (
                item["opportunity_id"] == opportunity_id
                and item["component_type"] == "venue_taker_fee"
                and item["leg"] == "buy"
            )
        ]
        variants["missing"] = missing
        duplicate = copy.deepcopy(base)
        duplicate["cost_components"].append(
            copy.deepcopy(duplicate["cost_components"][0])
        )
        variants["duplicate"] = duplicate
        orphan = copy.deepcopy(base)
        orphan_row = copy.deepcopy(orphan["cost_components"][0])
        orphan_row["opportunity_id"] = "orphan-opportunity"
        orphan["cost_components"].append(orphan_row)
        variants["orphan"] = orphan

        validator = release_checker._validate_loaded_route_opportunity_release
        for label, bundle in variants.items():
            with self.subTest(label=label):
                candidate = dict(loaded)
                candidate["bundle"] = bundle
                candidate["cost_components"] = bundle["cost_components"]
                with self.assertRaisesRegex(ReleaseCheckError, label):
                    validator(candidate, now=self.validated_at)

    def test_fake_zero_gas_assumption_promotion_and_pool_fee_double_count_fail(self):
        loaded = self._loaded()
        base = loaded["bundle"]
        row = base["opportunities"][0]
        opportunity_id = row["opportunity_id"]
        fee = next(
            item for item in base["cost_components"]
            if item["opportunity_id"] == opportunity_id
            and item["leg"] == "buy"
        )
        cases = []

        fake_gas = copy.deepcopy(base)
        gas = next(
            item for item in fake_gas["cost_components"]
            if item["opportunity_id"] == opportunity_id and item["leg"] == "buy"
        )
        gas["component_type"] = "network_gas"
        gas["amount_usd"] = "0"
        gas["rate_bps"] = "0"
        cases.append(("fake zero gas", fake_gas))

        promoted = copy.deepcopy(base)
        assumed = next(
            item for item in promoted["cost_components"]
            if item["opportunity_id"] == opportunity_id and item["leg"] == "buy"
        )
        assumed["value_status"] = "assumed"
        assumed["strict_eligible"] = True
        cases.append(("assumed cost promoted to strict", promoted))

        validator = release_checker._validate_loaded_route_opportunity_release
        for label, bundle in cases:
            with self.subTest(label=label):
                candidate = dict(loaded)
                candidate["bundle"] = bundle
                candidate["cost_components"] = bundle["cost_components"]
                candidate["opportunities"] = bundle["opportunities"]
                with self.assertRaisesRegex(ReleaseCheckError, label.split()[0]):
                    validator(candidate, now=self.validated_at)

        pool_row = copy.deepcopy(row)
        pool_row.update({
            "strict_eligible": False,
            "strict_ready_for_publication": False,
            "opportunity_class": "research_estimate",
        })
        pool_route = copy.deepcopy(base["routes"][0])
        pool_route.update({
            "route_mode": "atomic_onchain",
            "buy_market_id": "dex:eth:uniswap:0xbuy:AAVE",
            "sell_market_id": "dex:eth:uniswap:0xsell:AAVE",
        })
        pool_row.update({
            "buy_market_id": pool_route["buy_market_id"],
            "sell_market_id": pool_route["sell_market_id"],
            "route_mode": pool_route["route_mode"],
        })
        route_component = next(
            item for item in base["cost_components"]
            if item["opportunity_id"] == opportunity_id
            and item["leg"] == "route"
        )
        pool_components = []
        for leg, component_type in release_checker._route_expected_component_keys(
            pool_route
        ):
            component = copy.deepcopy(
                route_component if leg == "route" else fee
            )
            component.update({
                "leg": leg,
                "component_type": component_type,
                "market_id": (
                    "" if leg == "route" else pool_route[leg + "_market_id"]
                ),
            })
            pool_components.append(component)
        pool_row["cost_component_set_sha256"] = release_checker._route_canonical_sha256(
            sorted(
                pool_components,
                key=lambda item: (
                    item["opportunity_id"], item["leg"], item["component_type"]
                ),
            )
        )
        pool_row["evidence_binding_sha256"] = _opportunity_binding(pool_row)
        pool_legs = []
        for side, source_leg in zip(("buy", "sell"), base["legs"]):
            leg = copy.deepcopy(source_leg)
            leg["market_id"] = pool_route[side + "_market_id"]
            pool_legs.append(leg)
        with self.assertRaisesRegex(ReleaseCheckError, "reflected pool fee"):
            release_checker._validate_route_opportunity_row(
                pool_row,
                route=pool_route,
                legs_by_market={
                    item["market_id"]: item for item in pool_legs
                },
                components=pool_components,
                core_manifest_sha256=base["core_manifest_sha256"],
                now_epoch=release_checker._route_now_epoch(
                    self.validated_at
                )[0],
            )

    def test_attestation_transplant_wrong_core_and_secret_sentinel_fail(self):
        loaded = self._loaded()
        mutations = {}
        transplant = copy.deepcopy(loaded["bundle"])
        transplant["opportunities"][0]["publication_attestation_sha256"] = (
            transplant["opportunities"][1]["publication_attestation_sha256"]
        )
        transplant["opportunities"][0]["evidence_binding_sha256"] = (
            _opportunity_binding(transplant["opportunities"][0])
        )
        mutations["attestation"] = transplant
        wrong_core = copy.deepcopy(loaded["bundle"])
        wrong_core["opportunities"][0]["buy_core_manifest_sha256"] = "f" * 64
        wrong_core["opportunities"][0]["evidence_binding_sha256"] = (
            _opportunity_binding(wrong_core["opportunities"][0])
        )
        mutations["core"] = wrong_core
        secret = copy.deepcopy(loaded["bundle"])
        secret["input_generations"]["adapter_versions"]["api_key"] = (
            "SECRET_SENTINEL"
        )
        mutations["unsafe"] = secret

        for label, bundle in mutations.items():
            with self.subTest(label=label):
                shutil.rmtree(self.routes_root)
                shutil.copytree(self.base_routes, self.routes_root)
                self._rewrite(bundle)
                with self.assertRaisesRegex(ReleaseCheckError, "validation failed"):
                    self._validate()

    def test_newer_core_only_generation_does_not_replace_last_complete_bundle(self):
        from tests.test_route_publication import _second_cohort

        before = self._validate()
        route_publication.publish_route_cohort_bundle(
            _second_cohort(), core_root=self.routes_root / "core"
        )
        after = self._validate()

        self.assertEqual(after["route_cohort_id"], before["route_cohort_id"])
        self.assertEqual(after["manifest_sha256"], before["manifest_sha256"])


class DashboardReleaseSmokeTest(unittest.TestCase):
    def test_release_checker_uses_dashboard_route_root_override(self):
        with patch.dict(
            release_checker.os.environ,
            {
                "MARKET_DATA_DIR": "/runtime/market-data",
                "MARKET_ROUTE_DATA_DIR": "/runtime/custom-routes",
            },
        ):
            route_root = release_checker.configured_route_root()

        self.assertEqual(route_root, Path("/runtime/custom-routes"))

    def test_release_cli_parses_required_route_cohort_flag(self):
        for flag in (
            "--require-route-cohort",
            "--require-route-opportunities",
        ):
            with self.subTest(flag=flag), patch(
                "sys.argv", ["check_dashboard_release.py", flag]
            ):
                args = release_checker.parse_args()

            self.assertTrue(args.require_route_cohort)
            self.assertEqual(args.opportunity_raw_max, 2_000_000)
            self.assertEqual(args.opportunity_gzip_max, 300_000)

    def test_release_checks_required_complete_bundle_before_remote_requests(self):
        args = argparse.Namespace(
            base_url="https://dashboard.test",
            timeout=1.0,
            require_route_cohort=True,
        )
        with tempfile.TemporaryDirectory() as directory_name:
            with ExitStack() as stack:
                stack.enter_context(
                    patch.dict(
                        release_checker.os.environ,
                        {"MARKET_DATA_DIR": directory_name},
                    )
                )
                route_validator = stack.enter_context(
                    patch(
                        "scripts.check_dashboard_release.validate_route_opportunity_release",
                        side_effect=ReleaseCheckError("required route sentinel"),
                    )
                )
                fetch_json = stack.enter_context(
                    patch("scripts.check_dashboard_release.fetch_json")
                )

                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "required route sentinel",
                ):
                    release_check(args)

            route_validator.assert_called_once_with(
                Path(directory_name) / "routes",
                required=True,
            )
            fetch_json.assert_not_called()

    def test_public_asset_check_excludes_protected_admin_bundle(self):
        self.assertIn("actions.css", STATIC_ASSET_FILENAMES)
        self.assertIn("actions.js", STATIC_ASSET_FILENAMES)
        self.assertNotIn("admin.css", STATIC_ASSET_FILENAMES)
        self.assertNotIn("admin.js", STATIC_ASSET_FILENAMES)

    def test_checker_fetches_exact_public_bundle_and_reproduces_server_hash(self):
        class AssetResponse:
            status = 200
            headers = {}

            def __init__(self, body):
                self.body = body

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit=None):
                return self.body

        with tempfile.TemporaryDirectory() as directory_name:
            static_root = Path(directory_name)
            source_by_name = dict(PUBLIC_STATIC_ASSET_SOURCES)
            body_by_name = {}
            for served_name, source_path in PUBLIC_STATIC_ASSET_SOURCES:
                source = static_root / source_path
                source.parent.mkdir(parents=True, exist_ok=True)
                body = f"asset:{served_name}".encode("utf-8")
                source.write_bytes(body)
                body_by_name[served_name] = body

            requested = []

            def fake_urlopen(request, timeout):
                self.assertEqual(timeout, 1.0)
                served_name = request.full_url.split(
                    "https://dashboard.test/", 1
                )[1].split("?", 1)[0]
                requested.append(served_name)
                self.assertIn(served_name, source_by_name)
                return AssetResponse(body_by_name[served_name])

            with patch.object(server, "STATIC_ROOT", static_root):
                expected_sha = server._compute_static_asset_sha()
            with patch(
                "scripts.check_dashboard_release.urlopen",
                side_effect=fake_urlopen,
            ):
                actual_sha, metrics = fetch_static_asset_bundle(
                    "https://dashboard.test",
                    "a" * 12 + "-" + "b" * 12,
                    timeout=1.0,
                )

        self.assertEqual(actual_sha, expected_sha)
        self.assertEqual(requested, list(STATIC_ASSET_FILENAMES))
        self.assertEqual(len(metrics), len(STATIC_ASSET_FILENAMES))

    def freshness(self):
        checked_at = "2026-02-01T01:00:00+00:00"
        return {
            "checked_at": checked_at,
            "overall_status": "current",
            "common_comparable_end": "2026-01-31",
            "cex_daily": {
                "source": "cex_daily",
                "status": "current",
                "available_start": "2026-01-01",
                "available_end": "2026-01-31",
                "latest_completed_utc_day": "2026-01-31",
                "lag_days": 0,
                "max_lag_days": 1,
            },
            "dex_daily": {
                "source": "dex_daily",
                "status": "current",
                "available_start": "2026-01-01",
                "available_end": "2026-01-31",
                "latest_completed_utc_day": "2026-01-31",
                "lag_days": 0,
                "max_lag_days": 1,
            },
            **{
                source: {
                    "source": source,
                    "status": "current",
                    "observed_at": "2026-02-01T00:00:00+00:00",
                    "age_hours": 1.0,
                    "max_age_hours": maximum,
                }
                for source, maximum in (
                    ("dex_tvl", 26.0),
                    ("cex_depth", 2.0),
                    ("dex_depth", 2.0),
                    ("cex_execution", 2.0),
                    ("dex_execution", 2.0),
                )
            },
        }

    def summary(self):
        return {
            "metadata": {
                "response_scope": "screener_summary",
                "summary_version": 3,
                "data_generation": "generation-1",
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
                "default_workspace_token": "AAVE",
                "token_count": 1,
                "catalog_market_count": 2,
                "freshness": self.freshness(),
                "cex_instrument_lifecycle": {
                    "schema": "cex_instrument_lifecycle/v1",
                    "reviewed_market_count": 2,
                    "absence_market_count": 0,
                    "applied_market_count": 0,
                    "withheld_payload_market_count": 0,
                    "stale_evidence_market_count": 0,
                    "official_inventory_count": 1_000,
                    "response_sha256": "9" * 64,
                    "configured_market_ids_sha256": (
                        configured_market_ids_sha256(
                            {
                                "cex:crypto_com:AAVE/USDT",
                                "cex:crypto_com:UNI/USDT",
                            }
                        )
                    ),
                    "freshness_max_age_seconds": 129600,
                    "checked_at_min": "2026-02-01T00:30:00+00:00",
                    "checked_at_max": "2026-02-01T00:30:00+00:00",
                },
                "configured_cex_market_identities": {
                    "schema": "configured_cex_market_identities/v1",
                    "upbit": {
                        "market_count": 2,
                        "market_ids": [
                            "cex:upbit:AAVE/USDT",
                            "cex:upbit:UNI/USDT",
                        ],
                        "market_ids_sha256": (
                            "556bd70f57ba9cac453a87e26c2e5a1b"
                            "7098133cdfc1956cfad0e20dda693635"
                        ),
                    },
                },
            },
            "tokens": [{
                "token_symbol": "AAVE",
                "market_count": 2,
                "quality_status_counts": {"ok": 2},
                "quality_alert_counts": {"info": 1},
                "price_spread": 0.01,
                "price_spread_method": "directional_dex_over_cex_minus_one",
                "absolute_price_gap": 2 / 202,
                "absolute_price_gap_method": (
                    "symmetric_midpoint_relative_gap"
                ),
                "maximum_absolute_price_spread": 0.03,
                "mean_absolute_price_spread": 0.02,
                "median_absolute_price_spread": 0.015,
                "spread_comparable_days": 20,
                "primary_cex": {
                    "refresh_market_id": "cex:crypto_com:AAVE/USDT",
                    "token_symbol": "AAVE",
                    "venue": "crypto_com",
                    "instrument": "AAVE/USDT",
                    "depth_status": "observed",
                    "depth_na_reason": "observed",
                    "depth_retryable": False,
                    "tvl_status": "not_applicable",
                    "tvl_na_reason": "cex_markets_do_not_have_pool_tvl",
                    "tvl_retryable": False,
                },
                "primary_dex": {
                    "refresh_market_id": "dex:eth:uniswap_v3:pool:AAVE",
                    "token_symbol": "AAVE",
                    "venue": "eth / uniswap_v3",
                    "pool_address": "pool",
                    "depth_status": "collection_failed",
                    "depth_na_reason": "source_unavailable",
                    "depth_retryable": True,
                    "tvl_status": "collection_failed",
                    "tvl_na_reason": "source_unavailable",
                    "tvl_retryable": True,
                },
            }],
        }

    def metrics(self, path="/api/markets/summary", raw=1000, wire=500):
        return ResponseMetrics(path, 1.0, wire, raw, True)

    def screening_quality(self, token="AAVE"):
        def fact(status, reason_code, retryable=False, action=None):
            return {
                "status": status,
                "reason_code": reason_code,
                "retryable": retryable,
                "action": action,
                "quality_flags": [],
            }

        def daily_fact():
            return {
                **fact("observed", "observed"),
                "daily_evidence_mode": "published_daily_audit",
                "reason_code_counts": {},
                "issue_status_counts": {},
                "issue_outcome_counts": [],
                "affected_dates": [],
                "affected_date_count": 0,
            }

        market_ids = [
            f"cex:crypto_com:{token}/USDT",
            f"dex:eth:uniswap_v3:pool:{token}",
        ]
        zero_rollups = [
            {
                "market_id": market_id,
                "issue_count": 0,
                "reason_code_counts": {},
                "status_counts": {},
                "issue_outcome_counts": [],
                "affected_dates": [],
                "affected_date_count": 0,
                "evidence_mode": "published_daily_audit",
                "fact_outcome": {
                    "status": "observed",
                    "reason_code": "observed",
                    "retryable": False,
                    "action": None,
                },
            }
            for market_id in market_ids
        ]

        return {
            "metadata": {
                "contract_version": 4,
                "data_generation": "generation-1",
                "scope": "all",
                "daily_quality_report": {
                    "status": "matched",
                    "evidence_mode": "published_daily_audit",
                    "identity_status": "matched_current_import",
                    "schema": "fact_quality_report/v1",
                    "selected_window_issue_count": 0,
                    "reason_code_counts": {},
                    "status_counts": {},
                    "issue_outcome_counts": [],
                    "affected_dates": [],
                    "affected_date_count": 0,
                    "market_issue_rollups": zero_rollups,
                },
            },
            "token_symbol": token,
            "markets": [
                {
                    "market_id": f"cex:crypto_com:{token}/USDT",
                    "market_type": "cex",
                    "token_symbol": token,
                    "quality_status": "ok",
                    "quality_flags": [],
                    "facts": {
                        "daily": daily_fact(),
                        "tvl": fact(
                            "not_applicable",
                            "cex_markets_do_not_have_pool_tvl",
                        ),
                        "depth": fact("observed", "observed"),
                        "execution": fact("observed", "observed"),
                    },
                    "screening_quality_status": "ok",
                    "screening_quality_scope": "catalog",
                    "screening_quality_window": {
                        "start": "2026-01-01",
                        "end": "2026-01-31",
                        "method": "max_query_source_market_observed_start",
                    },
                    "screening_quality_flags": [
                        {
                            "code": "depth_unavailable",
                            "severity": "info",
                            "category": "availability",
                            "message": (
                                "No executable-depth observation is available."
                            ),
                            "observed_value": None,
                            "threshold": None,
                        }
                    ],
                },
                {
                    "market_id": f"dex:eth:uniswap_v3:pool:{token}",
                    "market_type": "dex",
                    "token_symbol": token,
                    "quality_status": "ok",
                    "quality_flags": [],
                    "facts": {
                        "daily": daily_fact(),
                        "tvl": fact("observed", "observed"),
                        "depth": fact(
                            "collection_failed",
                            "source_unavailable",
                            True,
                            "retry_depth_collection",
                        ),
                        "execution": fact(
                            "unsupported",
                            "unsupported_protocol_or_chain",
                        ),
                    },
                    "screening_quality_status": "ok",
                    "screening_quality_scope": "catalog",
                    "screening_quality_window": {
                        "start": "2026-01-01",
                        "end": "2026-01-31",
                        "method": "max_query_source_market_observed_start",
                    },
                    "screening_quality_flags": [],
                },
            ],
        }

    def test_release_exact_cex_identity_rejects_legacy_quote_aliases(self):
        def validate(market_id, token, configured_upbit_market_ids):
            try:
                return validate_exact_cex_market_identity(
                    market_id,
                    token,
                    configured_upbit_market_ids=(
                        configured_upbit_market_ids
                    ),
                )
            except TypeError as error:
                self.fail(
                    "Exact identity validation must accept authoritative "
                    "Upbit configuration: {}".format(error)
                )

        valid = (
            ("cex:coinbase:AAVE/USD", "AAVE"),
            ("cex:kraken:AAVE/USD", "AAVE"),
            ("cex:binance:AAVE/USDT", "AAVE"),
        )
        for market_id, token in valid:
            with self.subTest(valid=market_id):
                validate(
                    market_id,
                    token,
                    {
                        "cex:upbit:AAVE/USDT",
                    },
                )

        for configured_market_id in (
            "cex:upbit:AAVE/USDT",
            "cex:upbit:AAVE/KRW",
        ):
            with self.subTest(configured_upbit=configured_market_id):
                try:
                    validate(
                        configured_market_id,
                        "AAVE",
                        {configured_market_id},
                    )
                except ReleaseCheckError as error:
                    self.fail(
                        "An explicitly configured Upbit quote is an exact "
                        "market identity: {}".format(error)
                    )

        invalid = (
            "cex:coinbase:AAVE/USDT",
            "cex:kraken:AAVE/USDT",
            "cex:coinbase:UNI/USD",
        )
        for market_id in invalid:
            with self.subTest(invalid=market_id), self.assertRaisesRegex(
                ReleaseCheckError,
                "exact CEX identity",
            ):
                validate(
                    market_id,
                    "AAVE",
                    {
                        "cex:upbit:AAVE/USDT",
                    },
                )

        try:
            validate(
                "cex:upbit:AAVE/KRW",
                "AAVE",
                {"cex:upbit:AAVE/USDT"},
            )
        except ReleaseCheckError:
            pass
        else:
            self.fail(
                "Upbit KRW must be rejected when the authoritative market is USDT"
            )

    def test_screening_quality_must_match_summary_counts(self):
        summary_row = self.summary()["tokens"][0]
        quality = self.screening_quality()
        parity = validate_screening_quality_parity(
            summary_row,
            quality,
            expected_generation="generation-1",
        )
        self.assertEqual(parity["market_count"], 2)
        self.assertEqual(
            parity["market_ids"],
            [
                "cex:crypto_com:AAVE/USDT",
                "dex:eth:uniswap_v3:pool:AAVE",
            ],
        )
        self.assertEqual(parity["status_counts"], {"ok": 2})
        self.assertEqual(parity["alert_counts"], {"info": 1})

        fallback = copy.deepcopy(quality)
        fallback["markets"][0]["screening_quality_flags"] = []
        fallback["markets"][0]["screening_quality_status"] = "warning"
        with self.assertRaisesRegex(ReleaseCheckError, "fallback alert"):
            validate_screening_quality_parity(
                summary_row,
                fallback,
                expected_generation="generation-1",
            )

        status_mismatch = copy.deepcopy(quality)
        status_mismatch["markets"][1]["screening_quality_status"] = "critical"
        status_mismatch["markets"][1]["screening_quality_flags"] = [{
            "code": "depth_failed",
            "severity": "critical",
            "category": "data_health",
            "message": "The latest depth collection failed.",
            "observed_value": "collection_failed",
            "threshold": None,
        }]
        with self.assertRaisesRegex(ReleaseCheckError, "screening quality"):
            validate_screening_quality_parity(
                summary_row,
                status_mismatch,
                expected_generation="generation-1",
            )

        severity_status_drift = copy.deepcopy(quality)
        severity_status_drift["markets"][0]["screening_quality_status"] = "warning"
        severity_status_drift["markets"][0]["screening_quality_flags"][0][
            "severity"
        ] = "critical"
        severity_status_drift["markets"][0]["screening_quality_flags"][0][
            "category"
        ] = "data_health"
        drift_summary = copy.deepcopy(summary_row)
        drift_summary["quality_status_counts"] = {"ok": 1, "warning": 1}
        drift_summary["quality_alert_counts"] = {"critical": 1}
        with self.assertRaisesRegex(ReleaseCheckError, "status differs from its flags"):
            validate_screening_quality_parity(
                drift_summary,
                severity_status_drift,
                expected_generation="generation-1",
            )

        generation_mismatch = copy.deepcopy(quality)
        generation_mismatch["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation"):
            validate_screening_quality_parity(
                summary_row,
                generation_mismatch,
                expected_generation="generation-1",
            )

    def test_screening_quality_requires_contract_v4_before_generation_checks(self):
        quality = self.screening_quality()
        quality["metadata"]["contract_version"] = 3
        quality["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "contract v4"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                quality,
                expected_generation="generation-1",
            )

    def test_screening_quality_validates_every_market_fact_family(self):
        quality = self.screening_quality()
        quality["markets"][1]["facts"]["tvl"].update(
            {
                "status": "failed",
                "reason_code": "execution_calculation_failed",
                "retryable": True,
                "action": "retry_tvl_collection",
            }
        )

        with self.assertRaisesRegex(ReleaseCheckError, "canonical fact"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                quality,
                expected_generation="generation-1",
            )

        selected_status_drift = self.screening_quality()
        selected_status_drift["markets"][1]["quality_status"] = "warning"
        with self.assertRaisesRegex(ReleaseCheckError, "selected quality"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                selected_status_drift,
                expected_generation="generation-1",
            )

        daily_count_drift = self.screening_quality()
        daily_count_drift["markets"][1]["facts"]["daily"][
            "affected_date_count"
        ] = 1
        with self.assertRaisesRegex(ReleaseCheckError, "daily fact"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                daily_count_drift,
                expected_generation="generation-1",
            )

        fallback_audit = self.screening_quality()
        fallback_audit["metadata"]["daily_quality_report"].update(
            {
                "status": "unavailable",
                "evidence_mode": "catalog_window_inference",
                "identity_status": "unavailable",
            }
        )
        for field in ("schema", "market_issue_rollups", "issue_outcome_counts"):
            fallback_audit["metadata"]["daily_quality_report"].pop(field, None)
        with self.assertRaisesRegex(ReleaseCheckError, "matched"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                fallback_audit,
                expected_generation="generation-1",
            )

    def test_screening_quality_rejects_invalid_flag_contract(self):
        valid_flag = self.screening_quality()["markets"][0][
            "screening_quality_flags"
        ][0]
        cases = {
            "missing field": {key: value for key, value in valid_flag.items()
                              if key != "message"},
            "unknown field": {**valid_flag, "source_url": "redacted"},
            "bad severity": {**valid_flag, "severity": "error"},
            "non-string severity": {**valid_flag, "severity": []},
            "bad category": {**valid_flag, "category": "source_outcome"},
            "non-string category": {**valid_flag, "category": []},
            "bad code case": {**valid_flag, "code": "Depth_Unavailable"},
            "bad code punctuation": {**valid_flag, "code": "depth-unavailable"},
            "bad code unicode": {**valid_flag, "code": "dépth_unavailable"},
            "long code": {**valid_flag, "code": "a" * 65},
            "empty message": {**valid_flag, "message": ""},
            "long message": {**valid_flag, "message": "x" * 241},
            "url message": {**valid_flag, "message": "See https://example.test"},
            "path message": {**valid_flag, "message": "Read /private/data/a.json"},
            "generic path message": {**valid_flag, "message": "Read /srv/app/a.json"},
            "equals srv path": {
                **valid_flag,
                "message": "error=/srv/app/secret",
            },
            "colon var path": {
                **valid_flag,
                "message": "path:/var/lib/dashboard/data.json",
            },
            "colon tmp path": {
                **valid_flag,
                "message": "path:/tmp/secret",
            },
            "colon etc path": {
                **valid_flag,
                "message": "path:/etc/passwd",
            },
            "colon opt path": {
                **valid_flag,
                "message": "path:/opt/app/secret",
            },
            "equals home path": {
                **valid_flag,
                "message": "error=/home/ugs/secret",
            },
            "uppercase home path": {
                **valid_flag,
                "message": "ERROR=/HOME/UGS/SECRET",
            },
            "colon private path": {
                **valid_flag,
                "message": "path:/private/tmp/x",
            },
            "bracket users path": {
                **valid_flag,
                "message": "Read [/Users/name/key]",
            },
            "unc path": {
                **valid_flag,
                "message": r"Read \\server\share\secret",
            },
            "backslash path": {
                **valid_flag,
                "message": r"Read home\ugs\secret",
            },
            "control message": {**valid_flag, "message": "line one\nline two"},
            "unicode control message": {
                **valid_flag,
                "message": "hidden\u200bmarker",
            },
            "non-dict flag": "depth_unavailable",
        }
        for label, flag in cases.items():
            with self.subTest(label=label):
                quality = self.screening_quality()
                quality["markets"][0]["screening_quality_flags"] = [flag]
                with self.assertRaises(ReleaseCheckError):
                    validate_screening_quality_parity(
                        self.summary()["tokens"][0],
                        quality,
                        expected_generation="generation-1",
                    )

        safe_slash = self.screening_quality()
        safe_slash["markets"][0]["screening_quality_flags"][0]["message"] = (
            "CEX/DEX facts remain visible; measured values are not N/A."
        )
        validate_screening_quality_parity(
            self.summary()["tokens"][0],
            safe_slash,
            expected_generation="generation-1",
        )
        for safe_message in (
            "CEX/DEX and TVL/depth remain visible when the value is N/A.",
            "Punctuation such as :/ or [/] is not itself a source path.",
            "The A/B comparison uses 1/2 only as ordinary prose.",
        ):
            with self.subTest(safe_message=safe_message):
                safe = self.screening_quality()
                safe["markets"][0]["screening_quality_flags"][0][
                    "message"
                ] = safe_message
                validate_screening_quality_parity(
                    self.summary()["tokens"][0],
                    safe,
                    expected_generation="generation-1",
                )

    def test_screening_quality_rejects_bad_market_shapes_and_fallbacks(self):
        mutations = []

        selected_scope = self.screening_quality()
        selected_scope["metadata"]["scope"] = "selected"
        mutations.append(("all scope", selected_scope))

        missing_scope = self.screening_quality()
        missing_scope["metadata"].pop("scope")
        mutations.append(("all scope", missing_scope))

        missing_status = self.screening_quality()
        missing_status["markets"][0].pop("screening_quality_status")
        mutations.append(("screening quality fields", missing_status))

        unknown_screening = self.screening_quality()
        unknown_screening["markets"][0]["screening_quality_reasons"] = []
        mutations.append(("unknown screening", unknown_screening))

        bad_status = self.screening_quality()
        bad_status["markets"][0]["screening_quality_status"] = "unknown"
        mutations.append(("status", bad_status))

        non_string_status = self.screening_quality()
        non_string_status["markets"][0]["screening_quality_status"] = []
        mutations.append(("status", non_string_status))

        non_list_flags = self.screening_quality()
        non_list_flags["markets"][0]["screening_quality_flags"] = {}
        mutations.append(("flags", non_list_flags))

        duplicate_ids = self.screening_quality()
        duplicate_ids["markets"][1]["market_id"] = duplicate_ids["markets"][0][
            "market_id"
        ]
        mutations.append(("duplicated|unique", duplicate_ids))

        empty_id = self.screening_quality()
        empty_id["markets"][0]["market_id"] = ""
        mutations.append(("market ID", empty_id))

        whitespace_id = self.screening_quality()
        whitespace_id["markets"][0]["market_id"] = " "
        mutations.append(("market ID", whitespace_id))

        wrong_count = self.screening_quality()
        wrong_count["markets"].pop()
        mutations.append(("market count", wrong_count))

        wrong_token = self.screening_quality()
        wrong_token["token_symbol"] = "UNI"
        mutations.append(("Token", wrong_token))

        missing_market_token = self.screening_quality()
        missing_market_token["markets"][0].pop("token_symbol")
        mutations.append(("market Token", missing_market_token))

        wrong_market_token = self.screening_quality()
        wrong_market_token["markets"][0]["token_symbol"] = "UNI"
        mutations.append(("market Token", wrong_market_token))

        for message, quality in mutations:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ReleaseCheckError, message):
                    validate_screening_quality_parity(
                        self.summary()["tokens"][0],
                        quality,
                        expected_generation="generation-1",
                    )

        info_status_without_flag = self.screening_quality()
        info_status_without_flag["markets"][1]["screening_quality_status"] = "info"
        with self.assertRaisesRegex(ReleaseCheckError, "fallback alert"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                info_status_without_flag,
                expected_generation="generation-1",
            )

        generation_first = copy.deepcopy(missing_status)
        generation_first["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                generation_first,
                expected_generation="generation-1",
            )

        generation_before_scope = copy.deepcopy(selected_scope)
        generation_before_scope["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                generation_before_scope,
                expected_generation="generation-1",
            )

    def test_screening_quality_zero_counts_normalize_and_bool_counts_fail(self):
        summary_row = copy.deepcopy(self.summary()["tokens"][0])
        summary_row["quality_status_counts"] = {
            "ok": 2,
            "info": 0,
            "warning": 0,
            "critical": 0,
        }
        summary_row["quality_alert_counts"] = {
            "info": 1,
            "warning": 0,
            "critical": 0,
        }
        validate_screening_quality_parity(
            summary_row,
            self.screening_quality(),
            expected_generation="generation-1",
        )

        for field in ("quality_status_counts", "quality_alert_counts"):
            with self.subTest(field=field):
                invalid = copy.deepcopy(summary_row)
                first_key = next(iter(invalid[field]))
                invalid[field][first_key] = True
                with self.assertRaisesRegex(ReleaseCheckError, "counts"):
                    validate_screening_quality_parity(
                        invalid,
                        self.screening_quality(),
                        expected_generation="generation-1",
                    )

        unknown_status = copy.deepcopy(summary_row)
        unknown_status["quality_status_counts"]["degraded"] = 0
        with self.assertRaisesRegex(ReleaseCheckError, "status counts"):
            validate_screening_quality_parity(
                unknown_status,
                self.screening_quality(),
                expected_generation="generation-1",
            )
        unknown_severity = copy.deepcopy(summary_row)
        unknown_severity["quality_alert_counts"]["error"] = 0
        with self.assertRaisesRegex(ReleaseCheckError, "alert counts"):
            validate_screening_quality_parity(
                unknown_severity,
                self.screening_quality(),
                expected_generation="generation-1",
            )

    def test_screening_quality_status_and_alert_mismatches_are_independent(self):
        quality = self.screening_quality()

        status_only = copy.deepcopy(self.summary()["tokens"][0])
        status_only["quality_status_counts"] = {"ok": 1, "warning": 1}
        with self.assertRaisesRegex(ReleaseCheckError, "status counts"):
            validate_screening_quality_parity(
                status_only,
                quality,
                expected_generation="generation-1",
            )

        alerts_only = copy.deepcopy(self.summary()["tokens"][0])
        alerts_only["quality_alert_counts"] = {"warning": 1}
        with self.assertRaisesRegex(ReleaseCheckError, "alert counts"):
            validate_screening_quality_parity(
                alerts_only,
                quality,
                expected_generation="generation-1",
            )

    def test_screening_quality_counts_every_flag_without_filtering_or_deduping(self):
        quality = self.screening_quality()
        info_flag = quality["markets"][0]["screening_quality_flags"][0]
        quality["markets"][0]["screening_quality_flags"] = [
            info_flag,
            copy.deepcopy(info_flag),
            {
                "code": "wide_quoted_spread",
                "severity": "warning",
                "category": "market_condition",
                "message": "Quoted CEX spread exceeds the quality threshold.",
                "observed_value": 125.0,
                "threshold": 100.0,
            },
        ]
        summary_row = copy.deepcopy(self.summary()["tokens"][0])
        summary_row["quality_alert_counts"] = {"info": 2, "warning": 1}
        parity = validate_screening_quality_parity(
            summary_row,
            quality,
            expected_generation="generation-1",
        )
        self.assertEqual(parity["alert_counts"], {"info": 2, "warning": 1})

    def test_release_fetches_all_token_quality_once_and_retains_metrics(self):
        summary = self.summary()
        second_row = copy.deepcopy(summary["tokens"][0])
        second_row["token_symbol"] = "UNI"
        second_row["primary_cex"].update(
            {
                "token_symbol": "UNI",
                "instrument": "UNI/USDT",
                "refresh_market_id": "cex:crypto_com:UNI/USDT",
            }
        )
        second_row["primary_dex"].update(
            {
                "token_symbol": "UNI",
                "refresh_market_id": "dex:eth:uniswap_v3:pool:UNI",
            }
        )
        summary["tokens"].append(second_row)
        summary["metadata"]["token_count"] = 2
        summary["metadata"]["catalog_market_count"] = 4
        quality_by_token = {
            token: self.screening_quality(token)
            for token in ("AAVE", "UNI")
        }
        def rename_quality_market(payload, index, new_market_id):
            old_market_id = payload["markets"][index]["market_id"]
            payload["markets"][index]["market_id"] = new_market_id
            for rollup in payload["metadata"]["daily_quality_report"][
                "market_issue_rollups"
            ]:
                if rollup["market_id"] == old_market_id:
                    rollup["market_id"] = new_market_id
        valid_quality_by_token = copy.deepcopy(quality_by_token)
        full_catalog = {
            "metadata": {
                "data_generation": "generation-1",
                "configured_cex_market_identities": copy.deepcopy(
                    summary["metadata"]["configured_cex_market_identities"]
                ),
            },
            "markets": [
                {
                    "market_id": "cex:crypto_com:AAVE/USDT",
                    "token_symbol": "AAVE",
                },
                {
                    "market_id": "dex:eth:uniswap_v3:pool:AAVE",
                    "token_symbol": "AAVE",
                },
                {
                    "market_id": "cex:crypto_com:UNI/USDT",
                    "token_symbol": "UNI",
                },
                {
                    "market_id": "dex:eth:uniswap_v3:pool:UNI",
                    "token_symbol": "UNI",
                },
            ],
        }
        valid_full_markets = copy.deepcopy(full_catalog["markets"])
        event = {
            "token_symbol": "AAVE",
            "time": {
                "effective_date_start": "2026-01-10",
                "effective_date_end": "2026-01-10",
            },
            "lifecycle": "occurred",
            "clock": {"state": "past"},
        }
        all_events = {
            "coverage": {
                "covered_tokens": ["AAVE", "UNI"],
                "uncovered_tokens": [],
                "covered_token_count": 2,
            },
            "bundle_id": "a" * 24,
        }
        health_payload = {
            "status": "ok",
            "data_ready": True,
            "data_status": "current",
            "freshness": self.freshness(),
            "cex_instrument_lifecycle": copy.deepcopy(
                summary["metadata"]["cex_instrument_lifecycle"]
            ),
            "application_sha": "a" * 40,
            "asset_sha": "b" * 64,
            "asset_version": f"{'a' * 12}-{'b' * 12}",
        }
        fetched_paths = []
        served_asset_state = {"sha": "b" * 64}
        summary_state = {
            "count": 0,
            "tail_generation": None,
            "tail_freshness_stale": False,
        }

        def fake_fetch(_base_url, path, *, timeout):
            fetched_paths.append(path)
            if path.startswith("/api/markets/opportunities?"):
                payload = server.attach_public_action_capabilities(
                    build_unavailable_opportunity_payload(
                        opportunity_class=(
                            "strict" if "class=strict" in path
                            else "estimate" if "class=estimate" in path
                            else "all"
                        ),
                        availability=(
                            "unavailable"
                            if "availability=unavailable" in path
                            else "all"
                        ),
                        sort="route_id",
                        direction="asc",
                    )
                )
            elif path == "/health":
                payload = copy.deepcopy(health_payload)
            elif path == "/api/markets/summary":
                summary_state["count"] += 1
                payload = copy.deepcopy(summary)
                if (
                    summary_state["count"] > 1
                    and summary_state["tail_generation"] is not None
                ):
                    payload["metadata"]["data_generation"] = summary_state[
                        "tail_generation"
                    ]
                if (
                    summary_state["count"] > 1
                    and summary_state["tail_freshness_stale"]
                ):
                    payload["metadata"]["freshness"]["overall_status"] = "stale"
                    payload["metadata"]["freshness"]["cex_depth"]["status"] = "stale"
                    payload["metadata"]["freshness"]["cex_depth"]["age_hours"] = 3.0
            elif path == "/api/markets/catalog":
                payload = full_catalog
            elif path.startswith("/api/markets/catalog?"):
                payload = {
                    "metadata": {
                        "configured_cex_market_identities": copy.deepcopy(
                            summary["metadata"][
                                "configured_cex_market_identities"
                            ]
                        ),
                    },
                    "token_summary": {},
                }
            elif path == "/api/markets/events":
                payload = all_events
            elif "scope=all" in path and path.startswith("/api/markets/quality?"):
                token = "AAVE" if "token=AAVE" in path else "UNI"
                payload = quality_by_token[token]
            else:
                payload = {}
            return payload, self.metrics(path)

        args = argparse.Namespace(
            base_url="https://dashboard.test",
            timeout=1.0,
            summary_raw_max=2_000,
            summary_gzip_max=1_000,
            token_raw_max=2_000,
            token_gzip_max=1_000,
            expected_application_sha="a" * 40,
            expected_asset_sha="b" * 64,
        )
        markets = [
            {"market_id": "cex:crypto_com:AAVE/USDT", "market_type": "cex"},
            {
                "market_id": "dex:eth:uniswap_v3:pool:AAVE",
                "market_type": "dex",
            },
        ]
        validator_calls = {}
        def run_release(*, bypass_summary_validation=False):
            summary_state["count"] = 0
            with ExitStack() as stack:
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.fetch_json",
                    side_effect=fake_fetch,
                ))
                if bypass_summary_validation:
                    stack.enter_context(patch(
                        "scripts.check_dashboard_release.validate_summary",
                        return_value=(
                            "AAVE",
                            "2026-01-01",
                            "2026-01-31",
                            "generation-1",
                        ),
                    ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_token_catalog",
                    return_value=markets,
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.fetch_static_asset_bundle",
                    side_effect=lambda *_args, **_kwargs: (
                        served_asset_state["sha"],
                        [],
                    ),
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_events",
                    return_value=[event],
                ))
                comparison_validator = stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_comparison"
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_quality"
                ))
                execution_validator = stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_execution"
                ))
                result = release_check(args)
                validator_calls["comparison"] = comparison_validator.call_args
                validator_calls["execution"] = execution_validator.call_args
                return result

        result = run_release()

        self.assertEqual(result["application_sha"], "a" * 40)
        self.assertEqual(result["asset_sha"], "b" * 64)
        self.assertEqual(
            result["route_opportunities"],
            {
                "status": "unavailable",
                "reason": "complete_pointer_absent",
            },
        )
        self.assertEqual(
            validator_calls["comparison"].kwargs[
                "expected_comparison_generation"
            ],
            "generation-1",
        )
        self.assertEqual(
            validator_calls["execution"].kwargs[
                "expected_execution_generation"
            ],
            "generation-1",
        )

        baseline_summary = copy.deepcopy(summary)
        baseline_quality_by_token = copy.deepcopy(quality_by_token)
        baseline_full_catalog = copy.deepcopy(full_catalog)
        baseline_token_markets = copy.deepcopy(markets)
        baseline_fetched_paths = list(fetched_paths)
        try:
            configured_krw = {
                "schema": "configured_cex_market_identities/v1",
                "upbit": {
                    "market_count": 2,
                    "market_ids": [
                        "cex:upbit:AAVE/KRW",
                        "cex:upbit:UNI/USDT",
                    ],
                    "market_ids_sha256": (
                        "440b52cffc9da70c7adaf402da4131c48"
                        "1e4356cb85ef9994da59d0a2f1f9154"
                    ),
                },
            }
            summary["metadata"]["configured_cex_market_identities"] = (
                copy.deepcopy(configured_krw)
            )
            summary["metadata"]["catalog_market_count"] = 5
            summary["tokens"][0]["market_count"] = 3
            summary["tokens"][0]["quality_status_counts"] = {"ok": 3}
            summary["tokens"][0]["quality_alert_counts"] = {"info": 2}

            upbit_quality = copy.deepcopy(
                quality_by_token["AAVE"]["markets"][0]
            )
            upbit_quality["market_id"] = "cex:upbit:AAVE/KRW"
            quality_by_token["AAVE"]["markets"].append(upbit_quality)
            upbit_rollup = copy.deepcopy(
                quality_by_token["AAVE"]["metadata"][
                    "daily_quality_report"
                ]["market_issue_rollups"][0]
            )
            upbit_rollup["market_id"] = "cex:upbit:AAVE/KRW"
            quality_by_token["AAVE"]["metadata"][
                "daily_quality_report"
            ]["market_issue_rollups"].append(upbit_rollup)

            full_catalog["metadata"][
                "configured_cex_market_identities"
            ] = copy.deepcopy(configured_krw)
            full_catalog["markets"].append({
                "market_id": "cex:upbit:AAVE/KRW",
                "token_symbol": "AAVE",
            })
            markets.append({
                "market_id": "cex:upbit:AAVE/KRW",
                "market_type": "cex",
            })

            try:
                run_release()
            except ReleaseCheckError as error:
                self.fail(
                    "Full release rejected configured Upbit KRW: {}".format(
                        error
                    )
                )

            configured_usdt = {
                "schema": "configured_cex_market_identities/v1",
                "upbit": {
                    "market_count": 2,
                    "market_ids": [
                        "cex:upbit:AAVE/USDT",
                        "cex:upbit:UNI/USDT",
                    ],
                    "market_ids_sha256": (
                        "556bd70f57ba9cac453a87e26c2e5a1b"
                        "7098133cdfc1956cfad0e20dda693635"
                    ),
                },
            }
            summary["metadata"]["configured_cex_market_identities"] = (
                copy.deepcopy(configured_usdt)
            )
            full_catalog["metadata"][
                "configured_cex_market_identities"
            ] = copy.deepcopy(configured_usdt)
            with self.assertRaisesRegex(
                ReleaseCheckError,
                "configured Upbit",
            ):
                run_release()
        finally:
            summary = baseline_summary
            quality_by_token = baseline_quality_by_token
            full_catalog = baseline_full_catalog
            markets = baseline_token_markets
            fetched_paths[:] = baseline_fetched_paths

        health_payload["data_status"] = "stale"
        health_payload["freshness"]["overall_status"] = "stale"
        health_payload["freshness"]["cex_depth"]["status"] = "stale"
        health_payload["freshness"]["cex_depth"]["age_hours"] = 3.0
        with self.assertRaisesRegex(ReleaseCheckError, "freshness"):
            run_release()
        health_payload["data_status"] = "current"
        health_payload["freshness"] = self.freshness()

        stale_lifecycle = copy.deepcopy(summary)
        stale_lifecycle["metadata"]["cex_instrument_lifecycle"][
            "stale_evidence_market_count"
        ] = 1
        original_summary = summary
        summary = stale_lifecycle
        try:
            with self.assertRaisesRegex(ReleaseCheckError, "lifecycle"):
                run_release()
        finally:
            summary = original_summary

        health_payload["application_sha"] = "c" * 40
        with self.assertRaisesRegex(ReleaseCheckError, "application SHA"):
            run_release()
        health_payload["application_sha"] = "a" * 40

        health_payload["asset_version"] = f"{'a' * 12}-{'c' * 12}"
        with self.assertRaisesRegex(ReleaseCheckError, "asset version"):
            run_release()
        health_payload["asset_version"] = f"{'a' * 12}-{'b' * 12}"

        served_asset_state["sha"] = "c" * 64
        with self.assertRaisesRegex(ReleaseCheckError, "served assets"):
            run_release()
        served_asset_state["sha"] = "b" * 64

        all_quality_paths = [
            path
            for path in fetched_paths
            if path.startswith("/api/markets/quality?") and "scope=all" in path
        ]
        self.assertEqual(len(all_quality_paths), 2)
        self.assertEqual(
            {path.split("token=")[1].split("&")[0] for path in all_quality_paths},
            {"AAVE", "UNI"},
        )
        self.assertEqual(result["screening_quality_parity_count"], 2)
        self.assertEqual(result["screening_quality_market_count"], 4)
        metric_paths = [row["path"] for row in result["requests"]]
        self.assertTrue(set(all_quality_paths).issubset(metric_paths))
        self.assertEqual(
            metric_paths.count("/api/markets/summary"),
            2,
        )

        lifecycle = summary["metadata"]["cex_instrument_lifecycle"]
        valid_lifecycle_hash = lifecycle["configured_market_ids_sha256"]
        lifecycle["configured_market_ids_sha256"] = "0" * 64
        with self.assertRaisesRegex(ReleaseCheckError, "lifecycle catalog"):
            run_release()
        lifecycle["configured_market_ids_sha256"] = valid_lifecycle_hash

        lifecycle["reviewed_market_count"] = 1
        with self.assertRaisesRegex(ReleaseCheckError, "lifecycle catalog"):
            run_release()
        lifecycle["reviewed_market_count"] = 2

        summary_state["tail_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation changed"):
            run_release()
        summary_state["tail_generation"] = None

        summary_state["tail_freshness_stale"] = True
        with self.assertRaisesRegex(ReleaseCheckError, "freshness"):
            run_release()
        summary_state["tail_freshness_stale"] = False

        quality_by_token["UNI"]["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        full_catalog["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "full catalog generation"):
            run_release()
        full_catalog["metadata"]["data_generation"] = "generation-1"

        missing_catalog_generation = full_catalog.pop("metadata")
        with self.assertRaisesRegex(ReleaseCheckError, "full catalog generation"):
            run_release()
        full_catalog["metadata"] = missing_catalog_generation

        rename_quality_market(
            quality_by_token["UNI"],
            0,
            quality_by_token["AAVE"]["markets"][0]["market_id"],
        )
        with self.assertRaisesRegex(ReleaseCheckError, "reused across Tokens"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        rename_quality_market(
            quality_by_token["AAVE"],
            0,
            "cex:bogus:AAVE/USDT",
        )
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        aave_market_id = quality_by_token["AAVE"]["markets"][0]["market_id"]
        uni_market_id = quality_by_token["UNI"]["markets"][0]["market_id"]
        rename_quality_market(quality_by_token["AAVE"], 0, uni_market_id)
        rename_quality_market(quality_by_token["UNI"], 0, aave_market_id)
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        substituted_full_catalog = copy.deepcopy(valid_full_markets)
        substituted_full_catalog[0]["market_id"] = "cex:bogus:AAVE/USDT"
        full_catalog["markets"] = substituted_full_catalog
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        full_catalog["markets"] = copy.deepcopy(valid_full_markets)

        summary["tokens"][0]["primary_cex"].update(
            {
                "venue": "bogus",
                "refresh_market_id": "cex:bogus:AAVE/USDT",
            }
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "Summary primary market.*full catalog",
        ):
            run_release()
        summary["tokens"][0]["primary_cex"].update(
            {
                "venue": "crypto_com",
                "refresh_market_id": "cex:crypto_com:AAVE/USDT",
            }
        )

        missing_market_token = copy.deepcopy(valid_full_markets)
        missing_market_token[0].pop("token_symbol")
        full_catalog["markets"] = missing_market_token
        with self.assertRaisesRegex(ReleaseCheckError, "market Token identity"):
            run_release()

        missing_token_catalog = copy.deepcopy(valid_full_markets)
        for market in missing_token_catalog:
            market["token_symbol"] = "AAVE"
            if market["market_id"] == "cex:crypto_com:UNI/USDT":
                market["market_id"] = "dex:eth:replacement:pool:AAVE"
        full_catalog["markets"] = missing_token_catalog
        with self.assertRaisesRegex(ReleaseCheckError, "Token inventory"):
            run_release()

        full_catalog["markets"] = copy.deepcopy(valid_full_markets[:3])
        with self.assertRaisesRegex(ReleaseCheckError, "catalog count"):
            run_release()

        full_catalog["markets"] = copy.deepcopy(valid_full_markets)
        summary["metadata"]["token_count"] = 1
        with self.assertRaisesRegex(ReleaseCheckError, "parity Token count"):
            run_release(bypass_summary_validation=True)
        summary["metadata"]["token_count"] = 2

        summary["metadata"]["catalog_market_count"] = 3
        with self.assertRaisesRegex(ReleaseCheckError, "parity market count"):
            run_release(bypass_summary_validation=True)

        summary["metadata"]["catalog_market_count"] = 4
        markets.pop()
        with self.assertRaisesRegex(ReleaseCheckError, "Token catalog inventory"):
            run_release()

    def test_summary_rejects_heavy_arrays_and_payload_budget_regression(self):
        summary = self.summary()
        token, start, end, generation = validate_summary(
            summary,
            self.metrics(),
            raw_max=2000,
            gzip_max=1000,
        )
        self.assertEqual((token, start, end, generation), (
            "AAVE",
            "2026-01-01",
            "2026-01-31",
            "generation-1",
        ))

        with self.assertRaisesRegex(ReleaseCheckError, "heavy root field"):
            validate_summary(
                {**summary, "markets": []},
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "exceeds"):
            validate_summary(
                summary,
                self.metrics(raw=2001),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "version is not 3"):
            validate_summary(
                {
                    **summary,
                    "metadata": {**summary["metadata"], "summary_version": 1},
                },
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "retryability"):
            broken = self.summary()
            broken["tokens"][0]["primary_cex"].pop("depth_retryable")
            validate_summary(
                broken,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "refresh identity"):
            wrong_refresh = self.summary()
            wrong_refresh["tokens"][0]["primary_cex"][
                "refresh_market_id"
            ] = "cex:bogus:AAVE/USDT"
            validate_summary(
                wrong_refresh,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "N/A outcome"):
            missing_reason = self.summary()
            missing_reason["tokens"][0]["primary_cex"].pop(
                "depth_na_reason"
            )
            validate_summary(
                missing_reason,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "N/A outcome"):
            mismatched_outcome = self.summary()
            mismatched_outcome["tokens"][0]["primary_dex"][
                "depth_na_reason"
            ] = "unsupported_protocol"
            validate_summary(
                mismatched_outcome,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "N/A outcome"):
            impossible_cex_tvl = self.summary()
            impossible_cex_tvl["tokens"][0]["primary_cex"].update(
                {
                    "tvl_status": "observed",
                    "tvl_na_reason": "observed",
                    "tvl_retryable": False,
                }
            )
            validate_summary(
                impossible_cex_tvl,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "N/A outcome"):
            fail_closed_cex_tvl = self.summary()
            fail_closed_cex_tvl["tokens"][0]["primary_cex"].update(
                {
                    "tvl_status": "needs_review",
                    "tvl_na_reason": "daily_quality_outcome_invalid",
                    "tvl_retryable": False,
                }
            )
            validate_summary(
                fail_closed_cex_tvl,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "spread contract"):
            directional_only = self.summary()
            directional_only["tokens"][0].pop("absolute_price_gap")
            validate_summary(
                directional_only,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "spread contract"):
            wrong_method = self.summary()
            wrong_method["tokens"][0]["absolute_price_gap_method"] = (
                "absolute_directional_gap"
            )
            validate_summary(
                wrong_method,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "not unique"):
            duplicated = self.summary()
            duplicated["tokens"].append(copy.deepcopy(duplicated["tokens"][0]))
            duplicated["metadata"]["token_count"] = 2
            duplicated["metadata"]["catalog_market_count"] = 4
            validate_summary(
                duplicated,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )

        count_mutations = (
            ("token_count", None),
            ("token_count", True),
            ("token_count", 2),
            ("catalog_market_count", None),
            ("catalog_market_count", True),
            ("catalog_market_count", 1),
        )
        for field, value in count_mutations:
            with self.subTest(field=field, value=value):
                invalid = self.summary()
                if value is None:
                    invalid["metadata"].pop(field)
                else:
                    invalid["metadata"][field] = value
                with self.assertRaisesRegex(ReleaseCheckError, field):
                    validate_summary(
                        invalid,
                        self.metrics(),
                        raw_max=2000,
                        gzip_max=1000,
                    )

        for field in (
            "absence_market_count",
            "withheld_payload_market_count",
            "official_inventory_count",
            "response_sha256",
            "configured_market_ids_sha256",
        ):
            with self.subTest(lifecycle_root_field=field):
                invalid = self.summary()
                invalid["metadata"]["cex_instrument_lifecycle"].pop(field)
                with self.assertRaisesRegex(ReleaseCheckError, "lifecycle"):
                    validate_summary(
                        invalid,
                        self.metrics(),
                        raw_max=2000,
                        gzip_max=1000,
                    )

    def test_token_catalog_rejects_cross_token_or_generation_mismatch(self):
        catalog = {
            "token_symbol": "AAVE",
            "metadata": {
                "window_start": "2026-01-01",
                "window_end": "2026-01-31",
                "data_generation": "generation-1",
                "configured_cex_market_identities": {
                    "schema": "configured_cex_market_identities/v1",
                    "upbit": {
                        "market_count": 1,
                        "market_ids": ["cex:upbit:AAVE/KRW"],
                        "market_ids_sha256": (
                            "f6a0641ba18fc9fe86dc38d1535009418"
                            "92ab681d9b78ce29cdf9cb1b316a8e5"
                        ),
                    },
                },
            },
            "markets": [{"token_symbol": "AAVE", "market_id": "a"}],
        }
        markets = validate_token_catalog(
            catalog,
            self.metrics("/api/markets/catalog"),
            token="AAVE",
            start="2026-01-01",
            end="2026-01-31",
            generation="generation-1",
            raw_max=2000,
            gzip_max=1000,
        )
        self.assertEqual(len(markets), 1)

        configured_krw = copy.deepcopy(catalog)
        configured_krw["markets"] = [{
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/KRW",
        }]
        try:
            validate_token_catalog(
                configured_krw,
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )
        except ReleaseCheckError as error:
            self.fail(
                "Token catalog rejected configured Upbit KRW: {}".format(
                    error
                )
            )

        mismatched_upbit = copy.deepcopy(configured_krw)
        mismatched_upbit["metadata"]["configured_cex_market_identities"][
            "upbit"
        ] = {
            "market_count": 1,
            "market_ids": ["cex:upbit:AAVE/USDT"],
            "market_ids_sha256": (
                "4a493498d2a13699db76b760609e91071"
                "5cd3df58dc1ad988984f0e3b61a9960"
            ),
        }
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "configured Upbit",
        ):
            validate_token_catalog(
                mismatched_upbit,
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )

        tampered_authority = copy.deepcopy(configured_krw)
        tampered_authority["metadata"][
            "configured_cex_market_identities"
        ]["upbit"]["market_ids_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "count or hash",
        ):
            validate_token_catalog(
                tampered_authority,
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )

        with self.assertRaisesRegex(ReleaseCheckError, "leaked another Token"):
            validate_token_catalog(
                {**catalog, "markets": [{"token_symbol": "UNI"}]},
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "generations differ"):
            validate_token_catalog(
                catalog,
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-2",
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "exact CEX identity"):
            validate_token_catalog(
                {
                    **catalog,
                    "markets": [{
                        "token_symbol": "AAVE",
                        "market_id": "cex:coinbase:AAVE/USDT",
                    }],
                },
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )

    def test_cex_lifecycle_fallback_requires_exact_flag_and_rejects_dex(self):
        for status, reason_code, action, flag_code in (
            (
                "source_no_observation",
                "instrument_absent_from_current_catalog",
                "operator_review_source_outcome",
                "inactive_cex_instrument",
            ),
            (
                "needs_review",
                "official_catalog_evidence_stale",
                "operator_manual_review",
                "stale_cex_lifecycle_evidence",
            ),
        ):
            fact = {
                "status": status,
                "reason_code": reason_code,
                "retryable": False,
                "action": action,
                "quality_flags": [{"code": flag_code}],
            }
            with self.subTest(cex_lifecycle_fallback=reason_code):
                evidence = _validate_daily_fact_evidence(
                    fact,
                    market_type="cex",
                    report_status="unavailable",
                )
                self.assertEqual(evidence["mode"], None)
                self.assertEqual(evidence["issue_count"], 0)

                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "lacks required published evidence/action",
                ):
                    _validate_daily_fact_evidence(
                        {**fact, "quality_flags": []},
                        market_type="cex",
                        report_status="unavailable",
                    )

                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "lacks required published evidence/action",
                ):
                    _validate_daily_fact_evidence(
                        fact,
                        market_type="dex",
                        report_status="unavailable",
                    )

    def test_expert_endpoint_validators_reject_empty_or_unmeasured_results(self):
        market_a = "cex:binance:AAVE/USDT"
        market_b = "dex:eth:uniswap_v3:pool:AAVE"
        comparison = {
            "token_symbol": "AAVE",
            "market_a": {"market_id": market_a},
            "market_b": {"market_id": market_b},
            "metadata": {
                "data_generation": "generation-1",
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
                "comparison_days": 1,
            },
            "observations": [{"date": "2026-01-15"}],
            "latest_comparable_observation": {"date": "2026-01-15"},
        }
        validate_comparison(
            comparison,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            start="2026-01-01",
            end="2026-01-31",
            expected_generation="generation-1",
        )
        comparison_with_generation = copy.deepcopy(comparison)
        comparison_with_generation["metadata"]["comparison_generation"] = (
            "comparison-generation-1"
        )
        validate_comparison(
            comparison_with_generation,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            start="2026-01-01",
            end="2026-01-31",
            expected_generation="generation-1",
            expected_comparison_generation="comparison-generation-1",
        )
        for comparison_generation in (None, "comparison-generation-2"):
            with self.subTest(
                comparison_generation=comparison_generation,
            ):
                invalid = copy.deepcopy(comparison)
                if comparison_generation is not None:
                    invalid["metadata"]["comparison_generation"] = (
                        comparison_generation
                    )
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "Comparison generation",
                ):
                    validate_comparison(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        start="2026-01-01",
                        end="2026-01-31",
                        expected_generation="generation-1",
                        expected_comparison_generation=(
                            "comparison-generation-1"
                        ),
                    )
        stale_comparison = copy.deepcopy(comparison)
        stale_comparison["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generations differ"):
            validate_comparison(
                stale_comparison,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                start="2026-01-01",
                end="2026-01-31",
                expected_generation="generation-1",
            )
        with self.assertRaisesRegex(ReleaseCheckError, "no daily observations"):
            validate_comparison(
                {**comparison, "observations": []},
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                start="2026-01-01",
                end="2026-01-31",
                expected_generation="generation-1",
            )

        quality_markets = [
            {
                "market_id": market_id,
                "market_type": (
                    "cex" if market_id.startswith("cex:") else "dex"
                ),
                "token_symbol": "AAVE",
                "quality_status": "ok",
                "quality_flags": [],
                "screening_quality_status": "ok",
                "screening_quality_flags": [],
                "screening_quality_scope": "catalog",
                "screening_quality_window": {
                    "start": "2026-01-01",
                    "end": "2026-01-31",
                    "method": "max_query_source_market_observed_start",
                },
                "facts": {
                    fact_name: {
                        "status": (
                            "not_applicable"
                            if market_id.startswith("cex:")
                            and fact_name == "tvl"
                            else "observed"
                        ),
                        "reason_code": (
                            "cex_markets_do_not_have_pool_tvl"
                            if market_id.startswith("cex:")
                            and fact_name == "tvl"
                            else "observed"
                        ),
                        "retryable": False,
                        "action": None,
                        "quality_flags": [],
                    }
                    for fact_name in ("daily", "tvl", "depth", "execution")
                },
            }
            for market_id in (market_a, market_b)
        ]
        quality = {
            "token_symbol": "AAVE",
            "metadata": {
                "contract_version": 4,
                "data_generation": "generation-1",
                "scope": "selected",
                "selected_market_ids": [market_a, market_b],
                "daily_quality_report": {
                    "status": "matched",
                    "evidence_mode": "published_daily_audit",
                    "identity_status": "matched_current_import",
                    "schema": "fact_quality_report/v1",
                    "selected_window_issue_count": 0,
                    "issue_outcome_counts": [],
                    "reason_code_counts": {},
                    "status_counts": {},
                    "affected_date_count": 0,
                    "affected_dates": [],
                    "market_issue_rollups": [
                        {
                            "market_id": market_id,
                            "issue_count": 0,
                            "issue_outcome_counts": [],
                            "reason_code_counts": {},
                            "status_counts": {},
                            "affected_date_count": 0,
                            "affected_dates": [],
                            "evidence_mode": "published_daily_audit",
                            "fact_outcome": {
                                "status": "observed",
                                "reason_code": "observed",
                                "retryable": False,
                                "action": None,
                            },
                        }
                        for market_id in (market_a, market_b)
                    ],
                },
            },
            "markets": quality_markets,
        }
        for market in quality["markets"]:
            market["facts"]["daily"].update(
                {
                    "daily_evidence_mode": "published_daily_audit",
                    "issue_status_counts": {},
                    "issue_outcome_counts": [],
                    "reason_code_counts": {},
                    "affected_date_count": 0,
                    "affected_dates": [],
                }
            )
        validate_quality(
            quality,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        fallback = copy.deepcopy(quality)
        fallback["metadata"]["daily_quality_report"] = {
            "status": "unavailable",
            "evidence_mode": "catalog_window_inference",
            "identity_status": "not_verified",
            "selected_window_issue_count": 0,
            "reason_code_counts": {},
            "status_counts": {},
            "affected_date_count": 0,
            "affected_dates": [],
        }
        for market in fallback["markets"]:
            for field in DAILY_FACT_EVIDENCE_FIELDS:
                market["facts"]["daily"].pop(field, None)
        validate_quality(
            fallback,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        fallback_with_published_evidence = copy.deepcopy(fallback)
        fallback_with_published_evidence["markets"][0]["facts"][
            "daily"
        ]["affected_dates"] = []
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "daily fact evidence/action mode is invalid",
        ):
            validate_quality(
                fallback_with_published_evidence,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        spoofed_market_type = copy.deepcopy(quality)
        spoofed_market_type["markets"][0]["market_type"] = "dex"
        spoofed_market_type["markets"][0]["facts"]["tvl"].update(
            {
                "status": "observed",
                "reason_code": "observed",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "market identity/type",
        ):
            validate_quality(
                spoofed_market_type,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        legacy_daily = copy.deepcopy(quality)
        legacy_daily["markets"][0]["facts"]["daily"].update(
            {
                "status": "legacy_ohlcv_snapshot",
                "reason_code": "legacy_ohlcv_snapshot",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                legacy_daily,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        unsupported_without_fact_evidence = copy.deepcopy(quality)
        unsupported_without_fact_evidence["metadata"][
            "daily_quality_report"
        ].update(
            {
                "selected_window_issue_count": 1,
                "issue_outcome_counts": [
                    {
                        "status": "unsupported",
                        "reason_code": "source_range_unavailable",
                        "count": 1,
                    }
                ],
                "reason_code_counts": {"source_range_unavailable": 1},
                "status_counts": {"unsupported": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
            }
        )
        unsupported_without_fact_evidence["metadata"][
            "daily_quality_report"
        ]["market_issue_rollups"][0].update(
            {
                "issue_count": 1,
                "issue_outcome_counts": [
                    {
                        "status": "unsupported",
                        "reason_code": "source_range_unavailable",
                        "count": 1,
                    }
                ],
                "reason_code_counts": {
                    "source_range_unavailable": 1,
                },
                "status_counts": {"unsupported": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
                "evidence_mode": "published_daily_audit",
                "fact_outcome": {
                    "status": "unsupported",
                    "reason_code": "source_range_unavailable",
                    "retryable": False,
                    "action": "operator_review_source_outcome",
                },
            }
        )
        unsupported_without_fact_evidence["markets"][0]["facts"][
            "daily"
        ].update(
            {
                "status": "unsupported",
                "reason_code": "source_range_unavailable",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "daily.*(?:action|evidence)",
        ):
            validate_quality(
                unsupported_without_fact_evidence,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        for (
            lifecycle_status,
            lifecycle_reason,
            lifecycle_action,
            lifecycle_flag_code,
        ) in (
            (
                "source_no_observation",
                "instrument_absent_from_current_catalog",
                "operator_review_source_outcome",
                "inactive_cex_instrument",
            ),
            (
                "needs_review",
                "official_catalog_evidence_stale",
                "operator_manual_review",
                "stale_cex_lifecycle_evidence",
            ),
        ):
            with self.subTest(
                lifecycle_without_daily_issue=lifecycle_reason,
            ):
                lifecycle_only = copy.deepcopy(quality)
                lifecycle_flag = {
                    "code": lifecycle_flag_code,
                    "severity": "critical",
                    "category": "data_health",
                    "message": "Official CEX catalog evidence withholds current facts.",
                    "observed_value": "2026-01-16T00:00:00+00:00",
                    "threshold": "present_and_current_official_catalog_evidence",
                }
                lifecycle_only["markets"][0]["facts"]["daily"].update(
                    {
                        "status": lifecycle_status,
                        "reason_code": lifecycle_reason,
                        "retryable": False,
                        "action": lifecycle_action,
                        "quality_flags": [lifecycle_flag],
                    }
                )
                lifecycle_only["markets"][0].update(
                    {
                        "quality_status": "critical",
                        "quality_flags": [lifecycle_flag],
                    }
                )
                lifecycle_only["metadata"]["daily_quality_report"][
                    "market_issue_rollups"
                ][0]["fact_outcome"] = {
                    "status": lifecycle_status,
                    "reason_code": lifecycle_reason,
                    "retryable": False,
                    "action": lifecycle_action,
                }

                validate_quality(
                    lifecycle_only,
                    token="AAVE",
                    market_a=market_a,
                    market_b=market_b,
                    expected_generation="generation-1",
                )

                missing_lifecycle_flag = copy.deepcopy(lifecycle_only)
                missing_lifecycle_flag["markets"][0]["facts"]["daily"][
                    "quality_flags"
                ] = []
                missing_lifecycle_flag["markets"][0].update(
                    {"quality_status": "ok", "quality_flags": []}
                )
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "zero evidence/action",
                ):
                    validate_quality(
                        missing_lifecycle_flag,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                    )

        dex_lifecycle = copy.deepcopy(quality)
        dex_lifecycle_flag = {
            "code": "inactive_cex_instrument",
            "severity": "critical",
            "category": "data_health",
            "message": "Official CEX catalog evidence withholds current facts.",
            "observed_value": "2026-01-16T00:00:00+00:00",
            "threshold": "present_and_current_official_catalog_evidence",
        }
        dex_lifecycle["markets"][1]["facts"]["daily"].update(
            {
                "status": "source_no_observation",
                "reason_code": "instrument_absent_from_current_catalog",
                "retryable": False,
                "action": "operator_review_source_outcome",
                "quality_flags": [dex_lifecycle_flag],
            }
        )
        dex_lifecycle["markets"][1].update(
            {
                "quality_status": "critical",
                "quality_flags": [dex_lifecycle_flag],
            }
        )
        dex_lifecycle["metadata"]["daily_quality_report"][
            "market_issue_rollups"
        ][1]["fact_outcome"] = {
            "status": "source_no_observation",
            "reason_code": "instrument_absent_from_current_catalog",
            "retryable": False,
            "action": "operator_review_source_outcome",
        }
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "zero evidence/action",
        ):
            validate_quality(
                dex_lifecycle,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        instrument_absent = copy.deepcopy(quality)
        instrument_absent["metadata"]["daily_quality_report"].update(
            {
                "selected_window_issue_count": 1,
                "issue_outcome_counts": [
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "count": 1,
                    }
                ],
                "reason_code_counts": {
                    "instrument_absent_from_current_catalog": 1,
                },
                "status_counts": {"source_no_observation": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
            }
        )
        instrument_absent["metadata"]["daily_quality_report"][
            "market_issue_rollups"
        ][0].update(
            {
                "issue_count": 1,
                "issue_outcome_counts": [
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "count": 1,
                    }
                ],
                "reason_code_counts": {
                    "instrument_absent_from_current_catalog": 1,
                },
                "status_counts": {"source_no_observation": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
                "evidence_mode": "published_daily_audit",
                "fact_outcome": {
                    "status": "source_no_observation",
                    "reason_code": (
                        "instrument_absent_from_current_catalog"
                    ),
                    "retryable": False,
                    "action": "operator_review_source_outcome",
                },
            }
        )
        instrument_absent["markets"][0]["facts"]["daily"].update(
            {
                "status": "source_no_observation",
                "reason_code": "instrument_absent_from_current_catalog",
                "retryable": False,
                "action": "operator_review_source_outcome",
                "daily_evidence_mode": "published_daily_audit",
                "issue_status_counts": {"source_no_observation": 1},
                "issue_outcome_counts": [
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "count": 1,
                    }
                ],
                "reason_code_counts": {
                    "instrument_absent_from_current_catalog": 1,
                },
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
            }
        )
        validate_quality(
            instrument_absent,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )
        for lifecycle_fact_name in ("depth", "execution"):
            with self.subTest(cex_lifecycle_fact=lifecycle_fact_name):
                cex_lifecycle_absence = copy.deepcopy(quality)
                cex_lifecycle_absence["markets"][0]["facts"][
                    lifecycle_fact_name
                ].update(
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "retryable": False,
                        "action": None,
                    }
                )
                validate_quality(
                    cex_lifecycle_absence,
                    token="AAVE",
                    market_a=market_a,
                    market_b=market_b,
                    expected_generation="generation-1",
                )

            with self.subTest(dex_lifecycle_fact=lifecycle_fact_name):
                dex_lifecycle_absence = copy.deepcopy(quality)
                dex_lifecycle_absence["markets"][1]["facts"][
                    lifecycle_fact_name
                ].update(
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "retryable": False,
                        "action": None,
                    }
                )
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "canonical outcome",
                ):
                    validate_quality(
                        dex_lifecycle_absence,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                    )

        mixed_report = copy.deepcopy(quality)
        mixed_report["metadata"]["daily_quality_report"].update(
            {
                "selected_window_issue_count": 2,
                "issue_outcome_counts": [
                    {
                        "status": "collection_failed",
                        "reason_code": "network",
                        "count": 1,
                    },
                    {
                        "status": "needs_review",
                        "reason_code": "not_listed",
                        "count": 1,
                    },
                ],
                "reason_code_counts": {"network": 1, "not_listed": 1},
                "status_counts": {
                    "collection_failed": 1,
                    "needs_review": 1,
                },
                "affected_date_count": 2,
                "affected_dates": ["2026-01-15", "2026-01-16"],
            }
        )
        mixed_report["metadata"]["daily_quality_report"][
            "market_issue_rollups"
        ][0].update(
            {
                "issue_count": 2,
                "issue_outcome_counts": [
                    {
                        "status": "collection_failed",
                        "reason_code": "network",
                        "count": 1,
                    },
                    {
                        "status": "needs_review",
                        "reason_code": "not_listed",
                        "count": 1,
                    },
                ],
                "reason_code_counts": {
                    "network": 1,
                    "not_listed": 1,
                },
                "status_counts": {
                    "collection_failed": 1,
                    "needs_review": 1,
                },
                "affected_date_count": 2,
                "affected_dates": ["2026-01-15", "2026-01-16"],
                "evidence_mode": "published_daily_audit",
                "fact_outcome": {
                    "status": "collection_failed",
                    "reason_code": "multiple_daily_quality_reasons",
                    "retryable": True,
                    "action": "operator_review_retry_and_manual_queues",
                },
            }
        )
        mixed_daily = mixed_report["markets"][0]["facts"]["daily"]
        mixed_daily.update(
            {
                "status": "collection_failed",
                "reason_code": "multiple_daily_quality_reasons",
                "retryable": True,
                "action": "operator_review_retry_queue",
            }
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "daily.*(?:action|evidence)",
        ):
            validate_quality(
                mixed_report,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        mixed_daily.update(
            {
                "action": "operator_review_retry_and_manual_queues",
                "daily_evidence_mode": "published_daily_audit",
                "issue_status_counts": {
                    "collection_failed": 1,
                    "needs_review": 1,
                },
                "issue_outcome_counts": [
                    {
                        "status": "collection_failed",
                        "reason_code": "network",
                        "count": 1,
                    },
                    {
                        "status": "needs_review",
                        "reason_code": "not_listed",
                        "count": 1,
                    },
                ],
                "reason_code_counts": {
                    "network": 1,
                    "not_listed": 1,
                },
                "affected_date_count": 2,
                "affected_dates": ["2026-01-15", "2026-01-16"],
            }
        )
        validate_quality(
            mixed_report,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        impossible_marginals = copy.deepcopy(mixed_report)
        impossible_report = impossible_marginals["metadata"][
            "daily_quality_report"
        ]
        impossible_report["reason_code_counts"] = {"network": 2}
        impossible_report["issue_outcome_counts"] = [
            {
                "status": "collection_failed",
                "reason_code": "network",
                "count": 1,
            },
            {
                "status": "needs_review",
                "reason_code": "not_listed",
                "count": 1,
            },
        ]
        impossible_rollup = impossible_report["market_issue_rollups"][0]
        impossible_rollup["reason_code_counts"] = {"network": 2}
        impossible_rollup["issue_outcome_counts"] = copy.deepcopy(
            impossible_report["issue_outcome_counts"]
        )
        impossible_fact = impossible_marginals["markets"][0]["facts"][
            "daily"
        ]
        impossible_fact["reason_code"] = "network"
        impossible_fact["reason_code_counts"] = {"network": 2}
        impossible_fact["issue_outcome_counts"] = copy.deepcopy(
            impossible_report["issue_outcome_counts"]
        )
        impossible_rollup["fact_outcome"]["reason_code"] = "network"
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "outcome counts",
        ):
            validate_quality(
                impossible_marginals,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        distinct_zero_outcomes = copy.deepcopy(quality)
        first_daily = distinct_zero_outcomes["markets"][0]["facts"][
            "daily"
        ]
        first_daily.update(
            {
                "status": "not_applicable",
                "reason_code": (
                    "selected_window_before_first_market_observation"
                ),
                "retryable": False,
                "action": None,
            }
        )
        distinct_zero_outcomes["metadata"]["daily_quality_report"][
            "market_issue_rollups"
        ][0]["fact_outcome"] = {
            "status": "not_applicable",
            "reason_code": "selected_window_before_first_market_observation",
            "retryable": False,
            "action": None,
        }
        validate_quality(
            distinct_zero_outcomes,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )
        swapped_zero_facts = copy.deepcopy(distinct_zero_outcomes)
        facts_a = swapped_zero_facts["markets"][0]["facts"]
        facts_b = swapped_zero_facts["markets"][1]["facts"]
        facts_a["daily"], facts_b["daily"] = facts_b["daily"], facts_a[
            "daily"
        ]
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "market rollup",
        ):
            validate_quality(
                swapped_zero_facts,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        impossible_cex_tvl = copy.deepcopy(quality)
        impossible_cex_tvl["markets"][0]["facts"]["tvl"].update(
            {
                "status": "observed",
                "reason_code": "observed",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                impossible_cex_tvl,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        fail_closed_cex_tvl = copy.deepcopy(quality)
        fail_closed_cex_tvl["markets"][0]["facts"]["tvl"].update(
            {
                "status": "needs_review",
                "reason_code": "daily_quality_outcome_invalid",
                "retryable": False,
                "action": "operator_manual_review",
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                fail_closed_cex_tvl,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        needs_review = copy.deepcopy(quality)
        needs_review["markets"][0]["facts"]["depth"].update(
            {
                "status": "needs_review",
                "reason_code": "not_listed",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                needs_review,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )
        needs_review["markets"][0]["facts"]["depth"][
            "action"
        ] = "operator_manual_review"
        validate_quality(
            needs_review,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        producer_retry_actions = (
            (
                0,
                "depth",
                "collection_failed",
                "network",
                "retry_depth_collection",
            ),
            (
                0,
                "execution",
                "failed",
                "execution_snapshot_invalid",
                "retry_execution_collection",
            ),
            (
                1,
                "tvl",
                "collection_failed",
                "source_unavailable",
                "retry_tvl_collection",
            ),
        )
        for (
            market_index,
            fact_name,
            status,
            reason_code,
            action,
        ) in producer_retry_actions:
            with self.subTest(producer_retry_action=action):
                retryable_fact = copy.deepcopy(quality)
                retryable_fact["markets"][market_index]["facts"][
                    fact_name
                ].update(
                    {
                        "status": status,
                        "reason_code": reason_code,
                        "retryable": True,
                        "action": action,
                    }
                )
                validate_quality(
                    retryable_fact,
                    token="AAVE",
                    market_a=market_a,
                    market_b=market_b,
                    expected_generation="generation-1",
                )
        missing_action = copy.deepcopy(quality)
        missing_action["markets"][0]["facts"]["depth"].pop("action")
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "selected quality contract",
        ):
            validate_quality(
                missing_action,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        invalid_tuple = copy.deepcopy(quality)
        invalid_tuple["markets"][0]["facts"]["depth"].update(
            {
                "status": "unsupported",
                "reason_code": "network",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                invalid_tuple,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        critical_flag = {
            "code": "depth_failed",
            "severity": "critical",
            "category": "data_health",
            "message": "Depth collection failed validation.",
            "observed_value": None,
            "threshold": None,
        }
        explicit_null_measurements = copy.deepcopy(quality)
        explicit_null_measurements["markets"][0]["quality_status"] = (
            "critical"
        )
        explicit_null_measurements["markets"][0]["quality_flags"] = [
            copy.deepcopy(critical_flag)
        ]
        explicit_null_measurements["markets"][0]["facts"]["depth"][
            "quality_flags"
        ] = [copy.deepcopy(critical_flag)]
        validate_quality(
            explicit_null_measurements,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )
        for measurement_field in ("observed_value", "threshold"):
            with self.subTest(missing_measurement_field=measurement_field):
                missing_measurement = copy.deepcopy(
                    explicit_null_measurements
                )
                missing_measurement["markets"][0]["quality_flags"][0].pop(
                    measurement_field
                )
                missing_measurement["markets"][0]["facts"]["depth"][
                    "quality_flags"
                ][0].pop(measurement_field)
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "missing or unknown fields",
                ):
                    validate_quality(
                        missing_measurement,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                    )
        status_drift = copy.deepcopy(quality)
        status_drift["markets"][0]["quality_flags"] = [critical_flag]
        status_drift["markets"][0]["facts"]["depth"][
            "quality_flags"
        ] = [copy.deepcopy(critical_flag)]
        with self.assertRaisesRegex(ReleaseCheckError, "status.*flags"):
            validate_quality(
                status_drift,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        fact_flag_drift = copy.deepcopy(quality)
        fact_flag_drift["markets"][0]["quality_status"] = "critical"
        fact_flag_drift["markets"][0]["quality_flags"] = [critical_flag]
        with self.assertRaisesRegex(ReleaseCheckError, "fact flag projection"):
            validate_quality(
                fact_flag_drift,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        selected_contract_mutations = {
            "missing selected status": lambda row: row.pop("quality_status"),
            "missing selected flags": lambda row: row.pop("quality_flags"),
            "missing screening status": lambda row: row.pop(
                "screening_quality_status"
            ),
            "missing screening flags": lambda row: row.pop(
                "screening_quality_flags"
            ),
            "missing fact family": lambda row: row["facts"].pop("execution"),
            "unknown fact family": lambda row: row["facts"].update(
                {"funding": {"status": "observed"}}
            ),
            "fact without retryability": lambda row: row["facts"]["depth"].pop(
                "retryable"
            ),
        }
        for label, mutate in selected_contract_mutations.items():
            with self.subTest(label=label):
                invalid = copy.deepcopy(quality)
                mutate(invalid["markets"][0])
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "selected quality contract",
                ):
                    validate_quality(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                    )
        stale_quality = copy.deepcopy(quality)
        stale_quality["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generations differ"):
            validate_quality(
                stale_quality,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )
        v3_quality = copy.deepcopy(quality)
        v3_quality["metadata"]["contract_version"] = 3
        with self.assertRaisesRegex(ReleaseCheckError, "not v4"):
            validate_quality(
                v3_quality,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "both selected markets"):
            validate_quality(
                {**quality, "markets": quality_markets[:1]},
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
            )

        for selected_ids in (
            [market_a, market_a, market_b],
            [market_a, market_b, "cex:other:AAVE/USDT"],
        ):
            with self.subTest(selected_ids=selected_ids):
                invalid = copy.deepcopy(quality)
                invalid["metadata"]["selected_market_ids"] = selected_ids
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "wrong selected markets",
                ):
                    validate_quality(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                    )

        bool_issue_count = copy.deepcopy(quality)
        bool_issue_count["metadata"]["daily_quality_report"][
            "selected_window_issue_count"
        ] = False
        with self.assertRaisesRegex(ReleaseCheckError, "reason/status counts"):
            validate_quality(
                bool_issue_count,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
            )

        bool_date_count = copy.deepcopy(quality)
        bool_date_count["metadata"]["daily_quality_report"][
            "affected_date_count"
        ] = False
        with self.assertRaisesRegex(ReleaseCheckError, "affected dates"):
            validate_quality(
                bool_date_count,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
            )

        for affected_date in (
            "2026-02-30",
            "2026-W01-1",
            "2026-1-01",
            "not-a-date",
        ):
            with self.subTest(affected_date=affected_date):
                invalid = copy.deepcopy(quality)
                report = invalid["metadata"]["daily_quality_report"]
                report["affected_date_count"] = 1
                report["affected_dates"] = [affected_date]
                with self.assertRaisesRegex(ReleaseCheckError, "affected dates"):
                    validate_quality(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                    )

        def execution_rows(market_id, status):
            cohort_id = f"{market_id.split(':', 1)[0]}-cohort-1"
            return [
                {
                    "market_id": market_id,
                    "token_symbol": "AAVE",
                    "observed_at": "2026-01-02T00:00:07+00:00",
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "status": status,
                    "snapshot_id": cohort_id,
                    "source_snapshot_id": cohort_id,
                }
                for direction in ("sell_token", "buy_token")
                for notional in (1_000, 5_000, 10_000, 50_000, 100_000)
            ]

        execution = {
            "metadata": {
                "data_generation": "generation-1",
                "cohort_observation_model": "bounded_sequential_observations",
                "snapshots": {
                    "cex": {
                        "snapshot_ids": ["cex-cohort-1"],
                        "source_snapshot_ids": ["cex-cohort-1"],
                        "observed_at": "2026-01-02T00:00:05+00:00",
                        "observed_at_min": "2026-01-02T00:00:05+00:00",
                        "observed_at_max": "2026-01-02T00:00:09+00:00",
                        "observation_span_seconds": 4,
                        "market_count": 1,
                    },
                    "dex": {
                        "snapshot_ids": ["dex-cohort-1"],
                        "source_snapshot_ids": ["dex-cohort-1"],
                        "observed_at": "2026-01-02T00:00:05+00:00",
                        "observed_at_min": "2026-01-02T00:00:05+00:00",
                        "observed_at_max": "2026-01-02T00:00:09+00:00",
                        "observation_span_seconds": 4,
                        "market_count": 1,
                    },
                },
                "cohort_lineage": {
                    "cex": {
                        "market_type": "cex",
                        "depth_snapshot_id": "cex-cohort-1",
                        "execution_snapshot_id": "cex-cohort-1",
                        "execution_source_snapshot_id": "cex-cohort-1",
                        "depth_market_count": 1,
                        "execution_market_count": 1,
                    },
                    "dex": {
                        "market_type": "dex",
                        "depth_snapshot_id": "dex-cohort-1",
                        "execution_snapshot_id": "dex-cohort-1",
                        "execution_source_snapshot_id": "dex-cohort-1",
                        "depth_market_count": 1,
                        "execution_market_count": 1,
                    },
                },
            },
            "token_symbol": "AAVE",
            "market_a": {
                "market": {"market_id": market_a},
                "status": "available",
                "rows": execution_rows(market_a, "observed"),
            },
            "market_b": {
                "market": {"market_id": market_b},
                "status": "available",
                "rows": execution_rows(market_b, "unsupported"),
            },
        }
        catalog_metadata = {
            "cex_depth_snapshot": {
                "snapshot_ids": ["cex-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:04+00:00",
                "observation_span_seconds": 4,
                "market_rows": 1,
            },
            "dex_depth_snapshot": {
                "snapshot_ids": ["dex-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:04+00:00",
                "observation_span_seconds": 4,
                "pool_rows": 1,
            },
        }
        self.assertIn(
            "catalog_metadata",
            inspect.signature(validate_execution).parameters,
        )
        validate_execution(
            execution,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
            catalog_metadata=catalog_metadata,
        )
        execution_with_generation = copy.deepcopy(execution)
        execution_with_generation["metadata"]["execution_generation"] = (
            "execution-generation-1"
        )
        validate_execution(
            execution_with_generation,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
            expected_execution_generation="execution-generation-1",
            catalog_metadata=catalog_metadata,
        )
        for execution_generation in (None, "execution-generation-2"):
            with self.subTest(execution_generation=execution_generation):
                invalid = copy.deepcopy(execution)
                if execution_generation is not None:
                    invalid["metadata"]["execution_generation"] = (
                        execution_generation
                    )
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "Execution generation",
                ):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        expected_execution_generation=(
                            "execution-generation-1"
                        ),
                        catalog_metadata=catalog_metadata,
                    )
        stale_execution = copy.deepcopy(execution)
        stale_execution["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generations differ"):
            validate_execution(
                stale_execution,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
                catalog_metadata=catalog_metadata,
            )
        unsupported_execution = {
            **execution,
            "market_a": {
                **execution["market_a"],
                "rows": execution_rows(market_a, "unsupported"),
            },
        }
        with self.assertRaisesRegex(ReleaseCheckError, "no observed or partial"):
            validate_execution(
                unsupported_execution,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
                catalog_metadata=catalog_metadata,
            )

    def test_release_execution_cohort_lineage_counterexamples_fail_closed(self):
        market_a = "cex:binance:AAVE/USDT"
        market_b = "dex:eth:uniswap_v3:pool:AAVE"

        def rows(market_id, cohort_id, status):
            return [
                {
                    "market_id": market_id,
                    "token_symbol": "AAVE",
                    "observed_at": "2026-01-02T00:00:07+00:00",
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "status": status,
                    "snapshot_id": cohort_id,
                    "source_snapshot_id": cohort_id,
                }
                for direction in ("sell_token", "buy_token")
                for notional in (1_000, 5_000, 10_000, 50_000, 100_000)
            ]

        def lineage(market_type, cohort_id):
            return {
                "market_type": market_type,
                "depth_snapshot_id": cohort_id,
                "execution_snapshot_id": cohort_id,
                "execution_source_snapshot_id": cohort_id,
                "depth_market_count": 1,
                "execution_market_count": 1,
            }

        payload = {
            "metadata": {
                "data_generation": "generation-1",
                "cohort_observation_model": "bounded_sequential_observations",
                "snapshots": {
                    "cex": {
                        "snapshot_ids": ["cex-cohort-1"],
                        "source_snapshot_ids": ["cex-cohort-1"],
                        "observed_at": "2026-01-02T00:00:05+00:00",
                        "observed_at_min": "2026-01-02T00:00:05+00:00",
                        "observed_at_max": "2026-01-02T00:00:09+00:00",
                        "observation_span_seconds": 4,
                        "market_count": 1,
                    },
                    "dex": {
                        "snapshot_ids": ["dex-cohort-1"],
                        "source_snapshot_ids": ["dex-cohort-1"],
                        "observed_at": "2026-01-02T00:00:05+00:00",
                        "observed_at_min": "2026-01-02T00:00:05+00:00",
                        "observed_at_max": "2026-01-02T00:00:09+00:00",
                        "observation_span_seconds": 4,
                        "market_count": 1,
                    },
                },
                "cohort_lineage": {
                    "cex": lineage("cex", "cex-cohort-1"),
                    "dex": lineage("dex", "dex-cohort-1"),
                },
            },
            "token_symbol": "AAVE",
            "market_a": {
                "market": {
                    "market_id": market_a,
                    "market_type": "cex",
                },
                "status": "available",
                "rows": rows(market_a, "cex-cohort-1", "observed"),
            },
            "market_b": {
                "market": {
                    "market_id": market_b,
                    "market_type": "dex",
                },
                "status": "available",
                "rows": rows(market_b, "dex-cohort-1", "unsupported"),
            },
        }
        catalog_metadata = {
            "cex_depth_snapshot": {
                "snapshot_ids": ["cex-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:04+00:00",
                "observation_span_seconds": 4,
                "market_rows": 1,
            },
            "dex_depth_snapshot": {
                "snapshot_ids": ["dex-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:04+00:00",
                "observation_span_seconds": 4,
                "pool_rows": 1,
            },
        }
        self.assertIn(
            "catalog_metadata",
            inspect.signature(validate_execution).parameters,
        )
        validate_execution(
            payload,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
            catalog_metadata=catalog_metadata,
        )

        simultaneous_claim = copy.deepcopy(payload)
        simultaneous_claim["metadata"]["cohort_observation_model"] = (
            "simultaneous_observations"
        )
        with self.assertRaises(ReleaseCheckError):
            validate_execution(
                simultaneous_claim,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
                catalog_metadata=catalog_metadata,
            )

        for metadata_field in ("cohort_lineage", "snapshots"):
            with self.subTest(extra_metadata=metadata_field):
                invalid = copy.deepcopy(payload)
                invalid["metadata"][metadata_field]["unexpected"] = {
                    "simultaneous": True,
                }
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=catalog_metadata,
                    )

        for count_field in (
            "depth_market_count",
            "execution_market_count",
        ):
            for invalid_count in (True, 1.0):
                with self.subTest(
                    count_field=count_field,
                    invalid_count=invalid_count,
                ):
                    invalid = copy.deepcopy(payload)
                    invalid["metadata"]["cohort_lineage"]["cex"][
                        count_field
                    ] = invalid_count
                    with self.assertRaises(ReleaseCheckError):
                        validate_execution(
                            invalid,
                            token="AAVE",
                            market_a=market_a,
                            market_b=market_b,
                            expected_generation="generation-1",
                            catalog_metadata=catalog_metadata,
                        )

        bounds_counterexamples = (
            ("depth", "observed_at_min", "not-a-time"),
            (
                "depth",
                "observed_at_min",
                "0001-01-01T00:00:00+23:59",
            ),
            ("depth", "observed_at_max", "2026-01-02T00:00:04"),
            ("depth", "observed_at", "2026-01-02T00:00:01+00:00"),
            ("depth", "observation_span_seconds", -1),
            ("depth", "observation_span_seconds", 5),
            ("execution", "observed_at_min", None),
            (
                "execution",
                "observed_at_max",
                "9999-12-31T23:59:59-23:59",
            ),
            ("execution", "observation_span_seconds", True),
            ("execution", "observation_span_seconds", 5),
        )
        for location, field, invalid_value in bounds_counterexamples:
            with self.subTest(
                location=location,
                field=field,
                invalid_value=invalid_value,
            ):
                invalid_payload = copy.deepcopy(payload)
                invalid_catalog = copy.deepcopy(catalog_metadata)
                target = (
                    invalid_catalog["cex_depth_snapshot"]
                    if location == "depth"
                    else invalid_payload["metadata"]["snapshots"]["cex"]
                )
                target[field] = invalid_value
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid_payload,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=invalid_catalog,
                    )

        counterexamples = {
            "market_type": "dex",
            "depth_snapshot_id": "wrong-depth",
            "execution_snapshot_id": "wrong-execution",
            "execution_source_snapshot_id": "wrong-source",
            "depth_market_count": 2,
            "execution_market_count": 2,
        }
        for field, wrong_value in counterexamples.items():
            with self.subTest(field=field):
                invalid = copy.deepcopy(payload)
                invalid["metadata"]["cohort_lineage"]["cex"][field] = (
                    wrong_value
                )
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=catalog_metadata,
                    )

        for field in ("snapshot_id", "source_snapshot_id"):
            with self.subTest(row_field=field):
                invalid = copy.deepcopy(payload)
                invalid["market_a"]["rows"][0][field] = None
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=catalog_metadata,
                    )

        for location in ("depth", "execution"):
            with self.subTest(all_null_bounds=location):
                invalid_payload = copy.deepcopy(payload)
                invalid_catalog = copy.deepcopy(catalog_metadata)
                target = (
                    invalid_catalog["cex_depth_snapshot"]
                    if location == "depth"
                    else invalid_payload["metadata"]["snapshots"]["cex"]
                )
                for field in (
                    "observed_at",
                    "observed_at_min",
                    "observed_at_max",
                    "observation_span_seconds",
                ):
                    target[field] = None
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid_payload,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=invalid_catalog,
                    )

        for invalid_time in (
            None,
            "",
            "not-a-time",
            "2026-01-02T00:00:10+00:00",
        ):
            with self.subTest(row_observed_at=invalid_time):
                invalid = copy.deepcopy(payload)
                invalid["market_a"]["rows"][0]["observed_at"] = invalid_time
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=catalog_metadata,
                    )

    def event_payload(self):
        event = {
            "event_id": "strk-unlock-2026-08-15",
            "revision": 1,
            "token_symbol": "STRK",
            "event_type": "unlock",
            "event_subtype": "scheduled_release",
            "event_name": "Scheduled STRK unlock",
            "lifecycle": "scheduled",
            "clock": {
                "state": "past",
                "as_of_utc": "2026-08-16T12:00:00Z",
                "basis": "effective_date_interval",
            },
            "evidence_status": "primary_confirmed",
            "time": {
                "effective_at": "2026-08-15",
                "effective_at_precision": "day",
                "effective_date_start": "2026-08-15",
                "effective_date_end": "2026-08-15",
            },
            "size": {
                "amount_token": "127000000",
                "amount_usd": None,
                "amount_usd_basis": None,
                "percent_of_supply": "1.27",
                "relation": "up_to",
            },
            "market": {"venue": None, "market_symbol": None, "market_id": None},
            "onchain": {
                "chain": "starknet",
                "related_address": None,
                "related_tx_hash": None,
            },
            "source": {
                "kind": "official_project",
                "url": "https://example.test/official",
                "published_at": "2024-02-22",
                "published_at_precision": "day",
                "checked_at_utc": "2026-07-29T08:30:00Z",
                "record_sha256": "a" * 64,
                "record_locator": "facts.unlock_schedule",
            },
            "revision_lineage": {
                "recorded_at_utc": "2026-07-29T08:30:00Z",
                "reason": "initial",
            },
            "notes": None,
        }
        return {
            "schema": "event_facts_api/v2",
            "fact_schema": "event_facts/v1",
            "fact_boundary": (
                "Source-backed event facts only. No return, market-impact, "
                "importance, sentiment, or causal result is included."
            ),
            "bundle_id": "a" * 24,
            "built_at_utc": "2026-07-29T08:30:00Z",
            "clock_as_of_utc": "2026-08-16T12:00:00Z",
            "availability": {"status": "available", "reason": None},
            "coverage": {
                "configured_token_count": 1,
                "covered_token_count": 1,
                "covered_tokens": ["STRK"],
                "uncovered_tokens": [],
                "query_token_has_published_fact": True,
            },
            "query": {
                "token": "STRK",
                "start": "2026-08-15",
                "end": "2026-08-15",
                "lifecycle": "scheduled",
                "clock_state": None,
            },
            "event_count": 1,
            "event_type_counts": {"unlock": 1},
            "lifecycle_counts": {"scheduled": 1},
            "evidence_status_counts": {"primary_confirmed": 1},
            "clock_state_counts": {"past": 1},
            "events": [event],
        }

    def test_event_validator_enforces_scope_lineage_and_fact_boundary(self):
        payload = self.event_payload()
        events = validate_events(
            payload,
            token="STRK",
            start="2026-08-15",
            end="2026-08-15",
            lifecycle="scheduled",
        )
        self.assertEqual(events[0]["event_id"], "strk-unlock-2026-08-15")

        filtered = self.event_payload()
        filtered["query"]["clock_state"] = "past"
        events = validate_events(
            filtered,
            token="STRK",
            start="2026-08-15",
            end="2026-08-15",
            lifecycle="scheduled",
            clock_state="past",
        )
        self.assertEqual(events[0]["clock"]["state"], "past")

        unavailable = {
            **payload,
            "availability": {
                "status": "unavailable",
                "reason": "event_bundle_not_published",
            },
            "event_count": 0,
            "events": [],
        }
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "publication is unavailable",
        ):
            validate_events(
                unavailable,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        leaked = self.event_payload()
        leaked["events"][0]["future_return"] = 0.25
        with self.assertRaisesRegex(ReleaseCheckError, "event-study result"):
            validate_events(
                leaked,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        wrong_counts = self.event_payload()
        wrong_counts["event_type_counts"] = {"cex_listing": 1}
        wrong_counts["lifecycle_counts"] = {"occurred": 1}
        wrong_counts["evidence_status_counts"] = {"cross_checked": 1}
        with self.assertRaisesRegex(ReleaseCheckError, "does not match"):
            validate_events(
                wrong_counts,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        wrong_coverage = self.event_payload()
        wrong_coverage["coverage"]["uncovered_tokens"] = ["AAVE"]
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "coverage counts are inconsistent",
        ):
            validate_events(
                wrong_coverage,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        invalid_clock = self.event_payload()
        invalid_clock["events"][0]["clock"]["state"] = "predicted"
        with self.assertRaisesRegex(ReleaseCheckError, "clock state is invalid"):
            validate_events(
                invalid_clock,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        wrong_clock = self.event_payload()
        wrong_clock["events"][0]["clock"] = {
            "state": "current_window",
            "as_of_utc": wrong_clock["clock_as_of_utc"],
            "basis": "effective_date_interval",
        }
        with self.assertRaisesRegex(ReleaseCheckError, "clock projection"):
            validate_events(
                wrong_clock,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        wrong_shared_clock = self.event_payload()
        wrong_shared_clock["events"][0]["clock"]["as_of_utc"] = (
            "2026-08-16T11:59:59Z"
        )
        with self.assertRaisesRegex(ReleaseCheckError, "shared response clock"):
            validate_events(
                wrong_shared_clock,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

    def test_event_validator_rejects_cross_scope_and_missing_evidence(self):
        wrong_token = self.event_payload()
        wrong_token["events"][0]["token_symbol"] = "AAVE"
        with self.assertRaisesRegex(ReleaseCheckError, "another Token"):
            validate_events(
                wrong_token,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        missing_source = self.event_payload()
        missing_source["events"][0]["source"]["record_locator"] = ""
        with self.assertRaisesRegex(ReleaseCheckError, "locator is missing"):
            validate_events(
                missing_source,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )


if __name__ == "__main__":
    unittest.main()
