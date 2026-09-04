import copy
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from dashboard import opportunity_facts, server
from dashboard.opportunity_facts import (
    OpportunityBundleInvalid,
    OpportunityBundleUnavailable,
    OpportunityQueryError,
    build_opportunity_payload,
    build_unavailable_opportunity_payload,
    load_latest_opportunities,
    normalize_opportunity_filters,
    opportunity_publication_health,
    resolve_opportunity_bundle,
)
from scripts.route_publication import publish_complete_route_bundle
from scripts.route_opportunity import build_terminal_route_opportunity
from tests.test_route_cost_topology import (
    BUY_MARKET_ID,
    OPPORTUNITY_ID,
    SELL_MARKET_ID,
    historical_rows,
)
from tests.test_route_publication import _task7_cex_inputs
from tests.test_route_opportunity import terminal_route_fixture


NOW = datetime(2026, 8, 1, 12, 1, 30, tzinfo=timezone.utc)
COHORT_ID = "cohort:" + "1" * 64


def _row(
    route_id,
    opportunity_id,
    *,
    opportunity_class="executable_candidate",
    strict_eligible=True,
    strict_ready=True,
    requested_notional="10000",
    strict_net_edge_usd="180",
    strict_net_edge_bps="180",
    research_net_edge_usd="180",
    research_net_edge_bps="180",
    primary_reason="positive_strict_net_edge",
    reason_codes=None,
    buy_market_id="cex:binance:AAVE/USDT",
    sell_market_id="cex:bybit:AAVE/USDT",
):
    return {
        "cohort_id": COHORT_ID,
        "route_id": route_id,
        "opportunity_id": opportunity_id,
        "token_symbol": "AAVE",
        "buy_market_id": buy_market_id,
        "sell_market_id": sell_market_id,
        "route_mode": "prepositioned_inventory",
        "requested_notional_usd": requested_notional,
        "target_token_quantity": "100",
        "buy_state_observed_at": "2026-08-01T12:01:00Z",
        "sell_state_observed_at": "2026-08-01T12:00:30Z",
        "skew_seconds": "30",
        "route_age_seconds": "30",
        "gross_buy_cost_usd": "10000",
        "gross_sell_proceeds_usd": "10200",
        "gross_edge_usd": "200",
        "gross_edge_bps": "200",
        "gross_edge_bps_numerator": "200",
        "gross_edge_bps_denominator": "1",
        "strict_nonembedded_cost_usd": "20",
        "research_bounded_cost_usd": (
            "0" if opportunity_class == "executable_candidate" else "5"
        ),
        "research_assumed_cost_usd": "0",
        "strict_net_edge_usd": strict_net_edge_usd,
        "strict_net_edge_bps": strict_net_edge_bps,
        "strict_net_edge_bps_numerator": strict_net_edge_bps,
        "strict_net_edge_bps_denominator": "1",
        "research_net_edge_usd": research_net_edge_usd,
        "research_net_edge_bps": research_net_edge_bps,
        "research_net_edge_bps_numerator": research_net_edge_bps,
        "research_net_edge_bps_denominator": (
            "1" if research_net_edge_bps is not None else None
        ),
        "cost_completeness": (
            "incomplete"
            if opportunity_class == "research_estimate"
            else "complete"
        ),
        "scenario_cost_completeness": "complete",
        "reflected_or_embedded_component_keys": [],
        "maximum_proved_capacity_quantity": "250",
        "opportunity_class": opportunity_class,
        "primary_reason": primary_reason,
        "reason_codes": list(reason_codes or []),
        "strict_eligible": strict_eligible,
        "strict_ready_for_publication": strict_ready,
        "publication_attestation_sha256": (
            "a" * 64 if strict_eligible else None
        ),
    }


def _cost(
    opportunity_id,
    *,
    leg="buy",
    market_id="cex:binance:AAVE/USDT",
    component_type="venue_taker_fee",
    value_status="authenticated",
    amount="10",
    rate_bps="10",
    strict_eligible=True,
    requested_notional="10000",
    target_quantity="100",
):
    row = {
        "cohort_id": COHORT_ID,
        "opportunity_id": opportunity_id,
        "leg": leg,
        "market_id": market_id,
        "component_type": component_type,
        "value_status": value_status,
        "amount_usd": amount,
        "rate_bps": rate_bps,
        "requested_notional_usd": requested_notional,
        "target_token_quantity": target_quantity,
        "strict_eligible": strict_eligible,
        "embedded_in_leg_quote": False,
        "basis": "authenticated account fee tier",
        "reason_code": None,
    }
    if value_status in {"measured", "authenticated", "quoted"}:
        row["observed_at"] = "2026-08-01T12:00:00Z"
    if value_status in {"authenticated", "quoted"}:
        row["valid_until"] = "2026-08-01T13:00:00Z"
    return row


def _route_costs(row):
    shared = {
        "requested_notional": row["requested_notional_usd"],
        "target_quantity": row["target_token_quantity"],
    }
    route_cost = {
        "value_status": "not_applicable",
        "amount": None,
        "rate_bps": None,
        "strict_eligible": True,
    }
    if row["opportunity_class"] == "research_estimate":
        route_cost = {
            "value_status": "bounded_estimate",
            "amount": "5",
            "rate_bps": "5",
            "strict_eligible": False,
        }
    return [
        _cost(
            row["opportunity_id"],
            leg="buy",
            market_id=row["buy_market_id"],
            **shared,
        ),
        _cost(
            row["opportunity_id"],
            leg="sell",
            market_id=row["sell_market_id"],
            **shared,
        ),
        _cost(
            row["opportunity_id"],
            leg="route",
            market_id="",
            component_type="rebalancing_or_transfer",
            **route_cost,
            **shared,
        ),
    ]


def _manifest(rows, *, core_manifest_sha256=None):
    manifest = {
        "route_cohort_id": COHORT_ID,
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "counts": {
            "routes": len({row["route_id"] for row in rows}),
            "opportunities": len(rows),
            "classification": {
                "strict": sum(row["strict_eligible"] for row in rows),
                "research": sum(
                    row["opportunity_class"] == "research_estimate"
                    for row in rows
                ),
                "unavailable": sum(
                    row["opportunity_class"] == "unavailable"
                    for row in rows
                ),
            },
        },
    }
    if core_manifest_sha256 is not None:
        manifest["core_manifest_sha256"] = core_manifest_sha256
    return manifest


def _rehash_terminal_row(row):
    normalized = dict(row)
    normalized.pop("evidence_binding_sha256", None)
    normalized["evidence_binding_sha256"] = hashlib.sha256(json.dumps(
        normalized,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    return normalized


LEGS = [
    {
        "market_id": "cex:binance:AAVE/USDT",
        "source_endpoint": (
            "https://api.binance.com:443/api/v3/depth?account=secret"
        ),
    },
    {
        "market_id": "cex:bybit:AAVE/USDT",
        "source_endpoint": "https://api.bybit.com/v5/market/orderbook",
    },
]


class OpportunityBundleReaderTests(unittest.TestCase):
    def _publish(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        core_root = root / "routes/core"
        raw_root = root / "raw/route-cohort"
        fixture = _task7_cex_inputs(
            core_root,
            raw_root,
            root / "typed-sources",
            root / "private-profiles",
        )
        routes_root = root / "routes"
        publish_complete_route_bundle(
            core_root=core_root,
            raw_root=raw_root,
            routes_root=routes_root,
            source_root=fixture["source_root"],
            fee_profile_path=fixture["fee_profile_path"],
            fee_profile_id=fixture["fee_profile_id"],
            inventory_profile_path=fixture["inventory_profile_path"],
            opportunity_inputs=fixture["opportunity_inputs"],
        )
        return routes_root

    def test_resolver_prefers_explicit_root_without_traversing_pointer(self):
        root = Path("/controlled/routes")

        self.assertEqual(resolve_opportunity_bundle(root), root)

    def test_loader_traverses_only_complete_pointer_and_validates_bundle(self):
        root = self._publish()

        loaded = load_latest_opportunities(root)

        self.assertEqual(loaded["pointer"]["bundle_stage"], "route_opportunity/v1")
        self.assertEqual(len(loaded["opportunities"]), 5)
        self.assertEqual(len(loaded["cost_components"]), 15)

    def test_missing_complete_pointer_has_one_stable_unavailable_reason(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "routes"
        root.mkdir()

        with self.assertRaises(OpportunityBundleUnavailable) as raised:
            load_latest_opportunities(root)

        self.assertEqual(raised.exception.reason, "complete_pointer_absent")
        self.assertEqual(str(raised.exception), "complete_pointer_absent")

    def test_manifest_hash_tamper_becomes_bounded_invalid_error(self):
        root = self._publish()
        loaded = load_latest_opportunities(root)
        manifest = loaded["path"] / "manifest.json"
        manifest.write_bytes(manifest.read_bytes() + b" ")

        with self.assertRaises(OpportunityBundleInvalid) as raised:
            load_latest_opportunities(root)

        self.assertEqual(raised.exception.reason, "opportunity_bundle_validation_failed")
        self.assertNotIn(str(root), str(raised.exception))

    def test_csv_sqlite_fingerprint_mismatch_is_not_returned(self):
        root = self._publish()
        loaded = load_latest_opportunities(root)
        csv_path = loaded["path"] / "route_opportunities.csv"
        csv_path.write_bytes(csv_path.read_bytes() + b"\n")

        with self.assertRaises(OpportunityBundleInvalid):
            load_latest_opportunities(root)

    def test_publication_health_distinguishes_current_stale_missing_and_invalid(self):
        root = self._publish()
        current = opportunity_publication_health(root, now=NOW)
        stale = opportunity_publication_health(
            root,
            now=datetime(2026, 8, 1, 12, 3, 1, tzinfo=timezone.utc),
        )
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        missing_root = Path(temporary.name) / "missing-routes"
        missing = opportunity_publication_health(missing_root, now=NOW)
        loaded = load_latest_opportunities(root)
        manifest = loaded["path"] / "manifest.json"
        manifest.write_bytes(manifest.read_bytes() + b" ")
        invalid = opportunity_publication_health(root, now=NOW)

        self.assertEqual(current["status"], "current")
        self.assertEqual(stale["status"], "stale")
        self.assertEqual(stale["reason"], "cohort_stale")
        self.assertEqual(missing["status"], "missing")
        self.assertEqual(missing["reason"], "complete_pointer_absent")
        self.assertEqual(invalid["status"], "invalid")
        self.assertEqual(
            invalid["reason"],
            "opportunity_bundle_validation_failed",
        )

    def test_publication_health_reports_the_worst_individual_route(self):
        current_row = _row("route:current", "opportunity:current")
        stale_row = _row("route:stale", "opportunity:stale")
        stale_row["buy_state_observed_at"] = "2026-08-01T11:58:00Z"
        stale_row["sell_state_observed_at"] = "2026-08-01T11:57:45Z"
        skewed_row = _row("route:skewed", "opportunity:skewed")
        skewed_row["buy_state_observed_at"] = "2026-08-01T12:01:00Z"
        skewed_row["sell_state_observed_at"] = "2026-08-01T11:59:59Z"

        stale_rows = [current_row, stale_row]
        unavailable_rows = [current_row, stale_row, skewed_row]

        with patch(
            "dashboard.opportunity_facts.load_latest_opportunities",
            return_value={
                "manifest": _manifest(stale_rows),
                "manifest_sha256": "a" * 64,
                "opportunities": stale_rows,
                "cost_components": [
                    component
                    for row in stale_rows
                    for component in _route_costs(row)
                ],
            },
        ):
            stale = opportunity_publication_health(now=NOW)
        with patch(
            "dashboard.opportunity_facts.load_latest_opportunities",
            return_value={
                "manifest": _manifest(unavailable_rows),
                "manifest_sha256": "a" * 64,
                "opportunities": unavailable_rows,
                "cost_components": [
                    component
                    for row in unavailable_rows
                    for component in _route_costs(row)
                ],
            },
        ):
            unavailable = opportunity_publication_health(now=NOW)

        self.assertEqual(stale["status"], "stale")
        self.assertEqual(stale["reason"], "cohort_stale")
        self.assertEqual(unavailable["status"], "unavailable")
        self.assertEqual(unavailable["reason"], "snapshot_skew_exceeded")

    def test_publication_health_marks_expired_required_cost_stale(self):
        row = _row("route:cost-expiry", "opportunity:cost-expiry")
        costs = _route_costs(row)
        for component in costs:
            if component["component_type"] == "venue_taker_fee":
                component["observed_at"] = "2026-08-01T12:00:00Z"
                component["valid_until"] = (
                    "2026-08-01T12:02:00.500000Z"
                )
        loaded = {
            "manifest": _manifest([row]),
            "manifest_sha256": "a" * 64,
            "opportunities": [row],
            "cost_components": costs,
        }

        with patch(
            "dashboard.opportunity_facts.load_latest_opportunities",
            return_value=loaded,
        ):
            health = opportunity_publication_health(
                now=datetime(
                    2026, 8, 1, 12, 2, 0, 500_000,
                    tzinfo=timezone.utc,
                )
            )

        self.assertEqual(health["status"], "stale")
        self.assertEqual(health["reason"], "cost_component_stale")

    def test_publication_health_fails_closed_on_any_invalid_route_timestamp(self):
        current_row = _row("route:current", "opportunity:current")
        future_row = _row("route:future", "opportunity:future")
        future_row["buy_state_observed_at"] = "2026-08-01T12:02:00Z"
        rows = [current_row, future_row]
        loaded = {
            "manifest": _manifest(rows),
            "manifest_sha256": "a" * 64,
            "opportunities": rows,
            "cost_components": [
                component
                for row in rows
                for component in _route_costs(row)
            ],
        }

        with patch(
            "dashboard.opportunity_facts.load_latest_opportunities",
            return_value=loaded,
        ):
            health = opportunity_publication_health(now=NOW)

        self.assertEqual(health["status"], "invalid")
        self.assertEqual(
            health["reason"],
            "opportunity_bundle_validation_failed",
        )

    def test_publication_health_reuses_runtime_strict_economic_validation(self):
        row = _row(
            "route:invalid-economic",
            "opportunity:invalid-economic",
            strict_net_edge_usd="-1",
        )
        loaded = {
            "manifest": _manifest([row]),
            "manifest_sha256": "a" * 64,
            "opportunities": [row],
            "cost_components": _route_costs(row),
        }

        with patch(
            "dashboard.opportunity_facts.load_latest_opportunities",
            return_value=loaded,
        ):
            health = opportunity_publication_health(now=NOW)

        self.assertEqual(health["status"], "invalid")
        self.assertEqual(
            health["reason"],
            "opportunity_bundle_validation_failed",
        )


class TerminalOpportunityBundleTests(unittest.TestCase):
    def _terminal(self):
        build_inputs = terminal_route_fixture(cohort_id=COHORT_ID)
        row = build_terminal_route_opportunity(**build_inputs)
        route_candidate = {
            **build_inputs["route"],
            "route_volume_usd": None,
            "route_volume_basis": "minimum_leg_source_horizon_usd",
        }
        return build_inputs, row, route_candidate

    def test_terminal_shape_is_rendered_without_numeric_economics(self):
        build_inputs, row, route_candidate = self._terminal()
        try:
            payload = build_opportunity_payload(
                [row],
                manifest=_manifest(
                    [row],
                    core_manifest_sha256=build_inputs[
                        "core_manifest_sha256"
                    ],
                ),
                legs=[build_inputs["buy_leg"], build_inputs["sell_leg"]],
                cost_components=build_inputs["cost_components"],
                route_candidates=[route_candidate],
                now=NOW,
            )
        except OpportunityBundleInvalid as error:
            self.fail("terminal dashboard bundle was rejected: {}".format(error))

        projected = payload["routes"][0]
        self.assertEqual(
            projected["availability"],
            {"status": "unavailable", "reason": "sell_leg_unavailable"},
        )
        self.assertIsNone(projected["target_token_quantity"])
        self.assertIsNone(projected["gross_edge_usd"])
        self.assertIsNone(projected["net_edge_usd"])
        self.assertTrue(all(
            item["amount_usd"] is None and item["rate_bps"] is None
            for item in projected["cost_components"]
        ))

    def test_terminal_identity_and_core_lineage_are_trusted(self):
        build_inputs, row, _route_candidate = self._terminal()
        baseline_legs = [build_inputs["buy_leg"], build_inputs["sell_leg"]]
        baseline_costs = build_inputs["cost_components"]
        manifest = _manifest(
            [row],
            core_manifest_sha256=build_inputs["core_manifest_sha256"],
        )
        cases = []

        wrong_route = {**row, "route_id": "route:forged"}
        cases.append((
            "route identity",
            _rehash_terminal_row(wrong_route),
            baseline_costs,
        ))

        wrong_opportunity_id = "route:forged:10000"
        wrong_opportunity_costs = copy.deepcopy(baseline_costs)
        for component in wrong_opportunity_costs:
            component["opportunity_id"] = wrong_opportunity_id
        canonical_costs = sorted(
            wrong_opportunity_costs,
            key=lambda component: (
                component["leg"], component["component_type"]
            ),
        )
        wrong_opportunity = {
            **row,
            "opportunity_id": wrong_opportunity_id,
            "cost_component_set_sha256": hashlib.sha256(json.dumps(
                canonical_costs,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")).hexdigest(),
        }
        cases.append((
            "opportunity identity",
            _rehash_terminal_row(wrong_opportunity),
            wrong_opportunity_costs,
        ))

        for label, field in (
            ("buy core hash", "buy_core_manifest_sha256"),
            ("sell core hash", "sell_core_manifest_sha256"),
        ):
            cases.append((
                label,
                _rehash_terminal_row({**row, field: "e" * 64}),
                baseline_costs,
            ))

        for label, mutated_row, costs in cases:
            with self.subTest(label=label):
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [mutated_row],
                        manifest=manifest,
                        legs=baseline_legs,
                        cost_components=costs,
                        now=NOW,
                    )

    def test_standard_null_target_and_terminal_mutations_fail_closed(self):
        build_inputs, row, route_candidate = self._terminal()
        baseline_legs = [build_inputs["buy_leg"], build_inputs["sell_leg"]]
        baseline_costs = build_inputs["cost_components"]
        cases = {}

        within_sla_legs = copy.deepcopy(baseline_legs)
        within_sla_legs[1].update({
            "status": "observed",
            "available": True,
            "reason_code": "observed",
            "state_observed_at": "2026-08-01T12:00:30Z",
        })
        cases["within SLA timing"] = (row, within_sla_legs, baseline_costs)

        nonterminal_costs = copy.deepcopy(baseline_costs)
        nonterminal_costs[0]["value_status"] = "authenticated"
        cases["nonterminal cost"] = (row, baseline_legs, nonterminal_costs)

        different_legs = copy.deepcopy(baseline_legs)
        different_legs[0]["market_id"] = "cex:other:CAKE/USDT"
        cases["different core leg"] = (row, different_legs, baseline_costs)

        for label, field, value in (
            ("numeric target", "target_token_quantity", "1"),
            ("numeric economics", "gross_edge_usd", "1"),
            ("state ID", "buy_state_id", "fabricated-state"),
            ("attestation", "publication_attestation_sha256", "f" * 64),
        ):
            cases[label] = ({**row, field: value}, baseline_legs, baseline_costs)

        standard = _row("route:standard", "opportunity:standard")
        standard["target_token_quantity"] = None
        cases["standard null target"] = (
            standard,
            LEGS,
            _route_costs({**standard, "target_token_quantity": "100"}),
        )

        for label, (mutated_row, legs, costs) in cases.items():
            with self.subTest(label=label):
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [mutated_row],
                        manifest=_manifest(
                            [mutated_row],
                            core_manifest_sha256=(
                                None
                                if label == "standard null target"
                                else build_inputs["core_manifest_sha256"]
                            ),
                        ),
                        legs=legs,
                        cost_components=costs,
                        route_candidates=(
                            [route_candidate]
                            if label != "standard null target"
                            else None
                        ),
                        now=NOW,
                    )


class OpportunityPayloadTests(unittest.TestCase):
    def test_live_api_projection_rejects_historical_nine_row_inventory(self):
        row = _row(
            "route:historical",
            OPPORTUNITY_ID,
            opportunity_class="unavailable",
            strict_eligible=False,
            strict_ready=False,
            requested_notional="1000",
            strict_net_edge_usd=None,
            strict_net_edge_bps=None,
            research_net_edge_usd=None,
            research_net_edge_bps=None,
            primary_reason="buy_leg_unavailable",
            reason_codes=["buy_leg_unavailable"],
            buy_market_id=BUY_MARKET_ID,
            sell_market_id=SELL_MARKET_ID,
        )
        row["route_mode"] = "atomic_onchain"
        row["target_token_quantity"] = "10"
        rows = historical_rows()
        self.assertEqual(len(rows), 9)

        with patch(
            "dashboard.opportunity_facts.live_complete_cost_component_keys",
            wraps=opportunity_facts.live_complete_cost_component_keys,
        ) as live_keys:
            with self.assertRaises(OpportunityBundleInvalid):
                build_opportunity_payload(
                    [row],
                    manifest=_manifest([row]),
                    cost_components=rows,
                    now=NOW,
                )
        live_keys.assert_called_once_with(row)

    def test_unknown_route_modes_and_reasons_fail_closed(self):
        base = _row("route:a", "opportunity:a")
        mutations = {
            "route mode": ("route_mode", "spaceship"),
            "primary reason": ("primary_reason", "totally_unknown_reason"),
            "reason codes type": ("reason_codes", "cohort_stale"),
            "reason code": ("reason_codes", ["totally_unknown_reason"]),
            "duplicate reason": (
                "reason_codes",
                ["cohort_stale", "cohort_stale"],
            ),
            "cost completeness": ("cost_completeness", "mostly_complete"),
            "scenario completeness": (
                "scenario_cost_completeness",
                "sometimes_complete",
            ),
        }

        for label, (field, value) in mutations.items():
            with self.subTest(label=label):
                row = copy.deepcopy(base)
                row[field] = value
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [row],
                        manifest=_manifest([row]),
                        cost_components=self._costs([row]),
                        now=NOW,
                    )

    def test_route_class_and_reason_semantics_cannot_contradict(self):
        strict = _row("route:strict", "opportunity:strict")
        unavailable = _row(
            "route:unavailable",
            "opportunity:unavailable",
            opportunity_class="unavailable",
            strict_eligible=False,
            strict_ready=False,
            strict_net_edge_usd=None,
            strict_net_edge_bps=None,
            research_net_edge_usd=None,
            research_net_edge_bps=None,
            primary_reason="buy_leg_unavailable",
            reason_codes=["buy_leg_unavailable"],
        )
        cases = []
        wrong_strict_primary = copy.deepcopy(strict)
        wrong_strict_primary["primary_reason"] = "cohort_stale"
        cases.append(wrong_strict_primary)
        strict_with_reason = copy.deepcopy(strict)
        strict_with_reason["reason_codes"] = ["cohort_stale"]
        cases.append(strict_with_reason)
        positive_unavailable = copy.deepcopy(unavailable)
        positive_unavailable["primary_reason"] = "positive_strict_net_edge"
        positive_unavailable["reason_codes"] = ["positive_strict_net_edge"]
        cases.append(positive_unavailable)
        mismatched_first_reason = copy.deepcopy(unavailable)
        mismatched_first_reason["reason_codes"] = ["sell_leg_unavailable"]
        cases.append(mismatched_first_reason)
        empty_unavailable_reasons = copy.deepcopy(unavailable)
        empty_unavailable_reasons["reason_codes"] = []
        cases.append(empty_unavailable_reasons)

        for row in cases:
            with self.subTest(row=row):
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [row],
                        manifest=_manifest([row]),
                        cost_components=self._costs([row]),
                        now=NOW,
                    )

    def test_timing_sort_defaults_put_freshest_and_lowest_skew_first(self):
        self.assertEqual(
            normalize_opportunity_filters(sort="route_age_seconds")["direction"],
            "asc",
        )
        self.assertEqual(
            normalize_opportunity_filters(sort="skew_seconds")["direction"],
            "asc",
        )
        self.assertEqual(
            normalize_opportunity_filters(sort="net_edge_usd")["direction"],
            "desc",
        )
        self.assertEqual(
            normalize_opportunity_filters(sort="volume")["direction"],
            "desc",
        )
        self.assertEqual(
            normalize_opportunity_filters(
                sort="route_age_seconds", direction="desc"
            )["direction"],
            "desc",
        )

    def test_volume_sort_uses_sealed_route_lineage_and_keeps_na_last(self):
        rows = [
            _row("route:a", "opportunity:a"),
            _row("route:b", "opportunity:b"),
            _row("route:c", "opportunity:c"),
        ]
        route_candidates = [
            {
                "route_id": "route:a",
                "route_volume_usd": "500",
                "route_volume_basis": "minimum_leg_source_horizon_usd",
            },
            {
                "route_id": "route:b",
                "route_volume_usd": "100",
                "route_volume_basis": "minimum_leg_source_horizon_usd",
            },
            {
                "route_id": "route:c",
                "route_volume_usd": None,
                "route_volume_basis": "minimum_leg_source_horizon_usd",
            },
        ]

        descending = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            cost_components=self._costs(rows),
            route_candidates=route_candidates,
            sort="volume",
            direction="desc",
            now=NOW,
        )
        ascending = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            cost_components=self._costs(rows),
            route_candidates=route_candidates,
            sort="volume",
            direction="asc",
            now=NOW,
        )

        self.assertEqual(
            [row["route_id"] for row in descending["routes"]],
            ["route:a", "route:b", "route:c"],
        )
        self.assertEqual(
            [row["route_id"] for row in ascending["routes"]],
            ["route:b", "route:a", "route:c"],
        )
        self.assertEqual(
            descending["routes"][0]["route_volume_usd"], "500"
        )
        self.assertEqual(
            descending["routes"][0]["route_volume_basis"],
            "minimum_leg_source_horizon_usd",
        )
        self.assertIsNone(descending["routes"][-1]["route_volume_usd"])

    def _rows(self):
        strict = _row("route:a", "opportunity:a")
        estimate = _row(
            "route:b",
            "opportunity:b",
            opportunity_class="research_estimate",
            strict_eligible=False,
            strict_ready=False,
            strict_net_edge_usd="180",
            strict_net_edge_bps="180",
            research_net_edge_usd="175",
            research_net_edge_bps="175",
            primary_reason="cost_component_estimated",
            reason_codes=["cost_component_estimated"],
        )
        unavailable = _row(
            "route:c",
            "opportunity:c",
            opportunity_class="unavailable",
            strict_eligible=False,
            strict_ready=False,
            strict_net_edge_usd=None,
            strict_net_edge_bps=None,
            research_net_edge_usd=None,
            research_net_edge_bps=None,
            primary_reason="buy_leg_unavailable",
            reason_codes=["buy_leg_unavailable"],
        )
        return [strict, estimate, unavailable]

    def _costs(self, rows=None):
        return [
            cost
            for row in (rows or self._rows())
            for cost in _route_costs(row)
        ]

    def test_strict_alias_returns_only_canonical_executable_candidates(self):
        rows = self._rows()

        payload = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            legs=LEGS,
            cost_components=self._costs(rows),
            token="AAVE",
            notional_usd=10000,
            opportunity_class="strict",
            route_type="cex_cex",
            availability="available",
            sort="net_edge_usd",
            direction="desc",
            now=NOW,
        )

        self.assertEqual(payload["availability"]["status"], "available")
        self.assertEqual(len(payload["routes"]), 1)
        self.assertEqual(
            payload["routes"][0]["opportunity_class"],
            "executable_candidate",
        )
        self.assertEqual(payload["filters"]["opportunity_class"], "strict")

    def test_venue_filter_exactly_matches_canonical_leg_labels(self):
        binance_bybit = _row("route:a", "opportunity:a")
        coinbase_kraken = _row(
            "route:b",
            "opportunity:b",
            buy_market_id="cex:coinbase:AAVE/USD",
            sell_market_id="cex:kraken:AAVE/USD",
        )
        rows = [binance_bybit, coinbase_kraken]
        costs = self._costs(rows)

        kraken = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            cost_components=costs,
            venue=" KRAKEN ",
            now=NOW,
        )
        partial = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            cost_components=costs,
            venue="krak",
            now=NOW,
        )

        self.assertEqual(
            [route["route_id"] for route in kraken["routes"]],
            ["route:b"],
        )
        self.assertEqual(kraken["filters"]["venue"], "kraken")
        self.assertEqual(
            kraken["routes"][0]["leg_venues"],
            {"buy": "coinbase", "sell": "kraken"},
        )
        self.assertEqual(
            kraken["metadata"]["available_venues"],
            ["binance", "bybit", "coinbase", "kraken"],
        )
        self.assertEqual(partial["routes"], [])
        self.assertEqual(partial["metadata"]["coverage"]["returned_count"], 0)

    def test_dex_venue_filter_uses_protocol_label_not_chain_or_pool(self):
        row = _row(
            "route:dex",
            "opportunity:dex",
            opportunity_class="unavailable",
            strict_eligible=False,
            strict_ready=False,
            strict_net_edge_usd=None,
            strict_net_edge_bps=None,
            research_net_edge_usd=None,
            research_net_edge_bps=None,
            primary_reason="sell_leg_unavailable",
            reason_codes=["sell_leg_unavailable"],
            sell_market_id="dex:ethereum:uniswap-v3:0xPool:AAVE",
        )
        shared = {
            "requested_notional": row["requested_notional_usd"],
            "target_quantity": row["target_token_quantity"],
        }
        costs = [
            _cost(
                row["opportunity_id"],
                leg="buy",
                market_id=row["buy_market_id"],
                **shared,
            ),
        ]
        for component_type in (
            "pool_swap_fee",
            "network_gas",
            "router_or_integrator_fee",
            "token_transfer_tax",
        ):
            costs.append(_cost(
                row["opportunity_id"],
                leg="sell",
                market_id=row["sell_market_id"],
                component_type=component_type,
                value_status="unavailable",
                amount=None,
                rate_bps=None,
                strict_eligible=False,
                **shared,
            ))
        for component_type in ("rebalancing_or_transfer", "mev_buffer"):
            costs.append(_cost(
                row["opportunity_id"],
                leg="route",
                market_id="",
                component_type=component_type,
                value_status="unavailable",
                amount=None,
                rate_bps=None,
                strict_eligible=False,
                **shared,
            ))

        dex = build_opportunity_payload(
            [row],
            manifest=_manifest([row]),
            cost_components=costs,
            venue="uniswap-v3",
            now=NOW,
        )
        chain = build_opportunity_payload(
            [row],
            manifest=_manifest([row]),
            cost_components=costs,
            venue="ethereum",
            now=NOW,
        )

        self.assertEqual([route["route_id"] for route in dex["routes"]], ["route:dex"])
        self.assertEqual(
            dex["routes"][0]["leg_venues"],
            {"buy": "binance", "sell": "uniswap-v3"},
        )
        self.assertEqual(chain["routes"], [])

    def test_estimate_and_unavailable_are_separate_inventories(self):
        rows = self._rows()
        manifest = _manifest(rows)
        costs = self._costs(rows)

        estimate = build_opportunity_payload(
            rows,
            manifest=manifest,
            cost_components=costs,
            opportunity_class="estimate",
            now=NOW,
        )
        unavailable = build_opportunity_payload(
            rows,
            manifest=manifest,
            cost_components=costs,
            opportunity_class="all",
            availability="unavailable",
            now=NOW,
        )

        self.assertEqual(
            [row["opportunity_class"] for row in estimate["routes"]],
            ["research_estimate"],
        )
        self.assertEqual(
            [row["opportunity_class"] for row in unavailable["routes"]],
            ["unavailable"],
        )
        self.assertEqual(
            unavailable["routes"][0]["availability"]["reason"],
            "buy_leg_unavailable",
        )

    def test_null_sort_values_are_last_for_both_directions(self):
        rows = self._rows()
        costs = self._costs(rows)

        for direction in ("asc", "desc"):
            with self.subTest(direction=direction):
                payload = build_opportunity_payload(
                    rows,
                    manifest=_manifest(rows),
                    cost_components=costs,
                    opportunity_class="all",
                    sort="net_edge_usd",
                    direction=direction,
                    now=NOW,
                )
                self.assertEqual(payload["routes"][-1]["route_id"], "route:c")

    def test_canonical_route_id_breaks_numeric_sort_ties(self):
        first = _row("route:z", "opportunity:z", strict_net_edge_usd="180")
        second = _row("route:a", "opportunity:a", strict_net_edge_usd="180")
        rows = [first, second]

        payload = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            cost_components=self._costs(rows),
            sort="net_edge_usd",
            direction="desc",
            now=NOW,
        )

        self.assertEqual(
            [row["route_id"] for row in payload["routes"]],
            ["route:a", "route:z"],
        )

    def test_stale_strict_route_retains_identity_but_not_numeric_rank(self):
        rows = [_row("route:a", "opportunity:a")]

        payload = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            cost_components=self._costs(rows),
            opportunity_class="strict",
            availability="unavailable",
            now=datetime(2026, 8, 1, 12, 3, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(len(payload["routes"]), 1)
        route = payload["routes"][0]
        self.assertEqual(route["opportunity_class"], "executable_candidate")
        self.assertEqual(route["availability"]["reason"], "cohort_stale")
        self.assertIsNone(route["net_edge_usd"])
        self.assertIsNone(route["net_edge_bps"])
        self.assertIsNone(route["gross_edge_usd"])
        self.assertIsNone(route["gross_edge_bps"])
        self.assertEqual(route["cost_breakdown"], {
            "strict_nonembedded_usd": None,
            "research_bounded_usd": None,
            "research_assumed_usd": None,
        })
        self.assertTrue(route["cost_components"])
        self.assertTrue(all(
            component["amount_usd"] is None
            and component["rate_bps"] is None
            for component in route["cost_components"]
        ))
        self.assertIsNone(route["target_token_quantity"])
        self.assertIsNone(route["capacity_quantity"])
        self.assertEqual(route["route_age_seconds"], 121.0)

    def test_unavailable_filter_keeps_deadline_for_current_rows_it_omits(self):
        rows = [_row("route:a", "opportunity:a")]
        manifest = _manifest(rows)
        costs = self._costs(rows)

        before_deadline = build_opportunity_payload(
            rows,
            manifest=manifest,
            cost_components=costs,
            availability="unavailable",
            now=datetime(
                2026, 8, 1, 12, 2, 59, 900_000, tzinfo=timezone.utc
            ),
        )
        after_deadline = build_opportunity_payload(
            rows,
            manifest=manifest,
            cost_components=costs,
            availability="unavailable",
            now=datetime(
                2026, 8, 1, 12, 3, 0, 100_000, tzinfo=timezone.utc
            ),
        )

        self.assertEqual(before_deadline["routes"], [])
        self.assertEqual(
            before_deadline["metadata"]["next_freshness_deadline_at"],
            "2026-08-01T12:03:00+00:00",
        )
        self.assertEqual(len(after_deadline["routes"]), 1)
        self.assertEqual(
            after_deadline["routes"][0]["availability"]["reason"],
            "cohort_stale",
        )
        self.assertIsNone(
            after_deadline["metadata"]["next_freshness_deadline_at"]
        )

    def test_cost_expiry_is_exclusive_and_clears_route_economics(self):
        row = _row("route:a", "opportunity:a")
        costs = self._costs([row])
        for component in costs:
            if component["component_type"] == "venue_taker_fee":
                component["observed_at"] = "2026-08-01T12:00:00Z"
                component["valid_until"] = (
                    "2026-08-01T12:02:00.500000Z"
                )

        before = build_opportunity_payload(
            [row],
            manifest=_manifest([row]),
            cost_components=costs,
            now=datetime(
                2026, 8, 1, 12, 2, 0, 499_999, tzinfo=timezone.utc
            ),
        )
        expired = build_opportunity_payload(
            [row],
            manifest=_manifest([row]),
            cost_components=costs,
            now=datetime(
                2026, 8, 1, 12, 2, 0, 500_000, tzinfo=timezone.utc
            ),
        )

        self.assertEqual(
            before["metadata"]["next_freshness_deadline_at"],
            "2026-08-01T12:02:00.500000+00:00",
        )
        self.assertIs(
            before["metadata"]["next_freshness_deadline_exclusive"],
            True,
        )
        self.assertEqual(
            before["routes"][0]["availability"],
            {"status": "available", "reason": None},
        )
        route = expired["routes"][0]
        self.assertEqual(
            route["availability"],
            {"status": "unavailable", "reason": "cost_component_stale"},
        )
        self.assertIsNone(route["gross_edge_usd"])
        self.assertIsNone(route["net_edge_usd"])
        self.assertTrue(all(
            component["amount_usd"] is None
            and component["rate_bps"] is None
            for component in route["cost_components"]
        ))
        self.assertEqual(
            expired["metadata"]["next_freshness_deadline_at"],
            "2026-08-01T12:03:00+00:00",
        )
        self.assertIs(
            expired["metadata"]["next_freshness_deadline_exclusive"],
            False,
        )

    def test_stored_unavailable_route_has_no_economic_numeric_residue(self):
        row = self._rows()[-1]

        payload = build_opportunity_payload(
            [row],
            manifest=_manifest([row]),
            cost_components=self._costs([row]),
            availability="unavailable",
            now=NOW,
        )

        route = payload["routes"][0]
        self.assertEqual(route["route_id"], "route:c")
        self.assertEqual(route["availability"]["reason"], "buy_leg_unavailable")
        self.assertIsNone(route["gross_edge_usd"])
        self.assertIsNone(route["gross_edge_bps"])
        self.assertIsNone(route["net_edge_usd"])
        self.assertIsNone(route["net_edge_bps"])
        self.assertIsNone(route["target_token_quantity"])
        self.assertIsNone(route["capacity_quantity"])
        self.assertTrue(all(
            value is None for value in route["cost_breakdown"].values()
        ))
        self.assertTrue(all(
            component["amount_usd"] is None
            and component["rate_bps"] is None
            for component in route["cost_components"]
        ))

    def test_strict_net_edge_must_be_finite_and_positive(self):
        for value in ("Infinity", "NaN", "0", "-1"):
            with self.subTest(value=value):
                row = _row(
                    "route:a",
                    "opportunity:a",
                    strict_net_edge_usd=value,
                )
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [row],
                        manifest=_manifest([row]),
                        cost_components=self._costs([row]),
                        now=NOW,
                    )

    def test_strict_gross_cost_and_net_arithmetic_must_recompute(self):
        mutations = {
            "gross edge": ("gross_edge_usd", "201"),
            "strict cost": ("strict_nonembedded_cost_usd", "19"),
            "strict net": ("strict_net_edge_usd", "179"),
        }
        for label, (field, value) in mutations.items():
            with self.subTest(label=label):
                row = _row("route:a", "opportunity:a")
                row[field] = value
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [row],
                        manifest=_manifest([row]),
                        cost_components=self._costs([row]),
                        now=NOW,
                    )

    def test_strict_usd_edges_and_bps_must_recompute(self):
        for field in ("gross_edge_bps", "strict_net_edge_bps"):
            with self.subTest(field=field):
                row = _row("route:a", "opportunity:a")
                row[field] = "999"
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [row],
                        manifest=_manifest([row]),
                        cost_components=self._costs([row]),
                        now=NOW,
                    )

    def test_strict_completeness_and_component_inventory_fail_closed(self):
        row = _row("route:a", "opportunity:a")
        cases = []
        incomplete = copy.deepcopy(row)
        incomplete["cost_completeness"] = "incomplete"
        cases.append(("cost completeness", incomplete, self._costs([incomplete])))
        scenario_incomplete = copy.deepcopy(row)
        scenario_incomplete["scenario_cost_completeness"] = "incomplete"
        cases.append((
            "scenario completeness",
            scenario_incomplete,
            self._costs([scenario_incomplete]),
        ))
        missing = self._costs([row])[:-1]
        cases.append(("missing component", row, missing))
        duplicate = self._costs([row])
        duplicate.append(copy.deepcopy(duplicate[0]))
        cases.append(("duplicate component", row, duplicate))
        wrong_total = self._costs([row])
        wrong_total[0]["amount_usd"] = "11"
        wrong_total[0]["rate_bps"] = "11"
        cases.append(("component total", row, wrong_total))

        for label, candidate, costs in cases:
            with self.subTest(label=label):
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [candidate],
                        manifest=_manifest([candidate]),
                        cost_components=costs,
                        now=NOW,
                    )

    def test_available_research_economics_must_be_finite_and_recompute(self):
        mutations = {
            "gross finite": ("gross_edge_usd", "Infinity"),
            "research finite": ("research_net_edge_usd", "Infinity"),
            "bounded cost finite": ("research_bounded_cost_usd", "Infinity"),
            "capacity finite": ("maximum_proved_capacity_quantity", "Infinity"),
            "gross arithmetic": ("gross_edge_usd", "201"),
            "research arithmetic": ("research_net_edge_usd", "174"),
            "gross bps": ("gross_edge_bps", "999"),
            "research bps": ("research_net_edge_bps", "999"),
        }
        for label, (field, value) in mutations.items():
            with self.subTest(label=label):
                row = self._rows()[1]
                row[field] = value
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [row],
                        manifest=_manifest([row]),
                        cost_components=self._costs([row]),
                        now=NOW,
                    )

    def test_available_research_capacity_may_be_absent(self):
        row = self._rows()[1]
        row["maximum_proved_capacity_quantity"] = None

        payload = build_opportunity_payload(
            [row],
            manifest=_manifest([row]),
            cost_components=self._costs([row]),
            now=NOW,
        )

        self.assertEqual(payload["routes"][0]["availability"]["status"], "available")
        self.assertIsNone(payload["routes"][0]["capacity_quantity"])

    def test_available_empty_is_not_mislabeled_as_missing_bundle(self):
        rows = self._rows()

        available_empty = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            cost_components=self._costs(rows),
            token="UNI",
            now=NOW,
        )
        missing = build_unavailable_opportunity_payload(
            reason="complete_pointer_absent",
            token="UNI",
        )

        self.assertEqual(available_empty["availability"]["status"], "available")
        self.assertEqual(available_empty["routes"], [])
        self.assertEqual(missing["availability"]["status"], "unavailable")
        self.assertEqual(missing["availability"]["reason"], "complete_pointer_absent")

    def test_extreme_notional_is_a_stable_bounded_query_error(self):
        for value in ("1e1000000", "1e-1000000", "9" * 1000):
            with self.subTest(value=value[:32]):
                with self.assertRaises(OpportunityQueryError):
                    build_unavailable_opportunity_payload(
                        notional_usd=value,
                    )

    def test_compact_projection_has_costs_timestamps_capacity_and_safe_sources(self):
        rows = [_row("route:a", "opportunity:a")]

        payload = build_opportunity_payload(
            rows,
            manifest=_manifest(rows),
            legs=LEGS,
            cost_components=self._costs(rows),
            now=NOW,
        )

        route = payload["routes"][0]
        self.assertEqual(route["cost_breakdown"]["strict_nonembedded_usd"], "20")
        self.assertEqual(
            route["cost_components"][0]["component_type"],
            "venue_taker_fee",
        )
        self.assertIs(route["cost_components"][0]["strict_eligible"], True)
        self.assertIs(
            route["cost_components"][0]["embedded_in_leg_quote"],
            False,
        )
        self.assertIs(
            route["cost_components"][0]["reflected_or_embedded"],
            False,
        )
        self.assertEqual(route["leg_timestamps"]["buy"], "2026-08-01T12:01:00Z")
        self.assertEqual(route["capacity_quantity"], "250")
        self.assertEqual(route["source_links"], [
            {
                "market_id": "cex:binance:AAVE/USDT",
                "url": "https://api.binance.com",
            },
            {
                "market_id": "cex:bybit:AAVE/USDT",
                "url": "https://api.bybit.com",
            },
        ])
        serialized = json.dumps(payload).lower()
        self.assertNotIn("orderbook", serialized)
        self.assertNotIn("account", serialized)
        self.assertNotIn("raw_response", serialized)

    def test_public_component_marks_route_level_reflected_costs(self):
        row = self._rows()[2]
        row["reflected_or_embedded_component_keys"] = [
            "buy:venue_taker_fee"
        ]

        payload = build_opportunity_payload(
            [row],
            manifest=_manifest([row]),
            cost_components=self._costs([row]),
            now=NOW,
        )

        component = next(
            item
            for item in payload["routes"][0]["cost_components"]
            if item["leg"] == "buy"
            and item["component_type"] == "venue_taker_fee"
        )
        self.assertIs(component["embedded_in_leg_quote"], False)
        self.assertIs(component["reflected_or_embedded"], True)

    def test_source_evidence_rejects_non_public_origins_without_dropping_identity(self):
        row = _row("route:a", "opportunity:a")
        unsafe_origins = (
            "http://api.exchange.example/orderbook",
            "https://127.0.0.1:8545/orderbook",
            "https://10.0.0.5:8545/orderbook",
            "https://169.254.169.254/latest/meta-data",
            "https://orderbook/internal",
            "https://rpc.company.corp/orderbook",
            "https://api.exchange.example/orderbook",
            "https://metadata.google.internal/computeMetadata/v1",
            "https://api.binance.com:8443/api/v3/depth",
            None,
        )

        for endpoint in unsafe_origins:
            with self.subTest(endpoint=endpoint):
                legs = copy.deepcopy(LEGS)
                legs[0]["source_endpoint"] = endpoint
                payload = build_opportunity_payload(
                    [row],
                    manifest=_manifest([row]),
                    legs=legs,
                    cost_components=self._costs([row]),
                    now=NOW,
                )

                self.assertEqual(payload["routes"][0]["source_links"][0], {
                    "market_id": "cex:binance:AAVE/USDT",
                    "url": None,
                })
                self.assertEqual(
                    payload["routes"][0]["source_links"][1]["url"],
                    "https://api.bybit.com",
                )

    def test_duplicate_route_scenario_identity_fails_closed(self):
        row = _row("route:a", "opportunity:a")
        rows = [row, copy.deepcopy(row)]

        with self.assertRaisesRegex(
            OpportunityBundleInvalid,
            "opportunity_bundle_validation_failed",
        ):
            build_opportunity_payload(
                rows,
                manifest=_manifest(rows),
                cost_components=[_cost("opportunity:a")],
                now=NOW,
            )

    def test_manifest_route_opportunity_and_class_counts_must_match_inventory(self):
        row = _row("route:a", "opportunity:a")
        mutations = (
            ("routes", lambda manifest: manifest["counts"].__setitem__(
                "routes", 2
            )),
            ("opportunities", lambda manifest: manifest["counts"].__setitem__(
                "opportunities", 2
            )),
            ("classification", lambda manifest: manifest["counts"][
                "classification"
            ].__setitem__("strict", 2)),
        )

        for label, mutate in mutations:
            with self.subTest(label=label):
                manifest = _manifest([row])
                mutate(manifest)
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [row],
                        manifest=manifest,
                        cost_components=self._costs([row]),
                        now=NOW,
                    )

    def test_unknown_classification_fails_closed(self):
        row = _row("route:a", "opportunity:a")
        row["opportunity_class"] = "maybe_executable"

        with self.assertRaises(OpportunityBundleInvalid):
            build_opportunity_payload(
                [row],
                manifest=_manifest([row]),
                cost_components=[_cost("opportunity:a")],
                now=NOW,
            )

    def test_assumed_cost_cannot_enter_strict_inventory(self):
        row = _row("route:a", "opportunity:a")

        with self.assertRaises(OpportunityBundleInvalid):
            build_opportunity_payload(
                [row],
                manifest=_manifest([row]),
                cost_components=[
                    _cost("opportunity:a", value_status="assumed")
                ],
                now=NOW,
            )

    def test_missing_or_orphan_cost_inventory_fails_closed(self):
        row = _row("route:a", "opportunity:a")

        for costs in ([], [_cost("opportunity:orphan")]):
            with self.subTest(costs=costs):
                with self.assertRaises(OpportunityBundleInvalid):
                    build_opportunity_payload(
                        [row],
                        manifest=_manifest([row]),
                        cost_components=costs,
                        now=NOW,
                    )


class OpportunityServerTests(unittest.TestCase):
    def _publish(self):
        return OpportunityBundleReaderTests._publish(self)

    def test_query_normalization_is_bounded_and_unknown_keys_do_not_own_cache(self):
        query = {
            "token": [" aave "],
            "venue": [" KRAKEN "],
            "notional": ["10000.0"],
            "class": [" STRICT "],
            "route_type": [" CEX_CEX "],
            "availability": [" AVAILABLE "],
            "sort": [" NET_EDGE_USD "],
            "dir": [" DESC "],
            "cache_buster": ["ignored-unbounded-value"],
        }

        self.assertEqual(
            server.public_api_query_items("opportunities", query),
            (
                ("token", "AAVE"),
                ("venue", "kraken"),
                ("notional", "10000"),
                ("class", "strict"),
                ("route_type", "cex_cex"),
                ("availability", "available"),
                ("sort", "net_edge_usd"),
                ("dir", "desc"),
            ),
        )

    def test_invalid_query_enums_and_numbers_are_bounded_client_errors(self):
        invalid = (
            {"token": ["AAVE/../../private"]},
            {"venue": ["kraken/binance"]},
            {"venue": ["all"]},
            {"notional": ["nan"]},
            {"notional": ["-1"]},
            {"notional": ["123"]},
            {"class": ["guaranteed"]},
            {"route_type": ["cex_wallet"]},
            {"availability": ["maybe"]},
            {"sort": ["private_account_balance"]},
            {"dir": ["sideways"]},
        )
        for query in invalid:
            with self.subTest(query=query):
                with self.assertRaises(server.PublicClientRequestError):
                    server.public_api_query_items("opportunities", query)

    def test_uncollected_notional_is_a_client_error_not_available_empty(self):
        root = self._publish()
        with patch.dict(
            server.os.environ,
            {"MARKET_ROUTE_DATA_DIR": str(root)},
            clear=True,
        ):
            with self.assertRaises(server.PublicClientRequestError) as raised:
                server._build_public_api_payload(
                    "opportunities",
                    (("notional", "123"),),
                )

        self.assertIn("collected", str(raised.exception))
        self.assertNotIn(str(root), str(raised.exception))

    def test_missing_pointer_is_http_200_with_stable_unavailable_payload(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "routes"
        root.mkdir()
        signature = (("route-pointer", "missing"),)
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/api/markets/opportunities?token=AAVE&class=strict"
        handler.headers = {}

        server.clear_runtime_caches()
        try:
            with patch.dict(
                server.os.environ,
                {"MARKET_ROUTE_DATA_DIR": str(root)},
                clear=True,
            ), patch.object(
                server,
                "api_source_signature",
                return_value=signature,
            ), patch.object(
                server,
                "api_freshness_bucket",
                return_value=100,
            ), patch.object(
                server.MarketMonitorHandler,
                "send_encoded_json",
            ) as send_encoded, patch.object(
                server.MarketMonitorHandler,
                "send_json",
            ) as send_json:
                handler.do_GET()
        finally:
            server.clear_runtime_caches()

        send_json.assert_not_called()
        body, compressed = send_encoded.call_args.args
        self.assertFalse(compressed)
        payload = json.loads(body)
        self.assertEqual(payload["availability"], {
            "status": "unavailable",
            "reason": "complete_pointer_absent",
        })
        self.assertEqual(payload["routes"], [])

    def test_missing_pointer_is_http_200_when_market_facts_are_unavailable(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "routes"
        root.mkdir()
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/api/markets/opportunities?token=AAVE&class=strict"
        handler.headers = {}

        server.clear_runtime_caches()
        try:
            with patch.dict(
                server.os.environ,
                {"MARKET_ROUTE_DATA_DIR": str(root)},
                clear=True,
            ), patch.object(
                server,
                "resolve_database_path",
                return_value=None,
            ), patch.object(
                server,
                "resolve_data_paths",
                side_effect=FileNotFoundError("market facts unavailable"),
            ), patch.object(
                server,
                "api_freshness_bucket",
                return_value=100,
            ), patch.object(
                server.MarketMonitorHandler,
                "send_encoded_json",
            ) as send_encoded, patch.object(
                server.MarketMonitorHandler,
                "send_json",
            ) as send_json:
                handler.do_GET()
        finally:
            server.clear_runtime_caches()

        send_json.assert_not_called()
        body, compressed = send_encoded.call_args.args
        self.assertFalse(compressed)
        payload = json.loads(body)
        self.assertEqual(payload["availability"], {
            "status": "unavailable",
            "reason": "complete_pointer_absent",
        })
        self.assertEqual(payload["routes"], [])

    def test_corrupt_publication_is_fixed_http_503_without_private_details(self):
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/api/markets/opportunities"
        private_error = OpportunityBundleInvalid(
            "opportunity_bundle_validation_failed"
        )
        with patch.object(
            server.MarketMonitorHandler,
            "send_public_api",
            side_effect=private_error,
        ), patch.object(
            server.MarketMonitorHandler,
            "send_json",
        ) as send_json:
            handler.do_GET()

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

    def test_opportunity_rebuilds_after_route_generation_changes_once(self):
        old_signature = (("routes/latest.json", 1, 10, 11, 12),)
        new_signature = (("routes/latest.json", 2, 10, 11, 12),)
        old_payload = {
            "metadata": {"data_generation": "old-generation"},
            "routes": [],
        }
        new_payload = {
            "metadata": {"data_generation": "new-generation"},
            "routes": [],
        }

        with patch.object(
            server,
            "route_source_signature",
            side_effect=(
                old_signature,
                old_signature,
                new_signature,
                new_signature,
                new_signature,
                new_signature,
            ),
        ), patch.object(
            server,
            "_build_public_api_payload",
            side_effect=(old_payload, new_payload),
        ) as build_payload:
            body, compressed = server.build_public_api_response(
                "opportunities",
                (),
                False,
            )

        self.assertFalse(compressed)
        self.assertEqual(
            json.loads(body)["metadata"]["data_generation"],
            "new-generation",
        )
        self.assertEqual(
            [
                invocation.kwargs["source_signature"]
                for invocation in build_payload.call_args_list
            ],
            [old_signature, new_signature],
        )

    def test_opportunity_fails_closed_after_three_route_generation_changes(self):
        signatures = tuple(
            (("routes/latest.json", value, 10, 11, 12),)
            for value in range(1, 7)
        )
        payload = {"metadata": {}, "routes": []}

        with patch.object(
            server,
            "route_source_signature",
            side_effect=signatures,
        ), patch.object(
            server,
            "_build_public_api_payload",
            return_value=payload,
        ) as build_payload:
            with self.assertRaises(server.SourceGenerationChanged):
                server.build_public_api_response("opportunities", (), False)

        self.assertEqual(build_payload.call_count, 3)

    def test_opportunity_generation_failures_are_fixed_validation_503(self):
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/api/markets/opportunities"
        expected = {
            "code": "opportunity_bundle_validation_failed",
            "message": (
                "Published route opportunity data failed validation. "
                "Retry after the next complete publication."
            ),
        }
        for error in (
            server.SourceGenerationChanged(),
            server.OpportunityResponseUnstable("route generation unstable"),
        ):
            with self.subTest(error=type(error).__name__), patch.object(
                server.MarketMonitorHandler,
                "send_public_api",
                side_effect=error,
            ), patch.object(
                server.MarketMonitorHandler,
                "send_json",
            ) as send_json:
                handler.do_GET()

            send_json.assert_called_once_with(expected, 503)

    def test_source_signature_tracks_pointer_manifest_and_complete_members(self):
        root = self._publish()
        pointer = json.loads((root / "latest.json").read_text(encoding="utf-8"))
        manifest_path = (
            root / "bundles" / pointer["route_cohort_id"] / "manifest.json"
        )

        with patch.dict(
            server.os.environ,
            {"MARKET_ROUTE_DATA_DIR": str(root)},
            clear=True,
        ):
            first = server.route_source_signature()
            pointer_path = root / "latest.json"
            self.assertTrue(any(item[0] == str(pointer_path) for item in first))
            self.assertTrue(any(item[0] == str(manifest_path) for item in first))
            manifest_path.touch()
            second = server.route_source_signature()

        self.assertNotEqual(first, second)

    def test_api_source_signature_contains_route_generation(self):
        route_signature = (("routes/latest.json", 11, 22, 33, 44),)
        base_signature = (("facts.sqlite3", 1, 2, 3, 4),)
        with patch.object(
            server,
            "resolve_database_path",
            return_value=Path("/ignored/facts.sqlite3"),
        ), patch.object(
            server,
            "data_signature",
            return_value=base_signature,
        ), patch.object(
            server,
            "resolve_tvl_path",
            return_value=None,
        ), patch.object(
            server,
            "resolve_cex_depth_path",
            return_value=None,
        ), patch.object(
            server,
            "resolve_dex_depth_path",
            return_value=None,
        ), patch.object(
            server,
            "resolve_cex_execution_cost_path",
            return_value=None,
        ), patch.object(
            server,
            "resolve_dex_execution_cost_path",
            return_value=None,
        ), patch.object(
            server,
            "resolve_daily_quality_report_path",
            return_value=None,
        ), patch.object(
            server,
            "resolve_cex_instrument_lifecycle_path",
            return_value=Path("/ignored/lifecycle.json"),
        ), patch.object(
            server,
            "resolve_runtime_token_registry_path",
            return_value=Path("/ignored/registry.json"),
        ), patch.object(
            server,
            "_safe_path_signature",
            return_value=(),
        ), patch.object(
            server,
            "event_source_signature",
            return_value=(),
        ), patch.object(
            server,
            "route_source_signature",
            return_value=route_signature,
        ):
            result = server.api_source_signature()

        self.assertEqual(result, base_signature + route_signature)

    def test_opportunity_response_rejects_generation_change_after_build(self):
        signature_a = (("routes/latest.json", 1, 100),)
        signature_b = (("routes/latest.json", 2, 100),)
        payload = build_unavailable_opportunity_payload()
        server._build_public_api_response_cached.cache_clear()
        with patch.object(
            server,
            "_build_public_api_payload",
            return_value=payload,
        ), patch.object(
            server,
            "api_source_signature",
            return_value=signature_b,
        ):
            with self.assertRaises(server.SourceGenerationChanged):
                server._build_public_api_response_cached(
                    "opportunities",
                    (),
                    signature_a,
                    100,
                )

    def test_health_exposes_missing_current_stale_and_invalid_publication(self):
        base_payload = {
            "metadata": {
                "storage": {"engine": "sqlite"},
                "freshness": {"overall_status": "current"},
                "cex_instrument_lifecycle": {
                    "absence_market_count": 1,
                    "applied_market_count": 1,
                    "stale_evidence_market_count": 0,
                },
            }
        }
        for status in ("missing", "current", "stale", "invalid"):
            with self.subTest(status=status):
                handler = object.__new__(server.MarketMonitorHandler)
                handler.path = "/health"
                route_health = {
                    "status": status,
                    "reason": (
                        None if status == "current" else
                        "complete_pointer_absent" if status == "missing" else
                        "cohort_stale" if status == "stale" else
                        "opportunity_bundle_validation_failed"
                    ),
                }
                with patch.object(
                    server,
                    "build_market_payload",
                    return_value=base_payload,
                ), patch.object(
                    server,
                    "opportunity_publication_health",
                    return_value=route_health,
                ), patch.object(
                    server.MarketMonitorHandler,
                    "send_json",
                ) as send_json:
                    handler.do_GET()

                self.assertEqual(
                    send_json.call_args.args[0]["route_opportunities"],
                    route_health,
                )


if __name__ == "__main__":
    unittest.main()
