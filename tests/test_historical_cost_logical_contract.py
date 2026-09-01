"""Adversarial logical-contract tests for historical Opportunity costs."""

from __future__ import annotations

import copy
import json
import unittest

import scripts.route_publication as route_publication
from scripts.route_cost_topology import HISTORICAL_ATOMIC_COMPONENT_MATRIX


_HISTORICAL_COMPONENT_INDEX = {
    (leg, component_type): index
    for index, (leg, component_type, _status, _embedded) in enumerate(
        HISTORICAL_ATOMIC_COMPONENT_MATRIX
    )
}


def _rehash_cost_binding(bundle, opportunity_id):
    rows = [
        row for row in bundle["cost_components"]
        if row["opportunity_id"] == opportunity_id
    ]
    rows.sort(key=lambda row: _HISTORICAL_COMPONENT_INDEX[
        (row["leg"], row["component_type"])
    ])
    opportunity = next(
        row for row in bundle["opportunities"]
        if row["opportunity_id"] == opportunity_id
    )
    opportunity["cost_component_set_sha256"] = (
        route_publication._canonical_input_sha256(rows)
    )
    unsigned = dict(opportunity)
    unsigned.pop("evidence_binding_sha256")
    opportunity["evidence_binding_sha256"] = (
        route_publication._opportunity_binding_sha256(unsigned)
    )


class HistoricalCostLogicalContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import scripts.historical_route_publication as publication
        from tests.test_historical_foundry_replay import (
            HistoricalOpportunityBridgeTests,
        )

        cls._fixture_helper = HistoricalOpportunityBridgeTests
        cls._fixture_state = HistoricalOpportunityBridgeTests._open_stage()
        try:
            payload = publication._build_historical_complete_payload(
                context=cls._fixture_state[3]
            )
            cls._valid_bundle = json.loads(json.dumps(
                payload["bundle"], allow_nan=False,
                ensure_ascii=False, sort_keys=True,
            ))
            route_publication._validate_complete_logical_bundle_shared(
                cls._valid_bundle, historical_atomic=True
            )
        except BaseException:
            HistoricalOpportunityBridgeTests._close_stage(*cls._fixture_state)
            cls._fixture_state = None
            raise

    @classmethod
    def tearDownClass(cls):
        if cls._fixture_state is not None:
            cls._fixture_helper._close_stage(*cls._fixture_state)
            cls._fixture_state = None

    def _bundle(self):
        return copy.deepcopy(self._valid_bundle)

    def _assert_rehashed_cost_attack_rejected(self, mutate):
        bundle = self._bundle()
        cost = bundle["cost_components"][0]
        opportunity_id = cost["opportunity_id"]
        original_costs = copy.deepcopy(bundle["cost_components"])
        mutate(bundle, cost)
        self.assertNotEqual(
            json.dumps(
                bundle["cost_components"], allow_nan=False,
                sort_keys=True, separators=(",", ":"),
            ),
            json.dumps(
                original_costs, allow_nan=False,
                sort_keys=True, separators=(",", ":"),
            ),
        )
        bundle["cost_components"].sort(key=lambda row: (
            row["opportunity_id"], row["leg"], row["component_type"],
        ))
        _rehash_cost_binding(bundle, opportunity_id)
        with self.assertRaises(route_publication.RoutePublicationError):
            route_publication._validate_complete_logical_bundle_shared(
                bundle, historical_atomic=True
            )

    def test_rejects_rehashed_notional_and_status_cost_mutations(self):
        attacks = {
            "requested_notional_usd": lambda _bundle, cost: cost.__setitem__(
                "requested_notional_usd", "999"
            ),
            "value_status": lambda _bundle, cost: cost.__setitem__(
                "value_status", "assumed"
            ),
            "embedded_in_leg_quote_type": lambda _bundle, cost: (
                cost.__setitem__(
                    "embedded_in_leg_quote",
                    int(cost["embedded_in_leg_quote"]),
                )
            ),
        }
        for label, mutate in attacks.items():
            with self.subTest(field=label):
                self._assert_rehashed_cost_attack_rejected(mutate)

    def test_rejects_rehashed_duplicate_historical_component(self):
        def duplicate(bundle, cost):
            bundle["cost_components"].append(copy.deepcopy(cost))

        self._assert_rehashed_cost_attack_rejected(duplicate)

    def test_rejects_rehashed_cost_common_field_mismatches(self):
        attacks = {
            "cohort_id": "cohort:" + "f" * 64,
            "market_id": "dex:ethereum:forged_v2:pool:UNI-WETH",
            "target_token_quantity": "1",
        }
        for field, replacement in attacks.items():
            with self.subTest(field=field):
                self._assert_rehashed_cost_attack_rejected(
                    lambda _bundle, cost, field=field, replacement=replacement:
                    cost.__setitem__(field, replacement)
                )


if __name__ == "__main__":
    unittest.main()
