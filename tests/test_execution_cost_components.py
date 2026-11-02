"""Contract tests for route execution-cost component facts."""

from __future__ import annotations

import unittest
from decimal import Decimal, localcontext

from scripts.execution_cost_components import (
    COMPONENT_TYPES,
    COST_COMPONENT_CONTRACT_VERSION,
    VALUE_STATUSES,
    aggregate_cost_components,
    cost_component_row,
    validate_cost_components,
)


HASH = "a" * 64
OBSERVED_AT = "2026-08-01T12:00:00Z"
VALID_UNTIL = "2026-08-01T12:02:00Z"


def component(**overrides):
    values = {
        "cohort_id": "cohort-1",
        "opportunity_id": "route-1:10000",
        "leg": "buy",
        "market_id": "cex:alpha:UNI/USDT",
        "direction": "buy_token",
        "requested_notional_usd": Decimal("10000"),
        "target_token_quantity": Decimal("100"),
        "component_type": "venue_taker_fee",
        "value_status": "authenticated",
        "amount_usd": Decimal("10"),
        "rate_bps": Decimal("10"),
        "basis": "authenticated account taker fee on requested notional",
        "strict_eligible": True,
        "observed_at": OBSERVED_AT,
        "valid_until": VALID_UNTIL,
        "source": "redacted authenticated fee response",
        "source_record_sha256": HASH,
    }
    values.update(overrides)
    return cost_component_row(**values)


def not_applicable(component_type, *, leg="route", market_id="", direction="route"):
    return component(
        leg=leg,
        market_id=market_id,
        direction=direction,
        component_type=component_type,
        value_status="not_applicable",
        amount_usd=None,
        rate_bps=None,
        basis="route contract proves this component does not apply",
        strict_eligible=True,
        observed_at=None,
        valid_until=None,
        source="validated route contract",
        source_record_sha256=None,
    )


class CostComponentRowTests(unittest.TestCase):
    def test_normalizes_exact_decimals_and_contract_fields(self):
        row = component()

        self.assertEqual(row["contract_version"], "1")
        self.assertEqual(COST_COMPONENT_CONTRACT_VERSION, "1")
        self.assertEqual(row["requested_notional_usd"], "10000")
        self.assertEqual(row["target_token_quantity"], "100")
        self.assertEqual(row["amount_usd"], "10")
        self.assertEqual(row["rate_bps"], "10")
        self.assertIs(row["strict_eligible"], True)
        self.assertIs(row["embedded_in_leg_quote"], False)

    def test_exposes_only_the_declared_component_and_status_enums(self):
        self.assertEqual(
            COMPONENT_TYPES,
            {
                "venue_taker_fee",
                "pool_swap_fee",
                "network_gas",
                "router_or_integrator_fee",
                "token_transfer_tax",
                "rebalancing_or_transfer",
                "mev_buffer",
            },
        )
        self.assertEqual(
            VALUE_STATUSES,
            {
                "measured",
                "authenticated",
                "quoted",
                "bounded_estimate",
                "assumed",
                "not_applicable",
                "unavailable",
                "unsupported",
                "failed",
                "stale",
            },
        )
        for field, value in (
            ("component_type", "latency_guess"),
            ("value_status", "estimated"),
        ):
            with self.subTest(field=field, value=value):
                with self.assertRaisesRegex(ValueError, field):
                    component(**{field: value})

    def test_assumed_and_bounded_components_cannot_be_strict_eligible(self):
        for status in ("assumed", "bounded_estimate"):
            with self.subTest(status=status):
                with self.assertRaisesRegex(ValueError, status + ".*strict"):
                    component(
                        component_type="mev_buffer",
                        leg="route",
                        market_id="",
                        direction="route",
                        value_status=status,
                        strict_eligible=True,
                        observed_at=None,
                        valid_until=None,
                        source="user scenario",
                        source_record_sha256=None,
                    )

    def test_negative_nonfinite_and_float_values_are_rejected(self):
        for field, value in (
            ("requested_notional_usd", Decimal("0")),
            ("target_token_quantity", Decimal("-1")),
            ("amount_usd", Decimal("-0.01")),
            ("rate_bps", Decimal("NaN")),
            ("amount_usd", Decimal("Infinity")),
            ("amount_usd", 10.0),
        ):
            with self.subTest(field=field, value=value):
                with self.assertRaisesRegex(ValueError, field):
                    component(**{field: value})

    def test_amount_and_rate_recompute_exactly_from_requested_notional(self):
        row = component(amount_usd=Decimal("12.5"), rate_bps=Decimal("12.5"))
        self.assertEqual(row["amount_usd"], "12.5")
        self.assertEqual(row["rate_bps"], "12.5")

        with self.assertRaisesRegex(ValueError, "amount_usd.*rate_bps"):
            component(amount_usd=Decimal("12.5"), rate_bps=Decimal("12.5000001"))

    def test_rate_identity_rejects_context_rounded_near_match(self):
        with localcontext() as context:
            context.prec = 5
            with self.assertRaisesRegex(ValueError, "amount_usd.*rate_bps"):
                component(
                    amount_usd=Decimal("12.5"),
                    rate_bps=Decimal("12.50001"),
                )

    def test_exact_rate_identity_is_unchanged_by_low_ambient_context(self):
        with localcontext() as context:
            context.prec = 2
            row = component(
                amount_usd=Decimal("12.34567890123456789"),
                rate_bps=Decimal("12.34567890123456789"),
            )

        self.assertEqual(row["amount_usd"], "12.34567890123456789")
        self.assertEqual(row["rate_bps"], "12.34567890123456789")

    def test_numeric_components_require_both_amount_rate_and_basis(self):
        for overrides in (
            {"amount_usd": None},
            {"rate_bps": None},
            {"basis": ""},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, "amount_usd|rate_bps|basis"):
                    component(**overrides)

    def test_not_applicable_has_no_numeric_values_and_requires_proof_basis(self):
        row = not_applicable("network_gas")
        self.assertIsNone(row["amount_usd"])
        self.assertIsNone(row["rate_bps"])

        with self.assertRaisesRegex(ValueError, "not_applicable.*numeric"):
            component(
                value_status="not_applicable",
                amount_usd=Decimal("0"),
                rate_bps=Decimal("0"),
                observed_at=None,
                valid_until=None,
                source="validated route contract",
                source_record_sha256=None,
            )
        with self.assertRaisesRegex(ValueError, "not_applicable.*basis"):
            component(
                leg="route",
                market_id="",
                direction="route",
                component_type="network_gas",
                value_status="not_applicable",
                amount_usd=None,
                rate_bps=None,
                basis="",
                strict_eligible=True,
                observed_at=None,
                valid_until=None,
                source="validated route contract",
                source_record_sha256=None,
            )

    def test_terminal_statuses_are_null_non_strict_and_reasoned(self):
        for status in ("unavailable", "unsupported", "failed", "stale"):
            with self.subTest(status=status):
                row = component(
                    value_status=status,
                    amount_usd=None,
                    rate_bps=None,
                    strict_eligible=False,
                    observed_at=None,
                    valid_until=None,
                    source_record_sha256=None,
                    reason_code="cost_evidence_" + status,
                )
                self.assertIsNone(row["amount_usd"])
                self.assertIsNone(row["rate_bps"])
                with self.assertRaisesRegex(ValueError, status + ".*numeric"):
                    component(
                        value_status=status,
                        strict_eligible=False,
                        reason_code="cost_evidence_" + status,
                    )

    def test_measured_authenticated_and_quoted_values_require_lineage(self):
        cases = (
            ("measured", {"observed_at": None}),
            ("measured", {"source_record_sha256": None}),
            ("authenticated", {"valid_until": None}),
            ("quoted", {"valid_until": None}),
            ("quoted", {"source_record_sha256": "bad-hash"}),
        )
        for status, overrides in cases:
            with self.subTest(status=status, overrides=overrides):
                with self.assertRaisesRegex(
                    ValueError, "observed_at|valid_until|source_record_sha256"
                ):
                    component(value_status=status, **overrides)

    def test_validity_window_must_follow_observation(self):
        with self.assertRaisesRegex(ValueError, "valid_until.*observed_at"):
            component(valid_until=OBSERVED_AT)

    def test_pool_fee_must_be_marked_embedded_and_other_costs_cannot_be(self):
        pool = component(
            component_type="pool_swap_fee",
            value_status="measured",
            embedded_in_leg_quote=True,
            valid_until=None,
        )
        self.assertIs(pool["embedded_in_leg_quote"], True)

        with self.assertRaisesRegex(ValueError, "pool_swap_fee.*embedded"):
            component(
                component_type="pool_swap_fee",
                value_status="measured",
                valid_until=None,
            )
        with self.assertRaisesRegex(ValueError, "embedded.*pool_swap_fee"):
            component(embedded_in_leg_quote=True)

    def test_mev_buffer_rejects_strict_fact_statuses_and_not_applicable(self):
        numeric_statuses = ("measured", "authenticated", "quoted")
        for status in numeric_statuses:
            with self.subTest(status=status):
                with self.assertRaisesRegex(ValueError, "mev_buffer.*scenario"):
                    component(
                        leg="route",
                        market_id="",
                        direction="route",
                        component_type="mev_buffer",
                        value_status=status,
                        valid_until=(
                            None if status == "measured" else VALID_UNTIL
                        ),
                    )
        with self.assertRaisesRegex(ValueError, "mev_buffer.*scenario"):
            component(
                leg="route",
                market_id="",
                direction="route",
                component_type="mev_buffer",
                value_status="not_applicable",
                amount_usd=None,
                rate_bps=None,
                basis="route claims MEV does not apply",
                strict_eligible=True,
                observed_at=None,
                valid_until=None,
                source="route claim",
                source_record_sha256=None,
            )


class CostComponentValidationTests(unittest.TestCase):
    def test_all_terminal_cex_topology_is_the_only_null_target_scenario(self):
        shared = {
            "cohort_id": "cohort-terminal",
            "opportunity_id": "route-terminal:10000",
            "requested_notional_usd": Decimal("10000"),
            "target_token_quantity": None,
            "value_status": "unavailable",
            "amount_usd": None,
            "rate_bps": None,
            "basis": "retained route timing proves route unavailable",
            "strict_eligible": False,
            "observed_at": None,
            "valid_until": None,
            "source": "retained route timing",
            "source_record_sha256": None,
            "reason_code": "sell_leg_unavailable",
        }
        shapes = (
            ("buy", "cex:alpha:CAKE/USDT", "buy_token", "venue_taker_fee"),
            ("sell", "cex:beta:CAKE/USDT", "sell_token", "venue_taker_fee"),
            ("route", "", "route", "rebalancing_or_transfer"),
        )

        try:
            rows = [
                cost_component_row(
                    **shared,
                    leg=leg,
                    market_id=market_id,
                    direction=direction,
                    component_type=component_type,
                )
                for leg, market_id, direction, component_type in shapes
            ]
            validate_cost_components(rows)
        except ValueError as error:
            self.fail("all-terminal null targets were rejected: {}".format(error))

        mutations = []
        positive_status = dict(rows[0])
        positive_status["value_status"] = "authenticated"
        mutations.append(("positive status", [positive_status, *rows[1:]]))
        numeric_values = dict(rows[0])
        numeric_values.update({"amount_usd": "10", "rate_bps": "10"})
        mutations.append(("numeric values", [numeric_values, *rows[1:]]))
        strict = dict(rows[0])
        strict["strict_eligible"] = True
        mutations.append(("strict flag", [strict, *rows[1:]]))
        mixed_target = dict(rows[0])
        mixed_target["target_token_quantity"] = "100"
        mutations.append(("mixed targets", [mixed_target, *rows[1:]]))
        mixed_reason = dict(rows[0])
        mixed_reason["reason_code"] = "buy_leg_unavailable"
        mutations.append(("mixed terminal reasons", [mixed_reason, *rows[1:]]))

        for label, mutated in mutations:
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    validate_cost_components(mutated)

    def test_duplicate_fixed_grain_key_is_rejected(self):
        row = component()
        with self.assertRaisesRegex(ValueError, "duplicate cost component"):
            validate_cost_components([row, dict(row)])

    def test_validation_recomputes_serialized_rows_not_only_builder_inputs(self):
        row = {**component(), "rate_bps": "11"}
        with self.assertRaisesRegex(ValueError, "amount_usd.*rate_bps"):
            validate_cost_components([row])

    def test_schema_rejects_missing_extra_and_non_string_keys_as_value_errors(self):
        missing = dict(component())
        del missing["source"]
        extra = {**component(), "unexpected": "value"}
        non_string = dict(component())
        non_string[1] = "value"
        for label, row in (
            ("missing", missing),
            ("extra", extra),
            ("non_string", non_string),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, "schema|key"):
                    validate_cost_components([row])

    def test_unknown_leg_and_direction_are_controlled_value_errors(self):
        with self.assertRaisesRegex(ValueError, "leg"):
            component(leg="entry")
        with self.assertRaisesRegex(ValueError, "direction"):
            component(direction="hold_token")

    def test_validation_rejects_mixed_scenario_quantity_for_one_opportunity(self):
        rows = [
            component(),
            component(
                leg="sell",
                market_id="cex:beta:UNI/USDT",
                direction="sell_token",
                component_type="network_gas",
                target_token_quantity=Decimal("99"),
            ),
        ]
        with self.assertRaisesRegex(ValueError, "target_token_quantity"):
            validate_cost_components(rows)

    def test_validation_rejects_mixed_market_identity_for_one_leg(self):
        rows = [
            component(),
            component(
                component_type="network_gas",
                market_id="cex:other:UNI/USDT",
            ),
        ]
        with self.assertRaisesRegex(ValueError, "market_id.*direction"):
            validate_cost_components(rows)


class CostComponentAggregationTests(unittest.TestCase):
    def complete_strict_rows(self):
        return [
            component(),
            component(
                leg="sell",
                market_id="dex:eth:uniswap_v2:0xpool:UNI",
                direction="sell_token",
                component_type="pool_swap_fee",
                value_status="measured",
                amount_usd=Decimal("30"),
                rate_bps=Decimal("30"),
                basis="fixed-block V2 pool swap fee",
                valid_until=None,
                source="fixed-block pool-state quote",
                embedded_in_leg_quote=True,
            ),
            component(
                leg="sell",
                market_id="dex:eth:uniswap_v2:0xpool:UNI",
                direction="sell_token",
                component_type="network_gas",
                value_status="quoted",
                amount_usd=Decimal("5"),
                rate_bps=Decimal("5"),
                basis="concrete transaction gas quote",
                source="fixed-block transaction simulation",
            ),
            not_applicable("router_or_integrator_fee"),
            not_applicable("token_transfer_tax"),
            not_applicable("rebalancing_or_transfer"),
        ]

    def test_complete_strict_aggregate_excludes_embedded_pool_fee(self):
        result = aggregate_cost_components(
            self.complete_strict_rows(), include_assumptions=False
        )

        self.assertEqual(result["strict_amount_usd"], "15")
        self.assertEqual(result["scenario_amount_usd"], "15")
        self.assertEqual(result["missing_required_kinds"], [])
        self.assertEqual(result["completeness"], "complete")

    def test_aggregate_preserves_more_than_one_hundred_digits_exactly(self):
        rows = self.complete_strict_rows()
        huge = "1" + "0" * 119
        for row in rows:
            if row["component_type"] == "venue_taker_fee":
                row["amount_usd"] = huge
                row["rate_bps"] = huge
            elif row["component_type"] == "network_gas":
                row["amount_usd"] = "1"
                row["rate_bps"] = "1"

        result = aggregate_cost_components(rows, include_assumptions=False)

        self.assertEqual(result["strict_amount_usd"], str(int(huge) + 1))

    def test_aggregate_is_unchanged_by_low_ambient_decimal_context(self):
        rows = self.complete_strict_rows()
        for row in rows:
            if row["component_type"] == "venue_taker_fee":
                row["amount_usd"] = "12.345"
                row["rate_bps"] = "12.345"
            elif row["component_type"] == "network_gas":
                row["amount_usd"] = "0.006"
                row["rate_bps"] = "0.006"
        with localcontext() as context:
            context.prec = 2
            result = aggregate_cost_components(
                rows, include_assumptions=False
            )

        self.assertEqual(result["strict_amount_usd"], "12.351")

    def test_mev_buffer_is_scenario_only_and_never_changes_strict_total(self):
        rows = self.complete_strict_rows()
        rows.append(
            component(
                leg="route",
                market_id="",
                direction="route",
                component_type="mev_buffer",
                value_status="assumed",
                amount_usd=Decimal("2"),
                rate_bps=Decimal("2"),
                basis="user MEV scenario buffer",
                strict_eligible=False,
                observed_at=None,
                valid_until=None,
                source="user scenario",
                source_record_sha256=None,
            )
        )

        result = aggregate_cost_components(rows, include_assumptions=True)

        self.assertEqual(result["strict_amount_usd"], "15")
        self.assertEqual(result["scenario_amount_usd"], "17")

    def test_missing_component_stays_null_instead_of_becoming_zero(self):
        rows = [
            row
            for row in self.complete_strict_rows()
            if row["component_type"] != "network_gas"
        ]
        result = aggregate_cost_components(rows, include_assumptions=False)

        self.assertIsNone(result["strict_amount_usd"])
        self.assertIsNone(result["scenario_amount_usd"])
        self.assertEqual(result["missing_required_kinds"], ["network_gas"])
        self.assertEqual(result["completeness"], "incomplete")

    def test_assumptions_are_scenario_only_when_explicitly_included(self):
        rows = [
            row
            for row in self.complete_strict_rows()
            if row["component_type"] != "network_gas"
        ]
        rows.append(
            component(
                leg="sell",
                market_id="dex:eth:uniswap_v2:0xpool:UNI",
                direction="sell_token",
                component_type="network_gas",
                value_status="bounded_estimate",
                amount_usd=Decimal("7"),
                rate_bps=Decimal("7"),
                basis="public gas-price bound",
                strict_eligible=False,
                observed_at=None,
                valid_until=None,
                source="public schedule",
                source_record_sha256=None,
            )
        )

        excluded = aggregate_cost_components(rows, include_assumptions=False)
        included = aggregate_cost_components(rows, include_assumptions=True)

        self.assertIsNone(excluded["strict_amount_usd"])
        self.assertIsNone(excluded["scenario_amount_usd"])
        self.assertIsNone(included["strict_amount_usd"])
        self.assertEqual(included["scenario_amount_usd"], "17")
        self.assertEqual(included["scenario_completeness"], "complete")


if __name__ == "__main__":
    unittest.main()
