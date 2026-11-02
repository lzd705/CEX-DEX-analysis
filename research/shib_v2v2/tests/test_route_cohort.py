import unittest
from decimal import Decimal

from scripts.route_cohort import (
    canonical_route_id,
    classify_route_timing,
    validate_route_cohort_rows,
)
from scripts.timestamp_contract import exact_timestamp_skew_seconds


def candidate(**overrides):
    row = {
        "token_symbol": "UNI",
        "buy_market_id": "cex:alpha:UNI/USDT",
        "sell_market_id": "dex:eth:uniswap:0xpool:UNI",
        "route_mode": "timing_only",
        "skew_sla_seconds": "60",
    }
    row.update(overrides)
    return row


def leg(leg_id, market_id, state_observed_at, **overrides):
    row = {
        "leg_id": leg_id,
        "market_id": market_id,
        "state_observed_at": state_observed_at,
        "available": True,
        "execution_adapter_status": "supported",
    }
    row.update(overrides)
    return row


class ExactTimestampContractTests(unittest.TestCase):
    def test_nanosecond_exact_sixty_second_boundary(self):
        self.assertEqual(
            exact_timestamp_skew_seconds(
                "2026-08-01T12:00:00.000000000Z",
                "2026-08-01T12:01:00.000000000Z",
            ),
            Decimal("60.000000000"),
        )

    def test_preserves_fractional_skew_across_timezone_offsets(self):
        self.assertEqual(
            exact_timestamp_skew_seconds(
                "2026-08-01T20:00:00.000000000+08:00",
                "2026-08-01T12:01:00.000000001Z",
            ),
            Decimal("60.000000001"),
        )

    def test_preserves_arbitrary_fractional_precision_past_decimal_context(self):
        left = "2026-08-01T12:00:00.0000000000000000000Z"
        right = "2026-08-01T12:01:00.0000000000000000001Z"

        self.assertEqual(
            exact_timestamp_skew_seconds(left, right),
            Decimal("60.0000000000000000001"),
        )
        result = classify_route_timing(
            candidate(),
            leg("buy-1", "cex:alpha:UNI/USDT", left),
            leg("sell-1", "dex:eth:uniswap:0xpool:UNI", right),
        )
        self.assertEqual(result["timing_status"], "outside_sla")
        self.assertEqual(result["skew_seconds"], "60.0000000000000000001")


class RouteTimingTests(unittest.TestCase):
    def test_exact_sixty_second_route_skew_is_within_sla(self):
        route = candidate()
        buy_leg = leg(
            "buy-1", route["buy_market_id"], "2026-08-01T12:00:00.000000000Z"
        )
        sell_leg = leg(
            "sell-1", route["sell_market_id"], "2026-08-01T12:01:00.000000000Z"
        )

        result = classify_route_timing(route, buy_leg, sell_leg)

        self.assertEqual(result["timing_status"], "within_sla")
        self.assertEqual(result["skew_seconds"], "60.000000000")

    def test_skew_one_nanosecond_above_sla_is_outside(self):
        route = candidate()
        buy_leg = leg(
            "buy-1", route["buy_market_id"], "2026-08-01T12:00:00.000000000Z"
        )
        sell_leg = leg(
            "sell-1", route["sell_market_id"], "2026-08-01T12:01:00.000000001Z"
        )

        result = classify_route_timing(route, buy_leg, sell_leg)

        self.assertEqual(result["timing_status"], "outside_sla")
        self.assertEqual(result["reason_code"], "snapshot_skew_exceeded")
        self.assertEqual(result["skew_seconds"], "60.000000001")

    def test_invalid_or_future_state_timestamp_is_a_stable_unavailable_result(self):
        route = candidate(validated_at="2026-08-01T12:02:00Z")
        sell_leg = leg(
            "sell-1", route["sell_market_id"], "2026-08-01T12:01:00Z"
        )
        for state_observed_at in (
            None,
            "2026-08-01T12:00:00",
            "not-a-timestamp",
            "2026-08-01T12:02:00.000000001Z",
        ):
            with self.subTest(state_observed_at=state_observed_at):
                result = classify_route_timing(
                    route,
                    leg("buy-1", route["buy_market_id"], state_observed_at),
                    sell_leg,
                )

                self.assertEqual(result["timing_status"], "unavailable")
                self.assertEqual(result["reason_code"], "invalid_state_timestamp")
                self.assertIsNone(result["skew_seconds"])
                self.assertNotIn("error", result)

    def test_route_failure_reason_priority_is_stable(self):
        route = candidate(
            route_deadline_exceeded=True,
            execution_adapter_status="unsupported",
            route_mode="research_only",
        )
        buy_leg = leg(
            "buy-1",
            route["buy_market_id"],
            "not-a-timestamp",
            available=False,
        )
        sell_leg = leg(
            "sell-1",
            route["sell_market_id"],
            "2026-08-01T12:01:01Z",
            available=False,
        )

        result = classify_route_timing(route, buy_leg, sell_leg)

        self.assertEqual(result["reason_code"], "route_deadline_exceeded")
        self.assertEqual(result["timing_status"], "unavailable")

    def test_each_route_failure_reason_uses_the_declared_priority_order(self):
        normal = candidate()
        cases = (
            (
                candidate(route_deadline_exceeded=True),
                leg("buy-1", normal["buy_market_id"], "2026-08-01T12:00:00Z"),
                leg("sell-1", normal["sell_market_id"], "2026-08-01T12:00:01Z"),
                "route_deadline_exceeded",
            ),
            (
                candidate(execution_adapter_status="unsupported"),
                leg("buy-1", normal["buy_market_id"], "2026-08-01T12:00:00Z"),
                leg("sell-1", normal["sell_market_id"], "2026-08-01T12:00:01Z"),
                "execution_adapter_unsupported",
            ),
            (
                normal,
                leg(
                    "buy-1", normal["buy_market_id"], "2026-08-01T12:00:00Z", available=False
                ),
                leg("sell-1", normal["sell_market_id"], "2026-08-01T12:00:01Z"),
                "buy_leg_unavailable",
            ),
            (
                normal,
                leg("buy-1", normal["buy_market_id"], "2026-08-01T12:00:00Z"),
                leg(
                    "sell-1", normal["sell_market_id"], "2026-08-01T12:00:01Z", available=False
                ),
                "sell_leg_unavailable",
            ),
            (
                normal,
                leg("buy-1", normal["buy_market_id"], "bad-timestamp"),
                leg("sell-1", normal["sell_market_id"], "2026-08-01T12:00:01Z"),
                "invalid_state_timestamp",
            ),
            (
                normal,
                leg("buy-1", normal["buy_market_id"], "2026-08-01T12:00:00Z"),
                leg("sell-1", normal["sell_market_id"], "2026-08-01T12:01:01Z"),
                "snapshot_skew_exceeded",
            ),
            (
                candidate(route_mode="research_only"),
                leg("buy-1", normal["buy_market_id"], "2026-08-01T12:00:00Z"),
                leg("sell-1", normal["sell_market_id"], "2026-08-01T12:00:01Z"),
                "route_mode_not_executable",
            ),
        )

        for route, buy_leg, sell_leg, expected_reason in cases:
            with self.subTest(reason=expected_reason):
                self.assertEqual(
                    classify_route_timing(route, buy_leg, sell_leg)["reason_code"],
                    expected_reason,
                )


class RouteCohortIdentityTests(unittest.TestCase):
    def test_directional_route_ids_and_duplicate_rows_fail_closed(self):
        route = candidate()
        reverse_route = candidate(
            buy_market_id=route["sell_market_id"],
            sell_market_id=route["buy_market_id"],
        )
        buy_leg = leg("buy-1", route["buy_market_id"], "2026-08-01T12:00:00Z")
        sell_leg = leg(
            "sell-1", route["sell_market_id"], "2026-08-01T12:00:01Z"
        )

        self.assertNotEqual(canonical_route_id(route), canonical_route_id(reverse_route))
        with self.assertRaisesRegex(ValueError, "duplicate route candidate"):
            validate_route_cohort_rows([route, dict(route)], [buy_leg, sell_leg])
        with self.assertRaisesRegex(ValueError, "duplicate route leg"):
            validate_route_cohort_rows(
                [route], [buy_leg, {**sell_leg, "leg_id": buy_leg["leg_id"]}]
            )

    def test_invalid_route_identity_values_fail_with_stable_value_error(self):
        route = candidate()
        invalid_values = (
            ("token_symbol", None),
            ("token_symbol", ""),
            ("token_symbol", 1),
            ("token_symbol", "uni"),
            ("buy_market_id", None),
            ("buy_market_id", ""),
            ("buy_market_id", 1),
            ("buy_market_id", " cex:alpha:UNI/USDT"),
            ("sell_market_id", None),
            ("sell_market_id", ""),
            ("sell_market_id", 1),
            ("route_mode", None),
            ("route_mode", ""),
            ("route_mode", 1),
            ("route_mode", "Timing Only"),
        )
        for field, value in invalid_values:
            with self.subTest(field=field, value=value):
                with self.assertRaisesRegex(
                    ValueError, "route candidate identity is invalid"
                ):
                    canonical_route_id(candidate(**{field: value}))

        with self.assertRaisesRegex(
            ValueError, "route candidate identity is invalid"
        ):
            canonical_route_id(
                candidate(buy_market_id=1, sell_market_id="1")
            )

    def test_unhashable_candidate_id_fails_with_stable_value_error(self):
        route = candidate(candidate_id=["candidate-1"])
        buy_leg = leg("buy-1", route["buy_market_id"], "2026-08-01T12:00:00Z")
        sell_leg = leg(
            "sell-1", route["sell_market_id"], "2026-08-01T12:00:01Z"
        )

        with self.assertRaisesRegex(ValueError, "route candidate ID is invalid"):
            validate_route_cohort_rows([route], [buy_leg, sell_leg])

    def test_public_route_entry_points_reject_same_market_candidates(self):
        route = candidate(sell_market_id="cex:alpha:UNI/USDT")
        buy_leg = leg("buy-1", route["buy_market_id"], "2026-08-01T12:00:00Z")
        sell_leg = leg(
            "sell-1", route["sell_market_id"], "2026-08-01T12:00:01Z"
        )

        with self.assertRaisesRegex(
            ValueError, "route candidate legs must be directional"
        ):
            canonical_route_id(route)
        with self.assertRaisesRegex(
            ValueError, "route candidate legs must be directional"
        ):
            classify_route_timing(route, buy_leg, sell_leg)
        with self.assertRaisesRegex(
            ValueError, "route candidate legs must be directional"
        ):
            validate_route_cohort_rows([route], [buy_leg, sell_leg])


if __name__ == "__main__":
    unittest.main()
