"""Tests for deterministic prepublication Shadow audit metrics."""

from __future__ import annotations

import copy
from decimal import Decimal, Inexact, localcontext
import hashlib
import json
import unittest

from scripts.route_shadow_audit import (
    AUDIT_FIELDS,
    IMPLICIT_CANARY_PHASE_SHA256,
    ROUTE_SHADOW_AUDIT_SCHEMA,
    RouteShadowAuditError,
    _ratio_metric,
    build_shadow_audit,
    nearest_rank,
    validate_shadow_audit,
)


CANDIDATE_GENERATION = "a" * 64


def _canonical_sha256(value):
    return hashlib.sha256(json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _rehash(cohort):
    value = copy.deepcopy(cohort)
    for field, key in (
        ("routes", "route_id"),
        ("legs", "market_id"),
        ("route_rows", "route_id"),
    ):
        value[field] = sorted(value[field], key=lambda row: row[key])
    value.pop("route_cohort_id", None)
    value.pop("fingerprint", None)
    value["route_cohort_id"] = "cohort:" + _canonical_sha256(value)
    value["fingerprint"] = _canonical_sha256(value)
    return value


def _route(token_symbol, buy_market_id, sell_market_id):
    route_id = "route:{}:{}->{}:prepositioned_inventory".format(
        token_symbol, buy_market_id, sell_market_id
    )
    return {
        "token_symbol": token_symbol,
        "buy_market_id": buy_market_id,
        "sell_market_id": sell_market_id,
        "route_mode": "prepositioned_inventory",
        "route_id": route_id,
        "route_class": "candidate",
        "settlement_reason": None,
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "candidate_source_generation": CANDIDATE_GENERATION,
        "buy_reference_volume_usd": "9000",
        "sell_reference_volume_usd": "7000",
        "route_volume_usd": "7000",
        "route_volume_basis": "minimum_leg_source_horizon_usd",
    }


def _cohort():
    alpha = "cex:alpha:UNI/USDT"
    beta = "cex:beta:UNI/USDT"
    routes = [
        _route("UNI", alpha, beta),
        _route("UNI", beta, alpha),
    ]
    legs = [
        {
            "leg_id": alpha,
            "market_id": alpha,
            "market_type": "cex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:01.000000000Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://api.alpha.example/orderbook",
            "raw_response_sha256": "a" * 64,
        },
        {
            "leg_id": beta,
            "market_id": beta,
            "market_type": "cex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:02.000000000Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://api.beta.example/orderbook",
            "raw_response_sha256": "b" * 64,
        },
    ]
    route_rows = [{
        **route,
        "validated_at": "2026-08-01T12:00:03Z",
        "skew_seconds": "1.000000000",
        "timing_status": "within_sla",
        "reason_code": None,
    } for route in routes]
    return _rehash({
        "schema": "route_cohort_collection/v1",
        "candidate_source_generation": CANDIDATE_GENERATION,
        "collection_input_generation": "collection-generation-a",
        "source_state": {
            "candidate_source_generation": CANDIDATE_GENERATION,
            "collection_input_generation": "collection-generation-a",
        },
        "raw_evidence_run_id": "snapshot-a",
        "target_observed_at": "2026-08-01T12:00:00Z",
        "collection_started_at": "2026-08-01T12:00:00Z",
        "collection_completed_at": "2026-08-01T12:00:03Z",
        "collection_deadline_at": "2026-08-01T12:01:00Z",
        "skew_sla_seconds": "60",
        "route_age_sla_seconds": "120",
        "selection_window": {"start": "2026-07-25", "end": "2026-08-01"},
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "legs": legs,
        "routes": routes,
        "route_rows": route_rows,
    })


def _mixed_research_and_outside_cohort():
    cohort = _cohort()
    research_buy = "dex:eth:uniswap:0xaaa:UNI"
    research_sell = "dex:arb:uniswap:0xbbb:UNI"
    outside_buy = "cex:alpha:UNI/USDT"
    outside_sell = "cex:beta:UNI/USDT"
    research = {
        **_route("UNI", research_buy, research_sell),
        "route_mode": "research_only",
        "route_id": "route:UNI:{}->{}:research_only".format(
            research_buy, research_sell
        ),
        "route_class": "research_only",
        "settlement_reason": "unsupported_cross_chain_settlement",
    }
    outside = _route("UNI", outside_buy, outside_sell)
    cohort["collection_completed_at"] = "2026-08-01T12:01:03Z"
    cohort["collection_deadline_at"] = "2026-08-01T12:02:00Z"
    cohort["routes"] = [research, outside]
    cohort["legs"] = [
        {
            "leg_id": research_buy,
            "market_id": research_buy,
            "market_type": "dex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:01Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://rpc.example/eth",
            "raw_response_sha256": "1" * 64,
            "fixed_block_number": "100",
            "fixed_block_timestamp": "2026-08-01T11:59:59Z",
        },
        {
            "leg_id": research_sell,
            "market_id": research_sell,
            "market_type": "dex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:02Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://rpc.example/arb",
            "raw_response_sha256": "2" * 64,
            "fixed_block_number": "200",
            "fixed_block_timestamp": "2026-08-01T11:59:58Z",
        },
        {
            "leg_id": outside_buy,
            "market_id": outside_buy,
            "market_type": "cex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:00Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://api.alpha.example/orderbook",
            "raw_response_sha256": "3" * 64,
        },
        {
            "leg_id": outside_sell,
            "market_id": outside_sell,
            "market_type": "cex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:01:01Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://api.beta.example/orderbook",
            "raw_response_sha256": "4" * 64,
        },
    ]
    cohort["route_rows"] = [
        {
            **research,
            "validated_at": "2026-08-01T12:01:03Z",
            "skew_seconds": None,
            "timing_status": "unavailable",
            "reason_code": "route_mode_not_executable",
        },
        {
            **outside,
            "validated_at": "2026-08-01T12:01:03Z",
            "skew_seconds": "61",
            "timing_status": "outside_sla",
            "reason_code": "snapshot_skew_exceeded",
        },
    ]
    return _rehash(cohort)


def _core_pointer(cohort):
    return {
        "schema": "route_cohort_core_pointer/v1",
        "bundle_stage": "route_cohort_core/v1",
        "route_cohort_id": cohort["route_cohort_id"],
        "manifest_sha256": "c" * 64,
    }


def _run(**overrides):
    value = {
        "run_id": "run-001",
        "phase_state_sha256": IMPLICIT_CANARY_PHASE_SHA256,
        "phase_transition_id": None,
        "route_universe_sha256": "d" * 64,
        "baseline_manifest_sha256": "e" * 64,
        "candidate_source_generation": CANDIDATE_GENERATION,
        "route_cost_evidence_sha256": "f" * 64,
    }
    value.update(overrides)
    return value


class NearestRankTests(unittest.TestCase):
    def test_zero_one_two_and_twenty_samples_use_nearest_rank(self):
        self.assertIsNone(nearest_rank([], Decimal("0.95")))
        self.assertEqual(nearest_rank([Decimal("1.2500")], Decimal("0.95")), "1.25")
        self.assertEqual(
            nearest_rank([Decimal("1"), Decimal("2")], Decimal("0.95")),
            "2",
        )
        self.assertEqual(
            nearest_rank([Decimal(value) for value in range(1, 21)], Decimal("0.95")),
            "19",
        )

    def test_invalid_percentiles_and_values_fail_closed(self):
        for percentile in (Decimal("0"), Decimal("1.01"), Decimal("NaN")):
            with self.subTest(percentile=percentile):
                with self.assertRaises(ValueError):
                    nearest_rank([Decimal("1")], percentile)
        for value in (Decimal("NaN"), Decimal("Infinity"), Decimal("-1"), Decimal("-0")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    nearest_rank([value], Decimal("0.95"))

    def test_extreme_negative_percentile_exponent_short_circuits_to_first_rank(self):
        self.assertEqual(
            nearest_rank(
                [Decimal("3"), Decimal("1"), Decimal("2")],
                Decimal("1e-999999999"),
            ),
            "1",
        )


class RatioMetricTests(unittest.TestCase):
    def test_ratio_rounding_is_local_exact_and_canonical(self):
        with localcontext() as context:
            context.prec = 2
            context.traps[Inexact] = True
            self.assertEqual(_ratio_metric(1, 3), {
                "status": "evaluated",
                "numerator": 1,
                "denominator": 3,
                "value": "0.333333333333",
            })
            self.assertEqual(_ratio_metric(1, 2)["value"], "0.5")
            huge = 10 ** 80
            self.assertEqual(
                _ratio_metric(huge // 3, huge)["value"],
                "0.333333333333",
            )

    def test_zero_denominator_is_not_evaluated(self):
        self.assertEqual(_ratio_metric(0, 0), {
            "status": "not_evaluated",
            "numerator": 0,
            "denominator": 0,
            "value": None,
        })


class BuildShadowAuditTests(unittest.TestCase):
    def test_builds_exact_metrics_and_lineage(self):
        cohort = _cohort()
        pointer = _core_pointer(cohort)
        audit = build_shadow_audit(
            cohort,
            core_pointer=pointer,
            run=_run(),
            phase="canary",
            audit_finished_at="2026-08-01T12:00:10Z",
        )

        self.assertEqual(set(audit), {
            "schema", "run_id", "phase", "route_cohort_id",
            "phase_state_sha256", "phase_transition_id",
            "core_pointer_sha256", "core_manifest_sha256",
            "route_cost_evidence_sha256", "route_universe_sha256",
            "baseline_manifest_sha256", "candidate_source_generation",
            "audit_finished_at", "metrics",
        })
        self.assertEqual(set(audit), AUDIT_FIELDS)
        self.assertEqual(audit["schema"], ROUTE_SHADOW_AUDIT_SCHEMA)
        self.assertEqual(audit["route_cohort_id"], cohort["route_cohort_id"])
        self.assertEqual(audit["core_manifest_sha256"], "c" * 64)
        pointer_bytes = json.dumps(
            pointer, ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8") + b"\n"
        self.assertEqual(
            audit["core_pointer_sha256"], hashlib.sha256(pointer_bytes).hexdigest()
        )
        self.assertEqual(audit["metrics"], {
            "leg_availability": {
                "status": "evaluated", "numerator": 2,
                "denominator": 2, "value": "1",
            },
            "timing_availability": {
                "status": "evaluated", "numerator": 2,
                "denominator": 2, "value": "1",
            },
            "conditional_skew_sla": {
                "status": "evaluated", "numerator": 2,
                "denominator": 2, "value": "1",
            },
            "passing_skew_seconds_p95": {
                "status": "evaluated", "sample_count": 2, "value": "1",
            },
            "passing_skew_seconds_max": {
                "status": "evaluated", "sample_count": 2, "value": "1",
            },
            "route_age_seconds_p95": {
                "status": "evaluated", "sample_count": 2, "value": "9",
            },
            "route_age_seconds_max": {
                "status": "evaluated", "sample_count": 2, "value": "9",
            },
        })
        self.assertEqual(validate_shadow_audit(audit), audit)

    def test_validator_rejects_schema_metric_and_numeric_mutations(self):
        cohort = _cohort()
        audit = build_shadow_audit(
            cohort,
            core_pointer=_core_pointer(cohort),
            run=_run(),
            phase="canary",
            audit_finished_at="2026-08-01T12:00:10Z",
        )
        mutations = []
        extra = copy.deepcopy(audit)
        extra["extra"] = None
        mutations.append(extra)
        wrong_schema = copy.deepcopy(audit)
        wrong_schema["schema"] = "route_shadow_audit/v2"
        mutations.append(wrong_schema)
        extra_metric = copy.deepcopy(audit)
        extra_metric["metrics"]["venue_breakdown"] = {}
        mutations.append(extra_metric)
        wrong_ratio = copy.deepcopy(audit)
        wrong_ratio["metrics"]["leg_availability"]["value"] = "1.0"
        mutations.append(wrong_ratio)
        wrong_percentile = copy.deepcopy(audit)
        wrong_percentile["metrics"]["route_age_seconds_p95"]["value"] = "9.0"
        mutations.append(wrong_percentile)
        negative_zero = copy.deepcopy(audit)
        negative_zero["metrics"]["passing_skew_seconds_max"]["value"] = "-0"
        mutations.append(negative_zero)
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaises(ValueError):
                    validate_shadow_audit(mutation)

    def test_partial_leg_without_literal_true_is_available(self):
        cohort = _cohort()
        cohort["legs"][0]["status"] = "partial"
        cohort["legs"][0]["reason_code"] = "source_level_limit"
        cohort["legs"][0].pop("available")
        cohort = _rehash(cohort)
        audit = build_shadow_audit(
            cohort,
            core_pointer=_core_pointer(cohort),
            run=_run(),
            phase="canary",
            audit_finished_at="2026-08-01T12:00:10Z",
        )
        self.assertEqual(audit["metrics"]["leg_availability"]["numerator"], 2)

    def test_partial_cex_leg_without_reason_code_fails_closed(self):
        cohort = _cohort()
        cohort["legs"][0]["status"] = "partial"
        cohort["legs"][0].pop("available")
        cohort = _rehash(cohort)
        with self.assertRaisesRegex(
            RouteShadowAuditError,
            "CEX leg status and reason conflict",
        ):
            build_shadow_audit(
                cohort,
                core_pointer=_core_pointer(cohort),
                run=_run(),
                phase="canary",
                audit_finished_at="2026-08-01T12:00:10Z",
            )

    def test_empty_conditional_samples_are_not_evaluated(self):
        cohort = _cohort()
        for leg in cohort["legs"]:
            leg["status"] = "failed"
            leg["available"] = False
            leg["reason_code"] = "collection_failed"
            leg["state_observed_at"] = None
            leg["snapshot_id"] = None
            leg["raw_response_sha256"] = None
        for row in cohort["route_rows"]:
            row["skew_seconds"] = None
            row["timing_status"] = "unavailable"
            row["reason_code"] = "buy_leg_unavailable"
        cohort = _rehash(cohort)
        audit = build_shadow_audit(
            cohort,
            core_pointer=_core_pointer(cohort),
            run=_run(),
            phase="canary",
            audit_finished_at="2026-08-01T12:00:10Z",
        )
        self.assertEqual(audit["metrics"]["conditional_skew_sla"], {
            "status": "not_evaluated", "numerator": 0,
            "denominator": 0, "value": None,
        })
        self.assertEqual(audit["metrics"]["passing_skew_seconds_p95"], {
            "status": "not_evaluated", "sample_count": 0, "value": None,
        })
        self.assertEqual(audit["metrics"]["route_age_seconds_max"], {
            "status": "not_evaluated", "sample_count": 0, "value": None,
        })

    def test_research_age_and_outside_skew_use_distinct_denominators(self):
        cohort = _mixed_research_and_outside_cohort()
        audit = build_shadow_audit(
            cohort,
            core_pointer=_core_pointer(cohort),
            run=_run(),
            phase="canary",
            audit_finished_at="2026-08-01T12:01:10Z",
        )
        metrics = audit["metrics"]
        self.assertEqual(metrics["timing_availability"], {
            "status": "evaluated", "numerator": 1,
            "denominator": 2, "value": "0.5",
        })
        self.assertEqual(metrics["conditional_skew_sla"], {
            "status": "evaluated", "numerator": 0,
            "denominator": 1, "value": "0",
        })
        self.assertEqual(metrics["passing_skew_seconds_p95"], {
            "status": "not_evaluated", "sample_count": 0, "value": None,
        })
        self.assertEqual(metrics["passing_skew_seconds_max"], {
            "status": "not_evaluated", "sample_count": 0, "value": None,
        })
        self.assertEqual(metrics["route_age_seconds_p95"], {
            "status": "evaluated", "sample_count": 2, "value": "70",
        })
        self.assertEqual(metrics["route_age_seconds_max"], {
            "status": "evaluated", "sample_count": 2, "value": "70",
        })

        future_state = copy.deepcopy(cohort)
        future_state["legs"][0]["state_observed_at"] = "2026-08-01T12:01:11Z"
        future_state = _rehash(future_state)
        with self.assertRaises(ValueError):
            build_shadow_audit(
                future_state,
                core_pointer=_core_pointer(future_state),
                run=_run(),
                phase="canary",
                audit_finished_at="2026-08-01T12:01:10Z",
            )

    def test_fractional_route_age_is_exact_across_decimal_contexts(self):
        cohort = _cohort()
        cohort["legs"][0]["state_observed_at"] = (
            "2026-08-01T12:00:01.123456789012345678Z"
        )
        cohort["legs"][1]["state_observed_at"] = (
            "2026-08-01T12:00:02.123456789012345678Z"
        )
        for row in cohort["route_rows"]:
            row["skew_seconds"] = "1.000000000000000000"
        cohort = _rehash(cohort)
        baseline = build_shadow_audit(
            cohort,
            core_pointer=_core_pointer(cohort),
            run=_run(),
            phase="canary",
            audit_finished_at="2026-08-01T12:00:10Z",
        )
        with localcontext() as context:
            context.prec = 2
            context.traps[Inexact] = True
            stressed = build_shadow_audit(
                cohort,
                core_pointer=_core_pointer(cohort),
                run=_run(),
                phase="canary",
                audit_finished_at="2026-08-01T12:00:10Z",
            )
        expected = {
            "status": "evaluated",
            "sample_count": 2,
            "value": "8.876543210987654322",
        }
        self.assertEqual(baseline["metrics"]["route_age_seconds_p95"], expected)
        self.assertEqual(stressed["metrics"]["route_age_seconds_p95"], expected)
        self.assertEqual(stressed["metrics"], baseline["metrics"])

    def test_strict_run_pointer_phase_and_time_validation(self):
        cohort = _cohort()
        pointer = _core_pointer(cohort)
        cases = []
        extra_run = _run(extra="forbidden")
        cases.append((pointer, extra_run, "canary", "2026-08-01T12:00:10Z"))
        missing_run = _run()
        missing_run.pop("route_cost_evidence_sha256")
        cases.append((pointer, missing_run, "canary", "2026-08-01T12:00:10Z"))
        extra_pointer = dict(pointer, extra="forbidden")
        cases.append((extra_pointer, _run(), "canary", "2026-08-01T12:00:10Z"))
        cases.append((pointer, _run(), "candidate", "2026-08-01T12:00:10Z"))
        cases.append((pointer, _run(), "canary", "2026-08-01T12:00:10.1Z"))
        cases.append((pointer, _run(), "canary", "2026-08-01T12:00:02Z"))
        for case in cases:
            with self.subTest(case=case[1:]):
                with self.assertRaises(ValueError):
                    build_shadow_audit(
                        cohort,
                        core_pointer=case[0],
                        run=case[1],
                        phase=case[2],
                        audit_finished_at=case[3],
                    )

    def test_full_requires_transition_and_matching_run_phase(self):
        cohort = _cohort()
        transition = "1" * 64
        state_sha = "2" * 64
        audit = build_shadow_audit(
            cohort,
            core_pointer=_core_pointer(cohort),
            run=_run(
                phase_state_sha256=state_sha,
                phase_transition_id=transition,
            ),
            phase="full",
            audit_finished_at="2026-08-01T12:00:10Z",
        )
        self.assertEqual(audit["phase_state_sha256"], state_sha)
        self.assertEqual(audit["phase_transition_id"], transition)
        with self.assertRaises(ValueError):
            build_shadow_audit(
                cohort,
                core_pointer=_core_pointer(cohort),
                run=_run(phase_state_sha256=state_sha),
                phase="full",
                audit_finished_at="2026-08-01T12:00:10Z",
            )

    def test_legacy_generation_label_is_rejected(self):
        cohort = _cohort()
        with self.assertRaises(ValueError):
            build_shadow_audit(
                cohort,
                core_pointer=_core_pointer(cohort),
                run=_run(candidate_source_generation="candidate-generation-a"),
                phase="canary",
                audit_finished_at="2026-08-01T12:00:10Z",
            )
        audit = build_shadow_audit(
            cohort,
            core_pointer=_core_pointer(cohort),
            run=_run(),
            phase="canary",
            audit_finished_at="2026-08-01T12:00:10Z",
        )
        audit["candidate_source_generation"] = "candidate-generation-a"
        with self.assertRaises(ValueError):
            validate_shadow_audit(audit)

    def test_duplicate_missing_lineage_and_unknown_status_fail_closed(self):
        base = _cohort()
        mutations = []
        duplicate_route = copy.deepcopy(base)
        duplicate_route["routes"].append(copy.deepcopy(duplicate_route["routes"][0]))
        mutations.append(duplicate_route)
        missing_leg = copy.deepcopy(base)
        missing_leg["legs"].pop()
        mutations.append(missing_leg)
        unknown_status = copy.deepcopy(base)
        unknown_status["route_rows"][0]["timing_status"] = "stale"
        mutations.append(unknown_status)
        for cohort in mutations:
            with self.subTest(mutation=cohort):
                with self.assertRaises(ValueError):
                    build_shadow_audit(
                        cohort,
                        core_pointer=_core_pointer(base),
                        run=_run(),
                        phase="canary",
                        audit_finished_at="2026-08-01T12:00:10Z",
                    )


if __name__ == "__main__":
    unittest.main()
