import json
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from scripts.publication_gate import (
    COVERAGE_GATE_LOG_MARKER,
    CoverageRegressionError,
    bind_passing_coverage_report,
    enforce_publication_coverage,
    enforce_publication_coverage_bundle,
    evaluate_publication_coverage,
    validate_passing_coverage_report,
)


def row(market_id, status="observed", venue="main"):
    return {
        "market_id": market_id,
        "status": status,
        "venue": venue,
    }


def market_identity(item):
    return item["market_id"]


class PublicationCoverageGateTest(unittest.TestCase):
    def evaluate(self, candidate, baseline=None, **kwargs):
        options = {
            "fact_family": "test_depth",
            "identity": market_identity,
            "usable_statuses": {"observed"},
        }
        options.update(kwargs)
        return evaluate_publication_coverage(candidate, baseline, **options)

    def test_first_publish_is_allowed_and_reports_missing_baseline(self):
        report = self.evaluate([row("a"), row("b")])

        self.assertEqual(report["schema"], "publication_coverage_gate/v1")
        self.assertEqual(report["status"], "passed")
        self.assertTrue(report["passed"])
        self.assertEqual(report["candidate"]["eligible_count"], 2)
        self.assertEqual(report["candidate"]["usable_count"], 2)
        self.assertEqual(report["candidate"]["usable_bps"], 10_000)
        self.assertEqual(report["comparison"]["retention_check"], "skipped")
        self.assertEqual(report["skipped_reason"], "no_baseline")

    def test_catalog_additions_and_removals_do_not_penalise_retention(self):
        baseline = [row(key) for key in ("a", "b", "c", "removed")]
        candidate = [row(key) for key in ("a", "b", "c", "added")]

        report = self.evaluate(candidate, baseline)

        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["comparison"]["common_identity_count"], 3)
        self.assertEqual(
            report["comparison"]["comparable_baseline_usable_count"], 3
        )
        self.assertEqual(report["comparison"]["retained_count"], 3)
        self.assertEqual(report["comparison"]["lost_count"], 0)
        self.assertEqual(report["comparison"]["retention_bps"], 10_000)

    def test_exact_candidate_and_baseline_boundaries_pass(self):
        with self.subTest("candidate absolute coverage at 8000 bps"):
            candidate = [row(str(index)) for index in range(4)]
            candidate.append(row("failed", "failed"))
            report = self.evaluate(candidate)
            self.assertEqual(report["candidate"]["usable_bps"], 8000)
            self.assertEqual(report["status"], "passed")

        with self.subTest("baseline retention at 9500 bps"):
            baseline = [row(str(index)) for index in range(20)]
            candidate = [
                row(str(index), "failed" if index == 19 else "observed")
                for index in range(20)
            ]
            report = self.evaluate(candidate, baseline)
            self.assertEqual(report["comparison"]["retention_bps"], 9500)
            self.assertEqual(report["status"], "passed")

    def test_baseline_retention_below_boundary_is_rejected(self):
        baseline = [row(str(index)) for index in range(100)]
        candidate = [
            row(str(index), "failed" if index < 6 else "observed")
            for index in range(100)
        ]

        report = self.evaluate(candidate, baseline)

        self.assertEqual(report["candidate"]["absolute_check"], "passed")
        self.assertEqual(report["comparison"]["retention_bps"], 9400)
        self.assertEqual(report["comparison"]["retention_check"], "rejected")
        self.assertEqual(report["status"], "rejected")

    def test_partial_can_be_configured_as_usable(self):
        candidate = [
            row("a"),
            row("b"),
            row("c"),
            row("d", "partial"),
            row("e", "failed"),
        ]

        report = self.evaluate(
            candidate, usable_statuses={"observed", "partial"}
        )

        self.assertEqual(report["candidate"]["status_counts"]["partial"], 1)
        self.assertEqual(report["candidate"]["usable_count"], 4)
        self.assertEqual(report["candidate"]["usable_bps"], 8000)
        self.assertEqual(report["status"], "passed")

    def test_dex_unsupported_is_excluded_but_prior_observed_becomes_lost(self):
        baseline = [row(str(index)) for index in range(20)]
        candidate = [
            row(str(index), "unsupported" if index < 2 else "observed")
            for index in range(20)
        ]

        report = self.evaluate(
            candidate,
            baseline,
            excluded_statuses={"unsupported"},
        )

        self.assertEqual(report["candidate"]["eligible_count"], 18)
        self.assertEqual(report["candidate"]["usable_count"], 18)
        self.assertEqual(report["candidate"]["usable_bps"], 10_000)
        self.assertEqual(report["comparison"]["retained_count"], 18)
        self.assertEqual(report["comparison"]["lost_count"], 2)
        self.assertEqual(report["comparison"]["retention_bps"], 9000)
        self.assertEqual(report["status"], "rejected")

    def test_bad_candidate_absolute_coverage_is_rejected_without_baseline(self):
        candidate = [
            row("a"),
            row("b"),
            row("c"),
            row("d", "failed"),
            row("e", "failed"),
        ]

        report = self.evaluate(candidate)

        self.assertEqual(report["candidate"]["usable_bps"], 6000)
        self.assertEqual(report["candidate"]["absolute_check"], "rejected")
        self.assertIn(
            "candidate_usable_coverage_below_threshold", report["reasons"]
        )
        self.assertEqual(report["status"], "rejected")
        self.assertEqual(report["skipped_reason"], "no_baseline")

    def test_no_eligible_candidate_and_empty_existing_baseline_fail_closed(self):
        with self.subTest("all candidate rows excluded"):
            report = self.evaluate(
                [row("a", "unsupported")],
                excluded_statuses={"unsupported"},
            )
            self.assertEqual(report["status"], "rejected")
            self.assertIn("candidate_has_no_eligible_rows", report["reasons"])

        with self.subTest("explicit structurally unsupported family"):
            report = self.evaluate(
                [row("a", "unsupported")],
                excluded_statuses={"unsupported"},
                allow_no_eligible_candidate=True,
            )
            self.assertEqual(report["status"], "passed")
            self.assertEqual(
                report["skipped_reason"],
                "no_candidate_eligible_rows_allowed;no_baseline",
            )

        with self.subTest("existing baseline is empty"):
            report = self.evaluate([row("a")], [])
            self.assertEqual(report["status"], "rejected")
            self.assertIn(
                "baseline_has_no_rows",
                {error["code"] for error in report["errors"]},
            )

    def test_duplicate_and_missing_identities_fail_closed(self):
        cases = {
            "candidate duplicate": (
                [row("a"), row("a")],
                None,
                "candidate_identity_duplicate",
            ),
            "candidate missing": (
                [row("")],
                None,
                "candidate_identity_missing",
            ),
            "baseline duplicate": (
                [row("a")],
                [row("a"), row("a")],
                "baseline_identity_duplicate",
            ),
        }
        for name, (candidate, baseline, error_code) in cases.items():
            with self.subTest(name):
                report = self.evaluate(candidate, baseline)
                self.assertEqual(report["status"], "rejected")
                self.assertIn(
                    error_code,
                    {error["code"] for error in report["errors"]},
                )

    def test_configured_status_contract_rejects_corrupt_baseline(self):
        report = self.evaluate(
            [row("a")],
            [row("a", "corrupt")],
            valid_statuses={"observed", "failed"},
        )

        self.assertEqual(report["status"], "rejected")
        self.assertIn(
            "baseline_status_invalid",
            {error["code"] for error in report["errors"]},
        )

    def test_identity_callback_error_is_rejected_and_enforce_uses_log_marker(self):
        def broken_identity(_item):
            raise KeyError("identity unavailable")

        with self.assertRaises(CoverageRegressionError) as raised:
            enforce_publication_coverage(
                [row("a")],
                None,
                fact_family="test_depth",
                identity=broken_identity,
                usable_statuses={"observed"},
            )

        message = str(raised.exception)
        self.assertTrue(message.startswith(COVERAGE_GATE_LOG_MARKER))
        compact_json = message[len(COVERAGE_GATE_LOG_MARKER) :]
        self.assertNotIn('": ', compact_json)
        self.assertEqual(json.loads(compact_json), raised.exception.report)
        self.assertEqual(raised.exception.report["status"], "rejected")

    def test_bundle_retains_passing_and_rejected_family_reports(self):
        with tempfile.TemporaryDirectory() as directory_name:
            passing = bind_passing_coverage_report(
                self.evaluate([row("good")], fact_family="good"),
                fact_family="good",
                baseline_path=Path(directory_name) / "good-latest.csv",
            )

            def reject():
                return enforce_publication_coverage(
                    [row("bad", "failed")],
                    None,
                    fact_family="bad",
                    identity=market_identity,
                    usable_statuses={"observed"},
                )

            with self.assertRaises(CoverageRegressionError) as raised:
                enforce_publication_coverage_bundle(
                    (
                        ("good", lambda: passing),
                        ("bad", reject),
                    ),
                    bundle="test_bundle",
                )

        bundle = raised.exception.report
        self.assertEqual(
            bundle["schema"],
            "publication_coverage_gate_bundle/v1",
        )
        self.assertEqual(
            set(bundle["publication_gates"]),
            {"good", "bad"},
        )
        self.assertTrue(bundle["publication_gates"]["good"]["passed"])
        self.assertFalse(bundle["publication_gates"]["bad"]["passed"])

    def test_reused_preflight_report_must_be_matching_and_passing(self):
        with tempfile.TemporaryDirectory() as directory_name:
            baseline_path = Path(directory_name) / "latest.csv"
            report = bind_passing_coverage_report(
                self.evaluate([row("a")]),
                fact_family="test_depth",
                baseline_path=baseline_path,
            )
        self.assertEqual(
            validate_passing_coverage_report(
                report,
                fact_family="test_depth",
            ),
            report,
        )
        with self.assertRaisesRegex(ValueError, "does not match"):
            validate_passing_coverage_report(
                report,
                fact_family="other_depth",
            )
        with self.assertRaisesRegex(ValueError, "not passing"):
            validate_passing_coverage_report(
                self.evaluate([row("a", "failed")]),
                fact_family="test_depth",
            )
        expected_policy = {
            "thresholds": report["thresholds"],
            "usable_statuses": report["usable_statuses"],
            "excluded_statuses": report["excluded_statuses"],
            "valid_statuses": report["valid_statuses"],
        }
        validate_passing_coverage_report(
            report,
            fact_family="test_depth",
            expected_policy=expected_policy,
        )
        with tempfile.TemporaryDirectory() as directory_name:
            other_policy_report = bind_passing_coverage_report(
                self.evaluate(
                    [row("a")],
                    minimum_candidate_usable_bps=7000,
                ),
                fact_family="test_depth",
                baseline_path=Path(directory_name) / "latest.csv",
            )
        with self.assertRaisesRegex(ValueError, "policy"):
            validate_passing_coverage_report(
                other_policy_report,
                fact_family="test_depth",
                expected_policy=expected_policy,
            )

    def test_rejected_verdict_cannot_be_changed_into_passing_report(self):
        rejected = self.evaluate([row("a", "failed")])
        forged = deepcopy(rejected)
        forged["status"] = "passed"
        forged["passed"] = True
        with self.assertRaisesRegex(ValueError, "rejection evidence"):
            validate_passing_coverage_report(
                forged,
                fact_family="test_depth",
            )

        forged["reasons"] = []
        forged["candidate"]["absolute_check"] = "passed"
        forged["comparison"]["retention_check"] = "skipped"
        with self.assertRaisesRegex(ValueError, "invalid seal"):
            validate_passing_coverage_report(
                forged,
                fact_family="test_depth",
            )

    def test_no_common_previous_success_is_allowed_and_reported(self):
        baseline = [row("old-a"), row("old-b")]
        candidate = [row("new-a"), row("new-b")]

        report = self.evaluate(candidate, baseline)

        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["comparison"]["common_identity_count"], 0)
        self.assertEqual(
            report["comparison"]["comparable_baseline_usable_count"], 0
        )
        self.assertEqual(
            report["skipped_reason"], "no_common_previous_success"
        )

    def test_cohort_regression_rejects_when_global_retention_still_passes(self):
        baseline = [
            row(
                str(index),
                venue="fragile" if index < 5 else "broad",
            )
            for index in range(100)
        ]
        candidate = [
            row(
                str(index),
                status="failed" if index < 3 else "observed",
                venue="fragile" if index < 5 else "broad",
            )
            for index in range(100)
        ]

        report = self.evaluate(
            candidate,
            baseline,
            cohort=lambda item: item["venue"],
        )

        self.assertEqual(report["comparison"]["retention_bps"], 9700)
        self.assertEqual(report["comparison"]["retention_check"], "passed")
        self.assertEqual(report["status"], "rejected")
        self.assertEqual(len(report["cohort"]["failed_checks"]), 1)
        failed = report["cohort"]["failed_checks"][0]
        self.assertEqual(failed["cohort"], "fragile")
        self.assertEqual(failed["baseline_usable_count"], 5)
        self.assertEqual(failed["lost_count"], 3)
        self.assertEqual(failed["retention_bps"], 4000)


if __name__ == "__main__":
    unittest.main()
