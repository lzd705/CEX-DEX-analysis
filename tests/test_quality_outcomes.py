import unittest

from scripts.quality_outcomes import quality_outcome_rule


class QualityOutcomeRuleTest(unittest.TestCase):
    def test_allowlisted_outcomes_have_exact_resolution_semantics(self):
        cases = {
            ("observed", "observed"): (False, True, "observed"),
            ("partial", "source_level_limit"): (False, True, "partial"),
            ("source_no_observation", "no_candles"): (
                False, True, "confirmed_absence"
            ),
            ("source_no_observation", "source_no_two_sided_book"): (
                False, True, "confirmed_absence"
            ),
            ("unsupported", "unsupported_chain"): (
                False, True, "confirmed_unsupported"
            ),
            ("needs_review", "not_listed"): (
                False, False, "manual_review"
            ),
            ("needs_review", "daily_quality_outcome_invalid"): (
                False, False, "manual_review"
            ),
            ("collection_failed", "network"): (
                True, False, "retry_open"
            ),
            ("backfill_pending", "missing_unexplained"): (
                True, False, "retry_open"
            ),
            ("invalid", "invalid_positive_ohlc"): (
                False, False, "blocked_invalid"
            ),
        }
        for pair, expected in cases.items():
            with self.subTest(pair=pair):
                rule = quality_outcome_rule(*pair)
                self.assertIsNotNone(rule)
                self.assertEqual(
                    (rule.retryable, rule.terminal, rule.resolution),
                    expected,
                )

    def test_unknown_status_reason_pairs_fail_closed(self):
        for pair in (
            ("observed", "unknown"),
            ("partial", "unknown"),
            ("needs_review", "source_range_unavailable"),
            ("unsupported", "network"),
        ):
            with self.subTest(pair=pair):
                self.assertIsNone(quality_outcome_rule(*pair))


if __name__ == "__main__":
    unittest.main()
