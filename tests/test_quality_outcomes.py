import unittest

from scripts import quality_outcomes


quality_outcome_rule = quality_outcomes.quality_outcome_rule


class QualityOutcomeRuleTest(unittest.TestCase):
    def required_helper(self, name):
        helper = getattr(quality_outcomes, name, None)
        self.assertIsNotNone(helper, f"missing public helper: {name}")
        return helper

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

    def test_family_normalizers_return_only_allowlisted_bounded_outcomes(self):
        normalize_tvl_source_outcome = self.required_helper(
            "normalize_tvl_source_outcome"
        )
        normalize_dex_depth_source_outcome = self.required_helper(
            "normalize_dex_depth_source_outcome"
        )
        normalize_execution_source_outcome = self.required_helper(
            "normalize_execution_source_outcome"
        )
        cases = (
            (
                normalize_tvl_source_outcome,
                ("failed", None, "PermissionError: /srv/private/tvl.csv"),
                ("collection_failed", "source_unavailable"),
            ),
            (
                normalize_tvl_source_outcome,
                ("missing", None, "Source returned raw private detail"),
                ("source_no_observation", "source_no_tvl_observation"),
            ),
            (
                normalize_dex_depth_source_outcome,
                ("failed", None, "RPCError: https://node.test/key/secret"),
                ("collection_failed", "source_unavailable"),
            ),
            (
                normalize_dex_depth_source_outcome,
                ("unsupported", None, "unsupported_chain:solana"),
                ("unsupported", "unsupported_chain"),
            ),
        )
        for normalizer, arguments, expected in cases:
            with self.subTest(normalizer=normalizer.__name__, arguments=arguments):
                pair = normalizer(*arguments)
                self.assertEqual(pair, expected)
                self.assertIsNotNone(quality_outcome_rule(*pair))

        execution_pair = normalize_execution_source_outcome(
            "cex",
            "failed",
            "SourceBookError: source returned no order book",
            "SourceBookError: source returned no order book at /srv/private",
        )
        self.assertEqual(
            execution_pair,
            ("source_no_observation", "source_no_order_book"),
        )
        self.assertIsNotNone(quality_outcome_rule(*execution_pair))

    def test_unknown_family_outcomes_fail_closed_to_manual_review(self):
        fail_closed_quality_outcome = self.required_helper(
            "fail_closed_quality_outcome"
        )
        normalize_dex_depth_source_outcome = self.required_helper(
            "normalize_dex_depth_source_outcome"
        )
        expected = ("needs_review", "daily_quality_outcome_invalid")
        self.assertEqual(
            fail_closed_quality_outcome("new_status", "raw private reason"),
            expected,
        )
        self.assertEqual(
            normalize_dex_depth_source_outcome(
                "new_status",
                "unbounded_reason",
                "PermissionError: C:\\private\\secret.csv",
            ),
            expected,
        )
        self.assertIsNotNone(quality_outcome_rule(*expected))

    def test_fact_contract_rejects_cross_family_and_cross_market_outcomes(self):
        canonical_rule = self.required_helper(
            "canonical_quality_fact_rule"
        )
        rejected = (
            ("cex", "tvl", "observed", "observed"),
            ("dex", "tvl", "failed", "execution_calculation_failed"),
            (
                "cex",
                "daily",
                "legacy_ohlcv_snapshot",
                "legacy_ohlcv_snapshot",
            ),
            (
                "dex",
                "daily",
                "legacy_ohlcv_snapshot",
                "legacy_ohlcv_snapshot",
            ),
            (
                "cex",
                "depth",
                "legacy_ohlcv_snapshot",
                "legacy_ohlcv_snapshot",
            ),
            (
                "dex",
                "depth",
                "legacy_ohlcv_snapshot",
                "legacy_ohlcv_snapshot",
            ),
            (
                "dex",
                "execution",
                "source_no_observation",
                "source_no_tvl_observation",
            ),
            (
                "cex",
                "execution",
                "source_no_observation",
                "source_no_tvl_observation",
            ),
        )
        for market_type, fact_name, status, reason_code in rejected:
            with self.subTest(
                market_type=market_type,
                fact_name=fact_name,
                status=status,
                reason_code=reason_code,
            ):
                self.assertIsNone(
                    canonical_rule(
                        market_type,
                        fact_name,
                        status,
                        reason_code,
                    )
                )

        self.assertIsNotNone(
            canonical_rule(
                "cex",
                "tvl",
                "not_applicable",
                "cex_markets_do_not_have_pool_tvl",
            )
        )
        self.assertIsNotNone(
            canonical_rule(
                "cex",
                "depth",
                "needs_review",
                "not_listed",
            )
        )
        for status in (
            "collection_failed",
            "needs_review",
            "backfill_pending",
            "source_no_observation",
            "unsupported",
        ):
            with self.subTest(daily_multiple_status=status):
                self.assertIsNotNone(
                    canonical_rule(
                        "cex",
                        "daily",
                        status,
                        "multiple_daily_quality_reasons",
                    )
                )

    def test_fact_action_is_derived_from_the_same_canonical_contract(self):
        canonical_action = self.required_helper(
            "canonical_quality_fact_action"
        )
        self.assertEqual(
            canonical_action(
                "cex",
                "depth",
                "needs_review",
                "not_listed",
                False,
            ),
            "operator_manual_review",
        )
        self.assertEqual(
            canonical_action(
                "dex",
                "execution",
                "collection_failed",
                "network",
                True,
            ),
            "retry_execution_collection",
        )
        self.assertEqual(
            canonical_action(
                "cex",
                "daily",
                "unsupported",
                "source_range_unavailable",
                False,
            ),
            "operator_review_source_outcome",
        )

    def test_tvl_normalizer_preserves_bounded_terminal_absence(self):
        normalize_tvl_source_outcome = self.required_helper(
            "normalize_tvl_source_outcome"
        )
        for source_status, expected in (
            (
                "missing",
                ("source_no_observation", "source_no_tvl_observation"),
            ),
            (
                "not_found",
                ("source_no_observation", "source_pool_not_found"),
            ),
        ):
            with self.subTest(source_status=source_status):
                normalized = normalize_tvl_source_outcome(source_status)
                self.assertEqual(normalized, expected)
                self.assertEqual(
                    normalize_tvl_source_outcome(*normalized),
                    expected,
                )

    def test_source_endpoint_projection_keeps_only_safe_http_origin(self):
        sanitize_public_source_endpoint = self.required_helper(
            "sanitize_public_source_endpoint"
        )
        cases = {
            "https://user:pass@api.example.com:8443/v2/pools?token=secret#raw": (
                "https://api.example.com:8443"
            ),
            "https://8.8.8.8/dns-query?name=secret#raw": "https://8.8.8.8",
            "https://[2606:4700:4700::1111]:8443/dns-query": (
                "https://[2606:4700:4700::1111]:8443"
            ),
            "http://127.0.0.1:8080/private/path": None,
            "http://10.0.0.1/private/path": None,
            "http://172.16.0.1/private/path": None,
            "http://192.168.0.1/private/path": None,
            "http://169.254.1.1/private/path": None,
            "http://192.0.2.1/reserved": None,
            "http://0.0.0.0/unspecified": None,
            "http://224.0.0.1/multicast": None,
            "http://[::1]/loopback": None,
            "http://[fc00::1]/private": None,
            "http://[fe80::1]/link-local": None,
            "http://[2001:db8::1]/reserved": None,
            "http://[::]/unspecified": None,
            "http://[ff02::1]/multicast": None,
            "http://localhost/internal": None,
            "http://api.localhost/internal": None,
            "http://collector/internal": None,
            "http://printer.local/internal": None,
            "file:///srv/private/facts.csv": None,
            "C:\\private\\facts.csv": None,
            "https://safe.example.com/path\nX-Secret: value": None,
            "https://safe.example.com/path\n": None,
        }
        for endpoint, expected in cases.items():
            with self.subTest(endpoint=endpoint):
                self.assertEqual(
                    sanitize_public_source_endpoint(endpoint),
                    expected,
                )

    def test_source_endpoint_projection_rejects_legacy_ipv4_spellings(self):
        sanitize_public_source_endpoint = self.required_helper(
            "sanitize_public_source_endpoint"
        )
        for endpoint in (
            "http://127.1/internal",
            "http://0177.0.0.1/internal",
            "http://0x7f.0.0.1/internal",
        ):
            with self.subTest(endpoint=endpoint):
                self.assertIsNone(sanitize_public_source_endpoint(endpoint))

    def test_retry_resolution_state_is_exact_and_never_resolves_review_outcomes(self):
        quality_outcome_resolution_state = self.required_helper(
            "quality_outcome_resolution_state"
        )
        cases = {
            ("observed", "observed"): "observed",
            (
                "source_no_observation",
                "source_no_two_sided_book",
            ): "confirmed_terminal_absence",
            (
                "unsupported",
                "unsupported_chain",
            ): "confirmed_terminal_absence",
            ("needs_review", "not_listed"): "unresolved",
            ("partial", "measurement_limit"): "unresolved",
            ("invalid", "source_invalid_order_book"): "unresolved",
            ("unknown", "unknown"): "unresolved",
        }
        for pair, expected in cases.items():
            with self.subTest(pair=pair):
                self.assertEqual(
                    quality_outcome_resolution_state(*pair),
                    expected,
                )


if __name__ == "__main__":
    unittest.main()
