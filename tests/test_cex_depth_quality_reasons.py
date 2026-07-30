import unittest

from dashboard import server


class CexDepthQualityReasonTest(unittest.TestCase):
    def test_legacy_empty_book_is_a_non_retryable_source_outcome(self):
        reason = server.cex_depth_reason_code(
            {
                "status": "failed",
                "error": (
                    "SourceBookError: crypto_com returned an empty "
                    "order-book side"
                ),
            }
        )
        self.assertEqual(reason, "source_no_two_sided_book")

        fact = server._depth_quality_fact(
            {
                "market_type": "cex",
                "depth_status": "failed",
                "depth_reason_code": reason,
                "depth_error": "SourceBookError: empty order-book side",
            }
        )
        self.assertEqual(fact["reason_code"], "source_no_two_sided_book")
        self.assertFalse(fact["retryable"])
        self.assertIsNone(fact["action"])

    def test_transport_failure_remains_retryable(self):
        fact = server._depth_quality_fact(
            {
                "market_type": "cex",
                "depth_status": "failed",
                "depth_reason_code": "network",
                "depth_error": "URLError: temporary DNS failure",
            }
        )
        self.assertEqual(fact["reason_code"], "network")
        self.assertTrue(fact["retryable"])
        self.assertEqual(fact["action"], "retry_depth_collection")

    def test_collector_reason_code_wins_over_legacy_error_text(self):
        self.assertEqual(
            server.cex_depth_reason_code(
                {
                    "status": "failed",
                    "reason_code": "rate_limit",
                    "error": "SourceBookError: returned no order book",
                }
            ),
            "rate_limit",
        )
        self.assertEqual(
            server.cex_depth_reason_code(
                {
                    "status": "failed",
                    "reason_code": "unexpected_unbounded_reason",
                }
            ),
            "collection_failed",
        )

    def test_execution_from_same_empty_book_is_not_retryable(self):
        market_id = "cex:crypto_com:GMX/USDT"
        rows = [
            {
                "status": "failed",
                "status_reason": "source_no_two_sided_book",
                "error": "SourceBookError: empty order-book side",
                "snapshot_id": "depth-1",
                "source_snapshot_id": "depth-1",
                "market_id": market_id,
                "market_type": "cex",
                "direction": "buy_token",
                "requested_notional_usd": "1000",
                "observed_at": "2026-07-30T00:00:00+00:00",
            }
        ]
        fact = server._execution_quality_fact(
            {"market_id": market_id},
            {
                "snapshot": {
                    "by_market": {market_id: rows},
                    "observed_at": "2026-07-30T00:00:00+00:00",
                },
                "error_code": None,
            },
        )
        self.assertEqual(fact["reason_code"], "source_no_two_sided_book")
        self.assertFalse(fact["retryable"])
        self.assertIsNone(fact["action"])


if __name__ == "__main__":
    unittest.main()
