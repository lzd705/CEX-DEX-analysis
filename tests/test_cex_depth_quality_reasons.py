import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard import server
from scripts.execution_cost import EXECUTION_COST_COLUMNS
from scripts.fetch_cex_depth import failed_execution_rows


class SourceBookError(Exception):
    pass


def write_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def public_empty_book_execution_fact():
    with tempfile.TemporaryDirectory() as directory:
        data_dir = Path(directory)
        cex_path = data_dir / server.CEX_FILENAME
        dex_path = data_dir / server.DEX_FILENAME
        execution_path = data_dir / server.CEX_EXECUTION_COST_FILENAME
        write_csv(
            cex_path,
            [
                "date", "token_symbol", "exchange", "cex_symbol", "open",
                "high", "low", "close", "base_volume", "quote_volume_usd",
            ],
            [{
                "date": "2026-07-30", "token_symbol": "GMX",
                "exchange": "crypto_com", "cex_symbol": "GMX/USDT",
                "open": "1", "high": "1", "low": "1", "close": "1",
                "base_volume": "1", "quote_volume_usd": "1",
            }],
        )
        write_csv(
            dex_path,
            [
                "date", "token_symbol", "chain", "dex", "pool_address",
                "pool_name", "open", "high", "low", "close",
                "dex_volume_usd", "pool_tvl_usd",
            ],
            [{
                "date": "2026-07-30", "token_symbol": "GMX",
                "chain": "eth", "dex": "uniswap", "pool_address": "0xpool",
                "pool_name": "GMX / USDC", "open": "1", "high": "1",
                "low": "1", "close": "1", "dex_volume_usd": "1",
                "pool_tvl_usd": "1",
            }],
        )
        market = {
            "token_symbol": "GMX", "exchange": "crypto_com",
            "cex_symbol": "GMX/USDT",
        }
        raw_error = SourceBookError(
            "crypto_com returned an empty order-book side"
        )
        rows = failed_execution_rows(
            market,
            snapshot_id="cex-depth-1",
            request_started_at="2026-07-30T00:00:00+00:00",
            response_received_at="2026-07-30T00:00:01+00:00",
            error=raw_error,
            status_reason=(
                "SourceBookError: crypto_com returned an empty order-book side"
            ),
        )
        write_csv(execution_path, EXECUTION_COST_COLUMNS, rows)
        environment = {
            "MARKET_CEX_DATA": str(cex_path),
            "MARKET_DEX_DATA": str(dex_path),
            "MARKET_CEX_EXECUTION_COST_DATA": str(execution_path),
        }
        server.clear_runtime_caches()
        try:
            with patch.dict(server.os.environ, environment, clear=True):
                quality = server.build_market_quality("GMX")
        finally:
            server.clear_runtime_caches()
        market_quality = next(
            market for market in quality["markets"]
            if market["market_id"] == "cex:crypto_com:GMX/USDT"
        )
        return market_quality


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
        market_quality = public_empty_book_execution_fact()
        fact = market_quality["facts"]["execution"]
        self.assertEqual(fact["status"], "source_no_observation")
        self.assertEqual(fact["reason_code"], "source_no_two_sided_book")
        self.assertFalse(fact["retryable"])
        self.assertIsNone(fact["action"])
        self.assertNotIn("SourceBookError", json.dumps(fact))
        self.assertEqual(market_quality["quality_status"], "ok")

    def test_unsupported_dex_depth_does_not_invent_temporal_mismatch(self):
        fact = server._depth_quality_fact({
            "market_type": "dex",
            "depth_status": "unsupported",
            "depth_error": "unsupported_chain:solana",
            "depth_usd_price_freshness_status": "unavailable",
            "total_depth_10bps_usd": None,
            "total_depth_25bps_usd": None,
            "total_depth_50bps_usd": None,
            "total_depth_100bps_usd": None,
        })
        self.assertEqual(fact["status"], "unsupported")
        self.assertNotIn(
            "depth_usd_price_time_mismatch",
            {flag["code"] for flag in fact["quality_flags"]},
        )

    def test_legacy_empty_book_is_projected_as_source_no_observation(self):
        fact = server._depth_quality_fact({
            "market_type": "cex",
            "depth_status": "failed",
            "depth_reason_code": (
                "SourceBookError: crypto_com returned an empty order-book side"
            ),
            "depth_error": (
                "SourceBookError: crypto_com returned an empty order-book side"
            ),
            "bid_depth_10bps_usd": 0,
            "ask_depth_10bps_usd": 0,
            "total_depth_10bps_usd": 0,
            "depth_10bps_complete": True,
        })
        self.assertEqual(fact["status"], "source_no_observation")
        self.assertEqual(fact["reason_code"], "source_no_two_sided_book")
        self.assertFalse(fact["retryable"])
        self.assertNotIn(
            "depth_failed",
            {flag["code"] for flag in fact["quality_flags"]},
        )
        self.assertEqual(
            fact["bands_bps"]["10"],
            {"sell_token_usd": None, "buy_token_usd": None,
             "total_usd": None, "complete": False},
        )

    def test_empty_book_execution_is_same_source_outcome(self):
        market_quality = public_empty_book_execution_fact()
        fact = market_quality["facts"]["execution"]
        self.assertEqual(fact["status"], "source_no_observation")
        self.assertEqual(fact["reason_code"], "source_no_two_sided_book")
        self.assertFalse(fact["retryable"])
        self.assertFalse(any(
            flag["category"] == "data_health"
            for flag in fact["quality_flags"]
        ))

    def test_unknown_depth_status_fails_closed_to_bounded_review_outcome(self):
        fact = server._depth_quality_fact({
            "market_type": "dex",
            "depth_status": "new_unrecognized_adapter_status",
            "depth_error": "/private/protected/adapter-error",
        })
        self.assertEqual(fact["status"], "needs_review")
        self.assertEqual(fact["reason_code"], "daily_quality_outcome_invalid")
        self.assertFalse(fact["retryable"])
        self.assertNotIn("/private/protected", json.dumps(fact))

    def test_unknown_cex_depth_status_also_fails_closed(self):
        fact = server._depth_quality_fact({
            "market_type": "cex",
            "depth_status": "new_unrecognized_adapter_status",
            "depth_error": "SourceAdapterError: opaque protected detail",
        })
        self.assertEqual(fact["status"], "needs_review")
        self.assertEqual(fact["reason_code"], "daily_quality_outcome_invalid")
        self.assertFalse(fact["retryable"])


if __name__ == "__main__":
    unittest.main()
