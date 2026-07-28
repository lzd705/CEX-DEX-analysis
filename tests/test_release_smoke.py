import unittest

from scripts.check_dashboard_release import (
    ReleaseCheckError,
    ResponseMetrics,
    validate_comparison,
    validate_execution,
    validate_quality,
    validate_summary,
    validate_token_catalog,
)


class DashboardReleaseSmokeTest(unittest.TestCase):
    def summary(self):
        return {
            "metadata": {
                "response_scope": "screener_summary",
                "summary_version": 1,
                "data_generation": "generation-1",
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
                "default_workspace_token": "AAVE",
            },
            "tokens": [{"token_symbol": "AAVE"}],
        }

    def metrics(self, path="/api/markets/summary", raw=1000, wire=500):
        return ResponseMetrics(path, 1.0, wire, raw, True)

    def test_summary_rejects_heavy_arrays_and_payload_budget_regression(self):
        summary = self.summary()
        token, start, end, generation = validate_summary(
            summary,
            self.metrics(),
            raw_max=2000,
            gzip_max=1000,
        )
        self.assertEqual((token, start, end, generation), (
            "AAVE",
            "2026-01-01",
            "2026-01-31",
            "generation-1",
        ))

        with self.assertRaisesRegex(ReleaseCheckError, "heavy root field"):
            validate_summary(
                {**summary, "markets": []},
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "exceeds"):
            validate_summary(
                summary,
                self.metrics(raw=2001),
                raw_max=2000,
                gzip_max=1000,
            )

    def test_token_catalog_rejects_cross_token_or_generation_mismatch(self):
        catalog = {
            "token_symbol": "AAVE",
            "metadata": {
                "window_start": "2026-01-01",
                "window_end": "2026-01-31",
                "data_generation": "generation-1",
            },
            "markets": [{"token_symbol": "AAVE", "market_id": "a"}],
        }
        markets = validate_token_catalog(
            catalog,
            self.metrics("/api/markets/catalog"),
            token="AAVE",
            start="2026-01-01",
            end="2026-01-31",
            generation="generation-1",
            raw_max=2000,
            gzip_max=1000,
        )
        self.assertEqual(len(markets), 1)

        with self.assertRaisesRegex(ReleaseCheckError, "leaked another Token"):
            validate_token_catalog(
                {**catalog, "markets": [{"token_symbol": "UNI"}]},
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "generations differ"):
            validate_token_catalog(
                catalog,
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-2",
                raw_max=2000,
                gzip_max=1000,
            )

    def test_expert_endpoint_validators_reject_empty_or_unmeasured_results(self):
        market_a = "cex:binance:AAVE/USDT"
        market_b = "dex:eth:uniswap_v3:pool:AAVE"
        comparison = {
            "token_symbol": "AAVE",
            "market_a": {"market_id": market_a},
            "market_b": {"market_id": market_b},
            "metadata": {
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
                "comparison_days": 1,
            },
            "observations": [{"date": "2026-01-15"}],
            "latest_comparable_observation": {"date": "2026-01-15"},
        }
        validate_comparison(
            comparison,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            start="2026-01-01",
            end="2026-01-31",
        )
        with self.assertRaisesRegex(ReleaseCheckError, "no daily observations"):
            validate_comparison(
                {**comparison, "observations": []},
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                start="2026-01-01",
                end="2026-01-31",
            )

        quality_markets = [
            {
                "market_id": market_id,
                "token_symbol": "AAVE",
                "facts": {"daily": {"status": "observed"}},
            }
            for market_id in (market_a, market_b)
        ]
        quality = {
            "token_symbol": "AAVE",
            "metadata": {
                "scope": "selected",
                "selected_market_ids": [market_a, market_b],
            },
            "markets": quality_markets,
        }
        validate_quality(
            quality,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
        )
        with self.assertRaisesRegex(ReleaseCheckError, "both selected markets"):
            validate_quality(
                {**quality, "markets": quality_markets[:1]},
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
            )

        def execution_rows(market_id, status):
            return [
                {
                    "market_id": market_id,
                    "token_symbol": "AAVE",
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "status": status,
                }
                for direction in ("sell_token", "buy_token")
                for notional in (1_000, 5_000, 10_000, 50_000, 100_000)
            ]

        execution = {
            "token_symbol": "AAVE",
            "market_a": {
                "market": {"market_id": market_a},
                "status": "available",
                "rows": execution_rows(market_a, "observed"),
            },
            "market_b": {
                "market": {"market_id": market_b},
                "status": "available",
                "rows": execution_rows(market_b, "unsupported"),
            },
        }
        validate_execution(
            execution,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
        )
        unsupported_execution = {
            **execution,
            "market_a": {
                **execution["market_a"],
                "rows": execution_rows(market_a, "unsupported"),
            },
        }
        with self.assertRaisesRegex(ReleaseCheckError, "no observed or partial"):
            validate_execution(
                unsupported_execution,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
            )


if __name__ == "__main__":
    unittest.main()
