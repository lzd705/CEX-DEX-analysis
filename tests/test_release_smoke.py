import unittest

from scripts.check_dashboard_release import (
    ReleaseCheckError,
    ResponseMetrics,
    validate_comparison,
    validate_events,
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

    def event_payload(self):
        event = {
            "event_id": "strk-unlock-2026-08-15",
            "revision": 1,
            "token_symbol": "STRK",
            "event_type": "unlock",
            "event_subtype": "scheduled_release",
            "event_name": "Scheduled STRK unlock",
            "lifecycle": "scheduled",
            "evidence_status": "primary_confirmed",
            "time": {
                "effective_at": "2026-08-15",
                "effective_at_precision": "day",
                "effective_date_start": "2026-08-15",
                "effective_date_end": "2026-08-15",
            },
            "size": {
                "amount_token": "127000000",
                "amount_usd": None,
                "amount_usd_basis": None,
                "percent_of_supply": "1.27",
                "relation": "up_to",
            },
            "market": {"venue": None, "market_symbol": None, "market_id": None},
            "onchain": {
                "chain": "starknet",
                "related_address": None,
                "related_tx_hash": None,
            },
            "source": {
                "kind": "official_project",
                "url": "https://example.test/official",
                "published_at": "2024-02-22",
                "published_at_precision": "day",
                "checked_at_utc": "2026-07-29T08:30:00Z",
                "record_sha256": "a" * 64,
                "record_locator": "facts.unlock_schedule",
            },
            "revision_lineage": {
                "recorded_at_utc": "2026-07-29T08:30:00Z",
                "reason": "initial",
            },
            "notes": None,
        }
        return {
            "schema": "event_facts_api/v1",
            "fact_schema": "event_facts/v1",
            "fact_boundary": (
                "Source-backed event facts only. No return, market-impact, "
                "importance, sentiment, or causal result is included."
            ),
            "bundle_id": "a" * 24,
            "built_at_utc": "2026-07-29T08:30:00Z",
            "availability": {"status": "available", "reason": None},
            "coverage": {
                "configured_token_count": 1,
                "covered_token_count": 1,
                "covered_tokens": ["STRK"],
                "uncovered_tokens": [],
                "query_token_has_published_fact": True,
            },
            "query": {
                "token": "STRK",
                "start": "2026-08-15",
                "end": "2026-08-15",
                "lifecycle": "scheduled",
            },
            "event_count": 1,
            "event_type_counts": {"unlock": 1},
            "lifecycle_counts": {"scheduled": 1},
            "evidence_status_counts": {"primary_confirmed": 1},
            "events": [event],
        }

    def test_event_validator_enforces_scope_lineage_and_fact_boundary(self):
        payload = self.event_payload()
        events = validate_events(
            payload,
            token="STRK",
            start="2026-08-15",
            end="2026-08-15",
            lifecycle="scheduled",
        )
        self.assertEqual(events[0]["event_id"], "strk-unlock-2026-08-15")

        unavailable = {
            **payload,
            "availability": {
                "status": "unavailable",
                "reason": "event_bundle_not_published",
            },
            "event_count": 0,
            "events": [],
        }
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "publication is unavailable",
        ):
            validate_events(
                unavailable,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        leaked = self.event_payload()
        leaked["events"][0]["future_return"] = 0.25
        with self.assertRaisesRegex(ReleaseCheckError, "event-study result"):
            validate_events(
                leaked,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        wrong_counts = self.event_payload()
        wrong_counts["event_type_counts"] = {"cex_listing": 1}
        wrong_counts["lifecycle_counts"] = {"occurred": 1}
        wrong_counts["evidence_status_counts"] = {"cross_checked": 1}
        with self.assertRaisesRegex(ReleaseCheckError, "does not match"):
            validate_events(
                wrong_counts,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        wrong_coverage = self.event_payload()
        wrong_coverage["coverage"]["uncovered_tokens"] = ["AAVE"]
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "coverage counts are inconsistent",
        ):
            validate_events(
                wrong_coverage,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

    def test_event_validator_rejects_cross_scope_and_missing_evidence(self):
        wrong_token = self.event_payload()
        wrong_token["events"][0]["token_symbol"] = "AAVE"
        with self.assertRaisesRegex(ReleaseCheckError, "another Token"):
            validate_events(
                wrong_token,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )

        missing_source = self.event_payload()
        missing_source["events"][0]["source"]["record_locator"] = ""
        with self.assertRaisesRegex(ReleaseCheckError, "locator is missing"):
            validate_events(
                missing_source,
                token="STRK",
                start="2026-08-15",
                end="2026-08-15",
                lifecycle="scheduled",
            )


if __name__ == "__main__":
    unittest.main()
