import csv
import json
import shutil
import subprocess
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from dashboard import server
from scripts.check_dashboard_release import validate_quality
from scripts.fact_quality import build_report
from scripts.market_database import build_database


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = PROJECT_ROOT / "dashboard/static/app.js"


def write_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


class PublicDailyQualityOverlayTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temporary_directory.name)
        self.quality_dir = self.data_dir / "quality"
        self.quality_dir.mkdir()
        self.report_path = self.quality_dir / "daily-latest.json"
        daily_rows = [
            {
                "date": day,
                "token_symbol": "BTC",
                "exchange": "binance",
                "cex_symbol": "BTC/USDT",
                "open": price,
                "high": str(int(price) + 2),
                "low": str(int(price) - 2),
                "close": price,
                "base_volume": "10",
                "quote_volume_usd": "1000",
            }
            for day, price in (
                ("2026-01-01", "100"),
                ("2026-01-08", "108"),
            )
        ]
        write_csv(
            self.data_dir / server.CEX_FILENAME,
            [
                "date",
                "token_symbol",
                "exchange",
                "cex_symbol",
                "open",
                "high",
                "low",
                "close",
                "base_volume",
                "quote_volume_usd",
            ],
            daily_rows,
        )
        write_csv(
            self.data_dir / server.DEX_FILENAME,
            [
                "date",
                "token_symbol",
                "chain",
                "dex",
                "pool_address",
                "pool_name",
                "open",
                "high",
                "low",
                "close",
                "dex_volume_usd",
                "pool_tvl_usd",
            ],
            [
                {
                    "date": day,
                    "token_symbol": "BTC",
                    "chain": "eth",
                    "dex": "uniswap",
                    "pool_address": "0xpool",
                    "pool_name": "WBTC / USDC",
                    "open": price,
                    "high": str(int(price) + 2),
                    "low": str(int(price) - 2),
                    "close": price,
                    "dex_volume_usd": "400",
                    "pool_tvl_usd": "5000",
                }
                for day, price in (
                    ("2026-01-01", "101"),
                    ("2026-01-08", "109"),
                )
            ],
        )
        database_result = build_database(
            self.data_dir,
            self.data_dir / server.DATABASE_FILENAME,
        )
        self.import_run_id = database_result["import_run_id"]
        self.environment = {"MARKET_DATA_DIR": str(self.data_dir)}
        server.clear_runtime_caches()

    def tearDown(self):
        server.clear_runtime_caches()
        self.temporary_directory.cleanup()

    @staticmethod
    def issue(day, reason_code, status, retryable):
        return {
            "issue_id": "issue-{}-{}".format(
                day,
                reason_code,
            ),
            "category": "historical_gap",
            "status": status,
            "reason_code": reason_code,
            "retryable": retryable,
            "market": {
                "market_id": "cex:binance:BTC/USDT",
                "market_type": "cex",
                "token_symbol": "BTC",
                "exchange": "binance",
                "instrument": "BTC/USDT",
            },
            "date": day,
            "message": "Protected report detail",
            "details": {},
            "source_url_hints": [],
        }

    def write_report(
        self,
        issues,
        *,
        import_run_id=None,
        extra=None,
    ):
        payload = {
            "schema": "fact_quality_report/v1",
            "generated_at_utc": "2026-01-09T01:00:00+00:00",
            "audit_date": "2026-01-09",
            "latest_completed_utc_day": "2026-01-08",
            "status": "warning",
            "summary": {"issue_count": len(issues)},
            "issues": issues,
            "publication": {
                "status": "published_with_backfill",
                "import_run_id": import_run_id or self.import_run_id,
            },
        }
        if extra:
            payload.update(extra)
        self.report_path.write_text(
            json.dumps(payload),
            encoding="utf-8",
        )
        server.clear_runtime_caches()

    def quality_for_day(self, day):
        with patch.dict(server.os.environ, self.environment, clear=True):
            payload = server.build_market_quality(
                "BTC",
                start=day,
                end=day,
            )
        market = next(
            item
            for item in payload["markets"]
            if item["market_id"] == "cex:binance:BTC/USDT"
        )
        return payload, market

    def test_reason_mapping_date_filter_and_mixed_status_priority(self):
        issues = [
            self.issue("2026-01-02", "network", "collection_failed", True),
            self.issue(
                "2026-01-02",
                "rate_limit",
                "collection_failed",
                True,
            ),
            self.issue(
                "2026-01-02",
                "source_unavailable",
                "collection_failed",
                True,
            ),
            self.issue("2026-01-02", "parse", "collection_failed", True),
            self.issue(
                "2026-01-02",
                "validation",
                "collection_failed",
                True,
            ),
            self.issue(
                "2026-01-03",
                "no_candles",
                "source_no_observation",
                False,
            ),
            self.issue("2026-01-04", "not_listed", "needs_review", False),
            self.issue(
                "2026-01-05",
                "source_range_unavailable",
                "needs_review",
                False,
            ),
            self.issue(
                "2026-01-06",
                "missing_unexplained",
                "backfill_pending",
                True,
            ),
        ]
        foreign_issue = self.issue(
            "2026-01-02",
            "network",
            "collection_failed",
            True,
        )
        foreign_issue["market"] = {
            **foreign_issue["market"],
            "market_id": "cex:binance:ETH/USDT",
            "token_symbol": "ETH",
            "instrument": "ETH/USDT",
        }
        issues.append(foreign_issue)
        self.write_report(issues)
        expected = {
            "2026-01-02": (
                "collection_failed",
                True,
                "operator_review_retry_queue",
                {
                    "network": 1,
                    "parse": 1,
                    "rate_limit": 1,
                    "source_unavailable": 1,
                    "validation": 1,
                },
            ),
            "2026-01-03": (
                "source_no_observation",
                False,
                "operator_review_source_outcome",
                {"no_candles": 1},
            ),
            "2026-01-04": (
                "needs_review",
                False,
                "operator_manual_review",
                {"not_listed": 1},
            ),
            "2026-01-05": (
                "needs_review",
                False,
                "operator_manual_review",
                {"source_range_unavailable": 1},
            ),
            "2026-01-06": (
                "backfill_pending",
                True,
                "operator_review_retry_queue",
                {"missing_unexplained": 1},
            ),
        }
        for day, (
            status,
            retryable,
            action,
            reasons,
        ) in expected.items():
            with self.subTest(day=day):
                payload, market = self.quality_for_day(day)
                fact = market["facts"]["daily"]
                report = payload["metadata"]["daily_quality_report"]
                self.assertEqual(fact["status"], status)
                self.assertIs(fact["retryable"], retryable)
                self.assertEqual(fact["action"], action)
                self.assertEqual(fact["reason_code_counts"], reasons)
                self.assertEqual(fact["affected_dates"], [day])
                self.assertEqual(report["status"], "matched")
                self.assertEqual(
                    report["identity_status"],
                    "matched_current_import",
                )
                self.assertEqual(
                    report["schema"],
                    "fact_quality_report/v1",
                )
                self.assertEqual(
                    report["selected_window_issue_count"],
                    sum(reasons.values()),
                )
                self.assertEqual(report["reason_code_counts"], reasons)
                self.assertEqual(report["affected_dates"], [day])

        with patch.dict(server.os.environ, self.environment, clear=True):
            mixed_payload = server.build_market_quality(
                "BTC",
                start="2026-01-02",
                end="2026-01-05",
            )
        mixed_market = next(
            item
            for item in mixed_payload["markets"]
            if item["market_id"] == "cex:binance:BTC/USDT"
        )
        mixed_fact = mixed_market["facts"]["daily"]
        self.assertEqual(mixed_fact["status"], "collection_failed")
        self.assertTrue(mixed_fact["retryable"])
        self.assertEqual(
            mixed_fact["action"],
            "operator_review_retry_and_manual_queues",
        )
        self.assertEqual(
            mixed_fact["issue_status_counts"],
            {
                "collection_failed": 5,
                "needs_review": 2,
                "source_no_observation": 1,
            },
        )
        self.assertIn(
            "daily_collection_failed",
            {
                flag["code"]
                for flag in mixed_fact["quality_flags"]
            },
        )
        self.assertIn(
            "daily_needs_review",
            {
                flag["code"]
                for flag in mixed_fact["quality_flags"]
            },
        )
        self.assertEqual(mixed_market["quality_status"], "critical")

    def test_lifecycle_source_no_observation_category_is_public_information(self):
        lifecycle_issue = self.issue(
            "2026-01-03",
            "no_candles",
            "source_no_observation",
            False,
        )
        lifecycle_issue["category"] = "source_no_observation"
        self.write_report([lifecycle_issue])

        payload, market = self.quality_for_day("2026-01-03")
        fact = market["facts"]["daily"]
        report = payload["metadata"]["daily_quality_report"]

        self.assertEqual(fact["status"], "source_no_observation")
        self.assertFalse(fact["retryable"])
        self.assertEqual(
            fact["action"],
            "operator_review_source_outcome",
        )
        self.assertEqual(
            fact["reason_code_counts"],
            {"no_candles": 1},
        )
        self.assertEqual(market["quality_status"], "info")
        source_flag = next(
            flag
            for flag in fact["quality_flags"]
            if flag["code"] == "daily_source_no_observation"
        )
        self.assertEqual(source_flag["severity"], "info")
        self.assertEqual(report["selected_window_issue_count"], 1)

    def test_real_fact_quality_report_is_accepted_end_to_end(self):
        report = build_report(
            self.data_dir / server.CEX_FILENAME,
            self.data_dir / server.DEX_FILENAME,
            today=date(2026, 1, 9),
        )
        report["publication"] = {
            "status": "published_with_backfill",
            "import_run_id": self.import_run_id,
        }
        self.report_path.write_text(
            json.dumps(report),
            encoding="utf-8",
        )
        server.clear_runtime_caches()
        payload, market = self.quality_for_day("2026-01-02")
        report_state = payload["metadata"]["daily_quality_report"]
        fact = market["facts"]["daily"]
        self.assertEqual(report_state["status"], "matched")
        self.assertEqual(
            report_state["evidence_mode"],
            "published_daily_audit",
        )
        self.assertEqual(
            report_state["reason_code_counts"],
            {"missing_unexplained": 2},
        )
        self.assertEqual(
            fact["reason_code_counts"],
            {"missing_unexplained": 1},
        )
        self.assertEqual(fact["status"], "backfill_pending")
        self.assertTrue(fact["retryable"])
        self.assertEqual(fact["action"], "operator_review_retry_queue")
        with patch.dict(server.os.environ, self.environment, clear=True):
            selected = server.build_market_quality(
                "BTC",
                scope="selected",
                market_a_id="cex:binance:BTC/USDT",
                market_b_id="dex:eth:uniswap:0xpool:BTC",
                start="2026-01-02",
                end="2026-01-02",
            )
        validate_quality(
            selected,
            token="BTC",
            market_a="cex:binance:BTC/USDT",
            market_b="dex:eth:uniswap:0xpool:BTC",
        )

    def test_identity_mismatch_falls_back_without_trusting_report(self):
        self.write_report(
            [
                self.issue(
                    "2026-01-02",
                    "no_candles",
                    "source_no_observation",
                    False,
                )
            ],
            import_run_id="0" * 32,
        )
        payload, market = self.quality_for_day("2026-01-02")
        report = payload["metadata"]["daily_quality_report"]
        fact = market["facts"]["daily"]
        self.assertEqual(report["status"], "ignored_identity_mismatch")
        self.assertEqual(report["identity_status"], "mismatch")
        self.assertEqual(
            report["evidence_mode"],
            "catalog_window_inference",
        )
        self.assertEqual(report["selected_window_issue_count"], 0)
        self.assertEqual(fact["status"], "backfill_pending")
        self.assertEqual(fact["action"], "operator_review_retry_queue")
        self.assertNotIn("no_candles", fact.get("reason_code_counts", {}))

    def test_matched_report_without_exact_issue_never_invents_retry(self):
        self.write_report(
            [
                self.issue(
                    "2026-01-02",
                    "network",
                    "collection_failed",
                    True,
                )
            ]
        )
        payload, market = self.quality_for_day("2026-01-06")
        report = payload["metadata"]["daily_quality_report"]
        fact = market["facts"]["daily"]
        self.assertEqual(report["status"], "matched")
        self.assertEqual(report["selected_window_issue_count"], 0)
        self.assertEqual(fact["status"], "needs_review")
        self.assertFalse(fact["retryable"])
        self.assertEqual(fact["action"], "operator_manual_review")
        self.assertEqual(
            fact["reason_code"],
            "daily_audit_no_matching_issue",
        )
        self.assertEqual(fact["affected_dates"], [])

    def test_malformed_oversized_and_path_content_fail_closed(self):
        cases = {
            "malformed": (
                b'{"schema":"fact_quality_report/v1",'
                b'"message":"/Users/alice/private.csv"',
                "ignored_invalid",
            ),
            "oversized": (
                b"x" * (server.MAX_DAILY_QUALITY_REPORT_BYTES + 1),
                "ignored_invalid",
            ),
            "wrong_schema": (
                json.dumps(
                    {
                        "schema": "fact_quality_report/v999",
                        "message": "/home/ugs/private.csv",
                    }
                ).encode("utf-8"),
                "ignored_invalid",
            ),
        }
        for name, (content, expected_status) in cases.items():
            with self.subTest(case=name):
                self.report_path.write_bytes(content)
                server.clear_runtime_caches()
                payload, _market = self.quality_for_day("2026-01-02")
                serialized = json.dumps(payload)
                self.assertEqual(
                    payload["metadata"]["daily_quality_report"]["status"],
                    expected_status,
                )
                self.assertNotIn("/Users/", serialized)
                self.assertNotIn("/home/", serialized)
                self.assertNotIn(str(self.data_dir), serialized)

    def test_report_path_cannot_escape_data_root_and_signature_tracks_file(self):
        with patch.dict(server.os.environ, self.environment, clear=True):
            before = server.api_source_signature()
        self.write_report([])
        with patch.dict(server.os.environ, self.environment, clear=True):
            after = server.api_source_signature()
        self.assertNotEqual(before, after)

        outside = self.data_dir.parent / (
            self.data_dir.name + "-outside-quality.json"
        )
        outside.write_text(
            '{"schema":"fact_quality_report/v1","secret":"/private/secret"}',
            encoding="utf-8",
        )
        try:
            self.report_path.unlink()
            self.report_path.symlink_to(outside)
            server.clear_runtime_caches()
            payload, _market = self.quality_for_day("2026-01-02")
            self.assertEqual(
                payload["metadata"]["daily_quality_report"]["status"],
                "unavailable",
            )
            self.assertNotIn(
                "/private/secret",
                json.dumps(payload),
            )
        finally:
            outside.unlink(missing_ok=True)


class PublicDailyQualityFrontendTest(unittest.TestCase):
    def test_daily_reason_tooltip_and_operator_only_actions_are_visible(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node.js is not installed in this runtime")
        script = APP_PATH.read_text(encoding="utf-8") + """
const markup = qualityFactMarkup("daily", {
  status: "collection_failed",
  reason: "A bounded public reason.",
  reason_code_counts: { network: 1, rate_limit: 2 },
  affected_dates: ["2026-01-02", "2026-01-03"],
  affected_date_count: 2,
  retryable: true,
  action: "operator_review_retry_queue",
  coverage_ratio: 0.5,
});
const mixedMarkup = qualityFactMarkup("daily", {
  status: "collection_failed",
  reason_code_counts: { network: 1, not_listed: 1 },
  affected_dates: ["2026-01-02", "2026-01-03"],
  retryable: true,
  action: "operator_review_retry_and_manual_queues",
});
console.log(JSON.stringify({ markup, mixedMarkup }));
"""
        completed = subprocess.run(
            [node, "-e", script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        result = json.loads(completed.stdout)
        markup = result["markup"]
        self.assertIn('tabindex="0"', markup)
        self.assertIn('data-tooltip="A bounded public reason.', markup)
        self.assertIn('aria-label="collection failed. A bounded public reason.', markup)
        self.assertIn('class="quality-fact-details"', markup)
        self.assertIn('aria-label="Open daily Fact details"', markup)
        self.assertIn("Network request failed (1)", markup)
        self.assertIn("Source rate limit (2)", markup)
        self.assertIn("2026-01-02, 2026-01-03", markup)
        self.assertIn("protected operator retry queue", markup)
        self.assertIn("public page is read-only", markup)
        self.assertNotIn("retry available", markup)
        persistent = markup.split(
            '<details class="quality-fact-details">',
            1,
        )[0]
        self.assertNotIn("Network request failed", persistent)
        self.assertNotIn("protected operator", persistent)
        self.assertNotIn("<small", persistent)
        self.assertNotIn("<p>", persistent)
        self.assertIn(
            "both protected operator queues: retry and manual review",
            result["mixedMarkup"],
        )


if __name__ == "__main__":
    unittest.main()
