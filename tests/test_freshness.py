import unittest
from datetime import datetime, timezone

from dashboard.freshness import (
    build_source_freshness,
    daily_freshness,
    snapshot_freshness,
)


NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


class FreshnessTest(unittest.TestCase):
    def test_daily_freshness_uses_latest_completed_utc_day(self):
        current = daily_freshness(
            "cex_daily",
            {"available_start": "2026-01-01", "available_end": "2026-07-25"},
            now=NOW,
        )
        stale = daily_freshness(
            "dex_daily",
            {"available_start": "2026-01-01", "available_end": "2026-07-23"},
            now=NOW,
        )

        self.assertEqual(current["latest_completed_utc_day"], "2026-07-26")
        self.assertEqual(current["lag_days"], 1)
        self.assertEqual(current["status"], "current")
        self.assertEqual(stale["lag_days"], 3)
        self.assertEqual(stale["status"], "stale")

    def test_snapshot_freshness_preserves_unavailable_and_stale_states(self):
        unavailable = snapshot_freshness(
            "dex_tvl",
            None,
            now=NOW,
            max_age_hours=26,
        )
        stale = snapshot_freshness(
            "cex_depth",
            "2026-07-27T09:00:00Z",
            now=NOW,
            max_age_hours=2,
        )

        self.assertEqual(unavailable["status"], "unavailable")
        self.assertEqual(stale["status"], "stale")
        self.assertEqual(stale["age_hours"], 3)

    def test_snapshot_freshness_accepts_python38_incompatible_nanoseconds(self):
        current = snapshot_freshness(
            "cex_depth",
            "2026-07-27T11:00:00.019274961Z",
            now=NOW,
            max_age_hours=2,
        )

        self.assertEqual(current["status"], "current")
        self.assertEqual(
            current["observed_at"],
            "2026-07-27T11:00:00.019274+00:00",
        )

    def test_snapshot_freshness_rejects_timezone_normalization_overflow(self):
        with self.assertRaises(ValueError):
            snapshot_freshness(
                "cex_depth",
                "0001-01-01T00:00:00+23:59",
                now=NOW,
                max_age_hours=2,
            )

    def test_snapshot_freshness_rejects_future_clock_skew_beyond_tolerance(self):
        tolerated = snapshot_freshness(
            "cex_depth",
            "2026-07-27T12:05:00+00:00",
            now=NOW,
            max_age_hours=2,
        )
        self.assertEqual(tolerated["status"], "current")
        self.assertEqual(tolerated["age_hours"], 0.0)

        with self.assertRaises(ValueError):
            snapshot_freshness(
                "cex_depth",
                "2026-07-27T12:05:01+00:00",
                now=NOW,
                max_age_hours=2,
            )

    def test_source_summary_reports_common_end_and_overall_status(self):
        result = build_source_freshness(
            {
                "cex_daily": {
                    "available_start": "2026-01-01",
                    "available_end": "2026-07-25",
                },
                "dex_daily": {
                    "available_start": "2025-01-01",
                    "available_end": "2026-07-23",
                },
            },
            tvl_observed_at="2026-07-26T11:00:00+00:00",
            depth_observed_at="2026-07-27T10:30:00+00:00",
            dex_depth_observed_at="2026-07-27T10:45:00+00:00",
            cex_execution_observed_at="2026-07-27T11:00:00+00:00",
            dex_execution_observed_at="2026-07-27T11:15:00+00:00",
            now=NOW,
        )

        self.assertEqual(result["common_comparable_end"], "2026-07-23")
        self.assertEqual(result["dex_tvl"]["status"], "current")
        self.assertEqual(result["cex_depth"]["status"], "current")
        self.assertEqual(result["dex_depth"]["status"], "current")
        self.assertEqual(result["cex_execution"]["status"], "current")
        self.assertEqual(result["dex_execution"]["status"], "current")
        self.assertEqual(
            result["cex_execution"]["observed_at"],
            "2026-07-27T11:00:00+00:00",
        )
        self.assertEqual(result["overall_status"], "stale")

    def test_execution_freshness_never_inherits_depth_timestamp(self):
        result = build_source_freshness(
            {},
            depth_observed_at="2026-07-27T11:45:00+00:00",
            dex_depth_observed_at="2026-07-27T11:45:00+00:00",
            cex_execution_observed_at="2026-07-27T08:00:00+00:00",
            dex_execution_observed_at=None,
            now=NOW,
        )

        self.assertEqual(result["cex_depth"]["status"], "current")
        self.assertEqual(result["cex_execution"]["status"], "stale")
        self.assertEqual(result["cex_execution"]["age_hours"], 4)
        self.assertEqual(result["dex_execution"]["status"], "unavailable")


if __name__ == "__main__":
    unittest.main()
