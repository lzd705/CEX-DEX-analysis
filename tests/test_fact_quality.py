import csv
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, timedelta
from pathlib import Path

from scripts.fact_quality import (
    CEX_REQUIRED_COLUMNS,
    DEX_REQUIRED_COLUMNS,
    _attempt_matches_market,
    _attempt_source,
    attempt_for_gap,
    build_report,
    cex_market,
    gap_evidence,
    main,
    normalize_collection_attempts,
    source_url_hints,
)
from scripts.quality_outcomes import quality_outcome_rule


CEX_COLUMNS = [
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
]
DEX_COLUMNS = [
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
]


def cex_row(day, **overrides):
    row = {
        "date": day,
        "token_symbol": "AAVE",
        "exchange": "binance",
        "cex_symbol": "AAVE/USDT",
        "open": "100",
        "high": "105",
        "low": "95",
        "close": "102",
        "base_volume": "10",
        "quote_volume_usd": "1020",
    }
    row.update(overrides)
    return row


def dex_row(day, **overrides):
    row = {
        "date": day,
        "token_symbol": "AAVE",
        "chain": "eth",
        "dex": "uniswap_v3",
        "pool_address": "0xAAVEPOOL",
        "pool_name": "AAVE / WETH",
        "open": "100",
        "high": "105",
        "low": "95",
        "close": "101",
        "dex_volume_usd": "500",
        "pool_tvl_usd": "1000000",
    }
    row.update(overrides)
    return row


def write_csv(path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_attempt_ledger(
    path,
    *,
    market_type,
    source_csv,
    attempts,
    source_sha256=None,
):
    path.write_text(
        json.dumps(
            {
                "schema": "daily_collection_attempts/v1",
                "collector": market_type,
                "generated_at_utc": "2026-07-20T01:00:00+00:00",
                "requested_window": {
                    "start_date": attempts[0]["requested_start_date"],
                    "end_date": attempts[0]["requested_end_date"],
                },
                "source_csv": source_csv.name,
                "source_csv_sha256": (
                    source_sha256
                    if source_sha256 is not None
                    else hashlib.sha256(source_csv.read_bytes()).hexdigest()
                ),
                "attempt_count": len(attempts),
                "attempts": attempts,
            }
        )
        + "\n",
        encoding="utf-8",
    )


def cex_attempt(day, **overrides):
    row = {
        "attempt_id": "attempt-cex",
        "market_type": "cex",
        "token_symbol": "AAVE",
        "exchange": "binance",
        "instrument": "AAVE/USDT",
        "chain": None,
        "dex": None,
        "pool_address": None,
        "requested_start_date": day,
        "requested_end_date": day,
        "observed_dates": [],
        "observed_day_count": 0,
        "status": "failed",
        "outcome": "request_failed",
        "reason_code": "rate_limit",
        "http_status": 429,
        "error": "The source rejected the request because its rate limit was reached.",
        "finished_at_utc": "2026-07-20T00:30:00+00:00",
    }
    row.update(overrides)
    return row


def dex_attempt(day, **overrides):
    row = {
        "attempt_id": "attempt-dex",
        "market_type": "dex",
        "token_symbol": "AAVE",
        "exchange": None,
        "instrument": None,
        "chain": "eth",
        "dex": "uniswap_v3",
        "pool_address": "0xaavepool",
        "requested_start_date": day,
        "requested_end_date": day,
        "observed_dates": [],
        "observed_day_count": 0,
        "status": "no_data",
        "outcome": "no_candles",
        "reason_code": "no_candles",
        "http_status": None,
        "error": "The source returned no daily candles inside the requested window.",
        "finished_at_utc": "2026-07-20T00:30:00+00:00",
    }
    row.update(overrides)
    return row


class FactQualityTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.cex_path = self.root / "cex.csv"
        self.dex_path = self.root / "dex.csv"
        write_csv(self.cex_path, CEX_COLUMNS, [])
        write_csv(self.dex_path, DEX_COLUMNS, [])

    def tearDown(self):
        self.temporary.cleanup()

    def report(self, *, today=date(2026, 7, 20)):
        return build_report(self.cex_path, self.dex_path, today=today)

    def test_required_column_sets_match_daily_contracts(self):
        self.assertEqual(CEX_REQUIRED_COLUMNS, set(CEX_COLUMNS))
        self.assertEqual(DEX_REQUIRED_COLUMNS, set(DEX_COLUMNS))

    def test_gap_evidence_uses_the_shared_daily_outcome_matrix(self):
        market = cex_market(cex_row("2026-07-19"))
        cases = (
            ("missing", None, "backfill_pending", "missing_unexplained", True),
            ("network", "network", "collection_failed", "network", True),
            ("rate limit", "rate_limit", "collection_failed", "rate_limit", True),
            (
                "source unavailable",
                "source_unavailable",
                "collection_failed",
                "source_unavailable",
                True,
            ),
            (
                "generic collection failure",
                "collection_failed",
                "collection_failed",
                "collection_failed",
                True,
            ),
            ("parse", "parse", "collection_failed", "parse", True),
            ("validation", "validation", "collection_failed", "validation", True),
            (
                "no candles",
                "no_candles",
                "source_no_observation",
                "no_candles",
                False,
            ),
            ("not listed", "not_listed", "needs_review", "not_listed", False),
            (
                "source range unavailable",
                "source_range_unavailable",
                "unsupported",
                "source_range_unavailable",
                False,
            ),
        )
        for name, attempt_reason, status, reason_code, retryable in cases:
            with self.subTest(case=name):
                attempts = []
                if attempt_reason is not None:
                    attempt = cex_attempt("2026-07-19", reason_code=attempt_reason)
                    if attempt_reason == "no_candles":
                        attempt.update(
                            status="no_data",
                            outcome="no_candles",
                            http_status=None,
                        )
                    elif attempt_reason == "source_range_unavailable":
                        attempt.update(
                            status="unsupported",
                            outcome="range_unavailable",
                            http_status=None,
                        )
                    attempts = [attempt]
                evidence = gap_evidence(
                    attempts=attempts,
                    market=market,
                    missing_day=date(2026, 7, 19),
                    default_message="Missing daily observation.",
                )
                rule = quality_outcome_rule(status, reason_code)
                self.assertIsNotNone(rule)
                self.assertEqual(
                    (evidence["status"], evidence["retryable"]),
                    (status, retryable),
                )
                self.assertEqual(
                    (rule.retryable, rule.terminal),
                    (retryable, status not in {"collection_failed", "needs_review", "backfill_pending"}),
                )

    def test_gap_evidence_fails_closed_for_an_unknown_attempt_outcome(self):
        evidence = gap_evidence(
            attempts=[cex_attempt("2026-07-19", reason_code="unknown_reason")],
            market=cex_market(cex_row("2026-07-19")),
            missing_day=date(2026, 7, 19),
            default_message="Missing daily observation.",
        )

        self.assertEqual(evidence["status"], "needs_review")
        self.assertEqual(
            evidence["reason_code"], "daily_quality_outcome_invalid"
        )
        self.assertFalse(evidence["retryable"])

    def test_cex_attempt_cannot_cross_quote_instruments(self):
        market = {
            "market_type": "cex",
            "token_symbol": "AAVE",
            "exchange": "upbit",
            "instrument": "AAVE/USDT",
        }
        attempt = cex_attempt(
            "2026-07-19",
            exchange="upbit",
            instrument="AAVE/KRW",
        )

        self.assertIsNone(attempt_for_gap([attempt], market, date(2026, 7, 19)))

    def test_invalid_attempt_ledger_invalidates_the_whole_ledger(self):
        for name, mutation in {
            "empty_id": {"attempt_id": ""},
            "naive_timestamp": {"finished_at_utc": "2026-07-20T00:30:00"},
            "outside_window": {"observed_dates": ["2026-07-21"], "observed_day_count": 1},
            "missing_instrument": {"instrument": None},
        }.items():
            with self.subTest(mutation=name):
                ledger = self.root / (name + ".json")
                attempt = cex_attempt("2026-07-19", **mutation)
                write_attempt_ledger(
                    ledger,
                    market_type="cex",
                    source_csv=self.cex_path,
                    attempts=[attempt],
                )
                attempts, metadata = _attempt_source(
                    path=ledger,
                    market_type="cex",
                    source_csv_sha256=hashlib.sha256(self.cex_path.read_bytes()).hexdigest(),
                )
                self.assertEqual(attempts, [])
                self.assertEqual(metadata["status"], "ignored_invalid")

    def test_duplicate_attempt_id_invalidates_the_whole_ledger(self):
        ledger = self.root / "duplicate.json"
        attempts = [
            cex_attempt("2026-07-19", attempt_id="repeated-id"),
            cex_attempt("2026-07-20", attempt_id="repeated-id"),
        ]
        write_attempt_ledger(
            ledger,
            market_type="cex",
            source_csv=self.cex_path,
            attempts=attempts,
        )

        loaded, metadata = _attempt_source(
            path=ledger,
            market_type="cex",
            source_csv_sha256=hashlib.sha256(self.cex_path.read_bytes()).hexdigest(),
        )

        self.assertEqual(loaded, [])
        self.assertEqual(metadata["status"], "ignored_invalid")

    def test_attempt_selection_uses_actual_utc_completion_instant(self):
        earlier = cex_attempt(
            "2026-07-19",
            attempt_id="earlier",
            finished_at_utc="2026-07-20T01:00:00+02:00",
        )
        later = cex_attempt(
            "2026-07-19",
            attempt_id="later",
            finished_at_utc="2026-07-20T00:30:00Z",
        )

        selected = attempt_for_gap(
            [later, earlier],
            cex_market(cex_row("2026-07-19")),
            date(2026, 7, 19),
        )

        self.assertEqual(selected["attempt_id"], "later")

    def test_cex_attempt_evidence_requires_the_exact_source_instrument(self):
        valid = cex_attempt(
            "2026-07-19",
            exchange="upbit",
            instrument="AAVE/USDT",
            source_instrument="AAVE/USDT",
            source_instrument_alias_validated=False,
        )
        normalized = normalize_collection_attempts([valid], market_type="cex")
        self.assertEqual(normalized[0]["source_instrument"], "AAVE/USDT")
        self.assertFalse(normalized[0]["source_instrument_alias_validated"])

        invalid = (
            {"source_instrument": "AAVE/KRW"},
            {
                "source_instrument": "AAVE/KRW",
                "source_instrument_alias_validated": True,
            },
            {"source_instrument": "UNI/KRW"},
            {"source_instrument": "AAVE/USDT", "source_instrument_alias_validated": True},
            {"source_instrument": None, "source_instrument_alias_validated": True},
        )
        for mutation in invalid:
            with self.subTest(mutation=mutation):
                candidate = dict(valid)
                candidate.update(mutation)
                with self.assertRaises(ValueError):
                    normalize_collection_attempts([candidate], market_type="cex")

    def test_missing_or_noncanonical_window_dates_and_count_contract_invalidate_ledger(self):
        invalid = {
            "missing_window": {"requested_start_date": None},
            "noncanonical_window": {"requested_start_date": "2026-7-19"},
            "noncanonical_observed_date": {"observed_dates": ["2026-7-19"], "observed_day_count": 1},
            "succeeded_without_full_window": {
                "status": "succeeded", "outcome": "observed", "reason_code": "observed",
                "error": None, "http_status": None,
            },
            "no_data_with_observation": {
                "status": "no_data", "outcome": "no_candles", "reason_code": "no_candles",
                "error": "The source returned no daily candles inside the requested window.",
                "observed_dates": ["2026-07-19"], "observed_day_count": 1,
            },
        }
        for name, mutation in invalid.items():
            with self.subTest(mutation=name):
                candidate = cex_attempt("2026-07-19", **mutation)
                with self.assertRaises(ValueError):
                    normalize_collection_attempts([candidate], market_type="cex")

    def test_partial_and_no_data_use_only_their_exact_outcomes(self):
        invalid = (
            {"status": "partial", "outcome": "no_candles", "observed_dates": ["2026-07-19"], "observed_day_count": 1},
            {"status": "no_data", "outcome": "partial_observation"},
        )
        for mutation in invalid:
            with self.subTest(mutation=mutation):
                candidate = cex_attempt("2026-07-19", **mutation)
                candidate["reason_code"] = "no_candles"
                candidate["error"] = "The source returned no daily candles inside the requested window."
                with self.assertRaises(ValueError):
                    normalize_collection_attempts([candidate], market_type="cex")

    def test_malformed_canonical_cex_instrument_is_invalid(self):
        invalid = (
            "AAVEUSDT",
            "AAVE/",
            "/USDT",
            "AAVE//USDT",
            " AAVE/USDT",
            "AAVE/USDT ",
            "AA VE/USDT",
            "AAVE/US DT",
            "AAVE/USDT\n",
            "AAVE/US\x00DT",
            "ÅAVE/USDT",
            "AAVE/ＵＳＤＴ",
            "A" * 33 + "/USDT",
            "AAVE/" + "U" * 33,
        )
        for instrument in invalid:
            with self.subTest(instrument=instrument), self.assertRaises(ValueError):
                normalize_collection_attempts(
                    [cex_attempt("2026-07-19", instrument=instrument)], market_type="cex"
                )

    def test_canonical_cex_instrument_accepts_ascii_grammar_and_exact_bounds(self):
        normalized = normalize_collection_attempts(
            [
                cex_attempt(
                    "2026-07-19",
                    instrument="a.b_c-d/usdt",
                    source_instrument="a.b_c-d/usdt",
                    source_instrument_alias_validated=False,
                ),
                cex_attempt(
                    "2026-07-20",
                    attempt_id="boundary-pair",
                    instrument="A" * 32 + "/" + "Q" * 32,
                ),
            ],
            market_type="cex",
        )
        self.assertEqual(normalized[0]["instrument"], "A.B_C-D/USDT")
        self.assertEqual(normalized[0]["source_instrument"], "A.B_C-D/USDT")
        self.assertEqual(len(normalized[1]["instrument"]), 65)

    def test_source_instrument_uses_the_same_strict_ascii_pair_grammar(self):
        invalid_sources = (
            "",
            " AAVE/KRW",
            "AAVE/KR W",
            "AAVE/KRW\n",
            "AAVE/KR\x01W",
            "AAVÉ/KRW",
            "A" * 33 + "/KRW",
            "AAVE/" + "K" * 33,
        )
        for source in invalid_sources:
            with self.subTest(source=source), self.assertRaises(ValueError):
                normalize_collection_attempts(
                    [
                        cex_attempt(
                            "2026-07-19",
                            exchange="upbit",
                            source_instrument=source,
                            source_instrument_alias_validated=True,
                        )
                    ],
                    market_type="cex",
                )

    def test_z_completion_time_is_normalized_to_canonical_utc(self):
        normalized = normalize_collection_attempts(
            [cex_attempt("2026-07-19", finished_at_utc="2026-07-20T00:30:00Z")],
            market_type="cex",
        )
        self.assertEqual(normalized[0]["finished_at_utc"], "2026-07-20T00:30:00+00:00")

    def test_dex_attempt_cannot_cross_dex_adapter_identity(self):
        attempt = dex_attempt("2026-07-19", dex="curve")
        market = {
            "market_type": "dex", "token_symbol": "AAVE", "chain": "eth",
            "dex": "uniswap_v3", "pool_address": "0xaavepool",
        }
        self.assertFalse(_attempt_matches_market(attempt, market))

    def test_whole_invalid_ledger_leaves_gap_unexplained(self):
        write_csv(self.cex_path, CEX_COLUMNS, [
            cex_row("2026-07-16"), cex_row("2026-07-17"), cex_row("2026-07-18"),
        ])
        ledger = self.root / "invalid-whole-ledger.json"
        write_attempt_ledger(
            ledger, market_type="cex", source_csv=self.cex_path,
            attempts=[cex_attempt("2026-07-19", attempt_id="")],
        )
        report = build_report(
            self.cex_path, self.dex_path, cex_attempts=ledger, today=date(2026, 7, 20)
        )
        issue = next(item for item in report["issues"] if item["category"] == "d1_active_gap")
        self.assertEqual(report["attempt_sources"][0]["status"], "ignored_invalid")
        self.assertEqual(issue["reason_code"], "missing_unexplained")

    def test_upbit_review_hints_match_only_the_exact_fact_market(self):
        hints = source_url_hints(
            {
                "market_type": "cex",
                "exchange": "upbit",
                "instrument": "LDO/USDT",
            }
        )

        self.assertEqual(len(hints), 2)
        self.assertIn("market=USDT-LDO", hints[0])
        self.assertEqual(
            hints[1],
            "https://api.upbit.com/v1/market/all?is_details=true",
        )

    def test_lineage_matched_rate_limit_attempt_explains_d1_gap(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-16"),
                cex_row("2026-07-17"),
                cex_row("2026-07-18"),
            ],
        )
        attempts_path = self.root / "cex-attempts.json"
        write_attempt_ledger(
            attempts_path,
            market_type="cex",
            source_csv=self.cex_path,
            attempts=[cex_attempt("2026-07-19")],
        )

        report = build_report(
            self.cex_path,
            self.dex_path,
            cex_attempts=attempts_path,
            today=date(2026, 7, 20),
        )

        issue = next(
            item
            for item in report["issues"]
            if item["category"] == "d1_active_gap"
        )
        self.assertEqual(issue["status"], "collection_failed")
        self.assertEqual(issue["reason_code"], "rate_limit")
        self.assertTrue(issue["retryable"])
        self.assertEqual(
            issue["details"]["collection_attempt"]["http_status"],
            429,
        )
        self.assertEqual(report["attempt_sources"][0]["status"], "accepted")
        self.assertEqual(
            report["collection_attempt_summary"]["reason_code_counts"],
            {"rate_limit": 1},
        )

    def test_stale_attempt_ledger_cannot_explain_a_gap(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-16"),
                cex_row("2026-07-17"),
                cex_row("2026-07-18"),
            ],
        )
        attempts_path = self.root / "cex-attempts.json"
        write_attempt_ledger(
            attempts_path,
            market_type="cex",
            source_csv=self.cex_path,
            source_sha256="0" * 64,
            attempts=[cex_attempt("2026-07-19")],
        )

        report = build_report(
            self.cex_path,
            self.dex_path,
            cex_attempts=attempts_path,
            today=date(2026, 7, 20),
        )

        issue = next(
            item
            for item in report["issues"]
            if item["category"] == "d1_active_gap"
        )
        self.assertEqual(issue["reason_code"], "missing_unexplained")
        self.assertEqual(
            report["attempt_sources"][0]["status"],
            "ignored_stale",
        )

    def test_not_listed_attempt_is_non_retryable_manual_review(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-16"),
                cex_row("2026-07-17"),
                cex_row("2026-07-18"),
            ],
        )
        attempts_path = self.root / "cex-attempts.json"
        write_attempt_ledger(
            attempts_path,
            market_type="cex",
            source_csv=self.cex_path,
            attempts=[
                cex_attempt(
                    "2026-07-19",
                    status="failed",
                    reason_code="not_listed",
                    http_status=404,
                    error=(
                        "The source reported that the requested market was unavailable."
                    ),
                )
            ],
        )

        report = build_report(
            self.cex_path,
            self.dex_path,
            cex_attempts=attempts_path,
            today=date(2026, 7, 20),
        )

        issue = next(
            item
            for item in report["issues"]
            if item["category"] == "d1_active_gap"
        )
        self.assertEqual(issue["status"], "needs_review")
        self.assertFalse(issue["retryable"])
        self.assertEqual(report["retry_windows_by_token"], {})
        self.assertEqual(report["summary"]["manual_review_count"], 1)

    def test_source_range_unavailable_is_not_a_network_failure(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-16"),
                cex_row("2026-07-17"),
                cex_row("2026-07-18"),
            ],
        )
        attempts_path = self.root / "cex-attempts.json"
        write_attempt_ledger(
            attempts_path,
            market_type="cex",
            source_csv=self.cex_path,
            attempts=[
                cex_attempt(
                    "2026-07-19",
                    status="unsupported",
                    outcome="range_unavailable",
                    reason_code="source_range_unavailable",
                    http_status=None,
                    error=(
                        "The source endpoint cannot reach the requested date window."
                    ),
                )
            ],
        )

        report = build_report(
            self.cex_path,
            self.dex_path,
            cex_attempts=attempts_path,
            today=date(2026, 7, 20),
        )

        issue = next(
            item
            for item in report["issues"]
            if item["category"] == "d1_active_gap"
        )
        self.assertEqual(issue["status"], "unsupported")
        self.assertEqual(
            issue["reason_code"],
            "source_range_unavailable",
        )
        self.assertFalse(issue["retryable"])
        self.assertEqual(report["retry_windows_by_token"], {})
        self.assertEqual(report["summary"]["manual_review_count"], 0)
        self.assertEqual(report["status"], "ok")

    def test_dex_no_candles_attempt_explains_historical_gap(self):
        write_csv(
            self.dex_path,
            DEX_COLUMNS,
            [
                dex_row("2026-07-06"),
                dex_row("2026-07-08"),
            ],
        )
        attempts_path = self.root / "dex-attempts.json"
        write_attempt_ledger(
            attempts_path,
            market_type="dex",
            source_csv=self.dex_path,
            attempts=[dex_attempt("2026-07-07")],
        )

        report = build_report(
            self.cex_path,
            self.dex_path,
            dex_attempts=attempts_path,
            today=date(2026, 7, 20),
        )

        issue = next(
            item
            for item in report["issues"]
            if item["category"] == "historical_gap"
        )
        self.assertEqual(issue["status"], "source_no_observation")
        self.assertEqual(issue["reason_code"], "no_candles")
        self.assertFalse(issue["retryable"])
        self.assertEqual(report["retry_windows_by_token"], {})

    def test_dex_source_range_unavailable_is_not_retryable(self):
        write_csv(
            self.dex_path,
            DEX_COLUMNS,
            [
                dex_row("2026-07-06"),
                dex_row("2026-07-08"),
            ],
        )
        attempts_path = self.root / "dex-attempts.json"
        write_attempt_ledger(
            attempts_path,
            market_type="dex",
            source_csv=self.dex_path,
            attempts=[
                dex_attempt(
                    "2026-07-07",
                    status="unsupported",
                    outcome="range_unavailable",
                    reason_code="source_range_unavailable",
                    http_status=401,
                    error=(
                        "The public OHLCV endpoint does not permit the requested "
                        "historical date window."
                    ),
                )
            ],
        )

        report = build_report(
            self.cex_path,
            self.dex_path,
            dex_attempts=attempts_path,
            today=date(2026, 7, 20),
        )

        issue = next(
            item
            for item in report["issues"]
            if item["category"] == "historical_gap"
        )
        self.assertEqual(issue["status"], "unsupported")
        self.assertEqual(
            issue["reason_code"],
            "source_range_unavailable",
        )
        self.assertEqual(
            issue["details"]["collection_attempt"]["outcome"],
            "range_unavailable",
        )
        self.assertFalse(issue["retryable"])
        self.assertEqual(report["retry_windows_by_token"], {})
        self.assertEqual(report["summary"]["manual_review_count"], 0)
        self.assertEqual(report["status"], "ok")

    def test_aave_three_day_historical_gap_is_explicit_and_retryable(self):
        rows = [
            dex_row("2026-07-06"),
            dex_row("2026-07-07"),
            dex_row("2026-07-11"),
            dex_row("2026-07-12"),
        ]
        write_csv(self.dex_path, DEX_COLUMNS, rows)

        report = self.report()
        gaps = [
            issue
            for issue in report["issues"]
            if issue["category"] == "historical_gap"
        ]

        self.assertEqual(
            [issue["date"] for issue in gaps],
            ["2026-07-08", "2026-07-09", "2026-07-10"],
        )
        self.assertTrue(all(issue["status"] == "backfill_pending" for issue in gaps))
        self.assertTrue(all(issue["retryable"] is True for issue in gaps))
        self.assertEqual(report["summary"]["historical_gap_count"], 3)
        self.assertIn("api.geckoterminal.com", gaps[0]["source_url_hints"][0])
        self.assertEqual(
            report["retry_windows_by_token"]["AAVE"][0]["market_types"],
            ["dex"],
        )

    def test_dates_before_first_observation_are_not_prelisting_gaps(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-05"),
                cex_row("2026-07-06"),
                cex_row("2026-07-07"),
            ],
        )

        report = self.report()

        self.assertEqual(report["summary"]["historical_gap_count"], 0)
        market = report["markets"][0]
        self.assertEqual(market["first_observed_date"], "2026-07-05")
        self.assertEqual(market["historical_gap_count"], 0)

    def test_current_and_future_dates_are_hard_invalid_and_excluded_from_gap_range(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-19"),
                cex_row("2026-07-20"),
                cex_row("9999-12-31"),
            ],
        )

        report = self.report(today=date(2026, 7, 20))
        future_issues = [
            issue
            for issue in report["issues"]
            if issue["reason_code"] == "incomplete_or_future_date"
        ]

        self.assertEqual(
            [issue["date"] for issue in future_issues],
            ["2026-07-20", "9999-12-31"],
        )
        self.assertTrue(
            all(
                issue["category"] == "hard_invalid"
                and issue["retryable"] is False
                for issue in future_issues
            )
        )
        self.assertEqual(report["summary"]["historical_gap_count"], 0)
        self.assertEqual(report["markets"][0]["first_observed_date"], "2026-07-19")
        self.assertEqual(report["markets"][0]["last_observed_date"], "2026-07-19")

    def test_negative_and_non_finite_pool_tvl_are_hard_invalid(self):
        write_csv(
            self.dex_path,
            DEX_COLUMNS,
            [
                dex_row("2026-07-18", pool_tvl_usd="-1"),
                dex_row("2026-07-19", pool_tvl_usd="NaN"),
            ],
        )

        report = self.report(today=date(2026, 7, 20))
        tvl_issues = [
            issue
            for issue in report["issues"]
            if issue["reason_code"] == "invalid_non_negative_pool_tvl"
        ]

        self.assertEqual(len(tvl_issues), 2)
        self.assertEqual(
            [
                issue["details"]["observed_values"]["pool_tvl_usd"]
                for issue in tvl_issues
            ],
            ["-1", "NaN"],
        )
        self.assertTrue(
            all(
                issue["category"] == "hard_invalid"
                and issue["retryable"] is False
                for issue in tvl_issues
            )
        )

    def test_hard_invalid_values_and_duplicate_keys_enter_manual_review(self):
        invalid = cex_row(
            "2026-07-05",
            open="-1",
            high="90",
            low="110",
            close="100",
            quote_volume_usd="-5",
        )
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [invalid, dict(invalid)],
        )

        report = self.report()
        reason_codes = {
            issue["reason_code"]
            for issue in report["issues"]
            if issue["category"] == "hard_invalid"
        }

        self.assertIn("invalid_positive_ohlc", reason_codes)
        self.assertIn("invalid_non_negative_volume", reason_codes)
        self.assertIn("duplicate_primary_key", reason_codes)
        self.assertGreaterEqual(report["summary"]["hard_invalid_count"], 3)
        self.assertEqual(
            report["summary"]["manual_review_count"],
            report["summary"]["hard_invalid_count"],
        )
        self.assertTrue(
            all(
                item["review_status"] == "pending"
                for item in report["manual_review_queue"]
            )
        )
        self.assertTrue(
            all(
                item["source_url_hints"]
                for item in report["manual_review_queue"]
            )
        )

    def test_cex_token_must_match_the_exact_instrument_base_asset(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [cex_row("2026-07-19", token_symbol="AAVE", cex_symbol="BTC/USDT")],
        )

        report = self.report()
        mismatch = [
            issue for issue in report["issues"]
            if issue["reason_code"] == "cex_token_instrument_mismatch"
        ]

        self.assertEqual(len(mismatch), 1)
        self.assertEqual(mismatch[0]["category"], "hard_invalid")
        self.assertFalse(mismatch[0]["retryable"])
        self.assertEqual(report["markets"], [])

    def test_coinbase_and_kraken_rows_must_preserve_the_actual_usd_quote(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row(
                    "2026-07-18",
                    token_symbol="AAVE",
                    exchange="coinbase",
                    cex_symbol="AAVE/USDT",
                ),
                cex_row(
                    "2026-07-19",
                    token_symbol="AAVE",
                    exchange="kraken",
                    cex_symbol="AAVE/USDT",
                ),
            ],
        )

        report = self.report()
        mismatches = [
            issue for issue in report["issues"]
            if issue["reason_code"] == "cex_source_instrument_mismatch"
        ]

        self.assertEqual(len(mismatches), 2)
        self.assertTrue(all(item["category"] == "hard_invalid" for item in mismatches))
        self.assertEqual(report["markets"], [])

    def test_one_physical_dex_pool_cannot_drift_between_dex_labels(self):
        write_csv(
            self.dex_path,
            DEX_COLUMNS,
            [
                dex_row("2026-07-18", dex="uniswap_v3"),
                dex_row("2026-07-19", dex="uniswap-v3-ethereum"),
            ],
        )

        report = self.report()
        drift = [
            issue for issue in report["issues"]
            if issue["reason_code"] == "dex_pool_label_drift"
        ]

        self.assertEqual(len(drift), 1)
        self.assertEqual(drift[0]["category"], "hard_invalid")
        self.assertFalse(drift[0]["retryable"])
        self.assertEqual(report["markets"], [])

    def test_dex_pool_label_drift_quarantines_every_token_perspective(self):
        write_csv(
            self.dex_path,
            DEX_COLUMNS,
            [
                dex_row(
                    "2026-07-18",
                    token_symbol="AAVE",
                    dex="uniswap_v3",
                ),
                dex_row(
                    "2026-07-19",
                    token_symbol="CRV",
                    dex="uniswap-v3-ethereum",
                ),
            ],
        )

        report = self.report()
        drift = [
            issue for issue in report["issues"]
            if issue["reason_code"] == "dex_pool_label_drift"
        ]

        self.assertEqual(len(drift), 1)
        self.assertEqual(
            drift[0]["details"]["market_ids"],
            [
                "dex:eth:uniswap-v3-ethereum:0xaavepool:CRV",
                "dex:eth:uniswap_v3:0xaavepool:AAVE",
            ],
        )
        self.assertEqual(report["markets"], [])

    def test_inconsistent_high_low_is_a_separate_hard_error(self):
        write_csv(
            self.dex_path,
            DEX_COLUMNS,
            [
                dex_row(
                    "2026-07-05",
                    open="100",
                    high="99",
                    low="101",
                    close="100",
                )
            ],
        )

        report = self.report()

        issue = next(
            issue
            for issue in report["issues"]
            if issue["reason_code"] == "inconsistent_ohlc_bounds"
        )
        self.assertEqual(issue["status"], "invalid")
        self.assertFalse(issue["retryable"])

    def test_d1_gap_requires_an_active_market_and_is_separate(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-03"),
                cex_row("2026-07-04"),
                cex_row("2026-07-05"),
                cex_row("2026-07-06"),
                cex_row("2026-07-07"),
                cex_row("2026-07-08"),
            ],
        )

        report = self.report(today=date(2026, 7, 10))
        d1 = [
            issue
            for issue in report["issues"]
            if issue["category"] == "d1_active_gap"
        ]

        self.assertEqual(len(d1), 1)
        self.assertEqual(d1[0]["date"], "2026-07-09")
        self.assertEqual(d1[0]["reason_code"], "missing_unexplained")
        self.assertTrue(d1[0]["retryable"])
        self.assertEqual(report["summary"]["d1_active_gap_count"], 1)

        still_unresolved = self.report(today=date(2026, 8, 10))
        stale = [
            issue
            for issue in still_unresolved["issues"]
            if issue["category"] == "stale_market_unknown"
        ]
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["date"], "2026-08-09")
        self.assertEqual(
            stale[0]["reason_code"],
            "stale_market_lifecycle_unknown",
        )
        self.assertFalse(stale[0]["retryable"])
        self.assertEqual(
            still_unresolved["markets"][0]["stale_market_unknown"],
            True,
        )

    def test_stale_market_moves_to_manual_review_instead_of_disappearing(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-21"),
                cex_row("2026-07-22"),
                cex_row("2026-07-23"),
            ],
        )

        report = self.report(today=date(2026, 7, 29))
        stale = next(
            issue
            for issue in report["issues"]
            if issue["category"] == "stale_market_unknown"
        )

        self.assertFalse(stale["retryable"])
        self.assertEqual(report["retry_windows_by_token"], {})
        self.assertEqual(
            report["manual_review_queue"][0]["reason_code"],
            "stale_market_lifecycle_unknown",
        )

    def test_stale_market_with_latest_no_candle_evidence_is_informational(self):
        write_csv(
            self.dex_path,
            DEX_COLUMNS,
            [
                dex_row("2026-07-21"),
                dex_row("2026-07-22"),
                dex_row("2026-07-23"),
            ],
        )
        attempts_path = self.root / "dex-attempts.json"
        write_attempt_ledger(
            attempts_path,
            market_type="dex",
            source_csv=self.dex_path,
            attempts=[dex_attempt("2026-07-28")],
        )

        report = build_report(
            self.cex_path,
            self.dex_path,
            dex_attempts=attempts_path,
            today=date(2026, 7, 29),
        )

        issue = next(
            item
            for item in report["issues"]
            if item["category"] == "source_no_observation"
        )
        self.assertEqual(issue["status"], "source_no_observation")
        self.assertEqual(issue["reason_code"], "no_candles")
        self.assertFalse(issue["retryable"])
        self.assertEqual(
            issue["details"]["collection_attempt"]["attempt_id"],
            "attempt-dex",
        )
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["summary"]["stale_market_unknown_count"], 0)
        self.assertEqual(report["summary"]["source_no_observation_count"], 1)
        self.assertEqual(report["summary"]["manual_review_count"], 0)
        self.assertEqual(report["manual_review_queue"], [])
        self.assertFalse(report["markets"][0]["stale_market_unknown"])
        self.assertTrue(report["markets"][0]["source_no_observation"])

    def test_recently_active_trailing_days_remain_one_retry_window(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-23"),
                cex_row("2026-07-24"),
                cex_row("2026-07-25"),
            ],
        )

        report = self.report(today=date(2026, 7, 29))
        window = report["retry_windows_by_token"]["AAVE"][0]

        self.assertEqual(window["start_date"], "2026-07-26")
        self.assertEqual(window["end_date"], "2026-07-28")
        self.assertEqual(window["day_count"], 3)
        self.assertEqual(window["market_types"], ["cex"])
        self.assertEqual(window["reason_codes"], ["missing_unexplained"])

    def test_retry_windows_are_per_token_contiguous_and_never_exceed_180_days(self):
        start = date(2025, 1, 1)
        end = date(2026, 1, 1)
        write_csv(
            self.dex_path,
            DEX_COLUMNS,
            [
                dex_row(start.isoformat()),
                dex_row(end.isoformat()),
            ],
        )

        report = self.report(today=date(2026, 2, 1))
        windows = report["retry_windows_by_token"]["AAVE"]

        self.assertGreaterEqual(len(windows), 2)
        self.assertTrue(all(window["day_count"] <= 180 for window in windows))
        self.assertEqual(windows[0]["start_date"], (start + timedelta(days=1)).isoformat())
        self.assertEqual(windows[-1]["end_date"], (end - timedelta(days=1)).isoformat())
        self.assertTrue(
            all(window["reason_codes"] == ["missing_unexplained"] for window in windows)
        )

    def test_cli_emits_json_and_optional_failure_gates(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-05", close="NaN"),
            ],
        )
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(
                [
                    "--cex-csv",
                    str(self.cex_path),
                    "--dex-csv",
                    str(self.dex_path),
                    "--today",
                    "2026-07-20",
                    "--fail-on-hard",
                ]
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(payload["schema"], "fact_quality_report/v1")
        self.assertGreater(payload["summary"]["hard_invalid_count"], 0)

    def test_cli_can_fail_specifically_on_d1_active_gap(self):
        write_csv(
            self.cex_path,
            CEX_COLUMNS,
            [
                cex_row("2026-07-03"),
                cex_row("2026-07-04"),
                cex_row("2026-07-05"),
                cex_row("2026-07-06"),
                cex_row("2026-07-07"),
                cex_row("2026-07-08"),
            ],
        )
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(
                [
                    "--cex-csv",
                    str(self.cex_path),
                    "--dex-csv",
                    str(self.dex_path),
                    "--today",
                    "2026-07-10",
                    "--fail-on-d1",
                ]
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(payload["summary"]["hard_invalid_count"], 0)
        self.assertEqual(payload["summary"]["d1_active_gap_count"], 1)


if __name__ == "__main__":
    unittest.main()
