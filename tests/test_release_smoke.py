import argparse
import copy
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from scripts.check_dashboard_release import (
    ReleaseCheckError,
    ResponseMetrics,
    release_check,
    validate_comparison,
    validate_events,
    validate_execution,
    validate_quality,
    validate_screening_quality_parity,
    validate_summary,
    validate_token_catalog,
)


class DashboardReleaseSmokeTest(unittest.TestCase):
    def summary(self):
        return {
            "metadata": {
                "response_scope": "screener_summary",
                "summary_version": 2,
                "data_generation": "generation-1",
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
                "default_workspace_token": "AAVE",
                "token_count": 1,
                "catalog_market_count": 2,
            },
            "tokens": [{
                "token_symbol": "AAVE",
                "market_count": 2,
                "quality_status_counts": {"ok": 2},
                "quality_alert_counts": {"info": 1},
                "spread_comparable_days": 20,
                "primary_cex": {
                    "refresh_market_id": "cex:binance:AAVE/USDT",
                    "depth_status": "observed",
                    "depth_na_reason": "observed",
                    "depth_retryable": False,
                    "tvl_status": "not_applicable",
                    "tvl_na_reason": "cex_markets_do_not_have_pool_tvl",
                    "tvl_retryable": False,
                },
                "primary_dex": {
                    "refresh_market_id": "dex:eth:uniswap_v3:pool:AAVE",
                    "depth_status": "collection_failed",
                    "depth_na_reason": "source_unavailable",
                    "depth_retryable": True,
                    "tvl_status": "collection_failed",
                    "tvl_na_reason": "source_unavailable",
                    "tvl_retryable": True,
                },
            }],
        }

    def metrics(self, path="/api/markets/summary", raw=1000, wire=500):
        return ResponseMetrics(path, 1.0, wire, raw, True)

    def screening_quality(self, token="AAVE"):
        return {
            "metadata": {
                "contract_version": 4,
                "data_generation": "generation-1",
                "scope": "all",
            },
            "token_symbol": token,
            "markets": [
                {
                    "market_id": f"cex:binance:{token}/USDT",
                    "token_symbol": token,
                    "screening_quality_status": "ok",
                    "screening_quality_flags": [
                        {
                            "code": "depth_unavailable",
                            "severity": "info",
                            "category": "availability",
                            "message": (
                                "No executable-depth observation is available."
                            ),
                        }
                    ],
                },
                {
                    "market_id": f"dex:eth:uniswap_v3:pool:{token}",
                    "token_symbol": token,
                    "screening_quality_status": "ok",
                    "screening_quality_flags": [],
                },
            ],
        }

    def test_screening_quality_must_match_summary_counts(self):
        summary_row = self.summary()["tokens"][0]
        quality = self.screening_quality()
        parity = validate_screening_quality_parity(
            summary_row,
            quality,
            expected_generation="generation-1",
        )
        self.assertEqual(parity["market_count"], 2)
        self.assertEqual(
            parity["market_ids"],
            [
                "cex:binance:AAVE/USDT",
                "dex:eth:uniswap_v3:pool:AAVE",
            ],
        )
        self.assertEqual(parity["status_counts"], {"ok": 2})
        self.assertEqual(parity["alert_counts"], {"info": 1})

        fallback = copy.deepcopy(quality)
        fallback["markets"][0]["screening_quality_flags"] = []
        fallback["markets"][0]["screening_quality_status"] = "warning"
        with self.assertRaisesRegex(ReleaseCheckError, "fallback alert"):
            validate_screening_quality_parity(
                summary_row,
                fallback,
                expected_generation="generation-1",
            )

        status_mismatch = copy.deepcopy(quality)
        status_mismatch["markets"][1]["screening_quality_status"] = "critical"
        status_mismatch["markets"][1]["screening_quality_flags"] = [{
            "code": "depth_failed",
            "severity": "critical",
            "category": "data_health",
            "message": "The latest depth collection failed.",
        }]
        with self.assertRaisesRegex(ReleaseCheckError, "screening quality"):
            validate_screening_quality_parity(
                summary_row,
                status_mismatch,
                expected_generation="generation-1",
            )

        generation_mismatch = copy.deepcopy(quality)
        generation_mismatch["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation"):
            validate_screening_quality_parity(
                summary_row,
                generation_mismatch,
                expected_generation="generation-1",
            )

    def test_screening_quality_requires_contract_v4_before_generation_checks(self):
        quality = self.screening_quality()
        quality["metadata"]["contract_version"] = 3
        quality["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "contract v4"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                quality,
                expected_generation="generation-1",
            )

    def test_screening_quality_rejects_invalid_flag_contract(self):
        valid_flag = self.screening_quality()["markets"][0][
            "screening_quality_flags"
        ][0]
        cases = {
            "missing field": {key: value for key, value in valid_flag.items()
                              if key != "message"},
            "unknown field": {**valid_flag, "source_url": "redacted"},
            "bad severity": {**valid_flag, "severity": "error"},
            "non-string severity": {**valid_flag, "severity": []},
            "bad category": {**valid_flag, "category": "source_outcome"},
            "non-string category": {**valid_flag, "category": []},
            "bad code case": {**valid_flag, "code": "Depth_Unavailable"},
            "bad code punctuation": {**valid_flag, "code": "depth-unavailable"},
            "bad code unicode": {**valid_flag, "code": "dépth_unavailable"},
            "long code": {**valid_flag, "code": "a" * 65},
            "empty message": {**valid_flag, "message": ""},
            "long message": {**valid_flag, "message": "x" * 241},
            "url message": {**valid_flag, "message": "See https://example.test"},
            "path message": {**valid_flag, "message": "Read /private/data/a.json"},
            "generic path message": {**valid_flag, "message": "Read /srv/app/a.json"},
            "equals srv path": {
                **valid_flag,
                "message": "error=/srv/app/secret",
            },
            "colon var path": {
                **valid_flag,
                "message": "path:/var/lib/dashboard/data.json",
            },
            "colon tmp path": {
                **valid_flag,
                "message": "path:/tmp/secret",
            },
            "colon etc path": {
                **valid_flag,
                "message": "path:/etc/passwd",
            },
            "colon opt path": {
                **valid_flag,
                "message": "path:/opt/app/secret",
            },
            "equals home path": {
                **valid_flag,
                "message": "error=/home/ugs/secret",
            },
            "uppercase home path": {
                **valid_flag,
                "message": "ERROR=/HOME/UGS/SECRET",
            },
            "colon private path": {
                **valid_flag,
                "message": "path:/private/tmp/x",
            },
            "bracket users path": {
                **valid_flag,
                "message": "Read [/Users/name/key]",
            },
            "unc path": {
                **valid_flag,
                "message": r"Read \\server\share\secret",
            },
            "backslash path": {
                **valid_flag,
                "message": r"Read home\ugs\secret",
            },
            "control message": {**valid_flag, "message": "line one\nline two"},
            "unicode control message": {
                **valid_flag,
                "message": "hidden\u200bmarker",
            },
            "non-dict flag": "depth_unavailable",
        }
        for label, flag in cases.items():
            with self.subTest(label=label):
                quality = self.screening_quality()
                quality["markets"][0]["screening_quality_flags"] = [flag]
                with self.assertRaises(ReleaseCheckError):
                    validate_screening_quality_parity(
                        self.summary()["tokens"][0],
                        quality,
                        expected_generation="generation-1",
                    )

        safe_slash = self.screening_quality()
        safe_slash["markets"][0]["screening_quality_flags"][0]["message"] = (
            "CEX/DEX facts remain visible; measured values are not N/A."
        )
        validate_screening_quality_parity(
            self.summary()["tokens"][0],
            safe_slash,
            expected_generation="generation-1",
        )
        for safe_message in (
            "CEX/DEX and TVL/depth remain visible when the value is N/A.",
            "Punctuation such as :/ or [/] is not itself a source path.",
            "The A/B comparison uses 1/2 only as ordinary prose.",
        ):
            with self.subTest(safe_message=safe_message):
                safe = self.screening_quality()
                safe["markets"][0]["screening_quality_flags"][0][
                    "message"
                ] = safe_message
                validate_screening_quality_parity(
                    self.summary()["tokens"][0],
                    safe,
                    expected_generation="generation-1",
                )

    def test_screening_quality_rejects_bad_market_shapes_and_fallbacks(self):
        mutations = []

        selected_scope = self.screening_quality()
        selected_scope["metadata"]["scope"] = "selected"
        mutations.append(("all scope", selected_scope))

        missing_scope = self.screening_quality()
        missing_scope["metadata"].pop("scope")
        mutations.append(("all scope", missing_scope))

        missing_status = self.screening_quality()
        missing_status["markets"][0].pop("screening_quality_status")
        mutations.append(("screening quality fields", missing_status))

        unknown_screening = self.screening_quality()
        unknown_screening["markets"][0]["screening_quality_reasons"] = []
        mutations.append(("unknown screening", unknown_screening))

        bad_status = self.screening_quality()
        bad_status["markets"][0]["screening_quality_status"] = "unknown"
        mutations.append(("status", bad_status))

        non_string_status = self.screening_quality()
        non_string_status["markets"][0]["screening_quality_status"] = []
        mutations.append(("status", non_string_status))

        non_list_flags = self.screening_quality()
        non_list_flags["markets"][0]["screening_quality_flags"] = {}
        mutations.append(("flags", non_list_flags))

        duplicate_ids = self.screening_quality()
        duplicate_ids["markets"][1]["market_id"] = duplicate_ids["markets"][0][
            "market_id"
        ]
        mutations.append(("unique", duplicate_ids))

        empty_id = self.screening_quality()
        empty_id["markets"][0]["market_id"] = ""
        mutations.append(("market ID", empty_id))

        whitespace_id = self.screening_quality()
        whitespace_id["markets"][0]["market_id"] = " "
        mutations.append(("market ID", whitespace_id))

        wrong_count = self.screening_quality()
        wrong_count["markets"].pop()
        mutations.append(("market count", wrong_count))

        wrong_token = self.screening_quality()
        wrong_token["token_symbol"] = "UNI"
        mutations.append(("Token", wrong_token))

        missing_market_token = self.screening_quality()
        missing_market_token["markets"][0].pop("token_symbol")
        mutations.append(("market Token", missing_market_token))

        wrong_market_token = self.screening_quality()
        wrong_market_token["markets"][0]["token_symbol"] = "UNI"
        mutations.append(("market Token", wrong_market_token))

        for message, quality in mutations:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ReleaseCheckError, message):
                    validate_screening_quality_parity(
                        self.summary()["tokens"][0],
                        quality,
                        expected_generation="generation-1",
                    )

        info_status_without_flag = self.screening_quality()
        info_status_without_flag["markets"][1]["screening_quality_status"] = "info"
        with self.assertRaisesRegex(ReleaseCheckError, "fallback alert"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                info_status_without_flag,
                expected_generation="generation-1",
            )

        generation_first = copy.deepcopy(missing_status)
        generation_first["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                generation_first,
                expected_generation="generation-1",
            )

        generation_before_scope = copy.deepcopy(selected_scope)
        generation_before_scope["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                generation_before_scope,
                expected_generation="generation-1",
            )

    def test_screening_quality_zero_counts_normalize_and_bool_counts_fail(self):
        summary_row = copy.deepcopy(self.summary()["tokens"][0])
        summary_row["quality_status_counts"] = {
            "ok": 2,
            "info": 0,
            "warning": 0,
            "critical": 0,
        }
        summary_row["quality_alert_counts"] = {
            "info": 1,
            "warning": 0,
            "critical": 0,
        }
        validate_screening_quality_parity(
            summary_row,
            self.screening_quality(),
            expected_generation="generation-1",
        )

        for field in ("quality_status_counts", "quality_alert_counts"):
            with self.subTest(field=field):
                invalid = copy.deepcopy(summary_row)
                first_key = next(iter(invalid[field]))
                invalid[field][first_key] = True
                with self.assertRaisesRegex(ReleaseCheckError, "counts"):
                    validate_screening_quality_parity(
                        invalid,
                        self.screening_quality(),
                        expected_generation="generation-1",
                    )

        unknown_status = copy.deepcopy(summary_row)
        unknown_status["quality_status_counts"]["degraded"] = 0
        with self.assertRaisesRegex(ReleaseCheckError, "status counts"):
            validate_screening_quality_parity(
                unknown_status,
                self.screening_quality(),
                expected_generation="generation-1",
            )
        unknown_severity = copy.deepcopy(summary_row)
        unknown_severity["quality_alert_counts"]["error"] = 0
        with self.assertRaisesRegex(ReleaseCheckError, "alert counts"):
            validate_screening_quality_parity(
                unknown_severity,
                self.screening_quality(),
                expected_generation="generation-1",
            )

    def test_screening_quality_status_and_alert_mismatches_are_independent(self):
        quality = self.screening_quality()

        status_only = copy.deepcopy(self.summary()["tokens"][0])
        status_only["quality_status_counts"] = {"ok": 1, "warning": 1}
        with self.assertRaisesRegex(ReleaseCheckError, "status counts"):
            validate_screening_quality_parity(
                status_only,
                quality,
                expected_generation="generation-1",
            )

        alerts_only = copy.deepcopy(self.summary()["tokens"][0])
        alerts_only["quality_alert_counts"] = {"warning": 1}
        with self.assertRaisesRegex(ReleaseCheckError, "alert counts"):
            validate_screening_quality_parity(
                alerts_only,
                quality,
                expected_generation="generation-1",
            )

    def test_screening_quality_counts_every_flag_without_filtering_or_deduping(self):
        quality = self.screening_quality()
        info_flag = quality["markets"][0]["screening_quality_flags"][0]
        quality["markets"][0]["screening_quality_flags"] = [
            info_flag,
            copy.deepcopy(info_flag),
            {
                "code": "wide_quoted_spread",
                "severity": "warning",
                "category": "market_condition",
                "message": "Quoted CEX spread exceeds the quality threshold.",
            },
        ]
        summary_row = copy.deepcopy(self.summary()["tokens"][0])
        summary_row["quality_alert_counts"] = {"info": 2, "warning": 1}
        parity = validate_screening_quality_parity(
            summary_row,
            quality,
            expected_generation="generation-1",
        )
        self.assertEqual(parity["alert_counts"], {"info": 2, "warning": 1})

    def test_release_fetches_all_token_quality_once_and_retains_metrics(self):
        summary = self.summary()
        second_row = copy.deepcopy(summary["tokens"][0])
        second_row["token_symbol"] = "UNI"
        summary["tokens"].append(second_row)
        summary["metadata"]["token_count"] = 2
        summary["metadata"]["catalog_market_count"] = 4
        quality_by_token = {
            token: self.screening_quality(token)
            for token in ("AAVE", "UNI")
        }
        valid_quality_by_token = copy.deepcopy(quality_by_token)
        full_catalog = {
            "metadata": {"data_generation": "generation-1"},
            "markets": [
                {
                    "market_id": "cex:binance:AAVE/USDT",
                    "token_symbol": "AAVE",
                },
                {
                    "market_id": "dex:eth:uniswap_v3:pool:AAVE",
                    "token_symbol": "AAVE",
                },
                {
                    "market_id": "cex:binance:UNI/USDT",
                    "token_symbol": "UNI",
                },
                {
                    "market_id": "dex:eth:uniswap_v3:pool:UNI",
                    "token_symbol": "UNI",
                },
            ],
        }
        valid_full_markets = copy.deepcopy(full_catalog["markets"])
        event = {
            "token_symbol": "AAVE",
            "time": {
                "effective_date_start": "2026-01-10",
                "effective_date_end": "2026-01-10",
            },
            "lifecycle": "occurred",
        }
        all_events = {
            "coverage": {
                "covered_tokens": ["AAVE", "UNI"],
                "uncovered_tokens": [],
                "covered_token_count": 2,
            },
            "bundle_id": "a" * 24,
        }
        fetched_paths = []
        summary_state = {"count": 0, "tail_generation": None}

        def fake_fetch(_base_url, path, *, timeout):
            fetched_paths.append(path)
            if path == "/health":
                payload = {"status": "ok", "data_ready": True}
            elif path == "/api/markets/summary":
                summary_state["count"] += 1
                payload = copy.deepcopy(summary)
                if (
                    summary_state["count"] > 1
                    and summary_state["tail_generation"] is not None
                ):
                    payload["metadata"]["data_generation"] = summary_state[
                        "tail_generation"
                    ]
            elif path == "/api/markets/catalog":
                payload = full_catalog
            elif path.startswith("/api/markets/catalog?"):
                payload = {"token_summary": {}}
            elif path == "/api/markets/events":
                payload = all_events
            elif "scope=all" in path and path.startswith("/api/markets/quality?"):
                token = "AAVE" if "token=AAVE" in path else "UNI"
                payload = quality_by_token[token]
            else:
                payload = {}
            return payload, self.metrics(path)

        args = argparse.Namespace(
            base_url="https://dashboard.test",
            timeout=1.0,
            summary_raw_max=2_000,
            summary_gzip_max=1_000,
            token_raw_max=2_000,
            token_gzip_max=1_000,
        )
        markets = [
            {"market_id": "cex:binance:AAVE/USDT", "market_type": "cex"},
            {
                "market_id": "dex:eth:uniswap_v3:pool:AAVE",
                "market_type": "dex",
            },
        ]
        def run_release(*, bypass_summary_validation=False):
            summary_state["count"] = 0
            with ExitStack() as stack:
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.fetch_json",
                    side_effect=fake_fetch,
                ))
                if bypass_summary_validation:
                    stack.enter_context(patch(
                        "scripts.check_dashboard_release.validate_summary",
                        return_value=(
                            "AAVE",
                            "2026-01-01",
                            "2026-01-31",
                            "generation-1",
                        ),
                    ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_token_catalog",
                    return_value=markets,
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_events",
                    return_value=[event],
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_comparison"
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_quality"
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_execution"
                ))
                return release_check(args)

        result = run_release()

        all_quality_paths = [
            path
            for path in fetched_paths
            if path.startswith("/api/markets/quality?") and "scope=all" in path
        ]
        self.assertEqual(len(all_quality_paths), 2)
        self.assertEqual(
            {path.split("token=")[1].split("&")[0] for path in all_quality_paths},
            {"AAVE", "UNI"},
        )
        self.assertEqual(result["screening_quality_parity_count"], 2)
        self.assertEqual(result["screening_quality_market_count"], 4)
        metric_paths = [row["path"] for row in result["requests"]]
        self.assertTrue(set(all_quality_paths).issubset(metric_paths))
        self.assertEqual(
            metric_paths.count("/api/markets/summary"),
            2,
        )

        summary_state["tail_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation changed"):
            run_release()
        summary_state["tail_generation"] = None

        quality_by_token["UNI"]["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        full_catalog["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "full catalog generation"):
            run_release()
        full_catalog["metadata"]["data_generation"] = "generation-1"

        missing_catalog_generation = full_catalog.pop("metadata")
        with self.assertRaisesRegex(ReleaseCheckError, "full catalog generation"):
            run_release()
        full_catalog["metadata"] = missing_catalog_generation

        quality_by_token["UNI"]["markets"][0]["market_id"] = (
            quality_by_token["AAVE"]["markets"][0]["market_id"]
        )
        with self.assertRaisesRegex(ReleaseCheckError, "reused across Tokens"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        quality_by_token["AAVE"]["markets"][0]["market_id"] = (
            "cex:bogus:AAVE/USDT"
        )
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        aave_market_id = quality_by_token["AAVE"]["markets"][0]["market_id"]
        uni_market_id = quality_by_token["UNI"]["markets"][0]["market_id"]
        quality_by_token["AAVE"]["markets"][0]["market_id"] = uni_market_id
        quality_by_token["UNI"]["markets"][0]["market_id"] = aave_market_id
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        substituted_full_catalog = copy.deepcopy(valid_full_markets)
        substituted_full_catalog[0]["market_id"] = "cex:bogus:AAVE/USDT"
        full_catalog["markets"] = substituted_full_catalog
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        full_catalog["markets"] = copy.deepcopy(valid_full_markets)

        missing_market_token = copy.deepcopy(valid_full_markets)
        missing_market_token[0].pop("token_symbol")
        full_catalog["markets"] = missing_market_token
        with self.assertRaisesRegex(ReleaseCheckError, "market Token identity"):
            run_release()

        missing_token_catalog = copy.deepcopy(valid_full_markets)
        for market in missing_token_catalog:
            market["token_symbol"] = "AAVE"
        full_catalog["markets"] = missing_token_catalog
        with self.assertRaisesRegex(ReleaseCheckError, "Token inventory"):
            run_release()

        full_catalog["markets"] = copy.deepcopy(valid_full_markets[:3])
        with self.assertRaisesRegex(ReleaseCheckError, "catalog count"):
            run_release()

        full_catalog["markets"] = copy.deepcopy(valid_full_markets)
        summary["metadata"]["token_count"] = 1
        with self.assertRaisesRegex(ReleaseCheckError, "parity Token count"):
            run_release(bypass_summary_validation=True)
        summary["metadata"]["token_count"] = 2

        summary["metadata"]["catalog_market_count"] = 3
        with self.assertRaisesRegex(ReleaseCheckError, "parity market count"):
            run_release(bypass_summary_validation=True)

        summary["metadata"]["catalog_market_count"] = 4
        markets.pop()
        with self.assertRaisesRegex(ReleaseCheckError, "Token catalog inventory"):
            run_release()

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
        with self.assertRaisesRegex(ReleaseCheckError, "version is not 2"):
            validate_summary(
                {
                    **summary,
                    "metadata": {**summary["metadata"], "summary_version": 1},
                },
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "retryability"):
            broken = self.summary()
            broken["tokens"][0]["primary_cex"].pop("depth_retryable")
            validate_summary(
                broken,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "N/A outcome"):
            missing_reason = self.summary()
            missing_reason["tokens"][0]["primary_cex"].pop(
                "depth_na_reason"
            )
            validate_summary(
                missing_reason,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "N/A outcome"):
            mismatched_outcome = self.summary()
            mismatched_outcome["tokens"][0]["primary_dex"][
                "depth_na_reason"
            ] = "unsupported_protocol"
            validate_summary(
                mismatched_outcome,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "N/A outcome"):
            impossible_cex_tvl = self.summary()
            impossible_cex_tvl["tokens"][0]["primary_cex"].update(
                {
                    "tvl_status": "observed",
                    "tvl_na_reason": "observed",
                    "tvl_retryable": False,
                }
            )
            validate_summary(
                impossible_cex_tvl,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "not unique"):
            duplicated = self.summary()
            duplicated["tokens"].append(copy.deepcopy(duplicated["tokens"][0]))
            duplicated["metadata"]["token_count"] = 2
            duplicated["metadata"]["catalog_market_count"] = 4
            validate_summary(
                duplicated,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )

        count_mutations = (
            ("token_count", None),
            ("token_count", True),
            ("token_count", 2),
            ("catalog_market_count", None),
            ("catalog_market_count", True),
            ("catalog_market_count", 1),
        )
        for field, value in count_mutations:
            with self.subTest(field=field, value=value):
                invalid = self.summary()
                if value is None:
                    invalid["metadata"].pop(field)
                else:
                    invalid["metadata"][field] = value
                with self.assertRaisesRegex(ReleaseCheckError, field):
                    validate_summary(
                        invalid,
                        self.metrics(),
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
                "data_generation": "generation-1",
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
            expected_generation="generation-1",
        )
        stale_comparison = copy.deepcopy(comparison)
        stale_comparison["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generations differ"):
            validate_comparison(
                stale_comparison,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                start="2026-01-01",
                end="2026-01-31",
                expected_generation="generation-1",
            )
        with self.assertRaisesRegex(ReleaseCheckError, "no daily observations"):
            validate_comparison(
                {**comparison, "observations": []},
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                start="2026-01-01",
                end="2026-01-31",
                expected_generation="generation-1",
            )

        quality_markets = [
            {
                "market_id": market_id,
                "market_type": (
                    "cex" if market_id.startswith("cex:") else "dex"
                ),
                "token_symbol": "AAVE",
                "quality_status": "ok",
                "quality_flags": [],
                "screening_quality_status": "ok",
                "screening_quality_flags": [],
                "facts": {
                    fact_name: {
                        "status": (
                            "not_applicable"
                            if market_id.startswith("cex:")
                            and fact_name == "tvl"
                            else "observed"
                        ),
                        "reason_code": (
                            "cex_markets_do_not_have_pool_tvl"
                            if market_id.startswith("cex:")
                            and fact_name == "tvl"
                            else "observed"
                        ),
                        "retryable": False,
                        "action": None,
                        "quality_flags": [],
                    }
                    for fact_name in ("daily", "tvl", "depth", "execution")
                },
            }
            for market_id in (market_a, market_b)
        ]
        quality = {
            "token_symbol": "AAVE",
            "metadata": {
                "contract_version": 4,
                "data_generation": "generation-1",
                "scope": "selected",
                "selected_market_ids": [market_a, market_b],
                "daily_quality_report": {
                    "status": "matched",
                    "evidence_mode": "published_daily_audit",
                    "identity_status": "matched_current_import",
                    "schema": "fact_quality_report/v1",
                    "selected_window_issue_count": 0,
                    "reason_code_counts": {},
                    "status_counts": {},
                    "affected_date_count": 0,
                    "affected_dates": [],
                },
            },
            "markets": quality_markets,
        }
        validate_quality(
            quality,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        spoofed_market_type = copy.deepcopy(quality)
        spoofed_market_type["markets"][0]["market_type"] = "dex"
        spoofed_market_type["markets"][0]["facts"]["tvl"].update(
            {
                "status": "observed",
                "reason_code": "observed",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "market identity/type",
        ):
            validate_quality(
                spoofed_market_type,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        legacy_daily = copy.deepcopy(quality)
        legacy_daily["markets"][0]["facts"]["daily"].update(
            {
                "status": "legacy_ohlcv_snapshot",
                "reason_code": "legacy_ohlcv_snapshot",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                legacy_daily,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        unsupported_without_fact_evidence = copy.deepcopy(quality)
        unsupported_without_fact_evidence["metadata"][
            "daily_quality_report"
        ].update(
            {
                "selected_window_issue_count": 1,
                "reason_code_counts": {"source_range_unavailable": 1},
                "status_counts": {"unsupported": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
            }
        )
        unsupported_without_fact_evidence["markets"][0]["facts"][
            "daily"
        ].update(
            {
                "status": "unsupported",
                "reason_code": "source_range_unavailable",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "daily.*(?:action|evidence)",
        ):
            validate_quality(
                unsupported_without_fact_evidence,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        mixed_report = copy.deepcopy(quality)
        mixed_report["metadata"]["daily_quality_report"].update(
            {
                "selected_window_issue_count": 2,
                "reason_code_counts": {"network": 1, "not_listed": 1},
                "status_counts": {
                    "collection_failed": 1,
                    "needs_review": 1,
                },
                "affected_date_count": 2,
                "affected_dates": ["2026-01-15", "2026-01-16"],
            }
        )
        mixed_daily = mixed_report["markets"][0]["facts"]["daily"]
        mixed_daily.update(
            {
                "status": "collection_failed",
                "reason_code": "multiple_daily_quality_reasons",
                "retryable": True,
                "action": "operator_review_retry_queue",
            }
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "daily.*(?:action|evidence)",
        ):
            validate_quality(
                mixed_report,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        mixed_daily.update(
            {
                "action": "operator_review_retry_and_manual_queues",
                "daily_evidence_mode": "published_daily_audit",
                "issue_status_counts": {
                    "collection_failed": 1,
                    "needs_review": 1,
                },
                "reason_code_counts": {
                    "network": 1,
                    "not_listed": 1,
                },
                "affected_date_count": 2,
                "affected_dates": ["2026-01-15", "2026-01-16"],
            }
        )
        validate_quality(
            mixed_report,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        impossible_cex_tvl = copy.deepcopy(quality)
        impossible_cex_tvl["markets"][0]["facts"]["tvl"].update(
            {
                "status": "observed",
                "reason_code": "observed",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                impossible_cex_tvl,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        needs_review = copy.deepcopy(quality)
        needs_review["markets"][0]["facts"]["depth"].update(
            {
                "status": "needs_review",
                "reason_code": "not_listed",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                needs_review,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )
        needs_review["markets"][0]["facts"]["depth"][
            "action"
        ] = "operator_manual_review"
        validate_quality(
            needs_review,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        invalid_tuple = copy.deepcopy(quality)
        invalid_tuple["markets"][0]["facts"]["depth"].update(
            {
                "status": "unsupported",
                "reason_code": "network",
                "retryable": False,
                "action": None,
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                invalid_tuple,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        critical_flag = {
            "code": "depth_failed",
            "severity": "critical",
            "category": "data_health",
            "message": "Depth collection failed validation.",
            "observed_value": None,
            "threshold": None,
        }
        status_drift = copy.deepcopy(quality)
        status_drift["markets"][0]["quality_flags"] = [critical_flag]
        status_drift["markets"][0]["facts"]["depth"][
            "quality_flags"
        ] = [copy.deepcopy(critical_flag)]
        with self.assertRaisesRegex(ReleaseCheckError, "status.*flags"):
            validate_quality(
                status_drift,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        fact_flag_drift = copy.deepcopy(quality)
        fact_flag_drift["markets"][0]["quality_status"] = "critical"
        fact_flag_drift["markets"][0]["quality_flags"] = [critical_flag]
        with self.assertRaisesRegex(ReleaseCheckError, "fact flag projection"):
            validate_quality(
                fact_flag_drift,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        selected_contract_mutations = {
            "missing selected status": lambda row: row.pop("quality_status"),
            "missing selected flags": lambda row: row.pop("quality_flags"),
            "missing screening status": lambda row: row.pop(
                "screening_quality_status"
            ),
            "missing screening flags": lambda row: row.pop(
                "screening_quality_flags"
            ),
            "missing fact family": lambda row: row["facts"].pop("execution"),
            "unknown fact family": lambda row: row["facts"].update(
                {"funding": {"status": "observed"}}
            ),
            "fact without retryability": lambda row: row["facts"]["depth"].pop(
                "retryable"
            ),
        }
        for label, mutate in selected_contract_mutations.items():
            with self.subTest(label=label):
                invalid = copy.deepcopy(quality)
                mutate(invalid["markets"][0])
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "selected quality contract",
                ):
                    validate_quality(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                    )
        stale_quality = copy.deepcopy(quality)
        stale_quality["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generations differ"):
            validate_quality(
                stale_quality,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )
        v3_quality = copy.deepcopy(quality)
        v3_quality["metadata"]["contract_version"] = 3
        with self.assertRaisesRegex(ReleaseCheckError, "not v4"):
            validate_quality(
                v3_quality,
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

        for selected_ids in (
            [market_a, market_a, market_b],
            [market_a, market_b, "cex:other:AAVE/USDT"],
        ):
            with self.subTest(selected_ids=selected_ids):
                invalid = copy.deepcopy(quality)
                invalid["metadata"]["selected_market_ids"] = selected_ids
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "wrong selected markets",
                ):
                    validate_quality(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                    )

        bool_issue_count = copy.deepcopy(quality)
        bool_issue_count["metadata"]["daily_quality_report"][
            "selected_window_issue_count"
        ] = False
        with self.assertRaisesRegex(ReleaseCheckError, "reason/status counts"):
            validate_quality(
                bool_issue_count,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
            )

        bool_date_count = copy.deepcopy(quality)
        bool_date_count["metadata"]["daily_quality_report"][
            "affected_date_count"
        ] = False
        with self.assertRaisesRegex(ReleaseCheckError, "affected dates"):
            validate_quality(
                bool_date_count,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
            )

        for affected_date in (
            "2026-02-30",
            "2026-W01-1",
            "2026-1-01",
            "not-a-date",
        ):
            with self.subTest(affected_date=affected_date):
                invalid = copy.deepcopy(quality)
                report = invalid["metadata"]["daily_quality_report"]
                report["affected_date_count"] = 1
                report["affected_dates"] = [affected_date]
                with self.assertRaisesRegex(ReleaseCheckError, "affected dates"):
                    validate_quality(
                        invalid,
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
            "metadata": {"data_generation": "generation-1"},
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
            expected_generation="generation-1",
        )
        stale_execution = copy.deepcopy(execution)
        stale_execution["metadata"]["data_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generations differ"):
            validate_execution(
                stale_execution,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
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
                expected_generation="generation-1",
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
