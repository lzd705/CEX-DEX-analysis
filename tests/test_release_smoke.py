import argparse
import copy
import tempfile
import inspect
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from dashboard import server
from scripts.check_dashboard_release import (
    DAILY_FACT_EVIDENCE_FIELDS,
    ReleaseCheckError,
    ResponseMetrics,
    STATIC_ASSET_FILENAMES,
    _validate_daily_fact_evidence,
    fetch_static_asset_bundle,
    release_check,
    validate_comparison,
    validate_events,
    validate_execution,
    validate_exact_cex_market_identity,
    validate_quality,
    validate_screening_quality_parity,
    validate_summary,
    validate_token_catalog,
)
from scripts.cex_instrument_lifecycle import configured_market_ids_sha256
from scripts.static_asset_contract import PUBLIC_STATIC_ASSET_SOURCES


class DashboardReleaseSmokeTest(unittest.TestCase):
    def test_public_asset_check_excludes_protected_admin_bundle(self):
        self.assertIn("actions.css", STATIC_ASSET_FILENAMES)
        self.assertIn("actions.js", STATIC_ASSET_FILENAMES)
        self.assertNotIn("admin.css", STATIC_ASSET_FILENAMES)
        self.assertNotIn("admin.js", STATIC_ASSET_FILENAMES)

    def test_checker_fetches_exact_public_bundle_and_reproduces_server_hash(self):
        class AssetResponse:
            status = 200
            headers = {}

            def __init__(self, body):
                self.body = body

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, _limit=None):
                return self.body

        with tempfile.TemporaryDirectory() as directory_name:
            static_root = Path(directory_name)
            source_by_name = dict(PUBLIC_STATIC_ASSET_SOURCES)
            body_by_name = {}
            for served_name, source_path in PUBLIC_STATIC_ASSET_SOURCES:
                source = static_root / source_path
                source.parent.mkdir(parents=True, exist_ok=True)
                body = f"asset:{served_name}".encode("utf-8")
                source.write_bytes(body)
                body_by_name[served_name] = body

            requested = []

            def fake_urlopen(request, timeout):
                self.assertEqual(timeout, 1.0)
                served_name = request.full_url.split(
                    "https://dashboard.test/", 1
                )[1].split("?", 1)[0]
                requested.append(served_name)
                self.assertIn(served_name, source_by_name)
                return AssetResponse(body_by_name[served_name])

            with patch.object(server, "STATIC_ROOT", static_root):
                expected_sha = server._compute_static_asset_sha()
            with patch(
                "scripts.check_dashboard_release.urlopen",
                side_effect=fake_urlopen,
            ):
                actual_sha, metrics = fetch_static_asset_bundle(
                    "https://dashboard.test",
                    "a" * 12 + "-" + "b" * 12,
                    timeout=1.0,
                )

        self.assertEqual(actual_sha, expected_sha)
        self.assertEqual(requested, list(STATIC_ASSET_FILENAMES))
        self.assertEqual(len(metrics), len(STATIC_ASSET_FILENAMES))

    def freshness(self):
        checked_at = "2026-02-01T01:00:00+00:00"
        return {
            "checked_at": checked_at,
            "overall_status": "current",
            "common_comparable_end": "2026-01-31",
            "cex_daily": {
                "source": "cex_daily",
                "status": "current",
                "available_start": "2026-01-01",
                "available_end": "2026-01-31",
                "latest_completed_utc_day": "2026-01-31",
                "lag_days": 0,
                "max_lag_days": 1,
            },
            "dex_daily": {
                "source": "dex_daily",
                "status": "current",
                "available_start": "2026-01-01",
                "available_end": "2026-01-31",
                "latest_completed_utc_day": "2026-01-31",
                "lag_days": 0,
                "max_lag_days": 1,
            },
            **{
                source: {
                    "source": source,
                    "status": "current",
                    "observed_at": "2026-02-01T00:00:00+00:00",
                    "age_hours": 1.0,
                    "max_age_hours": maximum,
                }
                for source, maximum in (
                    ("dex_tvl", 26.0),
                    ("cex_depth", 2.0),
                    ("dex_depth", 2.0),
                    ("cex_execution", 2.0),
                    ("dex_execution", 2.0),
                )
            },
        }

    def summary(self):
        return {
            "metadata": {
                "response_scope": "screener_summary",
                "summary_version": 3,
                "data_generation": "generation-1",
                "start_date": "2026-01-01",
                "end_date": "2026-01-31",
                "default_workspace_token": "AAVE",
                "token_count": 1,
                "catalog_market_count": 2,
                "freshness": self.freshness(),
                "cex_instrument_lifecycle": {
                    "schema": "cex_instrument_lifecycle/v1",
                    "reviewed_market_count": 2,
                    "absence_market_count": 0,
                    "applied_market_count": 0,
                    "withheld_payload_market_count": 0,
                    "stale_evidence_market_count": 0,
                    "official_inventory_count": 1_000,
                    "response_sha256": "9" * 64,
                    "configured_market_ids_sha256": (
                        configured_market_ids_sha256(
                            {
                                "cex:crypto_com:AAVE/USDT",
                                "cex:crypto_com:UNI/USDT",
                            }
                        )
                    ),
                    "freshness_max_age_seconds": 129600,
                    "checked_at_min": "2026-02-01T00:30:00+00:00",
                    "checked_at_max": "2026-02-01T00:30:00+00:00",
                },
                "configured_cex_market_identities": {
                    "schema": "configured_cex_market_identities/v1",
                    "upbit": {
                        "market_count": 2,
                        "market_ids": [
                            "cex:upbit:AAVE/USDT",
                            "cex:upbit:UNI/USDT",
                        ],
                        "market_ids_sha256": (
                            "556bd70f57ba9cac453a87e26c2e5a1b"
                            "7098133cdfc1956cfad0e20dda693635"
                        ),
                    },
                },
            },
            "tokens": [{
                "token_symbol": "AAVE",
                "market_count": 2,
                "quality_status_counts": {"ok": 2},
                "quality_alert_counts": {"info": 1},
                "price_spread": 0.01,
                "price_spread_method": "directional_dex_over_cex_minus_one",
                "absolute_price_gap": 2 / 202,
                "absolute_price_gap_method": (
                    "symmetric_midpoint_relative_gap"
                ),
                "maximum_absolute_price_spread": 0.03,
                "mean_absolute_price_spread": 0.02,
                "median_absolute_price_spread": 0.015,
                "spread_comparable_days": 20,
                "primary_cex": {
                    "refresh_market_id": "cex:crypto_com:AAVE/USDT",
                    "token_symbol": "AAVE",
                    "venue": "crypto_com",
                    "instrument": "AAVE/USDT",
                    "depth_status": "observed",
                    "depth_na_reason": "observed",
                    "depth_retryable": False,
                    "tvl_status": "not_applicable",
                    "tvl_na_reason": "cex_markets_do_not_have_pool_tvl",
                    "tvl_retryable": False,
                },
                "primary_dex": {
                    "refresh_market_id": "dex:eth:uniswap_v3:pool:AAVE",
                    "token_symbol": "AAVE",
                    "venue": "eth / uniswap_v3",
                    "pool_address": "pool",
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
        def fact(status, reason_code, retryable=False, action=None):
            return {
                "status": status,
                "reason_code": reason_code,
                "retryable": retryable,
                "action": action,
                "quality_flags": [],
            }

        def daily_fact():
            return {
                **fact("observed", "observed"),
                "daily_evidence_mode": "published_daily_audit",
                "reason_code_counts": {},
                "issue_status_counts": {},
                "issue_outcome_counts": [],
                "affected_dates": [],
                "affected_date_count": 0,
            }

        market_ids = [
            f"cex:crypto_com:{token}/USDT",
            f"dex:eth:uniswap_v3:pool:{token}",
        ]
        zero_rollups = [
            {
                "market_id": market_id,
                "issue_count": 0,
                "reason_code_counts": {},
                "status_counts": {},
                "issue_outcome_counts": [],
                "affected_dates": [],
                "affected_date_count": 0,
                "evidence_mode": "published_daily_audit",
                "fact_outcome": {
                    "status": "observed",
                    "reason_code": "observed",
                    "retryable": False,
                    "action": None,
                },
            }
            for market_id in market_ids
        ]

        return {
            "metadata": {
                "contract_version": 4,
                "data_generation": "generation-1",
                "scope": "all",
                "daily_quality_report": {
                    "status": "matched",
                    "evidence_mode": "published_daily_audit",
                    "identity_status": "matched_current_import",
                    "schema": "fact_quality_report/v1",
                    "selected_window_issue_count": 0,
                    "reason_code_counts": {},
                    "status_counts": {},
                    "issue_outcome_counts": [],
                    "affected_dates": [],
                    "affected_date_count": 0,
                    "market_issue_rollups": zero_rollups,
                },
            },
            "token_symbol": token,
            "markets": [
                {
                    "market_id": f"cex:crypto_com:{token}/USDT",
                    "market_type": "cex",
                    "token_symbol": token,
                    "quality_status": "ok",
                    "quality_flags": [],
                    "facts": {
                        "daily": daily_fact(),
                        "tvl": fact(
                            "not_applicable",
                            "cex_markets_do_not_have_pool_tvl",
                        ),
                        "depth": fact("observed", "observed"),
                        "execution": fact("observed", "observed"),
                    },
                    "screening_quality_status": "ok",
                    "screening_quality_scope": "catalog",
                    "screening_quality_window": {
                        "start": "2026-01-01",
                        "end": "2026-01-31",
                        "method": "max_query_source_market_observed_start",
                    },
                    "screening_quality_flags": [
                        {
                            "code": "depth_unavailable",
                            "severity": "info",
                            "category": "availability",
                            "message": (
                                "No executable-depth observation is available."
                            ),
                            "observed_value": None,
                            "threshold": None,
                        }
                    ],
                },
                {
                    "market_id": f"dex:eth:uniswap_v3:pool:{token}",
                    "market_type": "dex",
                    "token_symbol": token,
                    "quality_status": "ok",
                    "quality_flags": [],
                    "facts": {
                        "daily": daily_fact(),
                        "tvl": fact("observed", "observed"),
                        "depth": fact(
                            "collection_failed",
                            "source_unavailable",
                            True,
                            "retry_depth_collection",
                        ),
                        "execution": fact(
                            "unsupported",
                            "unsupported_protocol_or_chain",
                        ),
                    },
                    "screening_quality_status": "ok",
                    "screening_quality_scope": "catalog",
                    "screening_quality_window": {
                        "start": "2026-01-01",
                        "end": "2026-01-31",
                        "method": "max_query_source_market_observed_start",
                    },
                    "screening_quality_flags": [],
                },
            ],
        }

    def test_release_exact_cex_identity_rejects_legacy_quote_aliases(self):
        def validate(market_id, token, configured_upbit_market_ids):
            try:
                return validate_exact_cex_market_identity(
                    market_id,
                    token,
                    configured_upbit_market_ids=(
                        configured_upbit_market_ids
                    ),
                )
            except TypeError as error:
                self.fail(
                    "Exact identity validation must accept authoritative "
                    "Upbit configuration: {}".format(error)
                )

        valid = (
            ("cex:coinbase:AAVE/USD", "AAVE"),
            ("cex:kraken:AAVE/USD", "AAVE"),
            ("cex:binance:AAVE/USDT", "AAVE"),
        )
        for market_id, token in valid:
            with self.subTest(valid=market_id):
                validate(
                    market_id,
                    token,
                    {
                        "cex:upbit:AAVE/USDT",
                    },
                )

        for configured_market_id in (
            "cex:upbit:AAVE/USDT",
            "cex:upbit:AAVE/KRW",
        ):
            with self.subTest(configured_upbit=configured_market_id):
                try:
                    validate(
                        configured_market_id,
                        "AAVE",
                        {configured_market_id},
                    )
                except ReleaseCheckError as error:
                    self.fail(
                        "An explicitly configured Upbit quote is an exact "
                        "market identity: {}".format(error)
                    )

        invalid = (
            "cex:coinbase:AAVE/USDT",
            "cex:kraken:AAVE/USDT",
            "cex:coinbase:UNI/USD",
        )
        for market_id in invalid:
            with self.subTest(invalid=market_id), self.assertRaisesRegex(
                ReleaseCheckError,
                "exact CEX identity",
            ):
                validate(
                    market_id,
                    "AAVE",
                    {
                        "cex:upbit:AAVE/USDT",
                    },
                )

        try:
            validate(
                "cex:upbit:AAVE/KRW",
                "AAVE",
                {"cex:upbit:AAVE/USDT"},
            )
        except ReleaseCheckError:
            pass
        else:
            self.fail(
                "Upbit KRW must be rejected when the authoritative market is USDT"
            )

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
                "cex:crypto_com:AAVE/USDT",
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
            "observed_value": "collection_failed",
            "threshold": None,
        }]
        with self.assertRaisesRegex(ReleaseCheckError, "screening quality"):
            validate_screening_quality_parity(
                summary_row,
                status_mismatch,
                expected_generation="generation-1",
            )

        severity_status_drift = copy.deepcopy(quality)
        severity_status_drift["markets"][0]["screening_quality_status"] = "warning"
        severity_status_drift["markets"][0]["screening_quality_flags"][0][
            "severity"
        ] = "critical"
        severity_status_drift["markets"][0]["screening_quality_flags"][0][
            "category"
        ] = "data_health"
        drift_summary = copy.deepcopy(summary_row)
        drift_summary["quality_status_counts"] = {"ok": 1, "warning": 1}
        drift_summary["quality_alert_counts"] = {"critical": 1}
        with self.assertRaisesRegex(ReleaseCheckError, "status differs from its flags"):
            validate_screening_quality_parity(
                drift_summary,
                severity_status_drift,
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

    def test_screening_quality_validates_every_market_fact_family(self):
        quality = self.screening_quality()
        quality["markets"][1]["facts"]["tvl"].update(
            {
                "status": "failed",
                "reason_code": "execution_calculation_failed",
                "retryable": True,
                "action": "retry_tvl_collection",
            }
        )

        with self.assertRaisesRegex(ReleaseCheckError, "canonical fact"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                quality,
                expected_generation="generation-1",
            )

        selected_status_drift = self.screening_quality()
        selected_status_drift["markets"][1]["quality_status"] = "warning"
        with self.assertRaisesRegex(ReleaseCheckError, "selected quality"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                selected_status_drift,
                expected_generation="generation-1",
            )

        daily_count_drift = self.screening_quality()
        daily_count_drift["markets"][1]["facts"]["daily"][
            "affected_date_count"
        ] = 1
        with self.assertRaisesRegex(ReleaseCheckError, "daily fact"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                daily_count_drift,
                expected_generation="generation-1",
            )

        fallback_audit = self.screening_quality()
        fallback_audit["metadata"]["daily_quality_report"].update(
            {
                "status": "unavailable",
                "evidence_mode": "catalog_window_inference",
                "identity_status": "unavailable",
            }
        )
        for field in ("schema", "market_issue_rollups", "issue_outcome_counts"):
            fallback_audit["metadata"]["daily_quality_report"].pop(field, None)
        with self.assertRaisesRegex(ReleaseCheckError, "matched"):
            validate_screening_quality_parity(
                self.summary()["tokens"][0],
                fallback_audit,
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
        mutations.append(("duplicated|unique", duplicate_ids))

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
                "observed_value": 125.0,
                "threshold": 100.0,
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
        second_row["primary_cex"].update(
            {
                "token_symbol": "UNI",
                "instrument": "UNI/USDT",
                "refresh_market_id": "cex:crypto_com:UNI/USDT",
            }
        )
        second_row["primary_dex"].update(
            {
                "token_symbol": "UNI",
                "refresh_market_id": "dex:eth:uniswap_v3:pool:UNI",
            }
        )
        summary["tokens"].append(second_row)
        summary["metadata"]["token_count"] = 2
        summary["metadata"]["catalog_market_count"] = 4
        quality_by_token = {
            token: self.screening_quality(token)
            for token in ("AAVE", "UNI")
        }
        def rename_quality_market(payload, index, new_market_id):
            old_market_id = payload["markets"][index]["market_id"]
            payload["markets"][index]["market_id"] = new_market_id
            for rollup in payload["metadata"]["daily_quality_report"][
                "market_issue_rollups"
            ]:
                if rollup["market_id"] == old_market_id:
                    rollup["market_id"] = new_market_id
        valid_quality_by_token = copy.deepcopy(quality_by_token)
        full_catalog = {
            "metadata": {
                "data_generation": "generation-1",
                "configured_cex_market_identities": copy.deepcopy(
                    summary["metadata"]["configured_cex_market_identities"]
                ),
            },
            "markets": [
                {
                    "market_id": "cex:crypto_com:AAVE/USDT",
                    "token_symbol": "AAVE",
                },
                {
                    "market_id": "dex:eth:uniswap_v3:pool:AAVE",
                    "token_symbol": "AAVE",
                },
                {
                    "market_id": "cex:crypto_com:UNI/USDT",
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
        health_payload = {
            "status": "ok",
            "data_ready": True,
            "data_status": "current",
            "freshness": self.freshness(),
            "cex_instrument_lifecycle": copy.deepcopy(
                summary["metadata"]["cex_instrument_lifecycle"]
            ),
            "application_sha": "a" * 40,
            "asset_sha": "b" * 64,
            "asset_version": f"{'a' * 12}-{'b' * 12}",
        }
        fetched_paths = []
        served_asset_state = {"sha": "b" * 64}
        summary_state = {
            "count": 0,
            "tail_generation": None,
            "tail_freshness_stale": False,
        }

        def fake_fetch(_base_url, path, *, timeout):
            fetched_paths.append(path)
            if path == "/health":
                payload = copy.deepcopy(health_payload)
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
                if (
                    summary_state["count"] > 1
                    and summary_state["tail_freshness_stale"]
                ):
                    payload["metadata"]["freshness"]["overall_status"] = "stale"
                    payload["metadata"]["freshness"]["cex_depth"]["status"] = "stale"
                    payload["metadata"]["freshness"]["cex_depth"]["age_hours"] = 3.0
            elif path == "/api/markets/catalog":
                payload = full_catalog
            elif path.startswith("/api/markets/catalog?"):
                payload = {
                    "metadata": {
                        "configured_cex_market_identities": copy.deepcopy(
                            summary["metadata"][
                                "configured_cex_market_identities"
                            ]
                        ),
                    },
                    "token_summary": {},
                }
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
            expected_application_sha="a" * 40,
            expected_asset_sha="b" * 64,
        )
        markets = [
            {"market_id": "cex:crypto_com:AAVE/USDT", "market_type": "cex"},
            {
                "market_id": "dex:eth:uniswap_v3:pool:AAVE",
                "market_type": "dex",
            },
        ]
        validator_calls = {}
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
                    "scripts.check_dashboard_release.fetch_static_asset_bundle",
                    side_effect=lambda *_args, **_kwargs: (
                        served_asset_state["sha"],
                        [],
                    ),
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_events",
                    return_value=[event],
                ))
                comparison_validator = stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_comparison"
                ))
                stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_quality"
                ))
                execution_validator = stack.enter_context(patch(
                    "scripts.check_dashboard_release.validate_execution"
                ))
                result = release_check(args)
                validator_calls["comparison"] = comparison_validator.call_args
                validator_calls["execution"] = execution_validator.call_args
                return result

        result = run_release()

        self.assertEqual(result["application_sha"], "a" * 40)
        self.assertEqual(result["asset_sha"], "b" * 64)
        self.assertEqual(
            validator_calls["comparison"].kwargs[
                "expected_comparison_generation"
            ],
            "generation-1",
        )
        self.assertEqual(
            validator_calls["execution"].kwargs[
                "expected_execution_generation"
            ],
            "generation-1",
        )

        baseline_summary = copy.deepcopy(summary)
        baseline_quality_by_token = copy.deepcopy(quality_by_token)
        baseline_full_catalog = copy.deepcopy(full_catalog)
        baseline_token_markets = copy.deepcopy(markets)
        baseline_fetched_paths = list(fetched_paths)
        try:
            configured_krw = {
                "schema": "configured_cex_market_identities/v1",
                "upbit": {
                    "market_count": 2,
                    "market_ids": [
                        "cex:upbit:AAVE/KRW",
                        "cex:upbit:UNI/USDT",
                    ],
                    "market_ids_sha256": (
                        "440b52cffc9da70c7adaf402da4131c48"
                        "1e4356cb85ef9994da59d0a2f1f9154"
                    ),
                },
            }
            summary["metadata"]["configured_cex_market_identities"] = (
                copy.deepcopy(configured_krw)
            )
            summary["metadata"]["catalog_market_count"] = 5
            summary["tokens"][0]["market_count"] = 3
            summary["tokens"][0]["quality_status_counts"] = {"ok": 3}
            summary["tokens"][0]["quality_alert_counts"] = {"info": 2}

            upbit_quality = copy.deepcopy(
                quality_by_token["AAVE"]["markets"][0]
            )
            upbit_quality["market_id"] = "cex:upbit:AAVE/KRW"
            quality_by_token["AAVE"]["markets"].append(upbit_quality)
            upbit_rollup = copy.deepcopy(
                quality_by_token["AAVE"]["metadata"][
                    "daily_quality_report"
                ]["market_issue_rollups"][0]
            )
            upbit_rollup["market_id"] = "cex:upbit:AAVE/KRW"
            quality_by_token["AAVE"]["metadata"][
                "daily_quality_report"
            ]["market_issue_rollups"].append(upbit_rollup)

            full_catalog["metadata"][
                "configured_cex_market_identities"
            ] = copy.deepcopy(configured_krw)
            full_catalog["markets"].append({
                "market_id": "cex:upbit:AAVE/KRW",
                "token_symbol": "AAVE",
            })
            markets.append({
                "market_id": "cex:upbit:AAVE/KRW",
                "market_type": "cex",
            })

            try:
                run_release()
            except ReleaseCheckError as error:
                self.fail(
                    "Full release rejected configured Upbit KRW: {}".format(
                        error
                    )
                )

            configured_usdt = {
                "schema": "configured_cex_market_identities/v1",
                "upbit": {
                    "market_count": 2,
                    "market_ids": [
                        "cex:upbit:AAVE/USDT",
                        "cex:upbit:UNI/USDT",
                    ],
                    "market_ids_sha256": (
                        "556bd70f57ba9cac453a87e26c2e5a1b"
                        "7098133cdfc1956cfad0e20dda693635"
                    ),
                },
            }
            summary["metadata"]["configured_cex_market_identities"] = (
                copy.deepcopy(configured_usdt)
            )
            full_catalog["metadata"][
                "configured_cex_market_identities"
            ] = copy.deepcopy(configured_usdt)
            with self.assertRaisesRegex(
                ReleaseCheckError,
                "configured Upbit",
            ):
                run_release()
        finally:
            summary = baseline_summary
            quality_by_token = baseline_quality_by_token
            full_catalog = baseline_full_catalog
            markets = baseline_token_markets
            fetched_paths[:] = baseline_fetched_paths

        health_payload["data_status"] = "stale"
        health_payload["freshness"]["overall_status"] = "stale"
        health_payload["freshness"]["cex_depth"]["status"] = "stale"
        health_payload["freshness"]["cex_depth"]["age_hours"] = 3.0
        with self.assertRaisesRegex(ReleaseCheckError, "freshness"):
            run_release()
        health_payload["data_status"] = "current"
        health_payload["freshness"] = self.freshness()

        stale_lifecycle = copy.deepcopy(summary)
        stale_lifecycle["metadata"]["cex_instrument_lifecycle"][
            "stale_evidence_market_count"
        ] = 1
        original_summary = summary
        summary = stale_lifecycle
        try:
            with self.assertRaisesRegex(ReleaseCheckError, "lifecycle"):
                run_release()
        finally:
            summary = original_summary

        health_payload["application_sha"] = "c" * 40
        with self.assertRaisesRegex(ReleaseCheckError, "application SHA"):
            run_release()
        health_payload["application_sha"] = "a" * 40

        health_payload["asset_version"] = f"{'a' * 12}-{'c' * 12}"
        with self.assertRaisesRegex(ReleaseCheckError, "asset version"):
            run_release()
        health_payload["asset_version"] = f"{'a' * 12}-{'b' * 12}"

        served_asset_state["sha"] = "c" * 64
        with self.assertRaisesRegex(ReleaseCheckError, "served assets"):
            run_release()
        served_asset_state["sha"] = "b" * 64

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

        lifecycle = summary["metadata"]["cex_instrument_lifecycle"]
        valid_lifecycle_hash = lifecycle["configured_market_ids_sha256"]
        lifecycle["configured_market_ids_sha256"] = "0" * 64
        with self.assertRaisesRegex(ReleaseCheckError, "lifecycle catalog"):
            run_release()
        lifecycle["configured_market_ids_sha256"] = valid_lifecycle_hash

        lifecycle["reviewed_market_count"] = 1
        with self.assertRaisesRegex(ReleaseCheckError, "lifecycle catalog"):
            run_release()
        lifecycle["reviewed_market_count"] = 2

        summary_state["tail_generation"] = "generation-2"
        with self.assertRaisesRegex(ReleaseCheckError, "generation changed"):
            run_release()
        summary_state["tail_generation"] = None

        summary_state["tail_freshness_stale"] = True
        with self.assertRaisesRegex(ReleaseCheckError, "freshness"):
            run_release()
        summary_state["tail_freshness_stale"] = False

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

        rename_quality_market(
            quality_by_token["UNI"],
            0,
            quality_by_token["AAVE"]["markets"][0]["market_id"],
        )
        with self.assertRaisesRegex(ReleaseCheckError, "reused across Tokens"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        rename_quality_market(
            quality_by_token["AAVE"],
            0,
            "cex:bogus:AAVE/USDT",
        )
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        aave_market_id = quality_by_token["AAVE"]["markets"][0]["market_id"]
        uni_market_id = quality_by_token["UNI"]["markets"][0]["market_id"]
        rename_quality_market(quality_by_token["AAVE"], 0, uni_market_id)
        rename_quality_market(quality_by_token["UNI"], 0, aave_market_id)
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        quality_by_token = copy.deepcopy(valid_quality_by_token)

        substituted_full_catalog = copy.deepcopy(valid_full_markets)
        substituted_full_catalog[0]["market_id"] = "cex:bogus:AAVE/USDT"
        full_catalog["markets"] = substituted_full_catalog
        with self.assertRaisesRegex(ReleaseCheckError, "exact market inventory"):
            run_release()
        full_catalog["markets"] = copy.deepcopy(valid_full_markets)

        summary["tokens"][0]["primary_cex"].update(
            {
                "venue": "bogus",
                "refresh_market_id": "cex:bogus:AAVE/USDT",
            }
        )
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "Summary primary market.*full catalog",
        ):
            run_release()
        summary["tokens"][0]["primary_cex"].update(
            {
                "venue": "crypto_com",
                "refresh_market_id": "cex:crypto_com:AAVE/USDT",
            }
        )

        missing_market_token = copy.deepcopy(valid_full_markets)
        missing_market_token[0].pop("token_symbol")
        full_catalog["markets"] = missing_market_token
        with self.assertRaisesRegex(ReleaseCheckError, "market Token identity"):
            run_release()

        missing_token_catalog = copy.deepcopy(valid_full_markets)
        for market in missing_token_catalog:
            market["token_symbol"] = "AAVE"
            if market["market_id"] == "cex:crypto_com:UNI/USDT":
                market["market_id"] = "dex:eth:replacement:pool:AAVE"
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
        with self.assertRaisesRegex(ReleaseCheckError, "version is not 3"):
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
        with self.assertRaisesRegex(ReleaseCheckError, "refresh identity"):
            wrong_refresh = self.summary()
            wrong_refresh["tokens"][0]["primary_cex"][
                "refresh_market_id"
            ] = "cex:bogus:AAVE/USDT"
            validate_summary(
                wrong_refresh,
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
        with self.assertRaisesRegex(ReleaseCheckError, "N/A outcome"):
            fail_closed_cex_tvl = self.summary()
            fail_closed_cex_tvl["tokens"][0]["primary_cex"].update(
                {
                    "tvl_status": "needs_review",
                    "tvl_na_reason": "daily_quality_outcome_invalid",
                    "tvl_retryable": False,
                }
            )
            validate_summary(
                fail_closed_cex_tvl,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "spread contract"):
            directional_only = self.summary()
            directional_only["tokens"][0].pop("absolute_price_gap")
            validate_summary(
                directional_only,
                self.metrics(),
                raw_max=2000,
                gzip_max=1000,
            )
        with self.assertRaisesRegex(ReleaseCheckError, "spread contract"):
            wrong_method = self.summary()
            wrong_method["tokens"][0]["absolute_price_gap_method"] = (
                "absolute_directional_gap"
            )
            validate_summary(
                wrong_method,
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

        for field in (
            "absence_market_count",
            "withheld_payload_market_count",
            "official_inventory_count",
            "response_sha256",
            "configured_market_ids_sha256",
        ):
            with self.subTest(lifecycle_root_field=field):
                invalid = self.summary()
                invalid["metadata"]["cex_instrument_lifecycle"].pop(field)
                with self.assertRaisesRegex(ReleaseCheckError, "lifecycle"):
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
                "configured_cex_market_identities": {
                    "schema": "configured_cex_market_identities/v1",
                    "upbit": {
                        "market_count": 1,
                        "market_ids": ["cex:upbit:AAVE/KRW"],
                        "market_ids_sha256": (
                            "f6a0641ba18fc9fe86dc38d1535009418"
                            "92ab681d9b78ce29cdf9cb1b316a8e5"
                        ),
                    },
                },
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

        configured_krw = copy.deepcopy(catalog)
        configured_krw["markets"] = [{
            "token_symbol": "AAVE",
            "market_id": "cex:upbit:AAVE/KRW",
        }]
        try:
            validate_token_catalog(
                configured_krw,
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )
        except ReleaseCheckError as error:
            self.fail(
                "Token catalog rejected configured Upbit KRW: {}".format(
                    error
                )
            )

        mismatched_upbit = copy.deepcopy(configured_krw)
        mismatched_upbit["metadata"]["configured_cex_market_identities"][
            "upbit"
        ] = {
            "market_count": 1,
            "market_ids": ["cex:upbit:AAVE/USDT"],
            "market_ids_sha256": (
                "4a493498d2a13699db76b760609e91071"
                "5cd3df58dc1ad988984f0e3b61a9960"
            ),
        }
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "configured Upbit",
        ):
            validate_token_catalog(
                mismatched_upbit,
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )

        tampered_authority = copy.deepcopy(configured_krw)
        tampered_authority["metadata"][
            "configured_cex_market_identities"
        ]["upbit"]["market_ids_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "count or hash",
        ):
            validate_token_catalog(
                tampered_authority,
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )

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
        with self.assertRaisesRegex(ReleaseCheckError, "exact CEX identity"):
            validate_token_catalog(
                {
                    **catalog,
                    "markets": [{
                        "token_symbol": "AAVE",
                        "market_id": "cex:coinbase:AAVE/USDT",
                    }],
                },
                self.metrics("/api/markets/catalog"),
                token="AAVE",
                start="2026-01-01",
                end="2026-01-31",
                generation="generation-1",
                raw_max=2000,
                gzip_max=1000,
            )

    def test_cex_lifecycle_fallback_requires_exact_flag_and_rejects_dex(self):
        for status, reason_code, action, flag_code in (
            (
                "source_no_observation",
                "instrument_absent_from_current_catalog",
                "operator_review_source_outcome",
                "inactive_cex_instrument",
            ),
            (
                "needs_review",
                "official_catalog_evidence_stale",
                "operator_manual_review",
                "stale_cex_lifecycle_evidence",
            ),
        ):
            fact = {
                "status": status,
                "reason_code": reason_code,
                "retryable": False,
                "action": action,
                "quality_flags": [{"code": flag_code}],
            }
            with self.subTest(cex_lifecycle_fallback=reason_code):
                evidence = _validate_daily_fact_evidence(
                    fact,
                    market_type="cex",
                    report_status="unavailable",
                )
                self.assertEqual(evidence["mode"], None)
                self.assertEqual(evidence["issue_count"], 0)

                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "lacks required published evidence/action",
                ):
                    _validate_daily_fact_evidence(
                        {**fact, "quality_flags": []},
                        market_type="cex",
                        report_status="unavailable",
                    )

                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "lacks required published evidence/action",
                ):
                    _validate_daily_fact_evidence(
                        fact,
                        market_type="dex",
                        report_status="unavailable",
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
        comparison_with_generation = copy.deepcopy(comparison)
        comparison_with_generation["metadata"]["comparison_generation"] = (
            "comparison-generation-1"
        )
        validate_comparison(
            comparison_with_generation,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            start="2026-01-01",
            end="2026-01-31",
            expected_generation="generation-1",
            expected_comparison_generation="comparison-generation-1",
        )
        for comparison_generation in (None, "comparison-generation-2"):
            with self.subTest(
                comparison_generation=comparison_generation,
            ):
                invalid = copy.deepcopy(comparison)
                if comparison_generation is not None:
                    invalid["metadata"]["comparison_generation"] = (
                        comparison_generation
                    )
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "Comparison generation",
                ):
                    validate_comparison(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        start="2026-01-01",
                        end="2026-01-31",
                        expected_generation="generation-1",
                        expected_comparison_generation=(
                            "comparison-generation-1"
                        ),
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
                "screening_quality_scope": "catalog",
                "screening_quality_window": {
                    "start": "2026-01-01",
                    "end": "2026-01-31",
                    "method": "max_query_source_market_observed_start",
                },
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
                    "issue_outcome_counts": [],
                    "reason_code_counts": {},
                    "status_counts": {},
                    "affected_date_count": 0,
                    "affected_dates": [],
                    "market_issue_rollups": [
                        {
                            "market_id": market_id,
                            "issue_count": 0,
                            "issue_outcome_counts": [],
                            "reason_code_counts": {},
                            "status_counts": {},
                            "affected_date_count": 0,
                            "affected_dates": [],
                            "evidence_mode": "published_daily_audit",
                            "fact_outcome": {
                                "status": "observed",
                                "reason_code": "observed",
                                "retryable": False,
                                "action": None,
                            },
                        }
                        for market_id in (market_a, market_b)
                    ],
                },
            },
            "markets": quality_markets,
        }
        for market in quality["markets"]:
            market["facts"]["daily"].update(
                {
                    "daily_evidence_mode": "published_daily_audit",
                    "issue_status_counts": {},
                    "issue_outcome_counts": [],
                    "reason_code_counts": {},
                    "affected_date_count": 0,
                    "affected_dates": [],
                }
            )
        validate_quality(
            quality,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        fallback = copy.deepcopy(quality)
        fallback["metadata"]["daily_quality_report"] = {
            "status": "unavailable",
            "evidence_mode": "catalog_window_inference",
            "identity_status": "not_verified",
            "selected_window_issue_count": 0,
            "reason_code_counts": {},
            "status_counts": {},
            "affected_date_count": 0,
            "affected_dates": [],
        }
        for market in fallback["markets"]:
            for field in DAILY_FACT_EVIDENCE_FIELDS:
                market["facts"]["daily"].pop(field, None)
        validate_quality(
            fallback,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )

        fallback_with_published_evidence = copy.deepcopy(fallback)
        fallback_with_published_evidence["markets"][0]["facts"][
            "daily"
        ]["affected_dates"] = []
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "daily fact evidence/action mode is invalid",
        ):
            validate_quality(
                fallback_with_published_evidence,
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
                "issue_outcome_counts": [
                    {
                        "status": "unsupported",
                        "reason_code": "source_range_unavailable",
                        "count": 1,
                    }
                ],
                "reason_code_counts": {"source_range_unavailable": 1},
                "status_counts": {"unsupported": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
            }
        )
        unsupported_without_fact_evidence["metadata"][
            "daily_quality_report"
        ]["market_issue_rollups"][0].update(
            {
                "issue_count": 1,
                "issue_outcome_counts": [
                    {
                        "status": "unsupported",
                        "reason_code": "source_range_unavailable",
                        "count": 1,
                    }
                ],
                "reason_code_counts": {
                    "source_range_unavailable": 1,
                },
                "status_counts": {"unsupported": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
                "evidence_mode": "published_daily_audit",
                "fact_outcome": {
                    "status": "unsupported",
                    "reason_code": "source_range_unavailable",
                    "retryable": False,
                    "action": "operator_review_source_outcome",
                },
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

        for (
            lifecycle_status,
            lifecycle_reason,
            lifecycle_action,
            lifecycle_flag_code,
        ) in (
            (
                "source_no_observation",
                "instrument_absent_from_current_catalog",
                "operator_review_source_outcome",
                "inactive_cex_instrument",
            ),
            (
                "needs_review",
                "official_catalog_evidence_stale",
                "operator_manual_review",
                "stale_cex_lifecycle_evidence",
            ),
        ):
            with self.subTest(
                lifecycle_without_daily_issue=lifecycle_reason,
            ):
                lifecycle_only = copy.deepcopy(quality)
                lifecycle_flag = {
                    "code": lifecycle_flag_code,
                    "severity": "critical",
                    "category": "data_health",
                    "message": "Official CEX catalog evidence withholds current facts.",
                    "observed_value": "2026-01-16T00:00:00+00:00",
                    "threshold": "present_and_current_official_catalog_evidence",
                }
                lifecycle_only["markets"][0]["facts"]["daily"].update(
                    {
                        "status": lifecycle_status,
                        "reason_code": lifecycle_reason,
                        "retryable": False,
                        "action": lifecycle_action,
                        "quality_flags": [lifecycle_flag],
                    }
                )
                lifecycle_only["markets"][0].update(
                    {
                        "quality_status": "critical",
                        "quality_flags": [lifecycle_flag],
                    }
                )
                lifecycle_only["metadata"]["daily_quality_report"][
                    "market_issue_rollups"
                ][0]["fact_outcome"] = {
                    "status": lifecycle_status,
                    "reason_code": lifecycle_reason,
                    "retryable": False,
                    "action": lifecycle_action,
                }

                validate_quality(
                    lifecycle_only,
                    token="AAVE",
                    market_a=market_a,
                    market_b=market_b,
                    expected_generation="generation-1",
                )

                missing_lifecycle_flag = copy.deepcopy(lifecycle_only)
                missing_lifecycle_flag["markets"][0]["facts"]["daily"][
                    "quality_flags"
                ] = []
                missing_lifecycle_flag["markets"][0].update(
                    {"quality_status": "ok", "quality_flags": []}
                )
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "zero evidence/action",
                ):
                    validate_quality(
                        missing_lifecycle_flag,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                    )

        dex_lifecycle = copy.deepcopy(quality)
        dex_lifecycle_flag = {
            "code": "inactive_cex_instrument",
            "severity": "critical",
            "category": "data_health",
            "message": "Official CEX catalog evidence withholds current facts.",
            "observed_value": "2026-01-16T00:00:00+00:00",
            "threshold": "present_and_current_official_catalog_evidence",
        }
        dex_lifecycle["markets"][1]["facts"]["daily"].update(
            {
                "status": "source_no_observation",
                "reason_code": "instrument_absent_from_current_catalog",
                "retryable": False,
                "action": "operator_review_source_outcome",
                "quality_flags": [dex_lifecycle_flag],
            }
        )
        dex_lifecycle["markets"][1].update(
            {
                "quality_status": "critical",
                "quality_flags": [dex_lifecycle_flag],
            }
        )
        dex_lifecycle["metadata"]["daily_quality_report"][
            "market_issue_rollups"
        ][1]["fact_outcome"] = {
            "status": "source_no_observation",
            "reason_code": "instrument_absent_from_current_catalog",
            "retryable": False,
            "action": "operator_review_source_outcome",
        }
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "zero evidence/action",
        ):
            validate_quality(
                dex_lifecycle,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        instrument_absent = copy.deepcopy(quality)
        instrument_absent["metadata"]["daily_quality_report"].update(
            {
                "selected_window_issue_count": 1,
                "issue_outcome_counts": [
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "count": 1,
                    }
                ],
                "reason_code_counts": {
                    "instrument_absent_from_current_catalog": 1,
                },
                "status_counts": {"source_no_observation": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
            }
        )
        instrument_absent["metadata"]["daily_quality_report"][
            "market_issue_rollups"
        ][0].update(
            {
                "issue_count": 1,
                "issue_outcome_counts": [
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "count": 1,
                    }
                ],
                "reason_code_counts": {
                    "instrument_absent_from_current_catalog": 1,
                },
                "status_counts": {"source_no_observation": 1},
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
                "evidence_mode": "published_daily_audit",
                "fact_outcome": {
                    "status": "source_no_observation",
                    "reason_code": (
                        "instrument_absent_from_current_catalog"
                    ),
                    "retryable": False,
                    "action": "operator_review_source_outcome",
                },
            }
        )
        instrument_absent["markets"][0]["facts"]["daily"].update(
            {
                "status": "source_no_observation",
                "reason_code": "instrument_absent_from_current_catalog",
                "retryable": False,
                "action": "operator_review_source_outcome",
                "daily_evidence_mode": "published_daily_audit",
                "issue_status_counts": {"source_no_observation": 1},
                "issue_outcome_counts": [
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "count": 1,
                    }
                ],
                "reason_code_counts": {
                    "instrument_absent_from_current_catalog": 1,
                },
                "affected_date_count": 1,
                "affected_dates": ["2026-01-15"],
            }
        )
        validate_quality(
            instrument_absent,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )
        for lifecycle_fact_name in ("depth", "execution"):
            with self.subTest(cex_lifecycle_fact=lifecycle_fact_name):
                cex_lifecycle_absence = copy.deepcopy(quality)
                cex_lifecycle_absence["markets"][0]["facts"][
                    lifecycle_fact_name
                ].update(
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "retryable": False,
                        "action": None,
                    }
                )
                validate_quality(
                    cex_lifecycle_absence,
                    token="AAVE",
                    market_a=market_a,
                    market_b=market_b,
                    expected_generation="generation-1",
                )

            with self.subTest(dex_lifecycle_fact=lifecycle_fact_name):
                dex_lifecycle_absence = copy.deepcopy(quality)
                dex_lifecycle_absence["markets"][1]["facts"][
                    lifecycle_fact_name
                ].update(
                    {
                        "status": "source_no_observation",
                        "reason_code": (
                            "instrument_absent_from_current_catalog"
                        ),
                        "retryable": False,
                        "action": None,
                    }
                )
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "canonical outcome",
                ):
                    validate_quality(
                        dex_lifecycle_absence,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                    )

        mixed_report = copy.deepcopy(quality)
        mixed_report["metadata"]["daily_quality_report"].update(
            {
                "selected_window_issue_count": 2,
                "issue_outcome_counts": [
                    {
                        "status": "collection_failed",
                        "reason_code": "network",
                        "count": 1,
                    },
                    {
                        "status": "needs_review",
                        "reason_code": "not_listed",
                        "count": 1,
                    },
                ],
                "reason_code_counts": {"network": 1, "not_listed": 1},
                "status_counts": {
                    "collection_failed": 1,
                    "needs_review": 1,
                },
                "affected_date_count": 2,
                "affected_dates": ["2026-01-15", "2026-01-16"],
            }
        )
        mixed_report["metadata"]["daily_quality_report"][
            "market_issue_rollups"
        ][0].update(
            {
                "issue_count": 2,
                "issue_outcome_counts": [
                    {
                        "status": "collection_failed",
                        "reason_code": "network",
                        "count": 1,
                    },
                    {
                        "status": "needs_review",
                        "reason_code": "not_listed",
                        "count": 1,
                    },
                ],
                "reason_code_counts": {
                    "network": 1,
                    "not_listed": 1,
                },
                "status_counts": {
                    "collection_failed": 1,
                    "needs_review": 1,
                },
                "affected_date_count": 2,
                "affected_dates": ["2026-01-15", "2026-01-16"],
                "evidence_mode": "published_daily_audit",
                "fact_outcome": {
                    "status": "collection_failed",
                    "reason_code": "multiple_daily_quality_reasons",
                    "retryable": True,
                    "action": "operator_review_retry_and_manual_queues",
                },
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
                "issue_outcome_counts": [
                    {
                        "status": "collection_failed",
                        "reason_code": "network",
                        "count": 1,
                    },
                    {
                        "status": "needs_review",
                        "reason_code": "not_listed",
                        "count": 1,
                    },
                ],
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

        impossible_marginals = copy.deepcopy(mixed_report)
        impossible_report = impossible_marginals["metadata"][
            "daily_quality_report"
        ]
        impossible_report["reason_code_counts"] = {"network": 2}
        impossible_report["issue_outcome_counts"] = [
            {
                "status": "collection_failed",
                "reason_code": "network",
                "count": 1,
            },
            {
                "status": "needs_review",
                "reason_code": "not_listed",
                "count": 1,
            },
        ]
        impossible_rollup = impossible_report["market_issue_rollups"][0]
        impossible_rollup["reason_code_counts"] = {"network": 2}
        impossible_rollup["issue_outcome_counts"] = copy.deepcopy(
            impossible_report["issue_outcome_counts"]
        )
        impossible_fact = impossible_marginals["markets"][0]["facts"][
            "daily"
        ]
        impossible_fact["reason_code"] = "network"
        impossible_fact["reason_code_counts"] = {"network": 2}
        impossible_fact["issue_outcome_counts"] = copy.deepcopy(
            impossible_report["issue_outcome_counts"]
        )
        impossible_rollup["fact_outcome"]["reason_code"] = "network"
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "outcome counts",
        ):
            validate_quality(
                impossible_marginals,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
            )

        distinct_zero_outcomes = copy.deepcopy(quality)
        first_daily = distinct_zero_outcomes["markets"][0]["facts"][
            "daily"
        ]
        first_daily.update(
            {
                "status": "not_applicable",
                "reason_code": (
                    "selected_window_before_first_market_observation"
                ),
                "retryable": False,
                "action": None,
            }
        )
        distinct_zero_outcomes["metadata"]["daily_quality_report"][
            "market_issue_rollups"
        ][0]["fact_outcome"] = {
            "status": "not_applicable",
            "reason_code": "selected_window_before_first_market_observation",
            "retryable": False,
            "action": None,
        }
        validate_quality(
            distinct_zero_outcomes,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )
        swapped_zero_facts = copy.deepcopy(distinct_zero_outcomes)
        facts_a = swapped_zero_facts["markets"][0]["facts"]
        facts_b = swapped_zero_facts["markets"][1]["facts"]
        facts_a["daily"], facts_b["daily"] = facts_b["daily"], facts_a[
            "daily"
        ]
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "market rollup",
        ):
            validate_quality(
                swapped_zero_facts,
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

        fail_closed_cex_tvl = copy.deepcopy(quality)
        fail_closed_cex_tvl["markets"][0]["facts"]["tvl"].update(
            {
                "status": "needs_review",
                "reason_code": "daily_quality_outcome_invalid",
                "retryable": False,
                "action": "operator_manual_review",
            }
        )
        with self.assertRaisesRegex(ReleaseCheckError, "canonical outcome"):
            validate_quality(
                fail_closed_cex_tvl,
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

        producer_retry_actions = (
            (
                0,
                "depth",
                "collection_failed",
                "network",
                "retry_depth_collection",
            ),
            (
                0,
                "execution",
                "failed",
                "execution_snapshot_invalid",
                "retry_execution_collection",
            ),
            (
                1,
                "tvl",
                "collection_failed",
                "source_unavailable",
                "retry_tvl_collection",
            ),
        )
        for (
            market_index,
            fact_name,
            status,
            reason_code,
            action,
        ) in producer_retry_actions:
            with self.subTest(producer_retry_action=action):
                retryable_fact = copy.deepcopy(quality)
                retryable_fact["markets"][market_index]["facts"][
                    fact_name
                ].update(
                    {
                        "status": status,
                        "reason_code": reason_code,
                        "retryable": True,
                        "action": action,
                    }
                )
                validate_quality(
                    retryable_fact,
                    token="AAVE",
                    market_a=market_a,
                    market_b=market_b,
                    expected_generation="generation-1",
                )
        missing_action = copy.deepcopy(quality)
        missing_action["markets"][0]["facts"]["depth"].pop("action")
        with self.assertRaisesRegex(
            ReleaseCheckError,
            "selected quality contract",
        ):
            validate_quality(
                missing_action,
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
        explicit_null_measurements = copy.deepcopy(quality)
        explicit_null_measurements["markets"][0]["quality_status"] = (
            "critical"
        )
        explicit_null_measurements["markets"][0]["quality_flags"] = [
            copy.deepcopy(critical_flag)
        ]
        explicit_null_measurements["markets"][0]["facts"]["depth"][
            "quality_flags"
        ] = [copy.deepcopy(critical_flag)]
        validate_quality(
            explicit_null_measurements,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
        )
        for measurement_field in ("observed_value", "threshold"):
            with self.subTest(missing_measurement_field=measurement_field):
                missing_measurement = copy.deepcopy(
                    explicit_null_measurements
                )
                missing_measurement["markets"][0]["quality_flags"][0].pop(
                    measurement_field
                )
                missing_measurement["markets"][0]["facts"]["depth"][
                    "quality_flags"
                ][0].pop(measurement_field)
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "missing or unknown fields",
                ):
                    validate_quality(
                        missing_measurement,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                    )
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
            cohort_id = f"{market_id.split(':', 1)[0]}-cohort-1"
            return [
                {
                    "market_id": market_id,
                    "token_symbol": "AAVE",
                    "observed_at": "2026-01-02T00:00:07+00:00",
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "status": status,
                    "snapshot_id": cohort_id,
                    "source_snapshot_id": cohort_id,
                }
                for direction in ("sell_token", "buy_token")
                for notional in (1_000, 5_000, 10_000, 50_000, 100_000)
            ]

        execution = {
            "metadata": {
                "data_generation": "generation-1",
                "cohort_observation_model": "bounded_sequential_observations",
                "snapshots": {
                    "cex": {
                        "snapshot_ids": ["cex-cohort-1"],
                        "source_snapshot_ids": ["cex-cohort-1"],
                        "observed_at": "2026-01-02T00:00:05+00:00",
                        "observed_at_min": "2026-01-02T00:00:05+00:00",
                        "observed_at_max": "2026-01-02T00:00:09+00:00",
                        "observation_span_seconds": 4,
                        "market_count": 1,
                    },
                    "dex": {
                        "snapshot_ids": ["dex-cohort-1"],
                        "source_snapshot_ids": ["dex-cohort-1"],
                        "observed_at": "2026-01-02T00:00:05+00:00",
                        "observed_at_min": "2026-01-02T00:00:05+00:00",
                        "observed_at_max": "2026-01-02T00:00:09+00:00",
                        "observation_span_seconds": 4,
                        "market_count": 1,
                    },
                },
                "cohort_lineage": {
                    "cex": {
                        "market_type": "cex",
                        "depth_snapshot_id": "cex-cohort-1",
                        "execution_snapshot_id": "cex-cohort-1",
                        "execution_source_snapshot_id": "cex-cohort-1",
                        "depth_market_count": 1,
                        "execution_market_count": 1,
                    },
                    "dex": {
                        "market_type": "dex",
                        "depth_snapshot_id": "dex-cohort-1",
                        "execution_snapshot_id": "dex-cohort-1",
                        "execution_source_snapshot_id": "dex-cohort-1",
                        "depth_market_count": 1,
                        "execution_market_count": 1,
                    },
                },
            },
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
        catalog_metadata = {
            "cex_depth_snapshot": {
                "snapshot_ids": ["cex-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:04+00:00",
                "observation_span_seconds": 4,
                "market_rows": 1,
            },
            "dex_depth_snapshot": {
                "snapshot_ids": ["dex-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:04+00:00",
                "observation_span_seconds": 4,
                "pool_rows": 1,
            },
        }
        self.assertIn(
            "catalog_metadata",
            inspect.signature(validate_execution).parameters,
        )
        validate_execution(
            execution,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
            catalog_metadata=catalog_metadata,
        )
        execution_with_generation = copy.deepcopy(execution)
        execution_with_generation["metadata"]["execution_generation"] = (
            "execution-generation-1"
        )
        validate_execution(
            execution_with_generation,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
            expected_execution_generation="execution-generation-1",
            catalog_metadata=catalog_metadata,
        )
        for execution_generation in (None, "execution-generation-2"):
            with self.subTest(execution_generation=execution_generation):
                invalid = copy.deepcopy(execution)
                if execution_generation is not None:
                    invalid["metadata"]["execution_generation"] = (
                        execution_generation
                    )
                with self.assertRaisesRegex(
                    ReleaseCheckError,
                    "Execution generation",
                ):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        expected_execution_generation=(
                            "execution-generation-1"
                        ),
                        catalog_metadata=catalog_metadata,
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
                catalog_metadata=catalog_metadata,
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
                catalog_metadata=catalog_metadata,
            )

    def test_release_execution_cohort_lineage_counterexamples_fail_closed(self):
        market_a = "cex:binance:AAVE/USDT"
        market_b = "dex:eth:uniswap_v3:pool:AAVE"

        def rows(market_id, cohort_id, status):
            return [
                {
                    "market_id": market_id,
                    "token_symbol": "AAVE",
                    "observed_at": "2026-01-02T00:00:07+00:00",
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "status": status,
                    "snapshot_id": cohort_id,
                    "source_snapshot_id": cohort_id,
                }
                for direction in ("sell_token", "buy_token")
                for notional in (1_000, 5_000, 10_000, 50_000, 100_000)
            ]

        def lineage(market_type, cohort_id):
            return {
                "market_type": market_type,
                "depth_snapshot_id": cohort_id,
                "execution_snapshot_id": cohort_id,
                "execution_source_snapshot_id": cohort_id,
                "depth_market_count": 1,
                "execution_market_count": 1,
            }

        payload = {
            "metadata": {
                "data_generation": "generation-1",
                "cohort_observation_model": "bounded_sequential_observations",
                "snapshots": {
                    "cex": {
                        "snapshot_ids": ["cex-cohort-1"],
                        "source_snapshot_ids": ["cex-cohort-1"],
                        "observed_at": "2026-01-02T00:00:05+00:00",
                        "observed_at_min": "2026-01-02T00:00:05+00:00",
                        "observed_at_max": "2026-01-02T00:00:09+00:00",
                        "observation_span_seconds": 4,
                        "market_count": 1,
                    },
                    "dex": {
                        "snapshot_ids": ["dex-cohort-1"],
                        "source_snapshot_ids": ["dex-cohort-1"],
                        "observed_at": "2026-01-02T00:00:05+00:00",
                        "observed_at_min": "2026-01-02T00:00:05+00:00",
                        "observed_at_max": "2026-01-02T00:00:09+00:00",
                        "observation_span_seconds": 4,
                        "market_count": 1,
                    },
                },
                "cohort_lineage": {
                    "cex": lineage("cex", "cex-cohort-1"),
                    "dex": lineage("dex", "dex-cohort-1"),
                },
            },
            "token_symbol": "AAVE",
            "market_a": {
                "market": {
                    "market_id": market_a,
                    "market_type": "cex",
                },
                "status": "available",
                "rows": rows(market_a, "cex-cohort-1", "observed"),
            },
            "market_b": {
                "market": {
                    "market_id": market_b,
                    "market_type": "dex",
                },
                "status": "available",
                "rows": rows(market_b, "dex-cohort-1", "unsupported"),
            },
        }
        catalog_metadata = {
            "cex_depth_snapshot": {
                "snapshot_ids": ["cex-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:04+00:00",
                "observation_span_seconds": 4,
                "market_rows": 1,
            },
            "dex_depth_snapshot": {
                "snapshot_ids": ["dex-cohort-1"],
                "observed_at": "2026-01-02T00:00:00+00:00",
                "observed_at_min": "2026-01-02T00:00:00+00:00",
                "observed_at_max": "2026-01-02T00:00:04+00:00",
                "observation_span_seconds": 4,
                "pool_rows": 1,
            },
        }
        self.assertIn(
            "catalog_metadata",
            inspect.signature(validate_execution).parameters,
        )
        validate_execution(
            payload,
            token="AAVE",
            market_a=market_a,
            market_b=market_b,
            expected_generation="generation-1",
            catalog_metadata=catalog_metadata,
        )

        simultaneous_claim = copy.deepcopy(payload)
        simultaneous_claim["metadata"]["cohort_observation_model"] = (
            "simultaneous_observations"
        )
        with self.assertRaises(ReleaseCheckError):
            validate_execution(
                simultaneous_claim,
                token="AAVE",
                market_a=market_a,
                market_b=market_b,
                expected_generation="generation-1",
                catalog_metadata=catalog_metadata,
            )

        for metadata_field in ("cohort_lineage", "snapshots"):
            with self.subTest(extra_metadata=metadata_field):
                invalid = copy.deepcopy(payload)
                invalid["metadata"][metadata_field]["unexpected"] = {
                    "simultaneous": True,
                }
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=catalog_metadata,
                    )

        for count_field in (
            "depth_market_count",
            "execution_market_count",
        ):
            for invalid_count in (True, 1.0):
                with self.subTest(
                    count_field=count_field,
                    invalid_count=invalid_count,
                ):
                    invalid = copy.deepcopy(payload)
                    invalid["metadata"]["cohort_lineage"]["cex"][
                        count_field
                    ] = invalid_count
                    with self.assertRaises(ReleaseCheckError):
                        validate_execution(
                            invalid,
                            token="AAVE",
                            market_a=market_a,
                            market_b=market_b,
                            expected_generation="generation-1",
                            catalog_metadata=catalog_metadata,
                        )

        bounds_counterexamples = (
            ("depth", "observed_at_min", "not-a-time"),
            (
                "depth",
                "observed_at_min",
                "0001-01-01T00:00:00+23:59",
            ),
            ("depth", "observed_at_max", "2026-01-02T00:00:04"),
            ("depth", "observed_at", "2026-01-02T00:00:01+00:00"),
            ("depth", "observation_span_seconds", -1),
            ("depth", "observation_span_seconds", 5),
            ("execution", "observed_at_min", None),
            (
                "execution",
                "observed_at_max",
                "9999-12-31T23:59:59-23:59",
            ),
            ("execution", "observation_span_seconds", True),
            ("execution", "observation_span_seconds", 5),
        )
        for location, field, invalid_value in bounds_counterexamples:
            with self.subTest(
                location=location,
                field=field,
                invalid_value=invalid_value,
            ):
                invalid_payload = copy.deepcopy(payload)
                invalid_catalog = copy.deepcopy(catalog_metadata)
                target = (
                    invalid_catalog["cex_depth_snapshot"]
                    if location == "depth"
                    else invalid_payload["metadata"]["snapshots"]["cex"]
                )
                target[field] = invalid_value
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid_payload,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=invalid_catalog,
                    )

        counterexamples = {
            "market_type": "dex",
            "depth_snapshot_id": "wrong-depth",
            "execution_snapshot_id": "wrong-execution",
            "execution_source_snapshot_id": "wrong-source",
            "depth_market_count": 2,
            "execution_market_count": 2,
        }
        for field, wrong_value in counterexamples.items():
            with self.subTest(field=field):
                invalid = copy.deepcopy(payload)
                invalid["metadata"]["cohort_lineage"]["cex"][field] = (
                    wrong_value
                )
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=catalog_metadata,
                    )

        for field in ("snapshot_id", "source_snapshot_id"):
            with self.subTest(row_field=field):
                invalid = copy.deepcopy(payload)
                invalid["market_a"]["rows"][0][field] = None
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=catalog_metadata,
                    )

        for location in ("depth", "execution"):
            with self.subTest(all_null_bounds=location):
                invalid_payload = copy.deepcopy(payload)
                invalid_catalog = copy.deepcopy(catalog_metadata)
                target = (
                    invalid_catalog["cex_depth_snapshot"]
                    if location == "depth"
                    else invalid_payload["metadata"]["snapshots"]["cex"]
                )
                for field in (
                    "observed_at",
                    "observed_at_min",
                    "observed_at_max",
                    "observation_span_seconds",
                ):
                    target[field] = None
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid_payload,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=invalid_catalog,
                    )

        for invalid_time in (
            None,
            "",
            "not-a-time",
            "2026-01-02T00:00:10+00:00",
        ):
            with self.subTest(row_observed_at=invalid_time):
                invalid = copy.deepcopy(payload)
                invalid["market_a"]["rows"][0]["observed_at"] = invalid_time
                with self.assertRaises(ReleaseCheckError):
                    validate_execution(
                        invalid,
                        token="AAVE",
                        market_a=market_a,
                        market_b=market_b,
                        expected_generation="generation-1",
                        catalog_metadata=catalog_metadata,
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
