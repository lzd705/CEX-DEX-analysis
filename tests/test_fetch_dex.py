import csv
from io import BytesIO
import os
import unittest
import urllib.parse
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import urllib.error
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts import fetch_dex
from scripts.fetch_dex import choose_main_pool
from scripts.fetch_dex import choose_top_pools
from scripts.fetch_dex import convert_ohlcv_row
from scripts.fetch_dex import group_chain_rows_by_token
from scripts.fetch_dex import get_retry_wait_seconds
from scripts.fetch_dex import get_status_code
from scripts.fetch_dex import get_token_side
from scripts.fetch_dex import safe_float
from scripts.fetch_dex import build_pool_result
from scripts.fetch_dex import aggregate_dex_pool_rows
from scripts.fetch_dex import filter_complete_dates
from scripts.fetch_dex import sort_pools_by_volume
from scripts.fetch_dex import write_pool_rows
from scripts.fetch_dex import read_token_config
from scripts.fetch_dex import read_token_chain_config
from scripts.fetch_dex import TOKEN_CONFIG_PATH
from scripts.fetch_dex import TOKEN_CHAIN_CONFIG_PATH
from scripts.fetch_dex import REQUEST_SLEEP_SECONDS
from scripts.fetch_dex import filter_token_rows
from scripts.fetch_dex import replace_token_rows
from scripts.fetch_dex import deduplicate_pool_volume_rows
from scripts.fetch_dex import TOP_POOL_COUNT
from scripts.fetch_dex import TLS_CONTEXT
from scripts.fetch_dex import merge_pool_volume_rows
from scripts.fetch_dex import load_existing_pool_inventory
from scripts.fetch_dex import remove_pool_rows
from scripts.fetch_dex import fetch_existing_pools
from scripts.fetch_dex import merge_runtime_token_config
from scripts.fetch_dex import runtime_registry_path
from scripts.fetch_dex import classify_attempt_error
from scripts.fetch_dex import SourceRangeUnavailable


class FetchDexTests(unittest.TestCase):
    def test_thirty_day_old_single_day_uses_gecko_before_timestamp(self):
        target_date = (
            datetime.now(timezone.utc).date() - timedelta(days=30)
        ).isoformat()
        target_time = datetime.strptime(target_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        target_timestamp = int(target_time.timestamp())
        pool = {
            "chain": "eth",
            "pool_address": "0xpool",
            "ohlcv_token": "quote",
        }
        payload = {
            "data": {
                "attributes": {
                    "ohlcv_list": [
                        [target_timestamp, 1, 2, 0.5, 1.5, 100]
                    ]
                }
            }
        }
        with patch(
            "scripts.fetch_dex.request_json",
            return_value=payload,
        ) as request:
            rows = fetch_dex.fetch_pool_ohlcv(
                pool,
                target_date,
                target_date,
            )

        requested_url = request.call_args.args[0]
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(requested_url).query)
        self.assertEqual(
            query["before_timestamp"],
            [str(target_timestamp + 86_400)],
        )
        self.assertEqual(rows[0][0], target_timestamp)

    def test_recent_gecko_request_does_not_add_historical_cursor(self):
        with patch(
            "scripts.fetch_dex.request_json",
            return_value={"data": {"attributes": {"ohlcv_list": []}}},
        ) as request:
            fetch_dex.fetch_pool_ohlcv(
                {
                    "chain": "eth",
                    "pool_address": "0xpool",
                    "ohlcv_token": "base",
                }
            )

        query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(request.call_args.args[0]).query
        )
        self.assertNotIn("before_timestamp", query)

    def test_runtime_registry_follows_market_data_dir_without_explicit_override(self):
        with TemporaryDirectory() as directory:
            with patch.dict(
                os.environ,
                {"MARKET_DATA_DIR": directory},
                clear=True,
            ):
                self.assertEqual(
                    runtime_registry_path(),
                    Path(directory).resolve() / "admin/token_registry.json",
                )

    def test_https_requests_use_a_verified_tls_context(self):
        self.assertEqual(TLS_CONTEXT.verify_mode, 2)
        self.assertTrue(TLS_CONTEXT.check_hostname)

    def test_merge_pool_volume_rows_preserves_other_tokens_and_dates(self):
        existing = [
            {"date": "2026-01-01", "token_symbol": "UNI", "chain": "eth", "pool_address": "0xuni", "close": "1"},
            {"date": "2026-01-01", "token_symbol": "AAVE", "chain": "eth", "pool_address": "0xaave", "close": "2"},
        ]
        updated = [
            {"date": "2026-01-01", "token_symbol": "UNI", "chain": "eth", "pool_address": "0xuni", "close": "1.5"},
            {"date": "2026-01-02", "token_symbol": "UNI", "chain": "eth", "pool_address": "0xuni", "close": "1.6"},
        ]

        result = merge_pool_volume_rows(existing, updated)

        by_key = {(row["token_symbol"], row["date"]): row for row in result}
        self.assertEqual(by_key[("UNI", "2026-01-01")]["close"], "1.5")
        self.assertEqual(by_key[("UNI", "2026-01-02")]["close"], "1.6")
        self.assertEqual(by_key[("AAVE", "2026-01-01")]["close"], "2")

    def test_existing_pool_inventory_uses_exact_quote_side_and_rejects_invalid_pool(self):
        target_address = "0x" + "11" * 20
        weth_address = "0x" + "22" * 20
        usd_address = "0x" + "33" * 20
        with TemporaryDirectory() as temp_dir:
            pool_path = Path(temp_dir) / "pools.csv"
            tvl_path = Path(temp_dir) / "tvl.csv"
            with pool_path.open("w", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "date",
                        "token_symbol",
                        "chain",
                        "dex",
                        "pool_address",
                        "pool_name",
                    ],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "date": "2026-07-25",
                            "token_symbol": "CRV",
                            "chain": "eth",
                            "dex": "uniswap_v3",
                            "pool_address": "0xvalid",
                            "pool_name": "WETH / CRV",
                        },
                        {
                            "date": "2026-07-25",
                            "token_symbol": "CRV",
                            "chain": "eth",
                            "dex": "curve",
                            "pool_address": "0xinvalid",
                            "pool_name": "USD / WETH / CRV",
                        },
                    ]
                )
            with tvl_path.open("w", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "token_symbol",
                        "chain",
                        "pool_address",
                        "source_dex",
                        "source_pool_name",
                        "base_token_id",
                        "quote_token_id",
                        "tvl_usd",
                        "volume_24h_usd",
                    ],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "token_symbol": "CRV",
                            "chain": "eth",
                            "pool_address": "0xvalid",
                            "source_dex": "uniswap_v3",
                            "source_pool_name": "WETH / CRV",
                            "base_token_id": "eth_" + weth_address,
                            "quote_token_id": "eth_" + target_address,
                            "tvl_usd": "100",
                            "volume_24h_usd": "50",
                        },
                        {
                            "token_symbol": "CRV",
                            "chain": "eth",
                            "pool_address": "0xinvalid",
                            "source_dex": "curve",
                            "source_pool_name": "USD / WETH / CRV",
                            "base_token_id": "eth_" + usd_address,
                            "quote_token_id": "eth_" + weth_address,
                            "tvl_usd": "200",
                            "volume_24h_usd": "60",
                        },
                    ]
                )

            pools, unresolved, resolved, invalid = load_existing_pool_inventory(
                pool_path,
                tvl_path,
                {"CRV": [{"chain": "eth", "contract_address": target_address}]},
                ["CRV"],
            )

        self.assertEqual(unresolved, [])
        self.assertEqual(resolved, ["CRV"])
        self.assertEqual(len(pools), 1)
        self.assertEqual(pools[0]["pool_address"], "0xvalid")
        self.assertEqual(pools[0]["ohlcv_token"], "quote")
        self.assertEqual(invalid, [("CRV", "eth", "0xinvalid")])

    def test_remove_pool_rows_only_drops_exact_invalid_identity(self):
        rows = [
            {
                "token_symbol": "CRV",
                "chain": "eth",
                "pool_address": "0xinvalid",
            },
            {
                "token_symbol": "CRV",
                "chain": "arbitrum",
                "pool_address": "0xinvalid",
            },
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "pool_address": "0xinvalid",
            },
        ]

        result = remove_pool_rows(rows, [("CRV", "eth", "0xinvalid")])

        self.assertEqual(result, rows[1:])

    def test_existing_pool_refresh_rejects_any_missing_pool_response(self):
        pools = [
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "pool_address": "0xone",
                "dex": "uniswap",
                "pool_name": "UNI / USD",
                "pool_tvl_usd": None,
            },
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "pool_address": "0xtwo",
                "dex": "uniswap",
                "pool_name": "UNI / WETH",
                "pool_tvl_usd": None,
            },
        ]
        observed = [[1704067200, 1, 2, 0.5, 1.5, 100]]
        attempts = []

        with patch(
            "scripts.fetch_dex.fetch_pool_ohlcv",
            side_effect=[observed, []],
        ), patch("scripts.fetch_dex.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "incomplete for 1 pools"):
                fetch_existing_pools(
                    pools,
                    attempt_records=attempts,
                    start_date="2024-01-01",
                    end_date="2024-01-01",
                )

        self.assertEqual(
            [(item["pool_address"], item["status"]) for item in attempts],
            [("0xone", "succeeded"), ("0xtwo", "no_data")],
        )
        self.assertEqual(attempts[1]["reason_code"], "no_candles")

    def test_managed_append_can_publish_attempt_evidence_for_missing_pool(self):
        pools = [
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "pool_address": "0xone",
                "dex": "uniswap",
                "pool_name": "UNI / USD",
                "pool_tvl_usd": None,
            }
        ]
        attempts = []
        with patch(
            "scripts.fetch_dex.fetch_pool_ohlcv",
            return_value=[],
        ), patch("scripts.fetch_dex.time.sleep"):
            rows = fetch_existing_pools(
                pools,
                attempt_records=attempts,
                start_date="2026-07-28",
                end_date="2026-07-28",
                fail_on_incomplete=False,
            )

        self.assertEqual(rows, [])
        self.assertEqual(attempts[0]["status"], "no_data")
        self.assertEqual(attempts[0]["reason_code"], "no_candles")

    def test_dex_attempt_error_is_classified_without_raw_url(self):
        error = urllib.error.HTTPError(
            "https://api.example/pool?token=secret",
            404,
            "Not Found",
            None,
            None,
        )

        classified = classify_attempt_error(error)

        self.assertEqual(classified["reason_code"], "not_listed")
        self.assertEqual(classified["http_status"], 404)
        self.assertNotIn("secret", classified["error"])

    def test_bare_unauthorized_response_remains_retryable_source_failure(self):
        error = urllib.error.HTTPError(
            "https://api.geckoterminal.com/api/v2/pools/secret/ohlcv/day",
            401,
            "Unauthorized",
            None,
            None,
        )

        classified = classify_attempt_error(error)
        attempt = fetch_dex.dex_attempt_record(
            "AAVE",
            "eth",
            "uniswap_v3",
            "0xAAVEPOOL",
            error=error,
            start_date="2026-01-01",
            end_date="2026-01-01",
        )

        self.assertEqual(
            classified["reason_code"],
            "source_unavailable",
        )
        self.assertEqual(classified["http_status"], 401)
        self.assertNotIn("secret", classified["error"])
        self.assertNotIn("http", classified["error"].lower())
        self.assertEqual(attempt["status"], "failed")
        self.assertEqual(attempt["outcome"], "request_failed")
        self.assertEqual(
            attempt["reason_code"],
            "source_unavailable",
        )
        self.assertEqual(attempt["http_status"], 401)

    def test_explicit_public_history_limit_is_bounded_and_unsupported(self):
        error = urllib.error.HTTPError(
            "https://api.geckoterminal.com/api/v2/pools/secret/ohlcv/day",
            401,
            "Unauthorized",
            None,
            BytesIO(
                b'{"errors":[{"title":"You can only access data from the '
                b'past 180 days with Public API."}]}'
            ),
        )
        with patch(
            "scripts.fetch_dex.urllib.request.urlopen",
            side_effect=error,
        ):
            with self.assertRaises(SourceRangeUnavailable) as raised:
                fetch_dex.request_json(
                    "https://api.geckoterminal.com/api/v2/networks/eth/"
                    "pools/secret/ohlcv/day"
                )

        classified = classify_attempt_error(raised.exception)
        attempt = fetch_dex.dex_attempt_record(
            "AAVE",
            "eth",
            "uniswap_v3",
            "0xAAVEPOOL",
            error=raised.exception,
            start_date="2026-01-01",
            end_date="2026-01-01",
        )
        self.assertEqual(
            classified["reason_code"],
            "source_range_unavailable",
        )
        self.assertEqual(classified["http_status"], 401)
        self.assertNotIn("secret", classified["error"])
        self.assertEqual(attempt["status"], "unsupported")
        self.assertEqual(attempt["outcome"], "range_unavailable")

    def test_same_history_text_on_non_ohlcv_request_stays_retryable(self):
        error = urllib.error.HTTPError(
            "https://api.geckoterminal.com/api/v2/networks/eth/tokens/secret",
            401,
            "Unauthorized",
            None,
            BytesIO(
                b'{"errors":[{"title":"You can only access data from the '
                b'past 180 days with Public API."}]}'
            ),
        )
        with patch(
            "scripts.fetch_dex.urllib.request.urlopen",
            side_effect=error,
        ):
            with self.assertRaises(urllib.error.HTTPError) as raised:
                fetch_dex.request_json(
                    "https://api.geckoterminal.com/api/v2/networks/eth/"
                    "tokens/secret"
                )

        classified = classify_attempt_error(raised.exception)
        self.assertEqual(classified["reason_code"], "source_unavailable")
        self.assertEqual(classified["http_status"], 401)

    def test_range_rejection_is_reused_for_remaining_inventory_pools(self):
        pools = [
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "dex": "uniswap_v3",
                "pool_address": "0xone",
            },
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "dex": "uniswap_v3",
                "pool_address": "0xtwo",
            },
        ]
        attempts = []
        range_error = SourceRangeUnavailable(
            "source_range_unavailable: public history limit"
        )
        with patch(
            "scripts.fetch_dex.fetch_pool_ohlcv",
            side_effect=range_error,
        ) as fetch, patch("scripts.fetch_dex.time.sleep") as sleep:
            rows = fetch_existing_pools(
                pools,
                attempt_records=attempts,
                start_date="2025-01-01",
                end_date="2025-01-02",
                fail_on_incomplete=False,
            )

        self.assertEqual(rows, [])
        fetch.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(len(attempts), 2)
        self.assertTrue(
            all(item["status"] == "unsupported" for item in attempts)
        )

    def test_every_configured_token_has_chain_config(self):
        token_rows = read_token_config(TOKEN_CONFIG_PATH)
        chain_rows = read_token_chain_config(TOKEN_CHAIN_CONFIG_PATH, token_rows)
        grouped_rows = group_chain_rows_by_token(chain_rows)

        configured_tokens = set()
        for token in token_rows:
            configured_tokens.add(token["token_symbol"])

        self.assertEqual(configured_tokens, set(grouped_rows.keys()))

    def test_runtime_registry_adds_dex_identity_without_cex_guess(self):
        record = {
            "token_symbol": "XYZ",
            "chain": "base",
            "contract_address": "0x" + "12" * 20,
            "coingecko_id": None,
            "status": "active",
        }
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "scripts.fetch_dex.TokenRegistry.list_records",
                return_value=[record],
            ) as list_records:
                tokens, chains = merge_runtime_token_config(
                    [{"token_symbol": "AAVE"}],
                    [
                        {
                            "token_symbol": "AAVE",
                            "chain": "eth",
                            "contract_address": "0x" + "34" * 20,
                        }
                    ],
                )

        runtime_token = next(row for row in tokens if row["token_symbol"] == "XYZ")
        runtime_chain = next(row for row in chains if row["token_symbol"] == "XYZ")
        list_records.assert_called_once_with(statuses={"active"})
        self.assertEqual(runtime_token["cex_symbol"], "")
        self.assertEqual(runtime_token["primary_cex"], "")
        self.assertEqual(runtime_chain["chain"], "base")
        self.assertEqual(runtime_chain["contract_address"], record["contract_address"])

    def test_runtime_registry_excludes_pending_without_explicit_job_override(self):
        pending = {
            "token_symbol": "PEND",
            "chain": "base",
            "contract_address": "0x" + "56" * 20,
            "coingecko_id": None,
            "status": "pending",
            "last_job_id": "job-pending",
        }
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "scripts.fetch_dex.TokenRegistry.list_records",
                return_value=[pending],
            ):
                tokens, chains = merge_runtime_token_config([], [])

        self.assertEqual(tokens, [])
        self.assertEqual(chains, [])

    def test_pending_runtime_token_requires_exact_single_token_job_override(self):
        pending = {
            "token_symbol": "PEND",
            "chain": "base",
            "contract_address": "0x" + "56" * 20,
            "coingecko_id": None,
            "status": "pending",
            "last_job_id": "job-pending",
        }
        with patch.dict(
            os.environ,
            {"TOKEN_ONBOARDING_JOB_ID": "job-pending"},
            clear=True,
        ):
            with patch(
                "scripts.fetch_dex.TokenRegistry.list_records",
                return_value=[pending],
            ) as list_records:
                tokens, chains = merge_runtime_token_config(
                    [],
                    [],
                    token_symbols=["PEND"],
                )

        list_records.assert_called_once_with(statuses={"active", "pending"})
        self.assertEqual([row["token_symbol"] for row in tokens], ["PEND"])
        self.assertEqual([row["token_symbol"] for row in chains], ["PEND"])

    def test_pending_runtime_token_rejects_wrong_job_override(self):
        pending = {
            "token_symbol": "PEND",
            "chain": "base",
            "contract_address": "0x" + "56" * 20,
            "coingecko_id": None,
            "status": "pending",
            "last_job_id": "job-pending",
        }
        with patch.dict(
            os.environ,
            {"TOKEN_ONBOARDING_JOB_ID": "wrong-job"},
            clear=True,
        ):
            with patch(
                "scripts.fetch_dex.TokenRegistry.list_records",
                return_value=[pending],
            ):
                with self.assertRaisesRegex(ValueError, "does not match"):
                    merge_runtime_token_config(
                        [],
                        [],
                        token_symbols=["PEND"],
                    )

    def test_pending_runtime_token_override_rejects_multiple_requested_tokens(self):
        with patch.dict(
            os.environ,
            {"TOKEN_ONBOARDING_JOB_ID": "job-pending"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "exactly one"):
                merge_runtime_token_config(
                    [],
                    [],
                    token_symbols=["PEND", "OTHER"],
                )

    def test_request_sleep_matches_public_rate_limit(self):
        self.assertEqual(REQUEST_SLEEP_SECONDS, 15.0)

    def test_filter_token_rows_keeps_requested_tokens(self):
        rows = [
            {"token_symbol": "UNI"},
            {"token_symbol": "AAVE"},
            {"token_symbol": "COMP"},
        ]

        result = filter_token_rows(rows, ["COMP", "AAVE"])

        self.assertEqual(
            result,
            [
                {"token_symbol": "AAVE"},
                {"token_symbol": "COMP"},
            ],
        )

    def test_replace_token_rows_keeps_unselected_existing_rows(self):
        existing_rows = [
            {"token_symbol": "UNI", "value": "old uni"},
            {"token_symbol": "COMP", "value": "old comp"},
        ]
        new_rows = [
            {"token_symbol": "COMP", "value": "new comp"},
        ]

        result = replace_token_rows(existing_rows, new_rows, ["COMP"])

        self.assertEqual(
            result,
            [
                {"token_symbol": "UNI", "value": "old uni"},
                {"token_symbol": "COMP", "value": "new comp"},
            ],
        )

    def test_deduplicate_pool_volume_rows_keeps_one_daily_pool_row(self):
        first_row = {
            "date": "2026-01-14",
            "token_symbol": "COMP",
            "chain": "eth",
            "pool_address": "0xpool",
            "open": 26.55,
            "dex_volume_usd": 3991.05,
        }
        duplicate_row = {
            "date": "2026-01-14",
            "token_symbol": "COMP",
            "chain": "eth",
            "pool_address": "0xpool",
            "open": 27.23,
            "dex_volume_usd": 3991.05,
        }

        result = deduplicate_pool_volume_rows([first_row, duplicate_row])

        self.assertEqual(result, [first_row])

    def test_safe_float_converts_missing_values_to_zero(self):
        self.assertEqual(safe_float(None), 0.0)
        self.assertEqual(safe_float(""), 0.0)
        self.assertEqual(safe_float("123.45"), 123.45)

    def test_pool_result_preserves_missing_source_facts(self):
        result = build_pool_result(
            {
                "attributes": {"address": "0xpool", "name": "TOKEN / USD"},
                "relationships": {},
            }
        )

        self.assertIsNone(result["pool_tvl_usd"])
        self.assertIsNone(result["volume_24h_usd"])

    def test_get_retry_wait_seconds_defaults_rate_limit_to_one_minute(self):
        result = get_retry_wait_seconds(429, None)
        self.assertEqual(result, 65)

    def test_get_retry_wait_seconds_uses_retry_after_header(self):
        result = get_retry_wait_seconds(429, "12")
        self.assertEqual(result, 12)

    def test_get_retry_wait_seconds_rejects_zero_retry_after(self):
        result = get_retry_wait_seconds(429, "0")
        self.assertEqual(result, 65)

    def test_get_status_code_detects_429_from_error_text(self):
        error = RuntimeError("HTTP Error 429: Too Many Requests")

        result = get_status_code(error)

        self.assertEqual(result, 429)

    def test_choose_main_pool_uses_highest_24h_volume(self):
        pools = [
            {
                "attributes": {
                    "address": "0xsmall",
                    "name": "SMALL / WETH",
                    "reserve_in_usd": "100",
                    "volume_usd": {"h24": "1000"},
                },
                "relationships": {
                    "base_token": {"data": {"id": "eth_0xaaa"}},
                    "quote_token": {"data": {"id": "eth_0xbbb"}},
                    "dex": {"data": {"id": "uniswap_v2"}},
                },
            },
            {
                "attributes": {
                    "address": "0xbig",
                    "name": "BIG / WETH",
                    "reserve_in_usd": "5000",
                    "volume_usd": {"h24": "500"},
                },
                "relationships": {
                    "base_token": {"data": {"id": "eth_0xccc"}},
                    "quote_token": {"data": {"id": "eth_0xddd"}},
                    "dex": {"data": {"id": "uniswap_v3"}},
                },
            },
        ]

        result = choose_main_pool(pools)

        self.assertEqual(result["pool_address"], "0xsmall")
        self.assertEqual(result["dex"], "uniswap_v2")
        self.assertEqual(result["pool_name"], "SMALL / WETH")
        self.assertEqual(result["pool_tvl_usd"], 100.0)
        self.assertEqual(result["volume_24h_usd"], 1000.0)

    def test_choose_top_pools_uses_three_highest_24h_volumes(self):
        pools = [
            {
                "attributes": {
                    "address": "0xone",
                    "name": "ONE / WETH",
                    "reserve_in_usd": "100",
                    "volume_usd": {"h24": "10"},
                },
                "relationships": {
                    "base_token": {"data": {"id": "eth_0xone"}},
                    "quote_token": {"data": {"id": "eth_0xweth"}},
                    "dex": {"data": {"id": "uniswap_v2"}},
                },
            },
            {
                "attributes": {
                    "address": "0xtwo",
                    "name": "TWO / WETH",
                    "reserve_in_usd": "200",
                    "volume_usd": {"h24": "40"},
                },
                "relationships": {
                    "base_token": {"data": {"id": "eth_0xtwo"}},
                    "quote_token": {"data": {"id": "eth_0xweth"}},
                    "dex": {"data": {"id": "uniswap_v3"}},
                },
            },
            {
                "attributes": {
                    "address": "0xthree",
                    "name": "THREE / WETH",
                    "reserve_in_usd": "300",
                    "volume_usd": {"h24": "30"},
                },
                "relationships": {
                    "base_token": {"data": {"id": "eth_0xthree"}},
                    "quote_token": {"data": {"id": "eth_0xweth"}},
                    "dex": {"data": {"id": "sushiswap"}},
                },
            },
            {
                "attributes": {
                    "address": "0xfour",
                    "name": "FOUR / WETH",
                    "reserve_in_usd": "400",
                    "volume_usd": {"h24": "20"},
                },
                "relationships": {
                    "base_token": {"data": {"id": "eth_0xfour"}},
                    "quote_token": {"data": {"id": "eth_0xweth"}},
                    "dex": {"data": {"id": "curve"}},
                },
            },
        ]

        result = choose_top_pools(pools, 3)

        addresses = []
        for pool in result:
            addresses.append(pool["pool_address"])

        self.assertEqual(addresses, ["0xtwo", "0xthree", "0xfour"])

    def test_top_pool_count_is_five_for_multichain_dex(self):
        self.assertEqual(TOP_POOL_COUNT, 5)

    def test_group_chain_rows_by_token_groups_multichain_config(self):
        rows = [
            {
                "token_symbol": "UNI",
                "chain": "eth",
                "contract_address": "0xeth",
            },
            {
                "token_symbol": "UNI",
                "chain": "arbitrum",
                "contract_address": "0xarb",
            },
            {
                "token_symbol": "AAVE",
                "chain": "eth",
                "contract_address": "0xaave",
            },
        ]

        result = group_chain_rows_by_token(rows)

        self.assertEqual(len(result["UNI"]), 2)
        self.assertEqual(result["UNI"][0]["chain"], "eth")
        self.assertEqual(result["UNI"][1]["chain"], "arbitrum")
        self.assertEqual(len(result["AAVE"]), 1)

    def test_get_token_side_detects_quote_token(self):
        base_address = "0x" + "11" * 20
        target_address = "0x" + "ab" * 20
        pool = {
            "base_token_id": "eth_" + base_address,
            "quote_token_id": "eth_0x" + "AB" * 20,
        }

        result = get_token_side(pool, "eth", target_address)

        self.assertEqual(result, "quote")

    def test_get_token_side_rejects_pool_without_target_token(self):
        pool = {
            "base_token_id": "eth_0x" + "11" * 20,
            "quote_token_id": "eth_0x" + "22" * 20,
        }

        with self.assertRaisesRegex(ValueError, "pool_token_mismatch"):
            get_token_side(pool, "eth", "0x" + "33" * 20)

    def test_get_token_side_preserves_solana_address_case(self):
        target_address = "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"
        case_changed_address = "jUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"
        exact_pool = {
            "base_token_id": "solana_" + target_address,
            "quote_token_id": (
                "solana_4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"
            ),
        }
        pool = {
            "base_token_id": "solana_" + case_changed_address,
            "quote_token_id": (
                "solana_4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"
            ),
        }

        self.assertEqual(get_token_side(exact_pool, "solana", target_address), "base")
        with self.assertRaisesRegex(ValueError, "pool_token_mismatch"):
            get_token_side(pool, "solana", target_address)

    def test_sort_pools_by_volume_descending(self):
        pools = [
            {"attributes": {"volume_usd": {"h24": "10"}}},
            {"attributes": {"volume_usd": {"h24": "30"}}},
            {"attributes": {"volume_usd": {"h24": "20"}}},
        ]

        result = sort_pools_by_volume(pools)

        volumes = []
        for pool in result:
            volume = pool["attributes"]["volume_usd"]["h24"]
            volumes.append(volume)

        self.assertEqual(volumes, ["30", "20", "10"])

    def test_convert_ohlcv_row_maps_volume_to_dex_volume(self):
        ohlcv = [
            1704067200,
            7.10,
            7.50,
            7.00,
            7.30,
            7300.0,
        ]

        pool = {
            "token_symbol": "UNI",
            "chain": "eth",
            "dex": "uniswap_v3",
            "pool_address": "0xpool",
            "pool_name": "UNI / WETH",
            "pool_tvl_usd": 1000000.0,
        }

        result = convert_ohlcv_row(ohlcv, pool)

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["token_symbol"], "UNI")
        self.assertEqual(result["chain"], "eth")
        self.assertEqual(result["dex"], "uniswap_v3")
        self.assertEqual(result["pool_address"], "0xpool")
        self.assertEqual(result["open"], 7.10)
        self.assertEqual(result["high"], 7.50)
        self.assertEqual(result["low"], 7.00)
        self.assertEqual(result["close"], 7.30)
        self.assertEqual(result["dex_volume_usd"], 7300.0)
        self.assertEqual(result["pool_tvl_usd"], 1000000.0)

    def test_historical_ohlcv_does_not_repeat_current_tvl_snapshot(self):
        ohlcv = [1704067200, 7.10, 7.50, 7.00, 7.30, 7300.0]
        pool = {
            "token_symbol": "UNI",
            "chain": "eth",
            "dex": "uniswap_v3",
            "pool_address": "0xpool",
            "pool_name": "UNI / WETH",
            "pool_tvl_usd": 1000000.0,
        }

        result = convert_ohlcv_row(ohlcv, pool, include_tvl_snapshot=False)

        self.assertIsNone(result["pool_tvl_usd"])

    def test_write_pool_rows_accepts_token_id_fields(self):
        with TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "dex_pools.csv"
            pools = [
                {
                    "token_symbol": "UNI",
                    "chain": "eth",
                    "contract_address": "0xtoken",
                    "dex": "uniswap_v3",
                    "pool_address": "0xpool",
                    "pool_name": "UNI / WETH",
                    "pool_tvl_usd": 1000.0,
                    "volume_24h_usd": 100.0,
                    "ohlcv_token": "base",
                    "base_token_id": "eth_0xtoken",
                    "quote_token_id": "eth_0xquote",
                }
            ]

            write_pool_rows(pools, output_path)

            text = output_path.read_text()
            self.assertIn("base_token_id", text)
            self.assertIn("quote_token_id", text)

            content = output_path.read_bytes()
            self.assertNotIn(b"\r\n", content)

    def test_aggregate_dex_pool_rows_sums_top_pool_volume(self):
        rows = [
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "chain": "eth",
                "dex": "uniswap_v3",
                "pool_address": "0xpool1",
                "pool_name": "UNI / WETH",
                "dex_volume_usd": 100.0,
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "chain": "eth",
                "dex": "sushiswap",
                "pool_address": "0xpool2",
                "pool_name": "UNI / USDC",
                "dex_volume_usd": 200.0,
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "chain": "eth",
                "dex": "curve",
                "pool_address": "0xpool3",
                "pool_name": "UNI / ETH",
                "dex_volume_usd": 300.0,
            },
        ]

        result = aggregate_dex_pool_rows(rows)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["date"], "2024-01-01")
        self.assertEqual(result[0]["token_symbol"], "UNI")
        self.assertEqual(result[0]["chain"], "eth")
        self.assertEqual(result[0]["dex_volume_usd"], 600.0)
        self.assertEqual(result[0]["pool_count"], 3)
        self.assertEqual(result[0]["included_dexes"], "curve;sushiswap;uniswap_v3")
        self.assertEqual(result[0]["included_pool_addresses"], "0xpool1;0xpool2;0xpool3")

    def test_aggregate_dex_pool_rows_keeps_selected_chains(self):
        rows = [
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "chain": "eth",
                "dex": "uniswap_v3",
                "pool_address": "0xpool1",
                "pool_name": "UNI / WETH",
                "dex_volume_usd": 100.0,
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "chain": "arbitrum",
                "dex": "uniswap_v3_arbitrum",
                "pool_address": "0xpool2",
                "pool_name": "UNI / WETH",
                "dex_volume_usd": 200.0,
            },
        ]

        result = aggregate_dex_pool_rows(rows)

        self.assertEqual(result[0]["selected_chains"], "arbitrum;eth")

    def test_filter_complete_dates_keeps_dates_with_all_tokens(self):
        rows = [
            {"date": "2024-01-01", "token_symbol": "AAVE"},
            {"date": "2024-01-01", "token_symbol": "UNI"},
            {"date": "2024-01-02", "token_symbol": "UNI"},
        ]

        result = filter_complete_dates(rows, 2)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["date"], "2024-01-01")
        self.assertEqual(result[1]["date"], "2024-01-01")


if __name__ == "__main__":
    unittest.main()
