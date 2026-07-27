import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

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


class FetchDexTests(unittest.TestCase):
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
                            "base_token_id": "eth_0xweth",
                            "quote_token_id": "eth_0xcrv",
                            "tvl_usd": "100",
                            "volume_24h_usd": "50",
                        },
                        {
                            "token_symbol": "CRV",
                            "chain": "eth",
                            "pool_address": "0xinvalid",
                            "source_dex": "curve",
                            "source_pool_name": "USD / WETH / CRV",
                            "base_token_id": "eth_0xusd",
                            "quote_token_id": "eth_0xweth",
                            "tvl_usd": "200",
                            "volume_24h_usd": "60",
                        },
                    ]
                )

            pools, unresolved, resolved, invalid = load_existing_pool_inventory(
                pool_path,
                tvl_path,
                {"CRV": [{"chain": "eth", "contract_address": "0xcrv"}]},
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

        with patch(
            "scripts.fetch_dex.fetch_pool_ohlcv",
            side_effect=[observed, []],
        ), patch("scripts.fetch_dex.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "incomplete for 1 pools"):
                fetch_existing_pools(pools)

    def test_every_configured_token_has_chain_config(self):
        token_rows = read_token_config(TOKEN_CONFIG_PATH)
        chain_rows = read_token_chain_config(TOKEN_CHAIN_CONFIG_PATH, token_rows)
        grouped_rows = group_chain_rows_by_token(chain_rows)

        configured_tokens = set()
        for token in token_rows:
            configured_tokens.add(token["token_symbol"])

        self.assertEqual(configured_tokens, set(grouped_rows.keys()))

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
        pool = {
            "base_token_id": "eth_0xbase",
            "quote_token_id": "eth_0xtarget",
        }

        result = get_token_side(pool, "eth", "0xtarget")

        self.assertEqual(result, "quote")

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
