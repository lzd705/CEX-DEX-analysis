import json
import unittest
import urllib.error
import urllib.parse
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock
from unittest.mock import patch

from scripts import fetch_cex
from scripts.fetch_cex import convert_binance_kline
from scripts.fetch_cex import convert_bybit_kline
from scripts.fetch_cex import convert_bitget_kline
from scripts.fetch_cex import convert_coinbase_candle
from scripts.fetch_cex import convert_crypto_com_candle
from scripts.fetch_cex import convert_gate_kline
from scripts.fetch_cex import convert_htx_kline
from scripts.fetch_cex import convert_kraken_kline
from scripts.fetch_cex import convert_kucoin_kline
from scripts.fetch_cex import convert_mexc_kline
from scripts.fetch_cex import convert_okx_kline
from scripts.fetch_cex import convert_upbit_candle
from scripts.fetch_cex import make_bybit_symbol
from scripts.fetch_cex import make_binance_symbol
from scripts.fetch_cex import make_bitget_symbol
from scripts.fetch_cex import make_coinbase_product_id
from scripts.fetch_cex import make_crypto_com_instrument
from scripts.fetch_cex import make_gate_currency_pair
from scripts.fetch_cex import make_htx_symbol
from scripts.fetch_cex import make_kraken_pair
from scripts.fetch_cex import make_kucoin_symbol
from scripts.fetch_cex import make_mexc_symbol
from scripts.fetch_cex import make_okx_inst_id
from scripts.fetch_cex import make_upbit_market_candidates
from scripts.fetch_cex import MIN_EXCHANGE_COUNT
from scripts.fetch_cex import TLS_CONTEXT
from scripts.fetch_cex import aggregate_cex_rows
from scripts.fetch_cex import build_coverage_rows
from scripts.fetch_cex import select_stable_exchanges
from scripts.fetch_cex import write_exchange_rows
from scripts.fetch_cex import merge_exchange_rows
from scripts.fetch_cex import build_rows
from scripts.fetch_cex import cex_attempt_record
from scripts.fetch_cex import classify_attempt_error
from scripts.fetch_cex import write_attempt_ledger


class FetchCexTests(unittest.TestCase):
    def test_thirty_day_old_single_day_binance_request_returns_target_row(self):
        target_date = (
            datetime.now(timezone.utc).date() - timedelta(days=30)
        ).isoformat()
        target_time = datetime.strptime(target_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        target_ms = int(target_time.timestamp() * 1000)
        kline = [
            target_ms,
            "7.10",
            "7.50",
            "7.00",
            "7.30",
            "1000",
            target_ms + 86_399_999,
            "7300",
            1,
            "0",
            "0",
            "0",
        ]
        response = MagicMock()
        response.read.return_value = json.dumps([kline]).encode("utf-8")
        context = MagicMock()
        context.__enter__.return_value = response
        with patch.object(
            fetch_cex,
            "BINANCE_BASE_URLS",
            ["https://binance.test"],
        ):
            with patch(
                "scripts.fetch_cex.urllib.request.urlopen",
                return_value=context,
            ) as request:
                rows = fetch_cex.fetch_exchange_rows(
                    "UNI",
                    "UNI/USDT",
                    "binance",
                    target_date,
                    target_date,
                )

        requested_url = request.call_args.args[0]
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(requested_url).query)
        self.assertEqual(query["startTime"], [str(target_ms)])
        self.assertEqual(
            query["endTime"],
            [str(target_ms + 86_400_000 - 1)],
        )
        self.assertEqual(rows[0]["date"], target_date)

    def test_recent_binance_request_keeps_recent_endpoint_shape(self):
        response = MagicMock()
        response.read.return_value = b"[]"
        context = MagicMock()
        context.__enter__.return_value = response
        with patch.object(
            fetch_cex,
            "BINANCE_BASE_URLS",
            ["https://binance.test"],
        ):
            with patch(
                "scripts.fetch_cex.urllib.request.urlopen",
                return_value=context,
            ) as request:
                fetch_cex.fetch_binance_klines("UNIUSDT", 4)

        query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(request.call_args.args[0]).query
        )
        self.assertNotIn("startTime", query)
        self.assertNotIn("endTime", query)

    def test_historical_window_is_sent_to_all_other_cex_adapters(self):
        target_date = (
            datetime.now(timezone.utc).date() - timedelta(days=30)
        ).isoformat()
        start_time = datetime.strptime(target_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        start_seconds = int(start_time.timestamp())
        end_seconds = start_seconds + 86_400
        cases = [
            (
                fetch_cex.fetch_okx_klines,
                ("UNI-USDT", 4, target_date, target_date),
                {"code": "0", "data": []},
                {"after": str(end_seconds * 1000)},
                "/api/v5/market/history-candles",
            ),
            (
                fetch_cex.fetch_bybit_klines,
                ("UNIUSDT", 4, target_date, target_date),
                {"retCode": 0, "result": {"list": []}},
                {
                    "start": str(start_seconds * 1000),
                    "end": str(end_seconds * 1000 - 1),
                },
                "/v5/market/kline",
            ),
            (
                fetch_cex.fetch_kucoin_klines,
                ("UNI-USDT", 4, target_date, target_date),
                {"code": "200000", "data": []},
                {"startAt": str(start_seconds), "endAt": str(end_seconds)},
                "/api/v1/market/candles",
            ),
            (
                fetch_cex.fetch_gate_klines,
                ("UNI_USDT", 4, target_date, target_date),
                [],
                {"from": str(start_seconds), "to": str(end_seconds - 1)},
                "/api/v4/spot/candlesticks",
            ),
            (
                fetch_cex.fetch_bitget_klines,
                ("UNIUSDT", 4, target_date, target_date),
                {"code": "00000", "data": []},
                {
                    "startTime": str(start_seconds * 1000),
                    "endTime": str(end_seconds * 1000 - 1),
                },
                "/api/v2/spot/market/candles",
            ),
            (
                fetch_cex.fetch_mexc_klines,
                ("UNIUSDT", 4, target_date, target_date),
                [],
                {
                    "startTime": str(start_seconds * 1000),
                    "endTime": str(end_seconds * 1000 - 1),
                },
                "/api/v3/klines",
            ),
            (
                fetch_cex.fetch_coinbase_candles,
                ("UNI-USD", 4, target_date, target_date),
                [],
                {"start": start_time.isoformat()},
                "/products/UNI-USD/candles",
            ),
            (
                fetch_cex.fetch_kraken_klines,
                ("UNIUSD", 4, target_date, target_date),
                {"error": [], "result": {"UNIUSD": [], "last": "0"}},
                {"since": str(start_seconds)},
                "/0/public/OHLC",
            ),
            (
                fetch_cex.fetch_crypto_com_candles,
                ("UNI_USDT", 4, target_date, target_date),
                {"code": 0, "result": {"data": []}},
                {
                    "start_ts": str(start_seconds * 1000),
                    "end_ts": str(end_seconds * 1000 - 1),
                },
                "/exchange/v1/public/get-candlestick",
            ),
            (
                fetch_cex.fetch_upbit_candles,
                ("KRW-UNI", 4, target_date),
                [],
                {"to": datetime.fromtimestamp(end_seconds, timezone.utc).isoformat().replace("+00:00", "Z")},
                "/v1/candles/days",
            ),
        ]
        for function, arguments, response, expected, path in cases:
            with self.subTest(adapter=function.__name__):
                with patch(
                    "scripts.fetch_cex.request_json",
                    return_value=response,
                ) as request:
                    function(*arguments)
                requested_url = request.call_args.args[0]
                query = urllib.parse.parse_qs(
                    urllib.parse.urlsplit(requested_url).query
                )
                self.assertIn(path, requested_url)
                for key, value in expected.items():
                    self.assertEqual(query[key], [value])

    def test_recent_only_adapters_reach_historical_window_or_fail_explicitly(self):
        target_date = (
            datetime.now(timezone.utc).date() - timedelta(days=30)
        ).isoformat()
        target_timestamp = int(
            datetime.strptime(target_date, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        with patch(
            "scripts.fetch_cex.request_json",
            return_value={
                "status": "ok",
                "data": [{"id": target_timestamp}],
            },
        ) as request:
            fetch_cex.fetch_htx_klines(
                "uniusdt", 4, target_date, target_date
            )
        htx_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(request.call_args.args[0]).query
        )
        self.assertGreater(int(htx_query["size"][0]), 30)
        self.assertLessEqual(
            int(htx_query["size"][0]), fetch_cex.HTX_RECENT_BAR_CAP
        )

        recent_timestamp = int(datetime.now(timezone.utc).timestamp())
        with patch(
            "scripts.fetch_cex.request_json",
            return_value={
                "status": "ok",
                "data": [{"id": recent_timestamp}],
            },
        ):
            with self.assertRaisesRegex(
                fetch_cex.SourceRangeUnavailable,
                "source_range_unavailable",
            ):
                fetch_cex.fetch_htx_klines(
                    "uniusdt", 4, "2010-01-01", "2010-01-01"
                )

    def test_kraken_historical_response_does_not_drop_requested_first_row(self):
        target_date = (
            datetime.now(timezone.utc).date() - timedelta(days=30)
        ).isoformat()
        target_timestamp = int(
            datetime.strptime(target_date, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        rows = [
            [target_timestamp + offset * 86_400, "1", "2", "0.5", "1.5", "1", "1", 1]
            for offset in range(20)
        ]
        with patch(
            "scripts.fetch_cex.request_json",
            return_value={"error": [], "result": {"UNIUSD": rows, "last": "0"}},
        ):
            result = fetch_cex.fetch_kraken_klines(
                "UNIUSD",
                4,
                target_date,
                target_date,
            )

        self.assertEqual(result[0][0], target_timestamp)
        self.assertEqual(len(result), 20)

    def test_attempt_error_is_classified_without_raw_url_or_secret(self):
        error = urllib.error.HTTPError(
            "https://source.example/candles?api_key=secret",
            429,
            "Too Many Requests",
            None,
            None,
        )

        classified = classify_attempt_error(error)

        self.assertEqual(classified["reason_code"], "rate_limit")
        self.assertEqual(classified["http_status"], 429)
        self.assertNotIn("secret", classified["error"])
        self.assertNotIn("http", classified["error"])

    def test_build_rows_records_failed_adapter_attempt(self):
        attempts = []
        error = urllib.error.HTTPError(
            "https://source.example/candles?api_key=secret",
            503,
            "Unavailable",
            None,
            None,
        )
        with patch(
            "scripts.fetch_cex.fetch_exchange_rows",
            side_effect=error,
        ):
            rows = build_rows(
                [{"token_symbol": "UNI", "cex_symbol": "UNI/USDT"}],
                ["binance"],
                attempt_records=attempts,
                start_date="2026-07-28",
                end_date="2026-07-28",
            )

        self.assertEqual(rows, [])
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["status"], "failed")
        self.assertEqual(attempts[0]["reason_code"], "source_unavailable")
        self.assertEqual(attempts[0]["http_status"], 503)

    def test_source_range_attempt_is_unsupported_not_network_failed(self):
        attempt = cex_attempt_record(
            "UNI",
            "htx",
            "UNI/USDT",
            error=fetch_cex.SourceRangeUnavailable(
                "source_range_unavailable: capped endpoint"
            ),
            start_date="2010-01-01",
            end_date="2010-01-01",
        )

        self.assertEqual(attempt["status"], "unsupported")
        self.assertEqual(attempt["outcome"], "range_unavailable")
        self.assertEqual(
            attempt["reason_code"],
            "source_range_unavailable",
        )

    def test_attempt_ledger_is_bound_to_exact_published_csv(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source_csv = root / "cex_exchange_volume_daily.csv"
            source_csv.write_text("date,token_symbol\n", encoding="utf-8")
            ledger_path = root / "attempts.json"
            attempt = cex_attempt_record(
                "UNI",
                "binance",
                "UNI/USDT",
                rows=[],
                start_date="2026-07-28",
                end_date="2026-07-28",
            )

            payload = write_attempt_ledger(
                ledger_path,
                [attempt],
                source_csv=source_csv,
                start_date="2026-07-28",
                end_date="2026-07-28",
            )

            persisted = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted, payload)
            self.assertEqual(payload["attempt_count"], 1)
            self.assertEqual(len(payload["source_csv_sha256"]), 64)

    def test_attempt_ids_include_the_single_captured_completion_time(self):
        with patch("scripts.fetch_cex.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = datetime(2026, 7, 28, 1, tzinfo=timezone.utc)
            first = cex_attempt_record(
                "UNI", "binance", "UNI/USDT", rows=[], start_date="2026-07-28", end_date="2026-07-28"
            )
            mocked_datetime.now.return_value = datetime(2026, 7, 28, 2, tzinfo=timezone.utc)
            second = cex_attempt_record(
                "UNI", "binance", "UNI/USDT", rows=[], start_date="2026-07-28", end_date="2026-07-28"
            )

        self.assertEqual(len(first["attempt_id"]), 20)
        self.assertNotEqual(first["attempt_id"], second["attempt_id"])

    def test_attempt_writer_rejects_duplicate_or_incomplete_identity(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source_csv = root / "cex.csv"
            source_csv.write_text("date,token_symbol\n", encoding="utf-8")
            attempt = cex_attempt_record(
                "UNI", "binance", "UNI/USDT", rows=[], start_date="2026-07-28", end_date="2026-07-28"
            )
            with self.assertRaises(ValueError):
                write_attempt_ledger(root / "duplicate.json", [attempt, dict(attempt)], source_csv=source_csv)
            incomplete = dict(attempt, instrument=None)
            with self.assertRaises(ValueError):
                write_attempt_ledger(root / "incomplete.json", [incomplete], source_csv=source_csv)
            with self.assertRaises(ValueError):
                write_attempt_ledger(root / "long-id.json", [dict(attempt, attempt_id="x" * 65)], source_csv=source_csv)
            for field in ("token_symbol", "exchange", "instrument"):
                with self.subTest(field=field), self.assertRaises(ValueError):
                    write_attempt_ledger(root / (field + ".json"), [dict(attempt, **{field: ""})], source_csv=source_csv)

    def test_upbit_fallback_keeps_the_configured_canonical_symbol_and_alias_lineage(self):
        candle = {
            "market": "KRW-AAVE",
            "candle_date_time_utc": "2026-07-28T00:00:00",
            "opening_price": 100000,
            "high_price": 110000,
            "low_price": 90000,
            "trade_price": 105000,
            "candle_acc_trade_volume": 10,
            "candle_acc_trade_price": 1050000,
        }
        reference = dict(candle, market="KRW-USDT", trade_price=1000)
        with patch("scripts.fetch_cex.fetch_upbit_candles", side_effect=[[candle], [reference]]):
            rows = fetch_cex.build_upbit_rows("AAVE", "AAVE/USDT", 1)
        attempt = cex_attempt_record(
            "AAVE", "upbit", "AAVE/USDT", rows=rows, start_date="2026-07-28", end_date="2026-07-28"
        )

        self.assertEqual(rows[0]["cex_symbol"], "AAVE/USDT")
        self.assertEqual(
            {key: attempt[key] for key in ("instrument", "source_instrument", "source_instrument_alias_validated")},
            {"instrument": "AAVE/USDT", "source_instrument": "AAVE/KRW", "source_instrument_alias_validated": True},
        )

    def test_upbit_canonicalization_preserves_ldo_usdt_review_identity(self):
        candle = {
            "market": "KRW-LDO", "candle_date_time_utc": "2026-07-28T00:00:00",
            "opening_price": 1000, "high_price": 1100, "low_price": 900,
            "trade_price": 1050, "candle_acc_trade_volume": 10,
            "candle_acc_trade_price": 10500,
        }
        reference = dict(candle, market="KRW-USDT", trade_price=1000)
        with patch("scripts.fetch_cex.fetch_upbit_candles", side_effect=[[candle], [reference]]):
            rows = fetch_cex.build_upbit_rows("LDO", "LDO/USDT", 1)
        self.assertEqual(rows[0]["cex_symbol"], "LDO/USDT")

    def test_https_requests_use_a_verified_tls_context(self):
        self.assertEqual(TLS_CONTEXT.verify_mode, 2)
        self.assertTrue(TLS_CONTEXT.check_hostname)

    def test_merge_exchange_rows_updates_only_matching_natural_key(self):
        existing = [
            {"date": "2026-01-01", "token_symbol": "UNI", "exchange": "binance", "cex_symbol": "UNI/USDT", "close": 1.0},
            {"date": "2026-01-01", "token_symbol": "AAVE", "exchange": "binance", "cex_symbol": "AAVE/USDT", "close": 2.0},
        ]
        updated = [
            {"date": "2026-01-01", "token_symbol": "UNI", "exchange": "binance", "cex_symbol": "UNI/USDT", "close": 1.5},
            {"date": "2026-01-02", "token_symbol": "UNI", "exchange": "binance", "cex_symbol": "UNI/USDT", "close": 1.6},
        ]

        result = merge_exchange_rows(existing, updated)

        by_key = {(row["token_symbol"], row["date"]): row for row in result}
        self.assertEqual(by_key[("UNI", "2026-01-01")]["close"], 1.5)
        self.assertEqual(by_key[("UNI", "2026-01-02")]["close"], 1.6)
        self.assertEqual(by_key[("AAVE", "2026-01-01")]["close"], 2.0)

    def test_default_minimum_exchange_count_is_three(self):
        self.assertEqual(MIN_EXCHANGE_COUNT, 3)

    def test_make_binance_symbol_removes_slash(self):
        result = make_binance_symbol("UNI/USDT")
        self.assertEqual(result, "UNIUSDT")

    def test_make_okx_inst_id_replaces_slash_with_dash(self):
        result = make_okx_inst_id("UNI/USDT")
        self.assertEqual(result, "UNI-USDT")

    def test_make_bybit_symbol_removes_slash(self):
        result = make_bybit_symbol("UNI/USDT")
        self.assertEqual(result, "UNIUSDT")

    def test_make_kucoin_symbol_replaces_slash_with_dash(self):
        result = make_kucoin_symbol("UNI/USDT")
        self.assertEqual(result, "UNI-USDT")

    def test_make_gate_currency_pair_replaces_slash_with_underscore(self):
        result = make_gate_currency_pair("UNI/USDT")
        self.assertEqual(result, "UNI_USDT")

    def test_make_other_exchange_symbols(self):
        self.assertEqual(make_bitget_symbol("UNI/USDT"), "UNIUSDT")
        self.assertEqual(make_mexc_symbol("UNI/USDT"), "UNIUSDT")
        self.assertEqual(make_htx_symbol("UNI/USDT"), "uniusdt")
        self.assertEqual(make_coinbase_product_id("UNI/USDT"), "UNI-USD")
        self.assertEqual(make_kraken_pair("UNI/USDT"), "UNIUSD")

    def test_make_crypto_com_instrument_uses_underscore(self):
        result = make_crypto_com_instrument("UNI/USDT")
        self.assertEqual(result, "UNI_USDT")

    def test_make_upbit_market_candidates_prefers_krw(self):
        result = make_upbit_market_candidates("UNI/USDT")
        self.assertEqual(result, ["KRW-UNI", "USDT-UNI"])

    def test_convert_binance_kline_uses_quote_volume(self):
        kline = [
            1704067200000,
            "7.10",
            "7.50",
            "7.00",
            "7.30",
            "1000",
            1704153599999,
            "7300",
            123,
            "500",
            "3650",
            "0",
        ]

        result = convert_binance_kline(kline, "UNI", "UNI/USDT", "binance")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["token_symbol"], "UNI")
        self.assertEqual(result["exchange"], "binance")
        self.assertEqual(result["cex_symbol"], "UNI/USDT")
        self.assertEqual(result["open"], 7.10)
        self.assertEqual(result["high"], 7.50)
        self.assertEqual(result["low"], 7.00)
        self.assertEqual(result["close"], 7.30)
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_okx_kline_uses_quote_volume(self):
        kline = [
            "1704067200000",
            "7.10",
            "7.50",
            "7.00",
            "7.30",
            "1000",
            "7300",
            "7300",
            "1",
        ]

        result = convert_okx_kline(kline, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "okx")
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_bybit_kline_uses_turnover_as_quote_volume(self):
        kline = [
            "1704067200000",
            "7.10",
            "7.50",
            "7.00",
            "7.30",
            "1000",
            "7300",
        ]

        result = convert_bybit_kline(kline, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "bybit")
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_kucoin_kline_uses_turnover_as_quote_volume(self):
        kline = [
            "1704067200",
            "7.10",
            "7.30",
            "7.50",
            "7.00",
            "1000",
            "7300",
        ]

        result = convert_kucoin_kline(kline, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "kucoin")
        self.assertEqual(result["open"], 7.10)
        self.assertEqual(result["close"], 7.30)
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_gate_kline_uses_quote_volume(self):
        kline = [
            "1704067200",
            "7300",
            "7.30",
            "7.50",
            "7.00",
            "7.10",
            "1000",
            "true",
        ]

        result = convert_gate_kline(kline, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "gate")
        self.assertEqual(result["open"], 7.10)
        self.assertEqual(result["close"], 7.30)
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_bitget_kline_uses_quote_volume(self):
        kline = [
            "1704067200000",
            "7.10",
            "7.50",
            "7.00",
            "7.30",
            "1000",
            "7300",
            "7300",
        ]

        result = convert_bitget_kline(kline, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "bitget")
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_mexc_kline_uses_quote_volume(self):
        kline = [
            1704067200000,
            "7.10",
            "7.50",
            "7.00",
            "7.30",
            "1000",
            1704153600000,
            "7300",
        ]

        result = convert_mexc_kline(kline, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "mexc")
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_htx_kline_uses_vol_as_quote_volume(self):
        kline = {
            "id": 1704067200,
            "open": 7.10,
            "high": 7.50,
            "low": 7.00,
            "close": 7.30,
            "amount": 1000.0,
            "vol": 7300.0,
        }

        result = convert_htx_kline(kline, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "htx")
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_coinbase_candle_approximates_quote_volume(self):
        candle = [
            1704067200,
            7.00,
            7.50,
            7.10,
            7.30,
            1000.0,
        ]

        result = convert_coinbase_candle(candle, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "coinbase")
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_kraken_kline_approximates_quote_volume(self):
        kline = [
            1704067200,
            "7.10",
            "7.50",
            "7.00",
            "7.30",
            "7.25",
            "1000",
            123,
        ]

        result = convert_kraken_kline(kline, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "kraken")
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_crypto_com_candle_approximates_quote_volume(self):
        candle = {
            "t": 1704067200000,
            "o": "7.10",
            "h": "7.50",
            "l": "7.00",
            "c": "7.30",
            "v": "1000",
        }

        result = convert_crypto_com_candle(candle, "UNI", "UNI/USDT")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "crypto_com")
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_convert_upbit_candle_converts_krw_volume_to_usd(self):
        candle = {
            "market": "KRW-UNI",
            "candle_date_time_utc": "2024-01-01T00:00:00",
            "opening_price": 7100.0,
            "high_price": 7500.0,
            "low_price": 7000.0,
            "trade_price": 7300.0,
            "candle_acc_trade_volume": 1000.0,
            "candle_acc_trade_price": 7300000.0,
        }

        result = convert_upbit_candle(candle, "UNI", quote_to_usd=0.001)

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "upbit")
        self.assertEqual(result["cex_symbol"], "UNI/KRW")
        self.assertEqual(result["close"], 7.30)
        self.assertEqual(result["base_volume"], 1000.0)
        self.assertEqual(result["quote_volume_usd"], 7300.0)

    def test_aggregate_cex_rows_sums_volume_and_keeps_binance_close(self):
        rows = [
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "binance",
                "close": 7.30,
                "quote_volume_usd": 100.0,
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "okx",
                "close": 7.31,
                "quote_volume_usd": 200.0,
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "bybit",
                "close": 7.32,
                "quote_volume_usd": 300.0,
            },
        ]

        result = aggregate_cex_rows(rows)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["date"], "2024-01-01")
        self.assertEqual(result[0]["token_symbol"], "UNI")
        self.assertEqual(result[0]["close"], 7.30)
        self.assertEqual(result[0]["cex_volume_usd"], 600.0)
        self.assertEqual(result[0]["exchange_count"], 3)
        self.assertEqual(result[0]["included_exchanges"], "binance;bybit;okx")

    def test_aggregate_cex_rows_can_filter_incomplete_exchange_count(self):
        rows = [
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "binance",
                "close": 7.30,
                "quote_volume_usd": 100.0,
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "okx",
                "close": 7.31,
                "quote_volume_usd": 200.0,
            },
        ]

        result = aggregate_cex_rows(rows, required_exchange_count=3)

        self.assertEqual(result, [])

    def test_select_stable_exchanges_uses_full_history(self):
        rows = []

        for date in ["2024-01-01", "2024-01-02"]:
            for exchange in ["binance", "okx", "bybit"]:
                rows.append(
                    {
                        "date": date,
                        "token_symbol": "UNI",
                        "exchange": exchange,
                    }
                )

        rows.append(
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "kraken",
            }
        )

        result = select_stable_exchanges(
            rows,
            minimum_history_days=2,
            minimum_exchange_count=3,
            price_exchange="binance",
        )

        self.assertEqual(result, {"UNI": ["binance", "bybit", "okx"]})

    def test_build_coverage_rows_marks_selected_exchanges(self):
        rows = [
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "binance",
            },
            {
                "date": "2024-01-02",
                "token_symbol": "UNI",
                "exchange": "binance",
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "kraken",
            },
        ]

        result = build_coverage_rows(rows, {"UNI": ["binance"]})

        self.assertEqual(result[0]["token_symbol"], "UNI")
        self.assertEqual(result[0]["exchange"], "binance")
        self.assertEqual(result[0]["observation_days"], 2)
        self.assertEqual(result[0]["first_date"], "2024-01-01")
        self.assertEqual(result[0]["last_date"], "2024-01-02")
        self.assertEqual(result[0]["is_selected"], 1)
        self.assertEqual(result[1]["is_selected"], 0)

    def test_build_coverage_rows_includes_missing_exchange(self):
        rows = [
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "binance",
            },
        ]

        result = build_coverage_rows(
            rows,
            {"UNI": ["binance"]},
            token_symbols=["UNI"],
            exchanges=["binance", "okx"],
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[1]["exchange"], "okx")
        self.assertEqual(result[1]["observation_days"], 0)
        self.assertEqual(result[1]["first_date"], "")
        self.assertEqual(result[1]["last_date"], "")
        self.assertEqual(result[1]["is_selected"], 0)

    def test_write_exchange_rows_uses_lf_line_endings(self):
        rows = [
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "binance",
                "cex_symbol": "UNI/USDT",
                "open": 7.10,
                "high": 7.50,
                "low": 7.00,
                "close": 7.30,
                "base_volume": 1000.0,
                "quote_volume_usd": 7300.0,
            },
        ]

        with TemporaryDirectory() as directory:
            output_path = Path(directory) / "rows.csv"
            write_exchange_rows(rows, output_path)
            content = output_path.read_bytes()

        self.assertNotIn(b"\r\n", content)

    def test_aggregate_cex_rows_requires_complete_stable_set(self):
        rows = [
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "binance",
                "close": 7.30,
                "quote_volume_usd": 100.0,
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "okx",
                "close": 7.31,
                "quote_volume_usd": 200.0,
            },
            {
                "date": "2024-01-01",
                "token_symbol": "UNI",
                "exchange": "bybit",
                "close": 7.32,
                "quote_volume_usd": 300.0,
            },
            {
                "date": "2024-01-02",
                "token_symbol": "UNI",
                "exchange": "binance",
                "close": 7.40,
                "quote_volume_usd": 110.0,
            },
            {
                "date": "2024-01-02",
                "token_symbol": "UNI",
                "exchange": "bybit",
                "close": 7.42,
                "quote_volume_usd": 310.0,
            },
        ]

        result = aggregate_cex_rows(
            rows,
            stable_exchanges_by_token={
                "UNI": ["binance", "bybit", "okx"],
            },
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["date"], "2024-01-01")
        self.assertEqual(result[0]["cex_volume_usd"], 600.0)


if __name__ == "__main__":
    unittest.main()
