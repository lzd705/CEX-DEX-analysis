import json
import subprocess
import sys
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
from scripts.fetch_cex import merge_conclusive_attempt_windows
from scripts.fetch_cex import build_rows
from scripts.fetch_cex import cex_attempt_record
from scripts.fetch_cex import classify_attempt_error
from scripts.fetch_cex import write_attempt_ledger


class FetchCexTests(unittest.TestCase):
    def test_fetch_cex_standalone_cli_help_loads_local_dependencies(self):
        result = subprocess.run(
            [sys.executable, str(Path(fetch_cex.__file__).resolve()), "--help"],
            cwd=Path(fetch_cex.__file__).resolve().parents[1],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage:", result.stdout.lower())

    def test_upbit_candidate_is_the_exact_configured_quote_market(self):
        self.assertEqual(
            make_upbit_market_candidates("AAVE/USDT"),
            ["USDT-AAVE"],
        )
        self.assertEqual(
            make_upbit_market_candidates("AAVE/KRW"),
            ["KRW-AAVE"],
        )

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

    def test_coinbase_rows_keep_the_exact_usd_source_identity(self):
        candle = [1704067200, 7.00, 7.50, 7.10, 7.30, 1000.0]
        with patch.object(
            fetch_cex,
            "fetch_coinbase_candles",
            return_value=[candle],
        ) as source:
            rows = fetch_cex.fetch_exchange_rows(
                "UNI",
                "UNI/USDT",
                "coinbase",
                "2024-01-01",
                "2024-01-01",
            )

        source.assert_called_once_with(
            "UNI-USD", fetch_cex.LIMIT_DAYS, "2024-01-01", "2024-01-01"
        )
        self.assertEqual(rows[0]["cex_symbol"], "UNI/USD")

    def test_kraken_rows_keep_the_exact_usd_source_identity(self):
        kline = [1704067200, "7.10", "7.50", "7.00", "7.30", "7.25", "1000", 123]
        with patch.object(
            fetch_cex,
            "fetch_kraken_klines",
            return_value=[kline],
        ) as source:
            rows = fetch_cex.fetch_exchange_rows(
                "UNI",
                "UNI/USDT",
                "kraken",
                "2024-01-01",
                "2024-01-01",
            )

        source.assert_called_once_with(
            "UNIUSD", fetch_cex.LIMIT_DAYS, "2024-01-01", "2024-01-01"
        )
        self.assertEqual(rows[0]["cex_symbol"], "UNI/USD")

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
                {"start": (start_time - timedelta(days=1)).isoformat()},
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

    def test_coinbase_full_overlap_response_reaches_window_bounding(self):
        target_date = "2026-07-28"
        target_timestamp = int(
            datetime.strptime(target_date, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        candles = [
            [target_timestamp - offset * 86_400, 1, 1, 1, 1, 1]
            for offset in (2, 1, 0)
        ]
        attempts = []
        with patch(
            "scripts.fetch_cex.request_json",
            return_value=candles,
        ), patch.object(fetch_cex, "LIMIT_DAYS", 1), patch.object(
            fetch_cex.time,
            "sleep",
        ):
            rows = build_rows(
                [{"token_symbol": "UNI", "cex_symbol": "UNI/USDT"}],
                ["coinbase"],
                attempt_records=attempts,
                start_date=target_date,
                end_date=target_date,
            )

        self.assertEqual([row["date"] for row in rows], [target_date])
        self.assertEqual(attempts[0]["status"], "succeeded")

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

    def test_untyped_attempt_error_stays_generic_instead_of_guessing_source_failure(self):
        classified = classify_attempt_error(
            PermissionError("/srv/private/collector-secret")
        )

        self.assertEqual(classified["reason_code"], "collection_failed")
        self.assertIsNone(classified["http_status"])
        self.assertNotIn("private", classified["error"])
        self.assertNotIn("secret", classified["error"])

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

    def test_build_rows_records_coinbase_attempt_under_exact_usd_identity(self):
        attempts = []
        row = {
            "date": "2026-07-28",
            "token_symbol": "UNI",
            "exchange": "coinbase",
            "cex_symbol": "UNI/USD",
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "base_volume": 1,
            "quote_volume_usd": 1,
            "source_instrument": "UNI/USD",
        }
        with patch(
            "scripts.fetch_cex.fetch_exchange_rows",
            return_value=[row],
        ), patch.object(fetch_cex.time, "sleep"):
            rows = build_rows(
                [{"token_symbol": "UNI", "cex_symbol": "UNI/USDT"}],
                ["coinbase"],
                attempt_records=attempts,
                start_date="2026-07-28",
                end_date="2026-07-28",
            )

        self.assertEqual(rows, [row])
        self.assertEqual(attempts[0]["instrument"], "UNI/USD")
        self.assertEqual(attempts[0]["source_instrument"], "UNI/USD")

    def test_build_rows_bounds_source_rows_to_the_requested_window(self):
        attempts = []
        rows = [
            {
                "date": day_text,
                "token_symbol": "UNI",
                "exchange": "kraken",
                "cex_symbol": "UNI/USD",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "base_volume": 1,
                "quote_volume_usd": 1,
                "source_instrument": "UNI/USD",
            }
            for day_text in ("2026-07-27", "2026-07-28", "2026-07-29")
        ]
        with patch(
            "scripts.fetch_cex.fetch_exchange_rows",
            return_value=rows,
        ), patch.object(fetch_cex.time, "sleep"):
            bounded = build_rows(
                [{"token_symbol": "UNI", "cex_symbol": "UNI/USDT"}],
                ["kraken"],
                attempt_records=attempts,
                start_date="2026-07-28",
                end_date="2026-07-28",
            )

        self.assertEqual([row["date"] for row in bounded], ["2026-07-28"])
        self.assertEqual(attempts[0]["status"], "succeeded")
        self.assertEqual(attempts[0]["observed_dates"], ["2026-07-28"])

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

    def test_attempt_writer_rejects_wrong_type_and_invalid_alias_before_publication(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source_csv = root / "cex.csv"
            source_csv.write_text("date,token_symbol\n", encoding="utf-8")
            valid = cex_attempt_record("AAVE", "upbit", "AAVE/USDT", rows=[], start_date="2026-07-28", end_date="2026-07-28")
            invalid_alias = dict(valid, source_instrument="UNI/KRW", source_instrument_alias_validated=True)
            for name, candidate in (("wrong-type", dict(valid, market_type="dex")), ("alias", invalid_alias)):
                path = root / (name + ".json")
                with self.subTest(case=name), self.assertRaises(ValueError):
                    write_attempt_ledger(path, [valid, candidate], source_csv=source_csv)
                self.assertFalse(path.exists())

    def test_producer_and_writer_share_strict_cex_pair_validation(self):
        invalid_pairs = (
            "",
            " AAVE/USDT",
            "AAVE/US DT",
            "AAVE/USDT\n",
            "AAVE/US\x00DT",
            "ÅAVE/USDT",
            "A" * 33 + "/USDT",
            "AAVE/" + "U" * 33,
        )
        for pair in invalid_pairs:
            with self.subTest(path="producer", pair=pair), self.assertRaises(ValueError):
                cex_attempt_record(
                    "AAVE", "binance", pair, rows=[],
                    start_date="2026-07-28", end_date="2026-07-28",
                )

        valid = cex_attempt_record(
            "AAVE", "upbit", "AAVE/USDT", rows=[],
            start_date="2026-07-28", end_date="2026-07-28",
        )
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source_csv = root / "cex.csv"
            source_csv.write_text("date,token_symbol\n", encoding="utf-8")
            for pair in invalid_pairs:
                path = root / "invalid.json"
                with self.subTest(path="writer", pair=pair), self.assertRaises(ValueError):
                    write_attempt_ledger(
                        path, [dict(valid, instrument=pair)], source_csv=source_csv
                    )
                self.assertFalse(path.exists())
                with self.subTest(path="writer-source", pair=pair), self.assertRaises(ValueError):
                    write_attempt_ledger(
                        path,
                        [
                            dict(
                                valid,
                                source_instrument=pair,
                                source_instrument_alias_validated=True,
                            )
                        ],
                        source_csv=source_csv,
                    )
                self.assertFalse(path.exists())

    def test_producer_rejects_invalid_source_pair_before_alias_validation(self):
        invalid_sources = (
            "",
            " AAVE/KRW",
            "AAVE/KR W",
            "AAVE/KRW\n",
            "AAVE/KR\x00W",
            "AAVÉ/KRW",
            "A" * 33 + "/KRW",
            "AAVE/" + "K" * 33,
        )
        for source in invalid_sources:
            rows = [{"date": "2026-07-28", "cex_symbol": "AAVE/USDT", "source_instrument": source}]
            with self.subTest(source=source), self.assertRaises(ValueError):
                cex_attempt_record(
                    "AAVE", "upbit", "AAVE/USDT", rows=rows,
                    start_date="2026-07-28", end_date="2026-07-28",
                )

    def test_producer_accepts_exact_cex_pair_boundary_and_canonicalizes_case(self):
        pair = "a" * 32 + "/" + "q" * 32
        attempt = cex_attempt_record(
            "AAVE", "binance", pair, rows=[],
            start_date="2026-07-28", end_date="2026-07-28",
        )
        self.assertEqual(attempt["instrument"], pair.upper())
        self.assertEqual(len(attempt["instrument"]), 65)

    def test_exchange_writer_strips_only_transient_source_instrument(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "rows.csv"
            row = {"date": "2026-07-28", "token_symbol": "AAVE", "exchange": "upbit", "cex_symbol": "AAVE/USDT", "open": 1, "high": 1, "low": 1, "close": 1, "base_volume": 1, "quote_volume_usd": 1, "source_instrument": "AAVE/USDT"}
            write_exchange_rows([row], path)
            self.assertNotIn("source_instrument", path.read_text(encoding="utf-8"))
            with self.assertRaises(ValueError):
                write_exchange_rows([dict(row, unexpected="no")], path)

    def test_unbounded_full_rebuild_suppresses_attempt_ledger(self):
        with TemporaryDirectory() as directory, patch("scripts.fetch_cex.build_rows", return_value=[]), patch("scripts.fetch_cex.write_attempt_ledger") as writer:
            fetch_cex.main(exchanges=["binance"], output_dir=Path(directory))
        writer.assert_not_called()

    def test_upbit_uses_only_the_exact_configured_venue_instrument(self):
        candle = {
            "market": "USDT-AAVE",
            "candle_date_time_utc": "2026-07-28T00:00:00",
            "opening_price": 100,
            "high_price": 110,
            "low_price": 90,
            "trade_price": 105,
            "candle_acc_trade_volume": 10,
            "candle_acc_trade_price": 1050,
        }
        with patch(
            "scripts.fetch_cex.fetch_upbit_candles",
            return_value=[candle],
        ) as fetcher:
            rows = fetch_cex.build_upbit_rows("AAVE", "AAVE/USDT", 1)
        attempt = cex_attempt_record(
            "AAVE", "upbit", "AAVE/USDT", rows=rows, start_date="2026-07-28", end_date="2026-07-28"
        )

        fetcher.assert_called_once_with("USDT-AAVE", 1, end_date=None)
        self.assertEqual(rows[0]["cex_symbol"], "AAVE/USDT")
        self.assertEqual(
            {key: rows[0][key] for key in ("open", "high", "low", "close", "quote_volume_usd")},
            {"open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "quote_volume_usd": 1050.0},
        )
        self.assertEqual(
            {key: attempt[key] for key in ("instrument", "source_instrument", "source_instrument_alias_validated")},
            {"instrument": "AAVE/USDT", "source_instrument": "AAVE/USDT", "source_instrument_alias_validated": False},
        )

    def test_upbit_empty_candidates_publish_terminal_no_candles_with_lineage(self):
        with patch(
            "scripts.fetch_cex.fetch_upbit_candles",
            return_value=[],
        ):
            try:
                rows = fetch_cex.build_upbit_rows(
                    "AAVE",
                    "AAVE/USDT",
                    1,
                    start_date="2026-07-28",
                    end_date="2026-07-28",
                )
            except Exception as error:
                self.fail(
                    "successful empty Upbit candidates raised {!r}".format(
                        error
                    )
                )

        self.assertEqual(rows, [])
        self.assertEqual(
            getattr(rows, "candidate_outcomes", None),
            [
                {
                    "market": "USDT-AAVE",
                    "source_instrument": "AAVE/USDT",
                    "stage": "candles",
                    "status": "no_data",
                    "reason_code": "no_candles",
                    "http_status": None,
                    "observation_count": 0,
                },
            ],
        )
        attempt = cex_attempt_record(
            "AAVE",
            "upbit",
            "AAVE/USDT",
            rows=rows,
            start_date="2026-07-28",
            end_date="2026-07-28",
        )
        self.assertEqual(
            {
                key: attempt[key]
                for key in (
                    "instrument",
                    "source_instrument",
                    "source_instrument_alias_validated",
                    "status",
                    "outcome",
                    "reason_code",
                )
            },
            {
                "instrument": "AAVE/USDT",
                "source_instrument": None,
                "source_instrument_alias_validated": False,
                "status": "no_data",
                "outcome": "no_candles",
                "reason_code": "no_candles",
            },
        )

    def test_upbit_exact_instrument_transport_error_is_not_reclassified(self):
        source_error = urllib.error.HTTPError(
            "https://source.example/candles",
            503,
            "Unavailable",
            None,
            None,
        )
        with patch(
            "scripts.fetch_cex.fetch_upbit_candles",
            side_effect=source_error,
        ):
            with self.assertRaises(RuntimeError) as context:
                fetch_cex.build_upbit_rows("AAVE", "AAVE/USDT", 1)

        self.assertEqual(
            getattr(context.exception, "candidate_outcomes", None),
            [
                {
                    "market": "USDT-AAVE",
                    "source_instrument": "AAVE/USDT",
                    "stage": "candles",
                    "status": "failed",
                    "reason_code": "source_unavailable",
                    "http_status": 503,
                    "observation_count": 0,
                },
            ],
        )
        self.assertEqual(
            classify_attempt_error(context.exception)["reason_code"],
            "source_unavailable",
        )

    def test_explicit_upbit_krw_instrument_keeps_krw_identity_and_fx_lineage(self):
        candle = {
            "market": "KRW-AAVE",
            "candle_date_time_utc": "2026-07-28T00:00:00",
            "opening_price": 1000,
            "high_price": 1100,
            "low_price": 900,
            "trade_price": 1050,
            "candle_acc_trade_volume": 10,
            "candle_acc_trade_price": 10500,
        }
        reference = dict(candle, market="KRW-USDT", trade_price=1000)
        with patch(
            "scripts.fetch_cex.fetch_upbit_candles",
            side_effect=[[candle], [reference]],
        ):
            rows = fetch_cex.build_upbit_rows("AAVE", "AAVE/KRW", 1)

        self.assertEqual(rows[0]["cex_symbol"], "AAVE/KRW")
        self.assertEqual(rows[0]["source_instrument"], "AAVE/KRW")
        self.assertEqual(rows[0]["close"], 1.05)

    def test_upbit_canonicalization_preserves_ldo_usdt_review_identity(self):
        candle = {
            "market": "USDT-LDO", "candle_date_time_utc": "2026-07-28T00:00:00",
            "opening_price": 1, "high_price": 1.1, "low_price": 0.9,
            "trade_price": 1.05, "candle_acc_trade_volume": 10,
            "candle_acc_trade_price": 10.5,
        }
        with patch("scripts.fetch_cex.fetch_upbit_candles", return_value=[candle]):
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

    def test_upbit_merge_preserves_distinct_venue_instruments(self):
        existing = [
            {
                "date": "2026-07-01",
                "token_symbol": "MORPHO",
                "exchange": "upbit",
                "cex_symbol": "MORPHO/KRW",
                "close": 1.0,
            },
            {
                "date": "2026-07-02",
                "token_symbol": "MORPHO",
                "exchange": "upbit",
                "cex_symbol": "MORPHO/KRW",
                "close": 2.0,
            },
        ]
        canonical = [
            {
                "date": "2026-07-02",
                "token_symbol": "MORPHO",
                "exchange": "upbit",
                "cex_symbol": "MORPHO/USDT",
                "source_instrument": "MORPHO/USDT",
                "close": 2.5,
            },
        ]

        result = merge_exchange_rows(existing, canonical)

        self.assertEqual(
            {(row["cex_symbol"], row["date"]) for row in result},
            {
                ("MORPHO/KRW", "2026-07-01"),
                ("MORPHO/KRW", "2026-07-02"),
                ("MORPHO/USDT", "2026-07-02"),
            },
        )
        latest = next(
            row for row in result
            if row["date"] == "2026-07-02"
            and row["cex_symbol"] == "MORPHO/USDT"
        )
        self.assertEqual(latest["close"], 2.5)

    def test_partial_refresh_replaces_only_observed_dates_and_preserves_other_baseline_dates(self):
        existing = [
            {
                "date": day,
                "token_symbol": "MORPHO",
                "exchange": "upbit",
                "cex_symbol": symbol,
                "close": value,
            }
            for symbol, day, value in (
                ("MORPHO/KRW", "2026-07-01", 1.0),
                ("MORPHO/KRW", "2026-07-02", 2.0),
                ("MORPHO/USDT", "2026-07-01", 1.1),
                ("MORPHO/USDT", "2026-07-02", 2.1),
            )
        ]
        new_rows = [{
            "date": "2026-07-02",
            "token_symbol": "MORPHO",
            "exchange": "upbit",
            "cex_symbol": "MORPHO/USDT",
            "close": 2.5,
        }]
        attempt = {
            "token_symbol": "MORPHO",
            "exchange": "upbit",
            "instrument": "MORPHO/USDT",
            "requested_start_date": "2026-07-01",
            "requested_end_date": "2026-07-02",
            "observed_dates": ["2026-07-02"],
            "status": "partial",
            "reason_code": "no_candles",
        }

        result = merge_conclusive_attempt_windows(
            existing,
            new_rows,
            [attempt],
        )

        self.assertEqual(
            {(row["cex_symbol"], row["date"]) for row in result},
            {
                ("MORPHO/KRW", "2026-07-01"),
                ("MORPHO/KRW", "2026-07-02"),
                ("MORPHO/USDT", "2026-07-01"),
                ("MORPHO/USDT", "2026-07-02"),
            },
        )

    def test_opt_in_upbit_partial_migration_removes_legacy_krw_only_on_observed_dates(self):
        existing = [
            {
                "date": day,
                "token_symbol": token,
                "exchange": exchange,
                "cex_symbol": symbol,
                "close": value,
            }
            for token, exchange, symbol, day, value in (
                ("MORPHO", "upbit", "MORPHO/KRW", "2026-06-30", 0.5),
                ("MORPHO", "upbit", "MORPHO/KRW", "2026-07-01", 1.0),
                ("MORPHO", "upbit", "MORPHO/KRW", "2026-07-02", 2.0),
                ("MORPHO", "upbit", "MORPHO/USDT", "2026-07-01", 1.1),
                ("MORPHO", "upbit", "MORPHO/USDT", "2026-07-02", 2.1),
                ("AAVE", "upbit", "AAVE/KRW", "2026-07-01", 3.0),
                ("MORPHO", "binance", "MORPHO/KRW", "2026-07-01", 4.0),
            )
        ]
        new_rows = [{
            "date": "2026-07-02",
            "token_symbol": "MORPHO",
            "exchange": "upbit",
            "cex_symbol": "MORPHO/USDT",
            "close": 2.5,
        }]
        attempt = {
            "token_symbol": "MORPHO",
            "exchange": "upbit",
            "instrument": "MORPHO/USDT",
            "requested_start_date": "2026-07-01",
            "requested_end_date": "2026-07-02",
            "observed_dates": ["2026-07-02"],
            "status": "partial",
            "reason_code": "no_candles",
        }

        result = merge_conclusive_attempt_windows(
            existing,
            new_rows,
            [attempt],
            remove_legacy_upbit_krw_fallback=True,
        )

        self.assertEqual(
            {
                (row["token_symbol"], row["exchange"], row["cex_symbol"], row["date"])
                for row in result
            },
            {
                ("MORPHO", "upbit", "MORPHO/KRW", "2026-06-30"),
                ("MORPHO", "upbit", "MORPHO/KRW", "2026-07-01"),
                ("MORPHO", "upbit", "MORPHO/USDT", "2026-07-01"),
                ("MORPHO", "upbit", "MORPHO/USDT", "2026-07-02"),
                ("AAVE", "upbit", "AAVE/KRW", "2026-07-01"),
                ("MORPHO", "binance", "MORPHO/KRW", "2026-07-01"),
            },
        )

    def test_opt_in_upbit_migration_retains_legacy_krw_after_technical_failure(self):
        existing = [{
            "date": "2026-07-01",
            "token_symbol": "MORPHO",
            "exchange": "upbit",
            "cex_symbol": "MORPHO/KRW",
            "close": 1.0,
        }]
        attempt = {
            "token_symbol": "MORPHO",
            "exchange": "upbit",
            "instrument": "MORPHO/USDT",
            "requested_start_date": "2026-07-01",
            "requested_end_date": "2026-07-02",
            "status": "failed",
            "reason_code": "network",
        }

        result = merge_conclusive_attempt_windows(
            existing,
            [],
            [attempt],
            remove_legacy_upbit_krw_fallback=True,
        )

        self.assertEqual(result, existing)

    def test_partial_coinbase_usd_refresh_removes_legacy_label_only_on_observed_dates(self):
        existing = [
            {
                "date": day,
                "token_symbol": "UNI",
                "exchange": exchange,
                "cex_symbol": symbol,
                "close": value,
            }
            for exchange, symbol, day, value in (
                ("coinbase", "UNI/USDT", "2026-07-01", 1.0),
                ("coinbase", "UNI/USDT", "2026-07-02", 2.0),
                ("coinbase", "UNI/USDT", "2026-06-30", 0.5),
                ("binance", "UNI/USDT", "2026-07-01", 1.1),
            )
        ]
        new_rows = [{
            "date": "2026-07-02",
            "token_symbol": "UNI",
            "exchange": "coinbase",
            "cex_symbol": "UNI/USD",
            "close": 2.5,
        }]
        attempt = {
            "token_symbol": "UNI",
            "exchange": "coinbase",
            "instrument": "UNI/USD",
            "requested_start_date": "2026-07-01",
            "requested_end_date": "2026-07-02",
            "observed_dates": ["2026-07-02"],
            "status": "partial",
            "reason_code": "no_candles",
        }

        result = merge_conclusive_attempt_windows(existing, new_rows, [attempt])

        self.assertEqual(
            {
                (row["exchange"], row["cex_symbol"], row["date"])
                for row in result
            },
            {
                ("coinbase", "UNI/USDT", "2026-06-30"),
                ("coinbase", "UNI/USDT", "2026-07-01"),
                ("coinbase", "UNI/USD", "2026-07-02"),
                ("binance", "UNI/USDT", "2026-07-01"),
            },
        )

    def test_crypto_com_inventory_is_checked_once_before_candles(self):
        tokens = [
            {"token_symbol": "GMX", "cex_symbol": "GMX/USDT"},
            {"token_symbol": "UNI", "cex_symbol": "UNI/USDT"},
        ]
        candle = {
            "t": 1_788_134_400_000,
            "o": "7",
            "h": "8",
            "l": "6",
            "c": "7.5",
            "v": "10",
        }
        attempts = []
        with patch.object(
            fetch_cex,
            "fetch_crypto_com_instruments",
            return_value={"UNI_USDT"},
        ) as inventory, patch.object(
            fetch_cex,
            "fetch_crypto_com_candles",
            return_value=[candle],
        ) as candles, patch.object(fetch_cex.time, "sleep"):
            rows = build_rows(
                tokens,
                exchanges=["crypto_com"],
                attempt_records=attempts,
            )

        inventory.assert_called_once_with()
        candles.assert_called_once()
        self.assertEqual([row["token_symbol"] for row in rows], ["UNI"])
        by_token = {attempt["token_symbol"]: attempt for attempt in attempts}
        self.assertEqual(by_token["GMX"]["reason_code"], "not_listed")
        self.assertEqual(by_token["GMX"]["status"], "failed")
        self.assertEqual(by_token["UNI"]["status"], "succeeded")

    def test_crypto_com_daily_preflight_uses_exact_official_spot_schema(self):
        payload = {
            "code": 0,
            "result": {
                "data": [
                    {
                        "symbol": "AAVE_USDT",
                        "inst_type": "CCY_PAIR",
                        "display_name": "AAVE/USDT",
                        "base_ccy": "AAVE",
                        "quote_ccy": "USDT",
                        "tradable": True,
                    }
                ]
            },
        }
        with patch.object(fetch_cex, "request_json", return_value=payload):
            instruments = fetch_cex.fetch_crypto_com_instruments()

        self.assertEqual(instruments, {"AAVE_USDT"})

    def test_crypto_com_inventory_failure_never_falls_through_to_candles(self):
        tokens = [{"token_symbol": "GMX", "cex_symbol": "GMX/USDT"}]
        attempts = []
        with patch.object(
            fetch_cex,
            "fetch_crypto_com_instruments",
            side_effect=urllib.error.URLError("offline"),
        ), patch.object(
            fetch_cex,
            "fetch_crypto_com_candles",
        ) as candles, patch.object(fetch_cex.time, "sleep"):
            rows = build_rows(
                tokens,
                exchanges=["crypto_com"],
                attempt_records=attempts,
            )

        candles.assert_not_called()
        self.assertEqual(rows, [])
        self.assertEqual(attempts[0]["reason_code"], "network")
        self.assertEqual(attempts[0]["status"], "failed")

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

    def test_make_upbit_market_candidates_uses_exact_configured_quote(self):
        result = make_upbit_market_candidates("UNI/USDT")
        self.assertEqual(result, ["USDT-UNI"])

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

        result = convert_coinbase_candle(candle, "UNI", "UNI/USD")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "coinbase")
        self.assertEqual(result["cex_symbol"], "UNI/USD")
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

        result = convert_kraken_kline(kline, "UNI", "UNI/USD")

        self.assertEqual(result["date"], "2024-01-01")
        self.assertEqual(result["exchange"], "kraken")
        self.assertEqual(result["cex_symbol"], "UNI/USD")
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
