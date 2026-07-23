import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

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
from scripts.fetch_cex import aggregate_cex_rows
from scripts.fetch_cex import build_coverage_rows
from scripts.fetch_cex import select_stable_exchanges
from scripts.fetch_cex import write_exchange_rows


class FetchCexTests(unittest.TestCase):
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
