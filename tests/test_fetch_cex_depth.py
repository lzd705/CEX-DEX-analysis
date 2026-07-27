import csv
import hashlib
import json
import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from scripts.fetch_cex_depth import (
    DEPTH_BANDS_BPS,
    HISTORY_FILENAME,
    LATEST_FILENAME,
    collect_depth,
    depth_metrics,
    load_markets_from_csv,
    load_markets_from_database,
    observed_row,
    parse_book,
    publish_snapshot,
    source_request,
    validate_snapshot,
)


def market(token="UNI", exchange="binance", symbol="UNI/USDT"):
    return {
        "token_symbol": token,
        "exchange": exchange,
        "cex_symbol": symbol,
    }


def complete_book():
    return {
        "bids": [
            (Decimal("99.99"), Decimal("2")),
            (Decimal("98.90"), Decimal("5")),
        ],
        "asks": [
            (Decimal("100.01"), Decimal("3")),
            (Decimal("101.10"), Decimal("7")),
        ],
        "source_instrument": "UNIUSDT",
        "source_sequence": "123",
        "source_observed_at": "2026-07-27T00:00:00+00:00",
        "source_endpoint": "https://example.test/depth",
        "raw": b'{"book":"raw"}',
        "source_quote_asset": "USDT",
        "quote_to_usd": Decimal("1"),
        "quote_conversion_method": "USDT=USD proxy",
        "quote_conversion_endpoint": "",
        "quote_conversion_response_sha256": "",
        "full_book_reported": False,
    }


class FetchCexDepthTest(unittest.TestCase):
    def test_depth_metrics_known_answer_and_complete_bands(self):
        result = depth_metrics(
            complete_book()["bids"],
            complete_book()["asks"],
            quote_to_usd=Decimal("1"),
            requested_limit=100,
            full_book_reported=False,
        )
        self.assertEqual(result["best_bid"], "99.99")
        self.assertEqual(result["best_ask"], "100.01")
        self.assertEqual(result["midpoint"], "100.00")
        self.assertEqual(result["spread_quote"], "0.02")
        self.assertEqual(result["spread_bps"], "2.0000")
        self.assertEqual(result["bid_depth_10bps_usd"], "199.98")
        self.assertEqual(result["ask_depth_10bps_usd"], "300.03")
        self.assertEqual(result["total_depth_10bps_usd"], "500.01")
        self.assertTrue(all(result[f"depth_{band}bps_complete"] == "1" for band in DEPTH_BANDS_BPS))

    def test_limited_book_is_marked_partial_not_complete(self):
        book = complete_book()
        book["bids"] = book["bids"][:1]
        book["asks"] = book["asks"][:1]
        row = observed_row(
            market(),
            book,
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        self.assertEqual(row["status"], "partial")
        self.assertEqual(row["depth_10bps_complete"], "0")
        self.assertIn("observed lower bound", row["error"])

    def test_full_book_source_can_prove_completeness(self):
        book = complete_book()
        book["bids"] = book["bids"][:1]
        book["asks"] = book["asks"][:1]
        book["full_book_reported"] = True
        row = observed_row(
            market(exchange="coinbase"),
            book,
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        self.assertEqual(row["status"], "observed")
        self.assertEqual(row["depth_100bps_complete"], "1")

    def test_parse_supported_exchange_shapes(self):
        cases = {
            "binance": {"bids": [["100", "2"]], "asks": [["101", "3"]], "lastUpdateId": 1},
            "okx": {
                "code": "0",
                "data": [{"bids": [["100", "2", "1", "1"]], "asks": [["101", "3", "1", "1"]], "ts": "1"}],
            },
            "bybit": {
                "retCode": 0,
                "result": {"s": "UNIUSDT", "b": [["100", "2"]], "a": [["101", "3"]], "ts": "1"},
            },
            "kucoin": {
                "code": "200000",
                "data": {"bids": [["100", "2"]], "asks": [["101", "3"]], "time": 1},
            },
            "gate": {"bids": [["100", "2"]], "asks": [["101", "3"]], "current": 1},
            "bitget": {
                "code": "00000",
                "data": {"bids": [["100", "2"]], "asks": [["101", "3"]], "ts": "1"},
            },
            "mexc": {"bids": [["100", "2"]], "asks": [["101", "3"]], "lastUpdateId": 1},
            "htx": {
                "status": "ok",
                "tick": {"bids": [[100, 2]], "asks": [[101, 3]], "ts": 1},
            },
            "coinbase": {
                "bids": [["100", "2", 1]],
                "asks": [["101", "3", 1]],
                "sequence": 1,
                "time": "2026-07-27T00:00:00Z",
            },
            "kraken": {
                "error": [],
                "result": {"UNIUSD": {"bids": [["100", "2", 1]], "asks": [["101", "3", 1]]}},
            },
            "crypto_com": {
                "code": 0,
                "result": {
                    "instrument_name": "UNI_USDT",
                    "data": [{"bids": [["100", "2", "1"]], "asks": [["101", "3", "1"]], "t": 1}],
                },
            },
            "upbit": [
                {
                    "market": "KRW-UNI",
                    "timestamp": 1,
                    "orderbook_units": [
                        {"bid_price": 100, "bid_size": 2, "ask_price": 101, "ask_size": 3}
                    ],
                }
            ],
        }
        for exchange, payload in cases.items():
            with self.subTest(exchange=exchange):
                result = parse_book(exchange, payload, requested_instrument="UNIUSDT")
                self.assertEqual(result["bids"][0], (Decimal("100"), Decimal("2")))
                self.assertEqual(result["asks"][0], (Decimal("101"), Decimal("3")))

    def test_parse_skips_zero_placeholders_without_counting_depth(self):
        result = parse_book(
            "binance",
            {
                "bids": [["100", "0"], ["99", "2"]],
                "asks": [["0", "3"], ["101", "4"]],
            },
            requested_instrument="UNIUSDT",
        )
        self.assertEqual(result["bids"], [(Decimal("99"), Decimal("2"))])
        self.assertEqual(result["asks"], [(Decimal("101"), Decimal("4"))])

    def test_source_requests_use_public_spot_order_books(self):
        expectations = {
            "binance": "/api/v3/depth",
            "okx": "/api/v5/market/books",
            "bybit": "/v5/market/orderbook",
            "kucoin": "/api/v1/market/orderbook/level2_100",
            "gate": "/api/v4/spot/order_book",
            "bitget": "/api/v2/spot/market/orderbook",
            "mexc": "/api/v3/depth",
            "htx": "/market/depth",
            "coinbase": "/products/UNI-USD/book",
            "kraken": "/0/public/Depth",
            "crypto_com": "/public/get-book",
        }
        for exchange, path in expectations.items():
            with self.subTest(exchange=exchange):
                url, instrument, _quote, _full = source_request(exchange, "UNI/USDT")
                self.assertIn(path, url)
                self.assertTrue(instrument)

    def test_database_and_csv_inventory_are_unique(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            database = directory / "facts.sqlite3"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "CREATE TABLE cex_market_daily (token_symbol TEXT, exchange TEXT, cex_symbol TEXT)"
                )
                connection.executemany(
                    "INSERT INTO cex_market_daily VALUES (?, ?, ?)",
                    [
                        ("UNI", "binance", "UNI/BUSD"),
                        ("UNI", "binance", "UNI/USDT"),
                        ("AAVE", "okx", "AAVE/USDT"),
                    ],
                )
                connection.commit()
            finally:
                connection.close()
            csv_path = directory / "cex.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["token_symbol", "exchange", "cex_symbol"],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        market(symbol="UNI/BUSD"),
                        market(),
                        market(token="AAVE", exchange="okx", symbol="AAVE/USDT"),
                    ]
                )

            database_rows = load_markets_from_database(database)
            csv_rows = load_markets_from_csv(csv_path)

        self.assertEqual(len(database_rows), 2)
        self.assertEqual(len(csv_rows), 2)
        self.assertEqual(
            next(row for row in database_rows if row["token_symbol"] == "UNI")[
                "cex_symbol"
            ],
            "UNI/USDT",
        )
        self.assertEqual(
            next(row for row in csv_rows if row["token_symbol"] == "UNI")[
                "cex_symbol"
            ],
            "UNI/USDT",
        )

    def test_collect_writes_raw_manifest_and_complete_inventory(self):
        response = json.dumps(
            {
                "bids": [["99.99", "2"], ["98.9", "5"]],
                "asks": [["100.01", "3"], ["101.1", "7"]],
                "lastUpdateId": 123,
            }
        ).encode()

        def fake_request(_url):
            return json.loads(response), response

        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name)
            snapshot_id, rows = collect_depth(
                [market()],
                raw_root=raw_root,
                request=fake_request,
                sleep_seconds=0,
            )
            manifest = json.loads((raw_root / snapshot_id / "manifest.json").read_text())

        self.assertEqual(rows[0]["status"], "observed")
        self.assertEqual(manifest["market_count"], 1)
        self.assertEqual(manifest["status_counts"]["observed"], 1)
        validate_snapshot([market()], rows)

    def test_invalid_success_response_retains_source_raw_and_endpoint(self):
        valid_response = json.dumps(
            {
                "bids": [["99", "2"], ["98", "5"]],
                "asks": [["101", "3"], ["102", "7"]],
            }
        ).encode()
        empty_response = json.dumps({"bids": [], "asks": [["101", "3"]]}).encode()

        def fake_request(url):
            raw = empty_response if "AAVEUSDT" in url else valid_response
            return json.loads(raw), raw

        inventory = [
            market(),
            market(token="AAVE", symbol="AAVE/USDT"),
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name)
            snapshot_id, rows = collect_depth(
                inventory,
                raw_root=raw_root,
                request=fake_request,
                sleep_seconds=0,
            )
            failed_raw = raw_root / snapshot_id / "002-binance-AAVE.json"
            failed = next(row for row in rows if row["token_symbol"] == "AAVE")
            failed_bytes = failed_raw.read_bytes()

        self.assertEqual(failed["status"], "failed")
        self.assertIn("/api/v3/depth", failed["source_endpoint"])
        self.assertEqual(failed_bytes, empty_response)
        self.assertEqual(
            failed["raw_response_sha256"],
            hashlib.sha256(empty_response).hexdigest(),
        )

    def test_validate_requires_exact_inventory_and_observed_book(self):
        row = observed_row(
            market(),
            complete_book(),
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        validate_snapshot([market()], [row])
        with self.assertRaisesRegex(ValueError, "coverage"):
            validate_snapshot([market(), market(token="AAVE", symbol="AAVE/USDT")], [row])

    def test_publish_appends_history_and_replaces_latest(self):
        first = observed_row(
            market(),
            complete_book(),
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        second = {**first, "snapshot_id": "depth-2", "observed_at": "2026-07-27T01:00:00+00:00"}
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            publish_snapshot([first], output_dir=directory / "processed", publish_dir=directory / "local")
            publish_snapshot([second], output_dir=directory / "processed", publish_dir=directory / "local")
            with (directory / "local" / HISTORY_FILENAME).open(newline="", encoding="utf-8") as handle:
                history = list(csv.DictReader(handle))
            with (directory / "local" / LATEST_FILENAME).open(newline="", encoding="utf-8") as handle:
                latest = list(csv.DictReader(handle))

        self.assertEqual([row["snapshot_id"] for row in history], ["depth-1", "depth-2"])
        self.assertEqual([row["snapshot_id"] for row in latest], ["depth-2"])


if __name__ == "__main__":
    unittest.main()
