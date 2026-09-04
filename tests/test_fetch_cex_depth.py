import csv
import hashlib
import http.server
import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import scripts.fetch_cex_depth as fetch_cex_depth
from scripts.fetch_cex_depth import (
    CURRENT_FILENAME,
    DEPTH_BANDS_BPS,
    EXECUTION_LATEST_FILENAME,
    HISTORY_FILENAME,
    LATEST_FILENAME,
    collect_depth,
    collect_depth_with_execution,
    collect_cex_market_observation,
    depth_failure_reason_code,
    depth_metrics,
    ensure_full_publish_scope,
    execution_rows_for_book,
    failed_execution_rows,
    failure_row,
    load_cataloged_markets,
    load_markets_from_csv,
    load_markets_from_database,
    merge_exact_publication_bundle,
    observed_row,
    parse_book,
    preflight_publication_bundle,
    publish_exact_publication_bundle,
    publish_execution_snapshot,
    publish_full_publication_bundle,
    publish_snapshot,
    source_request,
    timestamp_text,
    upbit_book,
    validate_snapshot,
    binance_market_rules_projection,
)
from scripts.publication_gate import CoverageRegressionError
from scripts.execution_cost import (
    EXECUTION_DIRECTIONS,
    EXECUTION_NOTIONALS_USD,
    validate_execution_snapshot,
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


def write_snapshot_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class FetchCexDepthTest(unittest.TestCase):
    def test_request_json_does_not_follow_redirects(self):
        from scripts.fetch_cex_depth import request_json

        class RedirectHandler(http.server.BaseHTTPRequestHandler):
            target_requests = 0

            def do_GET(self):
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/target")
                    self.end_headers()
                    return
                if self.path == "/target":
                    type(self).target_requests += 1
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b"{}")
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, _format, *_args):
                pass

        server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), RedirectHandler
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with self.assertRaises(urllib.error.HTTPError) as raised:
                request_json(
                    "http://127.0.0.1:{}/redirect".format(server.server_port),
                    max_retries=1,
                )
            self.assertEqual(raised.exception.code, 302)
            self.assertEqual(RedirectHandler.target_requests, 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_shared_binance_rules_projection_preserves_exact_filters(self):
        payload = {
            "symbols": [{
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00010000",
                        "stepSize": "0.00010000",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5.00000000"},
                ],
            }],
        }
        self.assertEqual(
            binance_market_rules_projection(
                payload,
                base_asset="ETH",
                quote_asset="USDT",
                source_instrument="ETHUSDT",
            ),
            {
                "price_tick": "0.01",
                "quantity_step": "0.0001",
                "min_quantity": "0.0001",
                "min_notional": "5",
            },
        )

    def test_shared_binance_rules_projection_rejects_duplicate_filters(self):
        payload = {
            "symbols": [{
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {"filterType": "PRICE_FILTER", "tickSize": "0.02"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.0001",
                        "stepSize": "0.0001",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                ],
            }],
        }
        with self.assertRaisesRegex(ValueError, "duplicated"):
            binance_market_rules_projection(
                payload,
                base_asset="ETH",
                quote_asset="USDT",
                source_instrument="ETHUSDT",
            )

    def test_shared_rules_decimal_contract_is_bounded_fixed_point_text(self):
        payload = {
            "symbols": [{
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.0001",
                        "stepSize": "0.0001",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                ],
            }],
        }

        def projection(value):
            mutated = json.loads(json.dumps(payload))
            mutated["symbols"][0]["filters"][0]["tickSize"] = value
            return binance_market_rules_projection(
                mutated,
                base_asset="ETH",
                quote_asset="USDT",
                source_instrument="ETHUSDT",
            )

        exact_token = "1" * fetch_cex_depth.MAX_RULE_DECIMAL_TOKEN_BYTES
        self.assertEqual(projection(exact_token)["price_tick"], exact_token)
        exact_places = "0." + "0" * (
            fetch_cex_depth.MAX_RULE_DECIMAL_PLACES - 1
        ) + "1"
        self.assertEqual(projection(exact_places)["price_tick"], exact_places)
        for value in (
            "1" * (fetch_cex_depth.MAX_RULE_DECIMAL_TOKEN_BYTES + 1),
            "0." + "0" * fetch_cex_depth.MAX_RULE_DECIMAL_PLACES + "1",
            "1e-1000000",
            "-0",
            1,
        ):
            with self.subTest(value_type=type(value).__name__, width=len(str(value))):
                with self.assertRaises(ValueError):
                    projection(value)

    def test_shared_binance_rules_rejects_ambiguous_notional_filters(self):
        payload = {
            "symbols": [{
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.0001",
                        "stepSize": "0.0001",
                    },
                    {"filterType": "NOTIONAL", "minNotional": "5"},
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                ],
            }],
        }
        with self.assertRaisesRegex(ValueError, "notional.*ambiguous|exactly one"):
            binance_market_rules_projection(
                payload,
                base_asset="ETH",
                quote_asset="USDT",
                source_instrument="ETHUSDT",
            )

    def test_binance_collector_emits_authoritative_typed_rules_and_conversion(self):
        book_raw = (
            b'{"bids":[["99.99","2"]],"asks":[["100.01","3"]],'
            b'"lastUpdateId":123}'
        )
        rules_raw = json.dumps({
            "symbols": [{
                "symbol": "UNIUSDT",
                "status": "TRADING",
                "baseAsset": "UNI",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 4,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "tickSize": "0.01000000",
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.10000000",
                        "stepSize": "0.01000000",
                    },
                    {
                        "filterType": "MIN_NOTIONAL",
                        "minNotional": "5.00000000",
                    },
                ],
            }],
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        requests = []

        def fake_request(url):
            requests.append(url)
            if "/api/v3/depth" in url:
                return json.loads(book_raw), book_raw
            if "/api/v3/exchangeInfo" in url:
                return json.loads(rules_raw), rules_raw
            raise AssertionError("unexpected CEX source request: {}".format(url))

        typed = []
        with tempfile.TemporaryDirectory() as directory_name, patch(
            "scripts.fetch_cex_depth.utc_now_text",
            return_value="2026-08-01T12:00:00+00:00",
        ):
            row, _execution = collect_cex_market_observation(
                market(),
                snapshot_id="typed-binance",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertIn(row["status"], {"observed", "partial"})
        self.assertEqual(
            requests,
            [
                "https://data-api.binance.vision/api/v3/depth?symbol=UNIUSDT&limit=100",
                "https://api.binance.com/api/v3/exchangeInfo?symbol=UNIUSDT",
            ],
        )
        self.assertEqual(
            [member["role"] for member in typed],
            ["cex_market_rules", "quote_usd_conversion"],
        )
        rules = json.loads(typed[0]["payload"].decode("utf-8"))
        self.assertEqual(rules, {
            "base_asset": "UNI",
            "base_increment": "0.01",
            "base_unit_decimals": 8,
            "market_id": "cex:binance:UNI/USDT",
            "min_base_quantity": "0.1",
            "min_quote_notional": "5",
            "observed_at": "2026-08-01T12:00:00+00:00",
            "quote_asset": "USDT",
            "quote_increment": "0.0001",
            "quote_unit_decimals": 4,
            "schema": "route_market_rules_source/v1",
            "valid_until": "2026-08-01T12:01:00+00:00",
        })
        conversion = json.loads(typed[1]["payload"].decode("utf-8"))
        self.assertEqual(conversion["quote_asset"], "USDT")
        self.assertEqual(conversion["usd_per_quote"], "1")
        self.assertEqual(conversion["observed_at"], rules["observed_at"])
        self.assertEqual(conversion["valid_until"], rules["valid_until"])
        self.assertNotEqual(
            conversion["valid_until"], conversion["observed_at"]
        )

    def test_bybit_typed_rules_use_instrument_precision_and_lot_filters(self):
        book_raw = json.dumps({
            "retCode": 0,
            "result": {
                "s": "UNIUSDT",
                "b": [["99.99", "2"]],
                "a": [["100.01", "3"]],
            },
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        rules_raw = json.dumps({
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": [{
                    "symbol": "UNIUSDT",
                    "status": "Trading",
                    "baseCoin": "UNI",
                    "quoteCoin": "USDT",
                    "lotSizeFilter": {
                        "basePrecision": "0.001",
                        "quotePrecision": "0.0001",
                        "minOrderQty": "0.01",
                        "minOrderAmt": "1",
                    },
                    "priceFilter": {"tickSize": "0.01"},
                }],
            },
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def fake_request(url):
            if "/v5/market/orderbook" in url:
                return json.loads(book_raw), book_raw
            if "/v5/market/instruments-info" in url:
                return json.loads(rules_raw), rules_raw
            raise AssertionError("unexpected CEX source request: {}".format(url))

        typed = []
        with tempfile.TemporaryDirectory() as directory_name, patch(
            "scripts.fetch_cex_depth.utc_now_text",
            return_value="2026-08-01T12:00:00+00:00",
        ):
            row, _execution = collect_cex_market_observation(
                market(exchange="bybit"),
                snapshot_id="typed-bybit",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertIn(row["status"], {"observed", "partial"})
        rules = json.loads(typed[0]["payload"].decode("utf-8"))
        self.assertEqual(rules["base_unit_decimals"], 3)
        self.assertEqual(rules["quote_unit_decimals"], 4)
        self.assertEqual(rules["base_increment"], "0.001")
        self.assertEqual(rules["quote_increment"], "0.0001")
        self.assertEqual(rules["min_base_quantity"], "0")
        self.assertEqual(rules["min_quote_notional"], "1")

    def test_bybit_deprecated_min_order_quantity_is_not_required(self):
        book_raw = (
            b'{"retCode":0,"result":{"s":"UNIUSDT",'
            b'"b":[["99.99","2"]],"a":[["100.01","3"]]}}'
        )
        rules = {
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": [{
                    "symbol": "UNIUSDT",
                    "status": "Trading",
                    "baseCoin": "UNI",
                    "quoteCoin": "USDT",
                    "lotSizeFilter": {
                        "basePrecision": "0.001",
                        "quotePrecision": "0.0001",
                        "minOrderAmt": "1",
                    },
                    "priceFilter": {"tickSize": "0.01"},
                }],
            },
        }
        rules_raw = json.dumps(
            rules, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")

        def fake_request(url):
            raw = book_raw if "/v5/market/orderbook" in url else rules_raw
            return json.loads(raw), raw

        typed = []
        with tempfile.TemporaryDirectory() as directory_name:
            row, _execution = collect_cex_market_observation(
                market(exchange="bybit"),
                snapshot_id="typed-bybit-no-deprecated-minimum",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertIn(row["status"], {"observed", "partial"})
        rules_payload = json.loads(typed[0]["payload"].decode("utf-8"))
        self.assertEqual(rules_payload["min_base_quantity"], "0")
        self.assertEqual(rules_payload["min_quote_notional"], "1")

    def test_bybit_requires_positive_min_order_amount(self):
        book_raw = (
            b'{"retCode":0,"result":{"s":"UNIUSDT",'
            b'"b":[["99.99","2"]],"a":[["100.01","3"]]}}'
        )
        base_record = {
            "symbol": "UNIUSDT",
            "status": "Trading",
            "baseCoin": "UNI",
            "quoteCoin": "USDT",
            "lotSizeFilter": {
                "basePrecision": "0.001",
                "quotePrecision": "0.0001",
            },
            "priceFilter": {"tickSize": "0.01"},
        }
        for name, min_order_amount in (("missing", None), ("zero", "0")):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory_name:
                record = json.loads(json.dumps(base_record))
                if min_order_amount is not None:
                    record["lotSizeFilter"]["minOrderAmt"] = min_order_amount
                rules_raw = json.dumps({
                    "retCode": 0,
                    "result": {"category": "spot", "list": [record]},
                }, sort_keys=True, separators=(",", ":")).encode("utf-8")

                def fake_request(url):
                    raw = book_raw if "/v5/market/orderbook" in url else rules_raw
                    return json.loads(raw), raw

                typed = []
                row, _execution = collect_cex_market_observation(
                    market(exchange="bybit"),
                    snapshot_id="typed-bybit-min-amount-" + name,
                    raw_path=Path(directory_name) / "book.json",
                    request=fake_request,
                    typed_source_payload_sink=typed.append,
                )

                self.assertIn(row["status"], {"observed", "partial"})
                self.assertEqual(typed, [])

    def test_suspended_instrument_never_emits_typed_rules_or_conversion(self):
        book_raw = b'{"bids":[["99","2"]],"asks":[["101","3"]]}'
        rules_raw = json.dumps({
            "symbols": [{
                "symbol": "UNIUSDT",
                "status": "BREAK",
                "baseAsset": "UNI",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 4,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.1",
                        "stepSize": "0.01",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                ],
            }],
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def fake_request(url):
            if "/api/v3/depth" in url:
                return json.loads(book_raw), book_raw
            return json.loads(rules_raw), rules_raw

        typed = []
        with tempfile.TemporaryDirectory() as directory_name:
            row, _execution = collect_cex_market_observation(
                market(),
                snapshot_id="typed-suspended",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertIn(row["status"], {"observed", "partial"})
        self.assertEqual(typed, [])

    def test_bybit_nontrading_instrument_never_emits_typed_payloads(self):
        book_raw = (
            b'{"retCode":0,"result":{"s":"UNIUSDT",'
            b'"b":[["99","2"]],"a":[["101","3"]]}}'
        )
        rules_raw = json.dumps({
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": [{
                    "symbol": "UNIUSDT",
                    "status": "PreLaunch",
                    "baseCoin": "UNI",
                    "quoteCoin": "USDT",
                    "lotSizeFilter": {
                        "basePrecision": "0.001",
                        "quotePrecision": "0.0001",
                        "minOrderQty": "0.01",
                        "minOrderAmt": "1",
                    },
                    "priceFilter": {"tickSize": "0.01"},
                }],
            },
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def fake_request(url):
            raw = book_raw if "/v5/market/orderbook" in url else rules_raw
            return json.loads(raw), raw

        typed = []
        with tempfile.TemporaryDirectory() as directory_name:
            row, _execution = collect_cex_market_observation(
                market(exchange="bybit"),
                snapshot_id="typed-bybit-nontrading",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertIn(row["status"], {"observed", "partial"})
        self.assertEqual(typed, [])

    def test_rules_observed_time_is_sampled_after_rules_response(self):
        book_raw = b'{"bids":[["99","2"]],"asks":[["101","3"]]}'
        rules_raw = json.dumps({
            "symbols": [{
                "symbol": "UNIUSDT",
                "status": "TRADING",
                "baseAsset": "UNI",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 4,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.1",
                        "stepSize": "0.01",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                ],
            }],
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def fake_request(url):
            if "/api/v3/depth" in url:
                return json.loads(book_raw), book_raw
            return json.loads(rules_raw), rules_raw

        clock = iter([
            "2026-08-01T12:00:00+00:00",
            "2026-08-01T12:00:01+00:00",
            "2026-08-01T12:00:02+00:00",
        ])
        typed = []
        with tempfile.TemporaryDirectory() as directory_name, patch(
            "scripts.fetch_cex_depth.utc_now_text", side_effect=lambda: next(clock)
        ):
            collect_cex_market_observation(
                market(),
                snapshot_id="typed-times",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        rules = json.loads(typed[0]["payload"].decode("utf-8"))
        conversion = json.loads(typed[1]["payload"].decode("utf-8"))
        self.assertEqual(rules["observed_at"], "2026-08-01T12:00:02+00:00")
        self.assertEqual(
            conversion["observed_at"], "2026-08-01T12:00:01+00:00"
        )
        self.assertLess(conversion["observed_at"], rules["observed_at"])
        cohort_now = "2026-08-01T12:00:30+00:00"
        self.assertLessEqual(rules["observed_at"], cohort_now)
        self.assertLess(cohort_now, rules["valid_until"])
        self.assertLessEqual(conversion["observed_at"], cohort_now)
        self.assertLess(cohort_now, conversion["valid_until"])

    def test_rules_that_cannot_construct_market_rules_emit_nothing(self):
        book_raw = b'{"bids":[["99","2"]],"asks":[["101","3"]]}'
        rules_raw = json.dumps({
            "symbols": [{
                "symbol": "UNIUSDT",
                "status": "TRADING",
                "baseAsset": "UNI",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 2,
                "quoteAssetPrecision": 4,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.1",
                        "stepSize": "0.001",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                ],
            }],
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def fake_request(url):
            raw = book_raw if "/api/v3/depth" in url else rules_raw
            return json.loads(raw), raw

        typed = []
        with tempfile.TemporaryDirectory() as directory_name:
            row, _execution = collect_cex_market_observation(
                market(),
                snapshot_id="typed-invalid-rule-lattice",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertIn(row["status"], {"observed", "partial"})
        self.assertEqual(typed, [])

    def test_binance_minima_allow_plain_zero_but_reject_signed_zero(self):
        book_raw = b'{"bids":[["99","2"]],"asks":[["101","3"]]}'
        base_rules = {
            "symbols": [{
                "symbol": "UNIUSDT",
                "status": "TRADING",
                "baseAsset": "UNI",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 4,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0",
                        "stepSize": "0.01",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "0"},
                ],
            }],
        }
        cases = (
            ("plain-zero", "0", "0", True),
            ("signed-zero-base", "-0", "0", False),
            ("signed-zero-notional", "0", "-0", False),
        )
        for name, min_quantity, min_notional, should_emit in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory_name:
                rules = json.loads(json.dumps(base_rules))
                rules["symbols"][0]["filters"][1]["minQty"] = min_quantity
                rules["symbols"][0]["filters"][2]["minNotional"] = min_notional
                rules_raw = json.dumps(
                    rules, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")

                def fake_request(url):
                    raw = book_raw if "/api/v3/depth" in url else rules_raw
                    return json.loads(raw), raw

                typed = []
                row, _execution = collect_cex_market_observation(
                    market(),
                    snapshot_id="typed-binance-" + name,
                    raw_path=Path(directory_name) / "book.json",
                    request=fake_request,
                    typed_source_payload_sink=typed.append,
                )

                self.assertIn(row["status"], {"observed", "partial"})
                if should_emit:
                    self.assertEqual(
                        [member["role"] for member in typed],
                        ["cex_market_rules", "quote_usd_conversion"],
                    )
                    payload = json.loads(typed[0]["payload"].decode("utf-8"))
                    self.assertEqual(payload["min_base_quantity"], "0")
                    self.assertEqual(payload["min_quote_notional"], "0")
                else:
                    self.assertEqual(typed, [])

    def test_conversion_uses_the_book_source_time_when_venue_supplies_one(self):
        book_raw = json.dumps({
            "retCode": 0,
            "result": {
                "s": "UNIUSDT",
                "b": [["99", "2"]],
                "a": [["101", "3"]],
                "ts": 1785585600000,
            },
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        rules_raw = json.dumps({
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": [{
                    "symbol": "UNIUSDT",
                    "status": "Trading",
                    "baseCoin": "UNI",
                    "quoteCoin": "USDT",
                    "lotSizeFilter": {
                        "basePrecision": "0.001",
                        "quotePrecision": "0.0001",
                        "minOrderQty": "0.01",
                        "minOrderAmt": "1",
                    },
                    "priceFilter": {"tickSize": "0.01"},
                }],
            },
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def fake_request(url):
            raw = book_raw if "/v5/market/orderbook" in url else rules_raw
            return json.loads(raw), raw

        typed = []
        with tempfile.TemporaryDirectory() as directory_name, patch(
            "scripts.fetch_cex_depth.utc_now_text",
            side_effect=[
                "2026-08-01T12:00:00+00:00",
                "2026-08-01T12:00:01+00:00",
                "2026-08-01T12:00:02+00:00",
            ],
        ):
            collect_cex_market_observation(
                market(exchange="bybit"),
                snapshot_id="typed-book-source-time",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        conversion = json.loads(typed[1]["payload"].decode("utf-8"))
        self.assertEqual(
            conversion["observed_at"], "2026-08-01T12:00:00+00:00"
        )

    def test_rules_http_body_is_read_with_an_explicit_bound(self):
        from scripts.fetch_cex_depth import (
            MAX_CEX_TYPED_RULE_RESPONSE_BYTES,
            request_json,
        )

        class BoundedResponse:
            def __init__(self):
                self.read_sizes = []

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, size=-1):
                self.read_sizes.append(size)
                return b"{}"

        response = BoundedResponse()
        with patch(
            "scripts.fetch_cex_depth.open_public_json_request",
            return_value=response,
        ):
            payload, raw = request_json(
                "https://api.binance.com/api/v3/exchangeInfo?symbol=UNIUSDT",
                max_bytes=MAX_CEX_TYPED_RULE_RESPONSE_BYTES,
                max_retries=1,
            )

        self.assertEqual((payload, raw), ({}, b"{}"))
        self.assertEqual(
            response.read_sizes, [MAX_CEX_TYPED_RULE_RESPONSE_BYTES + 1]
        )

    def test_rules_http_body_one_byte_over_bound_is_rejected(self):
        from scripts.fetch_cex_depth import (
            MAX_CEX_TYPED_RULE_RESPONSE_BYTES,
            request_json,
        )

        class OversizedResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, size=-1):
                return b"{" + b" " * (size - 1)

        with patch(
            "scripts.fetch_cex_depth.open_public_json_request",
            return_value=OversizedResponse(),
        ):
            with self.assertRaisesRegex(ValueError, "response exceeds"):
                request_json(
                    "https://api.binance.com/api/v3/exchangeInfo?symbol=UNIUSDT",
                    max_bytes=MAX_CEX_TYPED_RULE_RESPONSE_BYTES,
                    max_retries=1,
                )

    def test_unsupported_venue_does_not_emit_typed_payloads_or_fetch_rules(self):
        book_raw = b'{"code":"0","data":[{"bids":[["99","2"]],"asks":[["101","3"]]}]}'
        requests = []

        def fake_request(url):
            requests.append(url)
            return json.loads(book_raw), book_raw

        typed = []
        with tempfile.TemporaryDirectory() as directory_name:
            row, _execution = collect_cex_market_observation(
                market(exchange="okx"),
                snapshot_id="typed-unsupported",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertEqual(row["status"], "observed")
        self.assertEqual(typed, [])
        self.assertEqual(len(requests), 1)

    def test_malformed_authoritative_rules_fail_without_leaking_partial_typed_payloads(self):
        book_raw = b'{"bids":[["99","2"]],"asks":[["101","3"]]}'
        malformed_rules_raw = b'{"symbols":[{"symbol":"UNIUSDT","filters":[]}]}'

        def fake_request(url):
            if "/api/v3/depth" in url:
                return json.loads(book_raw), book_raw
            if "/api/v3/exchangeInfo" in url:
                return json.loads(malformed_rules_raw), malformed_rules_raw
            raise AssertionError("unexpected CEX source request: {}".format(url))

        typed = []
        with tempfile.TemporaryDirectory() as directory_name:
            row, _execution = collect_cex_market_observation(
                market(),
                snapshot_id="typed-invalid-rules",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertIn(row["status"], {"observed", "partial"})
        self.assertEqual(typed, [])

    def test_typed_rules_request_honors_the_shared_collection_deadline(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )

        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

        clock = Clock()
        deadline = CollectionDeadline.for_duration(1, clock=clock.monotonic)
        book_raw = b'{"bids":[["99","2"]],"asks":[["101","3"]]}'

        def expiring_request(url, *, deadline):
            if "/api/v3/depth" in url:
                clock.now = 0.5
                return json.loads(book_raw), book_raw
            clock.now = 2.0
            deadline.require_remaining()

        retained_raw = None
        with tempfile.TemporaryDirectory() as directory_name:
            raw_path = Path(directory_name) / "book.json"
            with self.assertRaisesRegex(
                CollectionDeadlineExceeded,
                "^collection deadline exceeded$",
            ):
                collect_cex_market_observation(
                    market(),
                    snapshot_id="typed-expired-rules",
                    raw_path=raw_path,
                    request=expiring_request,
                    deadline=deadline,
                    typed_source_payload_sink=lambda _value: None,
                )
            retained_raw = raw_path.read_bytes()

        self.assertEqual(retained_raw, book_raw)

    def test_bybit_returned_instrument_cannot_retarget_typed_rules(self):
        book_raw = json.dumps({
            "retCode": 0,
            "result": {
                "s": "AAVEUSDT",
                "b": [["99", "2"]],
                "a": [["101", "3"]],
            },
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        requests = []

        def fake_request(url):
            requests.append(url)
            return json.loads(book_raw), book_raw

        typed = []
        with tempfile.TemporaryDirectory() as directory_name:
            row, _execution = collect_cex_market_observation(
                market(exchange="bybit"),
                snapshot_id="typed-wrong-instrument",
                raw_path=Path(directory_name) / "book.json",
                request=fake_request,
                typed_source_payload_sink=typed.append,
            )

        self.assertEqual(row["status"], "failed")
        self.assertEqual(typed, [])
        self.assertEqual(len(requests), 1)

    def test_coinbase_nanosecond_timestamp_is_canonicalized(self):
        self.assertEqual(
            timestamp_text("2026-07-31T23:05:47.660676312Z"),
            "2026-07-31T23:05:47.660676+00:00",
        )

    def test_invalid_nonempty_source_timestamp_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "source timestamp"):
            timestamp_text("not-a-timestamp")

    def test_filtered_collection_cannot_replace_published_inventory(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            ensure_full_publish_scope(True, {"UNI"}, set())
        ensure_full_publish_scope(False, {"UNI"}, {"binance"})

    def test_exact_refresh_merges_one_market_without_collecting_other_markets(self):
        markets = [
            market(exchange="binance"),
            market(exchange="okx"),
        ]
        baseline_depth = [
            observed_row(
                item,
                complete_book(),
                snapshot_id="baseline-1",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
            for item in markets
        ]
        baseline_execution = [
            scenario
            for item in markets
            for scenario in execution_rows_for_book(
                item,
                complete_book(),
                snapshot_id="baseline-1",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
        ]
        fresh_book = complete_book()
        fresh_book["bids"] = [
            (Decimal("100"), Decimal("10")),
            (Decimal("98"), Decimal("10")),
        ]
        fresh_book["asks"] = [
            (Decimal("101"), Decimal("10")),
            (Decimal("103"), Decimal("10")),
        ]
        fresh_book["raw"] = b'{"book":"fresh"}'
        candidate_depth = [
            observed_row(
                markets[0],
                fresh_book,
                snapshot_id="candidate-2",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
            )
        ]
        candidate_execution = execution_rows_for_book(
            markets[0],
            fresh_book,
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
        )

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            publish_snapshot(
                baseline_depth,
                output_dir=root / "processed",
                publish_dir=published,
            )
            publish_execution_snapshot(
                baseline_execution,
                expected_market_ids={
                    "cex:binance:UNI/USDT",
                    "cex:okx:UNI/USDT",
                },
                output_dir=root / "processed",
                publish_dir=published,
            )

            merged_depth, merged_execution = merge_exact_publication_bundle(
                candidate_depth,
                candidate_execution,
                target_market_id="cex:binance:UNI/USDT",
                publish_dir=published,
            )
            mismatched_execution = [
                {
                    **row,
                    "snapshot_id": "different-generation",
                    "source_snapshot_id": "different-generation",
                }
                for row in merged_execution
            ]
            with self.assertRaisesRegex(ValueError, "same source"):
                preflight_publication_bundle(
                    merged_depth,
                    mismatched_execution,
                    published,
                    target_market_id="cex:binance:UNI/USDT",
                )
            reports = preflight_publication_bundle(
                merged_depth,
                merged_execution,
                published,
                target_market_id="cex:binance:UNI/USDT",
            )
            with self.assertRaisesRegex(ValueError, "history append"):
                publish_exact_publication_bundle(
                    merged_depth,
                    merged_execution,
                    target_market_id="cex:binance:UNI/USDT",
                    history_rows_to_append=[
                        {**candidate_depth[0], "best_bid": "999"}
                    ],
                    output_dir=root / "processed",
                    publish_dir=published,
                    preflight_reports=reports,
                )
            protected = [
                published / HISTORY_FILENAME,
                published / LATEST_FILENAME,
                published / CURRENT_FILENAME,
                published / EXECUTION_LATEST_FILENAME,
            ]
            originals = {path: path.read_bytes() for path in protected}
            from scripts import atomic_publication

            real_replace = atomic_publication.os.replace
            for fail_at in range(1, len(protected) + 1):
                calls = {"count": 0}

                def fail_once(source, destination):
                    calls["count"] += 1
                    if calls["count"] == fail_at:
                        raise OSError("injected publication failure")
                    return real_replace(source, destination)

                with self.subTest(fail_at=fail_at), patch(
                    "scripts.atomic_publication.os.replace",
                    side_effect=fail_once,
                ):
                    with self.assertRaises(OSError):
                        publish_exact_publication_bundle(
                            merged_depth,
                            merged_execution,
                            target_market_id="cex:binance:UNI/USDT",
                            history_rows_to_append=candidate_depth,
                            output_dir=root / "processed",
                            publish_dir=published,
                            preflight_reports=reports,
                        )
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    originals,
                )

            publish_exact_publication_bundle(
                merged_depth,
                merged_execution,
                target_market_id="cex:binance:UNI/USDT",
                history_rows_to_append=candidate_depth,
                output_dir=root / "processed",
                publish_dir=published,
                preflight_reports=reports,
            )
            with (published / HISTORY_FILENAME).open(
                newline="",
                encoding="utf-8",
            ) as handle:
                history = list(csv.DictReader(handle))

        self.assertEqual(len(merged_depth), 2)
        self.assertEqual(len(merged_execution), 20)
        self.assertEqual(
            {row["snapshot_id"] for row in merged_depth},
            {"candidate-2"},
        )
        by_exchange = {row["exchange"]: row for row in merged_depth}
        self.assertEqual(by_exchange["binance"]["best_bid"], "100")
        self.assertEqual(
            by_exchange["okx"]["best_bid"],
            baseline_depth[1]["best_bid"],
        )
        self.assertEqual(
            {row["source_snapshot_id"] for row in merged_execution},
            {"candidate-2"},
        )
        self.assertEqual(len(history), 3)

    def test_exact_publication_bundle_rejects_resolved_private_public_path_overlap_before_write(self):
        markets = [
            market(exchange="binance"),
            market(exchange="okx"),
        ]
        baseline_depth = [
            observed_row(
                item,
                complete_book(),
                snapshot_id="baseline-1",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
            for item in markets
        ]
        baseline_execution = [
            scenario
            for item in markets
            for scenario in execution_rows_for_book(
                item,
                complete_book(),
                snapshot_id="baseline-1",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
        ]
        candidate_depth = [
            observed_row(
                markets[0],
                complete_book(),
                snapshot_id="candidate-2",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
            )
        ]
        candidate_execution = execution_rows_for_book(
            markets[0],
            complete_book(),
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
        )

        for alias in ("same-directory", "dotdot-alias"):
            with self.subTest(alias=alias), tempfile.TemporaryDirectory() as directory_name:
                root = Path(directory_name)
                published = root / "local"
                publish_snapshot(
                    baseline_depth,
                    output_dir=root / "processed",
                    publish_dir=published,
                )
                publish_execution_snapshot(
                    baseline_execution,
                    expected_market_ids={
                        "cex:binance:UNI/USDT",
                        "cex:okx:UNI/USDT",
                    },
                    output_dir=root / "processed",
                    publish_dir=published,
                )
                merged_depth, merged_execution = (
                    merge_exact_publication_bundle(
                        candidate_depth,
                        candidate_execution,
                        target_market_id="cex:binance:UNI/USDT",
                        publish_dir=published,
                    )
                )
                reports = preflight_publication_bundle(
                    merged_depth,
                    merged_execution,
                    published,
                    target_market_id="cex:binance:UNI/USDT",
                )
                protected = [
                    published / HISTORY_FILENAME,
                    published / LATEST_FILENAME,
                    published / CURRENT_FILENAME,
                    published / EXECUTION_LATEST_FILENAME,
                ]
                originals = {path: path.read_bytes() for path in protected}
                output_dir = (
                    published
                    if alias == "same-directory"
                    else published / ".." / published.name
                )

                with self.assertRaisesRegex(ValueError, "overlap"):
                    publish_exact_publication_bundle(
                        merged_depth,
                        merged_execution,
                        target_market_id="cex:binance:UNI/USDT",
                        history_rows_to_append=candidate_depth,
                        output_dir=output_dir,
                        publish_dir=published,
                        preflight_reports=reports,
                    )
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    originals,
                )

    def test_full_publication_bundle_restores_every_public_destination_on_each_replace_failure(self):
        baseline_depth = [
            observed_row(
                market(),
                complete_book(),
                snapshot_id="baseline-1",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
        ]
        baseline_execution = execution_rows_for_book(
            market(),
            complete_book(),
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        candidate_depth = [
            observed_row(
                market(),
                complete_book(),
                snapshot_id="candidate-2",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
            )
        ]
        candidate_execution = execution_rows_for_book(
            market(),
            complete_book(),
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
        )

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            publish_snapshot(
                baseline_depth,
                output_dir=root / "processed",
                publish_dir=published,
            )
            publish_execution_snapshot(
                baseline_execution,
                expected_market_ids={"cex:binance:UNI/USDT"},
                output_dir=root / "processed",
                publish_dir=published,
            )
            reports = preflight_publication_bundle(
                candidate_depth,
                candidate_execution,
                published,
            )
            protected = [
                published / HISTORY_FILENAME,
                published / LATEST_FILENAME,
                published / CURRENT_FILENAME,
                published / EXECUTION_LATEST_FILENAME,
            ]
            originals = {path: path.read_bytes() for path in protected}
            from scripts import atomic_publication

            real_replace = atomic_publication.os.replace
            for fail_at, failed_path in enumerate(protected, start=1):
                public_calls = {"count": 0}
                failed_destination = {"path": None}

                def fail_public_replace(source, destination):
                    if Path(destination) in protected:
                        public_calls["count"] += 1
                        if public_calls["count"] == fail_at:
                            failed_destination["path"] = Path(destination)
                            raise OSError("injected full publication failure")
                    return real_replace(source, destination)

                with self.subTest(
                    fail_at=fail_at,
                    destination=failed_path.name,
                ), patch(
                    "scripts.atomic_publication.os.replace",
                    side_effect=fail_public_replace,
                ):
                    with self.assertRaisesRegex(
                        OSError,
                        "injected full publication failure",
                    ):
                        publish_full_publication_bundle(
                            candidate_depth,
                            candidate_execution,
                            output_dir=root / "processed",
                            publish_dir=published,
                            preflight_reports=reports,
                        )
                self.assertEqual(failed_destination["path"], failed_path)
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    originals,
                )

    def test_full_publication_bundle_rejects_resolved_private_public_path_overlap_before_write(self):
        baseline_depth = [
            observed_row(
                market(),
                complete_book(),
                snapshot_id="baseline-1",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
        ]
        baseline_execution = execution_rows_for_book(
            market(),
            complete_book(),
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        candidate_depth = [
            observed_row(
                market(),
                complete_book(),
                snapshot_id="candidate-2",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
            )
        ]
        candidate_execution = execution_rows_for_book(
            market(),
            complete_book(),
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
        )

        for alias in ("same-directory", "dotdot-alias"):
            with self.subTest(alias=alias), tempfile.TemporaryDirectory() as directory_name:
                root = Path(directory_name)
                published = root / "local"
                publish_snapshot(
                    baseline_depth,
                    output_dir=root / "processed",
                    publish_dir=published,
                )
                publish_execution_snapshot(
                    baseline_execution,
                    expected_market_ids={"cex:binance:UNI/USDT"},
                    output_dir=root / "processed",
                    publish_dir=published,
                )
                reports = preflight_publication_bundle(
                    candidate_depth,
                    candidate_execution,
                    published,
                )
                protected = [
                    published / HISTORY_FILENAME,
                    published / LATEST_FILENAME,
                    published / CURRENT_FILENAME,
                    published / EXECUTION_LATEST_FILENAME,
                ]
                originals = {path: path.read_bytes() for path in protected}
                output_dir = (
                    published
                    if alias == "same-directory"
                    else published / ".." / published.name
                )

                with self.assertRaisesRegex(ValueError, "overlap"):
                    publish_full_publication_bundle(
                        candidate_depth,
                        candidate_execution,
                        output_dir=output_dir,
                        publish_dir=published,
                        preflight_reports=reports,
                    )
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    originals,
                )

    def test_exact_preflight_accepts_one_observed_repair_below_full_coverage_floor(self):
        markets = [
            market(token="T0", symbol="T0/USDT"),
            market(token="T1", symbol="T1/USDT"),
        ]
        baseline_depth = [
            failure_row(
                item,
                snapshot_id="baseline-low",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
                error=ConnectionError("temporary venue failure"),
                reason_code="network",
            )
            for item in markets
        ]
        baseline_execution = [
            scenario
            for item in markets
            for scenario in failed_execution_rows(
                item,
                snapshot_id="baseline-low",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
                error=ConnectionError("temporary venue failure"),
                status_reason="network",
            )
        ]
        fresh_book = {**complete_book(), "source_instrument": "T0USDT"}
        candidate_depth = [
            observed_row(
                markets[0],
                fresh_book,
                snapshot_id="candidate-repair",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
            )
        ]
        candidate_execution = execution_rows_for_book(
            markets[0],
            fresh_book,
            snapshot_id="candidate-repair",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
        )

        with tempfile.TemporaryDirectory() as directory_name:
            published = Path(directory_name) / "local"
            write_snapshot_rows(
                published / LATEST_FILENAME,
                baseline_depth,
            )
            write_snapshot_rows(
                published / EXECUTION_LATEST_FILENAME,
                baseline_execution,
            )
            target_market_id = "cex:binance:T0/USDT"
            merged_depth, merged_execution = merge_exact_publication_bundle(
                candidate_depth,
                candidate_execution,
                target_market_id=target_market_id,
                publish_dir=published,
            )

            try:
                reports = preflight_publication_bundle(
                    merged_depth,
                    merged_execution,
                    published,
                    target_market_id=target_market_id,
                )
            except (CoverageRegressionError, TypeError) as error:
                self.fail(
                    "exact CEX repair was rejected by the full-publication "
                    f"coverage boundary: {error}"
                )

        self.assertTrue(reports["cex_depth"]["passed"])
        self.assertTrue(reports["cex_execution_cost"]["passed"])
        self.assertEqual(
            [row["status"] for row in merged_depth],
            ["observed", "failed"],
        )

    def test_exact_preflight_accepts_confirmed_terminal_on_all_failed_baseline(self):
        target = market(token="T0", symbol="T0/USDT")
        baseline_depth = [
            failure_row(
                target,
                snapshot_id="baseline-failed",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
                error=ConnectionError("temporary venue failure"),
                reason_code="network",
            )
        ]
        baseline_execution = failed_execution_rows(
            target,
            snapshot_id="baseline-failed",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            error=ConnectionError("temporary venue failure"),
            status_reason="network",
        )
        terminal_error = ValueError("empty order-book side")
        candidate_depth = [
            failure_row(
                target,
                snapshot_id="candidate-terminal",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
                error=terminal_error,
                reason_code="source_no_two_sided_book",
            )
        ]
        candidate_execution = failed_execution_rows(
            target,
            snapshot_id="candidate-terminal",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            error=terminal_error,
            status_reason="source_no_two_sided_book",
        )
        validate_snapshot(
            [target],
            candidate_depth,
            allow_terminal_only=True,
        )

        with tempfile.TemporaryDirectory() as directory_name:
            published = Path(directory_name) / "local"
            write_snapshot_rows(
                published / LATEST_FILENAME,
                baseline_depth,
            )
            write_snapshot_rows(
                published / EXECUTION_LATEST_FILENAME,
                baseline_execution,
            )
            target_market_id = "cex:binance:T0/USDT"
            try:
                merged_depth, merged_execution = merge_exact_publication_bundle(
                    candidate_depth,
                    candidate_execution,
                    target_market_id=target_market_id,
                    publish_dir=published,
                )
                reports = preflight_publication_bundle(
                    merged_depth,
                    merged_execution,
                    published,
                    target_market_id=target_market_id,
                )
            except (CoverageRegressionError, TypeError, ValueError) as error:
                self.fail(
                    "resolver-confirmed terminal CEX refresh was rejected: "
                    f"{error}"
                )

        self.assertTrue(reports["cex_depth"]["passed"])
        self.assertTrue(reports["cex_execution_cost"]["passed"])
        self.assertEqual(
            merged_depth[0]["reason_code"],
            "source_no_two_sided_book",
        )

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

    def execution_rows(self, book):
        return execution_rows_for_book(
            market(),
            book,
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )

    def test_execution_cost_known_answer_walks_partial_final_level(self):
        book = complete_book()
        book["bids"] = [
            (Decimal("99"), Decimal("6")),
            (Decimal("98"), Decimal("1000")),
        ]
        book["asks"] = [
            (Decimal("101"), Decimal("4")),
            (Decimal("102"), Decimal("1000")),
        ]
        rows = self.execution_rows(book)
        sell = next(
            row
            for row in rows
            if row["direction"] == "sell_token"
            and row["requested_notional_usd"] == "1000"
        )
        buy = next(
            row
            for row in rows
            if row["direction"] == "buy_token"
            and row["requested_notional_usd"] == "1000"
        )
        self.assertEqual(sell["target_token_quantity"], "10")
        self.assertEqual(sell["filled_token_quantity"], "10")
        self.assertEqual(sell["quote_amount"], "986")
        self.assertEqual(sell["filled_vwap_quote_per_token"], "98.6")
        self.assertEqual(sell["quoted_execution_cost_usd"], "14")
        self.assertEqual(sell["quoted_execution_cost_bps"], "140")
        self.assertEqual(buy["quote_amount"], "1016")
        self.assertEqual(buy["filled_vwap_quote_per_token"], "101.6")
        self.assertEqual(buy["quoted_execution_cost_usd"], "16")
        self.assertEqual(buy["quoted_execution_cost_bps"], "160")
        self.assertEqual(sell["fee_status"], "excluded_unknown_account_tier")
        self.assertEqual(sell["excluded_costs"], "taker_fee,lot_size,latency")
        validate_execution_snapshot(["cex:binance:UNI/USDT"], rows)

    def test_limited_execution_retains_partial_fill_but_withholds_cost(self):
        book = complete_book()
        book["bids"] = [(Decimal("99"), Decimal("2"))]
        book["asks"] = [(Decimal("101"), Decimal("2"))]
        rows = self.execution_rows(book)
        for direction, expected_quote in (
            ("sell_token", "198"),
            ("buy_token", "202"),
        ):
            row = next(
                item
                for item in rows
                if item["direction"] == direction
                and item["requested_notional_usd"] == "1000"
            )
            self.assertEqual(row["status"], "partial")
            self.assertEqual(row["status_reason"], "source_level_limit")
            self.assertEqual(row["filled_token_quantity"], "2")
            self.assertEqual(row["fill_ratio"], "0.2")
            self.assertEqual(row["quote_amount"], expected_quote)
            self.assertEqual(row["filled_vwap_quote_per_token"], "")
            self.assertEqual(row["quoted_execution_cost_usd"], "")
            self.assertEqual(row["quoted_execution_cost_bps"], "")
        validate_execution_snapshot(["cex:binance:UNI/USDT"], rows)

    def test_full_book_shortfall_is_explicitly_unfillable_not_a_cost(self):
        book = complete_book()
        book["bids"] = [(Decimal("99"), Decimal("2"))]
        book["asks"] = [(Decimal("101"), Decimal("2"))]
        book["full_book_reported"] = True
        row = next(
            item
            for item in self.execution_rows(book)
            if item["direction"] == "sell_token"
            and item["requested_notional_usd"] == "1000"
        )
        self.assertEqual(row["status"], "partial")
        self.assertEqual(
            row["status_reason"],
            "full_book_insufficient_liquidity",
        )
        self.assertEqual(row["quoted_execution_cost_usd"], "")

    def test_validation_rejects_nonmonotonic_source_level_walk(self):
        book = complete_book()
        book["bids"] = [
            (Decimal("99"), Decimal("10")),
            (Decimal("99.9"), Decimal("1000")),
        ]
        book["asks"] = [(Decimal("101"), Decimal("1000"))]
        rows = self.execution_rows(book)
        with self.assertRaisesRegex(
            ValueError,
            "cost decreases|VWAP improves",
        ):
            validate_execution_snapshot(["cex:binance:UNI/USDT"], rows)

    def test_failed_row_marks_every_execution_scenario_failed(self):
        rows = failed_execution_rows(
            market(),
            snapshot_id="depth-failed",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            error=RuntimeError("source unavailable"),
        )
        self.assertEqual(len(rows), 10)
        self.assertEqual({row["status"] for row in rows}, {"failed"})
        self.assertTrue(
            all(row["quoted_execution_cost_usd"] == "" for row in rows)
        )
        self.assertEqual({row["state_observed_at"] for row in rows}, {""})
        validate_execution_snapshot(["cex:binance:UNI/USDT"], rows)

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

    def test_catalog_loader_rejects_coinbase_rows_mislabeled_as_usdt(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            csv_path = directory / "cex.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["token_symbol", "exchange", "cex_symbol"],
                )
                writer.writeheader()
                writer.writerow(
                    market(
                        token="AAVE",
                        exchange="coinbase",
                        symbol="AAVE/USDT",
                    )
                )

            with self.assertRaisesRegex(ValueError, "source instrument identity"):
                load_cataloged_markets(
                    database_path=directory / "missing.sqlite3",
                    csv_path=csv_path,
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
            snapshot_id, rows, execution_rows = collect_depth_with_execution(
                [market()],
                raw_root=raw_root,
                request=fake_request,
                sleep_seconds=0,
            )
            manifest = json.loads((raw_root / snapshot_id / "manifest.json").read_text())

        self.assertEqual(rows[0]["status"], "observed")
        self.assertEqual(manifest["market_count"], 1)
        self.assertEqual(manifest["status_counts"]["observed"], 1)
        self.assertEqual(
            manifest["execution_notionals_usd"],
            [1000, 5000, 10000, 50000, 100000],
        )
        self.assertEqual(
            manifest["execution_cost_status_counts"],
            {
                "observed": 1,
                "partial": 9,
                "unsupported": 0,
                "failed": 0,
            },
        )
        self.assertEqual(manifest["execution_cost_row_count"], 10)
        validate_snapshot([market()], rows)
        validate_execution_snapshot(
            ["cex:binance:UNI/USDT"],
            execution_rows,
        )

    def test_execution_failure_preserves_observed_depth_and_raw_source(self):
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
            with patch(
                "scripts.fetch_cex_depth.execution_rows_for_book",
                side_effect=RuntimeError("calculation defect"),
            ):
                snapshot_id, rows, execution_rows = collect_depth_with_execution(
                    [market()],
                    raw_root=raw_root,
                    request=fake_request,
                    sleep_seconds=0,
                )
            raw_path = raw_root / snapshot_id / "001-binance-UNI.json"
            raw_bytes = raw_path.read_bytes()
            manifest = json.loads(
                (raw_root / snapshot_id / "manifest.json").read_text()
            )

        self.assertEqual(rows[0]["status"], "observed")
        self.assertEqual(
            rows[0]["raw_response_sha256"],
            hashlib.sha256(response).hexdigest(),
        )
        self.assertEqual(raw_bytes, response)
        self.assertEqual(len(execution_rows), 10)
        self.assertEqual({row["status"] for row in execution_rows}, {"failed"})
        self.assertEqual(
            {row["status_reason"] for row in execution_rows},
            {"execution_calculation_failed"},
        )
        self.assertEqual(
            {row["state_observed_at"] for row in execution_rows},
            {""},
        )
        self.assertTrue(
            all("calculation defect" in row["error"] for row in execution_rows)
        )
        self.assertEqual(manifest["status_counts"]["observed"], 1)
        self.assertEqual(
            manifest["execution_cost_status_counts"]["failed"],
            10,
        )

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
            snapshot_id, rows, execution_rows = collect_depth_with_execution(
                inventory,
                raw_root=raw_root,
                request=fake_request,
                sleep_seconds=0,
            )
            failed_raw = raw_root / snapshot_id / "002-binance-AAVE.json"
            failed = next(row for row in rows if row["token_symbol"] == "AAVE")
            failed_bytes = failed_raw.read_bytes()

        self.assertEqual(failed["status"], "failed")
        self.assertEqual(
            failed["reason_code"],
            "source_no_two_sided_book",
        )
        self.assertIn("/api/v3/depth", failed["source_endpoint"])
        self.assertEqual(failed_bytes, empty_response)
        self.assertEqual(
            failed["raw_response_sha256"],
            hashlib.sha256(empty_response).hexdigest(),
        )
        failed_execution = [
            row
            for row in execution_rows
            if row["market_id"] == "cex:binance:AAVE/USDT"
        ]
        self.assertEqual(len(failed_execution), 10)
        self.assertEqual(
            {row["status"] for row in failed_execution},
            {"failed"},
        )
        self.assertEqual(
            {row["status_reason"] for row in failed_execution},
            {"source_no_two_sided_book"},
        )

    def test_invalid_source_timestamp_is_published_as_parse_failure(self):
        invalid_response = json.dumps(
            {
                "bids": [["99", "2"]],
                "asks": [["101", "3"]],
                "sequence": 123,
                "time": "not-a-timestamp",
            }
        ).encode()
        valid_response = json.dumps(
            {
                "bids": [["99", "2"]],
                "asks": [["101", "3"]],
                "lastUpdateId": 123,
            }
        ).encode()

        def fake_request(url):
            raw = invalid_response if "coinbase.com" in url else valid_response
            return json.loads(raw), raw

        with tempfile.TemporaryDirectory() as directory_name:
            _, depth_rows, execution_rows = collect_depth_with_execution(
                [
                    market(),
                    market(
                        token="AAVE",
                        exchange="coinbase",
                        symbol="AAVE/USD",
                    ),
                ],
                raw_root=Path(directory_name),
                request=fake_request,
                sleep_seconds=0,
            )

        failed_depth = next(
            row for row in depth_rows if row["exchange"] == "coinbase"
        )
        self.assertEqual(failed_depth["status"], "failed")
        self.assertEqual(failed_depth["reason_code"], "parse")
        failed_execution = [
            row
            for row in execution_rows
            if row["market_id"] == "cex:coinbase:AAVE/USD"
        ]
        self.assertEqual(len(failed_execution), 10)
        self.assertEqual(
            {row["status"] for row in failed_execution},
            {"failed"},
        )
        self.assertEqual(
            {row["status_reason"] for row in failed_execution},
            {"parse"},
        )

    def test_depth_failure_reasons_separate_source_state_from_transport(self):
        self.assertEqual(
            depth_failure_reason_code(
                urllib.error.HTTPError(
                    "https://example.test/depth",
                    404,
                    "Not found",
                    {},
                    None,
                )
            ),
            "not_listed",
        )
        self.assertEqual(
            depth_failure_reason_code(
                urllib.error.HTTPError(
                    "https://example.test/depth",
                    429,
                    "Rate limit",
                    {},
                    None,
                )
            ),
            "rate_limit",
        )
        self.assertEqual(
            depth_failure_reason_code(
                urllib.error.URLError("temporary DNS failure")
            ),
            "network",
        )
        self.assertEqual(
            depth_failure_reason_code(
                ValueError("exchange returned a crossed or locked order book")
            ),
            "source_invalid_order_book",
        )

    def test_upbit_preserves_transport_reason_through_candidate_wrapping(self):
        def failed_request(_url):
            raise urllib.error.URLError("temporary DNS failure")

        with self.assertRaises(Exception) as context:
            upbit_book("LDO/USDT", failed_request)

        self.assertEqual(
            depth_failure_reason_code(context.exception),
            "network",
        )

    def test_upbit_depth_uses_only_the_exact_configured_instrument(self):
        usdt_book = [
            {
                "market": "USDT-TEST",
                "timestamp": 1785373200000,
                "orderbook_units": [
                    {
                        "bid_price": 999,
                        "bid_size": 2,
                        "ask_price": 1001,
                        "ask_size": 2,
                    }
                ],
            }
        ]

        requested = []

        def exact_request(url):
            requested.append(url)
            if "markets=USDT-TEST" in url:
                raw = json.dumps(usdt_book).encode()
                return usdt_book, raw
            raise AssertionError("Unexpected Upbit fallback request: {}".format(url))

        book = upbit_book("TEST/USDT", exact_request)

        self.assertEqual(len(requested), 1)
        self.assertIn("markets=USDT-TEST", requested[0])
        self.assertEqual(book["source_instrument"], "USDT-TEST")
        self.assertEqual(book["source_quote_asset"], "USDT")
        self.assertEqual(book["quote_to_usd"], Decimal(1))

    def test_upbit_fx_parse_failure_hashes_the_fx_response(self):
        krw_book = [
            {
                "market": "KRW-TEST",
                "timestamp": 1785373200000,
                "orderbook_units": [
                    {
                        "bid_price": 999,
                        "bid_size": 2,
                        "ask_price": 1001,
                        "ask_size": 2,
                    }
                ],
            }
        ]
        fx_raw = b'{"unexpected":"fx-response"}'

        def invalid_fx_request(url):
            if "markets=KRW-TEST" in url:
                raw = json.dumps(krw_book).encode()
                return krw_book, raw
            if "markets=KRW-USDT" in url:
                return {"unexpected": "fx-response"}, fx_raw
            raise urllib.error.HTTPError(url, 404, "Not found", {}, None)

        with self.assertRaises(Exception) as context:
            upbit_book("TEST/KRW", invalid_fx_request)

        error = context.exception
        self.assertIn("markets=KRW-USDT", error.endpoint)
        self.assertEqual(error.raw, fx_raw)
        self.assertNotIn("markets=KRW-TEST", error.endpoint)

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

    def test_validate_requires_canonical_utc_observed_at_for_every_market(self):
        row = observed_row(
            market(),
            complete_book(),
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        for observed_at in (
            "",
            "2026-07-27T00:00:01",
            "2026-07-27T00:00:01Z",
            "2026-07-27T08:00:01+08:00",
            " 2026-07-27T00:00:01+00:00",
        ):
            with self.subTest(observed_at=observed_at):
                with self.assertRaisesRegex(ValueError, "observed_at"):
                    validate_snapshot(
                        [market()],
                        [{**row, "observed_at": observed_at}],
                    )

    def test_exact_candidate_accepts_only_terminal_nonretryable_no_book(self):
        terminal = failure_row(
            market(),
            snapshot_id="candidate-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            error=ValueError("empty order-book side"),
            reason_code="source_no_two_sided_book",
        )
        with self.assertRaisesRegex(ValueError, "no observed"):
            validate_snapshot([market()], [terminal])
        validate_snapshot(
            [market()],
            [terminal],
            allow_terminal_only=True,
        )

        retryable = {
            **terminal,
            "reason_code": "network",
            "error": "URLError: temporary network failure",
        }
        with self.assertRaisesRegex(ValueError, "terminal non-retryable"):
            validate_snapshot(
                [market()],
                [retryable],
                allow_terminal_only=True,
            )

        partial = {
            **observed_row(
                market(),
                complete_book(),
                snapshot_id="candidate-2",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            ),
            "status": "partial",
            "reason_code": "source_level_limit",
        }
        with self.assertRaisesRegex(ValueError, "resolved exact candidate"):
            validate_snapshot(
                [market()],
                [partial],
                allow_terminal_only=True,
            )

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

    def test_execution_regression_preflight_preserves_depth_bundle(self):
        markets = [
            market(token=f"T{index}", symbol=f"T{index}/USDT")
            for index in range(20)
        ]
        baseline_depth = [
            observed_row(
                item,
                complete_book(),
                snapshot_id="healthy",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
            for item in markets
        ]
        baseline_execution = [
            row
            for item in markets
            for row in execution_rows_for_book(
                item,
                complete_book(),
                snapshot_id="healthy",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
        ]
        candidate_depth = [
            {
                **row,
                "snapshot_id": "degraded",
                "observed_at": "2026-07-27T01:00:01+00:00",
            }
            for row in baseline_depth
        ]
        candidate_execution = [
            row
            for item in markets[:18]
            for row in execution_rows_for_book(
                item,
                complete_book(),
                snapshot_id="degraded",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
            )
        ] + [
            row
            for item in markets[18:]
            for row in failed_execution_rows(
                item,
                snapshot_id="degraded",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
                error=RuntimeError("venue outage"),
            )
        ]

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            output = root / "processed"
            published = root / "local"
            publish_snapshot(
                baseline_depth,
                output_dir=output,
                publish_dir=published,
            )
            publish_execution_snapshot(
                baseline_execution,
                expected_market_ids=[
                    f"cex:binance:T{index}/USDT"
                    for index in range(20)
                ],
                output_dir=output,
                publish_dir=published,
            )
            protected_paths = [
                published / CURRENT_FILENAME,
                published / LATEST_FILENAME,
                published / HISTORY_FILENAME,
                published / EXECUTION_LATEST_FILENAME,
            ]
            before = {path: path.read_bytes() for path in protected_paths}

            with self.assertRaises(CoverageRegressionError) as raised:
                preflight_publication_bundle(
                    candidate_depth,
                    candidate_execution,
                    published,
                )

            self.assertEqual(
                raised.exception.report["bundle"],
                "cex_depth_execution",
            )
            self.assertTrue(
                raised.exception.report["publication_gates"]["cex_depth"][
                    "passed"
                ]
            )
            self.assertFalse(
                raised.exception.report["publication_gates"][
                    "cex_execution_cost"
                ]["passed"]
            )
            self.assertEqual(
                {path: path.read_bytes() for path in protected_paths},
                before,
            )

    def test_bundle_preflight_reports_are_reused_during_commit(self):
        depth_rows = [
            observed_row(
                market(),
                complete_book(),
                snapshot_id="depth-1",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
            )
        ]
        execution_rows = execution_rows_for_book(
            market(),
            complete_book(),
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            reports = preflight_publication_bundle(
                depth_rows,
                execution_rows,
                published,
            )
            with patch(
                "scripts.fetch_cex_depth.depth_publication_coverage_gate",
                side_effect=AssertionError("gate must not be re-evaluated"),
            ), patch(
                "scripts.fetch_cex_depth.execution_publication_coverage_gate",
                side_effect=AssertionError("gate must not be re-evaluated"),
            ):
                depth_result = publish_snapshot(
                    depth_rows,
                    output_dir=root / "processed",
                    publish_dir=published,
                    preflight_report=reports["cex_depth"],
                )
                execution_result = publish_execution_snapshot(
                    execution_rows,
                    expected_market_ids=["cex:binance:UNI/USDT"],
                    output_dir=root / "processed",
                    publish_dir=published,
                    preflight_report=reports["cex_execution_cost"],
                )

        self.assertEqual(
            depth_result["publication_gate"],
            reports["cex_depth"],
        )
        self.assertEqual(
            execution_result["publication_gate"],
            reports["cex_execution_cost"],
        )

    def test_preflight_report_rejects_wrong_rows_directory_and_stale_baseline(self):
        first = observed_row(
            market(),
            complete_book(),
            snapshot_id="first",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        candidate = {
            **first,
            "snapshot_id": "candidate",
            "observed_at": "2026-07-27T01:00:01+00:00",
        }
        execution_rows = execution_rows_for_book(
            market(),
            complete_book(),
            snapshot_id="candidate",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
        )
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            source_dir = root / "source"
            reports = preflight_publication_bundle(
                [candidate],
                execution_rows,
                source_dir,
            )

            with self.subTest("wrong candidate rows"):
                failed = {**candidate, "status": "failed"}
                with self.assertRaisesRegex(ValueError, "candidate rows"):
                    publish_snapshot(
                        [failed],
                        output_dir=root / "processed-wrong-rows",
                        publish_dir=source_dir,
                        preflight_report=reports["cex_depth"],
                    )
                self.assertFalse((source_dir / LATEST_FILENAME).exists())

            with self.subTest("wrong publication directory"):
                with self.assertRaisesRegex(ValueError, "baseline"):
                    publish_snapshot(
                        [candidate],
                        output_dir=root / "processed-wrong-dir",
                        publish_dir=root / "other",
                        preflight_report=reports["cex_depth"],
                    )
                self.assertFalse((root / "other" / LATEST_FILENAME).exists())

            publish_snapshot(
                [first],
                output_dir=root / "processed",
                publish_dir=source_dir,
            )
            current_reports = preflight_publication_bundle(
                [candidate],
                execution_rows,
                source_dir,
            )
            newer = {
                **first,
                "snapshot_id": "newer",
                "observed_at": "2026-07-27T02:00:01+00:00",
            }
            publish_snapshot(
                [newer],
                output_dir=root / "processed",
                publish_dir=source_dir,
            )

            with self.subTest("baseline changed after preflight"):
                before = (source_dir / LATEST_FILENAME).read_bytes()
                with self.assertRaisesRegex(ValueError, "baseline"):
                    publish_snapshot(
                        [candidate],
                        output_dir=root / "processed-stale",
                        publish_dir=source_dir,
                        preflight_report=current_reports["cex_depth"],
                    )
                self.assertEqual(
                    (source_dir / LATEST_FILENAME).read_bytes(),
                    before,
                )

    def test_execution_publish_replaces_latest_without_unbounded_history(self):
        book = complete_book()
        book["bids"] = [(Decimal("99"), Decimal("2000"))]
        book["asks"] = [(Decimal("101"), Decimal("2000"))]
        first = execution_rows_for_book(
            market(),
            book,
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        second = execution_rows_for_book(
            market(),
            book,
            snapshot_id="depth-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
        )
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            publish_execution_snapshot(
                first,
                expected_market_ids=["cex:binance:UNI/USDT"],
                output_dir=directory / "processed",
                publish_dir=directory / "local",
            )
            publish_execution_snapshot(
                second,
                expected_market_ids=["cex:binance:UNI/USDT"],
                output_dir=directory / "processed",
                publish_dir=directory / "local",
            )
            with (
                directory / "local" / EXECUTION_LATEST_FILENAME
            ).open(newline="", encoding="utf-8") as handle:
                latest = list(csv.DictReader(handle))
            history_exists = (
                directory / "local" / "cex_execution_cost_history.csv"
            ).exists()

        self.assertFalse(history_exists)
        self.assertEqual(len(latest), 10)
        self.assertEqual({row["snapshot_id"] for row in latest}, {"depth-2"})
        self.assertEqual(
            {
                (row["direction"], row["requested_notional_usd"])
                for row in latest
            },
            {
                (direction, str(int(notional)))
                for direction in EXECUTION_DIRECTIONS
                for notional in EXECUTION_NOTIONALS_USD
            },
        )

    def test_execution_publisher_rejects_incomplete_inventory_before_write(self):
        rows = execution_rows_for_book(
            market(),
            complete_book(),
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        with tempfile.TemporaryDirectory() as directory_name:
            output_dir = Path(directory_name)
            with self.assertRaisesRegex(ValueError, "coverage"):
                publish_execution_snapshot(
                    rows,
                    expected_market_ids=[
                        "cex:binance:UNI/USDT",
                        "cex:okx:AAVE/USDT",
                    ],
                    output_dir=output_dir,
                )
            current_exists = (
                output_dir / "cex_execution_cost_snapshot.csv"
            ).exists()

        self.assertFalse(current_exists)

    def test_one_market_primitive_matches_independent_golden_rows_and_raw_hash(self):
        from scripts.fetch_cex_depth import collect_cex_market_observation

        response = (
            b'{"bids": [["99.99", "2"], ["98.9", "5"]], '
            b'"asks": [["100.01", "3"], ["101.1", "7"]], '
            b'"lastUpdateId": 123}'
        )

        def fake_request(_url):
            return json.loads(response), response

        timestamp = "2026-08-01T12:00:00+00:00"
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            with patch(
                "scripts.fetch_cex_depth.utc_now_text",
                return_value=timestamp,
            ):
                one_depth, one_execution = collect_cex_market_observation(
                    market(),
                    snapshot_id="golden-cex-1",
                    raw_path=root / "one.json",
                    request=fake_request,
                )
            one_raw = (root / "one.json").read_bytes()

        expected_depth = {
            "snapshot_id": "golden-cex-1",
            "observed_at": timestamp,
            "request_started_at": timestamp,
            "response_received_at": timestamp,
            "token_symbol": "UNI",
            "exchange": "binance",
            "cex_symbol": "UNI/USDT",
            "source_instrument": "UNIUSDT",
            "base_asset": "UNI",
            "source_quote_asset": "USDT",
            "quote_to_usd": "1",
            "quote_conversion_method": "USDT=USD proxy",
            "quote_conversion_endpoint": "",
            "quote_conversion_response_sha256": "",
            "best_bid": "99.99",
            "best_ask": "100.01",
            "midpoint": "100.00",
            "spread_quote": "0.02",
            "spread_bps": "2.0000",
            "bid_levels_returned": "2",
            "ask_levels_returned": "2",
            "requested_level_limit": "100",
            "full_book_reported": "0",
            "bid_depth_10bps_usd": "199.98",
            "ask_depth_10bps_usd": "300.03",
            "total_depth_10bps_usd": "500.01",
            "depth_10bps_complete": "1",
            "bid_depth_25bps_usd": "199.98",
            "ask_depth_25bps_usd": "300.03",
            "total_depth_25bps_usd": "500.01",
            "depth_25bps_complete": "1",
            "bid_depth_50bps_usd": "199.98",
            "ask_depth_50bps_usd": "300.03",
            "total_depth_50bps_usd": "500.01",
            "depth_50bps_complete": "1",
            "bid_depth_100bps_usd": "199.98",
            "ask_depth_100bps_usd": "300.03",
            "total_depth_100bps_usd": "500.01",
            "depth_100bps_complete": "1",
            "depth_method": "midpoint_symmetric_quote_notional",
            "source": "binance public spot order-book API",
            "source_endpoint": (
                "https://data-api.binance.vision/api/v3/depth?"
                "symbol=UNIUSDT&limit=100"
            ),
            "source_sequence": "123",
            "raw_response_sha256": (
                "13b278c40ce93553f2c0edec997d4c37"
                "d3e9c1a665dd9a4bed9ebca6f3a6064a"
            ),
            "status": "observed",
            "reason_code": "observed",
            "error": "",
        }
        expected_common = {
            "snapshot_id": "golden-cex-1",
            "source_snapshot_id": "golden-cex-1",
            "contract_version": "1",
            "calculation_method": "normalized_order_book_level_walk",
            "observed_at": timestamp,
            "state_observed_at": timestamp,
            "request_started_at": timestamp,
            "response_received_at": timestamp,
            "market_id": "cex:binance:UNI/USDT",
            "market_type": "cex",
            "token_symbol": "UNI",
            "exchange": "binance",
            "cex_symbol": "UNI/USDT",
            "source_instrument": "UNIUSDT",
            "base_asset": "UNI",
            "source_quote_asset": "USDT",
            "chain": "",
            "dex": "",
            "pool_address": "",
            "block_number": "",
            "block_timestamp": "",
            "protocol_model": "",
            "target_token_address": "",
            "target_token_decimals": "",
            "quote_token_address": "",
            "quote_token_decimals": "",
            "notional_definition": (
                "target Token quantity valued at the snapshot pre-trade "
                "reference price"
            ),
            "reference_price_method": "order_book_midpoint",
            "reference_price_quote_per_token": "100",
            "quote_to_usd": "1",
            "reference_price_usd_per_token": "100",
            "usd_price_source_snapshot_id": "",
            "usd_price_observed_at": "",
            "levels_or_ticks_consumed": "2",
            "fee_status": "excluded_unknown_account_tier",
            "fee_rate_bps": "",
            "fee_amount_usd": "",
            "usd_conversion_status": "proxy_usdt_equals_usd",
            "excluded_costs": "taker_fee,lot_size,latency",
            "source": "binance public spot order-book API",
            "source_endpoint": (
                "https://data-api.binance.vision/api/v3/depth?"
                "symbol=UNIUSDT&limit=100"
            ),
            "source_sequence": "123",
            "raw_response_sha256": (
                "13b278c40ce93553f2c0edec997d4c37"
                "d3e9c1a665dd9a4bed9ebca6f3a6064a"
            ),
            "error": "",
        }
        expected_scenarios = [
            {
                "direction": "sell_token", "requested_notional_usd": "1000",
                "reference_notional_usd": "1000", "target_token_quantity": "10",
                "filled_token_quantity": "7", "fill_ratio": "0.7",
                "quote_amount": "694.48", "quote_amount_usd": "694.48",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "98.9",
                "status": "partial", "status_reason": "source_level_limit",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "1000",
                "reference_notional_usd": "1000", "target_token_quantity": "10",
                "filled_token_quantity": "10", "fill_ratio": "1",
                "quote_amount": "1007.73", "quote_amount_usd": "1007.73",
                "filled_vwap_quote_per_token": "100.773",
                "filled_vwap_usd_per_token": "100.773",
                "quoted_execution_cost_usd": "7.73",
                "quoted_execution_cost_bps": "77.3",
                "ending_marginal_price_quote_per_token": "101.1",
                "status": "observed", "status_reason": "target_filled",
            },
            {
                "direction": "sell_token", "requested_notional_usd": "5000",
                "reference_notional_usd": "5000", "target_token_quantity": "50",
                "filled_token_quantity": "7", "fill_ratio": "0.14",
                "quote_amount": "694.48", "quote_amount_usd": "694.48",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "98.9",
                "status": "partial", "status_reason": "source_level_limit",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "5000",
                "reference_notional_usd": "5000", "target_token_quantity": "50",
                "filled_token_quantity": "10", "fill_ratio": "0.2",
                "quote_amount": "1007.73", "quote_amount_usd": "1007.73",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "101.1",
                "status": "partial", "status_reason": "source_level_limit",
            },
            {
                "direction": "sell_token", "requested_notional_usd": "10000",
                "reference_notional_usd": "10000", "target_token_quantity": "100",
                "filled_token_quantity": "7", "fill_ratio": "0.07",
                "quote_amount": "694.48", "quote_amount_usd": "694.48",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "98.9",
                "status": "partial", "status_reason": "source_level_limit",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "10000",
                "reference_notional_usd": "10000", "target_token_quantity": "100",
                "filled_token_quantity": "10", "fill_ratio": "0.1",
                "quote_amount": "1007.73", "quote_amount_usd": "1007.73",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "101.1",
                "status": "partial", "status_reason": "source_level_limit",
            },
            {
                "direction": "sell_token", "requested_notional_usd": "50000",
                "reference_notional_usd": "50000", "target_token_quantity": "500",
                "filled_token_quantity": "7", "fill_ratio": "0.014",
                "quote_amount": "694.48", "quote_amount_usd": "694.48",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "98.9",
                "status": "partial", "status_reason": "source_level_limit",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "50000",
                "reference_notional_usd": "50000", "target_token_quantity": "500",
                "filled_token_quantity": "10", "fill_ratio": "0.02",
                "quote_amount": "1007.73", "quote_amount_usd": "1007.73",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "101.1",
                "status": "partial", "status_reason": "source_level_limit",
            },
            {
                "direction": "sell_token", "requested_notional_usd": "100000",
                "reference_notional_usd": "100000", "target_token_quantity": "1000",
                "filled_token_quantity": "7", "fill_ratio": "0.007",
                "quote_amount": "694.48", "quote_amount_usd": "694.48",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "98.9",
                "status": "partial", "status_reason": "source_level_limit",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "100000",
                "reference_notional_usd": "100000", "target_token_quantity": "1000",
                "filled_token_quantity": "10", "fill_ratio": "0.01",
                "quote_amount": "1007.73", "quote_amount_usd": "1007.73",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "ending_marginal_price_quote_per_token": "101.1",
                "status": "partial", "status_reason": "source_level_limit",
            },
        ]
        expected_execution = [
            {**expected_common, **scenario}
            for scenario in expected_scenarios
        ]

        self.assertEqual(one_depth, expected_depth)
        self.assertEqual(one_execution, expected_execution)
        self.assertEqual(one_raw, response)
        self.assertEqual(
            one_depth["raw_response_sha256"],
            "13b278c40ce93553f2c0edec997d4c37d3e9c1a665dd9a4bed9ebca6f3a6064a",
        )

    def test_one_market_primitive_propagates_expired_deadline_before_request(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_cex_depth import collect_cex_market_observation

        deadline = CollectionDeadline.for_duration(0)
        with tempfile.TemporaryDirectory() as directory_name:
            raw_path = Path(directory_name) / "expired.json"
            with patch("urllib.request.urlopen") as urlopen:
                with self.assertRaisesRegex(
                    CollectionDeadlineExceeded,
                    "^collection deadline exceeded$",
                ):
                    collect_cex_market_observation(
                        market(),
                        snapshot_id="expired-cex",
                        raw_path=raw_path,
                        deadline=deadline,
                    )

            self.assertFalse(raw_path.exists())
        urlopen.assert_not_called()

    def test_upbit_candidate_does_not_wrap_mid_request_deadline_exhaustion(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_cex_depth import collect_cex_market_observation

        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

        clock = Clock()
        deadline = CollectionDeadline.for_duration(
            1,
            clock=clock.monotonic,
            sleeper=lambda seconds: None,
        )

        def expiring_request(_url, *, deadline):
            clock.now = 2.0
            deadline.require_remaining()

        with tempfile.TemporaryDirectory() as directory_name:
            raw_path = Path(directory_name) / "expired-upbit.json"
            with self.assertRaisesRegex(
                CollectionDeadlineExceeded,
                "^collection deadline exceeded$",
            ):
                collect_cex_market_observation(
                    market(exchange="upbit"),
                    snapshot_id="expired-upbit",
                    raw_path=raw_path,
                    request=expiring_request,
                    deadline=deadline,
                )
            self.assertFalse(raw_path.exists())


if __name__ == "__main__":
    unittest.main()
