import csv
import hashlib
import importlib
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import scripts.cex_instrument_lifecycle as lifecycle_contract


NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
SOURCE_URL = "https://api.crypto.com/exchange/v1/public/get-instruments"


def official_payload(rows):
    return json.dumps(
        {
            "id": -1,
            "method": "public/get-instruments",
            "code": 0,
            "result": {"data": rows},
        },
        separators=(",", ":"),
    ).encode("utf-8")


def spot(symbol, base, quote):
    return {
        "symbol": symbol,
        "inst_type": "CCY_PAIR",
        "display_name": base + "/" + quote,
        "base_ccy": base,
        "quote_ccy": quote,
        "tradable": True,
    }


def perpetual(symbol, base, quote):
    return {
        "symbol": symbol,
        "inst_type": "PERPETUAL_SWAP",
        "display_name": symbol,
        "base_ccy": base,
        "quote_ccy": quote,
        "tradable": True,
    }


def write_token_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["token_symbol", "cex_symbol"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


class CollectCexInstrumentLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.tokens_csv = self.root / "tokens.csv"
        self.registry_path = self.root / "token_registry.json"
        self.manifest_path = self.root / "cex_instrument_lifecycle.json"
        self.raw_root = self.root / "raw"
        write_token_csv(
            self.tokens_csv,
            [
                {"token_symbol": "AAVE", "cex_symbol": "AAVE/USDT"},
                {"token_symbol": "GMX", "cex_symbol": "GMX/USDT"},
            ],
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def collector_module(self):
        try:
            module = importlib.import_module(
                "scripts.collect_cex_instrument_lifecycle"
            )
        except ModuleNotFoundError:
            module = None
        self.assertIsNotNone(
            module,
            "the lifecycle contract must expose its daily collector module",
        )
        return module

    def test_inventory_parser_accepts_only_exact_spot_market_identities(self):
        collector = self.collector_module()
        raw = official_payload(
            [
                spot("AAVE_USDT", "AAVE", "USDT"),
                spot("GMX_USD", "GMX", "USD"),
                perpetual("GMXUSDT-PERP", "GMX", "USDT"),
            ]
        )

        instruments, inventory_count = lifecycle_contract.parse_crypto_com_inventory(raw)

        self.assertEqual(instruments, {"AAVE_USDT", "GMX_USD"})
        self.assertEqual(inventory_count, 3)

    def test_tls_context_uses_bundled_certifi_trust_store_when_available(self):
        collector = self.collector_module()
        expected_context = object()
        with patch.object(
            collector,
            "certifi",
            SimpleNamespace(where=lambda: "/trusted/cacert.pem"),
        ), patch.object(
            collector.ssl,
            "create_default_context",
            return_value=expected_context,
        ) as create_context:
            context = collector.build_tls_context()

        self.assertIs(context, expected_context)
        create_context.assert_called_once_with(cafile="/trusted/cacert.pem")

    def test_inventory_parser_fails_closed_on_empty_or_inconsistent_catalog(self):
        malformed = official_payload(
            [spot("AAVE_USD", "AAVE", "USDT")]
        )
        wrong_display = spot("AAVE_USDT", "AAVE", "USDT")
        wrong_display["display_name"] = "AAVE/USD"
        not_tradable = spot("AAVE_USDT", "AAVE", "USDT")
        not_tradable["tradable"] = False
        for raw in (
            official_payload([]),
            official_payload([perpetual("AAVEUSDT-PERP", "AAVE", "USDT")]),
            malformed,
            official_payload([wrong_display]),
            official_payload([not_tradable]),
            b"not-json",
        ):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    lifecycle_contract.parse_crypto_com_inventory(raw)

    def test_inventory_parser_excludes_noncanonical_namespaced_spot_rows(self):
        namespaced = spot("SHIB_BTC@OEX_HK", "SHIB", "BTC")
        raw = official_payload(
            [
                spot("AAVE_USDT", "AAVE", "USDT"),
                namespaced,
            ]
        )

        instruments, inventory_count = lifecycle_contract.parse_crypto_com_inventory(
            raw
        )

        self.assertEqual(instruments, {"AAVE_USDT"})
        self.assertEqual(inventory_count, 2)

    def test_configured_catalog_includes_only_active_approved_runtime_mapping(self):
        collector = self.collector_module()
        runtime_record = {
            "token_symbol": "TEST",
            "token_name": "Test Token",
            "chain": "eth",
            "contract_address": "0x1111111111111111111111111111111111111111",
            "decimals": 18,
            "coingecko_id": None,
            "source": "geckoterminal",
            "source_token_id": None,
            "status": "active",
            "cex_mapping": {
                "status": "approved",
                "cex_symbol": "TEST/USDT",
                "exchanges": ["crypto_com"],
            },
            "created_at": "2026-08-01T00:00:00+00:00",
            "created_by": "admin",
            "activated_at": "2026-08-01T00:00:00+00:00",
            "last_job_id": None,
        }
        ignored_record = dict(runtime_record)
        ignored_record.update(
            {
                "token_symbol": "OTHER",
                "contract_address": "0x2222222222222222222222222222222222222222",
                "cex_mapping": {
                    "status": "approved",
                    "cex_symbol": "OTHER/USDT",
                    "exchanges": ["binance"],
                },
            }
        )
        self.registry_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "tokens": {
                        "eth:0x1111111111111111111111111111111111111111": runtime_record,
                        "eth:0x2222222222222222222222222222222222222222": ignored_record,
                    },
                }
            ),
            encoding="utf-8",
        )

        markets = collector.load_configured_crypto_com_markets(
            self.tokens_csv,
            self.registry_path,
        )

        self.assertEqual(
            [item["market_id"] for item in markets],
            [
                "cex:crypto_com:AAVE/USDT",
                "cex:crypto_com:GMX/USDT",
                "cex:crypto_com:TEST/USDT",
            ],
        )

    def test_success_keeps_raw_response_and_replaces_reviews_with_current_absence(self):
        collector = self.collector_module()
        old_manifest = {
            "schema": "cex_instrument_lifecycle/v1",
            "generated_at_utc": "2026-07-31T00:00:00+00:00",
            "review_count": 1,
            "reviews": [
                {
                    "market_id": "cex:crypto_com:AAVE/USDT",
                    "market_type": "cex",
                    "token_symbol": "AAVE",
                    "exchange": "crypto_com",
                    "instrument": "AAVE/USDT",
                    "current_listing_status": "absent_from_official_current_catalog",
                    "reason_code": "instrument_absent_from_current_catalog",
                    "checked_at_utc": "2026-07-31T00:00:00+00:00",
                    "source_url": SOURCE_URL,
                    "http_status": 200,
                    "response_sha256": "a" * 64,
                    "inventory_count": 1,
                    "instrument_present": False,
                }
            ],
        }
        self.manifest_path.write_text(
            json.dumps(old_manifest), encoding="utf-8"
        )
        raw = official_payload(
            [
                spot("AAVE_USDT", "AAVE", "USDT"),
                spot("GMX_USD", "GMX", "USD"),
            ]
        )

        result = collector.collect_crypto_com_lifecycle(
            tokens_csv=self.tokens_csv,
            runtime_registry=self.registry_path,
            manifest_path=self.manifest_path,
            raw_root=self.raw_root,
            now=NOW,
            fetcher=lambda _url: (200, raw),
        )

        digest = hashlib.sha256(raw).hexdigest()
        self.assertEqual((self.raw_root / (digest + ".json")).read_bytes(), raw)
        reviews = lifecycle_contract.load_cex_instrument_lifecycle(
            self.manifest_path
        )
        self.assertEqual(set(reviews), {"cex:crypto_com:GMX/USDT"})
        self.assertNotIn("cex:crypto_com:AAVE/USDT", reviews)
        self.assertEqual(result["review_count"], 1)
        self.assertEqual(result["response_sha256"], digest)

    def test_zero_absence_manifest_retains_root_source_and_freshness_evidence(self):
        collector = self.collector_module()
        raw = official_payload(
            [
                spot("AAVE_USDT", "AAVE", "USDT"),
                spot("GMX_USDT", "GMX", "USDT"),
            ]
        )

        collector.collect_crypto_com_lifecycle(
            tokens_csv=self.tokens_csv,
            runtime_registry=self.registry_path,
            manifest_path=self.manifest_path,
            raw_root=self.raw_root,
            now=NOW,
            fetcher=lambda _url: (200, raw),
        )

        payload = lifecycle_contract.load_cex_instrument_lifecycle_manifest(
            self.manifest_path
        )
        self.assertEqual(payload["review_count"], 0)
        self.assertEqual(payload["reviews"], [])
        self.assertEqual(payload["checked_at_utc"], NOW.isoformat())
        self.assertEqual(payload["generated_at_utc"], NOW.isoformat())
        self.assertEqual(payload["response_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(payload["inventory_count"], 2)
        self.assertEqual(payload["configured_market_count"], 2)
        self.assertEqual(
            payload["configured_market_ids_sha256"],
            "2ac2c0098299d53cc739440dae18d8a422e26054c429ed506ea4fa954c8695a9",
        )

    def test_manifest_accepts_registry_safe_token_punctuation(self):
        write_token_csv(
            self.tokens_csv,
            [{"token_symbol": "AAVE.X", "cex_symbol": "AAVE.X/USDT"}],
        )
        collector = self.collector_module()
        raw = official_payload([spot("BTC_USDT", "BTC", "USDT")])

        collector.collect_crypto_com_lifecycle(
            tokens_csv=self.tokens_csv,
            runtime_registry=self.registry_path,
            manifest_path=self.manifest_path,
            raw_root=self.raw_root,
            now=NOW,
            fetcher=lambda _url: (200, raw),
        )

        reviews = lifecycle_contract.load_cex_instrument_lifecycle(
            self.manifest_path
        )
        self.assertEqual(
            set(reviews),
            {"cex:crypto_com:AAVE.X/USDT"},
        )

    def test_parse_failure_retains_raw_evidence_without_overwriting_manifest(self):
        collector = self.collector_module()
        previous = b'{"previous":"manifest"}\n'
        self.manifest_path.write_bytes(previous)
        malformed = b'{"code":0,"result":{"data":[]}}'

        with self.assertRaises(ValueError):
            collector.collect_crypto_com_lifecycle(
                tokens_csv=self.tokens_csv,
                runtime_registry=self.registry_path,
                manifest_path=self.manifest_path,
                raw_root=self.raw_root,
                now=NOW,
                fetcher=lambda _url: (200, malformed),
            )

        digest = hashlib.sha256(malformed).hexdigest()
        self.assertEqual(self.manifest_path.read_bytes(), previous)
        self.assertEqual(
            (self.raw_root / (digest + ".json")).read_bytes(),
            malformed,
        )

    def test_network_failure_does_not_overwrite_previous_manifest(self):
        collector = self.collector_module()
        previous = b'{"previous":"manifest"}\n'
        self.manifest_path.write_bytes(previous)

        def fail(_url):
            raise OSError("network unavailable")

        with self.assertRaises(OSError):
            collector.collect_crypto_com_lifecycle(
                tokens_csv=self.tokens_csv,
                runtime_registry=self.registry_path,
                manifest_path=self.manifest_path,
                raw_root=self.raw_root,
                now=NOW,
                fetcher=fail,
            )

        self.assertEqual(self.manifest_path.read_bytes(), previous)
        self.assertFalse(self.raw_root.exists())


if __name__ == "__main__":
    unittest.main()
