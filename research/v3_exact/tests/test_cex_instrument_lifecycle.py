import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.cex_instrument_lifecycle import (
    load_cex_instrument_lifecycle,
    load_cex_instrument_lifecycle_manifest,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CexInstrumentLifecycleTest(unittest.TestCase):
    def test_repository_manifest_is_bounded_and_does_not_invent_delisting_dates(self):
        reviews = load_cex_instrument_lifecycle(
            PROJECT_ROOT / "data" / "curated" / "cex_instrument_lifecycle.json"
        )

        self.assertEqual(
            set(reviews),
            {
                "cex:crypto_com:CAKE/USDT",
                "cex:crypto_com:EIGEN/USDT",
                "cex:crypto_com:ETHFI/USDT",
                "cex:crypto_com:GMX/USDT",
                "cex:crypto_com:JTO/USDT",
                "cex:crypto_com:MORPHO/USDT",
                "cex:crypto_com:RAY/USDT",
            },
        )
        for review in reviews.values():
            self.assertEqual(
                review["current_listing_status"],
                "absent_from_official_current_catalog",
            )
            self.assertEqual(
                review["reason_code"],
                "instrument_absent_from_current_catalog",
            )
            self.assertNotIn("delisted_at", review)
            self.assertNotIn("effective_date", review)
        manifest = load_cex_instrument_lifecycle_manifest(
            PROJECT_ROOT / "data" / "curated" / "cex_instrument_lifecycle.json"
        )
        self.assertEqual(
            manifest["checked_at_utc"],
            manifest["generated_at_utc"],
        )
        self.assertRegex(manifest["response_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(
            manifest["configured_market_ids_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertGreater(manifest["inventory_count"], 0)
        self.assertGreaterEqual(manifest["configured_market_count"], 2)

    def test_manifest_rejects_unknown_fields_and_invalid_evidence(self):
        source = PROJECT_ROOT / "data" / "curated" / "cex_instrument_lifecycle.json"
        payload = json.loads(source.read_text(encoding="utf-8"))
        mutations = (
            lambda item: item.update({"delisted_at": "2025-01-01"}),
            lambda item: item.update({"response_sha256": "not-a-hash"}),
            lambda item: item.update({"inventory_count": -1}),
            lambda item: item.update({"inventory_count": 0}),
            lambda item: item.update({"source_url": "https://example.com/catalog"}),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                candidate = json.loads(json.dumps(payload))
                mutate(candidate["reviews"][0])
                with TemporaryDirectory() as directory:
                    path = Path(directory) / "manifest.json"
                    path.write_text(json.dumps(candidate), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        load_cex_instrument_lifecycle(path)

    def test_manifest_rejects_missing_or_invalid_configured_market_set_hash(self):
        source = PROJECT_ROOT / "data" / "curated" / "cex_instrument_lifecycle.json"
        payload = json.loads(source.read_text(encoding="utf-8"))
        candidates = []
        missing = json.loads(json.dumps(payload))
        missing.pop("configured_market_ids_sha256", None)
        candidates.append(missing)
        invalid = json.loads(json.dumps(payload))
        invalid["configured_market_ids_sha256"] = "not-a-hash"
        candidates.append(invalid)

        for candidate in candidates:
            with self.subTest(candidate=candidate.get("configured_market_ids_sha256")):
                with TemporaryDirectory() as directory:
                    path = Path(directory) / "manifest.json"
                    path.write_text(json.dumps(candidate), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        load_cex_instrument_lifecycle_manifest(path)


if __name__ == "__main__":
    unittest.main()
