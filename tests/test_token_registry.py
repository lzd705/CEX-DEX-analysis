import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.token_registry import (
    REGISTRY_SCHEMA_VERSION,
    TokenRegistry,
    TokenRegistryError,
    atomic_write_registry,
    load_registry,
    normalize_chain,
    normalize_contract_address,
    token_identity_key,
)


SOLANA_ADDRESS = "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"


def token_record(symbol="TEST", address="0x" + "12" * 20, status="pending"):
    return {
        "token_symbol": symbol,
        "token_name": "%s Token" % symbol,
        "chain": "eth",
        "contract_address": address,
        "decimals": 18,
        "coingecko_id": None,
        "source": "geckoterminal",
        "source_token_id": "eth_%s" % address.lower(),
        "status": status,
        "cex_mapping": {
            "status": "requires_manual_review",
            "cex_symbol": None,
            "exchanges": [],
        },
        "created_at": "2026-07-29T00:00:00+00:00",
        "created_by": "admin",
        "activated_at": None,
        "last_job_id": "job-1",
    }


class TokenRegistryTest(unittest.TestCase):
    def test_chain_allowlist_and_address_canonicalization(self):
        self.assertEqual(normalize_chain(" ETH "), "eth")
        self.assertEqual(
            normalize_contract_address("eth", "0x" + "AB" * 20),
            "0x" + "ab" * 20,
        )
        self.assertEqual(
            normalize_contract_address("starknet", "0x47"),
            "0x" + "0" * 62 + "47",
        )
        self.assertEqual(
            normalize_contract_address("solana", SOLANA_ADDRESS),
            SOLANA_ADDRESS,
        )

        with self.assertRaisesRegex(TokenRegistryError, "Unsupported chain"):
            normalize_chain("polygon")
        with self.assertRaises(TokenRegistryError) as context:
            normalize_contract_address("eth", "0x123")
        self.assertEqual(context.exception.code, "invalid_contract_address")
        with self.assertRaises(TokenRegistryError):
            normalize_contract_address("solana", "0" * 32)

    def test_atomic_registry_round_trip_uses_canonical_identity_key(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "token_registry.json"
            record = token_record()
            key = token_identity_key(record["chain"], record["contract_address"])
            atomic_write_registry(
                path,
                {
                    "schema_version": REGISTRY_SCHEMA_VERSION,
                    "tokens": {key: record},
                },
            )

            payload = load_registry(path)

            self.assertEqual(payload["tokens"][key]["token_symbol"], "TEST")
            self.assertEqual(
                payload["tokens"][key]["cex_mapping"]["status"],
                "requires_manual_review",
            )
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_atomic_replace_failure_preserves_previous_registry(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "token_registry.json"
            original = {"schema_version": REGISTRY_SCHEMA_VERSION, "tokens": {}}
            atomic_write_registry(path, original)
            before = path.read_bytes()
            record = token_record()
            key = token_identity_key(record["chain"], record["contract_address"])

            with patch("scripts.token_registry.os.replace", side_effect=OSError("blocked")):
                with self.assertRaises(OSError):
                    atomic_write_registry(
                        path,
                        {
                            "schema_version": REGISTRY_SCHEMA_VERSION,
                            "tokens": {key: record},
                        },
                    )

            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(
                [item for item in path.parent.iterdir() if item.suffix == ".tmp"],
                [],
            )

    def test_upsert_is_idempotent_and_rejects_symbol_collision(self):
        with TemporaryDirectory() as directory:
            registry = TokenRegistry(Path(directory) / "token_registry.json")
            first = registry.upsert(token_record())
            second = registry.upsert({**token_record(), "status": "active"})

            self.assertEqual(first["created_at"], second["created_at"])
            self.assertEqual(second["status"], "active")
            self.assertEqual(len(registry.list_records()), 1)

            conflicting = token_record(
                address="0x" + "34" * 20,
            )
            with self.assertRaises(TokenRegistryError) as context:
                registry.upsert(conflicting)
            self.assertEqual(context.exception.code, "symbol_collision")

    def test_reserved_static_symbol_and_identity_conflict_are_rejected(self):
        with TemporaryDirectory() as directory:
            registry = TokenRegistry(Path(directory) / "token_registry.json")
            with self.assertRaises(TokenRegistryError) as context:
                registry.upsert(token_record(), reserved_symbols={"TEST"})
            self.assertEqual(context.exception.code, "symbol_collision")

            registry.upsert(token_record())
            replacement = {**token_record(), "token_symbol": "OTHER"}
            with self.assertRaises(TokenRegistryError) as context:
                registry.upsert(replacement)
            self.assertEqual(context.exception.code, "identity_conflict")

    def test_registry_rejects_unapproved_cex_instrument(self):
        with TemporaryDirectory() as directory:
            registry = TokenRegistry(Path(directory) / "token_registry.json")
            record = token_record()
            record["cex_mapping"] = {
                "status": "requires_manual_review",
                "cex_symbol": "TEST/USDT",
                "exchanges": ["binance"],
            }

            with self.assertRaises(TokenRegistryError) as context:
                registry.upsert(record)

            self.assertEqual(context.exception.code, "invalid_registry_record")

    def test_invalid_json_and_schema_are_not_silently_replaced(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "token_registry.json"
            path.write_text("{", encoding="utf-8")
            with self.assertRaises(TokenRegistryError) as context:
                load_registry(path)
            self.assertEqual(context.exception.code, "invalid_registry")

            path.write_text(
                json.dumps({"schema_version": 999, "tokens": {}}),
                encoding="utf-8",
            )
            with self.assertRaises(TokenRegistryError) as context:
                load_registry(path)
            self.assertEqual(
                context.exception.code,
                "unsupported_registry_schema",
            )


if __name__ == "__main__":
    unittest.main()
