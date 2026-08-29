"""Contracts for the bounded SHIB V2/V2 research registry."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest

from scripts import shib_v2_research
from scripts import shib_v2_research_io
from scripts.shib_v2_research_io import load_bounded_json


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = PROJECT_ROOT / "config/shib_v2_research_pools.json"


def valid_registry_payload():
    return {
        "schema": "shib_v2_research_registry/v1",
        "chain": {"name": "eth", "chain_id": 1},
        "tokens": {
            "SHIB": {
                "address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
                "decimals": 18,
                "runtime_code_size_bytes": 4852,
                "runtime_code_sha256": "5c813da8be193a1a33a7533edc758e3ad29f1fa1730cbf2d8c9fc8a7f31c78f3",
            },
            "WETH": {
                "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "decimals": 18,
                "runtime_code_size_bytes": 3124,
                "runtime_code_sha256": "5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739",
            },
        },
        "pools": [
            {
                "dex": "uniswap_v2",
                "factory": {
                    "address": "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
                    "runtime_code_size_bytes": 13859,
                    "runtime_code_sha256": "3abc53f12a9cb8ae37ebfada9efc261c1ab4c2759d161e341a49bf67df3f8321",
                },
                "router": {
                    "address": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
                    "runtime_code_size_bytes": 21943,
                    "runtime_code_sha256": "ccef50da4af021b09ada39d78db5d281fffff81a57969c7028bccc1f50d37854",
                },
                "pair": {
                    "address": "0x811beed0119b4afce20d2583eb608c6f7af1954f",
                    "runtime_code_size_bytes": 11293,
                    "runtime_code_sha256": "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4",
                },
                "token0": "SHIB",
                "token1": "WETH",
                "fee_model": {
                    "formula": "amount_in_with_fee=amount_in*fee_numerator;denominator=reserve_in*fee_denominator+amount_in_with_fee",
                    "fee_bps": 30,
                    "fee_numerator": 997,
                    "fee_denominator": 1000,
                    "evidence": {"kind": "runtime_code_bound"},
                },
            },
            {
                "dex": "shibaswap_v1",
                "factory": {
                    "address": "0x115934131916c8b277dd010ee02de363c09d037c",
                    "runtime_code_size_bytes": 15527,
                    "runtime_code_sha256": "bccd00fecc8d072c7635ef40bd5b7721057975123aa8639d62a37f90f6a45b53",
                },
                "router": {
                    "address": "0x03f7724180aa6b939894b5ca4314783b0b36b329",
                    "runtime_code_size_bytes": 18469,
                    "runtime_code_sha256": "bb5f84ee54eacd3a273b2a3942ad904f8194a999f32394682cda2080b14b0423",
                },
                "pair": {
                    "address": "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
                    "runtime_code_size_bytes": 10654,
                    "runtime_code_sha256": "83589060885cd6b139ce4b4ed723653d124a00b50c0fa203dbd5a425cb272bc7",
                },
                "token0": "SHIB",
                "token1": "WETH",
                "fee_model": {
                    "formula": "amount_in_with_fee=amount_in*fee_numerator;denominator=reserve_in*fee_denominator+amount_in_with_fee",
                    "fee_bps": 30,
                    "fee_numerator": 997,
                    "fee_denominator": 1000,
                    "evidence": {
                        "kind": "pair_native_parameters",
                        "target": "pair",
                        "native_fee_denominator": 1000,
                        "total_fee": 3,
                        "alpha": 1,
                        "beta": 3,
                    },
                },
            },
        ],
        "usd_reference": {
            "kind": "chainlink_aggregator_v3",
            "proxy_address": "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419",
            "runtime_code_size_bytes": 9571,
            "runtime_code_sha256": "ed698309290de3517c7201fcad9a9dbd4b8cde4a72c9add23129201f299c6f2b",
            "description": "ETH / USD",
            "decimals": 8,
            "max_age_seconds": 3600,
        },
        "requested_notionals_usd": ["1000", "5000", "10000", "50000", "100000"],
    }


def add_unknown_field(payload):
    payload["unknown"] = "forbidden"
    return payload


def uppercase_shib(payload):
    payload["tokens"]["SHIB"]["address"] = (
        "0x95AD61B0A150D79219DCF64E1E6CC01F0B64C4CE"
    )
    return payload


def duplicate_first_pool(payload):
    payload["pools"].append(copy.deepcopy(payload["pools"][0]))
    return payload


class ResearchRegistryTests(unittest.TestCase):
    def test_repository_registry_fixes_exactly_two_shib_weth_pools(self):
        registry = shib_v2_research.load_research_registry(
            json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        )
        self.assertEqual(registry["schema"], "shib_v2_research_registry/v1")
        self.assertEqual(registry["chain"], {"name": "eth", "chain_id": 1})
        self.assertEqual(
            [pool["pair"]["address"] for pool in registry["pools"]],
            [
                "0x811beed0119b4afce20d2583eb608c6f7af1954f",
                "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
            ],
        )
        self.assertEqual(
            registry["requested_notionals_usd"],
            ["1000", "5000", "10000", "50000", "100000"],
        )

    def test_registry_rejects_unknown_fields_case_drift_and_duplicate_pools(self):
        for mutation in (add_unknown_field, uppercase_shib, duplicate_first_pool):
            with self.subTest(mutation=mutation.__name__):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.load_research_registry(
                        mutation(copy.deepcopy(valid_registry_payload()))
                    )

    def test_registry_rejects_fee_drift_that_preserves_native_relationships(self):
        mutations = (
            (0, {"fee_bps": 1, "fee_numerator": 999}),
            (1, {"fee_bps": 15, "fee_denominator": 2000}),
        )
        for pool_index, changes in mutations:
            with self.subTest(pool_index=pool_index, changes=changes):
                payload = valid_registry_payload()
                payload["pools"][pool_index]["fee_model"].update(changes)
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.load_research_registry(payload)


class SafeJsonBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_bounded_loader_rejects_duplicate_json_keys_and_symlink(self):
        duplicate = self.root / "duplicate.json"
        duplicate.write_text('{"schema":"a","schema":"b"}', encoding="utf-8")
        with self.assertRaisesRegex(
            shib_v2_research.ResearchContractError, "duplicate JSON key"
        ):
            load_bounded_json(duplicate, "registry")
        link = self.root / "link.json"
        link.symlink_to(duplicate)
        with self.assertRaisesRegex(
            shib_v2_research.ResearchContractError, "regular file"
        ):
            load_bounded_json(link, "registry")

    def test_bounded_loader_accepts_canonical_repository_registry(self):
        registry = load_bounded_json(REGISTRY_PATH, "repository registry")
        self.assertEqual(registry["tokens"]["SHIB"]["address"], (
            "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce"
        ))

    def test_public_scan_rejects_url_secret_private_path_and_provider_error(self):
        slash = chr(47)
        for value in (
            "https://rpc.example/key",
            "sk-live-secretmaterial",
            slash + "Users" + slash + "private" + slash + "research",
            {"provider_error": "arbitrary text"},
        ):
            with self.subTest(value=value):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.scan_public_payload({"value": value})

    def test_public_scan_rejects_private_paths_secret_and_key_aliases(self):
        for payload in (
            {"value": "/root/research"},
            {"value": "/etc/passwd"},
            {"value": "ghp_abcdefghijklmnopqrstuvwxyz1234567890"},
            {"privatePath": "hidden"},
            {"providerError": "hidden"},
            {"raw_payload": "hidden"},
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    shib_v2_research.scan_public_payload(payload)

    def test_public_scan_keeps_legitimate_token_and_evm_address_fields(self):
        self.assertIsNone(shib_v2_research.scan_public_payload({
            "token": "SHIB",
            "address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
            "description": "ETH / USD",
        }))

    def test_bounded_loader_rejects_float_exponent_and_nonfinite_tokens(self):
        for token in ("1.0", "1e3", "NaN", "Infinity", "-Infinity"):
            path = self.root / "numeric.json"
            path.write_text('{"value":' + token + '}', encoding="utf-8")
            with self.subTest(token=token):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    load_bounded_json(path, "numeric fixture")

    def test_bounded_loader_rejects_size_and_parser_bounds(self):
        cases = (
            ("size", b" " * (shib_v2_research_io.MAX_JSON_BYTES + 1)),
            ("nesting", b'{"value":' + b"[" * 65 + b"0" + b"]" * 65 + b"}\n"),
            (
                "members",
                b"{" + b",".join(
                    b'"k%05d":0' % index for index in range(4097)
                ) + b"}\n",
            ),
            (
                "string",
                b'{"value":"' + b"a" * (
                    shib_v2_research_io.MAX_JSON_STRING_TOKEN_BYTES + 1
                ) + b'"}\n',
            ),
            (
                "integer",
                b'{"value":' + b"1" * (
                    shib_v2_research_io.MAX_JSON_INTEGER_TOKEN_BYTES + 1
                ) + b"}\n",
            ),
        )
        path = self.root / "bounded.json"
        for label, raw in cases:
            with self.subTest(label=label):
                path.write_bytes(raw)
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    load_bounded_json(path, label)

    def test_bounded_loader_requires_canonical_bytes_and_writer_round_trips(self):
        from scripts.shib_v2_research_io import atomic_write_canonical_json

        path = self.root / "registry.json"
        path.write_text('{"b":1,"a":2}\n', encoding="utf-8")
        with self.assertRaises(shib_v2_research.ResearchContractError):
            load_bounded_json(path, "registry")
        atomic_write_canonical_json(path, {"b": 1, "a": 2})
        self.assertEqual(path.read_bytes(), b'{"a":2,"b":1}\n')
        self.assertEqual(load_bounded_json(path, "registry"), {"a": 2, "b": 1})

    def test_atomic_writer_preserves_existing_file_on_rejected_payload(self):
        from scripts.shib_v2_research_io import atomic_write_canonical_json

        path = self.root / "registry.json"
        original = b'{"kept":1}\n'
        path.write_bytes(original)
        rejected_payloads = (
            {"value": "a" * (shib_v2_research_io.MAX_JSON_BYTES + 1)},
            {"value": "a" * (
                shib_v2_research_io.MAX_JSON_STRING_TOKEN_BYTES + 1
            )},
        )
        for payload in rejected_payloads:
            with self.subTest(length=len(payload["value"])):
                with self.assertRaises(shib_v2_research.ResearchContractError):
                    atomic_write_canonical_json(path, payload)
                self.assertEqual(path.read_bytes(), original)
