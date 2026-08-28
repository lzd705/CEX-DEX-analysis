import hashlib
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import fetch_dex_depth
from scripts.execution_cost import EXECUTION_DIRECTIONS, EXECUTION_NOTIONALS_USD
from scripts.run_collection_cycle import build_step_commands


BLOCK_HASH = "0x" + "a" * 64


class ExactCandidateFixture:
    @staticmethod
    def quoter_calldata(selector, token_in, token_out, amount, fee, price_limit):
        return selector + "".join(
            (
                token_in[2:].rjust(64, "0"),
                token_out[2:].rjust(64, "0"),
                f"{amount:064x}",
                f"{fee:064x}",
                f"{price_limit:064x}",
            )
        )

    def __init__(self, root):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.authority_path = root / "authority.json"
        self.authority_path.write_bytes(
            fetch_dex_depth.DEFAULT_V3_EXECUTION_AUTHORITY.read_bytes()
        )
        self.authority = fetch_dex_depth.load_uniswap_v3_execution_authority(
            self.authority_path
        )
        self.market_ids = sorted(self.authority)
        self.tvl_snapshot_id = "tvl-snapshot"
        self.depth_snapshot_id = "depth-snapshot"
        self.tvl_raw_root = root / "raw/tvl"
        self.depth_raw_root = root / "raw/depth"
        self.tvl_directory = self.tvl_raw_root / self.tvl_snapshot_id
        self.depth_directory = self.depth_raw_root / self.depth_snapshot_id
        self.tvl_directory.mkdir(parents=True)
        self.depth_directory.mkdir(parents=True)

        gecko_payload = {
            "data": [
                {
                    "id": "eth_" + record["pool_address"],
                    "type": "pool",
                    "attributes": {"address": record["pool_address"]},
                    "relationships": {
                        "dex": {"data": {"id": record["dex"]}},
                        "base_token": {
                            "data": {"id": "eth_" + record["token0_address"]}
                        },
                        "quote_token": {
                            "data": {"id": "eth_" + record["token1_address"]}
                        },
                    },
                }
                for record in self.authority.values()
            ]
        }
        gecko_bytes = (
            json.dumps(gecko_payload, sort_keys=True) + "\n"
        ).encode("utf-8")
        self.gecko_path = self.tvl_directory / "001-eth.json"
        self.gecko_path.write_bytes(gecko_bytes)
        self.gecko_sha256 = hashlib.sha256(gecko_bytes).hexdigest()
        self.inventory = []
        self.depth_rows = []
        self.execution_rows = []
        self.transcript_paths = {}

        for index, market_id in enumerate(self.market_ids, start=1):
            authority = self.authority[market_id]
            inventory_row = {
                "market_id": market_id,
                "snapshot_id": self.tvl_snapshot_id,
                "token_symbol": "UNI",
                "chain": authority["chain"],
                "dex": authority["dex"],
                "pool_address": authority["pool_address"],
                "base_token_id": "eth_" + authority["token0_address"],
                "quote_token_id": "eth_" + authority["token1_address"],
                "status": "observed",
                "reason_code": "observed",
                "source": "GeckoTerminal API v2",
                "source_endpoint": (
                    "https://api.geckoterminal.com/api/v2/networks/eth/"
                    "pools/multi/two-authority-pools"
                ),
                "raw_response_sha256": self.gecko_sha256,
            }
            self.inventory.append(inventory_row)
            price_limits = {"zero_for_one": 111, "one_for_zero": 222}
            parity = []
            scenario_facts = {}
            records = [
                {
                    "request": {
                        "jsonrpc": "2.0",
                        "id": 0,
                        "method": "eth_getBlockByNumber",
                        "params": ["finalized", False],
                    },
                    "response": {
                        "jsonrpc": "2.0",
                        "id": 0,
                        "result": {
                            "number": "0x7b",
                            "hash": BLOCK_HASH,
                            "timestamp": "0x65920080",
                        },
                    },
                    "response_sha256": "e" * 64,
                }
            ]
            scenarios = (
                (notional, direction)
                for notional in EXECUTION_NOTIONALS_USD
                for direction in EXECUTION_DIRECTIONS
            )
            for request_id, (notional, direction) in enumerate(scenarios, start=1):
                selector = (
                    fetch_dex_depth.SELECTOR_QUOTE_EXACT_INPUT_SINGLE_V2
                    if direction == "sell_token"
                    else fetch_dex_depth.SELECTOR_QUOTE_EXACT_OUTPUT_SINGLE_V2
                )
                target_raw = request_id * 10**18
                amount_raw = index * 1000 + request_id
                zero_for_one = direction == "sell_token"
                token_in = (
                    authority["token0_address"]
                    if zero_for_one
                    else authority["token1_address"]
                )
                token_out = (
                    authority["token1_address"]
                    if zero_for_one
                    else authority["token0_address"]
                )
                price_limit = price_limits[
                    "zero_for_one" if zero_for_one else "one_for_zero"
                ]
                item = {
                    "direction": direction,
                    "requested_notional_usd": str(notional),
                    "status": "exact_match",
                    "amount_raw": amount_raw,
                    "sqrt_price_x96_after": index * 2000 + request_id,
                    "initialized_ticks_crossed": request_id,
                    "gas_estimate_raw": 100000 + request_id,
                    "core_liquidity_boundaries_crossed": request_id,
                }
                parity.append(item)
                scenario_facts[(direction, str(notional))] = {
                    "target_raw": target_raw,
                    "amount_raw": amount_raw,
                }
                result = "0x" + "".join(
                    f"{value:064x}"
                    for value in (
                        item["amount_raw"],
                        item["sqrt_price_x96_after"],
                        item["initialized_ticks_crossed"],
                        item["gas_estimate_raw"],
                    )
                )
                records.append(
                    {
                        "request": [
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "method": "eth_call",
                                "params": [
                                    {
                                        "to": authority["quoter_v2_address"],
                                        "data": self.quoter_calldata(
                                            selector,
                                            token_in,
                                            token_out,
                                            target_raw,
                                            authority["fee_pips"],
                                            price_limit,
                                        ),
                                    },
                                    "0x7b",
                                ],
                            }
                        ],
                        "response": [
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "result": result,
                            }
                        ],
                        "response_sha256": "f" * 64,
                    }
                )
            block = {
                "number": "123",
                "hash": BLOCK_HASH,
                "timestamp": "2024-01-01T00:00:00+00:00",
            }
            transcript = {
                "pool": {
                    "token_symbol": "UNI",
                    "chain": authority["chain"],
                    "dex": authority["dex"],
                    "pool_address": authority["pool_address"],
                },
                "block_number": 123,
                "source_endpoint": "https://user:secret@rpc.invalid/key",
                "records": records,
                "usd_price_evidence": {
                    "source_snapshot_id": self.tvl_snapshot_id,
                    "observed_at": "2024-01-01T00:00:00+00:00",
                    "source": "GeckoTerminal API v2",
                    "source_endpoint": inventory_row["source_endpoint"],
                    "raw_response_sha256": self.gecko_sha256,
                    "base_token_id": inventory_row["base_token_id"],
                    "quote_token_id": inventory_row["quote_token_id"],
                },
                "v3_tick_scan_manifest": {
                    "schema": "uniswap_v3_tick_scan_manifest/v1",
                    "market_id": market_id,
                    "chain_id": "0x1",
                    "block_number": 123,
                    "block_hash": BLOCK_HASH,
                    "pool_address": authority["pool_address"],
                    "authority": authority,
                    "block": block,
                    "block_final": dict(block),
                    "bitmap_words": [],
                    "tick_evidence": [],
                    "directions": {
                        name: {"price_limit_x96": price_limit}
                        for name, price_limit in price_limits.items()
                    },
                    "bitmap_word_radius": authority["bitmap_word_radius"],
                    "quoter_v2_parity": parity,
                },
            }
            transcript_path = self.depth_directory / f"{index:03d}-pool.json"
            transcript_path.write_text(
                json.dumps(transcript, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self.transcript_paths[market_id] = transcript_path
            transcript_hash = self.sha256(transcript_path)
            depth_row = {
                "snapshot_id": self.depth_snapshot_id,
                "token_symbol": "UNI",
                "chain": authority["chain"],
                "dex": authority["dex"],
                "pool_address": authority["pool_address"],
                "token0_address": authority["token0_address"],
                "token1_address": authority["token1_address"],
                "fee_bps": str(authority["fee_pips"] // 100),
                "status": "observed",
                "block_number": "123",
                "raw_response_sha256": transcript_hash,
                **{
                    f"depth_{band}bps_complete": "1"
                    for band in fetch_dex_depth.DEPTH_BANDS_BPS
                },
            }
            self.depth_rows.append(depth_row)
            for direction in EXECUTION_DIRECTIONS:
                for notional in EXECUTION_NOTIONALS_USD:
                    facts = scenario_facts[(direction, str(notional))]
                    target_quantity = Decimal(facts["target_raw"]) / Decimal(
                        10**authority["token0_decimals"]
                    )
                    quote_quantity = Decimal(facts["amount_raw"]) / Decimal(
                        10**authority["token1_decimals"]
                    )
                    self.execution_rows.append(
                        {
                            "snapshot_id": self.depth_snapshot_id,
                            "source_snapshot_id": self.depth_snapshot_id,
                            "market_id": market_id,
                            "direction": direction,
                            "requested_notional_usd": str(notional),
                            "target_token_address": authority["token0_address"],
                            "target_token_decimals": str(
                                authority["token0_decimals"]
                            ),
                            "quote_token_address": authority["token1_address"],
                            "quote_token_decimals": str(
                                authority["token1_decimals"]
                            ),
                            "target_token_quantity": format(target_quantity, "f"),
                            "filled_token_quantity": format(target_quantity, "f"),
                            "quote_amount": format(quote_quantity, "f"),
                            "fee_rate_bps": str(authority["fee_pips"] // 100),
                            "status": "observed",
                            "block_number": "123",
                            "raw_response_sha256": transcript_hash,
                        }
                    )

        self.write_manifests()

    @staticmethod
    def sha256(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    @staticmethod
    def replace_abi_word(value, index, replacement):
        start = 10 + index * 64
        return value[:start] + f"{replacement:064x}" + value[start + 64 :]

    @staticmethod
    def replace_address_word(value, index, replacement):
        start = 10 + index * 64
        word = replacement[2:].rjust(64, "0")
        return value[:start] + word + value[start + 64 :]

    def write_manifests(self):
        tvl_manifest = {
            "snapshot_id": self.tvl_snapshot_id,
            "pool_count": len(self.inventory),
            "token_count": 1,
            "chain_count": 1,
            "status_counts": {
                "observed": len(self.inventory),
                "missing": 0,
                "not_found": 0,
                "failed": 0,
            },
            "reason_code_counts": {"observed": len(self.inventory)},
            "raw_files": [self.gecko_path.name],
        }
        self.tvl_manifest_path = self.tvl_directory / "manifest.json"
        self.tvl_manifest_path.write_text(
            json.dumps(tvl_manifest, indent=2) + "\n", encoding="utf-8"
        )
        depth_manifest = {
            "snapshot_id": self.depth_snapshot_id,
            "pool_count": len(self.depth_rows),
            "execution_row_count": len(self.execution_rows),
            "status_counts": {"observed": len(self.depth_rows)},
            "raw_files": sorted(path.name for path in self.transcript_paths.values()),
        }
        self.depth_manifest_path = self.depth_directory / "manifest.json"
        self.depth_manifest_path.write_text(
            json.dumps(depth_manifest, indent=2) + "\n", encoding="utf-8"
        )

    def validate(self):
        return fetch_dex_depth.validate_uniswap_v3_exact_candidate(
            self.inventory,
            self.depth_rows,
            self.execution_rows,
            tvl_raw_root=self.tvl_raw_root,
            depth_raw_root=self.depth_raw_root,
            authority_path=self.authority_path,
        )

    def rewrite_transcript(self, market_id, mutate, *, rebind_rows=True):
        path = self.transcript_paths[market_id]
        payload = json.loads(path.read_text(encoding="utf-8"))
        mutate(payload)
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if rebind_rows:
            new_hash = self.sha256(path)
            for row in self.depth_rows:
                if fetch_dex_depth.dex_market_id(row) == market_id:
                    row["raw_response_sha256"] = new_hash
            for row in self.execution_rows:
                if row["market_id"] == market_id:
                    row["raw_response_sha256"] = new_hash

    def rebind_tvl_snapshot(self, snapshot_id, destination=None):
        if destination is not None and destination != self.tvl_directory:
            destination.parent.mkdir(parents=True, exist_ok=True)
            self.tvl_directory.rename(destination)
            self.tvl_directory = destination
            self.gecko_path = destination / self.gecko_path.name
            self.tvl_manifest_path = destination / "manifest.json"
        self.tvl_snapshot_id = snapshot_id
        for row in self.inventory:
            row["snapshot_id"] = snapshot_id
        for market_id in self.market_ids:
            self.rewrite_transcript(
                market_id,
                lambda transcript, snapshot_id=snapshot_id: transcript[
                    "usd_price_evidence"
                ].update(source_snapshot_id=snapshot_id),
            )
        self.write_manifests()

    def rebind_depth_snapshot(self, snapshot_id, destination=None):
        if destination is not None and destination != self.depth_directory:
            destination.parent.mkdir(parents=True, exist_ok=True)
            self.depth_directory.rename(destination)
            self.depth_directory = destination
            self.depth_manifest_path = destination / "manifest.json"
            self.transcript_paths = {
                market_id: destination / path.name
                for market_id, path in self.transcript_paths.items()
            }
        self.depth_snapshot_id = snapshot_id
        for row in self.depth_rows:
            row["snapshot_id"] = snapshot_id
        for row in self.execution_rows:
            row["snapshot_id"] = snapshot_id
            row["source_snapshot_id"] = snapshot_id
        self.write_manifests()


class UniswapV3ExactPublicationTest(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)
        self.fixture = ExactCandidateFixture(self.root)

    def test_complete_candidate_returns_deterministic_path_free_receipt(self):
        first = self.fixture.validate()
        second = self.fixture.validate()

        self.assertEqual(first, second)
        self.assertEqual(first["schema"], "uniswap_v3_exact_validation/v1")
        self.assertEqual(first["market_ids"], self.fixture.market_ids)
        self.assertEqual(first["depth_observed_count"], 2)
        self.assertEqual(first["execution_observed_scenario_count"], 20)
        self.assertEqual(first["shared_finalized_block"]["number"], 123)
        self.assertEqual(first["shared_finalized_block"]["hash"], BLOCK_HASH)
        self.assertEqual(len(first["pool_evidence"]), 2)
        self.assertEqual(
            first["geckoterminal_raw_response_sha256"],
            [self.fixture.gecko_sha256],
        )
        encoded = json.dumps(first, sort_keys=True)
        self.assertNotIn(str(self.root), encoded)
        self.assertNotIn("secret", encoded)
        self.assertNotIn("rpc.invalid", encoded)

    def test_missing_or_mismatched_production_inventory_is_rejected(self):
        cases = {
            "missing": self.fixture.inventory[:-1],
            "wrong_token": [
                {**self.fixture.inventory[0], "base_token_id": "eth_0x" + "f" * 40},
                self.fixture.inventory[1],
            ],
            "wrong_dex": [
                {**self.fixture.inventory[0], "dex": "sushiswap-v3-ethereum"},
                self.fixture.inventory[1],
            ],
        }
        for name, inventory in cases.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "production inventory"
            ):
                fetch_dex_depth.validate_uniswap_v3_exact_candidate(
                    inventory,
                    self.fixture.depth_rows,
                    self.fixture.execution_rows,
                    tvl_raw_root=self.fixture.tvl_raw_root,
                    depth_raw_root=self.fixture.depth_raw_root,
                    authority_path=self.fixture.authority_path,
                )

    def test_incomplete_or_duplicate_execution_scenario_is_rejected(self):
        for name, rows in (
            ("missing", self.fixture.execution_rows[:-1]),
            (
                "duplicate",
                self.fixture.execution_rows[:-1]
                + [dict(self.fixture.execution_rows[-2])],
            ),
        ):
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "execution scenario inventory"
            ):
                fetch_dex_depth.validate_uniswap_v3_exact_candidate(
                    self.fixture.inventory,
                    self.fixture.depth_rows,
                    rows,
                    tvl_raw_root=self.fixture.tvl_raw_root,
                    depth_raw_root=self.fixture.depth_raw_root,
                    authority_path=self.fixture.authority_path,
                )

    def test_partial_or_unsupported_candidate_rows_are_rejected(self):
        partial_depth = [dict(row) for row in self.fixture.depth_rows]
        partial_depth[0]["status"] = "partial"
        partial_depth[0]["depth_100bps_complete"] = "0"
        unsupported_execution = [dict(row) for row in self.fixture.execution_rows]
        unsupported_execution[0]["status"] = "unsupported"

        for name, depth_rows, execution_rows in (
            ("partial_depth", partial_depth, self.fixture.execution_rows),
            ("unsupported_execution", self.fixture.depth_rows, unsupported_execution),
        ):
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "observed"
            ):
                fetch_dex_depth.validate_uniswap_v3_exact_candidate(
                    self.fixture.inventory,
                    depth_rows,
                    execution_rows,
                    tvl_raw_root=self.fixture.tvl_raw_root,
                    depth_raw_root=self.fixture.depth_raw_root,
                    authority_path=self.fixture.authority_path,
                )

    def test_mixed_finalized_block_hashes_are_rejected(self):
        market_id = self.fixture.market_ids[1]

        def change_block(payload):
            manifest = payload["v3_tick_scan_manifest"]
            manifest["block_hash"] = "0x" + "b" * 64
            manifest["block"]["hash"] = "0x" + "b" * 64
            manifest["block_final"]["hash"] = "0x" + "b" * 64
            payload["records"][0]["response"]["result"]["hash"] = (
                "0x" + "b" * 64
            )

        self.fixture.rewrite_transcript(market_id, change_block)

        with self.assertRaisesRegex(ValueError, "shared finalized block"):
            self.fixture.validate()

    def test_non_exact_or_duplicate_raw_quoter_parity_is_rejected(self):
        market_id = self.fixture.market_ids[0]
        for name, mutate in (
            (
                "non_exact",
                lambda parity: parity[0].update(status="mismatch"),
            ),
            (
                "duplicate",
                lambda parity: parity.__setitem__(-1, dict(parity[-2])),
            ),
        ):
            with self.subTest(name=name):
                fresh = ExactCandidateFixture(self.root / name)
                fresh.rewrite_transcript(
                    market_id,
                    lambda payload: mutate(
                        payload["v3_tick_scan_manifest"]["quoter_v2_parity"]
                    ),
                )
                with self.assertRaisesRegex(ValueError, "Quoter"):
                    fresh.validate()

    def test_quoter_parity_must_match_retained_rpc_result(self):
        market_id = self.fixture.market_ids[0]
        self.fixture.rewrite_transcript(
            market_id,
            lambda payload: payload["v3_tick_scan_manifest"][
                "quoter_v2_parity"
            ][0].update(
                amount_raw=(
                    payload["v3_tick_scan_manifest"]["quoter_v2_parity"][0][
                        "amount_raw"
                    ]
                    + 1
                )
            ),
        )

        with self.assertRaisesRegex(ValueError, "raw Quoter"):
            self.fixture.validate()

    def test_quoter_calldata_must_match_authority_scenario_and_execution_row(self):
        market_id = self.fixture.market_ids[0]
        authority = self.fixture.authority[market_id]

        def mutate_data(payload, mutation):
            call = payload["records"][1]["request"][0]["params"][0]
            call["data"] = mutation(call["data"])

        cases = {
            "token_in": lambda data: self.fixture.replace_address_word(
                data, 0, authority["token1_address"]
            ),
            "token_out": lambda data: self.fixture.replace_address_word(
                data, 1, authority["token0_address"]
            ),
            "amount": lambda data: self.fixture.replace_abi_word(
                data, 2, 10**18 + 1
            ),
            "fee": lambda data: self.fixture.replace_abi_word(data, 3, 500),
            "price_limit": lambda data: self.fixture.replace_abi_word(
                data, 4, 112
            ),
        }
        for name, mutation in cases.items():
            with self.subTest(name=name):
                fresh = ExactCandidateFixture(self.root / f"calldata-{name}")
                fresh.rewrite_transcript(
                    market_id,
                    lambda payload, mutation=mutation: mutate_data(
                        payload, mutation
                    ),
                )
                with self.assertRaisesRegex(ValueError, "Quoter|execution"):
                    fresh.validate()

    def test_quoter_response_binds_all_words_and_exact_request_id(self):
        market_id = self.fixture.market_ids[0]
        cases = {
            "gas_estimate": lambda payload: payload["v3_tick_scan_manifest"][
                "quoter_v2_parity"
            ][0].update(gas_estimate_raw=100002),
            "sqrt_uint160": lambda payload: self._rebind_quoter_word(
                payload, 0, 1, 1 << 160, "sqrt_price_x96_after"
            ),
            "ticks_uint32": lambda payload: self._rebind_quoter_word(
                payload, 0, 2, 1 << 32, "initialized_ticks_crossed"
            ),
            "duplicate_response_id": lambda payload: payload["records"][1][
                "response"
            ].append(dict(payload["records"][1]["response"][0])),
            "orphan_response_id": lambda payload: payload["records"][1][
                "response"
            ].append(
                {
                    **payload["records"][1]["response"][0],
                    "id": 999,
                }
            ),
        }
        for name, mutation in cases.items():
            with self.subTest(name=name):
                fresh = ExactCandidateFixture(self.root / f"response-{name}")
                fresh.rewrite_transcript(market_id, mutation)
                with self.assertRaisesRegex(ValueError, "Quoter"):
                    fresh.validate()

    @staticmethod
    def _rebind_quoter_word(payload, parity_index, word_index, value, field):
        parity = payload["v3_tick_scan_manifest"]["quoter_v2_parity"][
            parity_index
        ]
        parity[field] = value
        response = payload["records"][parity_index + 1]["response"][0]
        response["result"] = ExactCandidateFixture.replace_abi_word(
            response["result"], word_index, value
        )

    def test_candidate_execution_output_must_match_quoter_result(self):
        self.fixture.execution_rows[0]["quote_amount"] = "999999"

        with self.assertRaisesRegex(ValueError, "Quoter|execution"):
            self.fixture.validate()

    def test_finalized_block_must_match_retained_rpc_result(self):
        market_id = self.fixture.market_ids[0]
        self.fixture.rewrite_transcript(
            market_id,
            lambda payload: payload["records"].pop(0),
        )

        with self.assertRaisesRegex(ValueError, "raw finalized block"):
            self.fixture.validate()

    def test_missing_or_tampered_transcript_and_scan_manifest_are_rejected(self):
        missing = ExactCandidateFixture(self.root / "missing-transcript")
        missing.transcript_paths[missing.market_ids[0]].unlink()
        with self.assertRaisesRegex(ValueError, "depth manifest|transcript"):
            missing.validate()

        tampered = ExactCandidateFixture(self.root / "tampered-transcript")
        tampered.transcript_paths[tampered.market_ids[0]].write_text(
            "{}\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "transcript"):
            tampered.validate()

        scan = ExactCandidateFixture(self.root / "scan-manifest")
        scan.rewrite_transcript(
            scan.market_ids[0],
            lambda payload: payload["v3_tick_scan_manifest"]["authority"].update(
                fee_pips=500
            ),
        )
        with self.assertRaisesRegex(ValueError, "scan manifest|authority"):
            scan.validate()

    def test_missing_or_tampered_depth_and_tvl_manifests_are_rejected(self):
        for name, target, mutation, pattern in (
            (
                "missing_depth",
                "depth",
                lambda path: path.unlink(),
                "depth manifest",
            ),
            (
                "tampered_depth",
                "depth",
                lambda path: path.write_text(
                    json.dumps(
                        {
                            **json.loads(path.read_text()),
                            "execution_row_count": 19,
                        }
                    ),
                    encoding="utf-8",
                ),
                "depth manifest",
            ),
            (
                "missing_tvl",
                "tvl",
                lambda path: path.unlink(),
                "TVL manifest",
            ),
            (
                "tampered_tvl",
                "tvl",
                lambda path: path.write_text(
                    json.dumps(
                        {
                            **json.loads(path.read_text()),
                            "raw_files": ["not-the-retained-response.json"],
                        }
                    ),
                    encoding="utf-8",
                ),
                "TVL manifest",
            ),
        ):
            with self.subTest(name=name):
                fresh = ExactCandidateFixture(self.root / name)
                path = (
                    fresh.depth_manifest_path
                    if target == "depth"
                    else fresh.tvl_manifest_path
                )
                mutation(path)
                with self.assertRaisesRegex(ValueError, pattern):
                    fresh.validate()

    def test_tvl_manifest_semantic_counters_are_recomputed(self):
        cases = {
            "token_count": 2,
            "chain_count": 2,
            "status_counts": {"observed": 1, "failed": 1},
            "reason_code_counts": {"observed": 1, "validation": 1},
        }
        for field, value in cases.items():
            with self.subTest(field=field):
                fresh = ExactCandidateFixture(self.root / f"tvl-{field}")
                manifest = json.loads(
                    fresh.tvl_manifest_path.read_text(encoding="utf-8")
                )
                manifest[field] = value
                fresh.tvl_manifest_path.write_text(
                    json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
                )
                with self.assertRaisesRegex(ValueError, "TVL manifest"):
                    fresh.validate()

    def test_tvl_manifest_raw_files_exactly_cover_candidate_inventory(self):
        extra_path = self.fixture.tvl_directory / "002-extra.json"
        extra_path.write_text('{"data": []}\n', encoding="utf-8")
        manifest = json.loads(
            self.fixture.tvl_manifest_path.read_text(encoding="utf-8")
        )
        manifest["raw_files"].append(extra_path.name)
        self.fixture.tvl_manifest_path.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )

        with self.assertRaisesRegex(ValueError, "GeckoTerminal|TVL manifest"):
            self.fixture.validate()

    def test_snapshot_ids_are_canonical_and_cannot_escape_evidence_roots(self):
        cases = (
            (
                "absolute_tvl",
                "tvl",
                lambda fresh: str(fresh.tvl_directory),
                lambda fresh, _snapshot_id: fresh.tvl_directory,
            ),
            (
                "credentialed_uri",
                "tvl",
                lambda _fresh: "https://user:secret@example.invalid/snapshot",
                lambda fresh, snapshot_id: fresh.tvl_raw_root / snapshot_id,
            ),
            (
                "traversal_tvl",
                "tvl",
                lambda _fresh: "../outside-tvl",
                lambda fresh, snapshot_id: fresh.tvl_raw_root / snapshot_id,
            ),
            (
                "traversal_depth",
                "depth",
                lambda _fresh: "nested/../depth-snapshot",
                lambda fresh, _snapshot_id: fresh.depth_directory,
            ),
        )
        for name, kind, snapshot_id_fn, destination_fn in cases:
            with self.subTest(name=name):
                fresh = ExactCandidateFixture(self.root / name)
                snapshot_id = snapshot_id_fn(fresh)
                destination = destination_fn(fresh, snapshot_id)
                if name == "traversal_depth":
                    (fresh.depth_raw_root / "nested").mkdir()
                if kind == "tvl":
                    fresh.rebind_tvl_snapshot(snapshot_id, destination)
                else:
                    fresh.rebind_depth_snapshot(snapshot_id, destination)
                with self.assertRaisesRegex(ValueError, "snapshot|evidence root"):
                    fresh.validate()

    def test_tampered_or_symlinked_geckoterminal_response_is_rejected(self):
        tampered = ExactCandidateFixture(self.root / "tampered-gecko")
        tampered.gecko_path.write_bytes(b'{"data":[]}\n')
        with self.assertRaisesRegex(ValueError, "GeckoTerminal"):
            tampered.validate()

        symlinked = ExactCandidateFixture(self.root / "symlink-gecko")
        target = symlinked.root / "outside.json"
        target.write_bytes(symlinked.gecko_path.read_bytes())
        symlinked.gecko_path.unlink()
        symlinked.gecko_path.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "regular evidence"):
            symlinked.validate()

    def test_geckoterminal_hash_cannot_hide_missing_pool_identity(self):
        payload = json.loads(self.fixture.gecko_path.read_text(encoding="utf-8"))
        payload["data"] = payload["data"][:-1]
        self.fixture.gecko_path.write_text(
            json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
        )
        new_hash = self.fixture.sha256(self.fixture.gecko_path)
        for row in self.fixture.inventory:
            row["raw_response_sha256"] = new_hash
        for market_id in self.fixture.market_ids:
            self.fixture.rewrite_transcript(
                market_id,
                lambda transcript, new_hash=new_hash: transcript[
                    "usd_price_evidence"
                ].update(raw_response_sha256=new_hash),
            )

        with self.assertRaisesRegex(ValueError, "GeckoTerminal.*identity"):
            self.fixture.validate()

    def test_authority_market_cannot_use_bounded_merge_publication(self):
        market_id = self.fixture.market_ids[0]
        with self.assertRaisesRegex(ValueError, "authority.*bounded"):
            fetch_dex_depth.require_uniswap_v3_publication_scope(
                self.fixture.inventory,
                market_id=market_id,
                merge_publish=True,
                exact_validation_enabled=False,
                publishing=True,
            )

    def test_authority_market_cannot_call_bounded_publication_helpers(self):
        market_id = self.fixture.market_ids[0]
        cases = (
            (
                "merge",
                lambda: fetch_dex_depth.merge_exact_publication_bundle(
                    [],
                    [],
                    target_market_id=market_id,
                    publish_dir=self.root / "missing-publish",
                ),
            ),
            (
                "publish",
                lambda: fetch_dex_depth.publish_exact_publication_bundle(
                    [],
                    [],
                    target_market_id=market_id,
                    history_rows_to_append=[],
                    output_dir=self.root / "processed",
                    publish_dir=self.root / "published",
                    preflight_reports={},
                ),
            ),
        )
        for name, call in cases:
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "authority.*bounded"):
                    call()

    def test_unrelated_bounded_recovery_remains_allowed(self):
        try:
            fetch_dex_depth.require_uniswap_v3_publication_scope(
                self.fixture.inventory,
                market_id=(
                    "dex:eth:uniswap_v3:"
                    "0x1111111111111111111111111111111111111111:UNI"
                ),
                merge_publish=True,
                exact_validation_enabled=False,
                publishing=True,
            )
        except ValueError as error:
            self.fail(f"unrelated bounded recovery was rejected: {error}")

    def test_rejected_candidate_preserves_existing_publication_bytes(self):
        publish_dir = self.root / "published"
        publish_dir.mkdir()
        protected = {
            publish_dir / fetch_dex_depth.LATEST_FILENAME: b"old-depth\n",
            publish_dir / fetch_dex_depth.EXECUTION_LATEST_FILENAME: b"old-execution\n",
        }
        for path, payload in protected.items():
            path.write_bytes(payload)
        self.fixture.gecko_path.write_bytes(b"tampered\n")
        arguments = SimpleNamespace(
            tvl_csv=self.root / "inventory.csv",
            output_dir=self.root / "processed",
            raw_root=self.fixture.depth_raw_root,
            tvl_raw_root=self.fixture.tvl_raw_root,
            publish_local=False,
            publish_dir=publish_dir,
            sleep_seconds=0,
            tokens=None,
            chains=None,
            market_id=None,
            merge_publish=False,
            require_uniswap_v3_exact_validation=True,
        )

        with patch.object(fetch_dex_depth, "parse_args", return_value=arguments), patch.object(
            fetch_dex_depth,
            "load_pool_inventory",
            return_value=self.fixture.inventory,
        ), patch.object(
            fetch_dex_depth,
            "collect_dex_depth_with_execution",
            return_value=(
                self.fixture.depth_snapshot_id,
                self.fixture.depth_rows,
                self.fixture.execution_rows,
            ),
        ), patch.object(
            fetch_dex_depth,
            "publish_full_publication_bundle",
            side_effect=AssertionError("publication must not run"),
        ):
            with self.assertRaisesRegex(ValueError, "GeckoTerminal"):
                fetch_dex_depth.main()

        self.assertEqual(
            {path: path.read_bytes() for path in protected},
            protected,
        )
        self.assertFalse(arguments.output_dir.exists())

    def test_runner_enables_exact_gate_for_production_and_explicit_no_publish(self):
        data_dir = self.root / "runtime"
        production = build_step_commands(
            "dex_depth",
            publish_local=True,
            data_dir=data_dir,
        )
        rehearsal = build_step_commands(
            "dex_depth",
            publish_local=False,
            data_dir=data_dir,
            require_uniswap_v3_exact_validation=True,
        )
        full_rehearsal = build_step_commands(
            "full",
            publish_local=False,
            data_dir=data_dir,
            require_uniswap_v3_exact_validation=True,
        )
        ordinary_fixture = build_step_commands(
            "dex_depth",
            publish_local=False,
            data_dir=data_dir,
        )

        self.assertIn("--require-uniswap-v3-exact-validation", production[-1][1])
        self.assertIn("--require-uniswap-v3-exact-validation", rehearsal[-1][1])
        self.assertIn(
            "--require-uniswap-v3-exact-validation", full_rehearsal[-1][1]
        )
        self.assertNotIn(
            "--require-uniswap-v3-exact-validation", ordinary_fixture[-1][1]
        )
        self.assertEqual(
            rehearsal[-1][1][rehearsal[-1][1].index("--tvl-raw-root") + 1],
            str(data_dir.resolve() / "raw/tvl"),
        )


if __name__ == "__main__":
    unittest.main()
