import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import fetch_dex_depth
from scripts.execution_cost import EXECUTION_DIRECTIONS, EXECUTION_NOTIONALS_USD
from scripts.run_collection_cycle import build_step_commands


BLOCK_HASH = "0x" + "a" * 64


class ExactCandidateFixture:
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
                "source": "GeckoTerminal API v2",
                "source_endpoint": (
                    "https://api.geckoterminal.com/api/v2/networks/eth/"
                    "pools/multi/two-authority-pools"
                ),
                "raw_response_sha256": self.gecko_sha256,
            }
            self.inventory.append(inventory_row)
            parity = [
                {
                    "direction": direction,
                    "requested_notional_usd": str(notional),
                    "status": "exact_match",
                    "amount_raw": index * 1000 + scenario_index,
                    "sqrt_price_x96_after": index * 2000 + scenario_index,
                    "initialized_ticks_crossed": scenario_index,
                    "core_liquidity_boundaries_crossed": scenario_index,
                }
                for scenario_index, (notional, direction) in enumerate(
                    (
                        (notional, direction)
                        for notional in EXECUTION_NOTIONALS_USD
                        for direction in EXECUTION_DIRECTIONS
                    ),
                    start=1,
                )
            ]
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
            for request_id, item in enumerate(parity, start=1):
                selector = (
                    fetch_dex_depth.SELECTOR_QUOTE_EXACT_INPUT_SINGLE_V2
                    if item["direction"] == "sell_token"
                    else fetch_dex_depth.SELECTOR_QUOTE_EXACT_OUTPUT_SINGLE_V2
                )
                result = "0x" + "".join(
                    f"{value:064x}"
                    for value in (
                        item["amount_raw"],
                        item["sqrt_price_x96_after"],
                        item["initialized_ticks_crossed"],
                        100000,
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
                                        "data": selector + "00" * 160,
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
                    "directions": {},
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
                    self.execution_rows.append(
                        {
                            "snapshot_id": self.depth_snapshot_id,
                            "source_snapshot_id": self.depth_snapshot_id,
                            "market_id": market_id,
                            "direction": direction,
                            "requested_notional_usd": str(notional),
                            "status": "observed",
                            "block_number": "123",
                            "raw_response_sha256": transcript_hash,
                        }
                    )

        self.write_manifests()

    @staticmethod
    def sha256(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

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
