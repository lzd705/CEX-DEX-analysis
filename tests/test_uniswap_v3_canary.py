import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_uniswap_v3_canary import run_canary
from scripts.execution_cost import EXECUTION_DIRECTIONS, EXECUTION_NOTIONALS_USD


MARKET_A = "dex:eth:uniswap_v3:0x1111111111111111111111111111111111111111:UNI"
MARKET_B = "dex:eth:uniswap_v3:0x2222222222222222222222222222222222222222:UNI"


def _authority():
    return {
        MARKET_A: {
            "chain": "eth",
            "dex": "uniswap_v3",
            "pool_address": "0x1111111111111111111111111111111111111111",
        },
        MARKET_B: {
            "chain": "eth",
            "dex": "uniswap_v3",
            "pool_address": "0x2222222222222222222222222222222222222222",
        },
    }


def _tvl_rows(raw_hash):
    return [
        {"market_id": MARKET_A, "status": "observed", "raw_response_sha256": raw_hash},
        {"market_id": MARKET_B, "status": "observed", "raw_response_sha256": raw_hash},
    ]


def _depth_rows(blocks=(123, 123)):
    return [
        {
            "token_symbol": "UNI",
            "chain": "eth",
            "dex": "uniswap_v3",
            "pool_address": MARKET_A.split(":")[-2],
            "status": "observed",
            "block_number": str(blocks[0]),
        },
        {
            "token_symbol": "UNI",
            "chain": "eth",
            "dex": "uniswap_v3",
            "pool_address": MARKET_B.split(":")[-2],
            "status": "observed",
            "block_number": str(blocks[1]),
        },
    ]


def _execution_rows(status="observed", blocks=(123, 123)):
    rows = []
    for market_id, block_number in zip((MARKET_A, MARKET_B), blocks):
        for direction in EXECUTION_DIRECTIONS:
            for notional in EXECUTION_NOTIONALS_USD:
                rows.append(
                    {
                        "market_id": market_id,
                        "direction": direction,
                        "requested_notional_usd": str(notional),
                        "status": status,
                        "block_number": str(block_number),
                    }
                )
    return rows


def _quoter_parity_rows():
    return [
        {
            "direction": direction,
            "requested_notional_usd": str(notional),
            "status": "exact_match",
        }
        for direction in EXECUTION_DIRECTIONS
        for notional in EXECUTION_NOTIONALS_USD
    ]


class UniswapV3CanaryTest(unittest.TestCase):
    def _run(
        self,
        depth_rows,
        execution_rows,
        *,
        block_hashes=("0x" + "a" * 64, "0x" + "a" * 64),
        parity_rows=None,
    ):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name) / "canary"
        tvl_payload = b'{"data":[]}\n'
        tvl_hash = hashlib.sha256(tvl_payload).hexdigest()

        def collect_tvl_fixture(_pools, *, raw_root, sleep_seconds):
            directory = raw_root / "tvl-snapshot"
            directory.mkdir(parents=True)
            (directory / "001-eth.json").write_bytes(tvl_payload)
            return "tvl-snapshot", _tvl_rows(tvl_hash)

        def collect_depth_fixture(_pools, *, raw_root, sleep_seconds):
            directory = raw_root / "depth-snapshot"
            directory.mkdir(parents=True)
            for index, (market_id, block_hash) in enumerate(
                zip((MARKET_A, MARKET_B), block_hashes),
                start=1,
            ):
                block_number = str(depth_rows[index - 1]["block_number"])
                block = {
                    "number": block_number,
                    "hash": block_hash,
                    "timestamp": "2024-01-01T00:00:00+00:00",
                }
                payload = {
                    "v3_tick_scan_manifest": {
                        "market_id": market_id,
                        "block": block,
                        "block_final": dict(block),
                        "quoter_v2_parity": (
                            _quoter_parity_rows()
                            if parity_rows is None
                            else parity_rows
                        ),
                    },
                    "usd_price_evidence": {
                        "observed_at": "2024-01-01T00:00:00+00:00",
                        "source": "GeckoTerminal API v2",
                        "source_endpoint": (
                            "https://api.geckoterminal.com/api/v2/networks/eth/pools/multi/x"
                        ),
                        "raw_response_sha256": tvl_hash,
                    },
                }
                (directory / ("00{}-pool.json".format(index))).write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )
            return "depth-snapshot", depth_rows, execution_rows

        with patch(
            "scripts.run_uniswap_v3_canary.load_uniswap_v3_execution_authority",
            return_value=_authority(),
        ):
            with patch(
                "scripts.run_uniswap_v3_canary.collect_tvl",
                side_effect=collect_tvl_fixture,
            ):
                with patch(
                    "scripts.run_uniswap_v3_canary.collect_dex_depth_with_execution",
                    side_effect=collect_depth_fixture,
                ):
                    return run_canary(root)

    def test_complete_two_pool_result_remains_non_publishing(self):
        result = self._run(_depth_rows(), _execution_rows())

        self.assertFalse(result["published"])
        self.assertEqual(result["block_numbers"], [123])
        self.assertEqual(result["depth_status_counts"], {"observed": 2})
        self.assertEqual(result["execution_status_counts"], {"observed": 20})
        self.assertEqual(result["execution_scenario_count"], 20)

    def test_partial_execution_is_not_a_passing_canary(self):
        with self.assertRaisesRegex(ValueError, "all 20 exact scenarios"):
            self._run(_depth_rows(), _execution_rows(status="partial"))

    def test_two_pool_canary_requires_one_shared_block(self):
        with self.assertRaisesRegex(ValueError, "one shared fixed block"):
            self._run(
                _depth_rows(blocks=(123, 124)),
                _execution_rows(blocks=(123, 124)),
            )

    def test_duplicate_scenario_cannot_replace_a_missing_scenario(self):
        execution = _execution_rows()
        execution[-1] = dict(execution[-2])

        with self.assertRaisesRegex(ValueError, "scenario inventory"):
            self._run(_depth_rows(), execution)

    def test_same_height_with_different_hash_is_not_one_shared_block(self):
        with self.assertRaisesRegex(ValueError, "block number and hash"):
            self._run(
                _depth_rows(),
                _execution_rows(),
                block_hashes=("0x" + "a" * 64, "0x" + "b" * 64),
            )

    def test_raw_quoter_proof_cannot_duplicate_one_scenario(self):
        parity_rows = _quoter_parity_rows()
        parity_rows[-1] = dict(parity_rows[-2])

        with self.assertRaisesRegex(ValueError, "Quoter scenario inventory"):
            self._run(
                _depth_rows(),
                _execution_rows(),
                parity_rows=parity_rows,
            )


if __name__ == "__main__":
    unittest.main()
