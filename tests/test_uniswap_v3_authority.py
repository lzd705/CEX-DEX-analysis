import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts.fetch_dex_depth import (
    V3_EXECUTION_AUTHORITY_PATH,
    load_uniswap_v3_execution_authority,
    match_uniswap_v3_execution_authority,
)


UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
QUOTER_V2 = "0x61ffe014ba17989e743c5f6cb21bf9697530b21e"
UNI_USDT_POOL = "0x3470447f3cecffac709d3e783a307790b0208d60"
UNI_WETH_POOL = "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801"
UNI_USDT_MARKET = "dex:eth:uniswap_v3:{}:UNI".format(UNI_USDT_POOL)
UNI_WETH_MARKET = "dex:eth:uniswap_v3:{}:UNI".format(UNI_WETH_POOL)


def authority_payload():
    common = {
        "chain": "eth", "chain_id": 1, "dex": "uniswap_v3", "factory_address": FACTORY,
        "quoter_v2_address": QUOTER_V2, "token0_address": UNI, "token0_decimals": 18,
        "fee_pips": 3000, "tick_spacing": 60, "bitmap_word_radius": 8,
    }
    return {"schema": "uniswap_v3_execution_markets/v1", "markets": [
        dict(common, market_id=UNI_USDT_MARKET, pool_address=UNI_USDT_POOL, token1_address=USDT, token1_decimals=6),
        dict(common, market_id=UNI_WETH_MARKET, pool_address=UNI_WETH_POOL, token1_address=WETH, token1_decimals=18),
    ]}


def pool_inventory_record(market_id=UNI_USDT_MARKET):
    return {"market_id": market_id, "token_symbol": "UNI", "chain": "eth", "dex": "uniswap_v3", "pool_address": market_id.split(":")[-2]}


def observed_identity(pool_address=UNI_USDT_POOL):
    return {"chain_id": 1, "pool_address": pool_address, "factory_address": FACTORY,
            "factory_get_pool_address": pool_address, "token0_address": UNI, "token0_decimals": 18,
            "token1_address": USDT, "token1_decimals": 6, "fee_pips": 3000, "tick_spacing": 60}


class UniswapV3AuthorityTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def write_authority(self, payload=None):
        path = self.root / "uniswap_v3_execution_markets.json"
        path.write_text(json.dumps(payload or authority_payload()), encoding="utf-8")
        return path

    def test_committed_authority_is_exactly_the_two_reviewed_uni_pools(self):
        authority = load_uniswap_v3_execution_authority(V3_EXECUTION_AUTHORITY_PATH)
        self.assertEqual(set(authority), {UNI_USDT_MARKET, UNI_WETH_MARKET})
        self.assertEqual(authority[UNI_USDT_MARKET], authority_payload()["markets"][0])
        self.assertEqual(authority[UNI_WETH_MARKET], authority_payload()["markets"][1])

    def test_loader_requires_every_identity_and_scan_bound_field(self):
        fields = ("market_id", "chain", "chain_id", "dex", "pool_address", "factory_address", "quoter_v2_address", "token0_address", "token0_decimals", "token1_address", "token1_decimals", "fee_pips", "tick_spacing", "bitmap_word_radius")
        for field in fields:
            with self.subTest(field=field):
                payload = authority_payload()
                del payload["markets"][0][field]
                with self.assertRaisesRegex(ValueError, field):
                    load_uniswap_v3_execution_authority(self.write_authority(payload))

    def test_loader_rejects_noncanonical_or_ambiguous_authority(self):
        cases = (
            lambda data: data.update(schema="v0"),
            lambda data: data["markets"].append(copy.deepcopy(data["markets"][0])),
            lambda data: data["markets"].pop(),
            lambda data: data["markets"][0].update(pool_address="0x" + "9" * 40),
            lambda data: data["markets"][0].update(chain="ETH"),
            lambda data: data["markets"][0].update(dex="sushiswap"),
            lambda data: data["markets"][0].update(factory_address="0x1234"),
            lambda data: data["markets"][0].update(token0_decimals=-1),
            lambda data: data["markets"][0].update(fee_pips=0),
            lambda data: data["markets"][0].update(bitmap_word_radius=0),
        )
        for mutate in cases:
            payload = authority_payload()
            mutate(payload)
            with self.assertRaises(ValueError):
                load_uniswap_v3_execution_authority(self.write_authority(payload))

    def test_exact_onchain_identity_matches_authority(self):
        authority = load_uniswap_v3_execution_authority(self.write_authority())
        self.assertEqual(
            match_uniswap_v3_execution_authority(pool_inventory_record(), observed_identity(), authority=authority),
            authority[UNI_USDT_MARKET],
        )

    def test_unapproved_pool_returns_none_only_for_unapproved_identity(self):
        authority = load_uniswap_v3_execution_authority(self.write_authority())
        unapproved = "0x" + "7" * 40
        self.assertIsNone(match_uniswap_v3_execution_authority(
            pool_inventory_record("dex:eth:uniswap_v3:{}:UNI".format(unapproved)),
            observed_identity(unapproved), authority=authority,
        ))

    def test_identity_mismatch_and_missing_identity_fail_closed(self):
        authority = load_uniswap_v3_execution_authority(self.write_authority())
        cases = {
            "chain": ({**pool_inventory_record(), "chain": "arbitrum"}, observed_identity()),
            "dex": ({**pool_inventory_record(), "dex": "sushiswap"}, observed_identity()),
            "pool_address": (pool_inventory_record(), {**observed_identity(), "pool_address": "0x" + "8" * 40}),
            "chain_id": (pool_inventory_record(), {**observed_identity(), "chain_id": 42161}),
            "factory_address": (pool_inventory_record(), {**observed_identity(), "factory_address": "0x" + "8" * 40}),
            "factory_get_pool_address": (pool_inventory_record(), {**observed_identity(), "factory_get_pool_address": "0x" + "8" * 40}),
            "token0_address": (pool_inventory_record(), {**observed_identity(), "token0_address": "0x" + "8" * 40}),
            "token0_decimals": (pool_inventory_record(), {**observed_identity(), "token0_decimals": 6}),
            "token1_address": (pool_inventory_record(), {**observed_identity(), "token1_address": "0x" + "8" * 40}),
            "token1_decimals": (pool_inventory_record(), {**observed_identity(), "token1_decimals": 18}),
            "fee_pips": (pool_inventory_record(), {**observed_identity(), "fee_pips": 500}),
            "tick_spacing": (pool_inventory_record(), {**observed_identity(), "tick_spacing": 10}),
        }
        for field, (pool, identity) in cases.items():
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, field):
                match_uniswap_v3_execution_authority(pool, identity, authority=authority)
        for field in observed_identity():
            identity = observed_identity()
            del identity[field]
            with self.subTest(missing=field), self.assertRaisesRegex(ValueError, field):
                match_uniswap_v3_execution_authority(pool_inventory_record(), identity, authority=authority)


if __name__ == "__main__":
    unittest.main()
