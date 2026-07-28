import csv
import json
import tempfile
import unittest
from decimal import Decimal, localcontext
from pathlib import Path

from scripts.fetch_dex_depth import (
    DEPTH_BANDS_BPS,
    DEX_DEPTH_COLUMNS,
    Q96,
    SELECTOR_DECIMALS,
    SELECTOR_GET_RESERVES,
    SELECTOR_SYMBOL,
    SELECTOR_TOKEN0,
    SELECTOR_TOKEN1,
    collect_dex_depth,
    decode_int,
    decode_symbol,
    depth_fields,
    encode_signed_word,
    load_pool_inventory,
    protocol_model,
    publish_snapshot,
    v2_band_amounts,
    v3_move_to_price,
)


def word(value):
    return f"{value % (1 << 256):064x}"


def address_result(address):
    return "0x" + ("0" * 24) + address[2:].lower()


def uint_result(*values):
    return "0x" + "".join(word(value) for value in values)


def string_result(value):
    encoded = value.encode("utf-8")
    padded = encoded.hex().ljust(((len(encoded) + 31) // 32) * 64, "0")
    return "0x" + word(32) + word(len(encoded)) + padded


class FakeV2Rpc:
    def __init__(self, chain, url):
        self.chain = chain
        self.url = url
        self.endpoint = "https://rpc.example.test"
        self.records = []

    def block_number(self):
        self.records.append({"request": "block", "response": "0x7b"})
        return 123

    def eth_calls(self, to, data_values, block_tag):
        self.records.append(
            {
                "request": {"to": to, "data": data_values, "block": block_tag},
                "response": "fixture",
            }
        )
        target = "0x1111111111111111111111111111111111111111"
        quote = "0x2222222222222222222222222222222222222222"
        if data_values == [SELECTOR_TOKEN0, SELECTOR_TOKEN1, SELECTOR_GET_RESERVES]:
            return [
                address_result(target),
                address_result(quote),
                uint_result(100 * 10**18, 10_000 * 10**6, 0),
            ]
        if to == target:
            self.assert_token_calls(data_values)
            return [uint_result(18), string_result("AAVE")]
        if to == quote:
            self.assert_token_calls(data_values)
            return [uint_result(6), string_result("USDC")]
        raise AssertionError((to, data_values))

    @staticmethod
    def assert_token_calls(data_values):
        if data_values != [SELECTOR_DECIMALS, SELECTOR_SYMBOL]:
            raise AssertionError(data_values)


class DexDepthMathTest(unittest.TestCase):
    def test_signed_word_and_decode_preserve_negative_int24(self):
        encoded = "0x" + encode_signed_word(-12345, 24)
        self.assertEqual(decode_int(encoded, bits=24), -12345)

    def test_dynamic_symbol_decode(self):
        self.assertEqual(decode_symbol(string_result("WETH")), "WETH")

    def test_v2_depth_is_positive_monotonic_and_fee_aware(self):
        ten = v2_band_amounts(
            Decimal(100 * 10**18),
            Decimal(10_000 * 10**6),
            Decimal(30),
            10,
        )
        hundred = v2_band_amounts(
            Decimal(100 * 10**18),
            Decimal(10_000 * 10**6),
            Decimal(30),
            100,
        )

        self.assertGreater(ten["zero_for_one_gross_input"], 0)
        self.assertGreater(ten["zero_for_one_output"], 0)
        self.assertGreater(
            hundred["zero_for_one_gross_input"],
            ten["zero_for_one_gross_input"],
        )
        self.assertGreater(
            hundred["one_for_zero_gross_input"],
            ten["one_for_zero_gross_input"],
        )

    def test_v3_no_tick_move_returns_complete_monotonic_amounts(self):
        liquidity = 10**24
        with localcontext() as context:
            context.prec = 100
            down_10 = Q96 * (Decimal("0.999")).sqrt()
            down_100 = Q96 * (Decimal("0.99")).sqrt()

        ten_input, ten_output, ten_complete = v3_move_to_price(
            int(Q96),
            down_10,
            liquidity,
            3000,
            {},
            zero_for_one=True,
        )
        hundred_input, hundred_output, hundred_complete = v3_move_to_price(
            int(Q96),
            down_100,
            liquidity,
            3000,
            {},
            zero_for_one=True,
        )

        self.assertTrue(ten_complete)
        self.assertTrue(hundred_complete)
        self.assertGreater(ten_input, 0)
        self.assertGreater(ten_output, 0)
        self.assertGreater(hundred_input, ten_input)
        self.assertGreater(hundred_output, ten_output)

    def test_depth_fields_maps_target_side_to_quote_notional(self):
        amounts = {
            band: {
                "zero_input": Decimal(2 * 10**18),
                "zero_output": Decimal(199 * 10**6),
                "one_input": Decimal(201 * 10**6),
                "one_output": Decimal(2 * 10**18),
                "zero_complete": True,
                "one_complete": True,
            }
            for band in DEPTH_BANDS_BPS
        }
        fields = depth_fields(
            target_position_index=0,
            token0_decimals=18,
            token1_decimals=6,
            token0_price=Decimal(100),
            token1_price=Decimal(1),
            band_amounts=amounts,
        )

        self.assertEqual(fields["sell_depth_10bps_usd"], "199")
        self.assertEqual(fields["buy_depth_10bps_usd"], "201")
        self.assertEqual(fields["total_depth_10bps_usd"], "400")
        self.assertEqual(fields["depth_10bps_complete"], "1")

    def test_protocol_classifier_does_not_guess_unsupported_models(self):
        self.assertEqual(
            protocol_model("uniswap_v3", "eth", "0x" + "1" * 40)[0],
            "concentrated_liquidity_v3",
        )
        self.assertEqual(
            protocol_model("aerodrome-slipstream", "base", "0x" + "1" * 40)[0],
            "concentrated_liquidity_v3",
        )
        self.assertEqual(
            protocol_model(
                "velodrome-finance-slipstream",
                "optimism",
                "0x" + "1" * 40,
            )[0],
            "concentrated_liquidity_v3",
        )
        self.assertEqual(
            protocol_model("shibaswap", "eth", "0x" + "1" * 40)[0],
            "constant_product_v2",
        )
        self.assertEqual(
            protocol_model("pancakeswap_v2", "bsc", "0x" + "1" * 40)[0],
            "constant_product_v2",
        )
        self.assertEqual(
            protocol_model("curve", "eth", "0x" + "1" * 40),
            ("unsupported", "unsupported_pool_model:curve"),
        )
        self.assertEqual(
            protocol_model("velodrome-finance-v2", "optimism", "0x" + "1" * 40),
            ("unsupported", "unsupported_pool_model:velodrome-finance-v2"),
        )
        self.assertEqual(
            protocol_model("orca", "solana", "solana-address")[0],
            "unsupported",
        )


class DexDepthCollectionTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.target = "0x1111111111111111111111111111111111111111"
        self.quote = "0x2222222222222222222222222222222222222222"
        self.pool = {
            "snapshot_id": "tvl-1",
            "observed_at": "2026-07-28T00:00:00+00:00",
            "response_received_at": "2026-07-28T00:00:01+00:00",
            "token_symbol": "AAVE",
            "chain": "eth",
            "dex": "uniswap_v2",
            "pool_address": "0x3333333333333333333333333333333333333333",
            "pool_name": "AAVE / USDC",
            "base_token_id": f"eth_{self.target}",
            "quote_token_id": f"eth_{self.quote}",
            "base_token_price_usd": "100",
            "quote_token_price_usd": "1",
            "status": "observed",
        }

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_inventory_keeps_latest_unique_token_pool_row(self):
        path = self.root / "tvl.csv"
        fields = list(self.pool)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerow(self.pool)
            writer.writerow(
                {
                    **self.pool,
                    "snapshot_id": "tvl-2",
                    "observed_at": "2026-07-28T01:00:00+00:00",
                    "response_received_at": "2026-07-28T01:00:01+00:00",
                    "base_token_price_usd": "101",
                }
            )

        inventory = load_pool_inventory(path)

        self.assertEqual(len(inventory), 1)
        self.assertEqual(inventory[0]["snapshot_id"], "tvl-2")
        self.assertEqual(inventory[0]["base_token_price_usd"], "101")

    def test_collects_fixed_block_v2_depth_and_retains_raw_transcript(self):
        snapshot_id, rows = collect_dex_depth(
            [self.pool],
            raw_root=self.root / "raw",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )

        self.assertTrue(snapshot_id)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["status"], "observed")
        self.assertEqual(row["block_number"], "123")
        self.assertEqual(row["target_token_position"], "token0")
        self.assertGreater(Decimal(row["total_depth_100bps_usd"]), 0)
        self.assertEqual(row["depth_100bps_complete"], "1")
        self.assertEqual(len(row["raw_response_sha256"]), 64)
        manifest = json.loads(
            next((self.root / "raw").glob("*/manifest.json")).read_text()
        )
        self.assertEqual(manifest["status_counts"], {"observed": 1})
        self.assertEqual(manifest["chain_blocks"], {"eth": 123})

    def test_unsupported_pool_stays_null_instead_of_using_tvl_proxy(self):
        unsupported = {
            **self.pool,
            "chain": "solana",
            "dex": "orca",
            "pool_address": "solana-pool-address",
        }
        _snapshot_id, rows = collect_dex_depth(
            [self.pool, unsupported],
            raw_root=self.root / "raw",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )

        row = next(item for item in rows if item["status"] == "unsupported")
        self.assertEqual(row["total_depth_100bps_usd"], "")
        self.assertIn("unsupported_chain", row["error"])

    def test_publish_appends_history_and_replaces_latest(self):
        row = {column: "" for column in DEX_DEPTH_COLUMNS}
        row.update(
            {
                "snapshot_id": "one",
                "observed_at": "2026-07-28T00:00:00+00:00",
                "token_symbol": "AAVE",
                "chain": "eth",
                "pool_address": self.pool["pool_address"],
                "status": "observed",
            }
        )
        publish_snapshot(
            [row],
            output_dir=self.root / "processed",
            publish_dir=self.root / "local",
        )
        second = {
            **row,
            "snapshot_id": "two",
            "observed_at": "2026-07-28T01:00:00+00:00",
        }
        publish_snapshot(
            [second],
            output_dir=self.root / "processed",
            publish_dir=self.root / "local",
        )

        with (self.root / "local/dex_depth_history.csv").open() as handle:
            history = list(csv.DictReader(handle))
        with (self.root / "local/dex_depth_latest.csv").open() as handle:
            latest = list(csv.DictReader(handle))
        self.assertEqual([row["snapshot_id"] for row in history], ["one", "two"])
        self.assertEqual([row["snapshot_id"] for row in latest], ["two"])


if __name__ == "__main__":
    unittest.main()
