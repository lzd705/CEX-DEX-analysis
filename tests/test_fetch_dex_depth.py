import csv
import json
import tempfile
import unittest
from decimal import Decimal, localcontext
from pathlib import Path
from unittest.mock import patch

from scripts.fetch_dex_depth import (
    DEPTH_BANDS_BPS,
    DEX_DEPTH_COLUMNS,
    EXECUTION_CURRENT_FILENAME,
    EXECUTION_LATEST_FILENAME,
    Q96,
    SELECTOR_DECIMALS,
    SELECTOR_FEE,
    SELECTOR_GET_RESERVES,
    SELECTOR_LIQUIDITY,
    SELECTOR_SLOT0,
    SELECTOR_SYMBOL,
    SELECTOR_TICK_BITMAP,
    SELECTOR_TICK_SPACING,
    SELECTOR_TOKEN0,
    SELECTOR_TOKEN1,
    _human_token1_per_token0,
    _quantized_target,
    collect_dex_depth,
    collect_dex_depth_with_execution,
    decimal_text,
    decode_int,
    decode_symbol,
    depth_fields,
    ensure_full_publish_scope,
    encode_signed_word,
    load_pool_inventory,
    protocol_model,
    publish_execution_snapshot,
    publish_snapshot,
    v2_band_amounts,
    v2_exact_input_quote,
    v2_exact_output_quote,
    v3_move_to_price,
)
from scripts.execution_cost import (
    EXECUTION_DIRECTIONS,
    EXECUTION_NOTIONALS_USD,
    RESULT_NUMERIC_COLUMNS,
    validate_execution_snapshot,
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

    def block(self, block_tag):
        self.records.append(
            {
                "request": {"method": "eth_getBlockByNumber", "block": block_tag},
                "response": {"number": "0x7b", "timestamp": "0x65920080"},
            }
        )
        return {"number": "0x7b", "timestamp": "0x65920080"}

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


class PartiallyFailingV2Rpc(FakeV2Rpc):
    def eth_calls(self, to, data_values, block_tag):
        if to == "0x4444444444444444444444444444444444444444":
            raise RuntimeError("fixture pool RPC failure")
        return super().eth_calls(to, data_values, block_tag)


class FakeV2TargetTokenOneRpc(FakeV2Rpc):
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
                address_result(quote),
                address_result(target),
                uint_result(10_000 * 10**6, 100 * 10**18, 0),
            ]
        if to == quote:
            self.assert_token_calls(data_values)
            return [uint_result(6), string_result("USDC")]
        if to == target:
            self.assert_token_calls(data_values)
            return [uint_result(18), string_result("AAVE")]
        raise AssertionError((to, data_values))


class FakeV3Rpc(FakeV2Rpc):
    def eth_calls(self, to, data_values, block_tag):
        self.records.append(
            {
                "request": {"to": to, "data": data_values, "block": block_tag},
                "response": "fixture",
            }
        )
        target = "0x1111111111111111111111111111111111111111"
        quote = "0x2222222222222222222222222222222222222222"
        if data_values == [
            SELECTOR_TOKEN0,
            SELECTOR_TOKEN1,
            SELECTOR_SLOT0,
            SELECTOR_LIQUIDITY,
            SELECTOR_FEE,
            SELECTOR_TICK_SPACING,
        ]:
            return [
                address_result(target),
                address_result(quote),
                uint_result(int(Q96), 0),
                uint_result(10**24),
                uint_result(3000),
                uint_result(60),
            ]
        if data_values and all(
            data.startswith(SELECTOR_TICK_BITMAP)
            for data in data_values
        ):
            return [uint_result(0) for _data in data_values]
        if to == target:
            self.assert_token_calls(data_values)
            return [uint_result(18), string_result("AAVE")]
        if to == quote:
            self.assert_token_calls(data_values)
            return [uint_result(18), string_result("WETH")]
        raise AssertionError((to, data_values))


class DexDepthMathTest(unittest.TestCase):
    def test_filtered_collection_cannot_replace_published_inventory(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            ensure_full_publish_scope(True, {"AAVE"}, set())
        ensure_full_publish_scope(False, {"AAVE"}, {"eth"})

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

    def test_v2_execution_known_answers_include_fee_and_integer_rounding(self):
        reserve_target = Decimal(100 * 10**18)
        reserve_quote = Decimal(10_000 * 10**6)
        target = Decimal(10 * 10**18)

        self.assertEqual(
            v2_exact_input_quote(
                reserve_target,
                reserve_quote,
                Decimal(30),
                target,
            ),
            Decimal(906_610_893),
        )
        self.assertEqual(
            v2_exact_output_quote(
                reserve_quote,
                reserve_target,
                Decimal(30),
                target,
            ),
            Decimal(1_114_454_475),
        )

    def test_v2_120_bit_known_answers_use_exact_integer_arithmetic(self):
        reserve_in = Decimal("664613997892457936451903530263629077")
        reserve_out = Decimal("664613997892457936451903531127826609")
        amount_in = Decimal("1267650600228229401496703217721")
        amount_out = Decimal("1267650600228229401496703259697")

        self.assertEqual(
            v2_exact_input_quote(
                reserve_in,
                reserve_out,
                Decimal(30),
                amount_in,
            ),
            Decimal("1263845245065824951200621566475"),
        )
        self.assertEqual(
            v2_exact_output_quote(
                reserve_in,
                reserve_out,
                Decimal(30),
                amount_out,
            ),
            Decimal("1271467420345516876198184410994"),
        )
        with self.assertRaisesRegex(ValueError, "integer"):
            v2_exact_input_quote(
                reserve_in,
                reserve_out,
                Decimal("30.5"),
                amount_in,
            )

    def test_v2_large_reserve_ratio_preserves_exact_target_floor(self):
        reserve0 = Decimal(
            "1309679745619980403629759256154709103010850614675869368730240"
        )
        reserve1 = Decimal("665075218150391531488264836545")
        ratio = _human_token1_per_token0(
            reserve0,
            reserve1,
            18,
            18,
        )

        target_raw, _target_quantity = _quantized_target(
            Decimal("100000"),
            ratio,
            Decimal(1),
            18,
        )

        self.assertEqual(
            target_raw,
            Decimal(
                "196922048796565792142707047348048557882102964176513414"
            ),
        )

    def test_decimal_text_round_trips_120_bit_values_without_context_rounding(self):
        integer = Decimal("664613997892457936451903530263629077")
        fractional = Decimal(
            "664613997892457936451903530263629077.123450000"
        )

        encoded_integer = decimal_text(integer)
        encoded_fractional = decimal_text(fractional)

        self.assertEqual(
            encoded_integer,
            "664613997892457936451903530263629077",
        )
        self.assertEqual(Decimal(encoded_integer), integer)
        self.assertEqual(
            encoded_fractional,
            "664613997892457936451903530263629077.12345",
        )
        self.assertEqual(Decimal(encoded_fractional), fractional)

    def test_v2_exact_output_rejects_pool_reserve_exhaustion(self):
        self.assertIsNone(
            v2_exact_output_quote(
                Decimal(10_000),
                Decimal(100),
                Decimal(30),
                Decimal(100),
            )
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
        snapshot_id, rows, execution_rows = collect_dex_depth_with_execution(
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
        self.assertEqual(len(execution_rows), 10)
        self.assertEqual(
            {item["direction"] for item in execution_rows},
            set(EXECUTION_DIRECTIONS),
        )
        self.assertEqual(
            {
                Decimal(item["requested_notional_usd"])
                for item in execution_rows
            },
            set(EXECUTION_NOTIONALS_USD),
        )
        sell_1000 = next(
            item
            for item in execution_rows
            if item["direction"] == "sell_token"
            and item["requested_notional_usd"] == "1000"
        )
        buy_1000 = next(
            item
            for item in execution_rows
            if item["direction"] == "buy_token"
            and item["requested_notional_usd"] == "1000"
        )
        self.assertEqual(sell_1000["target_token_quantity"], "10")
        self.assertEqual(sell_1000["quote_amount"], "906.610893")
        self.assertEqual(buy_1000["quote_amount"], "1114.454475")
        self.assertEqual(sell_1000["fee_status"], "included_protocol_fee")
        self.assertEqual(sell_1000["block_timestamp"], "2024-01-01T00:00:00+00:00")
        self.assertEqual(
            sell_1000["raw_response_sha256"],
            row["raw_response_sha256"],
        )
        validate_execution_snapshot(
            [sell_1000["market_id"]],
            execution_rows,
        )
        sell_costs = [
            Decimal(item["quoted_execution_cost_bps"])
            for item in execution_rows
            if item["direction"] == "sell_token"
            and item["status"] == "observed"
        ]
        self.assertEqual(sell_costs, sorted(sell_costs))
        buy_statuses = [
            item["status"]
            for item in execution_rows
            if item["direction"] == "buy_token"
        ]
        self.assertNotIn(
            "observed",
            buy_statuses[buy_statuses.index("partial") :]
            if "partial" in buy_statuses
            else [],
        )
        manifest = json.loads(
            next((self.root / "raw").glob("*/manifest.json")).read_text()
        )
        self.assertEqual(manifest["status_counts"], {"observed": 1})
        self.assertEqual(manifest["chain_blocks"], {"eth": 123})
        self.assertEqual(
            manifest["chain_block_timestamps"],
            {"eth": "2024-01-01T00:00:00+00:00"},
        )
        self.assertEqual(manifest["execution_row_count"], 10)

    def test_v3_depth_succeeds_but_execution_is_explicitly_unsupported(self):
        v3_pool = {
            **self.pool,
            "dex": "uniswap_v3",
            "pool_name": "AAVE / WETH",
            "base_token_price_usd": "1",
            "quote_token_price_usd": "1",
        }

        _snapshot_id, depth_rows, execution_rows = (
            collect_dex_depth_with_execution(
                [v3_pool],
                raw_root=self.root / "raw",
                sleep_seconds=0,
                rpc_factory=FakeV3Rpc,
            )
        )

        self.assertEqual(depth_rows[0]["status"], "observed")
        self.assertEqual(
            depth_rows[0]["protocol_model"],
            "concentrated_liquidity_v3",
        )
        self.assertGreater(
            Decimal(depth_rows[0]["total_depth_100bps_usd"]),
            0,
        )
        self.assertEqual(len(execution_rows), 10)
        self.assertTrue(
            all(row["status"] == "unsupported" for row in execution_rows)
        )
        self.assertTrue(
            all(
                row["status_reason"]
                == "exact_integer_swap_math_not_implemented"
                for row in execution_rows
            )
        )
        self.assertTrue(
            all(
                row.get(column, "") == ""
                for row in execution_rows
                for column in RESULT_NUMERIC_COLUMNS
            )
        )
        self.assertTrue(
            all(
                row["raw_response_sha256"]
                == depth_rows[0]["raw_response_sha256"]
                for row in execution_rows
            )
        )
        validate_execution_snapshot(
            {row["market_id"] for row in execution_rows},
            execution_rows,
        )

    def test_execution_calculation_failure_does_not_erase_observed_depth(self):
        with patch(
            "scripts.fetch_dex_depth.v2_execution_rows",
            side_effect=RuntimeError("execution math bug"),
        ):
            _snapshot_id, depth_rows, execution_rows = (
                collect_dex_depth_with_execution(
                    [self.pool],
                    raw_root=self.root / "raw",
                    sleep_seconds=0,
                    rpc_factory=FakeV2Rpc,
                )
            )

        self.assertEqual(depth_rows[0]["status"], "observed")
        self.assertGreater(
            Decimal(depth_rows[0]["total_depth_100bps_usd"]),
            0,
        )
        self.assertEqual(len(execution_rows), 10)
        self.assertTrue(
            all(row["status"] == "failed" for row in execution_rows)
        )
        self.assertTrue(
            all(
                row["status_reason"] == "execution_calculation_failed"
                for row in execution_rows
            )
        )
        self.assertTrue(
            all(
                row["error"] == "RuntimeError: execution math bug"
                for row in execution_rows
            )
        )
        self.assertTrue(
            all(row["quote_amount"] == "" for row in execution_rows)
        )
        self.assertTrue(
            all(
                row["raw_response_sha256"]
                == depth_rows[0]["raw_response_sha256"]
                for row in execution_rows
            )
        )
        validate_execution_snapshot(
            {row["market_id"] for row in execution_rows},
            execution_rows,
        )

    def test_unsupported_pool_stays_null_instead_of_using_tvl_proxy(self):
        unsupported = {
            **self.pool,
            "chain": "solana",
            "dex": "orca",
            "pool_address": "solana-pool-address",
        }
        _snapshot_id, rows, execution_rows = collect_dex_depth_with_execution(
            [self.pool, unsupported],
            raw_root=self.root / "raw",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )

        row = next(item for item in rows if item["status"] == "unsupported")
        self.assertEqual(row["total_depth_100bps_usd"], "")
        self.assertIn("unsupported_chain", row["error"])
        unsupported_execution = [
            item
            for item in execution_rows
            if item["market_id"].startswith("dex:solana:")
        ]
        self.assertEqual(len(unsupported_execution), 10)
        self.assertTrue(
            all(item["status"] == "unsupported" for item in unsupported_execution)
        )
        self.assertTrue(
            all(item["quoted_execution_cost_bps"] == "" for item in unsupported_execution)
        )

    def test_supported_pool_failure_publishes_ten_failed_scenarios(self):
        failed_pool = {
            **self.pool,
            "token_symbol": "COMP",
            "pool_address": "0x4444444444444444444444444444444444444444",
        }
        _snapshot_id, depth_rows, execution_rows = collect_dex_depth_with_execution(
            [self.pool, failed_pool],
            raw_root=self.root / "raw",
            sleep_seconds=0,
            rpc_factory=PartiallyFailingV2Rpc,
        )

        failed_depth = next(row for row in depth_rows if row["token_symbol"] == "COMP")
        self.assertEqual(failed_depth["status"], "failed")
        failed_execution = [
            row for row in execution_rows if row["token_symbol"] == "COMP"
        ]
        self.assertEqual(len(failed_execution), 10)
        self.assertTrue(all(row["status"] == "failed" for row in failed_execution))
        self.assertTrue(
            all(row["quote_amount"] == "" for row in failed_execution)
        )
        self.assertTrue(
            all(len(row["raw_response_sha256"]) == 64 for row in failed_execution)
        )

    def test_execution_direction_mapping_is_correct_when_target_is_token1(self):
        _snapshot_id, depth_rows, execution_rows = collect_dex_depth_with_execution(
            [self.pool],
            raw_root=self.root / "raw",
            sleep_seconds=0,
            rpc_factory=FakeV2TargetTokenOneRpc,
        )

        self.assertEqual(depth_rows[0]["target_token_position"], "token1")
        sell = next(
            row
            for row in execution_rows
            if row["direction"] == "sell_token"
            and row["requested_notional_usd"] == "1000"
        )
        buy = next(
            row
            for row in execution_rows
            if row["direction"] == "buy_token"
            and row["requested_notional_usd"] == "1000"
        )
        self.assertEqual(sell["quote_token_address"], self.quote)
        self.assertEqual(sell["quote_amount"], "906.610893")
        self.assertEqual(buy["quote_amount"], "1114.454475")
        self.assertLess(
            Decimal(sell["filled_vwap_usd_per_token"]),
            Decimal(sell["reference_price_usd_per_token"]),
        )
        self.assertGreater(
            Decimal(buy["filled_vwap_usd_per_token"]),
            Decimal(buy["reference_price_usd_per_token"]),
        )

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

    def test_execution_publication_writes_current_and_replaces_latest_only(self):
        _snapshot_id, _rows, execution_rows = collect_dex_depth_with_execution(
            [self.pool],
            raw_root=self.root / "raw",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )
        first = publish_execution_snapshot(
            execution_rows,
            expected_market_ids={row["market_id"] for row in execution_rows},
            output_dir=self.root / "processed",
            publish_dir=self.root / "local",
        )
        second_rows = [
            {
                **row,
                "snapshot_id": "second",
                "source_snapshot_id": "second",
                "observed_at": "2026-07-28T02:00:00+00:00",
            }
            for row in execution_rows
        ]
        second = publish_execution_snapshot(
            second_rows,
            expected_market_ids={row["market_id"] for row in second_rows},
            output_dir=self.root / "processed",
            publish_dir=self.root / "local",
        )

        self.assertTrue(
            Path(first["execution_current_path"]).name
            == EXECUTION_CURRENT_FILENAME
        )
        self.assertEqual(
            Path(second["execution_latest_path"]).name,
            EXECUTION_LATEST_FILENAME,
        )
        with Path(second["execution_latest_path"]).open() as handle:
            latest = list(csv.DictReader(handle))
        self.assertEqual(len(latest), 10)
        self.assertEqual({row["snapshot_id"] for row in latest}, {"second"})
        self.assertNotIn("execution_history_path", second)
        self.assertNotIn("execution_history_row_count", second)
        self.assertFalse(
            (self.root / "local/dex_execution_cost_history.csv").exists()
        )
        self.assertFalse(
            (self.root / "local" / EXECUTION_CURRENT_FILENAME).exists()
        )

    def test_execution_publisher_rejects_incomplete_inventory(self):
        _snapshot_id, _rows, execution_rows = collect_dex_depth_with_execution(
            [self.pool],
            raw_root=self.root / "raw",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )
        with self.assertRaisesRegex(ValueError, "expected 10"):
            publish_execution_snapshot(
                execution_rows[:-1],
                expected_market_ids={execution_rows[0]["market_id"]},
                output_dir=self.root / "processed",
            )
        self.assertFalse(
            (self.root / "processed" / EXECUTION_CURRENT_FILENAME).exists()
        )


if __name__ == "__main__":
    unittest.main()
