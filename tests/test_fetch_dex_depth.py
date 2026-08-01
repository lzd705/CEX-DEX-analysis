import csv
import json
import tempfile
import unittest
from decimal import Decimal, localcontext
from pathlib import Path
from unittest.mock import patch

from scripts.fetch_dex_depth import (
    CURRENT_FILENAME,
    DEPTH_BANDS_BPS,
    DEX_DEPTH_COLUMNS,
    EXECUTION_CURRENT_FILENAME,
    EXECUTION_LATEST_FILENAME,
    HISTORY_FILENAME,
    LATEST_FILENAME,
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
    execution_publication_coverage_gate,
    load_pool_inventory,
    merge_exact_publication_bundle,
    preflight_publication_bundle,
    publish_exact_publication_bundle,
    protocol_model,
    publish_execution_snapshot,
    publish_full_publication_bundle,
    publish_snapshot,
    terminal_execution_rows,
    unsupported_row,
    validate_snapshot as validate_depth_snapshot,
    v2_band_amounts,
    v2_exact_input_quote,
    v2_exact_output_quote,
    v2_execution_rows,
    v3_move_to_price,
)
from scripts.publication_gate import CoverageRegressionError
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


def write_snapshot_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


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


class FixedBlockV2Rpc(FakeV2Rpc):
    instances = []

    def __init__(self, chain, url):
        super().__init__(chain, url)
        self.next_rpc_id = 1
        self.rpc_ids = []
        type(self).instances.append(self)

    def block_number(self):
        raise AssertionError("a supplied fixed block must not query the head")

    def block(self, block_tag):
        self.rpc_ids.append(self.next_rpc_id)
        self.next_rpc_id += 1
        self.records.append(
            {
                "request": {
                    "id": self.rpc_ids[-1],
                    "method": "eth_getBlockByNumber",
                    "block": block_tag,
                },
                "response": {"number": block_tag, "timestamp": "0x65920080"},
            }
        )
        return {"number": block_tag, "timestamp": "0x65920080"}

    def eth_calls(self, to, data_values, block_tag):
        self.rpc_ids.append(self.next_rpc_id)
        self.next_rpc_id += 1
        return super().eth_calls(to, data_values, block_tag)


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

    def test_v2_zero_exact_input_output_is_not_published_as_observed(self):
        with self.assertRaisesRegex(ValueError, "zero quote output"):
            v2_execution_rows(
                {},
                common={},
                target_position_index=0,
                token0_decimals=1,
                token1_decimals=0,
                token0_price=Decimal(2000),
                token1_price=Decimal(2000),
                reserve0=Decimal(10_000),
                reserve1=Decimal(1_000),
                fee_bps=Decimal(30),
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
            "observed_at": "2024-01-01T00:00:00+00:00",
            "response_received_at": "2024-01-01T00:00:01+00:00",
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
                    "observed_at": "2024-01-01T01:00:00+00:00",
                    "response_received_at": "2024-01-01T01:00:01+00:00",
                    "base_token_price_usd": "101",
                }
            )

        inventory = load_pool_inventory(path)

        self.assertEqual(len(inventory), 1)
        self.assertEqual(inventory[0]["snapshot_id"], "tvl-2")
        self.assertEqual(inventory[0]["base_token_price_usd"], "101")

    def test_exact_candidate_accepts_only_structural_unsupported_outcome(self):
        terminal = unsupported_row(
            self.pool,
            snapshot_id="candidate-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            reason="unsupported_protocol:fixture",
        )
        with self.assertRaisesRegex(ValueError, "no measured"):
            validate_depth_snapshot([self.pool], [terminal])
        validate_depth_snapshot(
            [self.pool],
            [terminal],
            allow_terminal_only=True,
        )

        retryable = {
            **terminal,
            "status": "failed",
            "error": "RpcError: temporary source failure",
        }
        with self.assertRaisesRegex(ValueError, "terminal non-retryable"):
            validate_depth_snapshot(
                [self.pool],
                [retryable],
                allow_terminal_only=True,
            )

        partial = {
            **terminal,
            "status": "partial",
            "error": "depth_truncated: fixture measurement limit",
        }
        with self.assertRaisesRegex(ValueError, "resolved exact candidate"):
            validate_depth_snapshot(
                [self.pool],
                [partial],
                allow_terminal_only=True,
            )

    def test_validate_requires_canonical_utc_observed_at_for_every_pool(self):
        terminal = unsupported_row(
            self.pool,
            snapshot_id="candidate-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            reason="unsupported_protocol:fixture",
        )
        for observed_at in (
            "",
            "2026-07-27T00:00:01",
            "2026-07-27T00:00:01Z",
            "2026-07-27T08:00:01+08:00",
            " 2026-07-27T00:00:01+00:00",
        ):
            with self.subTest(observed_at=observed_at):
                with self.assertRaisesRegex(ValueError, "observed_at"):
                    validate_depth_snapshot(
                        [self.pool],
                        [{**terminal, "observed_at": observed_at}],
                        allow_terminal_only=True,
                    )

    def test_exact_pool_refresh_merges_without_collecting_other_pools(self):
        other_pool = {
            **self.pool,
            "pool_address": "0x4444444444444444444444444444444444444444",
            "pool_name": "AAVE / USDC second pool",
        }
        _baseline_id, baseline_depth, baseline_execution = (
            collect_dex_depth_with_execution(
                [self.pool, other_pool],
                raw_root=self.root / "raw-baseline",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        _candidate_id, candidate_depth, candidate_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-candidate",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        published = self.root / "local"
        publish_snapshot(
            baseline_depth,
            output_dir=self.root / "processed",
            publish_dir=published,
        )
        publish_execution_snapshot(
            baseline_execution,
            expected_market_ids={
                row["market_id"] for row in baseline_execution
            },
            output_dir=self.root / "processed",
            publish_dir=published,
        )

        merged_depth, merged_execution = merge_exact_publication_bundle(
            candidate_depth,
            candidate_execution,
            target_market_id=(
                "dex:eth:uniswap_v2:"
                "0x3333333333333333333333333333333333333333:AAVE"
            ),
            publish_dir=published,
        )

        self.assertEqual(len(merged_depth), 2)
        self.assertEqual(len(merged_execution), 20)
        self.assertEqual(
            {row["snapshot_id"] for row in merged_depth},
            {candidate_depth[0]["snapshot_id"]},
        )
        other_after = next(
            row
            for row in merged_depth
            if row["pool_address"] == other_pool["pool_address"]
        )
        other_before = next(
            row
            for row in baseline_depth
            if row["pool_address"] == other_pool["pool_address"]
        )
        self.assertEqual(other_after["observed_at"], other_before["observed_at"])
        self.assertEqual(
            {row["source_snapshot_id"] for row in merged_execution},
            {candidate_depth[0]["snapshot_id"]},
        )

        reports = preflight_publication_bundle(
            merged_depth,
            merged_execution,
            published,
            target_market_id=(
                "dex:eth:uniswap_v2:"
                "0x3333333333333333333333333333333333333333:AAVE"
            ),
        )
        protected = [
            published / HISTORY_FILENAME,
            published / LATEST_FILENAME,
            published / CURRENT_FILENAME,
            published / EXECUTION_LATEST_FILENAME,
        ]
        originals = {path: path.read_bytes() for path in protected}
        from scripts import atomic_publication

        real_replace = atomic_publication.os.replace
        for fail_at in range(1, len(protected) + 1):
            calls = {"count": 0}

            def fail_once(source, destination):
                calls["count"] += 1
                if calls["count"] == fail_at:
                    raise OSError("injected publication failure")
                return real_replace(source, destination)

            with self.subTest(fail_at=fail_at), patch(
                "scripts.atomic_publication.os.replace",
                side_effect=fail_once,
            ):
                with self.assertRaises(OSError):
                    publish_exact_publication_bundle(
                        merged_depth,
                        merged_execution,
                        target_market_id=(
                            "dex:eth:uniswap_v2:"
                            "0x3333333333333333333333333333333333333333:AAVE"
                        ),
                        history_rows_to_append=candidate_depth,
                        output_dir=self.root / "processed",
                        publish_dir=published,
                        preflight_reports=reports,
                    )
            self.assertEqual(
                {path: path.read_bytes() for path in protected},
                originals,
            )

        publish_exact_publication_bundle(
            merged_depth,
            merged_execution,
            target_market_id=(
                "dex:eth:uniswap_v2:"
                "0x3333333333333333333333333333333333333333:AAVE"
            ),
            history_rows_to_append=candidate_depth,
            output_dir=self.root / "processed",
            publish_dir=published,
            preflight_reports=reports,
        )
        with (published / HISTORY_FILENAME).open(
            newline="",
            encoding="utf-8",
        ) as handle:
            history = list(csv.DictReader(handle))
        self.assertEqual(len(history), 3)

    def test_exact_publication_bundle_rejects_resolved_private_public_path_overlap_before_write(self):
        other_pool = {
            **self.pool,
            "pool_address": "0x4444444444444444444444444444444444444444",
            "pool_name": "AAVE / USDC second pool",
        }
        _baseline_id, baseline_depth, baseline_execution = (
            collect_dex_depth_with_execution(
                [self.pool, other_pool],
                raw_root=self.root / "raw-exact-overlap-baseline",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        _candidate_id, candidate_depth, candidate_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-exact-overlap-candidate",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        target_market_id = (
            "dex:eth:uniswap_v2:"
            "0x3333333333333333333333333333333333333333:AAVE"
        )

        for alias in ("same-directory", "dotdot-alias"):
            with self.subTest(alias=alias):
                published = self.root / f"exact-overlap-{alias}-local"
                publish_snapshot(
                    baseline_depth,
                    output_dir=self.root / f"exact-overlap-{alias}-processed",
                    publish_dir=published,
                )
                publish_execution_snapshot(
                    baseline_execution,
                    expected_market_ids={
                        row["market_id"] for row in baseline_execution
                    },
                    output_dir=self.root / f"exact-overlap-{alias}-processed",
                    publish_dir=published,
                )
                merged_depth, merged_execution = (
                    merge_exact_publication_bundle(
                        candidate_depth,
                        candidate_execution,
                        target_market_id=target_market_id,
                        publish_dir=published,
                    )
                )
                reports = preflight_publication_bundle(
                    merged_depth,
                    merged_execution,
                    published,
                    target_market_id=target_market_id,
                )
                protected = [
                    published / HISTORY_FILENAME,
                    published / LATEST_FILENAME,
                    published / CURRENT_FILENAME,
                    published / EXECUTION_LATEST_FILENAME,
                ]
                originals = {path: path.read_bytes() for path in protected}
                output_dir = (
                    published
                    if alias == "same-directory"
                    else published / ".." / published.name
                )

                with self.assertRaisesRegex(ValueError, "overlap"):
                    publish_exact_publication_bundle(
                        merged_depth,
                        merged_execution,
                        target_market_id=target_market_id,
                        history_rows_to_append=candidate_depth,
                        output_dir=output_dir,
                        publish_dir=published,
                        preflight_reports=reports,
                    )
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    originals,
                )

    def test_full_publication_bundle_restores_every_public_destination_on_each_replace_failure(self):
        _baseline_id, baseline_depth, baseline_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-full-baseline",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        _candidate_id, candidate_depth, candidate_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-full-candidate",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        published = self.root / "full-local"
        publish_snapshot(
            baseline_depth,
            output_dir=self.root / "full-processed",
            publish_dir=published,
        )
        publish_execution_snapshot(
            baseline_execution,
            expected_market_ids={
                row["market_id"] for row in baseline_execution
            },
            output_dir=self.root / "full-processed",
            publish_dir=published,
        )
        reports = preflight_publication_bundle(
            candidate_depth,
            candidate_execution,
            published,
        )
        protected = [
            published / HISTORY_FILENAME,
            published / LATEST_FILENAME,
            published / CURRENT_FILENAME,
            published / EXECUTION_LATEST_FILENAME,
        ]
        originals = {path: path.read_bytes() for path in protected}
        from scripts import atomic_publication

        real_replace = atomic_publication.os.replace
        for fail_at, failed_path in enumerate(protected, start=1):
            public_calls = {"count": 0}
            failed_destination = {"path": None}

            def fail_public_replace(source, destination):
                if Path(destination) in protected:
                    public_calls["count"] += 1
                    if public_calls["count"] == fail_at:
                        failed_destination["path"] = Path(destination)
                        raise OSError("injected full publication failure")
                return real_replace(source, destination)

            with self.subTest(
                fail_at=fail_at,
                destination=failed_path.name,
            ), patch(
                "scripts.atomic_publication.os.replace",
                side_effect=fail_public_replace,
            ):
                with self.assertRaisesRegex(
                    OSError,
                    "injected full publication failure",
                ):
                    publish_full_publication_bundle(
                        candidate_depth,
                        candidate_execution,
                        output_dir=self.root / "full-processed",
                        publish_dir=published,
                        preflight_reports=reports,
                    )
            self.assertEqual(failed_destination["path"], failed_path)
            self.assertEqual(
                {path: path.read_bytes() for path in protected},
                originals,
            )

    def test_full_publication_bundle_rejects_resolved_private_public_path_overlap_before_write(self):
        _baseline_id, baseline_depth, baseline_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-overlap-baseline",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        _candidate_id, candidate_depth, candidate_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-overlap-candidate",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )

        for alias in ("same-directory", "dotdot-alias"):
            with self.subTest(alias=alias):
                published = self.root / f"overlap-{alias}-local"
                publish_snapshot(
                    baseline_depth,
                    output_dir=self.root / f"overlap-{alias}-processed",
                    publish_dir=published,
                )
                publish_execution_snapshot(
                    baseline_execution,
                    expected_market_ids={
                        row["market_id"] for row in baseline_execution
                    },
                    output_dir=self.root / f"overlap-{alias}-processed",
                    publish_dir=published,
                )
                reports = preflight_publication_bundle(
                    candidate_depth,
                    candidate_execution,
                    published,
                )
                protected = [
                    published / HISTORY_FILENAME,
                    published / LATEST_FILENAME,
                    published / CURRENT_FILENAME,
                    published / EXECUTION_LATEST_FILENAME,
                ]
                originals = {path: path.read_bytes() for path in protected}
                output_dir = (
                    published
                    if alias == "same-directory"
                    else published / ".." / published.name
                )

                with self.assertRaisesRegex(ValueError, "overlap"):
                    publish_full_publication_bundle(
                        candidate_depth,
                        candidate_execution,
                        output_dir=output_dir,
                        publish_dir=published,
                        preflight_reports=reports,
                    )
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    originals,
                )

    def test_exact_preflight_accepts_one_observed_repair_below_full_coverage_floor(self):
        other_pool = {
            **self.pool,
            "token_symbol": "COMP",
            "pool_address": "0x4444444444444444444444444444444444444444",
            "pool_name": "COMP / USDC",
        }
        pools = [self.pool, other_pool]
        baseline_depth = []
        baseline_execution = []
        for pool in pools:
            failed_depth = unsupported_row(
                pool,
                snapshot_id="baseline-low",
                request_started_at="2026-07-27T00:00:00+00:00",
                response_received_at="2026-07-27T00:00:01+00:00",
                reason="pool_state_collection_failed",
            )
            failed_depth.update(
                {
                    "protocol_model": "constant_product_v2",
                    "status": "failed",
                    "error": "RuntimeError: temporary RPC failure",
                }
            )
            baseline_depth.append(failed_depth)
            baseline_execution.extend(
                terminal_execution_rows(
                    pool,
                    snapshot_id="baseline-low",
                    request_started_at="2026-07-27T00:00:00+00:00",
                    response_received_at="2026-07-27T00:00:01+00:00",
                    protocol="constant_product_v2",
                    status="failed",
                    status_reason="pool_state_collection_failed",
                    error="RuntimeError: temporary RPC failure",
                )
            )
        _snapshot_id, candidate_depth, candidate_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-low-coverage-repair",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        published = self.root / "low-coverage-local"
        write_snapshot_rows(
            published / LATEST_FILENAME,
            baseline_depth,
        )
        write_snapshot_rows(
            published / EXECUTION_LATEST_FILENAME,
            baseline_execution,
        )
        target_market_id = (
            "dex:eth:uniswap_v2:"
            "0x3333333333333333333333333333333333333333:AAVE"
        )
        merged_depth, merged_execution = merge_exact_publication_bundle(
            candidate_depth,
            candidate_execution,
            target_market_id=target_market_id,
            publish_dir=published,
        )

        try:
            reports = preflight_publication_bundle(
                merged_depth,
                merged_execution,
                published,
                target_market_id=target_market_id,
            )
        except (CoverageRegressionError, TypeError) as error:
            self.fail(
                "exact DEX repair was rejected by the full-publication "
                f"coverage boundary: {error}"
            )

        self.assertTrue(reports["dex_depth"]["passed"])
        self.assertTrue(reports["dex_execution_cost"]["passed"])
        self.assertEqual(
            [row["status"] for row in merged_depth],
            ["observed", "failed"],
        )

    def test_exact_preflight_accepts_confirmed_terminal_on_all_failed_baseline(self):
        target_pool = {
            **self.pool,
            "chain": "solana",
            "dex": "orca",
            "pool_address": "solana-pool-address",
        }
        terminal_reason = "unsupported_chain:solana"
        candidate_depth = [
            unsupported_row(
                target_pool,
                snapshot_id="candidate-terminal",
                request_started_at="2026-07-27T01:00:00+00:00",
                response_received_at="2026-07-27T01:00:01+00:00",
                reason=terminal_reason,
            )
        ]
        candidate_execution = terminal_execution_rows(
            target_pool,
            snapshot_id="candidate-terminal",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            protocol="unsupported",
            status="unsupported",
            status_reason="unsupported_protocol_or_chain",
            error=terminal_reason,
        )
        baseline_depth = [
            {
                **candidate_depth[0],
                "snapshot_id": "baseline-failed",
                "status": "failed",
                "error": "RuntimeError: temporary RPC failure",
            }
        ]
        baseline_execution = [
            {
                **row,
                "snapshot_id": "baseline-failed",
                "source_snapshot_id": "baseline-failed",
                "status": "failed",
                "status_reason": "pool_state_collection_failed",
                "error": "RuntimeError: temporary RPC failure",
            }
            for row in candidate_execution
        ]
        validate_depth_snapshot(
            [target_pool],
            candidate_depth,
            allow_terminal_only=True,
        )
        published = self.root / "terminal-local"
        write_snapshot_rows(
            published / LATEST_FILENAME,
            baseline_depth,
        )
        write_snapshot_rows(
            published / EXECUTION_LATEST_FILENAME,
            baseline_execution,
        )
        target_market_id = "dex:solana:orca:solana-pool-address:AAVE"
        try:
            merged_depth, merged_execution = merge_exact_publication_bundle(
                candidate_depth,
                candidate_execution,
                target_market_id=target_market_id,
                publish_dir=published,
            )
            reports = preflight_publication_bundle(
                merged_depth,
                merged_execution,
                published,
                target_market_id=target_market_id,
            )
        except (CoverageRegressionError, TypeError, ValueError) as error:
            self.fail(
                "resolver-confirmed terminal DEX refresh was rejected: "
                f"{error}"
            )

        self.assertTrue(reports["dex_depth"]["passed"])
        self.assertTrue(reports["dex_execution_cost"]["passed"])
        self.assertEqual(merged_depth[0]["status"], "unsupported")

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
        self.assertEqual(row["block_timestamp"], "2024-01-01T00:00:00+00:00")
        self.assertEqual(row["usd_price_source_snapshot_id"], "tvl-1")
        self.assertEqual(
            row["usd_price_observed_at"],
            "2024-01-01T00:00:01+00:00",
        )
        self.assertEqual(row["usd_price_skew_seconds"], "1")
        self.assertEqual(row["usd_price_freshness_status"], "current")
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

    def test_stale_usd_price_fails_only_affected_pool_without_numeric_facts(self):
        stale_pool = {
            **self.pool,
            "pool_address": "0x5555555555555555555555555555555555555555",
            "response_received_at": "2023-12-31T21:59:59+00:00",
        }
        _snapshot_id, depth_rows, execution_rows = (
            collect_dex_depth_with_execution(
                [self.pool, stale_pool],
                raw_root=self.root / "raw",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )

        stale_depth = next(
            row
            for row in depth_rows
            if row["pool_address"] == stale_pool["pool_address"]
        )
        self.assertEqual(stale_depth["status"], "failed")
        self.assertEqual(stale_depth["usd_price_freshness_status"], "stale")
        self.assertEqual(stale_depth["total_depth_100bps_usd"], "")
        stale_execution = [
            row
            for row in execution_rows
            if row["pool_address"] == stale_pool["pool_address"]
        ]
        self.assertEqual(len(stale_execution), 10)
        self.assertTrue(
            all(row["status"] == "failed" for row in stale_execution)
        )
        self.assertTrue(
            all(
                row["status_reason"]
                == "usd_price_conversion_stale_or_unavailable"
                for row in stale_execution
            )
        )
        self.assertTrue(
            all(
                row[field] == ""
                for row in stale_execution
                for field in RESULT_NUMERIC_COLUMNS
            )
        )

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

    def test_execution_gate_excludes_only_structural_v3_unsupported(self):
        base = {
            "market_id": "dex:eth:0x" + "1" * 40,
            "direction": "sell_token",
            "requested_notional_usd": "100",
            "chain": "eth",
            "pool_address": "0x" + "1" * 40,
            "status": "unsupported",
        }
        with self.subTest("supported V2 failure cannot be excluded"):
            with self.assertRaises(CoverageRegressionError) as raised:
                execution_publication_coverage_gate(
                    [{**base, "dex": "uniswap_v2"}],
                    self.root / "v2-gate",
                )
            self.assertEqual(
                raised.exception.report["candidate"]["eligible_count"],
                1,
            )
            self.assertEqual(
                raised.exception.report["candidate"]["usable_count"],
                0,
            )

        with self.subTest("all V3 execution can be structurally unsupported"):
            report = execution_publication_coverage_gate(
                [{**base, "dex": "uniswap_v3"}],
                self.root / "v3-gate",
            )
            self.assertTrue(report["passed"])
            self.assertEqual(report["candidate"]["eligible_count"], 0)
            self.assertEqual(
                report["candidate"]["absolute_check"],
                "skipped",
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

    def test_supported_pools_marked_unsupported_cannot_replace_latest(self):
        baseline = []
        for index in range(10):
            row = {column: "" for column in DEX_DEPTH_COLUMNS}
            row.update(
                {
                    "snapshot_id": "healthy",
                    "observed_at": "2026-07-28T00:00:00+00:00",
                    "token_symbol": f"T{index}",
                    "chain": "eth",
                    "dex": "uniswap_v2",
                    "pool_address": "0x{:040x}".format(index + 1),
                    "protocol_model": "constant_product_v2",
                    "status": "observed",
                }
            )
            baseline.append(row)
        degraded = [
            {
                **row,
                "snapshot_id": "degraded",
                "observed_at": "2026-07-28T01:00:00+00:00",
                "protocol_model": (
                    "unsupported" if index < 2 else row["protocol_model"]
                ),
                "status": "unsupported" if index < 2 else "observed",
                "error": (
                    "missing_rpc_endpoint:eth" if index < 2 else ""
                ),
            }
            for index, row in enumerate(baseline)
        ]
        published = self.root / "coverage-local"
        output = self.root / "coverage-processed"
        publish_snapshot(
            baseline,
            output_dir=output,
            publish_dir=published,
        )
        protected_paths = [
            published / CURRENT_FILENAME,
            published / LATEST_FILENAME,
            published / HISTORY_FILENAME,
        ]
        before = {path: path.read_bytes() for path in protected_paths}

        with self.assertRaises(CoverageRegressionError) as raised:
            publish_snapshot(
                degraded,
                output_dir=output,
                publish_dir=published,
            )

        self.assertEqual(raised.exception.report["fact_family"], "dex_depth")
        self.assertEqual(
            raised.exception.report["candidate"]["eligible_count"],
            10,
        )
        self.assertEqual(
            {path: path.read_bytes() for path in protected_paths},
            before,
        )
        with (output / CURRENT_FILENAME).open(
            newline="",
            encoding="utf-8",
        ) as handle:
            processed = list(csv.DictReader(handle))
        self.assertEqual(
            {row["snapshot_id"] for row in processed},
            {"degraded"},
        )

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

    def test_one_pool_primitive_matches_full_collector_rows_and_transcript_hash(self):
        from scripts.fetch_dex_depth import collect_dex_pool_observation

        timestamp = "2026-08-01T12:00:00+00:00"
        with patch(
            "scripts.fetch_dex_depth.utc_now_text",
            return_value=timestamp,
        ):
            snapshot_id, full_depth, full_execution = (
                collect_dex_depth_with_execution(
                    [self.pool],
                    raw_root=self.root / "full",
                    sleep_seconds=0,
                    rpc_factory=FakeV2Rpc,
                )
            )
            one_depth, one_execution = collect_dex_pool_observation(
                self.pool,
                snapshot_id=snapshot_id,
                raw_path=self.root / "one.json",
                rpc_factory=FakeV2Rpc,
            )

        full_raw = (
            self.root
            / "full"
            / snapshot_id
            / "001-eth-AAVE-uniswap_v2.json"
        ).read_bytes()
        one_raw = (self.root / "one.json").read_bytes()
        self.assertEqual(one_depth, full_depth[0])
        self.assertEqual(one_execution, full_execution)
        self.assertEqual(len(one_execution), 10)
        self.assertEqual(one_raw, full_raw)
        self.assertEqual(
            one_depth["raw_response_sha256"],
            full_depth[0]["raw_response_sha256"],
        )

    def test_one_pool_fixed_block_and_client_transcript_are_isolated(self):
        from scripts.fetch_dex_depth import collect_dex_pool_observation

        FixedBlockV2Rpc.instances = []
        rows = []
        for index in range(2):
            row, execution_rows = collect_dex_pool_observation(
                self.pool,
                snapshot_id="route-cohort-1",
                raw_path=self.root / f"isolated-{index}.json",
                rpc_factory=FixedBlockV2Rpc,
                fixed_block_number=456,
            )
            rows.append(row)
            self.assertEqual(len(execution_rows), 10)

        self.assertEqual([row["block_number"] for row in rows], ["456", "456"])
        self.assertEqual(len(FixedBlockV2Rpc.instances), 2)
        first, second = FixedBlockV2Rpc.instances
        self.assertIsNot(first.records, second.records)
        self.assertEqual(first.rpc_ids[0], 1)
        self.assertEqual(second.rpc_ids[0], 1)
        for client in (first, second):
            block_requests = [
                record["request"]["block"]
                for record in client.records
                if isinstance(record["request"], dict)
                and record["request"].get("method") == "eth_getBlockByNumber"
            ]
            state_block_tags = [
                record["request"]["block"]
                for record in client.records
                if isinstance(record["request"], dict)
                and "to" in record["request"]
            ]
            self.assertEqual(block_requests, ["0x1c8"])
            self.assertTrue(state_block_tags)
            self.assertEqual(set(state_block_tags), {"0x1c8"})


if __name__ == "__main__":
    unittest.main()
