import csv
import hashlib
import json
import os
import tempfile
import unittest
import urllib.error
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
    atomic_write_csv,
    collect_dex_depth,
    collect_dex_depth_with_execution,
    decimal_text,
    decode_int,
    decode_symbol,
    dex_depth_failure_reason_code,
    depth_fields,
    ensure_full_publish_scope,
    encode_signed_word,
    execution_publication_coverage_gate,
    load_pool_inventory,
    merge_exact_publication_bundle,
    migrate_legacy_dex_depth_reason_codes,
    preflight_publication_bundle,
    publish_exact_publication_bundle,
    protocol_model,
    publish_execution_snapshot,
    publish_full_publication_bundle,
    publish_snapshot,
    freeze_v2_pool_state,
    route_quantity_quote_for_v2_pool,
    terminal_execution_rows,
    unsupported_row,
    validate_snapshot as validate_depth_snapshot,
    v2_band_amounts,
    v2_exact_input_quote,
    v2_exact_output_quote,
    v2_execution_rows,
    v3_move_to_price,
)
from scripts.route_quantity import CommonTarget, MarketRules
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


class ActualV2Transport:
    def __init__(self, *, after_batch=None):
        self.calls = []
        self.after_batch = after_batch

    def __call__(
        self,
        _url,
        payload,
        *,
        deadline=None,
        timeout_seconds=None,
        max_retries=None,
    ):
        self.calls.append(
            {
                "payload": payload,
                "deadline": deadline,
                "timeout_seconds": timeout_seconds,
                "max_retries": max_retries,
            }
        )
        if isinstance(payload, list):
            response = [self._response(item) for item in payload]
            if self.after_batch is not None:
                self.after_batch()
        else:
            response = self._response(payload)
        return response, json.dumps(response, sort_keys=True).encode("utf-8")

    @staticmethod
    def _response(payload):
        target = "0x1111111111111111111111111111111111111111"
        quote = "0x2222222222222222222222222222222222222222"
        if payload["method"] != "eth_call":
            result = "0x1"
        else:
            call = payload["params"][0]
            selector = call["data"]
            if selector == SELECTOR_TOKEN0:
                result = address_result(target)
            elif selector == SELECTOR_TOKEN1:
                result = address_result(quote)
            elif selector == SELECTOR_GET_RESERVES:
                result = uint_result(100 * 10**18, 10_000 * 10**6, 0)
            elif selector == SELECTOR_DECIMALS:
                result = uint_result(18 if call["to"] == target else 6)
            elif selector == SELECTOR_SYMBOL:
                result = string_result("AAVE" if call["to"] == target else "USDC")
            else:
                raise AssertionError(selector)
        return {
            "jsonrpc": "2.0",
            "id": payload["id"],
            "result": result,
        }


class FailoverV2Transport:
    """Literal JSON-RPC fixture for run-scoped fixed-block failover."""

    def __init__(self, *, chain_id="0x38", fail_primary=True, fail_all=False):
        self.chain_id = chain_id
        self.fail_primary = fail_primary
        self.fail_all = fail_all
        self.calls = []

    def __call__(
        self,
        url,
        payload,
        *,
        deadline=None,
        timeout_seconds=None,
        max_retries=None,
    ):
        del deadline, timeout_seconds, max_retries
        method = payload[0]["method"] if isinstance(payload, list) else payload["method"]
        self.calls.append((url, method, payload))
        if self.fail_all or (self.fail_primary and "primary" in url):
            raise TimeoutError("private timeout from " + url)
        if isinstance(payload, list):
            response = [self._response(item) for item in payload]
        else:
            response = self._response(payload)
        return response, json.dumps(response, sort_keys=True).encode("utf-8")

    def _response(self, payload):
        target = "0x1111111111111111111111111111111111111111"
        quote = "0x2222222222222222222222222222222222222222"
        method = payload["method"]
        if method == "eth_chainId":
            result = self.chain_id
        elif method == "eth_blockNumber":
            result = "0x7b"
        elif method == "eth_getBlockByNumber":
            result = {
                "number": "0x7b",
                "hash": "0x" + "a" * 64,
                "parentHash": "0x" + "b" * 64,
                "timestamp": "0x65920080",
            }
        elif method == "eth_call":
            call = payload["params"][0]
            selector = call["data"]
            if selector == SELECTOR_TOKEN0:
                result = address_result(target)
            elif selector == SELECTOR_TOKEN1:
                result = address_result(quote)
            elif selector == SELECTOR_GET_RESERVES:
                result = uint_result(100 * 10**18, 10_000 * 10**6, 0)
            elif selector == SELECTOR_DECIMALS:
                result = uint_result(18 if call["to"] == target else 6)
            elif selector == SELECTOR_SYMBOL:
                result = string_result("AAVE" if call["to"] == target else "USDC")
            else:
                raise AssertionError(selector)
        else:
            raise AssertionError(method)
        return {
            "jsonrpc": "2.0",
            "id": payload["id"],
            "result": result,
        }


class MidPoolFailoverV2Transport(FailoverV2Transport):
    def __init__(self):
        super().__init__(chain_id="0x1", fail_primary=False)
        self.primary_eth_call_count = 0
        self.failed = False

    def __call__(self, url, payload, **kwargs):
        method = payload[0]["method"] if isinstance(payload, list) else payload["method"]
        if "primary" in url and method == "eth_call":
            self.primary_eth_call_count += 1
            if self.primary_eth_call_count == 2 and not self.failed:
                self.failed = True
                self.calls.append((url, method, payload))
                raise urllib.error.HTTPError(
                    url,
                    403,
                    "private mid-pool failure " + url,
                    {},
                    None,
                )
        return super().__call__(url, payload, **kwargs)


class DexDepthMathTest(unittest.TestCase):
    def test_v2_quantity_adapter_freezes_mutable_state_once_for_hash_and_quote(self):
        pool_address = "0x3333333333333333333333333333333333333333"
        target_address = "0x1111111111111111111111111111111111111111"
        quote_address = "0x2222222222222222222222222222222222222222"
        stable = {
            "chain": "eth",
            "chain_id": 1,
            "dex": "uniswap_v2",
            "pool_address": pool_address,
            "token0_address": target_address,
            "token1_address": quote_address,
            "token0_decimals": 18,
            "token1_decimals": 6,
            "reserve0_raw": 100 * 10**18,
            "reserve1_raw": 10_000_000_000,
            "reserve_timestamp_last_raw": 1_704_067_200,
            "fee_bps": 30,
            "fee_numerator": 9_970,
            "fee_denominator": 10_000,
            "fee_formula": (
                "amount_in_with_fee=amount_in*fee_numerator;"
                "denominator=reserve_in*fee_denominator+amount_in_with_fee"
            ),
            "fee_proof_sha256": "c" * 64,
            "block_number": 123,
            "block_hash": "0x" + "d" * 64,
            "block_header_sha256": "e" * 64,
            "observed_at": "2026-08-01T12:00:00.0000005Z",
            "raw_response_sha256": "f" * 64,
        }
        expected_state_id = freeze_v2_pool_state(stable).state_id
        rules = MarketRules(
            market_id=f"dex:eth:uniswap_v2:{pool_address}:AAVE",
            base_asset="AAVE",
            quote_asset="USDC",
            base_unit_decimals=18,
            quote_unit_decimals=6,
            base_increment=Decimal("0.000000000000000001"),
            quote_increment=Decimal("0.000001"),
            min_base_quantity=Decimal("0"),
            min_quote_notional=Decimal("0"),
            observed_at="2026-08-01T12:00:00Z",
            valid_until="2026-08-01T12:05:00Z",
            source_record_sha256="a" * 64,
        )
        target = CommonTarget(
            asset="AAVE",
            unit_decimals=18,
            raw_quantity=10 * 10**18,
            lattice_raw=1,
        )

        class MutatingState(dict):
            def __init__(self, values):
                super().__init__(values)
                self.reserve_reads = 0

            def get(self, key, default=None):
                if key == "reserve0_raw":
                    self.reserve_reads += 1
                    if self.reserve_reads > 1:
                        return 500 * 10**18
                return super().get(key, default)

        mutable = MutatingState(stable)
        result = route_quantity_quote_for_v2_pool(
            mutable,
            direction="sell",
            target_token_quantity=target,
            market_rules=rules,
            target_token_address=target_address,
            quote_token_address=quote_address,
            expected_state_id=expected_state_id,
            cohort_now="2026-08-01T12:02:00.0000005Z",
        )

        self.assertEqual(mutable.reserve_reads, 1)
        self.assertEqual(result.state_id, expected_state_id)
        self.assertEqual(result.quote_received_quantity, Decimal("906.610893"))

    def test_v2_quantity_adapter_rejects_wrong_pool_block_hash_and_fee_proof_bindings(self):
        base = {
            "chain": "eth",
            "chain_id": 1,
            "dex": "uniswap_v2",
            "pool_address": "0x3333333333333333333333333333333333333333",
            "token0_address": "0x1111111111111111111111111111111111111111",
            "token1_address": "0x2222222222222222222222222222222222222222",
            "token0_decimals": 18,
            "token1_decimals": 6,
            "reserve0_raw": 100 * 10**18,
            "reserve1_raw": 10_000_000_000,
            "reserve_timestamp_last_raw": 1_704_067_200,
            "fee_bps": 30,
            "fee_numerator": 9_970,
            "fee_denominator": 10_000,
            "fee_formula": (
                "amount_in_with_fee=amount_in*fee_numerator;"
                "denominator=reserve_in*fee_denominator+amount_in_with_fee"
            ),
            "fee_proof_sha256": "c" * 64,
            "block_number": 123,
            "block_hash": "0x" + "d" * 64,
            "block_header_sha256": "e" * 64,
            "observed_at": "2026-08-01T12:00:00Z",
            "raw_response_sha256": "f" * 64,
        }
        expected = freeze_v2_pool_state(base).state_id
        changes = (
            ("pool_address", "0x4444444444444444444444444444444444444444"),
            ("block_number", 124),
            ("block_hash", "0x" + "1" * 64),
            ("block_header_sha256", "2" * 64),
            ("fee_proof_sha256", "3" * 64),
        )
        for field_name, value in changes:
            with self.subTest(field_name=field_name):
                with self.assertRaisesRegex(ValueError, "state binding"):
                    route_quantity_quote_for_v2_pool(
                        {**base, field_name: value},
                        direction="sell",
                        target_token_quantity=CommonTarget(
                            asset="AAVE",
                            unit_decimals=18,
                            raw_quantity=10 * 10**18,
                            lattice_raw=1,
                        ),
                        market_rules=MarketRules(
                            market_id=(
                                "dex:eth:uniswap_v2:"
                                "0x3333333333333333333333333333333333333333:AAVE"
                            ),
                            base_asset="AAVE",
                            quote_asset="USDC",
                            base_unit_decimals=18,
                            quote_unit_decimals=6,
                            base_increment=Decimal("0.000000000000000001"),
                            quote_increment=Decimal("0.000001"),
                            min_base_quantity=Decimal("0"),
                            min_quote_notional=Decimal("0"),
                            observed_at="2026-08-01T12:00:00Z",
                            valid_until="2026-08-01T12:05:00Z",
                            source_record_sha256="a" * 64,
                        ),
                        target_token_address=(
                            "0x1111111111111111111111111111111111111111"
                        ),
                        quote_token_address=(
                            "0x2222222222222222222222222222222222222222"
                        ),
                        expected_state_id=expected,
                        cohort_now="2026-08-01T12:02:00Z",
                    )

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

    def test_bsc_timeout_fails_over_once_binds_fixed_block_and_reuses_breaker(self):
        from scripts.fetch_dex_depth import RpcClient

        transport = FailoverV2Transport()
        pools = []
        for index in range(2):
            pools.append(
                {
                    **self.pool,
                    "chain": "bsc",
                    "pool_address": "0x{:040x}".format(index + 3),
                    "base_token_id": "bsc_" + self.target,
                    "quote_token_id": "bsc_" + self.quote,
                }
            )
        environment = {
            "DEX_DEPTH_RPC_BSC": (
                "https://user:primary-secret@primary.example.test/rpc?key=one"
            ),
            "DEX_DEPTH_RPC_BSC_FALLBACKS": json.dumps(
                ["https://user:fallback-secret@fallback.example.test/rpc?key=two"]
            ),
        }

        with patch.dict(os.environ, environment, clear=True), patch.object(
            RpcClient,
            "_default_one_attempt_request",
            new=staticmethod(transport),
        ):
            snapshot_id, rows, execution = collect_dex_depth_with_execution(
                pools,
                raw_root=self.root,
                sleep_seconds=0,
            )

        self.assertEqual({row["status"] for row in rows}, {"observed"})
        self.assertEqual(len(execution), 20)
        primary_calls = [call for call in transport.calls if "primary" in call[0]]
        self.assertEqual(len(primary_calls), 4)
        self.assertEqual({call[1] for call in primary_calls}, {"eth_blockNumber"})
        fallback_methods = [call[1] for call in transport.calls if "fallback" in call[0]]
        self.assertIn("eth_chainId", fallback_methods)
        self.assertIn("eth_getBlockByNumber", fallback_methods)
        raw_files = sorted((self.root / snapshot_id).glob("*.json"))
        self.assertEqual(len(raw_files), 3)
        transcripts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in raw_files
            if path.name != "manifest.json"
        ]
        self.assertTrue(all(item["block_number"] == 123 for item in transcripts))
        self.assertTrue(all("attempt_ledger" in item for item in transcripts))
        first_raw = next(
            path.read_bytes()
            for path in raw_files
            if path.name != "manifest.json"
        )
        tampered = json.loads(first_raw)
        tampered["attempt_ledger"][0]["decision"] = "tampered"
        tampered_bytes = (
            json.dumps(tampered, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        self.assertNotEqual(
            hashlib.sha256(first_raw).hexdigest(),
            hashlib.sha256(tampered_bytes).hexdigest(),
        )
        self.assertIn(
            hashlib.sha256(first_raw).hexdigest(),
            {row["raw_response_sha256"] for row in rows},
        )
        retained = json.dumps(transcripts, sort_keys=True)
        for secret in ("primary-secret", "fallback-secret", "?key=one", "?key=two"):
            self.assertNotIn(secret, retained)

    def test_mid_pool_switch_replays_only_fallback_state_but_keeps_failed_attempt(self):
        from scripts.fetch_dex_depth import RpcClient

        transport = MidPoolFailoverV2Transport()
        environment = {
            "DEX_DEPTH_RPC_ETH": "https://primary.example.test/rpc?secret=one",
            "DEX_DEPTH_RPC_ETH_FALLBACKS": json.dumps(
                ["https://fallback.example.test/rpc?secret=two"]
            ),
        }
        with patch.dict(os.environ, environment, clear=True), patch.object(
            RpcClient,
            "_default_one_attempt_request",
            new=staticmethod(transport),
        ), patch("scripts.fetch_dex_depth.validate_snapshot"):
            snapshot_id, rows, _execution = collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root,
                sleep_seconds=0,
            )

        self.assertEqual(rows[0]["status"], "observed")
        transcript = json.loads(
            next(
                path
                for path in (self.root / snapshot_id).glob("*.json")
                if path.name != "manifest.json"
            ).read_text(encoding="utf-8")
        )
        pool_state_records = [
            record
            for record in transcript["records"]
            if isinstance(record.get("request"), list)
            and any(
                request["params"][0]["data"] == SELECTOR_GET_RESERVES
                for request in record["request"]
            )
        ]
        self.assertEqual(len(pool_state_records), 1)
        self.assertTrue(
            transcript["source_endpoint"].startswith("rpc-endpoint-sha256:")
        )
        self.assertIn(
            "switch",
            {attempt["decision"] for attempt in transcript["attempt_ledger"]},
        )
        fallback_state_calls = [
            payload
            for url, method, payload in transport.calls
            if "fallback" in url and method == "eth_call" and isinstance(payload, list)
            and any(
                request["params"][0]["data"] == SELECTOR_GET_RESERVES
                for request in payload
            )
        ]
        self.assertEqual(len(fallback_state_calls), 1)

    def test_endpoint_exhaustion_returns_bounded_failed_rows_and_private_transcripts(self):
        from scripts.fetch_dex_depth import RpcClient

        transport = FailoverV2Transport(fail_all=True)
        environment = {
            "DEX_DEPTH_RPC_BSC": (
                "https://user:primary-secret@primary.example.test/rpc?key=one"
            ),
            "DEX_DEPTH_RPC_BSC_FALLBACKS": json.dumps(
                ["https://user:fallback-secret@fallback.example.test/rpc?key=two"]
            ),
        }
        pools = [
            {
                **self.pool,
                "chain": "bsc",
                "pool_address": "0x{:040x}".format(index + 31),
                "base_token_id": "bsc_" + self.target,
                "quote_token_id": "bsc_" + self.quote,
            }
            for index in range(2)
        ]

        with patch.dict(os.environ, environment, clear=True), patch.object(
            RpcClient,
            "_default_one_attempt_request",
            new=staticmethod(transport),
        ), patch("scripts.fetch_dex_depth.validate_snapshot"):
            snapshot_id, rows, _execution = collect_dex_depth_with_execution(
                pools,
                raw_root=self.root,
                sleep_seconds=0,
                allow_terminal_only=True,
            )

        self.assertEqual({row["status"] for row in rows}, {"failed"})
        self.assertEqual(
            {row["reason_code"] for row in rows},
            {"rpc_endpoint_exhausted"},
        )
        with self.assertRaisesRegex(ValueError, "terminal non-retryable"):
            validate_depth_snapshot(pools, rows, allow_terminal_only=True)
        retained = b"".join(
            path.read_bytes() for path in sorted((self.root / snapshot_id).glob("*.json"))
        ).decode("utf-8")
        for secret in (
            "primary-secret",
            "fallback-secret",
            "private timeout",
            "?key=one",
            "?key=two",
        ):
            self.assertNotIn(secret, retained)

    def test_whole_collection_deadline_is_checked_before_inter_pool_sleep(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )

        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

            def sleep(self, seconds):
                self.now += seconds

        clock = Clock()
        deadline = CollectionDeadline.for_duration(
            1,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        calls = []

        def factory(chain, url, /):
            calls.append((chain, url))
            return FakeV2Rpc(chain, url)

        with self.assertRaisesRegex(
            CollectionDeadlineExceeded,
            "^collection deadline exceeded$",
        ):
            collect_dex_depth_with_execution(
                [self.pool, {**self.pool, "pool_address": "0x" + "4" * 40}],
                raw_root=self.root,
                sleep_seconds=2,
                rpc_factory=factory,
                deadline=deadline,
            )
        self.assertEqual(len(calls), 1)

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
            "reason_code": "collection_failed",
            "raw_response_sha256": "a" * 64,
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
            "reason_code": "measurement_limit",
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
        history_path = published / HISTORY_FILENAME
        with history_path.open(newline="", encoding="utf-8") as handle:
            baseline_history = list(csv.DictReader(handle))
        legacy_columns = [
            field for field in DEX_DEPTH_COLUMNS if field != "reason_code"
        ]
        with history_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=legacy_columns)
            writer.writeheader()
            writer.writerows(
                {field: row.get(field, "") for field in legacy_columns}
                for row in baseline_history
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
        self.assertEqual(
            {row["reason_code"] for row in history},
            {"observed"},
        )

    def test_exact_pool_refresh_migrates_keyless_legacy_depth_reasons(self):
        other_pool = {
            **self.pool,
            "pool_address": "0x4444444444444444444444444444444444444444",
            "pool_name": "AAVE / USDC second pool",
        }
        _baseline_id, baseline_depth, baseline_execution = (
            collect_dex_depth_with_execution(
                [self.pool, other_pool],
                raw_root=self.root / "raw-legacy-baseline",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        _candidate_id, candidate_depth, candidate_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-legacy-candidate",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        published = self.root / "legacy-local"
        publish_snapshot(
            baseline_depth,
            output_dir=self.root / "legacy-processed",
            publish_dir=published,
        )
        publish_execution_snapshot(
            baseline_execution,
            expected_market_ids={row["market_id"] for row in baseline_execution},
            output_dir=self.root / "legacy-processed",
            publish_dir=published,
        )
        legacy_columns = [
            field for field in DEX_DEPTH_COLUMNS if field != "reason_code"
        ]
        with (published / LATEST_FILENAME).open(
            "w", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=legacy_columns)
            writer.writeheader()
            writer.writerows(
                {
                    field: row.get(field, "")
                    for field in legacy_columns
                }
                for row in baseline_depth
            )

        try:
            merged_depth, _merged_execution = merge_exact_publication_bundle(
                candidate_depth,
                candidate_execution,
                target_market_id=(
                    "dex:eth:uniswap_v2:"
                    "0x3333333333333333333333333333333333333333:AAVE"
                ),
                publish_dir=published,
            )
        except ValueError as error:
            self.fail(f"keyless legacy DEX depth migration was rejected: {error}")

        self.assertTrue(
            all(row.get("reason_code") == "observed" for row in merged_depth)
        )

    def test_legacy_dex_depth_reason_migration_rejects_mixed_schema(self):
        _snapshot_id, rows, _execution_rows = collect_dex_depth_with_execution(
            [self.pool, {**self.pool, "pool_address": "0x" + "4" * 40}],
            raw_root=self.root / "raw-mixed-reason-schema",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )
        rows[0].pop("reason_code")

        with self.assertRaisesRegex(ValueError, "mixed.*reason_code"):
            migrate_legacy_dex_depth_reason_codes(rows)

    def test_atomic_write_migrates_keyless_legacy_dex_reason_schema(self):
        _snapshot_id, rows, _execution_rows = collect_dex_depth_with_execution(
            [self.pool],
            raw_root=self.root / "raw-keyless-write",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )
        rows[0].pop("reason_code")
        path = self.root / "keyless-depth.csv"
        atomic_write_csv(path, rows)
        with path.open(newline="", encoding="utf-8") as handle:
            written = list(csv.DictReader(handle))

        self.assertEqual(written[0]["reason_code"], "observed")

    def test_atomic_write_rejects_blank_modern_dex_reason(self):
        _snapshot_id, rows, _execution_rows = collect_dex_depth_with_execution(
            [self.pool],
            raw_root=self.root / "raw-blank-write",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )
        rows[0]["reason_code"] = ""
        with self.assertRaisesRegex(ValueError, "reason_code"):
            atomic_write_csv(self.root / "blank-depth.csv", rows)

    def test_publish_snapshot_migrates_keyless_legacy_dex_history(self):
        _snapshot_id, rows, _execution_rows = collect_dex_depth_with_execution(
            [self.pool],
            raw_root=self.root / "raw-legacy-history",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )
        legacy = dict(rows[0])
        legacy.update(
            snapshot_id="legacy-snapshot",
            observed_at="2026-07-26T00:00:01+00:00",
            request_started_at="2026-07-26T00:00:00+00:00",
            response_received_at="2026-07-26T00:00:01+00:00",
        )
        legacy.pop("reason_code")
        published = self.root / "legacy-history-published"
        published.mkdir()
        history_path = published / HISTORY_FILENAME
        legacy_columns = [
            field for field in DEX_DEPTH_COLUMNS if field != "reason_code"
        ]
        with history_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=legacy_columns)
            writer.writeheader()
            writer.writerow(
                {field: legacy.get(field, "") for field in legacy_columns}
            )

        publish_snapshot(
            rows,
            output_dir=self.root / "legacy-history-processed",
            publish_dir=published,
        )
        with history_path.open(newline="", encoding="utf-8") as handle:
            history = list(csv.DictReader(handle))

        self.assertEqual(len(history), 2)
        self.assertEqual(
            {row["reason_code"] for row in history},
            {"observed"},
        )

    def test_full_bundle_migrates_keyless_legacy_dex_history(self):
        _baseline_id, baseline_depth, baseline_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-full-legacy-baseline",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        _candidate_id, candidate_depth, candidate_execution = (
            collect_dex_depth_with_execution(
                [self.pool],
                raw_root=self.root / "raw-full-legacy-candidate",
                sleep_seconds=0,
                rpc_factory=FakeV2Rpc,
            )
        )
        published = self.root / "full-legacy-published"
        processed = self.root / "full-legacy-processed"
        publish_snapshot(
            baseline_depth,
            output_dir=processed,
            publish_dir=published,
        )
        publish_execution_snapshot(
            baseline_execution,
            expected_market_ids={
                row["market_id"] for row in baseline_execution
            },
            output_dir=processed,
            publish_dir=published,
        )
        history_path = published / HISTORY_FILENAME
        with history_path.open(newline="", encoding="utf-8") as handle:
            baseline_history = list(csv.DictReader(handle))
        legacy_columns = [
            field for field in DEX_DEPTH_COLUMNS if field != "reason_code"
        ]
        with history_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=legacy_columns)
            writer.writeheader()
            writer.writerows(
                {field: row.get(field, "") for field in legacy_columns}
                for row in baseline_history
            )
        reports = preflight_publication_bundle(
            candidate_depth,
            candidate_execution,
            published,
        )

        publish_full_publication_bundle(
            candidate_depth,
            candidate_execution,
            output_dir=processed,
            publish_dir=published,
            preflight_reports=reports,
        )
        with history_path.open(newline="", encoding="utf-8") as handle:
            history = list(csv.DictReader(handle))

        self.assertEqual(len(history), 2)
        self.assertEqual(
            {row["reason_code"] for row in history},
            {"observed"},
        )

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
                    "reason_code": "collection_failed",
                    "raw_response_sha256": "a" * 64,
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
                "reason_code": "collection_failed",
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
        self.assertEqual(row["reason_code"], "observed")
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
        self.assertEqual(manifest["reason_code_counts"], {"observed": 1})
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
        self.assertEqual(
            stale_depth["reason_code"],
            "depth_usd_price_time_mismatch",
        )
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

    def test_dex_failure_classifier_uses_exception_types_not_raw_messages(self):
        failures = (
            (urllib.error.HTTPError("https://example.test", 429, "", {}, None), "rate_limit"),
            (urllib.error.HTTPError("https://example.test", 503, "", {}, None), "source_unavailable"),
            (urllib.error.URLError("private hostname"), "network"),
            (json.JSONDecodeError("private payload", "x", 0), "parse"),
            (ValueError("private invalid value"), "validation"),
            (PermissionError("/srv/private/depth"), "collection_failed"),
        )
        for error, expected in failures:
            with self.subTest(expected=expected):
                self.assertEqual(dex_depth_failure_reason_code(error), expected)

    def test_unsupported_rows_keep_a_specific_bounded_reason(self):
        cases = (
            ("unsupported_chain:solana", "unsupported_chain"),
            ("unsupported_pool_model:curve", "unsupported_protocol"),
            ("pool_is_not_an_evm_contract_address", "unsupported_method"),
            ("missing_rpc_endpoint:eth", "unsupported_source"),
        )
        for raw_reason, expected in cases:
            with self.subTest(raw_reason=raw_reason):
                row = unsupported_row(
                    self.pool,
                    snapshot_id="snapshot-1",
                    request_started_at="2026-07-27T00:00:00+00:00",
                    response_received_at="2026-07-27T00:00:01+00:00",
                    reason=raw_reason,
                )
                self.assertEqual(row["reason_code"], expected)

    def test_validator_accepts_legacy_missing_reason_but_rejects_unknown_reason(self):
        _snapshot_id, rows, _execution_rows = collect_dex_depth_with_execution(
            [self.pool],
            raw_root=self.root / "raw-reason-validation",
            sleep_seconds=0,
            rpc_factory=FakeV2Rpc,
        )
        legacy = [dict(rows[0])]
        legacy[0].pop("reason_code")
        validate_depth_snapshot([self.pool], legacy)
        with self.assertRaisesRegex(ValueError, "reason code"):
            validate_depth_snapshot(
                [self.pool],
                [{**rows[0], "reason_code": ""}],
            )
        with self.assertRaisesRegex(ValueError, "reason code"):
            validate_depth_snapshot(
                [self.pool],
                [{**rows[0], "reason_code": "private_raw_error"}],
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
        self.assertEqual(len(failed_depth["raw_response_sha256"]), 64)
        for invalid_hash in ("", "A" * 64):
            with self.subTest(invalid_hash=invalid_hash):
                corrupted = [dict(row) for row in depth_rows]
                next(
                    row
                    for row in corrupted
                    if row["token_symbol"] == "COMP"
                )["raw_response_sha256"] = invalid_hash
                with self.assertRaisesRegex(ValueError, "source hash"):
                    validate_depth_snapshot(
                        [self.pool, failed_pool],
                        corrupted,
                    )
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
                "reason_code": "observed",
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
                    "reason_code": "observed",
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
                "reason_code": (
                    "unsupported_source" if index < 2 else "observed"
                ),
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

    def test_one_pool_primitive_matches_independent_golden_rows_and_transcript_hash(self):
        from scripts.fetch_dex_depth import collect_dex_pool_observation

        timestamp = "2026-08-01T12:00:00+00:00"
        with patch(
            "scripts.fetch_dex_depth.utc_now_text",
            return_value=timestamp,
        ):
            one_depth, one_execution = collect_dex_pool_observation(
                self.pool,
                snapshot_id="golden-dex-1",
                raw_path=self.root / "one.json",
                rpc_factory=FakeV2Rpc,
            )

        one_raw = (self.root / "one.json").read_bytes()
        expected_raw = """{
  "attempt_ledger": [],
  "attempt_ledger_dropped": 0,
  "block_number": 123,
  "pool": {
    "chain": "eth",
    "dex": "uniswap_v2",
    "pool_address": "0x3333333333333333333333333333333333333333",
    "token_symbol": "AAVE"
  },
  "records": [
    {
      "request": "block",
      "response": "0x7b"
    },
    {
      "request": {
        "block": "0x7b",
        "method": "eth_getBlockByNumber"
      },
      "response": {
        "number": "0x7b",
        "timestamp": "0x65920080"
      }
    },
    {
      "request": {
        "block": "0x7b",
        "data": [
          "0x0dfe1681",
          "0xd21220a7",
          "0x0902f1ac"
        ],
        "to": "0x3333333333333333333333333333333333333333"
      },
      "response": "fixture"
    },
    {
      "request": {
        "block": "0x7b",
        "data": [
          "0x313ce567",
          "0x95d89b41"
        ],
        "to": "0x1111111111111111111111111111111111111111"
      },
      "response": "fixture"
    },
    {
      "request": {
        "block": "0x7b",
        "data": [
          "0x313ce567",
          "0x95d89b41"
        ],
        "to": "0x2222222222222222222222222222222222222222"
      },
      "response": "fixture"
    }
  ],
  "source_endpoint": "https://rpc.example.test"
}
""".encode("utf-8")
        expected_depth = {
            "snapshot_id": "golden-dex-1",
            "observed_at": timestamp,
            "request_started_at": timestamp,
            "response_received_at": timestamp,
            "token_symbol": "AAVE",
            "chain": "eth",
            "dex": "uniswap_v2",
            "pool_address": "0x3333333333333333333333333333333333333333",
            "pool_name": "AAVE / USDC",
            "protocol_model": "constant_product_v2",
            "block_number": "123",
            "block_timestamp": "2024-01-01T00:00:00+00:00",
            "target_token_address": "0x1111111111111111111111111111111111111111",
            "target_token_position": "token0",
            "token0_address": "0x1111111111111111111111111111111111111111",
            "token0_symbol": "AAVE",
            "token0_decimals": "18",
            "token0_price_usd": "100",
            "token1_address": "0x2222222222222222222222222222222222222222",
            "token1_symbol": "USDC",
            "token1_decimals": "6",
            "token1_price_usd": "1",
            "fee_bps": "30",
            "pool_state_price_usd": "100",
            "source_target_price_usd": "100",
            "price_difference_bps": "0",
            "usd_price_source_snapshot_id": "tvl-1",
            "usd_price_observed_at": "2024-01-01T00:00:01+00:00",
            "usd_price_skew_seconds": "1",
            "usd_price_freshness_status": "current",
            "usd_price_source": "",
            "usd_price_source_endpoint": "",
            "usd_price_raw_response_sha256": "",
            "sell_depth_10bps_usd": "5.001250625390898642739388842",
            "buy_depth_10bps_usd": "5.013792000611482680624751255",
            "total_depth_10bps_usd": "10.0150426260023813233641401",
            "depth_10bps_complete": "1",
            "sell_depth_25bps_usd": "12.50782228091054210980883597",
            "buy_depth_25bps_usd": "12.52978661022353445196196004",
            "total_depth_25bps_usd": "25.03760889113407656177079601",
            "depth_25bps_complete": "1",
            "sell_depth_50bps_usd": "25.03132836999833417305888293",
            "buy_depth_50bps_usd": "25.04395976099365634841449471",
            "total_depth_50bps_usd": "50.07528813099199052147337764",
            "depth_50bps_complete": "1",
            "sell_depth_100bps_usd": "50.12562893380045265520178999",
            "buy_depth_100bps_usd": "50.02569821553688086185046415",
            "total_depth_100bps_usd": "100.1513271493373335170522541",
            "depth_100bps_complete": "1",
            "depth_method": "fixed_block_pool_state_marginal_price_band",
            "source": "fixed-block EVM JSON-RPC eth_call",
            "source_endpoint": "https://rpc.example.test",
            "raw_response_sha256": (
                "91c7e052604c7a0946a516c828b3afc2"
                "59393e99aab5813f13ea370cb2ffcfa4"
            ),
            "status": "observed",
            "reason_code": "observed",
            "error": "",
        }
        expected_common = {
            "snapshot_id": "golden-dex-1",
            "source_snapshot_id": "golden-dex-1",
            "contract_version": "1",
            "calculation_method": "fixed_block_pool_state_exact_target_quantity_v1",
            "observed_at": timestamp,
            "state_observed_at": "2024-01-01T00:00:00+00:00",
            "request_started_at": timestamp,
            "response_received_at": timestamp,
            "market_id": (
                "dex:eth:uniswap_v2:"
                "0x3333333333333333333333333333333333333333:AAVE"
            ),
            "market_type": "dex",
            "token_symbol": "AAVE",
            "exchange": "",
            "cex_symbol": "",
            "source_instrument": "",
            "base_asset": "",
            "source_quote_asset": "",
            "chain": "eth",
            "dex": "uniswap_v2",
            "pool_address": "0x3333333333333333333333333333333333333333",
            "block_number": "123",
            "block_timestamp": "2024-01-01T00:00:00+00:00",
            "protocol_model": "constant_product_v2",
            "target_token_address": "0x1111111111111111111111111111111111111111",
            "target_token_decimals": "18",
            "quote_token_address": "0x2222222222222222222222222222222222222222",
            "quote_token_decimals": "6",
            "notional_definition": (
                "target Token quantity valued at the snapshot pre-trade "
                "reference price"
            ),
            "reference_price_method": "pre_fee_pool_state_marginal_price",
            "reference_price_quote_per_token": "100",
            "quote_to_usd": "1",
            "reference_price_usd_per_token": "100",
            "usd_price_source_snapshot_id": "tvl-1",
            "usd_price_observed_at": "2024-01-01T00:00:01+00:00",
            "fee_status": "included_protocol_fee",
            "fee_rate_bps": "30",
            "fee_amount_usd": "",
            "usd_conversion_status": "observed_inventory_token_price",
            "excluded_costs": (
                "gas,router_fee,token_transfer_tax,MEV,post_block_state_changes"
            ),
            "source": "fixed-block EVM JSON-RPC pool state",
            "source_endpoint": "https://rpc.example.test",
            "source_sequence": "123",
            "raw_response_sha256": (
                "91c7e052604c7a0946a516c828b3afc2"
                "59393e99aab5813f13ea370cb2ffcfa4"
            ),
            "error": "",
        }
        expected_scenarios = [
            {
                "direction": "sell_token", "requested_notional_usd": "1000",
                "reference_notional_usd": "1000", "target_token_quantity": "10",
                "filled_token_quantity": "10", "fill_ratio": "1",
                "quote_amount": "906.610893", "quote_amount_usd": "906.610893",
                "filled_vwap_quote_per_token": "90.6610893",
                "filled_vwap_usd_per_token": "90.6610893",
                "quoted_execution_cost_usd": "93.389107",
                "quoted_execution_cost_bps": "933.89107",
                "levels_or_ticks_consumed": "1",
                "ending_marginal_price_quote_per_token": "82.6671737",
                "status": "observed", "status_reason": "full_target_quantity_filled",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "1000",
                "reference_notional_usd": "1000", "target_token_quantity": "10",
                "filled_token_quantity": "10", "fill_ratio": "1",
                "quote_amount": "1114.454475", "quote_amount_usd": "1114.454475",
                "filled_vwap_quote_per_token": "111.4454475",
                "filled_vwap_usd_per_token": "111.4454475",
                "quoted_execution_cost_usd": "114.454475",
                "quoted_execution_cost_bps": "1144.54475",
                "levels_or_ticks_consumed": "1",
                "ending_marginal_price_quote_per_token": "123.49393861111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
                "status": "observed", "status_reason": "full_target_quantity_filled",
            },
            {
                "direction": "sell_token", "requested_notional_usd": "5000",
                "reference_notional_usd": "5000", "target_token_quantity": "50",
                "filled_token_quantity": "50", "fill_ratio": "1",
                "quote_amount": "3326.659993", "quote_amount_usd": "3326.659993",
                "filled_vwap_quote_per_token": "66.53319986",
                "filled_vwap_usd_per_token": "66.53319986",
                "quoted_execution_cost_usd": "1673.340007",
                "quoted_execution_cost_bps": "3346.680014",
                "levels_or_ticks_consumed": "1",
                "ending_marginal_price_quote_per_token": "44.48893338",
                "status": "observed", "status_reason": "full_target_quantity_filled",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "5000",
                "reference_notional_usd": "5000", "target_token_quantity": "50",
                "filled_token_quantity": "50", "fill_ratio": "1",
                "quote_amount": "10030.090271", "quote_amount_usd": "10030.090271",
                "filled_vwap_quote_per_token": "200.60180542",
                "filled_vwap_usd_per_token": "200.60180542",
                "quoted_execution_cost_usd": "5030.090271",
                "quoted_execution_cost_bps": "10060.180542",
                "levels_or_ticks_consumed": "1",
                "ending_marginal_price_quote_per_token": "400.60180542",
                "status": "observed", "status_reason": "full_target_quantity_filled",
            },
            {
                "direction": "sell_token", "requested_notional_usd": "10000",
                "reference_notional_usd": "10000", "target_token_quantity": "100",
                "filled_token_quantity": "100", "fill_ratio": "1",
                "quote_amount": "4992.488733", "quote_amount_usd": "4992.488733",
                "filled_vwap_quote_per_token": "49.92488733",
                "filled_vwap_usd_per_token": "49.92488733",
                "quoted_execution_cost_usd": "5007.511267",
                "quoted_execution_cost_bps": "5007.511267",
                "levels_or_ticks_consumed": "1",
                "ending_marginal_price_quote_per_token": "25.037556335",
                "status": "observed", "status_reason": "full_target_quantity_filled",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "10000",
                "reference_notional_usd": "10000", "target_token_quantity": "100",
                "filled_token_quantity": "", "fill_ratio": "",
                "quote_amount": "", "quote_amount_usd": "",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "levels_or_ticks_consumed": "",
                "ending_marginal_price_quote_per_token": "",
                "status": "partial", "status_reason": "full_pool_reserve_insufficient",
            },
            {
                "direction": "sell_token", "requested_notional_usd": "50000",
                "reference_notional_usd": "50000", "target_token_quantity": "500",
                "filled_token_quantity": "500", "fill_ratio": "1",
                "quote_amount": "8329.156223", "quote_amount_usd": "8329.156223",
                "filled_vwap_quote_per_token": "16.658312446",
                "filled_vwap_usd_per_token": "16.658312446",
                "quoted_execution_cost_usd": "41670.843777",
                "quoted_execution_cost_bps": "8334.1687554",
                "levels_or_ticks_consumed": "1",
                "ending_marginal_price_quote_per_token": "2.7847396283333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333",
                "status": "observed", "status_reason": "full_target_quantity_filled",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "50000",
                "reference_notional_usd": "50000", "target_token_quantity": "500",
                "filled_token_quantity": "", "fill_ratio": "",
                "quote_amount": "", "quote_amount_usd": "",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "levels_or_ticks_consumed": "",
                "ending_marginal_price_quote_per_token": "",
                "status": "partial", "status_reason": "full_pool_reserve_insufficient",
            },
            {
                "direction": "sell_token", "requested_notional_usd": "100000",
                "reference_notional_usd": "100000", "target_token_quantity": "1000",
                "filled_token_quantity": "1000", "fill_ratio": "1",
                "quote_amount": "9088.422971", "quote_amount_usd": "9088.422971",
                "filled_vwap_quote_per_token": "9.088422971",
                "filled_vwap_usd_per_token": "9.088422971",
                "quoted_execution_cost_usd": "90911.577029",
                "quoted_execution_cost_bps": "9091.1577029",
                "levels_or_ticks_consumed": "1",
                "ending_marginal_price_quote_per_token": "0.82870639",
                "status": "observed", "status_reason": "full_target_quantity_filled",
            },
            {
                "direction": "buy_token", "requested_notional_usd": "100000",
                "reference_notional_usd": "100000", "target_token_quantity": "1000",
                "filled_token_quantity": "", "fill_ratio": "",
                "quote_amount": "", "quote_amount_usd": "",
                "filled_vwap_quote_per_token": "", "filled_vwap_usd_per_token": "",
                "quoted_execution_cost_usd": "", "quoted_execution_cost_bps": "",
                "levels_or_ticks_consumed": "",
                "ending_marginal_price_quote_per_token": "",
                "status": "partial", "status_reason": "full_pool_reserve_insufficient",
            },
        ]
        expected_execution = [
            {**expected_common, **scenario}
            for scenario in expected_scenarios
        ]

        self.assertEqual(one_depth, expected_depth)
        self.assertEqual(one_execution, expected_execution)
        self.assertEqual(one_raw, expected_raw)
        self.assertEqual(
            one_depth["raw_response_sha256"],
            "91c7e052604c7a0946a516c828b3afc259393e99aab5813f13ea370cb2ffcfa4",
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

    def test_one_pool_primitive_rejects_expired_supplied_client_before_rpc(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_dex_depth import collect_dex_pool_observation

        client = FakeV2Rpc("eth", "https://rpc.example.test")
        raw_path = self.root / "expired-client.json"
        with self.assertRaisesRegex(
            CollectionDeadlineExceeded,
            "^collection deadline exceeded$",
        ):
            collect_dex_pool_observation(
                self.pool,
                snapshot_id="expired-dex",
                raw_path=raw_path,
                client=client,
                deadline=CollectionDeadline.for_duration(0),
            )

        self.assertEqual(client.records, [])
        self.assertFalse(raw_path.exists())

    def test_supplied_client_deadline_is_checked_between_rpc_calls(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_dex_depth import collect_dex_pool_observation

        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

        clock = Clock()

        class ExpiringV2Rpc(FakeV2Rpc):
            def block_number(self):
                result = super().block_number()
                clock.now = 2.0
                return result

        client = ExpiringV2Rpc("eth", "https://rpc.example.test")
        raw_path = self.root / "mid-sequence-expiry.json"
        deadline = CollectionDeadline.for_duration(
            1,
            clock=clock.monotonic,
            sleeper=lambda seconds: None,
        )
        with self.assertRaisesRegex(
            CollectionDeadlineExceeded,
            "^collection deadline exceeded$",
        ):
            collect_dex_pool_observation(
                self.pool,
                snapshot_id="mid-sequence-expiry",
                raw_path=raw_path,
                client=client,
                deadline=deadline,
            )

        self.assertEqual(client.records, [{"request": "block", "response": "0x7b"}])
        self.assertFalse(raw_path.exists())

    def test_production_supplied_client_rechecks_before_batch_fallback_rpc(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_dex_depth import RpcClient, collect_dex_pool_observation

        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

        clock = Clock()
        transport_calls = []

        def transport(
            _url,
            payload,
            *,
            deadline=None,
            timeout_seconds=None,
            max_retries=None,
        ):
            transport_calls.append(payload)
            if isinstance(payload, list):
                clock.now = 2.0
                return [], b"[]"
            response = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": "0x0",
            }
            return response, json.dumps(response).encode("utf-8")

        client = RpcClient(
            "eth",
            "https://rpc.example.test",
            request=transport,
        )
        deadline = CollectionDeadline.for_duration(
            1,
            clock=clock.monotonic,
            sleeper=lambda seconds: None,
        )
        with self.assertRaisesRegex(
            CollectionDeadlineExceeded,
            "^collection deadline exceeded$",
        ):
            collect_dex_pool_observation(
                self.pool,
                snapshot_id="batch-fallback-expiry",
                raw_path=self.root / "batch-fallback-expiry.json",
                client=client,
                fixed_block_number=123,
                fixed_block_timestamp="2024-01-01T00:00:00+00:00",
                deadline=deadline,
            )

        self.assertEqual(len(transport_calls), 1)
        self.assertIsInstance(transport_calls[0], list)

    def test_supplied_production_client_propagates_effective_route_deadline(self):
        from scripts.collection_deadline import CollectionDeadline
        from scripts.fetch_dex_depth import RpcClient, collect_dex_pool_observation

        transport = ActualV2Transport()
        client = RpcClient(
            "eth",
            "https://rpc.example.test",
            request=transport,
            timeout_seconds=9,
            max_retries=2,
        )
        route_deadline = CollectionDeadline.for_duration(30)

        row, execution_rows = collect_dex_pool_observation(
            self.pool,
            snapshot_id="propagated-route-deadline",
            raw_path=self.root / "propagated-route-deadline.json",
            client=client,
            fixed_block_number=123,
            fixed_block_timestamp="2024-01-01T00:00:00+00:00",
            deadline=route_deadline,
        )

        self.assertEqual(row["status"], "observed")
        self.assertEqual(len(execution_rows), 10)
        self.assertTrue(transport.calls)
        self.assertTrue(
            all(call["deadline"] is route_deadline for call in transport.calls)
        )
        self.assertEqual(
            {call["timeout_seconds"] for call in transport.calls},
            {9},
        )
        self.assertEqual(
            {call["max_retries"] for call in transport.calls},
            {1},
        )

    def test_success_restores_unbound_client_and_later_call_ignores_route_deadline(self):
        from scripts.collection_deadline import CollectionDeadline
        from scripts.fetch_dex_depth import RpcClient, collect_dex_pool_observation

        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

        clock = Clock()
        transport = ActualV2Transport()
        client = RpcClient(
            "eth",
            "https://rpc.example.test",
            request=transport,
        )
        route_deadline = CollectionDeadline.for_duration(
            1,
            clock=clock.monotonic,
            sleeper=lambda seconds: None,
        )
        row, _execution_rows = collect_dex_pool_observation(
            self.pool,
            snapshot_id="scoped-route-deadline",
            raw_path=self.root / "scoped-route-deadline.json",
            client=client,
            fixed_block_number=123,
            fixed_block_timestamp="2024-01-01T00:00:00+00:00",
            deadline=route_deadline,
        )

        self.assertEqual(row["status"], "observed")
        self.assertIsNone(client.deadline)
        self.assertIsNone(client._call_deadline)
        clock.now = 2.0
        self.assertEqual(client.method("ordinary_later_call", []), "0x1")
        self.assertIsNone(transport.calls[-1]["deadline"])

    def test_exception_restores_supplied_clients_preexisting_deadline(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_dex_depth import RpcClient, collect_dex_pool_observation

        class Clock:
            now = 0.0

            def monotonic(self):
                return self.now

        clock = Clock()
        preexisting_deadline = CollectionDeadline.for_duration(300)
        route_deadline = CollectionDeadline.for_duration(
            1,
            clock=clock.monotonic,
            sleeper=lambda seconds: None,
        )
        transport = ActualV2Transport(after_batch=lambda: setattr(clock, "now", 2.0))
        client = RpcClient(
            "eth",
            "https://rpc.example.test",
            request=transport,
            deadline=preexisting_deadline,
        )

        with self.assertRaises(CollectionDeadlineExceeded):
            collect_dex_pool_observation(
                self.pool,
                snapshot_id="restore-after-exception",
                raw_path=self.root / "restore-after-exception.json",
                client=client,
                fixed_block_number=123,
                fixed_block_timestamp="2024-01-01T00:00:00+00:00",
                deadline=route_deadline,
            )

        self.assertIs(client.deadline, preexisting_deadline)
        self.assertIs(client._call_deadline, preexisting_deadline)


class RpcEndpointFailoverTest(unittest.TestCase):
    def _endpoints(self):
        from scripts.fetch_dex_depth import RpcEndpoint

        return (
            RpcEndpoint(
                "eth-primary",
                "https://primary.example.test/secret?token=primary-secret",
            ),
            RpcEndpoint(
                "eth-fallback-1",
                "https://fallback.example.test/secret?token=fallback-secret",
            ),
        )

    def _result(self, payload):
        response = {
            "jsonrpc": "2.0",
            "id": payload["id"],
            "result": "0x1",
        }
        return response, json.dumps(response).encode("utf-8")

    def test_endpoint_configuration_preserves_legacy_primary_and_orders_two_fallbacks(self):
        from scripts.fetch_dex_depth import rpc_endpoints_for_chain, rpc_url_for_chain

        with patch.dict(os.environ, {}, clear=True):
            legacy = rpc_endpoints_for_chain("eth")
            self.assertEqual(len(legacy), 1)
            self.assertEqual(legacy[0].endpoint_id, "eth-primary")
            self.assertEqual(legacy[0].url, rpc_url_for_chain("eth"))

        with patch.dict(
            os.environ,
            {
                "DEX_DEPTH_RPC_ETH": "https://primary.example.test/rpc",
                "DEX_DEPTH_RPC_ETH_FALLBACKS": json.dumps(
                    [
                        "https://fallback-one.example.test/rpc",
                        "https://fallback-two.example.test/rpc",
                    ]
                ),
            },
            clear=True,
        ):
            endpoints = rpc_endpoints_for_chain("eth")

        self.assertEqual(
            [(item.endpoint_id, item.url) for item in endpoints],
            [
                ("eth-primary", "https://primary.example.test/rpc"),
                ("eth-fallback-1", "https://fallback-one.example.test/rpc"),
                ("eth-fallback-2", "https://fallback-two.example.test/rpc"),
            ],
        )

    def test_endpoint_configuration_rejects_invalid_fallback_arrays(self):
        from scripts.fetch_dex_depth import rpc_endpoints_for_chain

        invalid_values = (
            "{",
            json.dumps("https://fallback.example.test/rpc"),
            json.dumps([None]),
            json.dumps([""]),
            json.dumps(["https://primary.example.test/rpc"]),
            json.dumps(
                [
                    "https://one.example.test/rpc",
                    "https://two.example.test/rpc",
                    "https://three.example.test/rpc",
                ]
            ),
        )
        for fallback_value in invalid_values:
            with self.subTest(fallback_value=fallback_value), patch.dict(
                os.environ,
                {
                    "DEX_DEPTH_RPC_ETH": "https://primary.example.test/rpc",
                    "DEX_DEPTH_RPC_ETH_FALLBACKS": fallback_value,
                },
                clear=True,
            ):
                with self.assertRaisesRegex(ValueError, "fallback"):
                    rpc_endpoints_for_chain("eth")

    def test_http_403_immediately_switches_to_next_endpoint_and_records_no_secret(self):
        from scripts.fetch_dex_depth import RpcClient

        calls = []

        def request(url, payload):
            calls.append(url)
            if "primary" in url:
                raise urllib.error.HTTPError(url, 403, "private failure", {}, None)
            return self._result(payload)

        client = RpcClient(
            "eth",
            self._endpoints()[0].url,
            endpoints=self._endpoints(),
            request=request,
            sleeper=lambda _seconds: self.fail("403 must not retry"),
        )

        self.assertEqual(client.method("eth_chainId", []), "0x1")
        self.assertEqual(calls, [self._endpoints()[0].url, self._endpoints()[1].url])
        self.assertEqual(client.open_endpoint_ids, ("eth-primary",))
        self.assertEqual(client.endpoint_generation, 1)
        self.assertTrue(client.endpoint_attempts)
        retained = json.dumps(client.endpoint_attempts, sort_keys=True)
        self.assertNotIn("primary-secret", retained)
        self.assertNotIn("fallback-secret", retained)
        self.assertNotIn("private failure", retained)

    def test_fallback_must_prove_chain_and_exact_fixed_block_before_serving_call(self):
        from scripts.fetch_dex_depth import RpcClient, RpcError

        expected_block = {
            "number": "0x7b",
            "hash": "0x" + "a" * 64,
            "timestamp": "0x65920080",
        }
        cases = {
            "wrong_chain": {"chain_id": "0x38", "block": expected_block},
            "missing_block": {"chain_id": "0x1", "block": None},
            "wrong_hash": {
                "chain_id": "0x1",
                "block": {**expected_block, "hash": "0x" + "c" * 64},
            },
            "wrong_timestamp": {
                "chain_id": "0x1",
                "block": {**expected_block, "timestamp": "0x65920081"},
            },
        }
        for name, fallback in cases.items():
            calls = []
            fail_primary = [False]

            def request(url, payload):
                method = payload["method"]
                calls.append((url, method))
                if fail_primary[0] and "primary" in url:
                    raise urllib.error.HTTPError(
                        url,
                        403,
                        "private provider error " + url,
                        {},
                        None,
                    )
                if method == "eth_chainId":
                    result = fallback["chain_id"] if "fallback" in url else "0x1"
                elif method == "eth_getBlockByNumber":
                    result = fallback["block"] if "fallback" in url else expected_block
                elif method == "eth_call":
                    result = "0x1"
                else:
                    raise AssertionError(method)
                response = {"jsonrpc": "2.0", "id": payload["id"], "result": result}
                return response, json.dumps(response).encode("utf-8")

            with self.subTest(name=name):
                client = RpcClient(
                    "eth",
                    self._endpoints()[0].url,
                    endpoints=self._endpoints(),
                    request=request,
                )
                client.bind_fixed_block_identity(
                    chain_id=1,
                    block={
                        "number": "123",
                        "hash": expected_block["hash"],
                        "timestamp": "2024-01-01T00:00:00+00:00",
                    },
                )
                fail_primary[0] = True
                with self.assertRaises(RpcError):
                    client.method("eth_call", [])
                self.assertNotIn(
                    (self._endpoints()[1].url, "eth_call"),
                    calls,
                )
                retained = json.dumps(client.attempt_ledger, sort_keys=True)
                for secret in (
                    "primary-secret",
                    "fallback-secret",
                    "private provider error",
                ):
                    self.assertNotIn(secret, retained)

    def test_429_and_5xx_retry_one_endpoint_before_switching(self):
        from scripts.fetch_dex_depth import RpcClient

        for status in (429, 503):
            calls = []
            sleeps = []

            def request(url, payload):
                calls.append(url)
                if "primary" in url:
                    raise urllib.error.HTTPError(url, status, "private", {}, None)
                return self._result(payload)

            with self.subTest(status=status):
                client = RpcClient(
                    "eth",
                    self._endpoints()[0].url,
                    endpoints=self._endpoints(),
                    request=request,
                    sleeper=sleeps.append,
                )
                self.assertEqual(client.method("eth_chainId", []), "0x1")
                self.assertEqual(calls[:4], [self._endpoints()[0].url] * 4)
                self.assertEqual(calls[4:], [self._endpoints()[1].url])
                self.assertEqual(sleeps, [1.0, 2.0, 4.0])

    def test_urlerror_and_direct_timeout_retry_then_open_run_scoped_breaker(self):
        from scripts.fetch_dex_depth import RpcClient

        for error_factory in (
            lambda: urllib.error.URLError("private dns failure"),
            lambda: TimeoutError("private timeout failure"),
        ):
            calls = []

            def request(url, payload):
                calls.append(url)
                if "primary" in url:
                    raise error_factory()
                return self._result(payload)

            with self.subTest(error=error_factory().__class__.__name__):
                client = RpcClient(
                    "eth",
                    self._endpoints()[0].url,
                    endpoints=self._endpoints(),
                    request=request,
                    sleeper=lambda _seconds: None,
                )
                self.assertEqual(client.method("eth_chainId", []), "0x1")
                first_call_count = len(calls)
                self.assertEqual(client.method("eth_blockNumber", []), "0x1")
                self.assertEqual(first_call_count, 5)
                self.assertEqual(calls, [self._endpoints()[0].url] * 4 + [self._endpoints()[1].url] * 2)
                self.assertEqual(client.open_endpoint_ids, ("eth-primary",))

    def test_contract_revert_never_hops_to_another_provider(self):
        from scripts.fetch_dex_depth import RpcClient, RpcError

        calls = []

        def request(url, payload):
            calls.append(url)
            response = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "error": {"code": 3, "message": "private revert"},
            }
            return response, json.dumps(response).encode("utf-8")

        client = RpcClient(
            "eth",
            self._endpoints()[0].url,
            endpoints=self._endpoints(),
            request=request,
        )
        with self.assertRaisesRegex(RpcError, "eth_call failed code=3"):
            client.method("eth_call", [])
        self.assertEqual(calls, [self._endpoints()[0].url])
        self.assertEqual(client.open_endpoint_ids, ())

    def test_exhaustion_uses_bounded_error_and_sanitized_attempt_ledger(self):
        from scripts.fetch_dex_depth import RpcClient, RpcError

        def request(url, _payload):
            raise urllib.error.HTTPError(url, 403, "private provider failure", {}, None)

        client = RpcClient(
            "eth",
            self._endpoints()[0].url,
            endpoints=self._endpoints(),
            request=request,
        )
        with self.assertRaisesRegex(RpcError, "^rpc_endpoint_exhausted$") as raised:
            client.method("eth_chainId", [])
        retained = json.dumps(client.endpoint_attempts, sort_keys=True)
        self.assertEqual(len(client.endpoint_attempts), 2)
        self.assertNotIn("primary-secret", str(raised.exception))
        self.assertNotIn("fallback-secret", str(raised.exception))
        self.assertNotIn("private provider failure", retained)
        self.assertNotIn("primary-secret", retained)
        self.assertNotIn("fallback-secret", retained)

    def test_http_json_rpc_retries_direct_timeouterror(self):
        from scripts.fetch_dex_depth import RpcTransportError, http_json_rpc

        attempts = []

        def timeout(*_args, **_kwargs):
            attempts.append("timeout")
            raise TimeoutError("private timeout")

        with patch("urllib.request.urlopen", timeout), patch("time.sleep") as sleep:
            with self.assertRaisesRegex(
                RpcTransportError,
                "^rpc_transport_failed$",
            ) as raised:
                http_json_rpc(
                    "https://rpc.example.test",
                    {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
                    max_retries=2,
                )
        self.assertEqual(attempts, ["timeout", "timeout"])
        sleep.assert_called_once_with(1.0)
        self.assertEqual(raised.exception.outcome, "timeout")
        self.assertNotIn("private timeout", str(raised.exception))

    def test_malformed_scalar_fallback_and_injected_urls_fail_without_echo(self):
        from scripts.fetch_dex_depth import (
            RpcClient,
            RpcConfigurationError,
            RpcEndpoint,
            rpc_endpoints_for_chain,
        )

        malformed = "https://user:password@example.test:bad/rpc?api_key=private"
        cases = (
            {
                "DEX_DEPTH_RPC_ETH": malformed,
            },
            {
                "DEX_DEPTH_RPC_ETH": "https://rpc.example.test",
                "DEX_DEPTH_RPC_ETH_FALLBACKS": json.dumps([malformed]),
            },
            {
                "DEX_DEPTH_RPC_ETH": (
                    "ftp://user:password@example.test/rpc?api_key=private"
                ),
            },
        )
        for environment in cases:
            with self.subTest(environment=tuple(environment)), patch.dict(
                os.environ,
                environment,
                clear=True,
            ):
                with self.assertRaisesRegex(
                    RpcConfigurationError,
                    "^invalid_rpc_endpoint$",
                ) as raised:
                    rpc_endpoints_for_chain("eth")
                self.assertNotIn("password", str(raised.exception))
                self.assertNotIn("api_key", str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)

        for build in (
            lambda: RpcEndpoint("eth-primary", malformed),
            lambda: RpcClient("eth", malformed),
            lambda: RpcClient("eth", malformed, endpoints=self._endpoints()),
        ):
            with self.subTest(build=build):
                with self.assertRaisesRegex(
                    RpcConfigurationError,
                    "^invalid_rpc_endpoint$",
                ) as raised:
                    build()
                self.assertNotIn("password", str(raised.exception))
                self.assertNotIn("api_key", str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)

    def test_retry_configuration_cannot_exceed_four_attempts(self):
        from scripts.fetch_dex_depth import (
            RpcConfigurationError,
            http_json_rpc,
        )

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_chainId",
            "params": [],
        }
        for invalid_retries in (True, None, 5):
            with self.subTest(invalid_retries=invalid_retries), patch(
                "urllib.request.urlopen"
            ) as urlopen:
                with self.assertRaisesRegex(
                    RpcConfigurationError,
                    "^invalid_rpc_retry_configuration$",
                ):
                    http_json_rpc(
                        "https://rpc.example.test",
                        payload,
                        max_retries=invalid_retries,
                    )
                urlopen.assert_not_called()

    def test_request_construction_failure_is_bounded_and_suppresses_raw_context(self):
        from scripts.fetch_dex_depth import RpcConfigurationError, http_json_rpc

        with patch(
            "urllib.request.Request",
            side_effect=ValueError("private URL query and header failure"),
        ):
            with self.assertRaisesRegex(
                RpcConfigurationError,
                "^invalid_rpc_request$",
            ) as raised:
                http_json_rpc(
                    "https://rpc.example.test",
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_chainId",
                        "params": [],
                    },
                )

        self.assertNotIn("private", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)

    def test_non_hop_http_error_is_typed_bounded_and_does_not_switch(self):
        from scripts.fetch_dex_depth import RpcClient, RpcTransportError

        calls = []

        def request(url, _payload):
            calls.append(url)
            raise urllib.error.HTTPError(
                url,
                418,
                "private exception reason",
                {"X-Private": "secret-header"},
                None,
            )

        client = RpcClient(
            "eth",
            self._endpoints()[0].url,
            endpoints=self._endpoints(),
            request=request,
        )
        with self.assertRaisesRegex(
            RpcTransportError,
            "^rpc_transport_failed$",
        ) as raised:
            client.method("eth_chainId", [])

        self.assertEqual(calls, [self._endpoints()[0].url])
        self.assertEqual(client.open_endpoint_ids, ())
        self.assertEqual(client.endpoint_generation, 0)
        retained = json.dumps(client.endpoint_attempts, sort_keys=True)
        for private_value in (
            "primary-secret",
            "fallback-secret",
            "private exception reason",
            "secret-header",
        ):
            self.assertNotIn(private_value, str(raised.exception))
            self.assertNotIn(private_value, retained)
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)

    def test_collection_deadline_exception_never_retries_opens_or_hops(self):
        from scripts.collection_deadline import CollectionDeadlineExceeded
        from scripts.fetch_dex_depth import RpcClient

        calls = []

        def request(url, _payload):
            calls.append(url)
            raise CollectionDeadlineExceeded("collection deadline exceeded")

        client = RpcClient(
            "eth",
            self._endpoints()[0].url,
            endpoints=self._endpoints(),
            request=request,
        )
        with self.assertRaisesRegex(
            CollectionDeadlineExceeded,
            "^collection deadline exceeded$",
        ):
            client.method("eth_chainId", [])

        self.assertEqual(calls, [self._endpoints()[0].url])
        self.assertEqual(client.open_endpoint_ids, ())
        self.assertEqual(client.endpoint_generation, 0)

    def test_expired_deadline_aborts_before_retry_or_endpoint_switch(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_dex_depth import RpcClient

        for failure in (
            urllib.error.URLError("private DNS failure"),
            urllib.error.HTTPError(
                self._endpoints()[0].url,
                403,
                "private forbidden",
                {},
                None,
            ),
        ):
            class Clock:
                now = 0.0

                def monotonic(self):
                    return self.now

            clock = Clock()
            calls = []

            def request(url, _payload, **_kwargs):
                calls.append(url)
                clock.now = 2.0
                raise failure

            with self.subTest(failure=failure.__class__.__name__):
                client = RpcClient(
                    "eth",
                    self._endpoints()[0].url,
                    endpoints=self._endpoints(),
                    request=request,
                    deadline=CollectionDeadline.for_duration(
                        1,
                        clock=clock.monotonic,
                        sleeper=lambda _seconds: None,
                    ),
                )
                with self.assertRaisesRegex(
                    CollectionDeadlineExceeded,
                    "^collection deadline exceeded$",
                ):
                    client.method("eth_chainId", [])
                self.assertEqual(calls, [self._endpoints()[0].url])
                self.assertEqual(client.open_endpoint_ids, ())
                self.assertEqual(client.endpoint_generation, 0)

    def test_real_default_transport_has_one_four_attempt_owner_and_exact_ledger(self):
        from scripts.fetch_dex_depth import RpcClient

        calls = []

        class Response:
            def __init__(self, payload):
                self.payload = payload

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return self.payload

        def urlopen(request, **_kwargs):
            calls.append(request.full_url)
            if "primary" in request.full_url:
                raise urllib.error.HTTPError(
                    request.full_url,
                    503,
                    "private outage",
                    {},
                    None,
                )
            return Response(b'{"jsonrpc":"2.0","id":1,"result":"0x1"}')

        client = RpcClient(
            "eth",
            self._endpoints()[0].url,
            endpoints=self._endpoints(),
            sleeper=lambda _seconds: None,
        )
        with patch("urllib.request.urlopen", urlopen), patch("time.sleep"):
            self.assertEqual(client.method("eth_chainId", []), "0x1")

        self.assertEqual(
            calls,
            [self._endpoints()[0].url] * 4 + [self._endpoints()[1].url],
        )
        self.assertEqual(len(client.endpoint_attempts), 5)
        self.assertEqual(
            [record["attempt_ordinal"] for record in client.endpoint_attempts],
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(
            [record["decision"] for record in client.endpoint_attempts],
            ["retry", "retry", "retry", "switch", "use"],
        )

    def test_explicit_http_json_rpc_has_one_four_attempt_owner_and_exact_ledger(self):
        from scripts.fetch_dex_depth import RpcClient, RpcError, http_json_rpc

        calls = []
        url = "https://rpc.example.test"

        def unavailable(request, **_kwargs):
            calls.append(request.full_url)
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "private provider outage",
                {},
                None,
            )

        client = RpcClient(
            "eth",
            url,
            request=http_json_rpc,
            sleeper=lambda _seconds: None,
        )
        with patch("urllib.request.urlopen", unavailable), patch("time.sleep"):
            with self.assertRaisesRegex(RpcError, "^rpc_endpoint_exhausted$"):
                client.method("eth_chainId", [])

        self.assertEqual(calls, [url] * 4)
        self.assertEqual(len(client.endpoint_attempts), 4)
        self.assertEqual(
            [record["attempt_ordinal"] for record in client.endpoint_attempts],
            [1, 2, 3, 4],
        )
        self.assertEqual(
            [record["endpoint_attempt"] for record in client.endpoint_attempts],
            [1, 2, 3, 4],
        )
        self.assertEqual(
            [record["decision"] for record in client.endpoint_attempts],
            ["retry", "retry", "retry", "exhausted"],
        )

    def test_wrapped_retry_aware_request_receives_single_attempt_controls(self):
        import functools

        from scripts.fetch_dex_depth import RpcClient, http_json_rpc

        observed = []

        @functools.wraps(http_json_rpc)
        def wrapped(url, payload, **kwargs):
            observed.append((url, dict(kwargs)))
            return self._result(payload)

        client = RpcClient(
            "eth",
            "https://rpc.example.test",
            request=wrapped,
        )
        self.assertEqual(client.method("eth_chainId", []), "0x1")
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0][0], "https://rpc.example.test")
        self.assertEqual(
            observed[0][1],
            {
                "deadline": None,
                "timeout_seconds": 30.0,
                "max_retries": 1,
            },
        )

    def test_two_positional_wraps_boundary_uses_actual_call_signature(self):
        import functools

        from scripts.fetch_dex_depth import RpcClient, http_json_rpc

        calls = []

        @functools.wraps(http_json_rpc)
        def wrapped(url, payload):
            calls.append((url, payload))
            return self._result(payload)

        client = RpcClient(
            "eth",
            "https://rpc.example.test",
            request=wrapped,
        )
        self.assertEqual(client.method("eth_chainId", []), "0x1")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "https://rpc.example.test")
        self.assertEqual(calls[0][1]["method"], "eth_chainId")

    def test_legacy_two_positional_request_keeps_exact_call_shape(self):
        from scripts.fetch_dex_depth import RpcClient

        calls = []

        def legacy_request(url, payload, /):
            calls.append((url, payload))
            return self._result(payload)

        client = RpcClient(
            "eth",
            "https://rpc.example.test",
            request=legacy_request,
            timeout_seconds=7,
            max_retries=2,
        )
        self.assertEqual(client.method("eth_chainId", []), "0x1")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "https://rpc.example.test")
        self.assertEqual(calls[0][1]["method"], "eth_chainId")

    def test_uninspectable_request_boundary_fails_safely_without_invocation(self):
        from scripts.fetch_dex_depth import RpcClient, RpcConfigurationError

        class UninspectableRequest:
            calls = 0

            @property
            def __signature__(self):
                raise ValueError("private request signature")

            def __call__(self, _url, payload):
                self.calls += 1
                return self._result(payload)

        request = UninspectableRequest()
        with self.assertRaisesRegex(
            RpcConfigurationError,
            "^invalid_rpc_request_boundary$",
        ) as raised:
            RpcClient(
                "eth",
                "https://rpc.example.test",
                request=request,
            )

        self.assertEqual(request.calls, 0)
        self.assertNotIn("private", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)

    def test_401_and_404_switch_immediately_without_retry(self):
        from scripts.fetch_dex_depth import RpcClient

        for status in (401, 404):
            calls = []

            def request(url, payload):
                calls.append(url)
                if "primary" in url:
                    raise urllib.error.HTTPError(url, status, "private", {}, None)
                return self._result(payload)

            with self.subTest(status=status):
                client = RpcClient(
                    "eth",
                    self._endpoints()[0].url,
                    endpoints=self._endpoints(),
                    request=request,
                    sleeper=lambda _seconds: self.fail("must not retry"),
                )
                self.assertEqual(client.method("eth_chainId", []), "0x1")
                self.assertEqual(
                    calls,
                    [self._endpoints()[0].url, self._endpoints()[1].url],
                )

    def test_terminal_exhaustion_does_not_advance_selected_generation(self):
        from scripts.fetch_dex_depth import RpcClient, RpcError

        def request(url, _payload):
            raise urllib.error.HTTPError(url, 403, "private", {}, None)

        client = RpcClient(
            "eth",
            self._endpoints()[0].url,
            endpoints=self._endpoints(),
            request=request,
        )
        with self.assertRaisesRegex(RpcError, "^rpc_endpoint_exhausted$"):
            client.method("eth_chainId", [])

        self.assertEqual(client.endpoint_generation, 1)
        self.assertEqual(client.selected_endpoint_id, "eth-fallback-1")
        self.assertEqual(
            [record["decision"] for record in client.endpoint_attempts],
            ["switch", "exhausted"],
        )

    def test_attempt_ledger_has_a_hard_retention_cap(self):
        from scripts.fetch_dex_depth import (
            MAX_RPC_ATTEMPT_RECORDS,
            RpcClient,
        )

        client = RpcClient(
            "eth",
            "https://rpc.example.test",
            request=lambda _url, payload: self._result(payload),
        )
        for index in range(MAX_RPC_ATTEMPT_RECORDS + 3):
            self.assertEqual(client.method("method_{}".format(index), []), "0x1")

        self.assertEqual(len(client.endpoint_attempts), MAX_RPC_ATTEMPT_RECORDS)
        self.assertEqual(client.endpoint_attempts_dropped, 3)
        self.assertEqual(
            [record["attempt_ordinal"] for record in client.endpoint_attempts],
            list(range(1, MAX_RPC_ATTEMPT_RECORDS + 1)),
        )

    def test_standalone_http_json_rpc_retains_four_attempt_budget_with_typed_error(self):
        from scripts.fetch_dex_depth import RpcTransportError, http_json_rpc

        calls = []

        def unavailable(request, **_kwargs):
            calls.append(request.full_url)
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "private provider outage",
                {"X-Private": "header-secret"},
                None,
            )

        with patch("urllib.request.urlopen", unavailable), patch("time.sleep"):
            with self.assertRaisesRegex(
                RpcTransportError,
                "^rpc_transport_failed$",
            ) as raised:
                http_json_rpc(
                    "https://user:password@rpc.example.test/rpc?api_key=secret",
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_chainId",
                        "params": [],
                    },
                )

        self.assertEqual(len(calls), 4)
        self.assertNotIn("password", str(raised.exception))
        self.assertNotIn("api_key", str(raised.exception))
        self.assertNotIn("header-secret", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertTrue(raised.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main()
