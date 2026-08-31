"""Collector contract tests for the two-pool Uniswap V3 exact-swap MVP."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.execution_cost import (
    EXECUTION_DIRECTIONS,
    EXECUTION_NOTIONALS_USD,
    validate_execution_snapshot,
)
from scripts.fetch_dex_depth import (
    SELECTOR_DECIMALS,
    SELECTOR_FACTORY,
    SELECTOR_FACTORY_GET_POOL,
    SELECTOR_FEE,
    SELECTOR_LIQUIDITY,
    SELECTOR_QUOTE_EXACT_INPUT_SINGLE_V2,
    SELECTOR_QUOTE_EXACT_OUTPUT_SINGLE_V2,
    SELECTOR_SLOT0,
    SELECTOR_SYMBOL,
    SELECTOR_TICK_BITMAP,
    SELECTOR_TICK_SPACING,
    SELECTOR_TICKS,
    SELECTOR_TOKEN0,
    SELECTOR_TOKEN1,
    collect_dex_pool_observation,
    collect_dex_depth_with_execution,
    observed_pool_row,
)
from scripts.uniswap_v3_math import get_sqrt_ratio_at_tick


UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
QUOTER_V2 = "0x61ffe014ba17989e743c5f6cb21bf9697530b21e"
UNI_USDT_POOL = "0x3470447f3cecffac709d3e783a307790b0208d60"
BLOCK_NUMBER = 123
BLOCK_TAG = "0x7b"
BLOCK_HASH = "0x" + "a" * 64
CURRENT_TICK = -1
ACTIVE_LIQUIDITY = 10**7
FROZEN_QUOTER_CALL = (
    "0xc6a5026a"
    "0000000000000000000000001f9840a85d5af5bf1d1762f925bdaddc4201f984"
    "000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7"
    "000000000000000000000000000000000000000000000000000000003b9c509f"
    "0000000000000000000000000000000000000000000000000000000000000bb8"
    "0000000000000000000000000000000000000000008cb45b3c8084cd925c6929"
)
FROZEN_QUOTER_RESULT = (
    "0x"
    "00000000000000000000000000000000000000000000000000000000009710ac"
    "0000000000000000000000000000000000000000028abd57bf013f8e0298dd35"
    "0000000000000000000000000000000000000000000000000000000000000002"
    "00000000000000000000000000000000000000000000000000000000000186a0"
)


def _word(value):
    return f"{value % (1 << 256):064x}"


def _uint_result(*values):
    return "0x" + "".join(_word(value) for value in values)


def _address_result(address):
    return "0x" + ("0" * 24) + address[2:].lower()


def _string_result(value):
    encoded = value.encode("utf-8")
    padded = encoded.hex().ljust(((len(encoded) + 31) // 32) * 64, "0")
    return "0x" + _word(32) + _word(len(encoded)) + padded


def _signed_argument(data):
    value = int(data[-64:], 16)
    return value - (1 << 256) if value >= 1 << 255 else value


def _header():
    return {
        "number": BLOCK_TAG,
        "hash": BLOCK_HASH,
        "parentHash": "0x" + "b" * 64,
        "timestamp": "0x65920080",
        "baseFeePerGas": "0x3b9aca00",
        "gasUsed": "0xe4e1c0",
        "gasLimit": "0x1c9c380",
    }


def _pool(pool_address=UNI_USDT_POOL):
    return {
        "market_id": f"dex:eth:uniswap_v3:{pool_address}:UNI",
        "snapshot_id": "tvl-v3-fixture",
        "observed_at": "2024-01-01T00:00:00+00:00",
        "response_received_at": "2024-01-01T00:00:00+00:00",
        "token_symbol": "UNI",
        "chain": "eth",
        "dex": "uniswap_v3",
        "pool_address": pool_address,
        "pool_name": "UNI / USDT 0.3%",
        "base_token_id": f"eth_{UNI}",
        "quote_token_id": f"eth_{USDT}",
        # sqrtPriceX96 at tick -1 with 18/6 decimals is approximately 1e12
        # USDT per UNI.  The fixture intentionally uses that exact scale so
        # execution notionals exercise base-unit rounding and scan exhaustion.
        "base_token_price_usd": "999900009999.00009999",
        "quote_token_price_usd": "1",
        "source": "GeckoTerminal API v2",
        "source_endpoint": (
            "https://api.geckoterminal.com/api/v2/networks/eth/pools/multi/"
            + pool_address
        ),
        "raw_response_sha256": "d" * 64,
        "status": "observed",
    }


class FakeApprovedUniUsdtV3Rpc:
    """Fixed-block fixture with initialized ticks across a bitmap boundary."""

    def __init__(self, pool_address=UNI_USDT_POOL):
        self.pool_address = pool_address.lower()
        self.endpoint = "https://rpc.example.test"
        self.records = []
        self.chain_id_calls = 0
        self.block_calls = 0

    def block_number(self):
        raise AssertionError("the supplied fixed block must be used")

    def chain_id(self):
        self.chain_id_calls += 1
        self.records.append(
            {"request": {"method": "eth_chainId"}, "response": "0x1"}
        )
        return "0x1"

    def block(self, block_tag):
        if block_tag not in {BLOCK_TAG, "finalized"}:
            raise AssertionError(block_tag)
        self.block_calls += 1
        response = _header()
        self.records.append(
            {
                "request": {
                    "method": "eth_getBlockByNumber",
                    "block": block_tag,
                },
                "response": response,
            }
        )
        return response

    def eth_calls(self, to, data_values, block_tag):
        if block_tag != BLOCK_TAG:
            raise AssertionError("all state evidence must use the fixed block")
        normalized_to = to.lower()
        self.records.append(
            {
                "request": {
                    "to": normalized_to,
                    "data": list(data_values),
                    "block": block_tag,
                },
                "response": "fixture",
            }
        )
        return [self._response(normalized_to, data) for data in data_values]

    def _response(self, to, data):
        if to == self.pool_address:
            if data == SELECTOR_TOKEN0:
                return _address_result(UNI)
            if data == SELECTOR_TOKEN1:
                return _address_result(USDT)
            if data == SELECTOR_SLOT0:
                return _uint_result(
                    get_sqrt_ratio_at_tick(CURRENT_TICK),
                    CURRENT_TICK,
                    0,
                    0,
                    0,
                    0,
                    1,
                )
            if data == SELECTOR_LIQUIDITY:
                return _uint_result(ACTIVE_LIQUIDITY)
            if data == SELECTOR_FEE:
                return _uint_result(3000)
            if data == SELECTOR_TICK_SPACING:
                return _uint_result(60)
            if data == SELECTOR_FACTORY:
                return _address_result(FACTORY)
            if data.startswith(SELECTOR_TICK_BITMAP):
                word_position = _signed_argument(data)
                bitmap = {
                    # compressed tick -1 is bit 255 of signed word -1.
                    -1: 1 << 255,
                    # The next initialized tick crosses into bitmap word 0.
                    0: 1,
                }.get(word_position, 0)
                return _uint_result(bitmap)
            if data.startswith(SELECTOR_TICKS):
                tick = _signed_argument(data)
                if tick == -60:
                    return _uint_result(ACTIVE_LIQUIDITY, 0)
                if tick == 0:
                    return _uint_result(ACTIVE_LIQUIDITY, 0)
                raise AssertionError(f"unadvertised initialized tick: {tick}")
        if to == FACTORY and data.startswith(SELECTOR_FACTORY_GET_POOL):
            return _address_result(self.pool_address)
        if to == QUOTER_V2 and data[:10] in {
            SELECTOR_QUOTE_EXACT_INPUT_SINGLE_V2,
            SELECTOR_QUOTE_EXACT_OUTPUT_SINGLE_V2,
        }:
            if data != FROZEN_QUOTER_CALL:
                raise AssertionError("unexpected QuoterV2 fixture call")
            return FROZEN_QUOTER_RESULT
        if to == UNI:
            if data == SELECTOR_DECIMALS:
                return _uint_result(18)
            if data == SELECTOR_SYMBOL:
                return _string_result("UNI")
        if to == USDT:
            if data == SELECTOR_DECIMALS:
                return _uint_result(6)
            if data == SELECTOR_SYMBOL:
                return _string_result("USDT")
        raise AssertionError((to, data))


class MismatchingQuoterV3Rpc(FakeApprovedUniUsdtV3Rpc):
    def _response(self, to, data):
        result = super()._response(to, data)
        if to == QUOTER_V2:
            values = [result[index:index + 64] for index in range(2, len(result), 64)]
            values[0] = _word(int(values[0], 16) + 1)
            return "0x" + "".join(values)
        return result


class TruncatedQuoterV3Rpc(FakeApprovedUniUsdtV3Rpc):
    def _response(self, to, data):
        result = super()._response(to, data)
        if to == QUOTER_V2:
            return result[:-64]
        return result


class UnframedQuoterV3Rpc(FakeApprovedUniUsdtV3Rpc):
    def _response(self, to, data):
        result = super()._response(to, data)
        if to == QUOTER_V2:
            return result[2:]
        return result


class ReorgAfterQuoterV3Rpc(FakeApprovedUniUsdtV3Rpc):
    def __init__(self):
        super().__init__()
        self.quoter_called = False

    def eth_calls(self, to, data_values, block_tag):
        result = super().eth_calls(to, data_values, block_tag)
        if to.lower() == QUOTER_V2:
            self.quoter_called = True
        return result

    def block(self, block_tag):
        result = super().block(block_tag)
        if self.quoter_called:
            result["hash"] = "0x" + "c" * 64
        return result


class FinalizedHashMismatchV3Rpc(FakeApprovedUniUsdtV3Rpc):
    def block(self, block_tag):
        result = super().block(block_tag)
        if block_tag == "finalized":
            result["hash"] = "0x" + "e" * 64
        return result


class AdvancingFinalizedV3Rpc(FakeApprovedUniUsdtV3Rpc):
    def block(self, block_tag):
        result = super().block(block_tag)
        if block_tag == "finalized":
            result.update(
                {
                    "number": "0x9b",
                    "hash": "0x" + "c" * 64,
                    "timestamp": "0x65920880",
                }
            )
        return result


class OrderedTwoPoolV3Rpc(FakeApprovedUniUsdtV3Rpc):
    def __init__(self, secondary_pool):
        super().__init__()
        self.secondary_pool = secondary_pool.lower()

    def _response(self, to, data):
        if to != self.secondary_pool:
            return super()._response(to, data)
        primary_pool = self.pool_address
        self.pool_address = self.secondary_pool
        try:
            return super()._response(to, data)
        finally:
            self.pool_address = primary_pool


class MixedFinalizedCohortV3Rpc(OrderedTwoPoolV3Rpc):
    def __init__(self, secondary_pool):
        super().__init__(secondary_pool)
        self.finalized_calls = 0

    def block(self, block_tag):
        result = super().block(block_tag)
        if block_tag == "finalized":
            self.finalized_calls += 1
            if self.finalized_calls > 1:
                result["hash"] = "0x" + "e" * 64
        return result


class ResponseEventMixin:
    def __init__(self, events):
        super().__init__()
        self.events = events

    def block(self, block_tag):
        result = super().block(block_tag)
        self.events.append(("block", block_tag))
        return result

    def eth_calls(self, to, data_values, block_tag):
        result = super().eth_calls(to, data_values, block_tag)
        if to.lower() == QUOTER_V2:
            self.events.append(("quoter", block_tag))
        return result


class TimedApprovedV3Rpc(ResponseEventMixin, FakeApprovedUniUsdtV3Rpc):
    pass


class TimedMismatchingV3Rpc(ResponseEventMixin, MismatchingQuoterV3Rpc):
    pass


class UniswapV3CollectionTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_approved_pool_collects_one_verified_window_for_depth_and_execution(self):
        client = FakeApprovedUniUsdtV3Rpc()
        raw_path = self.root / "approved-uni-usdt.json"

        depth, execution = collect_dex_pool_observation(
            _pool(),
            snapshot_id="approved-uni-usdt",
            raw_path=raw_path,
            client=client,
            fixed_block_number=BLOCK_NUMBER,
        )

        self.assertEqual(client.chain_id_calls, 1)
        self.assertGreaterEqual(
            client.block_calls,
            3,
            "the block header must be checked before state and after Quoter",
        )
        self.assertIn(depth["status"], {"observed", "partial"})
        self.assertEqual(depth["block_number"], str(BLOCK_NUMBER))
        self.assertEqual(len(execution), 10)
        self.assertEqual(
            {
                (row["direction"], row["requested_notional_usd"])
                for row in execution
            },
            {
                (direction, notional)
                for direction in EXECUTION_DIRECTIONS
                for notional in map(str, EXECUTION_NOTIONALS_USD)
            },
        )
        statuses = {row["status"] for row in execution}
        self.assertTrue(statuses <= {"observed", "partial"})
        self.assertIn("observed", statuses)
        self.assertIn("partial", statuses)
        self.assertTrue(
            all(row["block_number"] == str(BLOCK_NUMBER) for row in execution)
        )
        self.assertTrue(
            all(row["fee_status"] == "included_protocol_fee" for row in execution)
        )
        self.assertTrue(
            all(row["fee_rate_bps"] == "30" for row in execution)
        )
        for row in execution:
            if row["status"] == "observed":
                self.assertEqual(row["fill_ratio"], "1")
                self.assertNotEqual(row["quote_amount"], "")
                self.assertNotEqual(row["quoted_execution_cost_bps"], "")
            else:
                self.assertNotEqual(row["status_reason"], "")
                self.assertEqual(row["quoted_execution_cost_bps"], "")
        validate_execution_snapshot(
            {f"dex:eth:uniswap_v3:{UNI_USDT_POOL}:UNI"},
            execution,
        )

        raw_bytes = raw_path.read_bytes()
        raw = json.loads(raw_bytes)
        raw_hash = hashlib.sha256(raw_bytes).hexdigest()
        self.assertEqual(depth["raw_response_sha256"], raw_hash)
        self.assertTrue(
            all(row["raw_response_sha256"] == raw_hash for row in execution)
        )
        self.assertEqual(
            raw["usd_price_evidence"]["raw_response_sha256"],
            "d" * 64,
        )
        self.assertEqual(
            raw["usd_price_evidence"]["source"],
            "GeckoTerminal API v2",
        )

        manifest = raw["v3_tick_scan_manifest"]
        self.assertEqual(manifest["schema"], "uniswap_v3_tick_scan_manifest/v1")
        self.assertEqual(manifest["chain_id"], "0x1")
        self.assertEqual(manifest["block_number"], BLOCK_NUMBER)
        self.assertEqual(manifest["block_hash"], BLOCK_HASH)
        self.assertEqual(manifest["pool_address"], UNI_USDT_POOL)
        self.assertEqual(manifest["authority"]["chain_id"], 1)
        self.assertEqual(manifest["authority"]["pool_address"], UNI_USDT_POOL)
        self.assertEqual(manifest["block"]["number"], str(BLOCK_NUMBER))
        self.assertEqual(manifest["block"]["hash"], BLOCK_HASH)
        self.assertEqual(manifest["block_final"], manifest["block"])
        self.assertEqual(
            manifest["block"]["timestamp"],
            "2024-01-01T00:00:00+00:00",
        )
        word_positions = {
            item["word_position"] for item in manifest["bitmap_words"]
        }
        self.assertIn(-1, word_positions)
        self.assertIn(0, word_positions)
        ticks = {item["tick"] for item in manifest["tick_evidence"]}
        self.assertEqual(ticks, {-60, 0})
        self.assertEqual(
            set(manifest["directions"]),
            {"zero_for_one", "one_for_zero"},
        )
        self.assertTrue(
            all(
                direction["terminal_reason"]
                for direction in manifest["directions"].values()
            )
        )
        parity = manifest["quoter_v2_parity"]
        self.assertEqual(len(parity), 10)
        self.assertTrue(
            all(
                item["status"] in {
                    "exact_match",
                    "not_checked_partial_scan",
                }
                for item in parity
            )
        )
        self.assertIn("exact_match", {item["status"] for item in parity})
        self.assertEqual(
            {
                item["gas_estimate_raw"]
                for item in parity
                if item["status"] == "exact_match"
            },
            {100000},
        )

        rpc_requests = [record["request"] for record in raw["records"]]
        state_requests = [
            request
            for request in rpc_requests
            if isinstance(request, dict) and "to" in request
        ]
        self.assertTrue(state_requests)
        self.assertEqual({request["block"] for request in state_requests}, {BLOCK_TAG})
        all_calls = [
            (request["to"], data)
            for request in state_requests
            for data in request["data"]
        ]
        self.assertIn((UNI_USDT_POOL, SELECTOR_FACTORY), all_calls)
        self.assertTrue(
            any(
                to == FACTORY and data.startswith(SELECTOR_FACTORY_GET_POOL)
                for to, data in all_calls
            )
        )
        bitmap_words = {
            _signed_argument(data)
            for to, data in all_calls
            if to == UNI_USDT_POOL and data.startswith(SELECTOR_TICK_BITMAP)
        }
        self.assertIn(-1, bitmap_words)
        self.assertIn(0, bitmap_words)
        tick_calls = {
            _signed_argument(data)
            for to, data in all_calls
            if to == UNI_USDT_POOL and data.startswith(SELECTOR_TICKS)
        }
        self.assertEqual(tick_calls, {-60, 0})

    def test_unapproved_v3_pool_remains_structurally_unsupported_for_execution(self):
        unapproved = "0x" + "7" * 40
        client = FakeApprovedUniUsdtV3Rpc(unapproved)
        raw_path = self.root / "unapproved.json"

        depth, execution = collect_dex_pool_observation(
            _pool(unapproved),
            snapshot_id="unapproved-v3",
            raw_path=raw_path,
            client=client,
            fixed_block_number=BLOCK_NUMBER,
        )

        self.assertIn(depth["status"], {"observed", "partial"})
        self.assertEqual(len(execution), 10)
        self.assertEqual({row["status"] for row in execution}, {"unsupported"})
        self.assertTrue(all(row["error"] for row in execution))
        self.assertNotIn("v3_tick_scan_manifest", json.loads(raw_path.read_bytes()))

    def test_observed_pool_row_preserves_the_live_two_value_contract(self):
        result = observed_pool_row(
            _pool(),
            snapshot_id="two-value-contract",
            block_number=BLOCK_NUMBER,
            block_timestamp="2024-01-01T00:00:00+00:00",
            client=FakeApprovedUniUsdtV3Rpc(),
            request_started_at="2024-01-01T00:00:00+00:00",
            raw_response_sha256="",
            protocol="concentrated_liquidity_v3",
        )

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)

    def test_mixed_finalized_cohort_hash_fails_approved_pool_in_either_order(self):
        unapproved = "0x" + "7" * 40
        for approved_first in (False, True):
            with self.subTest(approved_first=approved_first):
                client = MixedFinalizedCohortV3Rpc(unapproved)
                ordered_pools = (
                    [_pool(), _pool(unapproved)]
                    if approved_first
                    else [_pool(unapproved), _pool()]
                )

                _snapshot_id, depth, _execution = collect_dex_depth_with_execution(
                    ordered_pools,
                    raw_root=self.root / ("mixed-" + str(approved_first)),
                    sleep_seconds=0,
                    rpc_factory=lambda _chain, _url: client,
                )

                approved = next(
                    row for row in depth if row["pool_address"] == UNI_USDT_POOL
                )
                self.assertEqual(approved["status"], "failed")
                self.assertIn("finalized block authority changed", approved["error"])
                state_requests = [
                    record["request"]
                    for record in client.records
                    if isinstance(record.get("request"), dict)
                    and "to" in record["request"]
                ]
                self.assertEqual(
                    {request["block"] for request in state_requests},
                    {BLOCK_TAG},
                )

    def test_approved_pool_makes_the_chain_cohort_finalized_regardless_of_order(self):
        unapproved = "0x" + "7" * 40
        client = OrderedTwoPoolV3Rpc(unapproved)

        _snapshot_id, depth, _execution = collect_dex_depth_with_execution(
            [_pool(unapproved), _pool()],
            raw_root=self.root / "ordered-cohort",
            sleep_seconds=0,
            rpc_factory=lambda _chain, _url: client,
        )

        self.assertEqual({row["block_number"] for row in depth}, {str(BLOCK_NUMBER)})
        self.assertTrue(
            all(row["status"] in {"observed", "partial"} for row in depth)
        )
        self.assertGreaterEqual(client.block_calls, 4)
        state_requests = [
            record["request"]
            for record in client.records
            if isinstance(record.get("request"), dict)
            and "to" in record["request"]
        ]
        self.assertEqual({request["block"] for request in state_requests}, {BLOCK_TAG})

    def test_approved_pool_requires_auditable_usd_price_lineage(self):
        for field in (
            "snapshot_id",
            "source",
            "source_endpoint",
            "raw_response_sha256",
        ):
            with self.subTest(field=field):
                pool = _pool()
                pool[field] = ""
                depth, execution = collect_dex_pool_observation(
                    pool,
                    snapshot_id="missing-usd-lineage-" + field,
                    raw_path=self.root / ("missing-" + field + ".json"),
                    client=FakeApprovedUniUsdtV3Rpc(),
                    fixed_block_number=BLOCK_NUMBER,
                )
                self.assertEqual(depth["status"], "failed")
                self.assertEqual({row["status"] for row in execution}, {"failed"})

    def test_same_block_quoter_mismatch_fails_execution_without_erasing_depth(self):
        client = MismatchingQuoterV3Rpc()
        raw_path = self.root / "mismatching-quoter.json"

        depth, execution = collect_dex_pool_observation(
            _pool(),
            snapshot_id="mismatching-quoter",
            raw_path=raw_path,
            client=client,
            fixed_block_number=BLOCK_NUMBER,
        )

        self.assertIn(depth["status"], {"observed", "partial"})
        self.assertEqual({row["status"] for row in execution}, {"failed"})
        self.assertTrue(
            all(
                row["status_reason"] == "execution_calculation_failed"
                for row in execution
            )
        )
        manifest = json.loads(raw_path.read_bytes())["v3_tick_scan_manifest"]
        self.assertIn("does not match same-block QuoterV2", manifest["execution_error"])

    def test_truncated_quoter_abi_fails_execution(self):
        depth, execution = collect_dex_pool_observation(
            _pool(),
            snapshot_id="truncated-quoter",
            raw_path=self.root / "truncated-quoter.json",
            client=TruncatedQuoterV3Rpc(),
            fixed_block_number=BLOCK_NUMBER,
        )

        self.assertIn(depth["status"], {"observed", "partial"})
        self.assertEqual({row["status"] for row in execution}, {"failed"})
        self.assertTrue(
            all("four words" in row["error"] for row in execution)
        )

    def test_unframed_quoter_abi_fails_execution(self):
        depth, execution = collect_dex_pool_observation(
            _pool(),
            snapshot_id="unframed-quoter",
            raw_path=self.root / "unframed-quoter.json",
            client=UnframedQuoterV3Rpc(),
            fixed_block_number=BLOCK_NUMBER,
        )

        self.assertIn(depth["status"], {"observed", "partial"})
        self.assertEqual({row["status"] for row in execution}, {"failed"})
        self.assertTrue(
            all("0x-prefixed" in row["error"] for row in execution)
        )

    def test_exact_rows_are_timestamped_after_final_block_response(self):
        for client_type, expected_execution_status in (
            (TimedApprovedV3Rpc, {"observed", "partial"}),
            (TimedMismatchingV3Rpc, {"failed"}),
        ):
            with self.subTest(client_type=client_type.__name__):
                events = []
                timestamps = iter(
                    (
                        "2024-01-01T00:00:01+00:00",
                        "2024-01-01T00:00:02+00:00",
                        "2024-01-01T00:00:03+00:00",
                    )
                )

                def next_timestamp():
                    value = next(timestamps)
                    events.append(("clock", value))
                    return value

                with patch(
                    "scripts.fetch_dex_depth.utc_now_text",
                    side_effect=next_timestamp,
                ):
                    depth, execution = collect_dex_pool_observation(
                        _pool(),
                        snapshot_id="final-response-time-" + client_type.__name__,
                        raw_path=self.root / (client_type.__name__ + ".json"),
                        client=client_type(events),
                        fixed_block_number=BLOCK_NUMBER,
                    )

                final_timestamp = "2024-01-01T00:00:03+00:00"
                self.assertEqual(depth["response_received_at"], final_timestamp)
                self.assertEqual(depth["observed_at"], final_timestamp)
                self.assertEqual(
                    {row["status"] for row in execution},
                    expected_execution_status,
                )
                self.assertTrue(
                    all(
                        row["response_received_at"] == final_timestamp
                        and row["observed_at"] == final_timestamp
                        for row in execution
                    )
                )
                final_clock_index = events.index(("clock", final_timestamp))
                last_rpc_index = max(
                    index
                    for index, event in enumerate(events)
                    if event[0] in {"block", "quoter"}
                )
                self.assertGreater(final_clock_index, last_rpc_index)

    def test_block_identity_is_rechecked_after_quoter_evidence(self):
        client = ReorgAfterQuoterV3Rpc()
        raw_path = self.root / "reorg-after-quoter.json"

        depth, execution = collect_dex_pool_observation(
            _pool(),
            snapshot_id="reorg-after-quoter",
            raw_path=raw_path,
            client=client,
            fixed_block_number=BLOCK_NUMBER,
        )

        self.assertEqual(depth["status"], "failed")
        self.assertEqual({row["status"] for row in execution}, {"failed"})
        self.assertIn("fixed block identity changed", depth["error"])
        self.assertIn(
            "fixed block identity changed",
            json.loads(raw_path.read_bytes())["error"],
        )

    def test_finalized_header_hash_must_match_numeric_state_block(self):
        raw_path = self.root / "finalized-hash-mismatch.json"

        depth, execution = collect_dex_pool_observation(
            _pool(),
            snapshot_id="finalized-hash-mismatch",
            raw_path=raw_path,
            client=FinalizedHashMismatchV3Rpc(),
            fixed_block_number=BLOCK_NUMBER,
        )

        self.assertEqual(depth["status"], "failed")
        self.assertEqual({row["status"] for row in execution}, {"failed"})
        self.assertIn("finalized block identity", depth["error"])

    def test_later_finalized_checkpoint_keeps_every_state_request_pinned(self):
        raw_path = self.root / "advancing-finality.json"

        depth, execution = collect_dex_pool_observation(
            _pool(),
            snapshot_id="advancing-finality",
            raw_path=raw_path,
            client=AdvancingFinalizedV3Rpc(),
            fixed_block_number=123,
        )

        self.assertIn(depth["status"], {"observed", "partial"})
        self.assertTrue(
            all(row["status"] in {"observed", "partial"} for row in execution)
        )
        raw = json.loads(raw_path.read_bytes())
        finalized_headers = [
            record["response"]
            for record in raw["records"]
            if record.get("request", {}).get("method") == "eth_getBlockByNumber"
            and record["request"].get("block") == "finalized"
        ]
        self.assertEqual(
            finalized_headers,
            [
                {
                    "number": "0x9b",
                    "hash": "0x" + "c" * 64,
                    "parentHash": "0x" + "b" * 64,
                    "timestamp": "0x65920880",
                    "baseFeePerGas": "0x3b9aca00",
                    "gasUsed": "0xe4e1c0",
                    "gasLimit": "0x1c9c380",
                }
            ],
        )
        numeric_headers = [
            record["response"]
            for record in raw["records"]
            if record.get("request", {}).get("method") == "eth_getBlockByNumber"
            and record["request"].get("block") == "0x7b"
        ]
        self.assertTrue(numeric_headers)
        self.assertTrue(
            all(
                header["number"] == "0x7b"
                and header["hash"] == "0x" + "a" * 64
                and header["timestamp"] == "0x65920080"
                for header in numeric_headers
            )
        )
        state_requests = [
            record["request"]
            for record in raw["records"]
            if isinstance(record.get("request"), dict) and "to" in record["request"]
        ]
        self.assertTrue(state_requests)
        self.assertEqual({request["block"] for request in state_requests}, {"0x7b"})


if __name__ == "__main__":
    unittest.main()
