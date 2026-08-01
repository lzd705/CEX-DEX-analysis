"""Route-specific DEX gas and non-pool cost policy tests."""

from __future__ import annotations

import hashlib
import json
import unittest
from decimal import Decimal, localcontext

import scripts.dex_route_costs as dex_route_costs
from scripts.dex_route_costs import (
    GasQuoteRequest,
    estimate_route_gas,
    mev_route_policy,
    router_fee_component,
    transfer_tax_component,
)
from scripts.fetch_dex_depth import RpcClient, sanitize_endpoint


COHORT = "cohort-1"
OPPORTUNITY = "route-1:10000"
POOL = "0x3333333333333333333333333333333333333333"
MARKET = "dex:eth:uniswap_v3:{}:AAVE".format(POOL)
NOW = "2026-08-01T12:01:00Z"
OBSERVED = "2026-08-01T12:00:00Z"
FEE_VALID = "2026-08-01T12:02:00Z"
PRICE_VALID = "2026-08-01T12:03:00Z"
SENDER = "0x1111111111111111111111111111111111111111"
ROUTER = "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"
QUOTE_TOKEN = "0x2222222222222222222222222222222222222222"
MARKET_TOKEN = "0x4444444444444444444444444444444444444444"


def abi_address(value):
    return "0" * 24 + value[2:]


def abi_uint(value):
    return format(value, "064x")


TARGET_TOKEN_RAW = 100 * 10 ** 18
CALLDATA = "0x04e45aaf" + "".join(
    (
        abi_address(QUOTE_TOKEN),
        abi_address(MARKET_TOKEN),
        abi_uint(3000),
        abi_address(SENDER),
        abi_uint(10_000 * 10 ** 6),
        abi_uint(TARGET_TOKEN_RAW),
        abi_uint(0),
    )
)
BLOCK_HASH = "0x" + "a" * 64
BLOCK_TIMESTAMP = "0x6a6ddfc0"
SHA = "b" * 64
SENTINELS = (
    SENDER,
    "WALLET_SECRET_SENTINEL",
    "API_KEY_SENTINEL",
    "RPC_SECRET_SENTINEL",
    "/private/rpc/credentials.json",
)


def canonical_sha256(value):
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def tx_call():
    return {
        "from": SENDER,
        "to": ROUTER,
        "data": CALLDATA,
        "value": "0x0",
    }


def fee_cap_record(**overrides):
    row = {
        "chain_id": 1,
        "block_tag": "0x1234",
        "max_fee_per_gas_wei": "20000000000",
        "source": "eth_feeHistory",
        "observed_at": OBSERVED,
        "valid_until": FEE_VALID,
    }
    row.update(overrides)
    return row


def native_price_record(**overrides):
    row = {
        "chain_id": 1,
        "native_token_symbol": "ETH",
        "native_token_usd": "3500",
        "source": "synchronized_native_usd_quote",
        "observed_at": OBSERVED,
        "valid_until": PRICE_VALID,
    }
    row.update(overrides)
    return row


def gas_request(**overrides):
    call = tx_call()
    values = {
        "cohort_id": COHORT,
        "opportunity_id": OPPORTUNITY,
        "leg": "buy",
        "market_id": MARKET,
        "requested_notional_usd": Decimal("10000"),
        "target_token_quantity": Decimal("100"),
        "now": NOW,
        "chain_id": 1,
        "tx_call": call,
        "tx_call_sha256": canonical_sha256(call),
        "sender_policy": "opaque_simulation_sender",
        "allowance_basis": "preapproved_at_fixed_block",
        "block_tag": "0x1234",
        "max_fee_per_gas_wei": 20_000_000_000,
        "fee_cap_source": "eth_feeHistory",
        "fee_cap_observed_at": OBSERVED,
        "fee_cap_valid_until": FEE_VALID,
        "fee_cap_source_sha256": canonical_sha256(fee_cap_record()),
        "native_token_symbol": "ETH",
        "native_token_usd": Decimal("3500"),
        "native_price_source": "synchronized_native_usd_quote",
        "native_price_observed_at": OBSERVED,
        "native_price_valid_until": PRICE_VALID,
        "native_price_sha256": canonical_sha256(native_price_record()),
        "adapter_id": "uniswap_v3_router/v1",
    }
    values.update(overrides)
    return GasQuoteRequest(**values)


def controlled_call_evidence(**overrides):
    values = {
        "adapter_id": "uniswap_v3_router/v1",
        "market_id": MARKET,
        "direction": "buy_token",
        "requested_notional_usd": Decimal("10000"),
        "target_token_quantity": Decimal("100"),
        "block_tag": "0x1234",
        "tx_call": tx_call(),
        "market_token_address": MARKET_TOKEN,
        "market_token_decimals": 18,
        "pool_token0_address": QUOTE_TOKEN,
        "pool_token1_address": MARKET_TOKEN,
        "pool_fee": 3000,
    }
    values.update(overrides)
    return dex_route_costs.build_uniswap_v3_adapter_call_evidence(**values)


def native_price_evidence(**overrides):
    values = {
        "cohort_id": COHORT,
        "market_id": MARKET,
        "chain_id": 1,
        "block_tag": "0x1234",
        "block_number": 4660,
        "block_hash": BLOCK_HASH,
        "native_token_symbol": "ETH",
        "native_token_usd": "3500",
        "observed_at": OBSERVED,
        "valid_until": PRICE_VALID,
        "source_bundle_sha256": "d" * 64,
    }
    values.update(overrides)
    return dex_route_costs.build_synchronized_native_price_evidence(**values)


def controlled_gas_request(**overrides):
    request_overrides = {
        "max_fee_per_gas_wei": None,
        "fee_cap_source": None,
        "fee_cap_observed_at": None,
        "fee_cap_valid_until": None,
        "fee_cap_source_sha256": None,
        "native_token_symbol": None,
        "native_token_usd": None,
        "native_price_source": None,
        "native_price_observed_at": None,
        "native_price_valid_until": None,
        "native_price_sha256": None,
    }
    request_overrides.update(overrides)
    request = gas_request(**request_overrides)
    if request.adapter_call_evidence is None:
        object.__setattr__(
            request,
            "adapter_call_evidence",
            controlled_call_evidence(
                market_id=request.market_id,
                direction="buy_token" if request.leg == "buy" else "sell_token",
                requested_notional_usd=request.requested_notional_usd,
                target_token_quantity=request.target_token_quantity,
                block_tag=request.block_tag,
                tx_call=request.tx_call,
                adapter_id=request.adapter_id,
            ),
        )
    if request.native_price_evidence is None:
        object.__setattr__(
            request,
            "native_price_evidence",
            native_price_evidence(),
        )
    return request


class FakeGasRpc:
    endpoint = (
        "https://user:RPC_SECRET_SENTINEL@example.test/"
        "private/rpc/credentials.json?key=API_KEY_SENTINEL"
    )
    wallet = "WALLET_SECRET_SENTINEL"

    def __init__(self):
        self.calls = []

    def chain_id(self):
        self.calls.append(("eth_chainId",))
        return "0x1"

    def block(self, block_tag):
        self.calls.append(("eth_getBlockByNumber", block_tag))
        return {
            "number": "0x1234",
            "hash": BLOCK_HASH,
            "timestamp": BLOCK_TIMESTAMP,
            "baseFeePerGas": "0x1dcd65000",
        }

    def fee_history(self, block_count, newest_block, reward_percentiles):
        self.calls.append(
            (
                "eth_feeHistory",
                block_count,
                newest_block,
                reward_percentiles,
            )
        )
        return {
            "oldestBlock": "0x1234",
            "baseFeePerGas": ["0x1dcd65000", "0x1dcd65000"],
            "gasUsedRatio": [0.5],
            "reward": [["0xee6b2800"]],
        }

    def estimate_gas(self, call, block_tag):
        self.calls.append(("eth_estimateGas", call, block_tag))
        return "0x249f0"  # 150,000


def component_context(**overrides):
    values = {
        "cohort_id": COHORT,
        "opportunity_id": OPPORTUNITY,
        "leg": "buy",
        "market_id": MARKET,
        "requested_notional_usd": Decimal("10000"),
        "target_token_quantity": Decimal("100"),
        "now": NOW,
        "adapter_id": "uniswap_v3_router/v1",
        "block_tag": "0x1234",
        "tx_call_sha256": canonical_sha256(tx_call()),
    }
    values.update(overrides)
    return values


def adapter_evidence(kind, **overrides):
    if kind == "router_numeric":
        values = {
            "component_type": "router_or_integrator_fee",
            "evidence_kind": "numeric",
            "rate_bps": Decimal("2.5"),
            "basis_code": "router_fee_rate",
        }
    elif kind == "router_na":
        values = {
            "component_type": "router_or_integrator_fee",
            "evidence_kind": "not_applicable",
            "rate_bps": None,
            "basis_code": "router_fee_not_applicable",
        }
    elif kind == "transfer_numeric":
        values = {
            "component_type": "token_transfer_tax",
            "evidence_kind": "numeric",
            "rate_bps": Decimal("10"),
            "basis_code": "transfer_tax_rate",
        }
    else:
        values = {
            "component_type": "token_transfer_tax",
            "evidence_kind": "not_applicable",
            "rate_bps": None,
            "basis_code": "transfer_tax_not_applicable",
        }
    values.update(
        {
            "adapter_call_evidence": controlled_call_evidence(),
            "cohort_id": COHORT,
            "opportunity_id": OPPORTUNITY,
            "leg": "buy",
            "observed_at": OBSERVED,
            "valid_until": FEE_VALID,
        }
    )
    values.update(overrides)
    return dex_route_costs.build_route_adapter_cost_evidence(**values)


def protection_evidence(**overrides):
    values = {
        "route_id": "route-identity-1",
        "cohort_id": COHORT,
        "opportunity_id": OPPORTUNITY,
        "adapter_id": "uniswap_v3_router/v1",
        "submission_mode": "private_relay",
        "policy_code": "private_relay_bounded_loss",
        "max_loss_bps": Decimal("5"),
        "observed_at": OBSERVED,
        "valid_until": FEE_VALID,
    }
    values.update(overrides)
    return dex_route_costs.build_mev_protection_evidence(**values)


class GasQuoteTests(unittest.TestCase):
    def test_self_built_gas_integrity_records_never_authenticate_strict_quote(self):
        fake_market = "dex:eth:uniswap_v3:{}:AAVE".format("0x" + "5" * 40)
        requests = (
            controlled_gas_request(),
            controlled_gas_request(requested_notional_usd=Decimal("1")),
            controlled_gas_request(
                requested_notional_usd=Decimal("1000000000")
            ),
            controlled_gas_request(
                native_price_evidence=native_price_evidence(
                    native_token_usd="1"
                )
            ),
            controlled_gas_request(
                market_id=fake_market,
                native_price_evidence=native_price_evidence(
                    market_id=fake_market
                ),
            ),
        )

        for request in requests:
            with self.subTest(request=request):
                result = estimate_route_gas(rpc=FakeGasRpc(), request=request)
                self.assertEqual(result["value_status"], "assumed")
                self.assertIs(result["strict_eligible"], False)
                self.assertIsNotNone(result["amount_usd"])

    def test_block_base_fee_must_match_fee_history(self):
        class MismatchedBaseFeeRpc(FakeGasRpc):
            def block(self, block_tag):
                result = super().block(block_tag)
                result["baseFeePerGas"] = "0x1a13b8600"
                return result

        rpc = MismatchedBaseFeeRpc()
        result = estimate_route_gas(
            rpc=rpc,
            request=controlled_gas_request(),
        )

        self.assertEqual(result["value_status"], "failed")
        self.assertIs(result["strict_eligible"], False)
        self.assertIsNone(result["amount_usd"])
        self.assertNotIn("eth_estimateGas", [call[0] for call in rpc.calls])

    def test_controlled_adapter_call_decodes_and_binds_full_route_context(self):
        builder = getattr(
            dex_route_costs,
            "build_uniswap_v3_adapter_call_evidence",
            None,
        )
        self.assertTrue(callable(builder))

        evidence = builder(
            adapter_id="uniswap_v3_router/v1",
            market_id=MARKET,
            direction="buy_token",
            requested_notional_usd=Decimal("10000"),
            target_token_quantity=Decimal("100"),
            block_tag="0x1234",
            tx_call=tx_call(),
            market_token_address=MARKET_TOKEN,
            market_token_decimals=18,
            pool_token0_address=QUOTE_TOKEN,
            pool_token1_address=MARKET_TOKEN,
            pool_fee=3000,
        )

        self.assertEqual(evidence.market_id, MARKET)
        self.assertEqual(evidence.pool_address, POOL)
        self.assertEqual(evidence.market_token_address, MARKET_TOKEN)
        self.assertEqual(evidence.counter_token_address, QUOTE_TOKEN)
        self.assertEqual(evidence.direction, "buy_token")
        self.assertEqual(evidence.requested_notional_usd, "10000")
        self.assertEqual(evidence.target_token_quantity, "100")
        self.assertEqual(evidence.target_token_raw, str(TARGET_TOKEN_RAW))
        self.assertEqual(evidence.block_tag, "0x1234")
        self.assertEqual(evidence.tx_call_sha256, canonical_sha256(tx_call()))

        with self.assertRaisesRegex(ValueError, "target"):
            builder(
                adapter_id="uniswap_v3_router/v1",
                market_id=MARKET,
                direction="buy_token",
                requested_notional_usd=Decimal("10000"),
                target_token_quantity=Decimal("101"),
                block_tag="0x1234",
                tx_call=tx_call(),
                market_token_address=MARKET_TOKEN,
                market_token_decimals=18,
                pool_token0_address=QUOTE_TOKEN,
                pool_token1_address=MARKET_TOKEN,
                pool_fee=3000,
            )

    def test_fixed_block_rpc_calculation_is_exact_but_non_strict(self):
        rpc = FakeGasRpc()
        result = estimate_route_gas(rpc=rpc, request=controlled_gas_request())

        self.assertEqual(result["value_status"], "assumed")
        self.assertIs(result["strict_eligible"], False)
        self.assertEqual(result["gas_units"], "150000")
        self.assertEqual(result["amount_usd"], "10.5")
        self.assertEqual(result["rate_bps"], "10.5")
        self.assertEqual(result["chain_id"], "1")
        self.assertEqual(result["block_number"], "4660")
        self.assertEqual(result["block_hash"], BLOCK_HASH)
        self.assertEqual(result["tx_call_sha256"], canonical_sha256(tx_call()))
        self.assertEqual(
            result["cost_component"]["component_type"],
            "network_gas",
        )
        self.assertEqual(
            [call[0] for call in rpc.calls],
            [
                "eth_chainId",
                "eth_getBlockByNumber",
                "eth_feeHistory",
                "eth_estimateGas",
            ],
        )
        serialized = json.dumps(result, sort_keys=True)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)
        self.assertNotIn(CALLDATA, serialized)
        self.assertNotIn(ROUTER, serialized)

    def test_fee_history_and_native_integrity_record_remain_non_strict(self):
        rpc = FakeGasRpc()
        result = estimate_route_gas(
            rpc=rpc,
            request=controlled_gas_request(),
        )

        self.assertEqual(result["value_status"], "assumed")
        self.assertIs(result["strict_eligible"], False)
        self.assertEqual(result["max_fee_per_gas_wei"], "20000000000")
        self.assertEqual(result["amount_usd"], "10.5")
        self.assertEqual(
            [call[0] for call in rpc.calls],
            [
                "eth_chainId",
                "eth_getBlockByNumber",
                "eth_feeHistory",
                "eth_estimateGas",
            ],
        )

    def test_fee_history_shape_hex_and_block_lineage_fail_closed(self):
        valid = FakeGasRpc().fee_history("0x1", "0x1234", [50])
        cases = (
            {**valid, "oldestBlock": "0x1233"},
            {**valid, "baseFeePerGas": ["0x01", "0x1dcd65000"]},
            {**valid, "reward": [["0x01"]]},
            {**valid, "gasUsedRatio": [True]},
            {**valid, "baseFeePerGas": ["0x1dcd65000"]},
            {**valid, "unexpected": "RPC_SECRET_SENTINEL"},
        )

        for history in cases:
            class BadFeeHistoryRpc(FakeGasRpc):
                def fee_history(self, block_count, newest_block, reward_percentiles):
                    self.calls.append(
                        (
                            "eth_feeHistory",
                            block_count,
                            newest_block,
                            reward_percentiles,
                        )
                    )
                    return history

            rpc = BadFeeHistoryRpc()
            with self.subTest(history=history):
                result = estimate_route_gas(
                    rpc=rpc,
                    request=controlled_gas_request(),
                )
                self.assertEqual(result["value_status"], "failed")
                self.assertIs(result["strict_eligible"], False)
                self.assertIsNone(result["amount_usd"])
                self.assertNotIn(
                    "eth_estimateGas",
                    [call[0] for call in rpc.calls],
                )
                self.assertNotIn(
                    "RPC_SECRET_SENTINEL",
                    json.dumps(result, sort_keys=True),
                )

    def test_replayed_adapter_or_native_context_is_unavailable_before_estimate(self):
        target_replay = controlled_gas_request()
        object.__setattr__(
            target_replay,
            "target_token_quantity",
            Decimal("101"),
        )
        cohort_replay = controlled_gas_request()
        object.__setattr__(
            cohort_replay,
            "native_price_evidence",
            native_price_evidence(cohort_id="other-cohort"),
        )

        for request in (target_replay, cohort_replay):
            rpc = FakeGasRpc()
            with self.subTest(request=request):
                result = estimate_route_gas(rpc=rpc, request=request)
                self.assertEqual(result["value_status"], "unavailable")
                self.assertIs(result["strict_eligible"], False)
                self.assertIsNone(result["amount_usd"])
                self.assertEqual(rpc.calls, [])

    def test_caller_reported_fee_cap_or_native_price_is_never_strict(self):
        request = gas_request()
        object.__setattr__(
            request,
            "adapter_call_evidence",
            controlled_call_evidence(),
        )
        object.__setattr__(request, "native_price_evidence", native_price_evidence())

        result = estimate_route_gas(rpc=FakeGasRpc(), request=request)

        self.assertEqual(result["value_status"], "unavailable")
        self.assertIs(result["strict_eligible"], False)
        self.assertIsNone(result["amount_usd"])

    def test_native_price_requires_controlled_immutable_evidence(self):
        builder = getattr(
            dex_route_costs,
            "build_synchronized_native_price_evidence",
            None,
        )
        self.assertTrue(callable(builder))

        request = controlled_gas_request()
        object.__setattr__(
            request,
            "native_price_evidence",
            dict(vars(native_price_evidence())),
        )
        rpc = FakeGasRpc()
        result = estimate_route_gas(rpc=rpc, request=request)

        self.assertEqual(result["value_status"], "unavailable")
        self.assertIs(result["strict_eligible"], False)
        self.assertEqual(rpc.calls, [])

    def test_market_chain_adapter_and_registered_router_are_one_identity(self):
        invalid_market_ids = (
            "dex:eth:uniswap_v3:/private/rpc/credentials.json:AAVE",
            "dex:eth:uniswap_v3:{}:aave".format(POOL),
            "dex:ETH:uniswap_v3:{}:AAVE".format(POOL),
            "dex:eth:uniswap_v3:{}:AAVE/USDT".format(POOL),
        )
        for market_id in invalid_market_ids:
            rpc = FakeGasRpc()
            with self.subTest(market_id=market_id):
                with self.assertRaisesRegex(ValueError, "market_id"):
                    estimate_route_gas(
                        rpc=rpc,
                        request=gas_request(market_id=market_id),
                    )
                self.assertEqual(rpc.calls, [])

        wrong_router_call = tx_call()
        wrong_router_call["to"] = (
            "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f"
        )
        cases = (
            {"chain_id": 42161},
            {
                "market_id": "dex:eth:sushiswap:{}:AAVE".format(POOL),
            },
            {"adapter_id": "sushiswap_router/v1"},
            {
                "tx_call": wrong_router_call,
                "tx_call_sha256": canonical_sha256(wrong_router_call),
            },
        )
        for overrides in cases:
            rpc = FakeGasRpc()
            with self.subTest(overrides=overrides):
                result = estimate_route_gas(
                    rpc=rpc,
                    request=gas_request(**overrides),
                )
                self.assertEqual(result["value_status"], "unavailable")
                self.assertIs(result["strict_eligible"], False)
                self.assertIsNone(result["amount_usd"])
                self.assertEqual(rpc.calls, [])

    def test_missing_or_caller_only_lineage_is_unavailable_without_rpc(self):
        cases = (
            {"chain_id": None},
            {
                "tx_call": {
                    "to": ROUTER,
                    "data": CALLDATA,
                    "value": "0x0",
                }
            },
            {
                "tx_call": {
                    "from": SENDER,
                    "to": ROUTER,
                    "value": "0x0",
                }
            },
            {"tx_call_sha256": None},
            {"sender_policy": "WALLET_SECRET_SENTINEL"},
            {"allowance_basis": None},
            {"block_tag": "latest"},
            {"block_tag": None},
            {"max_fee_per_gas_wei": None},
            {"fee_cap_source": "caller_argument"},
            {"fee_cap_source_sha256": SHA},
            {"native_price_sha256": SHA},
            {"native_price_valid_until": None},
            {"native_token_usd": None},
            {"adapter_id": "API_KEY_SENTINEL"},
        )
        for overrides in cases:
            rpc = FakeGasRpc()
            with self.subTest(overrides=overrides):
                result = estimate_route_gas(
                    rpc=rpc,
                    request=gas_request(**overrides),
                )
                self.assertEqual(result["value_status"], "unavailable")
                self.assertIs(result["strict_eligible"], False)
                self.assertIsNone(result["amount_usd"])
                self.assertIsNone(result["gas_units"])
                self.assertEqual(rpc.calls, [])
                serialized = json.dumps(result, sort_keys=True)
                for sentinel in SENTINELS:
                    self.assertNotIn(sentinel, serialized)

    def test_future_or_expired_fee_and_price_lineage_is_never_strict(self):
        class FutureBlockRpc(FakeGasRpc):
            def block(self, block_tag):
                result = super().block(block_tag)
                result["timestamp"] = "0x6a6de001"
                return result

        cases = (
            (controlled_gas_request(), FutureBlockRpc(), "failed"),
            (
                controlled_gas_request(now=FEE_VALID),
                FakeGasRpc(),
                "stale",
            ),
            (
                controlled_gas_request(
                    native_price_evidence=native_price_evidence(
                        valid_until=NOW
                    )
                ),
                FakeGasRpc(),
                "stale",
            ),
        )
        for request, rpc, expected_status in cases:
            with self.subTest(expected_status=expected_status):
                result = estimate_route_gas(rpc=rpc, request=request)
                self.assertEqual(result["value_status"], expected_status)
                self.assertIs(result["strict_eligible"], False)
                self.assertIsNone(result["amount_usd"])
                self.assertNotIn("eth_estimateGas", [call[0] for call in rpc.calls])

    def test_rpc_chain_block_and_estimate_failures_are_terminal_and_redacted(self):
        class BadRpc(FakeGasRpc):
            mode = "chain"

            def chain_id(self):
                if self.mode == "chain":
                    return "0xa4b1"
                return super().chain_id()

            def block(self, block_tag):
                if self.mode == "block":
                    return {"number": "0x1235", "hash": BLOCK_HASH}
                return super().block(block_tag)

            def estimate_gas(self, call, block_tag):
                if self.mode == "raise":
                    raise RuntimeError(" ".join(SENTINELS))
                if self.mode == "zero":
                    return "0x0"
                return super().estimate_gas(call, block_tag)

        for mode in ("chain", "block", "raise", "zero"):
            rpc = BadRpc()
            rpc.mode = mode
            with self.subTest(mode=mode):
                result = estimate_route_gas(
                    rpc=rpc,
                    request=controlled_gas_request(),
                )
                self.assertEqual(result["value_status"], "failed")
                self.assertIs(result["strict_eligible"], False)
                self.assertIsNone(result["amount_usd"])
                serialized = json.dumps(result, sort_keys=True)
                for sentinel in SENTINELS:
                    self.assertNotIn(sentinel, serialized)

    def test_rpc_quantities_must_be_canonical_minimal_lowercase_hex(self):
        class NoncanonicalRpc(FakeGasRpc):
            chain_result = "0x1"
            block_result = "0x1234"
            base_fee_result = "0x1dcd65000"
            gas_result = "0x249f0"

            def chain_id(self):
                return self.chain_result

            def block(self, block_tag):
                return {
                    "number": self.block_result,
                    "hash": BLOCK_HASH,
                    "timestamp": BLOCK_TIMESTAMP,
                    "baseFeePerGas": self.base_fee_result,
                }

            def estimate_gas(self, call, block_tag):
                return self.gas_result

        cases = (
            ("chain_result", 1),
            ("chain_result", "1"),
            ("chain_result", "0x01"),
            ("chain_result", "0xA"),
            ("block_result", "0x01234"),
            ("block_result", "0X1234"),
            ("base_fee_result", 8_000_000_000),
            ("base_fee_result", "0x01"),
            ("base_fee_result", "0x1DCD65000"),
            ("gas_result", 150000),
            ("gas_result", "0x0249f0"),
            ("gas_result", "0x249F0"),
        )
        for field, value in cases:
            rpc = NoncanonicalRpc()
            setattr(rpc, field, value)
            with self.subTest(field=field, value=value):
                result = estimate_route_gas(
                    rpc=rpc,
                    request=controlled_gas_request(),
                )
                self.assertEqual(result["value_status"], "failed")
                self.assertIs(result["strict_eligible"], False)
                self.assertIsNone(result["amount_usd"])

    def test_exact_gas_math_ignores_low_decimal_context(self):
        with localcontext() as context:
            context.prec = 4
            result = estimate_route_gas(
                rpc=FakeGasRpc(),
                request=controlled_gas_request(
                    requested_notional_usd=Decimal("10000"),
                    native_price_evidence=native_price_evidence(
                        native_token_usd="3500.123456789012345678"
                    ),
                ),
            )

        self.assertEqual(result["amount_usd"], "10.500370370367037037034")
        self.assertEqual(result["rate_bps"], "10.500370370367037037034")


class RpcClientRouteCostIntegrationTests(unittest.TestCase):
    def test_endpoint_sanitizer_never_discloses_unapproved_hostnames(self):
        credential_host = (
            "https://user:WALLET_SECRET_SENTINEL@"
            "API_KEY_SENTINEL.rpc.example.invalid:443/"
            "private/rpc/credentials.json?key=RPC_SECRET_SENTINEL"
        )
        sanitized = sanitize_endpoint(credential_host)

        self.assertRegex(sanitized, r"^rpc-endpoint-sha256:[0-9a-f]{64}$")
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel.lower(), sanitized.lower())
        self.assertNotIn("rpc.example.invalid", sanitized)
        self.assertEqual(
            sanitize_endpoint(
                "https://user:RPC_SECRET_SENTINEL@example.test/path?key=secret"
            ),
            "https://example.test",
        )
        self.assertEqual(
            sanitize_endpoint("https://ethereum-rpc.publicnode.com/private"),
            "https://ethereum-rpc.publicnode.com",
        )

    def test_fee_history_method_uses_exact_json_rpc_params(self):
        requests = []

        def transport(_url, payload):
            requests.append(payload)
            response = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "oldestBlock": "0x1234",
                    "baseFeePerGas": ["0x1dcd65000", "0x1dcd65000"],
                    "gasUsedRatio": [0.5],
                    "reward": [["0xee6b2800"]],
                },
            }
            raw = json.dumps(response).encode("utf-8")
            return response, raw

        client = RpcClient("eth", "https://example.test", request=transport)
        result = client.fee_history("0x1", "0x1234", [50])

        self.assertEqual(result["oldestBlock"], "0x1234")
        self.assertEqual(requests[0]["method"], "eth_feeHistory")
        self.assertEqual(requests[0]["params"], ["0x1", "0x1234", [50]])

    def test_fee_history_rejects_wrong_envelope_or_id(self):
        cases = (
            {"jsonrpc": "1.0", "id": 1, "result": {}},
            {"jsonrpc": "2.0", "id": 2, "result": {}},
            {"jsonrpc": "2.0", "id": True, "result": {}},
        )
        for response_template in cases:
            def transport(_url, payload, template=response_template):
                response = dict(template)
                raw = json.dumps(response).encode("utf-8")
                return response, raw

            client = RpcClient("eth", "https://example.test", request=transport)
            with self.subTest(response=response_template):
                with self.assertRaises(Exception):
                    client.fee_history("0x1", "0x1234", [50])

    def test_chain_and_estimate_methods_redact_persisted_rpc_records(self):
        requests = []

        def transport(_url, payload):
            requests.append(payload)
            if payload["method"] == "eth_chainId":
                response = {"jsonrpc": "2.0", "id": payload["id"], "result": "0x1"}
            else:
                response = {
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": "0x249f0",
                }
            raw = json.dumps(response, separators=(",", ":")).encode("utf-8")
            return response, raw

        client = RpcClient(
            "eth",
            (
                "https://user:RPC_SECRET_SENTINEL@example.test/"
                "private/rpc/credentials.json?key=API_KEY_SENTINEL"
            ),
            request=transport,
        )
        self.assertEqual(client.chain_id(), "0x1")
        self.assertEqual(client.estimate_gas(tx_call(), "0x1234"), "0x249f0")

        self.assertEqual(requests[1]["method"], "eth_estimateGas")
        self.assertEqual(requests[1]["params"], [tx_call(), "0x1234"])
        self.assertEqual(client.endpoint, "https://example.test")
        serialized_records = json.dumps(client.records, sort_keys=True)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized_records)
        self.assertNotIn(CALLDATA, serialized_records)
        self.assertNotIn(ROUTER, serialized_records)
        self.assertIn(canonical_sha256(tx_call()), serialized_records)

    def test_malformed_estimate_response_and_block_text_are_redacted(self):
        def transport(_url, payload):
            response = {
                "jsonrpc": "RPC_SECRET_SENTINEL",
                "id": "WALLET_SECRET_SENTINEL",
                "error": {
                    "code": "API_KEY_SENTINEL",
                    "message": "WALLET_SECRET_SENTINEL",
                    "data": "/private/rpc/credentials.json",
                },
            }
            raw = json.dumps(response).encode("utf-8")
            return response, raw

        client = RpcClient("eth", "https://example.test", request=transport)
        with self.assertRaises(Exception) as captured:
            client.estimate_gas(tx_call(), "/private/rpc/credentials.json")

        self.assertNotIn("SENTINEL", str(captured.exception))
        serialized = json.dumps(client.records, sort_keys=True)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)
        self.assertIn("invalid_block", serialized)
        self.assertEqual(client.records[0]["response"]["id"], 1)

    def test_rpc_envelope_and_response_id_are_exact(self):
        cases = (
            {"jsonrpc": "1.0", "id": 1, "result": "0x1"},
            {"jsonrpc": "RPC_SECRET_SENTINEL", "id": 1, "result": "0x1"},
            {"jsonrpc": "2.0", "id": 2, "result": "0x1"},
            {"jsonrpc": "2.0", "id": True, "result": "0x1"},
            {"jsonrpc": "2.0", "id": "WALLET_SECRET_SENTINEL", "result": "0x1"},
        )
        for response_template in cases:
            def transport(_url, payload, template=response_template):
                response = dict(template)
                raw = json.dumps(response).encode("utf-8")
                return response, raw

            client = RpcClient("eth", "https://example.test", request=transport)
            with self.subTest(response=response_template):
                with self.assertRaises(Exception) as captured:
                    client.chain_id()
                self.assertNotIn("SENTINEL", str(captured.exception))
                serialized = json.dumps(client.records, sort_keys=True)
                for sentinel in SENTINELS:
                    self.assertNotIn(sentinel, serialized)
                self.assertEqual(client.records[0]["response"]["id"], 1)

    def test_rpc_client_rejects_noncanonical_chain_and_gas_quantities(self):
        for result in (1, "1", "0x01", "0xA", "0X1"):
            def transport(_url, payload, value=result):
                response = {
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": value,
                }
                raw = json.dumps(response).encode("utf-8")
                return response, raw

            client = RpcClient("eth", "https://example.test", request=transport)
            with self.subTest(result=result):
                with self.assertRaises(Exception):
                    client.chain_id()

        for result in (150000, "249f0", "0x0249f0", "0x249F0"):
            def transport(_url, payload, value=result):
                response = {
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": value,
                }
                raw = json.dumps(response).encode("utf-8")
                return response, raw

            client = RpcClient("eth", "https://example.test", request=transport)
            with self.subTest(result=result):
                with self.assertRaises(Exception):
                    client.estimate_gas(tx_call(), "0x1234")


class AdapterCostTests(unittest.TestCase):
    def test_public_adapter_integrity_records_cannot_create_strict_facts(self):
        cases = (
            (
                router_fee_component,
                adapter_evidence("router_numeric"),
                "assumed",
            ),
            (
                router_fee_component,
                adapter_evidence("router_na"),
                "unavailable",
            ),
            (
                transfer_tax_component,
                adapter_evidence("transfer_numeric"),
                "assumed",
            ),
            (
                transfer_tax_component,
                adapter_evidence("transfer_na"),
                "unavailable",
            ),
        )
        for builder, evidence, expected_status in cases:
            with self.subTest(evidence=evidence):
                row = builder(**component_context(), evidence=evidence)
                self.assertEqual(row["value_status"], expected_status)
                self.assertIs(row["strict_eligible"], False)
                if expected_status == "unavailable":
                    self.assertIsNone(row["amount_usd"])

    def test_adapter_cost_integrity_record_cannot_replay_context(self):
        builder = getattr(
            dex_route_costs,
            "build_route_adapter_cost_evidence",
            None,
        )
        self.assertTrue(callable(builder))

        evidence = builder(
            adapter_call_evidence=controlled_call_evidence(),
            cohort_id=COHORT,
            opportunity_id=OPPORTUNITY,
            leg="buy",
            component_type="router_or_integrator_fee",
            evidence_kind="numeric",
            rate_bps=Decimal("2.5"),
            basis_code="router_fee_rate",
            observed_at=OBSERVED,
            valid_until=FEE_VALID,
        )
        base = component_context(
            block_tag="0x1234",
            tx_call_sha256=canonical_sha256(tx_call()),
        )
        self.assertEqual(
            router_fee_component(**base, evidence=evidence)["value_status"],
            "assumed",
        )

        replay_contexts = (
            {**base, "cohort_id": "other-cohort"},
            {**base, "opportunity_id": "other-opportunity"},
            {**base, "requested_notional_usd": Decimal("9999")},
            {**base, "target_token_quantity": Decimal("101")},
            {**base, "block_tag": "0x1235"},
            {**base, "tx_call_sha256": "e" * 64},
            {
                **base,
                "market_id": "dex:eth:uniswap_v3:{}:AAVE".format(
                    "0x" + "5" * 40
                ),
            },
        )
        for replay in replay_contexts:
            with self.subTest(replay=replay):
                row = router_fee_component(**replay, evidence=evidence)
                self.assertEqual(row["value_status"], "unavailable")
                self.assertIs(row["strict_eligible"], False)
                self.assertIsNone(row["amount_usd"])

    def test_router_numeric_and_not_applicable_require_adapter_evidence(self):
        numeric = router_fee_component(
            **component_context(),
            evidence=adapter_evidence("router_numeric"),
        )
        not_applicable = router_fee_component(
            **component_context(),
            evidence=adapter_evidence("router_na"),
        )

        self.assertEqual(numeric["value_status"], "assumed")
        self.assertIs(numeric["strict_eligible"], False)
        self.assertEqual(numeric["amount_usd"], "2.5")
        self.assertEqual(not_applicable["value_status"], "unavailable")
        self.assertIs(not_applicable["strict_eligible"], False)
        self.assertIsNone(not_applicable["amount_usd"])

    def test_transfer_numeric_and_not_applicable_require_adapter_evidence(self):
        numeric = transfer_tax_component(
            **component_context(),
            evidence=adapter_evidence("transfer_numeric"),
        )
        not_applicable = transfer_tax_component(
            **component_context(),
            evidence=adapter_evidence("transfer_na"),
        )

        self.assertEqual(numeric["component_type"], "token_transfer_tax")
        self.assertEqual(numeric["value_status"], "assumed")
        self.assertIs(numeric["strict_eligible"], False)
        self.assertEqual(numeric["amount_usd"], "10")
        self.assertEqual(not_applicable["value_status"], "unavailable")
        self.assertIs(not_applicable["strict_eligible"], False)

    def test_unknown_or_unbound_behavior_stays_unavailable_and_redacted(self):
        cases = (
            None,
            {**vars(adapter_evidence("router_na")), "adapter_id": "other/v1"},
            {
                **vars(adapter_evidence("router_numeric")),
                "private_path": "/private/rpc/credentials.json",
            },
            {
                **vars(adapter_evidence("router_numeric")),
                "basis_code": "API_KEY_SENTINEL",
            },
            {
                **vars(adapter_evidence("router_numeric")),
                "source_record_sha256": SHA,
            },
        )
        for evidence in cases:
            with self.subTest(evidence=evidence):
                row = router_fee_component(
                    **component_context(),
                    evidence=evidence,
                )
                self.assertIn(row["value_status"], ("unavailable", "failed"))
                self.assertIs(row["strict_eligible"], False)
                self.assertIsNone(row["amount_usd"])
                serialized = json.dumps(row, sort_keys=True)
                for sentinel in SENTINELS:
                    self.assertNotIn(sentinel, serialized)

    def test_stale_adapter_evidence_cannot_be_numeric_or_not_applicable(self):
        for kind, builder in (
            ("router_numeric", router_fee_component),
            ("router_na", router_fee_component),
            ("transfer_numeric", transfer_tax_component),
            ("transfer_na", transfer_tax_component),
        ):
            evidence = adapter_evidence(kind, valid_until=NOW)
            with self.subTest(kind=kind):
                row = builder(**component_context(), evidence=evidence)
                self.assertEqual(row["value_status"], "stale")
                self.assertIs(row["strict_eligible"], False)
                self.assertIsNone(row["amount_usd"])


class MevPolicyTests(unittest.TestCase):
    def test_public_mev_integrity_record_never_authenticates_bound(self):
        row = mev_route_policy(
            **self.route_context(),
            submission_mode="private_relay",
            protection_policy="private_relay_bounded_loss",
            scenario_rate_bps=Decimal("2"),
            protection_evidence=protection_evidence(),
        )

        self.assertEqual(row["value_status"], "assumed")
        self.assertIs(row["strict_eligible"], False)
        self.assertIsNone(row["source_record_sha256"])

    def route_context(self, **overrides):
        values = component_context(
            leg="route",
            market_id="",
            adapter_id="uniswap_v3_router/v1",
            route_id="route-identity-1",
        )
        values.pop("block_tag")
        values.pop("tx_call_sha256")
        values.update(overrides)
        return values

    def test_self_built_bound_and_identity_fields_never_authenticate_mev(self):
        builder = getattr(
            dex_route_costs,
            "build_mev_protection_evidence",
            None,
        )
        self.assertTrue(callable(builder))
        evidence = builder(
            route_id="route-identity-1",
            cohort_id=COHORT,
            opportunity_id=OPPORTUNITY,
            adapter_id="uniswap_v3_router/v1",
            submission_mode="private_relay",
            policy_code="private_relay_bounded_loss",
            max_loss_bps=Decimal("5"),
            observed_at=OBSERVED,
            valid_until=FEE_VALID,
        )

        bounded = mev_route_policy(
            **self.route_context(),
            submission_mode="private_relay",
            protection_policy="private_relay_bounded_loss",
            scenario_rate_bps=Decimal("5"),
            protection_evidence=evidence,
        )
        above_bound = mev_route_policy(
            **self.route_context(),
            submission_mode="private_relay",
            protection_policy="private_relay_bounded_loss",
            scenario_rate_bps=Decimal("5.0001"),
            protection_evidence=evidence,
        )
        replayed = mev_route_policy(
            **self.route_context(route_id="route-identity-2"),
            submission_mode="private_relay",
            protection_policy="private_relay_bounded_loss",
            scenario_rate_bps=Decimal("2"),
            protection_evidence=evidence,
        )

        def rewritten_evidence(**overrides):
            record = dict(vars(evidence))
            record.pop("source_record_sha256")
            record.update(overrides)
            return dex_route_costs.MevProtectionEvidence(
                source_record_sha256=canonical_sha256(record),
                **record,
            )

        identity_replays = (
            rewritten_evidence(adapter_id="other_adapter/v1"),
            rewritten_evidence(submission_mode="public_mempool"),
            rewritten_evidence(policy_code="other_policy"),
        )

        self.assertEqual(bounded["value_status"], "assumed")
        self.assertEqual(above_bound["value_status"], "assumed")
        self.assertEqual(replayed["value_status"], "assumed")
        for row in (above_bound, replayed):
            self.assertIs(row["strict_eligible"], False)
            self.assertIsNone(row["source_record_sha256"])
        for replay_evidence in identity_replays:
            with self.subTest(replay_evidence=replay_evidence):
                row = mev_route_policy(
                    **self.route_context(),
                    submission_mode="private_relay",
                    protection_policy="private_relay_bounded_loss",
                    scenario_rate_bps=Decimal("2"),
                    protection_evidence=replay_evidence,
                )
                self.assertEqual(row["value_status"], "assumed")
                self.assertIsNone(row["source_record_sha256"])

    def test_public_mempool_without_protection_is_strict_unavailable(self):
        row = mev_route_policy(
            **self.route_context(),
            submission_mode="public_mempool",
        )

        self.assertEqual(row["value_status"], "unavailable")
        self.assertIs(row["strict_eligible"], False)
        self.assertIsNone(row["amount_usd"])
        self.assertEqual(row["reason_code"], "mev_protection_unavailable")

    def test_user_buffer_is_assumed_positive_and_never_strict(self):
        row = mev_route_policy(
            **self.route_context(),
            submission_mode="public_mempool",
            scenario_rate_bps=Decimal("5"),
        )

        self.assertEqual(row["value_status"], "assumed")
        self.assertIs(row["strict_eligible"], False)
        self.assertEqual(row["amount_usd"], "5")
        self.assertEqual(row["rate_bps"], "5")

        zero = mev_route_policy(
            **self.route_context(),
            submission_mode="public_mempool",
            scenario_rate_bps=Decimal("0"),
        )
        self.assertEqual(zero["value_status"], "unavailable")
        self.assertIsNone(zero["amount_usd"])

    def test_self_built_private_relay_policy_is_only_assumed(self):
        row = mev_route_policy(
            **self.route_context(),
            submission_mode="private_relay",
            protection_policy="private_relay_bounded_loss",
            scenario_rate_bps=Decimal("2"),
            protection_evidence=protection_evidence(),
        )

        self.assertEqual(row["value_status"], "assumed")
        self.assertIs(row["strict_eligible"], False)
        self.assertEqual(row["amount_usd"], "2")
        self.assertIsNone(row["source_record_sha256"])

    def test_uncontrolled_mev_inputs_never_leak_or_become_numeric(self):
        row = mev_route_policy(
            **self.route_context(),
            submission_mode="RPC_SECRET_SENTINEL",
            protection_policy="/private/rpc/credentials.json",
            scenario_rate_bps=Decimal("3"),
        )

        self.assertEqual(row["value_status"], "unavailable")
        self.assertIsNone(row["amount_usd"])
        serialized = json.dumps(row, sort_keys=True)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)


if __name__ == "__main__":
    unittest.main()
