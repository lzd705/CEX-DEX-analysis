"""Exact common-quantity and CEX route-quote contract tests."""

from __future__ import annotations

import hashlib
import inspect
import json
import unittest
from dataclasses import replace
from decimal import Decimal, localcontext

import scripts.route_quantity as route_quantity_module
from scripts.dex_route_costs import CHAIN_ID_BY_NAME
from scripts.fetch_cex_depth import (
    cex_quantity_state_id,
    execution_rows_for_book,
    route_quantity_quote_for_book,
)
from scripts.historical_foundry_contracts import quote_v2_exact_in
from scripts.route_opportunity import build_route_opportunity
from scripts.route_quantity import (
    CommonTarget,
    FeeSemantics,
    MarketRules,
    QuantityQuote,
    V2PoolState,
    common_net_target_quantity,
    quote_cex_book_quantity,
    quote_v2_pool_quantity,
    validate_v2_quantity_quote_against_state,
)


HASH = "a" * 64
OTHER_HASH = "b" * 64
OBSERVED_AT = "2026-08-01T12:00:00Z"
VALID_UNTIL = "2026-08-01T12:05:00Z"
POOL = "0x3333333333333333333333333333333333333333"
TOKEN0 = "0x1111111111111111111111111111111111111111"
TOKEN1 = "0x2222222222222222222222222222222222222222"


def rules(**overrides):
    values = {
        "market_id": "cex:alpha:AAVE/USDT",
        "base_asset": "AAVE",
        "quote_asset": "USDT",
        "base_unit_decimals": 2,
        "quote_unit_decimals": 4,
        "base_increment": Decimal("0.01"),
        "quote_increment": Decimal("0.0001"),
        "min_base_quantity": Decimal("0.01"),
        "min_quote_notional": Decimal("0"),
        "observed_at": OBSERVED_AT,
        "valid_until": VALID_UNTIL,
        "source_record_sha256": HASH,
    }
    values.update(overrides)
    return MarketRules(**values)


def fee(**overrides):
    values = {
        "rate_bps": Decimal("0"),
        "fee_asset": "USDT",
        "charge_basis": "spent_quote",
        "fee_increment": Decimal("0.0001"),
        "rounding_mode": "ceiling",
        "third_asset_quote_price": None,
        "observed_at": OBSERVED_AT,
        "valid_until": VALID_UNTIL,
        "source_record_sha256": OTHER_HASH,
        "conversion_source_record_sha256": None,
    }
    values.update(overrides)
    return FeeSemantics(**values)


def target(quantity="1", *, decimals=2, lattice_raw=1):
    scale = 10**decimals
    raw = int(Decimal(quantity) * scale)
    return CommonTarget(
        asset="AAVE",
        unit_decimals=decimals,
        raw_quantity=raw,
        lattice_raw=lattice_raw,
    )


def dex_rules(**overrides):
    values = {
        "market_id": f"dex:eth:uniswap_v2:{POOL}:AAVE",
        "base_asset": "AAVE",
        "quote_asset": "USDC",
        "base_unit_decimals": 18,
        "quote_unit_decimals": 6,
        "base_increment": Decimal("0.000000000000000001"),
        "quote_increment": Decimal("0.000001"),
        "min_base_quantity": Decimal("0"),
        "min_quote_notional": Decimal("0"),
        "observed_at": OBSERVED_AT,
        "valid_until": VALID_UNTIL,
        "source_record_sha256": HASH,
    }
    values.update(overrides)
    return MarketRules(**values)


def v2_state(**overrides):
    values = {
        "chain": "eth",
        "chain_id": 1,
        "dex": "uniswap_v2",
        "pool_address": POOL,
        "token0_address": TOKEN0,
        "token1_address": TOKEN1,
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
    values.update(overrides)
    return V2PoolState(**values)


def dex_target(raw=10 * 10**18, *, decimals=18):
    return CommonTarget(
        asset="AAVE",
        unit_decimals=decimals,
        raw_quantity=raw,
        lattice_raw=1,
    )


def collector_market(exchange="binance", symbol="AAVE/USDT"):
    return {
        "token_symbol": "AAVE",
        "exchange": exchange,
        "cex_symbol": symbol,
    }


def collector_rules(**overrides):
    values = {"market_id": "cex:binance:AAVE/USDT"}
    values.update(overrides)
    return rules(**values)


def collector_book(**overrides):
    values = {
        "bids": [(Decimal("99"), Decimal("2"))],
        "asks": [(Decimal("101"), Decimal("2"))],
        "source_instrument": "AAVEUSDT",
        "source_quote_asset": "USDT",
        "source_sequence": "book-sequence-1",
        "full_book_reported": False,
        "raw": b'{"book":"exact"}',
    }
    values.update(overrides)
    return values


class CommonTargetTests(unittest.TestCase):
    def test_10000_at_101_floors_to_exact_cross_market_lattice(self):
        buy = rules(base_increment=Decimal("0.01"))
        sell = rules(
            market_id="cex:beta:AAVE/USDT",
            base_increment=Decimal("0.001"),
            base_unit_decimals=3,
        )

        result = common_net_target_quantity(
            requested_notional_usd=Decimal("10000"),
            buy_reference_price_usd=Decimal("101"),
            buy_market_rules=buy,
            sell_market_rules=sell,
        )

        self.assertEqual(result.asset, "AAVE")
        self.assertEqual(result.quantity, Decimal("99"))
        self.assertEqual(result.canonical_text, "99")
        self.assertEqual(result.unit_decimals, 3)
        self.assertEqual(result.raw_quantity, 99_000)
        self.assertEqual(result.lattice_raw, 10)

    def test_nontrivial_lattice_and_decimal_context_are_exact(self):
        buy = rules(
            base_increment=Decimal("0.03"),
            base_unit_decimals=2,
        )
        sell = rules(
            market_id="cex:beta:AAVE/USDT",
            base_increment=Decimal("0.02"),
            base_unit_decimals=2,
        )
        with localcontext() as context:
            context.prec = 2
            result = common_net_target_quantity(
                requested_notional_usd=Decimal("10000"),
                buy_reference_price_usd=Decimal("101"),
                buy_market_rules=buy,
                sell_market_rules=sell,
            )

        self.assertEqual(result.quantity, Decimal("99"))
        self.assertEqual(result.raw_quantity, 9_900)
        self.assertEqual(result.lattice_raw, 6)

    def test_below_one_common_unit_and_inexact_inputs_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "below|minimum"):
            common_net_target_quantity(
                requested_notional_usd=Decimal("0.001"),
                buy_reference_price_usd=Decimal("100"),
                buy_market_rules=rules(),
                sell_market_rules=rules(
                    market_id="cex:beta:AAVE/USDT",
                ),
            )

        for value in (10000.0, True):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "exact"):
                    common_net_target_quantity(
                        requested_notional_usd=value,
                        buy_reference_price_usd=Decimal("100"),
                        buy_market_rules=rules(),
                        sell_market_rules=rules(
                            market_id="cex:beta:AAVE/USDT",
                        ),
                    )

    def test_rules_reject_market_asset_increment_and_lineage_drift(self):
        cases = (
            ({"base_asset": "aave"}, "asset"),
            ({"market_id": "cex:alpha:AAVE/USDC"}, "market"),
            ({"base_increment": Decimal("0.001")}, "base units"),
            ({"source_record_sha256": "not-a-hash"}, "SHA-256"),
            ({"base_unit_decimals": True}, "decimals"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, message):
                    rules(**overrides)


class CexQuantityQuoteTests(unittest.TestCase):
    def quote(
        self,
        levels,
        *,
        direction="buy",
        current_target=None,
        current_rules=None,
        current_fee=None,
        source_quote_asset="USDT",
        full_book_reported=False,
    ):
        return quote_cex_book_quantity(
            levels,
            current_target or target(),
            current_rules or rules(),
            current_fee or fee(),
            direction=direction,
            source_quote_asset=source_quote_asset,
            full_book_reported=full_book_reported,
            state_id="book-state-1",
        )

    def test_walks_partial_final_level_for_buy_and_sell(self):
        buy = self.quote([
            (Decimal("101"), Decimal("0.4")),
            (Decimal("102"), Decimal("1")),
        ])
        sell = self.quote(
            [
                (Decimal("100"), Decimal("0.7")),
                (Decimal("99"), Decimal("1")),
            ],
            direction="sell",
            current_fee=fee(charge_basis="received_quote"),
        )

        self.assertIsInstance(buy, QuantityQuote)
        self.assertEqual(buy.status, "calculation_complete")
        self.assertTrue(buy.complete)
        self.assertTrue(buy.calculation_complete)
        self.assertFalse(buy.strict_eligible)
        self.assertEqual(
            buy.reason_code,
            "authenticated_upstream_unavailable",
        )
        self.assertEqual(buy.filled_gross_base_quantity, Decimal("1"))
        self.assertEqual(buy.filled_gross_base_raw, 100)
        self.assertEqual(buy.net_base_received_quantity, Decimal("1"))
        self.assertEqual(buy.net_base_received_raw, 100)
        self.assertEqual(buy.gross_quote_quantity, Decimal("101.6"))
        self.assertEqual(buy.quote_debit_quantity, Decimal("101.6"))
        self.assertEqual(buy.fee_application, "additional_debit")
        self.assertIsNone(buy.target_token_address)
        self.assertIsNone(buy.quote_token_address)
        self.assertEqual(buy.levels_or_ticks_consumed, 2)
        self.assertEqual(buy.ending_price, Decimal("102"))

        self.assertEqual(sell.status, "calculation_complete")
        self.assertFalse(sell.strict_eligible)
        self.assertEqual(sell.base_debit_quantity, Decimal("1"))
        self.assertEqual(sell.base_debit_raw, 100)
        self.assertEqual(sell.gross_quote_quantity, Decimal("99.7"))
        self.assertEqual(sell.quote_received_quantity, Decimal("99.7"))
        self.assertEqual(sell.fee_application, "additional_debit")
        self.assertEqual(sell.levels_or_ticks_consumed, 2)

    def test_partial_book_retains_observed_fill_but_is_not_strict(self):
        limited = self.quote([
            (Decimal("101"), Decimal("0.4")),
        ])
        full_book = self.quote(
            [(Decimal("101"), Decimal("0.4"))],
            full_book_reported=True,
        )

        for result, reason in (
            (limited, "source_level_limit"),
            (full_book, "full_book_insufficient_liquidity"),
        ):
            self.assertEqual(result.status, "partial")
            self.assertFalse(result.complete)
            self.assertFalse(result.calculation_complete)
            self.assertFalse(result.strict_eligible)
            self.assertEqual(result.reason_code, reason)
            self.assertEqual(result.filled_gross_base_quantity, Decimal("0.4"))
            self.assertEqual(result.gross_quote_quantity, Decimal("40.4"))

    def test_lot_and_min_notional_apply_after_quote_rounding(self):
        lot_failure = self.quote(
            [(Decimal("100"), Decimal("2"))],
            current_target=target("0.95"),
            current_rules=rules(base_increment=Decimal("0.1")),
        )
        rounded_minimum = self.quote(
            [(Decimal("99.999"), Decimal("1"))],
            current_target=target("0.1"),
            current_rules=rules(
                quote_unit_decimals=2,
                quote_increment=Decimal("0.01"),
                min_quote_notional=Decimal("10"),
            ),
        )
        below_minimum = self.quote(
            [(Decimal("99"), Decimal("1"))],
            current_target=target("0.1"),
            current_rules=rules(min_quote_notional=Decimal("10")),
        )

        self.assertEqual(lot_failure.status, "unavailable")
        self.assertEqual(lot_failure.reason_code, "target_lot_misaligned")
        self.assertEqual(rounded_minimum.status, "calculation_complete")
        self.assertEqual(rounded_minimum.gross_quote_quantity, Decimal("10"))
        self.assertEqual(below_minimum.status, "unavailable")
        self.assertEqual(below_minimum.reason_code, "minimum_notional_not_met")

    def test_buy_fee_in_base_solves_exact_net_target(self):
        result = self.quote(
            [(Decimal("100"), Decimal("2"))],
            current_target=target("0.99"),
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="AAVE",
                charge_basis="received_base",
                fee_increment=Decimal("0.01"),
            ),
        )

        self.assertEqual(result.status, "calculation_complete")
        self.assertFalse(result.strict_eligible)
        self.assertEqual(result.order_base_quantity, Decimal("1"))
        self.assertEqual(result.order_base_raw, 100)
        self.assertEqual(result.gross_base_received_quantity, Decimal("1"))
        self.assertEqual(result.gross_base_received_raw, 100)
        self.assertEqual(result.fee_debit_asset, "AAVE")
        self.assertEqual(result.fee_debit_quantity, Decimal("0.01"))
        self.assertEqual(result.net_base_received_quantity, Decimal("0.99"))
        self.assertEqual(result.net_base_received_raw, 99)
        self.assertEqual(result.quote_debit_quantity, Decimal("100"))

    def test_buy_base_fee_solver_handles_nonmonotonic_rounding_exactly(self):
        cases = (
            ("4.93", "ceiling", "4.98", "0.05"),
            ("5.00", "floor", "5.05", "0.05"),
            ("4.95", "exact", "5.00", "0.05"),
        )
        for target_quantity, rounding, gross, fee_quantity in cases:
            with self.subTest(rounding=rounding):
                result = self.quote(
                    [(Decimal("100"), Decimal("10"))],
                    current_target=target(target_quantity),
                    current_fee=fee(
                        rate_bps=Decimal("100"),
                        fee_asset="AAVE",
                        charge_basis="received_base",
                        fee_increment=Decimal("0.05"),
                        rounding_mode=rounding,
                    ),
                )

                self.assertEqual(
                    result.status,
                    "calculation_complete",
                )
                self.assertEqual(result.order_base_quantity, Decimal(gross))
                self.assertEqual(
                    result.fee_debit_quantity,
                    Decimal(fee_quantity),
                )
                self.assertEqual(
                    result.net_base_received_quantity,
                    Decimal(target_quantity),
                )

    def test_buy_fee_in_quote_and_third_asset_conversion(self):
        levels = [
            (Decimal("101"), Decimal("0.4")),
            (Decimal("102"), Decimal("1")),
        ]
        quote_fee = self.quote(
            levels,
            current_fee=fee(rate_bps=Decimal("100")),
        )
        unavailable = self.quote(
            levels,
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="BNB",
                charge_basis="third_asset_quote_value",
                fee_increment=Decimal("0.000001"),
            ),
        )
        third_fee = self.quote(
            levels,
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="BNB",
                charge_basis="third_asset_quote_value",
                fee_increment=Decimal("0.000001"),
                third_asset_quote_price=Decimal("200"),
                conversion_source_record_sha256=HASH,
            ),
        )

        self.assertEqual(quote_fee.fee_debit_quantity, Decimal("1.016"))
        self.assertEqual(quote_fee.quote_debit_quantity, Decimal("102.616"))
        self.assertEqual(unavailable.status, "unavailable")
        self.assertEqual(
            unavailable.reason_code,
            "third_asset_conversion_unavailable",
        )
        self.assertEqual(third_fee.status, "calculation_complete")
        self.assertFalse(third_fee.strict_eligible)
        self.assertEqual(third_fee.fee_debit_asset, "BNB")
        self.assertEqual(third_fee.fee_debit_quantity, Decimal("0.00508"))
        self.assertEqual(third_fee.quote_debit_quantity, Decimal("101.6"))

    def test_sell_fee_in_quote_base_and_third_asset(self):
        levels = [
            (Decimal("100"), Decimal("0.7")),
            (Decimal("99"), Decimal("1")),
        ]
        quote_fee = self.quote(
            levels,
            direction="sell",
            current_fee=fee(
                rate_bps=Decimal("100"),
                charge_basis="received_quote",
            ),
        )
        base_fee = self.quote(
            levels,
            direction="sell",
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="AAVE",
                charge_basis="sold_base",
                fee_increment=Decimal("0.01"),
            ),
        )
        third_missing = self.quote(
            levels,
            direction="sell",
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="BNB",
                charge_basis="third_asset_quote_value",
                fee_increment=Decimal("0.000001"),
            ),
        )

        self.assertEqual(quote_fee.gross_quote_quantity, Decimal("99.7"))
        self.assertEqual(quote_fee.fee_debit_quantity, Decimal("0.997"))
        self.assertEqual(quote_fee.quote_received_quantity, Decimal("98.703"))
        self.assertEqual(base_fee.fee_debit_quantity, Decimal("0.01"))
        self.assertEqual(base_fee.base_debit_quantity, Decimal("1.01"))
        self.assertEqual(base_fee.base_debit_raw, 101)
        self.assertEqual(base_fee.quote_received_quantity, Decimal("99.7"))
        self.assertEqual(third_missing.status, "unavailable")

    def test_source_quote_asset_mismatch_is_fail_closed_for_upbit(self):
        result = self.quote(
            [(Decimal("100"), Decimal("2"))],
            current_rules=rules(
                market_id="cex:upbit:AAVE/USDT",
            ),
            source_quote_asset="KRW",
        )

        self.assertEqual(result.status, "unavailable")
        self.assertFalse(result.strict_eligible)
        self.assertEqual(result.reason_code, "source_quote_asset_mismatch")

    def test_large_book_math_ignores_decimal_context(self):
        levels = [
            (
                Decimal("12345678901234567890.1234"),
                Decimal("1.23"),
            ),
        ]
        with localcontext() as context:
            context.prec = 3
            result = self.quote(
                levels,
                current_target=target("1.23"),
                current_rules=rules(
                    quote_unit_decimals=6,
                    quote_increment=Decimal("0.000001"),
                ),
            )

        self.assertEqual(
            result.gross_quote_quantity,
            Decimal("15185185048518518504.851782"),
        )

    def test_collector_adapter_binds_market_side_and_raw_book_state(self):
        market = collector_market()
        book = collector_book()
        current_rules = collector_rules()
        snapshot_id = "snapshot-1"
        observed_at = "2026-08-01T12:00:30Z"
        cohort_now = "2026-08-01T12:01:00Z"
        expected_state_id = cex_quantity_state_id(
            market,
            book,
            snapshot_id=snapshot_id,
            observed_at=observed_at,
            cohort_now=cohort_now,
            market_rules=current_rules,
            fee_semantics=fee(),
        )

        result = route_quantity_quote_for_book(
            market,
            book,
            direction="buy",
            target_token_quantity=target(),
            market_rules=current_rules,
            fee_semantics=fee(),
            snapshot_id=snapshot_id,
            observed_at=observed_at,
            cohort_now=cohort_now,
            expected_state_id=expected_state_id,
        )

        self.assertEqual(result.status, "calculation_complete")
        self.assertTrue(result.calculation_complete)
        self.assertFalse(result.strict_eligible)
        self.assertEqual(result.ending_price, Decimal("101"))
        self.assertEqual(result.state_id, expected_state_id)
        self.assertEqual(result.snapshot_id, snapshot_id)
        self.assertEqual(
            result.raw_response_sha256,
            hashlib.sha256(book["raw"]).hexdigest(),
        )
        self.assertEqual(
            result.market_rules_binding_sha256,
            current_rules.record_binding_sha256,
        )
        self.assertEqual(
            result.fee_binding_sha256,
            fee().record_binding_sha256,
        )

        with self.assertRaisesRegex(ValueError, "Market ID"):
            route_quantity_quote_for_book(
                market,
                book,
                direction="sell",
                target_token_quantity=target(),
                market_rules=rules(
                    market_id="cex:beta:AAVE/USDT",
                ),
                fee_semantics=fee(charge_basis="received_quote"),
                snapshot_id=snapshot_id,
                observed_at=observed_at,
                cohort_now=cohort_now,
                expected_state_id=expected_state_id,
            )

    def test_collector_adapter_requires_exact_raw_evidence(self):
        market = collector_market()
        book = collector_book()
        del book["raw"]

        with self.assertRaisesRegex(ValueError, "raw response"):
            route_quantity_quote_for_book(
                market,
                book,
                direction="buy",
                target_token_quantity=target(),
                market_rules=collector_rules(),
                fee_semantics=fee(),
                snapshot_id="snapshot-1",
                observed_at="2026-08-01T12:00:30Z",
                cohort_now="2026-08-01T12:01:00Z",
                expected_state_id="cex-quantity:" + "0" * 64,
            )

    def test_collector_adapter_rejects_stale_rules_and_fee_at_cohort_now(self):
        market = collector_market()
        book = collector_book()
        observed_at = "2026-08-01T12:00:30Z"
        cohort_now = "2026-08-01T12:01:00Z"
        stale_rules = collector_rules(
            observed_at="2026-08-01T11:00:00Z",
            valid_until="2026-08-01T11:05:00Z",
        )
        stale_fee = fee(
            observed_at="2026-08-01T11:00:00Z",
            valid_until="2026-08-01T11:05:00Z",
        )

        for current_rules, current_fee, reason in (
            (stale_rules, fee(), "market_rules_not_current"),
            (collector_rules(), stale_fee, "fee_semantics_not_current"),
        ):
            with self.subTest(reason=reason):
                expected_state_id = cex_quantity_state_id(
                    market,
                    book,
                    snapshot_id="snapshot-1",
                    observed_at=observed_at,
                    cohort_now=cohort_now,
                    market_rules=current_rules,
                    fee_semantics=current_fee,
                )
                result = route_quantity_quote_for_book(
                    market,
                    book,
                    direction="buy",
                    target_token_quantity=target(),
                    market_rules=current_rules,
                    fee_semantics=current_fee,
                    snapshot_id="snapshot-1",
                    observed_at=observed_at,
                    cohort_now=cohort_now,
                    expected_state_id=expected_state_id,
                )
                self.assertEqual(result.status, "unavailable")
                self.assertFalse(result.calculation_complete)
                self.assertFalse(result.strict_eligible)
                self.assertEqual(result.reason_code, reason)

    def test_collector_adapter_rejects_changed_levels_reusing_state_binding(self):
        market = collector_market()
        book = collector_book()
        expected_state_id = cex_quantity_state_id(
            market,
            book,
            snapshot_id="snapshot-1",
            observed_at="2026-08-01T12:00:30Z",
            cohort_now="2026-08-01T12:01:00Z",
            market_rules=collector_rules(),
            fee_semantics=fee(),
        )
        changed = dict(book)
        changed["asks"] = [(Decimal("501"), Decimal("2"))]

        with self.assertRaisesRegex(ValueError, "state binding"):
            route_quantity_quote_for_book(
                market,
                changed,
                direction="buy",
                target_token_quantity=target(),
                market_rules=collector_rules(),
                fee_semantics=fee(),
                snapshot_id="snapshot-1",
                observed_at="2026-08-01T12:00:30Z",
                cohort_now="2026-08-01T12:01:00Z",
                expected_state_id=expected_state_id,
            )

    def test_collector_adapter_binds_snapshot_raw_time_and_full_records(self):
        market = collector_market()
        book = collector_book()
        current_rules = collector_rules()
        current_fee = fee()
        observed_at = "2026-08-01T12:00:30Z"
        cohort_now = "2026-08-01T12:01:00Z"
        expected_state_id = cex_quantity_state_id(
            market,
            book,
            snapshot_id="snapshot-1",
            observed_at=observed_at,
            cohort_now=cohort_now,
            market_rules=current_rules,
            fee_semantics=current_fee,
        )

        changed_raw = dict(book)
        changed_raw["raw"] = b'{"book":"different"}'
        for changed_book, snapshot_id, changed_observed, changed_now in (
            (changed_raw, "snapshot-1", observed_at, cohort_now),
            (book, "snapshot-2", observed_at, cohort_now),
            (book, "snapshot-1", "2026-08-01T12:00:31Z", cohort_now),
            (book, "snapshot-1", observed_at, "2026-08-01T12:01:01Z"),
        ):
            with self.subTest(
                snapshot_id=snapshot_id,
                observed_at=changed_observed,
                cohort_now=changed_now,
            ):
                with self.assertRaisesRegex(ValueError, "state binding"):
                    route_quantity_quote_for_book(
                        market,
                        changed_book,
                        direction="buy",
                        target_token_quantity=target(),
                        market_rules=current_rules,
                        fee_semantics=current_fee,
                        snapshot_id=snapshot_id,
                        observed_at=changed_observed,
                        cohort_now=changed_now,
                        expected_state_id=expected_state_id,
                    )

        object.__setattr__(
            current_rules,
            "base_increment",
            Decimal("1"),
        )
        with self.assertRaisesRegex(ValueError, "full-record binding"):
            route_quantity_quote_for_book(
                market,
                book,
                direction="buy",
                target_token_quantity=target(),
                market_rules=current_rules,
                fee_semantics=current_fee,
                snapshot_id="snapshot-1",
                observed_at=observed_at,
                cohort_now=cohort_now,
                expected_state_id=expected_state_id,
            )

    def test_collector_adapter_rejects_stale_or_future_book_state(self):
        market = collector_market()
        book = collector_book()
        for observed_at in (
            "2026-08-01T11:59:59Z",
            "2026-08-01T12:01:01Z",
        ):
            with self.subTest(observed_at=observed_at):
                expected_state_id = cex_quantity_state_id(
                    market,
                    book,
                    snapshot_id="snapshot-1",
                    observed_at=observed_at,
                    cohort_now="2026-08-01T12:01:00Z",
                    market_rules=collector_rules(),
                    fee_semantics=fee(),
                )
                result = route_quantity_quote_for_book(
                    market,
                    book,
                    direction="buy",
                    target_token_quantity=target(),
                    market_rules=collector_rules(),
                    fee_semantics=fee(),
                    snapshot_id="snapshot-1",
                    observed_at=observed_at,
                    cohort_now="2026-08-01T12:01:00Z",
                    expected_state_id=expected_state_id,
                )
                self.assertEqual(result.status, "unavailable")
                self.assertEqual(result.reason_code, "book_state_not_current")
                self.assertFalse(result.strict_eligible)

    def test_collector_book_freshness_is_exact_under_low_decimal_precision(self):
        market = collector_market()
        book = collector_book()
        observed_at = "2026-08-01T11:59:59.9999Z"
        cohort_now = "2026-08-01T12:01:00Z"

        with localcontext() as context:
            context.prec = 3
            expected_state_id = cex_quantity_state_id(
                market,
                book,
                snapshot_id="snapshot-1",
                observed_at=observed_at,
                cohort_now=cohort_now,
                market_rules=collector_rules(),
                fee_semantics=fee(),
            )
            result = route_quantity_quote_for_book(
                market,
                book,
                direction="buy",
                target_token_quantity=target(),
                market_rules=collector_rules(),
                fee_semantics=fee(),
                snapshot_id="snapshot-1",
                observed_at=observed_at,
                cohort_now=cohort_now,
                expected_state_id=expected_state_id,
            )

        self.assertEqual(result.status, "unavailable")
        self.assertEqual(result.reason_code, "book_state_not_current")

    def test_quantity_quote_validator_enforces_collector_freshness_semantics(self):
        market = collector_market()
        current_rules = collector_rules()
        current_fee = fee()
        cohort_now = "2026-08-01T12:01:00Z"

        def bound_quote(book, observed_at):
            expected_state_id = cex_quantity_state_id(
                market,
                book,
                snapshot_id="snapshot-1",
                observed_at=observed_at,
                cohort_now=cohort_now,
                market_rules=current_rules,
                fee_semantics=current_fee,
            )
            return route_quantity_quote_for_book(
                market,
                book,
                direction="buy",
                target_token_quantity=target(),
                market_rules=current_rules,
                fee_semantics=current_fee,
                snapshot_id="snapshot-1",
                observed_at=observed_at,
                cohort_now=cohort_now,
                expected_state_id=expected_state_id,
            )

        complete = bound_quote(collector_book(), "2026-08-01T12:00:30Z")
        partial = bound_quote(
            collector_book(asks=[(Decimal("101"), Decimal("0.4"))]),
            "2026-08-01T12:00:30Z",
        )
        stale = bound_quote(collector_book(), "2026-08-01T11:59:59Z")

        for calculated in (complete, partial):
            for forged_observed_at in (
                "2026-08-01T11:59:59Z",
                "2026-08-01T12:01:00.0001Z",
            ):
                with self.subTest(
                    status=calculated.status,
                    forged_observed_at=forged_observed_at,
                ):
                    with self.assertRaises(ValueError):
                        replace(
                            calculated,
                            state_observed_at=forged_observed_at,
                        )

        self.assertEqual(stale.reason_code, "book_state_not_current")
        with self.assertRaises(ValueError):
            replace(stale, state_observed_at="2026-08-01T12:00:30Z")
        with self.assertRaises(ValueError):
            replace(
                complete,
                market_id=(
                    "dex:ethereum:uniswap:"
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:AAVE"
                ),
            )

    def test_state_id_binds_submicrosecond_timestamp_exactly(self):
        common = {
            "snapshot_id": "snapshot-1",
            "cohort_now": "2026-08-01T12:01:00.0000005Z",
            "market_rules": collector_rules(),
            "fee_semantics": fee(),
        }

        fresh = cex_quantity_state_id(
            collector_market(),
            collector_book(),
            observed_at="2026-08-01T12:00:00.0000005Z",
            **common,
        )
        stale = cex_quantity_state_id(
            collector_market(),
            collector_book(),
            observed_at="2026-08-01T12:00:00.0000004Z",
            **common,
        )
        equivalent = cex_quantity_state_id(
            collector_market(),
            collector_book(),
            snapshot_id="snapshot-1",
            observed_at="2026-08-01T12:00:00.0000005000Z",
            cohort_now="2026-08-01T12:01:00.0000005000Z",
            market_rules=collector_rules(),
            fee_semantics=fee(),
        )

        self.assertNotEqual(fresh, stale)
        self.assertEqual(fresh, equivalent)

    def test_public_self_signed_records_never_become_strict(self):
        self_signed_rules = rules(source_record_sha256="f" * 64)
        self_signed_fee = fee(source_record_sha256="e" * 64)

        result = self.quote(
            [(Decimal("101"), Decimal("2"))],
            current_rules=self_signed_rules,
            current_fee=self_signed_fee,
        )

        self.assertEqual(result.status, "calculation_complete")
        self.assertTrue(result.calculation_complete)
        self.assertFalse(result.strict_eligible)
        self.assertEqual(
            result.reason_code,
            "authenticated_upstream_unavailable",
        )

    def test_quantity_quote_rejects_replace_based_contract_forgery(self):
        valid = self.quote([(Decimal("101"), Decimal("2"))])
        invalid_changes = (
            {"strict_eligible": True},
            {"contract_version": "2"},
            {"status": "unavailable"},
            {"reason_code": None},
            {"status": "unavailable", "reason_code": "invented_reason"},
            {"target_base_raw": 101},
            {"target_lattice_raw": 3},
            {
                "order_base_quantity": Decimal("2"),
                "order_base_raw": 200,
            },
            {"filled_gross_base_raw": 999},
            {"market_rules_binding_sha256": "z" * 64},
        )
        for changes in invalid_changes:
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    replace(valid, **changes)

        unavailable = self.quote(
            [(Decimal("100"), Decimal("2"))],
            current_target=target("0.95"),
            current_rules=rules(base_increment=Decimal("0.1")),
        )
        with self.assertRaises(ValueError):
            replace(unavailable, reason_code="invented_reason")
        with self.assertRaises(ValueError):
            replace(unavailable, quote_debit_asset="USDT")
        with self.assertRaises(ValueError):
            replace(unavailable, reason_code="target_asset_mismatch")

    def test_quantity_quote_rejects_buy_flow_forgeries(self):
        plain = self.quote([(Decimal("100"), Decimal("2"))])
        base_fee = self.quote(
            [(Decimal("100"), Decimal("2"))],
            current_target=target("0.99"),
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="AAVE",
                charge_basis="received_base",
                fee_increment=Decimal("0.01"),
            ),
        )
        quote_fee = self.quote(
            [(Decimal("100"), Decimal("2"))],
            current_fee=fee(rate_bps=Decimal("100")),
        )
        quote_fee_partial = self.quote(
            [(Decimal("100"), Decimal("0.4"))],
            current_fee=fee(rate_bps=Decimal("100")),
        )
        base_fee_partial = self.quote(
            [(Decimal("100"), Decimal("0.4"))],
            current_target=target("0.99"),
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="AAVE",
                charge_basis="received_base",
                fee_increment=Decimal("0.01"),
            ),
        )
        third_asset_partial = self.quote(
            [(Decimal("100"), Decimal("0.4"))],
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="BNB",
                charge_basis="third_asset_quote_value",
                fee_increment=Decimal("0.000001"),
                third_asset_quote_price=Decimal("200"),
                conversion_source_record_sha256=HASH,
            ),
        )
        cases = (
            (
                "same CEX base and quote asset",
                plain,
                {
                    "market_id": "cex:alpha:AAVE/AAVE",
                    "quote_debit_asset": "AAVE",
                },
            ),
            ("nonpositive ending price", plain, {"ending_price": Decimal("0")}),
            (
                "gross received differs from fill with matching raw alias",
                plain,
                {
                    "gross_base_received_quantity": Decimal("2"),
                    "gross_base_received_raw": 200,
                },
            ),
            (
                "net received exceeds gross with matching raw alias",
                plain,
                {
                    "net_base_received_quantity": Decimal("2"),
                    "net_base_received_raw": 200,
                },
            ),
            (
                "quote debit is below gross quote",
                plain,
                {
                    "quote_debit_quantity": Decimal("99"),
                    "net_quote_quantity": Decimal("99"),
                },
            ),
            (
                "positive CEX buy fill cannot settle to zero quote",
                plain,
                {
                    "gross_quote_quantity": Decimal("0"),
                    "quote_debit_quantity": Decimal("0"),
                    "net_quote_quantity": Decimal("0"),
                    "vwap_quote_per_base": Decimal("0"),
                    "vwap_quote_numerator": 0,
                    "vwap_quote_denominator": 1,
                },
            ),
            (
                "complete net receipt differs from paired target alias",
                plain,
                {
                    "target_base_quantity": Decimal("0.99"),
                    "target_base_raw": 99,
                },
            ),
            (
                "base fee differs from gross less net",
                base_fee,
                {"fee_debit_quantity": Decimal("0.02")},
            ),
            (
                "quote fee differs from debit less gross",
                quote_fee,
                {"fee_debit_quantity": Decimal("0.5")},
            ),
            (
                "partial quote-fee order falls below target",
                quote_fee_partial,
                {
                    "order_base_quantity": Decimal("0.5"),
                    "order_base_raw": 50,
                },
            ),
            (
                "partial quote-fee order exceeds target",
                quote_fee_partial,
                {
                    "order_base_quantity": Decimal("1.5"),
                    "order_base_raw": 150,
                },
            ),
            (
                "partial base-fee order falls below net target",
                base_fee_partial,
                {
                    "order_base_quantity": Decimal("0.5"),
                    "order_base_raw": 50,
                },
            ),
            (
                "partial base-fee order reserve is below observed fee",
                base_fee_partial,
                {
                    "order_base_quantity": Decimal("0.99"),
                    "order_base_raw": 99,
                },
            ),
            (
                "partial third-asset order differs from target",
                third_asset_partial,
                {
                    "order_base_quantity": Decimal("1.5"),
                    "order_base_raw": 150,
                },
            ),
            (
                "third asset fee changes base flow",
                third_asset_partial,
                {
                    "net_base_received_quantity": Decimal("0.39"),
                    "net_base_received_raw": 39,
                },
            ),
            (
                "third asset fee changes quote flow",
                third_asset_partial,
                {
                    "quote_debit_quantity": Decimal("40.1"),
                    "net_quote_quantity": Decimal("40.1"),
                },
            ),
        )
        for label, quote, changes in cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    replace(quote, **changes)

    def test_quantity_quote_rejects_sell_flow_forgeries(self):
        plain = self.quote(
            [(Decimal("100"), Decimal("2"))],
            direction="sell",
            current_fee=fee(charge_basis="received_quote"),
        )
        partial = self.quote(
            [(Decimal("100"), Decimal("0.4"))],
            direction="sell",
            current_fee=fee(charge_basis="received_quote"),
        )
        base_fee = self.quote(
            [(Decimal("100"), Decimal("2"))],
            direction="sell",
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="AAVE",
                charge_basis="sold_base",
                fee_increment=Decimal("0.01"),
            ),
        )
        quote_fee = self.quote(
            [(Decimal("100"), Decimal("2"))],
            direction="sell",
            current_fee=fee(
                rate_bps=Decimal("100"),
                charge_basis="received_quote",
            ),
        )
        third_asset_partial = self.quote(
            [(Decimal("100"), Decimal("0.4"))],
            direction="sell",
            current_fee=fee(
                rate_bps=Decimal("100"),
                fee_asset="BNB",
                charge_basis="third_asset_quote_value",
                fee_increment=Decimal("0.000001"),
                third_asset_quote_price=Decimal("200"),
                conversion_source_record_sha256=HASH,
            ),
        )
        cases = (
            (
                "order differs from target with matching raw alias",
                partial,
                {
                    "order_base_quantity": Decimal("0.9"),
                    "order_base_raw": 90,
                },
            ),
            (
                "base debit is below fill with matching raw alias",
                plain,
                {
                    "base_debit_quantity": Decimal("0.99"),
                    "base_debit_raw": 99,
                },
            ),
            (
                "quote received exceeds gross quote",
                plain,
                {
                    "quote_received_quantity": Decimal("101"),
                    "net_quote_quantity": Decimal("101"),
                },
            ),
            (
                "base fee differs from debit less fill",
                base_fee,
                {"fee_debit_quantity": Decimal("0.02")},
            ),
            (
                "quote fee differs from gross less receipt",
                quote_fee,
                {"fee_debit_quantity": Decimal("0.5")},
            ),
            (
                "third asset fee changes base flow",
                third_asset_partial,
                {
                    "base_debit_quantity": Decimal("0.41"),
                    "base_debit_raw": 41,
                },
            ),
            (
                "third asset fee changes quote flow",
                third_asset_partial,
                {
                    "quote_received_quantity": Decimal("39.9"),
                    "net_quote_quantity": Decimal("39.9"),
                },
            ),
        )
        for label, quote, changes in cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    replace(quote, **changes)

    def test_dex_quote_keeps_generic_boundaries_without_cex_fee_rules(self):
        cex_quote = self.quote([(Decimal("100"), Decimal("2"))])
        dex_quote = replace(
            cex_quote,
            market_id=(
                "dex:ethereum:uniswap:"
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:AAVE"
            ),
        )

        self.assertIsInstance(
            replace(dex_quote, fee_debit_quantity=Decimal("7")),
            QuantityQuote,
        )
        with self.assertRaises(ValueError):
            replace(dex_quote, quote_debit_asset=None)
        with self.assertRaises(ValueError):
            replace(dex_quote, quote_debit_asset="AAVE")
        with self.assertRaises(ValueError):
            replace(dex_quote, ending_price=Decimal("0"))
        with self.assertRaises(ValueError):
            replace(
                dex_quote,
                net_base_received_quantity=Decimal("2"),
                net_base_received_raw=200,
            )

        cex_sell = self.quote(
            [(Decimal("100"), Decimal("2"))],
            direction="sell",
            current_fee=fee(charge_basis="received_quote"),
        )
        dex_sell = replace(
            cex_sell,
            market_id=(
                "dex:ethereum:uniswap:"
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:AAVE"
            ),
        )
        self.assertIsInstance(dex_sell, QuantityQuote)
        with self.assertRaises(ValueError):
            replace(dex_sell, quote_received_asset=None)
        with self.assertRaises(ValueError):
            replace(dex_sell, quote_received_asset="AAVE")

    def test_quantity_quote_public_validator_detects_post_init_mutation(self):
        validator = getattr(
            route_quantity_module,
            "validate_quantity_quote",
            None,
        )
        self.assertTrue(callable(validator))
        valid = self.quote([(Decimal("101"), Decimal("2"))])
        self.assertIs(validator(valid), valid)

        object.__setattr__(valid, "strict_eligible", True)
        with self.assertRaisesRegex(ValueError, "strict_eligible"):
            validator(valid)

    def test_state_binding_validates_source_instrument_and_quote_mapping(self):
        market = collector_market()
        good_book = collector_book()
        current_rules = collector_rules()
        current_fee = fee()
        common = {
            "snapshot_id": "snapshot-1",
            "observed_at": "2026-08-01T12:00:30Z",
            "cohort_now": "2026-08-01T12:01:00Z",
            "market_rules": current_rules,
            "fee_semantics": current_fee,
        }
        self.assertTrue(
            cex_quantity_state_id(market, good_book, **common).startswith(
                "cex-quantity:"
            )
        )

        for changed, message in (
            (
                collector_book(source_instrument="AAVEUSDC"),
                "source_instrument",
            ),
            (
                collector_book(source_quote_asset="USDC"),
                "source_quote_asset",
            ),
        ):
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    cex_quantity_state_id(market, changed, **common)

        upbit_market = collector_market(exchange="upbit")
        upbit_rules = rules(market_id="cex:upbit:AAVE/USDT")
        with self.assertRaisesRegex(ValueError, "source_instrument"):
            cex_quantity_state_id(
                upbit_market,
                collector_book(source_instrument="KRW-AAVE"),
                **{**common, "market_rules": upbit_rules},
            )

    def test_collector_freezes_book_once_before_binding_and_quote(self):
        stable = collector_book()
        expected_state_id = cex_quantity_state_id(
            collector_market(),
            stable,
            snapshot_id="snapshot-1",
            observed_at="2026-08-01T12:00:30Z",
            cohort_now="2026-08-01T12:01:00Z",
            market_rules=collector_rules(),
            fee_semantics=fee(),
        )

        class MutatingBook(dict):
            def __init__(self, values):
                super().__init__(values)
                self.ask_reads = 0

            def get(self, key, default=None):
                if key == "asks":
                    self.ask_reads += 1
                    if self.ask_reads > 1:
                        return [(Decimal("501"), Decimal("2"))]
                return super().get(key, default)

        mutable_book = MutatingBook(stable)
        result = route_quantity_quote_for_book(
            collector_market(),
            mutable_book,
            direction="buy",
            target_token_quantity=target(),
            market_rules=collector_rules(),
            fee_semantics=fee(),
            snapshot_id="snapshot-1",
            observed_at="2026-08-01T12:00:30Z",
            cohort_now="2026-08-01T12:01:00Z",
            expected_state_id=expected_state_id,
        )

        self.assertEqual(result.ending_price, Decimal("101"))
        self.assertEqual(mutable_book.ask_reads, 1)

    def test_collector_unavailable_result_retains_all_state_bindings(self):
        market = collector_market()
        book = collector_book()
        current_rules = collector_rules(base_increment=Decimal("0.1"))
        expected_state_id = cex_quantity_state_id(
            market,
            book,
            snapshot_id="snapshot-1",
            observed_at="2026-08-01T12:00:30Z",
            cohort_now="2026-08-01T12:01:00Z",
            market_rules=current_rules,
            fee_semantics=fee(),
        )

        result = route_quantity_quote_for_book(
            market,
            book,
            direction="buy",
            target_token_quantity=target("0.95"),
            market_rules=current_rules,
            fee_semantics=fee(),
            snapshot_id="snapshot-1",
            observed_at="2026-08-01T12:00:30Z",
            cohort_now="2026-08-01T12:01:00Z",
            expected_state_id=expected_state_id,
        )

        self.assertEqual(result.status, "unavailable")
        self.assertEqual(result.reason_code, "target_lot_misaligned")
        self.assertEqual(result.state_id, expected_state_id)
        self.assertEqual(result.snapshot_id, "snapshot-1")
        self.assertEqual(
            result.raw_response_sha256,
            hashlib.sha256(book["raw"]).hexdigest(),
        )
        self.assertRegex(result.levels_binding_sha256, r"^[0-9a-f]{64}$")
        self.assertEqual(result.state_observed_at, "2026-08-01T12:00:30Z")
        self.assertEqual(result.cohort_now, "2026-08-01T12:01:00Z")

    def test_collector_target_mismatch_is_bound_unavailable_not_exception(self):
        market = collector_market()
        book = collector_book()
        expected_state_id = cex_quantity_state_id(
            market,
            book,
            snapshot_id="snapshot-1",
            observed_at="2026-08-01T12:00:30Z",
            cohort_now="2026-08-01T12:01:00Z",
            market_rules=collector_rules(),
            fee_semantics=fee(),
        )
        other_target = CommonTarget(
            asset="UNI",
            unit_decimals=2,
            raw_quantity=100,
            lattice_raw=1,
        )

        result = route_quantity_quote_for_book(
            market,
            book,
            direction="buy",
            target_token_quantity=other_target,
            market_rules=collector_rules(),
            fee_semantics=fee(),
            snapshot_id="snapshot-1",
            observed_at="2026-08-01T12:00:30Z",
            cohort_now="2026-08-01T12:01:00Z",
            expected_state_id=expected_state_id,
        )

        self.assertEqual(result.status, "unavailable")
        self.assertEqual(result.reason_code, "target_asset_mismatch")
        self.assertEqual(result.state_id, expected_state_id)
        self.assertRegex(result.raw_response_sha256, r"^[0-9a-f]{64}$")


class V2PoolQuantityQuoteTests(unittest.TestCase):
    def quote(
        self,
        *,
        direction,
        state=None,
        current_target=None,
        current_rules=None,
        target_address=TOKEN0,
        quote_address=TOKEN1,
        cohort_now="2026-08-01T12:02:00.0000005Z",
    ):
        return quote_v2_pool_quantity(
            state or v2_state(),
            current_target or dex_target(),
            current_rules or dex_rules(),
            direction=direction,
            target_token_address=target_address,
            quote_token_address=quote_address,
            cohort_now=cohort_now,
        )

    def test_known_answers_sell_exact_input_and_buy_exact_output(self):
        sell = self.quote(direction="sell")
        buy = self.quote(direction="buy")

        self.assertEqual(sell.status, "calculation_complete")
        self.assertEqual(sell.reason_code, "fixed_block_fee_proof_not_authenticated")
        self.assertFalse(sell.strict_eligible)
        self.assertEqual(sell.base_debit_quantity, Decimal("10"))
        self.assertEqual(sell.base_debit_raw, 10 * 10**18)
        self.assertEqual(sell.gross_quote_quantity, Decimal("906.610893"))
        self.assertEqual(sell.quote_received_asset, "USDC")
        self.assertEqual(sell.quote_received_quantity, Decimal("906.610893"))
        self.assertEqual(sell.fee_debit_asset, "AAVE")
        self.assertEqual(sell.fee_debit_quantity, Decimal("0"))
        self.assertEqual(sell.fee_application, "embedded_in_quote")
        self.assertEqual(sell.vwap_quote_per_base, Decimal("90.6610893"))
        self.assertEqual(sell.ending_price, Decimal("82.6671737"))
        self.assertEqual(
            (sell.ending_price_numerator, sell.ending_price_denominator),
            (826_671_737, 10_000_000),
        )

        self.assertEqual(buy.status, "calculation_complete")
        self.assertEqual(buy.gross_base_received_quantity, Decimal("10"))
        self.assertEqual(buy.net_base_received_quantity, Decimal("10"))
        self.assertEqual(buy.quote_debit_asset, "USDC")
        self.assertEqual(buy.quote_debit_quantity, Decimal("1114.454475"))
        self.assertEqual(buy.gross_quote_quantity, Decimal("1114.454475"))
        self.assertEqual(buy.fee_debit_asset, "USDC")
        self.assertEqual(buy.fee_debit_quantity, Decimal("0"))
        self.assertEqual(buy.fee_application, "embedded_in_quote")
        self.assertEqual(buy.vwap_quote_per_base, Decimal("111.4454475"))
        self.assertIsNone(buy.ending_price)
        self.assertEqual(
            (buy.ending_price_numerator, buy.ending_price_denominator),
            (444_578_179, 3_600_000),
        )

        for result in (sell, buy):
            self.assertEqual(result.levels_or_ticks_consumed, 1)
            self.assertEqual(result.target_token_address, TOKEN0)
            self.assertEqual(result.quote_token_address, TOKEN1)
            self.assertTrue(result.state_id.startswith("dex-v2-quantity:"))
            self.assertEqual(result.raw_response_sha256, "f" * 64)
            self.assertEqual(result.levels_binding_sha256, result.state_id.split(":", 1)[1])

    def test_token1_direction_uses_addresses_not_symbols(self):
        state = v2_state(
            token0_address=TOKEN1,
            token1_address=TOKEN0,
            token0_decimals=6,
            token1_decimals=18,
            reserve0_raw=10_000_000_000,
            reserve1_raw=100 * 10**18,
        )
        sell = self.quote(
            direction="sell",
            state=state,
            target_address=TOKEN0,
            quote_address=TOKEN1,
        )
        buy = self.quote(
            direction="buy",
            state=state,
            target_address=TOKEN0,
            quote_address=TOKEN1,
        )

        self.assertEqual(sell.quote_received_quantity, Decimal("906.610893"))
        self.assertEqual(buy.quote_debit_quantity, Decimal("1114.454475"))
        self.assertEqual(sell.fee_debit_asset, "AAVE")
        self.assertEqual(buy.fee_debit_asset, "USDC")

    def test_one_raw_target_is_quoted_with_integer_math(self):
        state = v2_state(
            token1_decimals=18,
            reserve0_raw=10**18,
            reserve1_raw=10**24,
        )
        current_rules = dex_rules(
            quote_unit_decimals=18,
            quote_increment=Decimal("0.000000000000000001"),
        )
        one_raw = dex_target(raw=1)

        sell = self.quote(
            direction="sell",
            state=state,
            current_target=one_raw,
            current_rules=current_rules,
        )
        buy = self.quote(
            direction="buy",
            state=state,
            current_target=one_raw,
            current_rules=current_rules,
        )

        self.assertEqual(sell.base_debit_raw, 1)
        self.assertEqual(sell.quote_received_quantity, Decimal("0.000000000000996999"))
        self.assertEqual(buy.net_base_received_raw, 1)
        self.assertEqual(buy.quote_debit_quantity, Decimal("0.000000000001003010"))

        zero_output = self.quote(
            direction="sell",
            current_target=dex_target(raw=1),
        )
        self.assertEqual(zero_output.status, "unavailable")
        self.assertEqual(zero_output.reason_code, "pool_output_below_one_raw")
        self.assertIsNone(zero_output.quote_received_quantity)
        self.assertIsNone(zero_output.fee_application)

    def test_insufficient_reserve_is_controlled_unavailable_without_residue(self):
        result = self.quote(
            direction="buy",
            current_target=dex_target(raw=100 * 10**18),
        )

        self.assertEqual(result.status, "unavailable")
        self.assertEqual(result.reason_code, "pool_reserve_insufficient")
        self.assertFalse(result.calculation_complete)
        self.assertFalse(result.strict_eligible)
        for field_name in (
            "order_base_quantity",
            "filled_gross_base_quantity",
            "gross_base_received_quantity",
            "net_base_received_quantity",
            "base_debit_quantity",
            "gross_quote_quantity",
            "net_quote_quantity",
            "quote_debit_asset",
            "quote_debit_quantity",
            "quote_received_asset",
            "quote_received_quantity",
            "fee_debit_asset",
            "fee_debit_quantity",
            "fee_application",
            "target_token_address",
            "quote_token_address",
            "ending_price",
            "ending_price_numerator",
            "ending_price_denominator",
            "vwap_quote_per_base",
        ):
            self.assertIsNone(getattr(result, field_name), field_name)
        self.assertEqual(result.levels_or_ticks_consumed, 0)

    def test_identity_mismatches_fail_closed_with_controlled_reasons(self):
        cases = (
            (
                {"target_address": "0x" + "9" * 40},
                "pool_state_token_address_mismatch",
            ),
            (
                {
                    "current_rules": dex_rules(
                        base_unit_decimals=17,
                        base_increment=Decimal("0.00000000000000001"),
                    )
                },
                "pool_state_token_decimals_mismatch",
            ),
            (
                {
                    "current_rules": dex_rules(
                        market_id=f"dex:eth:sushiswap:{POOL}:AAVE"
                    )
                },
                "pool_state_market_mismatch",
            ),
            (
                {
                    "current_rules": dex_rules(
                        market_id=(
                            "dex:eth:uniswap_v2:"
                            "0x4444444444444444444444444444444444444444:AAVE"
                        )
                    )
                },
                "pool_state_market_mismatch",
            ),
        )
        for kwargs, reason in cases:
            with self.subTest(reason=reason):
                result = self.quote(direction="sell", **kwargs)
                self.assertEqual(result.status, "unavailable")
                self.assertEqual(result.reason_code, reason)
                self.assertIsNone(result.gross_quote_quantity)

    def test_v2_adapter_rejects_a_cex_market_contract_without_assertions(self):
        with self.assertRaisesRegex(ValueError, "DEX MarketRules"):
            self.quote(direction="sell", current_rules=rules())

    def test_pool_state_rejects_malformed_block_hash_and_fee_proof(self):
        cases = (
            ({"block_number": 0}, "block_number"),
            ({"block_hash": "0x1234"}, "block_hash"),
            ({"block_header_sha256": "bad"}, "block_header"),
            ({"fee_proof_sha256": "bad"}, "fee_proof"),
            ({"fee_formula": "different-formula/v1"}, "fee_formula"),
            ({"raw_response_sha256": "bad"}, "raw_response"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, message):
                    v2_state(**overrides)

    def test_pool_state_rejects_noncanonical_or_zero_evm_identities(self):
        cases = (
            ({"pool_address": "0x" + "A" * 40}, "pool_address"),
            ({"token0_address": "0x" + "B" * 40}, "token0_address"),
            ({"token1_address": "0x" + "0" * 40}, "token1_address"),
            ({"pool_address": "0x" + "0" * 40}, "pool_address"),
            ({"block_hash": "0x" + "D" * 64}, "block_hash"),
            ({"block_hash": "0x" + "0" * 64}, "block_hash"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, message):
                    v2_state(**overrides)

    def test_pool_state_binds_supported_chain_name_to_chain_id(self):
        self.assertEqual(
            route_quantity_module.EVM_CHAIN_ID_BY_NAME,
            CHAIN_ID_BY_NAME,
        )
        with self.assertRaisesRegex(ValueError, "chain_id"):
            v2_state(chain_id=8453)
        self.assertEqual(
            v2_state(chain="base", chain_id=8453).chain_id,
            8453,
        )

    def test_pool_state_reserves_must_fit_the_v2_uint112_abi(self):
        for field_name in ("reserve0_raw", "reserve1_raw"):
            with self.subTest(field_name=field_name):
                self.assertEqual(
                    getattr(v2_state(**{field_name: (1 << 112) - 1}), field_name),
                    (1 << 112) - 1,
                )
                with self.assertRaisesRegex(ValueError, field_name):
                    v2_state(**{field_name: 1 << 112})

    def test_state_id_binds_exact_submicrosecond_time_and_all_evidence(self):
        fresh = v2_state(observed_at="2026-08-01T12:00:00.0000005Z")
        stale = v2_state(observed_at="2026-08-01T12:00:00.0000004Z")
        equivalent = v2_state(observed_at="2026-08-01T12:00:00.0000005000Z")

        self.assertNotEqual(fresh.state_id, stale.state_id)
        self.assertEqual(fresh.state_id, equivalent.state_id)
        for field_name, replacement in (
            ("chain", "base"),
            ("reserve_timestamp_last_raw", 1_704_067_201),
            ("block_number", 124),
            ("block_hash", "0x" + "1" * 64),
            ("block_header_sha256", "2" * 64),
            ("fee_proof_sha256", "3" * 64),
            ("raw_response_sha256", "4" * 64),
        ):
            with self.subTest(field_name=field_name):
                self.assertNotEqual(
                    fresh.state_id,
                    v2_state(
                        **(
                            {"chain": replacement, "chain_id": 8453}
                            if field_name == "chain"
                            else {field_name: replacement}
                        )
                    ).state_id,
                )
        self.assertNotEqual(
            fresh.state_id,
            v2_state(fee_bps=25, fee_numerator=9_975).state_id,
        )
        self.assertNotEqual(
            fresh.state_id,
            v2_state(fee_numerator=19_940, fee_denominator=20_000).state_id,
        )

    def test_pool_age_uses_exact_120_second_boundary(self):
        boundary = self.quote(
            direction="sell",
            cohort_now="2026-08-01T12:02:00.0000005Z",
        )
        stale = self.quote(
            direction="sell",
            cohort_now="2026-08-01T12:02:00.0000006Z",
        )
        future = self.quote(
            direction="sell",
            cohort_now="2026-08-01T11:59:59Z",
        )

        self.assertEqual(boundary.status, "calculation_complete")
        for result in (stale, future):
            self.assertEqual(result.status, "unavailable")
            self.assertEqual(result.reason_code, "pool_state_not_current")
            self.assertIsNone(result.gross_quote_quantity)

    def test_v2_market_rules_window_is_observed_inclusive_and_expiry_exclusive(self):
        at_observed = self.quote(
            direction="sell",
            state=v2_state(observed_at="2026-08-01T12:02:00Z"),
            current_rules=dex_rules(
                observed_at="2026-08-01T12:02:00Z",
                valid_until="2026-08-01T12:03:00Z",
            ),
            cohort_now="2026-08-01T12:02:00Z",
        )
        before_expiry = self.quote(
            direction="sell",
            current_rules=dex_rules(
                observed_at="2026-08-01T12:00:00Z",
                valid_until="2026-08-01T12:02:00.0000006Z",
            ),
            cohort_now="2026-08-01T12:02:00.0000005Z",
        )
        expired = self.quote(
            direction="sell",
            current_rules=dex_rules(
                observed_at="2026-08-01T11:00:00Z",
                valid_until="2026-08-01T12:01:00Z",
            ),
        )
        at_expiry = self.quote(
            direction="sell",
            current_rules=dex_rules(
                observed_at="2026-08-01T12:00:00Z",
                valid_until="2026-08-01T12:02:00.0000005Z",
            ),
        )
        future = self.quote(
            direction="sell",
            current_rules=dex_rules(
                observed_at="2026-08-01T12:03:00Z",
                valid_until="2026-08-01T12:05:00Z",
            ),
        )

        for result in (at_observed, before_expiry):
            self.assertEqual(result.status, "calculation_complete")
        for result in (expired, at_expiry, future):
            self.assertEqual(result.status, "unavailable")
            self.assertEqual(result.reason_code, "market_rules_not_current")
            self.assertIsNone(result.gross_quote_quantity)

    def test_shared_validator_rejects_exact_ending_price_forgery_for_cex_and_dex(self):
        cex = quote_cex_book_quantity(
            [(Decimal("101"), Decimal("2"))],
            target(),
            rules(),
            fee(),
            direction="buy",
            source_quote_asset="USDT",
            full_book_reported=False,
            state_id="book-state-1",
        )
        dex = self.quote(direction="sell")

        for current in (cex, dex):
            with self.subTest(market_id=current.market_id):
                with self.assertRaisesRegex(ValueError, "ending price"):
                    replace(current, ending_price_numerator=1)
        with self.assertRaisesRegex(ValueError, "fee_application"):
            replace(dex, fee_application="additional_debit")
        with self.assertRaisesRegex(ValueError, "token address"):
            replace(dex, quote_token_address=TOKEN0)

    def test_v2_evidence_validator_rejects_self_consistent_quote_forgery(self):
        state = v2_state()
        current_target = dex_target()
        current_rules = dex_rules()
        quote = self.quote(
            direction="sell",
            state=state,
            current_target=current_target,
            current_rules=current_rules,
        )
        doubled_quote = Decimal("1813.221786")
        forged_output = replace(
            quote,
            gross_quote_quantity=doubled_quote,
            net_quote_quantity=doubled_quote,
            quote_received_quantity=doubled_quote,
            vwap_quote_per_base=Decimal("181.3221786"),
            vwap_quote_numerator=906_610_893,
            vwap_quote_denominator=5_000_000,
        )
        forged_addresses = replace(
            quote,
            target_token_address="0x" + "4" * 40,
            quote_token_address="0x" + "5" * 40,
        )
        forged_market = replace(
            quote,
            market_id=(
                "dex:eth:uniswap_v2:"
                "0x4444444444444444444444444444444444444444:AAVE"
            ),
        )

        self.assertIs(
            validate_v2_quantity_quote_against_state(
                quote,
                state,
                current_target,
                current_rules,
                direction="sell",
                target_token_address=TOKEN0,
                quote_token_address=TOKEN1,
                cohort_now="2026-08-01T12:02:00.0000005Z",
            ),
            quote,
        )
        for forged in (forged_output, forged_addresses, forged_market):
            with self.subTest(forged=forged):
                with self.assertRaisesRegex(ValueError, "V2 quote evidence"):
                    validate_v2_quantity_quote_against_state(
                        forged,
                        state,
                        current_target,
                        current_rules,
                        direction="sell",
                        target_token_address=TOKEN0,
                        quote_token_address=TOKEN1,
                        cohort_now="2026-08-01T12:02:00.0000005Z",
                    )


class V2ExactInputIntegerMathTests(unittest.TestCase):
    @staticmethod
    def exact_input(**overrides):
        values = {
            "reserve_in_raw": 10,
            "reserve_out_raw": 10,
            "amount_in_raw": 4,
            "fee_numerator": 997,
            "fee_denominator": 1000,
        }
        values.update(overrides)
        return route_quantity_module.v2_exact_input_amount_out_raw(**values)

    @staticmethod
    def quote_wire_sha256(direction):
        quote = quote_v2_pool_quantity(
            v2_state(),
            dex_target(),
            dex_rules(),
            direction=direction,
            target_token_address=TOKEN0,
            quote_token_address=TOKEN1,
            cohort_now="2026-08-01T12:02:00.0000005Z",
        )
        payload = {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in vars(quote).items()
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def test_receipt_bound_two_leg_cashflow_exposes_exact_output_plateau(self):
        first_leg_uni_out = self.exact_input()
        second_leg_weth_out = self.exact_input(
            reserve_in_raw=10,
            reserve_out_raw=20,
            amount_in_raw=first_leg_uni_out,
        )
        legacy_buy = quote_v2_pool_quantity(
            v2_state(
                token0_decimals=0,
                token1_decimals=0,
                reserve0_raw=10,
                reserve1_raw=10,
            ),
            CommonTarget(
                asset="AAVE",
                unit_decimals=0,
                raw_quantity=2,
                lattice_raw=1,
            ),
            dex_rules(
                base_unit_decimals=0,
                quote_unit_decimals=0,
                base_increment=Decimal("1"),
                quote_increment=Decimal("1"),
            ),
            direction="buy",
            target_token_address=TOKEN0,
            quote_token_address=TOKEN1,
            cohort_now="2026-08-01T12:02:00.0000005Z",
        )

        self.assertEqual(first_leg_uni_out, 2)
        self.assertEqual(legacy_buy.quote_debit_quantity, Decimal("3"))
        self.assertEqual(4 - legacy_buy.quote_debit_quantity, Decimal("1"))
        self.assertEqual(second_leg_weth_out, 3)
        actual_gross_cashflow = Decimal(second_leg_weth_out) - Decimal(4)
        legacy_gross_cashflow = (
            Decimal(second_leg_weth_out) - legacy_buy.quote_debit_quantity
        )
        self.assertEqual(actual_gross_cashflow, Decimal("-1"))
        self.assertEqual(legacy_gross_cashflow, Decimal("0"))
        self.assertEqual(
            legacy_gross_cashflow - actual_gross_cashflow,
            Decimal("1"),
        )
        self.assertEqual(
            self.exact_input(
                reserve_in_raw=10,
                reserve_out_raw=20,
                amount_in_raw=first_leg_uni_out + 1,
            ),
            4,
        )

    def test_reserve_order_integer_floor_and_one_wei_boundary_are_exact(self):
        self.assertEqual(self.exact_input(amount_in_raw=2), 1)
        self.assertEqual(self.exact_input(amount_in_raw=3), 2)
        self.assertEqual(
            self.exact_input(reserve_in_raw=10, reserve_out_raw=20),
            5,
        )
        self.assertEqual(
            self.exact_input(reserve_in_raw=20, reserve_out_raw=10),
            1,
        )
        self.assertEqual(
            self.exact_input(
                reserve_in_raw=1000,
                reserve_out_raw=2000,
                amount_in_raw=100,
            ),
            181,
        )

    def test_all_inputs_must_be_positive_non_boolean_integers(self):
        fields = (
            "reserve_in_raw",
            "reserve_out_raw",
            "amount_in_raw",
            "fee_numerator",
            "fee_denominator",
        )
        for field_name in fields:
            for invalid in (0, -1, True, False, 1.0, "1"):
                with self.subTest(field_name=field_name, invalid=invalid):
                    with self.assertRaisesRegex(ValueError, "positive integer"):
                        self.exact_input(**{field_name: invalid})

    def test_zero_output_reserve_exhaustion_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "strictly between"):
            self.exact_input(reserve_out_raw=1)
        with self.assertRaisesRegex(ValueError, "strictly between"):
            self.exact_input(
                reserve_in_raw=10**30,
                reserve_out_raw=2,
                amount_in_raw=1,
            )

    def test_arbitrary_precision_math_does_not_overflow_intermediates(self):
        result = self.exact_input(
            reserve_in_raw=1 << 255,
            reserve_out_raw=1 << 200,
            amount_in_raw=1 << 255,
        )

        self.assertEqual(
            result,
            802262008075219481580038160272478274769472401002225566747857,
        )
        self.assertLess(result, 1 << 200)

    def test_997_over_1000_matches_phase2_foundry_integer_math(self):
        cases = (
            (1000, 100000, 100000),
            (100, 1000, 2000),
            (100, 2000, 1000),
            (1 << 255, 1 << 255, 1 << 200),
        )
        for amount_in, reserve_in, reserve_out in cases:
            with self.subTest(
                amount_in=amount_in,
                reserve_in=reserve_in,
                reserve_out=reserve_out,
            ):
                self.assertEqual(
                    self.exact_input(
                        amount_in_raw=amount_in,
                        reserve_in_raw=reserve_in,
                        reserve_out_raw=reserve_out,
                    ),
                    quote_v2_exact_in(amount_in, reserve_in, reserve_out),
                )

    def test_live_wrapper_signatures_and_known_answer_bytes_remain_frozen(self):
        expected_parameters = {
            route_quantity_module.v2_exact_input_amount_out_raw: (
                (
                    "reserve_in_raw",
                    "reserve_out_raw",
                    "amount_in_raw",
                    "fee_numerator",
                    "fee_denominator",
                ),
                0,
                (),
            ),
            quote_v2_pool_quantity: (
                (
                    "pool_state",
                    "target_token_quantity",
                    "market_rules",
                    "direction",
                    "target_token_address",
                    "quote_token_address",
                    "cohort_now",
                ),
                3,
                (),
            ),
            validate_v2_quantity_quote_against_state: (
                (
                    "quote",
                    "pool_state",
                    "target_token_quantity",
                    "market_rules",
                    "direction",
                    "target_token_address",
                    "quote_token_address",
                    "cohort_now",
                ),
                4,
                (),
            ),
            build_route_opportunity: (
                (
                    "cohort_id",
                    "route",
                    "requested_notional_usd",
                    "common_target",
                    "buy_leg",
                    "sell_leg",
                    "buy_quote",
                    "sell_quote",
                    "buy_quote_evidence",
                    "sell_quote_evidence",
                    "buy_usd_projection",
                    "sell_usd_projection",
                    "cost_components",
                    "mode_evidence",
                    "now",
                    "publication_attestation",
                ),
                0,
                ("publication_attestation",),
            ),
        }
        for function, contract in expected_parameters.items():
            with self.subTest(function=function.__name__):
                names, positional_count, defaulted_names = contract
                signature = inspect.signature(function)
                self.assertEqual(tuple(signature.parameters), names)
                for index, parameter in enumerate(signature.parameters.values()):
                    expected_kind = (
                        inspect.Parameter.POSITIONAL_OR_KEYWORD
                        if index < positional_count
                        else inspect.Parameter.KEYWORD_ONLY
                    )
                    self.assertIs(parameter.kind, expected_kind)
                    expected_default = (
                        None
                        if parameter.name in defaulted_names
                        else inspect.Parameter.empty
                    )
                    self.assertIs(parameter.default, expected_default)

        self.assertEqual(
            self.quote_wire_sha256("sell"),
            "329ee68cd5472c64cffc886b01108d1acb98bc5471cf9fa987bcad0d466c0889",
        )
        self.assertEqual(
            self.quote_wire_sha256("buy"),
            "64e233855d47a083c4bc17e856c89999c9209377c6ee38b9b7aef12e0796a7a1",
        )


class V1NonRegressionTests(unittest.TestCase):
    def test_fixed_notional_v1_rows_remain_byte_equivalent(self):
        market = {
            "token_symbol": "UNI",
            "exchange": "binance",
            "cex_symbol": "UNI/USDT",
        }
        book = {
            "bids": [
                (Decimal("99.99"), Decimal("2")),
                (Decimal("98.90"), Decimal("5")),
            ],
            "asks": [
                (Decimal("100.01"), Decimal("3")),
                (Decimal("101.10"), Decimal("7")),
            ],
            "source_instrument": "UNIUSDT",
            "source_sequence": "123",
            "source_observed_at": "2026-07-27T00:00:00+00:00",
            "source_endpoint": "https://example.test/depth",
            "raw": b'{"book":"raw"}',
            "source_quote_asset": "USDT",
            "quote_to_usd": Decimal("1"),
            "quote_conversion_method": "USDT=USD proxy",
            "quote_conversion_endpoint": "",
            "quote_conversion_response_sha256": "",
            "full_book_reported": False,
        }
        rows = execution_rows_for_book(
            market,
            book,
            snapshot_id="depth-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
        )
        encoded = json.dumps(
            rows,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")

        self.assertEqual(len(rows), 10)
        self.assertEqual(
            hashlib.sha256(encoded).hexdigest(),
            "bfa2a4d83a2fe6dcade45a045c4b361fb429e5f5fcd04de1b3d316c0d22b12ed",
        )


if __name__ == "__main__":
    unittest.main()
