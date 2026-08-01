"""Exact common-quantity and CEX route-quote contract tests."""

from __future__ import annotations

import hashlib
import json
import unittest
from dataclasses import replace
from decimal import Decimal, localcontext

import scripts.route_quantity as route_quantity_module
from scripts.fetch_cex_depth import (
    cex_quantity_state_id,
    execution_rows_for_book,
    route_quantity_quote_for_book,
)
from scripts.route_quantity import (
    CommonTarget,
    FeeSemantics,
    MarketRules,
    QuantityQuote,
    common_net_target_quantity,
    quote_cex_book_quantity,
)


HASH = "a" * 64
OTHER_HASH = "b" * 64
OBSERVED_AT = "2026-08-01T12:00:00Z"
VALID_UNTIL = "2026-08-01T12:05:00Z"


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
        self.assertEqual(buy.levels_or_ticks_consumed, 2)
        self.assertEqual(buy.ending_price, Decimal("102"))

        self.assertEqual(sell.status, "calculation_complete")
        self.assertFalse(sell.strict_eligible)
        self.assertEqual(sell.base_debit_quantity, Decimal("1"))
        self.assertEqual(sell.base_debit_raw, 100)
        self.assertEqual(sell.gross_quote_quantity, Decimal("99.7"))
        self.assertEqual(sell.quote_received_quantity, Decimal("99.7"))
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
