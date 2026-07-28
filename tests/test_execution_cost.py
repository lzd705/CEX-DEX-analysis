import unittest
from decimal import Decimal

from scripts.execution_cost import (
    EXECUTION_DIRECTIONS,
    EXECUTION_NOTIONALS_USD,
    decimal_text,
    execution_api_rows,
    execution_fact_row,
    usd_price_timing,
    validate_execution_snapshot,
)


def common_fields(market_id="cex:test:UNI/USDT", *, measured=True):
    common = {
        "snapshot_id": "execution-1",
        "source_snapshot_id": "depth-1",
        "calculation_method": "normalized_order_book_level_walk",
        "observed_at": "2026-07-28T00:00:00+00:00",
        "state_observed_at": "2026-07-28T00:00:00+00:00",
        "request_started_at": "2026-07-28T00:00:00+00:00",
        "response_received_at": "2026-07-28T00:00:01+00:00",
        "market_id": market_id,
        "market_type": "cex",
        "token_symbol": "UNI",
        "exchange": "test",
        "cex_symbol": "UNI/USDT",
        "source_instrument": "UNIUSDT",
        "base_asset": "UNI",
        "source_quote_asset": "USDT",
        "reference_price_method": "order_book_midpoint",
        "fee_status": "excluded_unknown_account_tier",
        "usd_conversion_status": "USDT=USD proxy",
        "excluded_costs": "taker_fee,lot_size,latency",
        "source": "test public spot order-book API",
        "source_endpoint": "https://example.test/depth",
        "source_sequence": "123",
        "raw_response_sha256": "a" * 64,
    }
    if market_id.startswith("dex:"):
        _prefix, chain, dex, pool_address, token = market_id.split(":", 4)
        common.update(
            {
                "market_type": "dex",
                "token_symbol": token,
                "exchange": "",
                "cex_symbol": "",
                "source_instrument": "",
                "base_asset": "",
                "source_quote_asset": "",
                "chain": chain,
                "dex": dex,
                "pool_address": pool_address,
                "block_number": "123",
                "block_timestamp": "2026-07-28T00:00:00+00:00",
                "protocol_model": "constant_product_v2",
                "target_token_address": "0x" + "1" * 40,
                "target_token_decimals": "18",
                "quote_token_address": "0x" + "2" * 40,
                "quote_token_decimals": "6",
                "fee_status": "included_protocol_fee",
                "fee_rate_bps": "30" if measured else "",
                "source_sequence": "123",
                "usd_price_source_snapshot_id": "tvl-1" if measured else "",
                "usd_price_observed_at": (
                    "2026-07-28T00:00:00+00:00" if measured else ""
                ),
                "usd_conversion_status": (
                    "observed_inventory_token_price" if measured else ""
                ),
            }
        )
    return common


def complete_rows(market_id="cex:test:UNI/USDT"):
    rows = []
    common = common_fields(market_id)
    for index, notional in enumerate(EXECUTION_NOTIONALS_USD, start=1):
        target = notional / Decimal(100)
        rate = Decimal(index) / Decimal(10_000)
        rows.append(
            execution_fact_row(
                common=common,
                direction="sell_token",
                requested_notional_usd=notional,
                status="observed",
                status_reason="target_filled",
                reference_price_quote_per_token=100,
                quote_to_usd=1,
                target_token_quantity=target,
                filled_token_quantity=target,
                quote_amount=notional * (Decimal(1) - rate),
                levels_or_ticks_consumed=index,
                ending_marginal_price_quote_per_token=100 * (Decimal(1) - rate),
            )
        )
        rows.append(
            execution_fact_row(
                common=common,
                direction="buy_token",
                requested_notional_usd=notional,
                status="observed",
                status_reason="target_filled",
                reference_price_quote_per_token=100,
                quote_to_usd=1,
                target_token_quantity=target,
                filled_token_quantity=target,
                quote_amount=notional * (Decimal(1) + rate),
                levels_or_ticks_consumed=index,
                ending_marginal_price_quote_per_token=100 * (Decimal(1) + rate),
            )
        )
    return rows


class ExecutionCostContractTest(unittest.TestCase):
    def test_usd_price_timing_boundaries_and_timezone_inputs(self):
        state = "2026-07-28T00:00:00Z"
        self.assertEqual(
            usd_price_timing(state, "2026-07-28T00:15:00+00:00")["status"],
            "current",
        )
        self.assertEqual(
            usd_price_timing(state, "2026-07-28T00:15:00.1+00:00")["status"],
            "warning",
        )
        self.assertTrue(
            usd_price_timing(state, "2026-07-28T02:00:00+00:00")["usable"]
        )
        stale = usd_price_timing(
            state,
            "2026-07-28T10:00:01+08:00",
        )
        self.assertEqual(stale["skew_seconds"], 7201)
        self.assertEqual(stale["status"], "stale")
        self.assertFalse(stale["usable"])
        self.assertEqual(
            usd_price_timing(state, "2026-07-28T00:00:00")["status"],
            "unavailable",
        )

    def test_publication_gate_rejects_stale_measured_dex_price(self):
        market_id = "dex:eth:test:0xpool:UNI"
        current = [
            {**row, "usd_price_observed_at": "2026-07-28T02:00:00+00:00"}
            for row in complete_rows(market_id)
        ]
        validate_execution_snapshot(
            [market_id],
            current,
            enforce_usd_price_timing=True,
        )
        stale = [
            {**row, "usd_price_observed_at": "2026-07-28T02:00:01+00:00"}
            for row in complete_rows(market_id)
        ]
        with self.assertRaisesRegex(ValueError, "stale USD price"):
            validate_execution_snapshot(
                [market_id],
                stale,
                enforce_usd_price_timing=True,
            )

    def test_decimal_text_preserves_more_than_default_context_precision(self):
        huge = Decimal("11351911656616966530148279426555390")
        small = Decimal("0.123456789012345678901234567890123456789")

        self.assertEqual(decimal_text(huge), str(huge))
        self.assertEqual(decimal_text(small), str(small))

    def test_complete_sell_and_buy_publish_symmetric_shortfall(self):
        rows = complete_rows()
        sell = next(
            row
            for row in rows
            if row["direction"] == "sell_token"
            and row["requested_notional_usd"] == "1000"
        )
        buy = next(
            row
            for row in rows
            if row["direction"] == "buy_token"
            and row["requested_notional_usd"] == "1000"
        )

        self.assertEqual(sell["quoted_execution_cost_usd"], "0.1")
        self.assertEqual(sell["quoted_execution_cost_bps"], "1")
        self.assertEqual(buy["quoted_execution_cost_usd"], "0.1")
        self.assertEqual(buy["quoted_execution_cost_bps"], "1")
        self.assertEqual(buy["fill_ratio"], "1")

    def test_observed_execution_rejects_zero_quote_output(self):
        with self.assertRaisesRegex(ValueError, "positive quote amount"):
            execution_fact_row(
                common=common_fields(),
                direction="sell_token",
                requested_notional_usd=1000,
                status="observed",
                status_reason="target_filled",
                reference_price_quote_per_token=100,
                quote_to_usd=1,
                target_token_quantity=10,
                filled_token_quantity=10,
                quote_amount=0,
            )

        rows = complete_rows()
        rows[0].update(
            {
                "quote_amount": "0",
                "quote_amount_usd": "0",
                "filled_vwap_quote_per_token": "0",
                "filled_vwap_usd_per_token": "0",
                "quoted_execution_cost_usd": "1000",
                "quoted_execution_cost_bps": "10000",
            }
        )
        with self.assertRaisesRegex(ValueError, "positive quote amount"):
            validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_quantized_reference_notional_is_not_counted_as_slippage(self):
        row = execution_fact_row(
            common=common_fields(),
            direction="sell_token",
            requested_notional_usd=1000,
            status="observed",
            status_reason="target_filled",
            reference_price_quote_per_token=100,
            quote_to_usd=1,
            target_token_quantity=Decimal("9.999999"),
            filled_token_quantity=Decimal("9.999999"),
            quote_amount=Decimal("999.9999"),
        )

        self.assertEqual(row["reference_notional_usd"], "999.9999")
        self.assertEqual(row["quoted_execution_cost_usd"], "0")
        self.assertEqual(row["quoted_execution_cost_bps"], "0")

    def test_partial_retains_fill_and_quote_but_withholds_full_cost(self):
        row = execution_fact_row(
            common=common_fields(),
            direction="sell_token",
            requested_notional_usd=5000,
            status="partial",
            status_reason="source_level_limit",
            reference_price_quote_per_token=100,
            quote_to_usd=1,
            target_token_quantity=50,
            filled_token_quantity=20,
            quote_amount=1980,
            levels_or_ticks_consumed=100,
        )

        self.assertEqual(row["fill_ratio"], "0.4")
        self.assertEqual(row["quote_amount_usd"], "1980")
        self.assertEqual(row["filled_vwap_quote_per_token"], "")
        self.assertEqual(row["quoted_execution_cost_usd"], "")
        self.assertEqual(row["quoted_execution_cost_bps"], "")

    def test_terminal_statuses_never_publish_execution_numbers(self):
        for status in ("unsupported", "failed"):
            with self.subTest(status=status):
                row = execution_fact_row(
                    common=common_fields(),
                    direction="sell_token",
                    requested_notional_usd=1000,
                    status=status,
                    status_reason=f"{status}_source",
                    error="source error" if status == "failed" else "",
                )
                self.assertEqual(row["target_token_quantity"], "")
                self.assertEqual(row["quoted_execution_cost_usd"], "")

    def test_snapshot_validation_checks_exact_ten_row_coverage(self):
        rows = complete_rows()
        validate_execution_snapshot(["cex:test:UNI/USDT"], rows)
        with self.assertRaisesRegex(ValueError, "expected 10"):
            validate_execution_snapshot(
                ["cex:test:UNI/USDT"],
                rows[:-1],
            )

    def test_snapshot_validation_rejects_mixed_snapshot_lineage(self):
        rows = complete_rows()
        rows[0] = {**rows[0], "snapshot_id": "execution-2"}

        with self.assertRaisesRegex(ValueError, "one non-empty snapshot_id"):
            validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_snapshot_validation_requires_source_identity(self):
        rows = complete_rows()
        rows[0] = {**rows[0], "source_snapshot_id": ""}

        with self.assertRaisesRegex(
            ValueError,
            "one non-empty source_snapshot_id",
        ):
            validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_measured_rows_require_raw_source_provenance(self):
        for field, value in (
            ("raw_response_sha256", ""),
            ("raw_response_sha256", "not-a-sha256"),
            ("state_observed_at", ""),
            ("source_endpoint", ""),
            ("reference_price_method", ""),
            ("fee_status", ""),
            ("usd_conversion_status", ""),
            ("excluded_costs", ""),
            ("source", ""),
        ):
            with self.subTest(field=field, value=value):
                rows = [{**row, field: value} for row in complete_rows()]
                with self.assertRaisesRegex(ValueError, "provenance|sha256"):
                    validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_measured_dex_rows_require_fixed_block_and_token_provenance(self):
        market_id = "dex:eth:test:0xpool:UNI"
        for field in (
            "block_number",
            "block_timestamp",
            "protocol_model",
            "target_token_address",
            "target_token_decimals",
            "quote_token_address",
            "quote_token_decimals",
            "fee_rate_bps",
            "usd_price_source_snapshot_id",
            "usd_price_observed_at",
        ):
            with self.subTest(field=field):
                rows = [{**row, field: ""} for row in complete_rows(market_id)]
                with self.assertRaisesRegex(ValueError, "fixed-block provenance"):
                    validate_execution_snapshot([market_id], rows)

    def test_measured_dex_rows_require_valid_integer_pool_fee(self):
        market_id = "dex:eth:test:0xpool:UNI"
        for value in ("30.5", "10000"):
            with self.subTest(value=value):
                rows = [
                    {**row, "fee_rate_bps": value}
                    for row in complete_rows(market_id)
                ]
                with self.assertRaisesRegex(ValueError, "fee_rate_bps"):
                    validate_execution_snapshot([market_id], rows)

    def test_measured_dex_rows_require_coherent_fixed_block_lineage(self):
        market_id = "dex:eth:test:0xpool:UNI"
        for field, value, message in (
            ("block_number", "not-a-block", "block_number"),
            (
                "state_observed_at",
                "2026-07-28T00:01:00+00:00",
                "block_timestamp",
            ),
            ("source_sequence", "124", "source_sequence"),
        ):
            with self.subTest(field=field):
                rows = [{**row, field: value} for row in complete_rows(market_id)]
                with self.assertRaisesRegex(ValueError, message):
                    validate_execution_snapshot([market_id], rows)

    def test_all_rows_require_the_contract_notional_definition(self):
        rows = [
            {**row, "notional_definition": "arbitrary"}
            for row in complete_rows()
        ]

        with self.assertRaisesRegex(ValueError, "notional definition"):
            validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_dex_target_must_be_an_integer_number_of_base_units(self):
        market_id = "dex:eth:test:0xpool:UNI"
        rows = [
            {
                **row,
                "target_token_decimals": "2",
                "quote_token_decimals": "2",
            }
            for row in complete_rows(market_id)
        ]
        rows[0] = {
            **rows[0],
            "target_token_quantity": "10.001",
            "filled_token_quantity": "10.001",
            "fill_ratio": "1",
        }

        with self.assertRaisesRegex(ValueError, "integer number of base units"):
            validate_execution_snapshot([market_id], rows)

    def test_dex_fill_and_quote_must_be_base_unit_aligned(self):
        market_id = "dex:eth:test:0xpool:UNI"
        rows = [
            {
                **row,
                "target_token_decimals": "2",
                "quote_token_decimals": "2",
            }
            for row in complete_rows(market_id)
        ]
        quote_rows = [{**row} for row in rows]
        quote_rows[0]["quote_amount"] = "999.9001"
        with self.assertRaisesRegex(ValueError, "quote_amount"):
            validate_execution_snapshot([market_id], quote_rows)

        fill_rows = [{**row} for row in rows]
        fill_rows[0]["filled_token_quantity"] = "9.999"
        with self.assertRaisesRegex(ValueError, "filled_token_quantity"):
            validate_execution_snapshot([market_id], fill_rows)

    def test_snapshot_validation_rejects_market_id_identity_mismatch(self):
        rows = complete_rows()
        rows[0] = {**rows[0], "exchange": "other"}

        with self.assertRaisesRegex(ValueError, "market_id"):
            validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_cex_token_identity_must_match_symbol_and_base_asset(self):
        for field, value in (
            ("token_symbol", "AAVE"),
            ("base_asset", ""),
            ("base_asset", "AAVE"),
        ):
            with self.subTest(field=field):
                rows = [{**row, field: value} for row in complete_rows()]
                with self.assertRaisesRegex(ValueError, "Token identity"):
                    validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_validation_rejects_observed_after_smaller_partial(self):
        rows = complete_rows()
        replacement = execution_fact_row(
            common=common_fields(),
            direction="sell_token",
            requested_notional_usd=5000,
            status="partial",
            status_reason="source_level_limit",
            reference_price_quote_per_token=100,
            quote_to_usd=1,
            target_token_quantity=50,
            filled_token_quantity=20,
            quote_amount=1980,
        )
        rows = [
            replacement
            if row["direction"] == "sell_token"
            and row["requested_notional_usd"] == "5000"
            else row
            for row in rows
        ]
        with self.assertRaisesRegex(ValueError, "becomes observed"):
            validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_partial_fill_and_quote_facts_must_be_present_together(self):
        for missing_field in ("filled_token_quantity", "quote_amount"):
            with self.subTest(missing_field=missing_field):
                rows = complete_rows()
                partial = next(
                    row
                    for row in rows
                    if row["direction"] == "sell_token"
                    and row["requested_notional_usd"] == "100000"
                )
                partial.update(
                    {
                        "status": "partial",
                        "status_reason": "source_level_limit",
                        "filled_vwap_quote_per_token": "",
                        "filled_vwap_usd_per_token": "",
                        "quoted_execution_cost_usd": "",
                        "quoted_execution_cost_bps": "",
                    }
                )
                partial[missing_field] = ""
                if missing_field == "filled_token_quantity":
                    partial["fill_ratio"] = ""
                else:
                    partial["quote_amount_usd"] = ""
                with self.assertRaisesRegex(ValueError, "present together"):
                    validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_validation_rejects_improvement_beyond_quote_unit_resolution(self):
        rows = complete_rows()
        improved = execution_fact_row(
            common=common_fields(),
            direction="sell_token",
            requested_notional_usd=5000,
            status="observed",
            status_reason="target_filled",
            reference_price_quote_per_token=100,
            quote_to_usd=1,
            target_token_quantity=50,
            filled_token_quantity=50,
            quote_amount=Decimal("4999.75"),
        )
        rows = [
            improved
            if row["direction"] == "sell_token"
            and row["requested_notional_usd"] == "5000"
            else row
            for row in rows
        ]

        with self.assertRaisesRegex(ValueError, "cost decreases"):
            validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_validation_rejects_csv_row_that_improves_beyond_reference(self):
        rows = complete_rows()
        sell = next(
            row
            for row in rows
            if row["direction"] == "sell_token"
            and row["requested_notional_usd"] == "1000"
        )
        sell.update(
            {
                "quote_amount": "1001",
                "quote_amount_usd": "1001",
                "filled_vwap_quote_per_token": "100.1",
                "filled_vwap_usd_per_token": "100.1",
                "quoted_execution_cost_usd": "0",
                "quoted_execution_cost_bps": "0",
            }
        )

        with self.assertRaisesRegex(ValueError, "improves beyond"):
            validate_execution_snapshot(["cex:test:UNI/USDT"], rows)

    def test_unsupported_market_requires_all_ten_terminal_rows(self):
        rows = [
            execution_fact_row(
                common=common_fields(
                    "dex:solana:orca:pool:UNI",
                    measured=False,
                ),
                direction=direction,
                requested_notional_usd=notional,
                status="unsupported",
                status_reason="unsupported_chain",
            )
            for notional in EXECUTION_NOTIONALS_USD
            for direction in EXECUTION_DIRECTIONS
        ]

        validate_execution_snapshot(["dex:solana:orca:pool:UNI"], rows)

    def test_api_shape_preserves_missing_as_none(self):
        partial = execution_fact_row(
            common=common_fields(),
            direction="sell_token",
            requested_notional_usd=1000,
            status="partial",
            status_reason="source_level_limit",
            reference_price_quote_per_token=100,
            quote_to_usd=1,
            target_token_quantity=10,
            filled_token_quantity=5,
            quote_amount=495,
        )
        parsed = execution_api_rows(
            [partial],
            number_parser=lambda value: None if value in (None, "") else float(value),
        )

        self.assertEqual(parsed[0]["requested_notional_usd"], 1000.0)
        self.assertIsNone(parsed[0]["quoted_execution_cost_usd"])

    def test_api_preserves_exact_decimal_quantities_as_strings(self):
        rows = complete_rows()
        huge = "11351911656616966530148279426555390"
        rows[0]["target_token_quantity"] = huge
        parsed = execution_api_rows(
            [rows[0]],
            number_parser=lambda value: float(value),
        )

        self.assertEqual(parsed[0]["target_token_quantity"], huge)
        self.assertIsInstance(parsed[0]["quoted_execution_cost_bps"], str)


if __name__ == "__main__":
    unittest.main()
