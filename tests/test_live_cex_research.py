import copy
from decimal import Decimal
import hashlib
import json
import unittest

from scripts.live_cex_research import (
    build_live_cex_research_universe,
    live_cex_research_generation,
    public_fee_semantics,
)
from scripts.route_quantity import MarketRules


class PublicFeeSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.now = "2026-09-04T12:00:00Z"
        self.rules = MarketRules(
            market_id="cex:binance:UNI/USDT",
            base_asset="UNI",
            quote_asset="USDT",
            base_unit_decimals=4,
            quote_unit_decimals=2,
            base_increment=Decimal("0.0001"),
            quote_increment=Decimal("0.01"),
            min_base_quantity=Decimal("0.0001"),
            min_quote_notional=Decimal("1"),
            observed_at="2026-09-04T11:55:00Z",
            valid_until="2026-09-04T13:00:00Z",
            source_record_sha256="f" * 64,
        )

    def component(self, *, direction="buy", status="bounded_estimate"):
        unavailable = status == "unavailable"
        return {
            "contract_version": "1",
            "cohort_id": "cohort:" + "c" * 64,
            "opportunity_id": "opportunity:" + "d" * 64,
            "leg": direction,
            "market_id": self.rules.market_id,
            "direction": "buy_token" if direction == "buy" else "sell_token",
            "requested_notional_usd": "1000",
            "target_token_quantity": "10",
            "component_type": "venue_taker_fee",
            "value_status": status,
            "amount_usd": None if unavailable else "1",
            "rate_bps": None if unavailable else "10",
            "basis": (
                "no current public fee reference matches the exact venue and "
                "instrument; no numeric fee inferred"
                if unavailable else
                "official public spot taker-fee range; public interval [4,10] "
                "bps; maximum reviewed public reference rate projected for a "
                "non-strict research scenario; not an authenticated account, "
                "regional, or pair-specific fee; fee_asset=UNI"
            ),
            "strict_eligible": False,
            "embedded_in_leg_quote": False,
            "observed_at": None if unavailable else "2026-09-04T11:58:00Z",
            "valid_until": None if unavailable else "2026-09-04T12:30:00Z",
            "source": (
                "CEX fee evidence unavailable"
                if unavailable else "https://www.binance.com/en/fee/trading"
            ),
            "source_record_sha256": None if unavailable else "a" * 64,
            "reason_code": (
                "cex_fee_public_bound_unavailable" if unavailable else None
            ),
        }

    def test_bounded_estimate_uses_maximum_public_reference_rate(self):
        buy_component = self.component()
        buy = public_fee_semantics(
            buy_component,
            direction="buy",
            rules=self.rules,
            now=self.now,
        )
        self.assertEqual(buy.rate_bps, Decimal("10"))
        self.assertEqual(buy.fee_asset, "UNI")
        self.assertEqual(buy.charge_basis, "received_base")
        self.assertEqual(buy.fee_increment, self.rules.base_increment)
        self.assertEqual(buy.rounding_mode, "ceiling")
        self.assertEqual(
            buy.source_record_sha256,
            buy_component["source_record_sha256"],
        )

        sell_component = self.component(direction="sell")
        sell = public_fee_semantics(
            sell_component,
            direction="sell",
            rules=self.rules,
            now=self.now,
        )
        self.assertEqual(sell.rate_bps, Decimal("10"))
        self.assertEqual(sell.fee_asset, "USDT")
        self.assertEqual(sell.charge_basis, "received_quote")
        self.assertEqual(sell.fee_increment, self.rules.quote_increment)
        self.assertEqual(
            sell.source_record_sha256,
            sell_component["source_record_sha256"],
        )

    def test_unavailable_component_stays_terminal_and_has_stable_gross_mechanics(self):
        component = self.component(status="unavailable")
        before = copy.deepcopy(component)
        fee = public_fee_semantics(
            component,
            direction="buy",
            rules=self.rules,
            now=self.now,
        )
        expected_hash = hashlib.sha256(json.dumps(
            component,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()

        self.assertEqual(component, before)
        self.assertEqual(component["value_status"], "unavailable")
        self.assertIsNone(component["rate_bps"])
        self.assertEqual(fee.rate_bps, Decimal("0"))
        self.assertEqual(fee.fee_asset, "UNI")
        self.assertEqual(fee.charge_basis, "received_base")
        self.assertEqual(fee.source_record_sha256, expected_hash)
        self.assertLessEqual(fee.observed_at, self.now)
        self.assertGreater(fee.valid_until, self.now)

    def test_mismatched_component_identity_rate_window_and_hash_are_rejected(self):
        cases = {
            "leg": {"leg": "sell", "direction": "sell_token"},
            "market": {"market_id": "cex:bybit:UNI/USDT"},
            "rate": {"rate_bps": "11"},
            "window": {"valid_until": self.now},
            "source hash": {"source_record_sha256": "A" * 64},
        }
        for label, mutation in cases.items():
            with self.subTest(label=label):
                component = self.component()
                component.update(mutation)
                with self.assertRaises(ValueError):
                    public_fee_semantics(
                        component,
                        direction="buy",
                        rules=self.rules,
                        now=self.now,
                    )


class LiveCexResearchUniverseTests(unittest.TestCase):
    def test_fixed_uni_and_cake_universe_is_complete_and_reproducible(self):
        universe = build_live_cex_research_universe()
        generation = (
            "2b473d16979914513eb60843c0c3574141b01ba0f0d193628aa54d62c101bb9b"
        )

        self.assertEqual(universe["schema"], "route_universe/v1")
        self.assertEqual(
            universe["requested_notionals_usd"],
            [1000, 5000, 10000, 50000, 100000],
        )
        self.assertEqual(
            [leg["market_id"] for leg in universe["selected_legs"]],
            [
                "cex:binance:UNI/USDT",
                "cex:bybit:UNI/USDT",
                "cex:binance:CAKE/USDT",
                "cex:bybit:CAKE/USDT",
            ],
        )
        self.assertEqual(
            [route["route_id"] for route in universe["routes"]],
            [
                "route:CAKE:cex:binance:CAKE/USDT->cex:bybit:CAKE/USDT:"
                "prepositioned_inventory",
                "route:CAKE:cex:bybit:CAKE/USDT->cex:binance:CAKE/USDT:"
                "prepositioned_inventory",
                "route:UNI:cex:binance:UNI/USDT->cex:bybit:UNI/USDT:"
                "prepositioned_inventory",
                "route:UNI:cex:bybit:UNI/USDT->cex:binance:UNI/USDT:"
                "prepositioned_inventory",
            ],
        )
        self.assertEqual(live_cex_research_generation(), generation)
        self.assertEqual(universe["candidate_source_generation"], generation)
        self.assertTrue(all(
            leg["execution_adapter_supported"] is True
            and leg["execution_adapter_status"] == "supported"
            and leg["selection_inputs"]["cex_selected_window_usd"] is None
            for leg in universe["selected_legs"]
        ))
        for route in universe["routes"]:
            self.assertEqual(
                route["buy_market_id"].split(":", 2)[2],
                route["sell_market_id"].split(":", 2)[2],
            )
            self.assertEqual(route["candidate_source_generation"], generation)
            self.assertEqual(
                route["requested_notionals_usd"],
                [1000, 5000, 10000, 50000, 100000],
            )
            self.assertIsNone(route["buy_reference_volume_usd"])
            self.assertIsNone(route["sell_reference_volume_usd"])
            self.assertIsNone(route["route_volume_usd"])
