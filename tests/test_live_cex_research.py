import unittest

from scripts.live_cex_research import (
    build_live_cex_research_universe,
    live_cex_research_generation,
)


class LiveCexResearchUniverseTests(unittest.TestCase):
    def test_fixed_uni_usdt_universe_is_complete_and_reproducible(self):
        universe = build_live_cex_research_universe()
        generation = (
            "9b742170b0c0af598adc16528523e306758a7a35d3f84e419e7e8aeb4dc2a3ce"
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
            ],
        )
        self.assertEqual(
            [route["route_id"] for route in universe["routes"]],
            [
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
            self.assertEqual(route["candidate_source_generation"], generation)
            self.assertEqual(
                route["requested_notionals_usd"],
                [1000, 5000, 10000, 50000, 100000],
            )
            self.assertIsNone(route["buy_reference_volume_usd"])
            self.assertIsNone(route["sell_reference_volume_usd"])
            self.assertIsNone(route["route_volume_usd"])
